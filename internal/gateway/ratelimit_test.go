package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

// callOrder 记下限流器和账本被调用的先后。
type callOrder struct {
	mu    sync.Mutex
	steps []string
}

func (tr *callOrder) add(s string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.steps = append(tr.steps, s)
}

func (tr *callOrder) get() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.steps)
}

// stubLimiter 是限流器的替身：按设定放行、拒绝或出错，记下每次收到的桶、context 的剩余时间和是否已取消。
type stubLimiter struct {
	denial              *ratelimit.Denial
	admitErr, adjustErr error
	trace               *callOrder

	mu      sync.Mutex
	admits  [][]ratelimit.Bucket
	adjusts [][]ratelimit.Bucket
	ctxs    []limiterCtx
}

type limiterCtx struct {
	method   string
	left     time.Duration
	canceled bool
}

func (s *stubLimiter) record(ctx context.Context, method string, bs []ratelimit.Bucket) {
	dl, has := ctx.Deadline()
	left := time.Duration(-1)
	if has {
		left = time.Until(dl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctxs = append(s.ctxs, limiterCtx{method, left, ctx.Err() != nil})
	if method == "Admit" {
		s.admits = append(s.admits, slices.Clone(bs))
	} else {
		s.adjusts = append(s.adjusts, slices.Clone(bs))
	}
	if s.trace != nil {
		s.trace.add(method)
	}
}

func (s *stubLimiter) Admit(ctx context.Context, bs []ratelimit.Bucket) (*ratelimit.Denial, error) {
	s.record(ctx, "Admit", bs)
	return s.denial, s.admitErr
}

func (s *stubLimiter) Adjust(ctx context.Context, bs []ratelimit.Bucket) error {
	s.record(ctx, "Adjust", bs)
	return s.adjustErr
}

func (s *stubLimiter) calls() (admits, adjusts [][]ratelimit.Bucket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.admits), slices.Clone(s.adjusts)
}

// rated 包住一个账本：查到的 Key 带上给定的 Key 和账户的限额；trace 不为 nil 时记下 Reserve。
type rated struct {
	Ledger
	key, account ledger.Rate
	trace        *callOrder
}

func (r *rated) KeyByHash(ctx context.Context, hash string) (*ledger.Key, error) {
	k, err := r.Ledger.KeyByHash(ctx, hash)
	if k != nil {
		k.Rate, k.AccountRate = r.key, r.account
	}
	return k, err
}

func (r *rated) Reserve(ctx context.Context, res ledger.Reservation) (*ledger.Replay, error) {
	if r.trace != nil {
		r.trace.add("Reserve")
	}
	return r.Ledger.Reserve(ctx, res)
}

// replaying 是总给出重放的账本替身。
type replaying struct{ Ledger }

func (replaying) Reserve(context.Context, ledger.Reservation) (*ledger.Replay, error) {
	return &ledger.Replay{CallID: "qf-original", Response: ledger.Response{Status: 200, Body: []byte(`{"ok":true}`)}}, nil
}

var bothRates = ledger.Rate{RPM: 100, TPM: 1_000_000}

// 一个请求要检查的桶：三层里限额不为 0 的那些；RPM 花 1，TPM 花预留量。
func TestBuckets(t *testing.T) {
	c := &call{
		key:     &ledger.Key{ID: 7, AccountID: 3, Rate: ledger.Rate{RPM: 10}, AccountRate: ledger.Rate{RPM: 20, TPM: 2000}},
		model:   &config.Model{Upstream: &config.Upstream{Name: "up:a", TPM: 9000}},
		planned: ledger.Reservation{EstInput: 40, EstOutput: 60},
	}
	want := []ratelimit.Bucket{
		{Layer: "key", ID: "7", Metric: "rpm", Limit: 10, Cost: 1},
		{Layer: "account", ID: "3", Metric: "rpm", Limit: 20, Cost: 1},
		{Layer: "account", ID: "3", Metric: "tpm", Limit: 2000, Cost: 100},
		{Layer: "upstream", ID: "up:a", Metric: "tpm", Limit: 9000, Cost: 100},
	}
	if got := buckets(c); !slices.Equal(got, want) {
		t.Fatalf("buckets = %+v\nwant %+v", got, want)
	}
	c.key.Rate, c.key.AccountRate, c.model.Upstream.TPM = ledger.Rate{}, ledger.Rate{}, 0
	if got := buckets(c); got != nil {
		t.Fatalf("no limits: buckets = %+v", got)
	}
}

// 准入在预留之前；结算之后修正 TPM 桶。
func TestAdmissionBeforeReservation(t *testing.T) {
	tr := &callOrder{}
	lim := &stubLimiter{trace: tr}
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates, trace: tr}, lim)
	if r := h.chat(t, `{"model":"shared"}`); r.status != 200 {
		t.Fatalf("status = %d %s", r.status, r.body)
	}
	if got := tr.get(); !slices.Equal(got, []string{"Admit", "Reserve", "Adjust"}) {
		t.Fatalf("order = %v", got)
	}
}

// 被限流：429 rate_limited，带 Retry-After；不预留、不调上游、不修正；日志记下是哪个桶。
// 限流器出错：503 rate_limiter_unavailable，不带 Retry-After，同样不预留、不放行。
func TestAdmissionRejects(t *testing.T) {
	denied := &ratelimit.Denial{Bucket: ratelimit.Bucket{Layer: "account", Metric: "tpm"}, RetryAfter: 7}
	cases := []struct {
		name          string
		lim           *stubLimiter
		status        int
		typ, code, ra string
		logKey, logV  string
	}{
		{"rate limited", &stubLimiter{denial: denied}, 429, "rate_limit_error", "rate_limited", "7", "rate_limited_by", "account/tpm"},
		{"limiter down", &stubLimiter{admitErr: errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")}, 503,
			"server_error", "rate_limiter_unavailable", "", "rate_limiter_error", "dial tcp 127.0.0.1:6379: connect: connection refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &callOrder{}
			tc.lim.trace = tr
			h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates, trace: tr}, tc.lim)
			r := h.chat(t, `{"model":"shared"}`)
			var e errorBody
			if err := json.Unmarshal(r.body, &e); err != nil {
				t.Fatal(err)
			}
			if r.status != tc.status || e.Error.Type != tc.typ || e.Error.Code != tc.code || r.header.Get("Retry-After") != tc.ra {
				t.Fatalf("got %d %s, Retry-After %q", r.status, r.body, r.header.Get("Retry-After"))
			}
			if got := tr.get(); !slices.Equal(got, []string{"Admit"}) {
				t.Fatalf("calls = %v, want only Admit", got)
			}
			if h.fake.Stats().ChatCompletions != 0 {
				t.Fatal("request reached the upstream")
			}
			l := h.logLine(t, requestID(t, r))
			if l["status"] != float64(tc.status) || l["error_code"] != tc.code || l[tc.logKey] != tc.logV {
				t.Fatalf("log = %v", l)
			}
			for _, k := range []string{"reserved_tokens", "call_status", "outcome"} {
				if _, ok := l[k]; ok {
					t.Fatalf("a rejected request logged %s: %v", k, l)
				}
			}
		})
	}
}

// 一个桶都没有就不碰限流器；只有 RPM 桶时不修正（RPM 不退）。
func TestLimiterUseDependsOnBuckets(t *testing.T) {
	none := &stubLimiter{admitErr: errors.New("must not be called")}
	h := newHarnessLimited(t, newAllowAll(), none)
	if r := h.chat(t, `{"model":"shared"}`); r.status != 200 {
		t.Fatalf("no limits: status = %d %s", r.status, r.body)
	}
	if a, j := none.calls(); len(a)+len(j) != 0 {
		t.Fatalf("no limits: limiter called %d, %d times", len(a), len(j))
	}

	rpm := &stubLimiter{}
	h = newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: ledger.Rate{RPM: 5}}, rpm)
	if r := h.chat(t, `{"model":"shared"}`); r.status != 200 {
		t.Fatalf("rpm only: status = %d", r.status)
	}
	if a, j := rpm.calls(); len(a) != 1 || len(j) != 0 {
		t.Fatalf("rpm only: %d admits, %d adjusts; want 1, 0", len(a), len(j))
	}
}

// 第 2.4 节的表：每种结局怎么修正 TPM 桶。want 是加回的令牌数（每个 TPM 桶都一样），nil 表示不修正；
// settled 时按“估算 − 实际”算，在用例里由预留量和响应的 usage 算出。
func TestAdjustByOutcome(t *testing.T) {
	const est = -1 // 占位：全部加回，值取这次的预留量
	cases := []struct {
		name    string
		model   string
		ledger  func() Ledger
		headers []string
		want    int64 // est：全部加回；0：不修正；1：估算 − 实际
	}{
		{"settled, actual below estimate", "shared", nil, nil, 1},
		{"settled, actual above estimate", "fake-overuse", nil, nil, 1},
		{"unknown: timeout", "fake-hang", nil, nil, 0},
		{"unknown: upstream 5xx", "fake-500", nil, nil, 0},
		{"unknown: 2xx without usage", "fake-no-usage", nil, nil, 0},
		{"quota exhausted", "shared", func() Ledger { return &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrQuotaExhausted} }, nil, est},
		{"ledger unavailable at reserve", "shared", func() Ledger { return &faulty{Ledger: newAllowAll(), reserveErr: errors.New("boom")} }, nil, est},
		{"idempotent replay", "shared", func() Ledger { return replaying{newAllowAll()} }, []string{"Idempotency-Key", "k"}, est},
		{"idempotency 409", "shared", func() Ledger { return &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrIdempotencyInProgress} }, []string{"Idempotency-Key", "k"}, est},
		{"idempotency 422", "shared", func() Ledger { return &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrIdempotencyKeyReused} }, []string{"Idempotency-Key", "k"}, est},
		{"send intent not recorded", "shared", func() Ledger { return &faulty{Ledger: newAllowAll(), markSentErr: errors.New("boom")} }, nil, est},
		{"connection not established", "dead", nil, nil, est},
		{"upstream 4xx", "status-400", nil, nil, est},
		{"upstream 429", "fake-429", nil, nil, est},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := newAllowAll()
			var l Ledger = base
			if tc.ledger != nil {
				l = tc.ledger()
			}
			lim := &stubLimiter{}
			h := newHarnessLimited(t, &rated{Ledger: l, key: bothRates, account: ledger.Rate{TPM: 5_000_000}}, lim)
			r := h.chat(t, `{"model":"`+tc.model+`","max_tokens":100}`, tc.headers...)
			admits, adjusts := lim.calls()
			if len(admits) != 1 {
				t.Fatalf("%d admissions", len(admits))
			}
			amount := admits[0][1].Cost // key/tpm 的花费就是预留量
			want := tc.want
			switch want {
			case est:
				want = amount
			case 1:
				var resp struct {
					Usage struct {
						Total int64 `json:"total_tokens"`
					}
				}
				if err := json.Unmarshal(r.body, &resp); err != nil || resp.Usage.Total == 0 {
					t.Fatalf("no usage in %s", r.body)
				}
				want = amount - resp.Usage.Total
			}
			if want == 0 {
				if len(adjusts) != 0 {
					t.Fatalf("adjusted %+v, want no adjustment", adjusts)
				}
				return
			}
			if len(adjusts) != 1 {
				t.Fatalf("%d adjustments, want 1 (response %d %s)", len(adjusts), r.status, r.body)
			}
			var names []string
			for _, b := range adjusts[0] {
				names = append(names, b.Name())
				if b.Cost != min(-want, b.Limit) { // 再扣时一次最多扣到容量
					t.Fatalf("%s adjusted by %d, want a refund of %d capped at -%d", b.Name(), -b.Cost, want, b.Limit)
				}
			}
			if !slices.Equal(names, []string{"key/tpm", "account/tpm"}) {
				t.Fatalf("adjusted %v, want only the TPM buckets", names)
			}
		})
	}
}

// 修正失败只记日志，不影响已经写回的响应。
func TestAdjustFailureIsLogged(t *testing.T) {
	lim := &stubLimiter{adjustErr: errors.New("i/o timeout")}
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, lim)
	r := h.chat(t, `{"model":"shared"}`)
	if r.status != 200 {
		t.Fatalf("status = %d", r.status)
	}
	if l := h.logLine(t, requestID(t, r)); l["rate_limiter_error"] != "i/o timeout" || l["call_status"] != "SETTLED" {
		t.Fatalf("log = %v", l)
	}
}

// 准入和修正都有不超过 0.5 秒的期限，而且不随客户端断开而取消。
func TestLimiterCallsHaveDeadline(t *testing.T) {
	lim := &stubLimiter{}
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, lim)
	if r := h.chat(t, `{"model":"shared"}`); r.status != 200 {
		t.Fatalf("status = %d", r.status)
	}
	// 发出之后客户端断开，上游 150 毫秒后照常回答、结算：修正时客户端早已断开，修正的 context 不能跟着取消。
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reply)
	go func() { done <- h.post(ctx, "/v1/chat/completions", clientKey, `{"model":"quick"}`) }()
	waitFor(t, func() bool { return h.fake.Stats().ChatCompletions == 2 })
	cancel()
	<-done
	waitFor(t, func() bool { lim.mu.Lock(); defer lim.mu.Unlock(); return len(lim.ctxs) == 4 })
	lim.mu.Lock()
	defer lim.mu.Unlock()
	for _, c := range lim.ctxs {
		if c.left <= 0 || c.left > config.RateLimitTimeout || c.canceled {
			t.Errorf("%s: %v left, canceled %v; want a deadline at most %v away", c.method, c.left, c.canceled, config.RateLimitTimeout)
		}
	}
}

// --- 以下用真的 Redis（和 MySQL）---

// redisLimiter 是只属于当前测试的真限流器（自己的 key 前缀），加一个直接读 Redis 的客户端。
func redisLimiter(t *testing.T) (*ratelimit.Limiter, *redis.Client) {
	t.Helper()
	url := dbtest.RedisURL(t)
	l, err := ratelimit.Open(url, dbtest.RedisPrefix())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { rdb.Close() })
	return l, rdb
}

// issueRated 给环境的账户发一把带限额的 Key，返回 Key 和它的号。
func (e *dbEnv) issueRated(t *testing.T, rate ledger.Rate) (string, int64) {
	t.Helper()
	k, err := e.store.IssueKey(context.Background(), e.account, allAliases, rate)
	if err != nil {
		t.Fatal(err)
	}
	return k.Secret, k.ID
}

func (e *dbEnv) sendWith(t *testing.T, key, body string) reply {
	t.Helper()
	r := e.post(context.Background(), "/v1/chat/completions", key, body)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

// RPM 为 2 的 Key：第 3 个请求 429，账本里没有它，额度不变。
func TestRateLimitedWithRedisAndMySQL(t *testing.T) {
	lim, _ := redisLimiter(t)
	e := newDBEnvLimited(t, 1_000_000, nil, lim)
	key, _ := e.issueRated(t, ledger.Rate{RPM: 2})
	for i := range 2 {
		if r := e.sendWith(t, key, `{"model":"shared"}`); r.status != 200 {
			t.Fatalf("request %d: %d %s", i+1, r.status, r.body)
		}
	}
	calls := e.callCount(t)
	_, _, consumed := e.budget(t)
	r := e.sendWith(t, key, `{"model":"shared"}`)
	ra, _ := strconv.Atoi(r.header.Get("Retry-After"))
	if r.status != 429 || errorCode(t, r) != "rate_limited" || ra < 1 || ra > 30 {
		t.Fatalf("request 3: %d %s, Retry-After %q", r.status, r.body, r.header.Get("Retry-After"))
	}
	if l := e.logLine(t, requestID(t, r)); l["rate_limited_by"] != "key/rpm" {
		t.Fatalf("log = %v", l)
	}
	if n := e.callCount(t); n != calls {
		t.Fatalf("calls %d -> %d: the limited request reached the ledger", calls, n)
	}
	if _, reserved, c := e.budget(t); reserved != 0 || c != consumed {
		t.Fatalf("budget changed: reserved %d, consumed %d -> %d", reserved, consumed, c)
	}
	dbtest.Verify(t, e.store)
}

// TPM 的退还：第一个请求结算后退回“估算 − 实际”，第二个请求靠它放得进。对照：用量未知不退，第二个请求被拒。
// 桶的大小按一次试探请求的预留量 E 和用量 U 定成 1.5E：没有退还时还差约 E/2，按 1.5E/分钟补要约 20 秒。
func TestTPMRefundWithRedisAndMySQL(t *testing.T) {
	lim, _ := redisLimiter(t)
	e := newDBEnvLimited(t, 1_000_000, nil, lim)
	const settled = `{"model":"shared","max_tokens":1000}`
	const unknown = `{"model":"fake-no-usage","max_tokens":1000}`
	probe := e.sendWith(t, e.key, settled)
	c := e.call(t, requestID(t, probe))
	est, used := c.reserved, c.usageTotal.Int64
	if probe.status != 200 || est < 1000 || used <= 0 || used*2 > est {
		t.Fatalf("probe: %d, reserved %d, used %d", probe.status, est, used)
	}
	tpm := est * 3 / 2

	refunded, _ := e.issueRated(t, ledger.Rate{TPM: tpm})
	for i := range 2 {
		if r := e.sendWith(t, refunded, settled); r.status != 200 {
			t.Fatalf("settled request %d: %d %s", i+1, r.status, r.body)
		}
	}

	kept, _ := e.issueRated(t, ledger.Rate{TPM: tpm})
	if r := e.sendWith(t, kept, unknown); r.status != 200 {
		t.Fatalf("unknown-usage request: %d %s", r.status, r.body)
	}
	r := e.sendWith(t, kept, settled)
	if r.status != 429 {
		t.Fatalf("after an unknown-usage request: %d %s, want 429", r.status, r.body)
	}
	if l := e.logLine(t, requestID(t, r)); l["rate_limited_by"] != "key/tpm" {
		t.Fatalf("log = %v", l)
	}
}

// 上游报的用量离谱（fake-overuse，prompt_tokens 是 100 万）：修正一次最多再扣这个桶的容量 L，
// 修正之后令牌数不低于“准入后 − L”，补充按 Redis 的时间算；容量大的账户桶照扣“实际 − 估算”。账本照旧按实际结算。
func TestOveruseChargeCappedAtCapacity(t *testing.T) {
	lim, rdb := redisLimiter(t)
	e := newDBEnvLimited(t, 10_000_000, nil, lim)
	const capacity = 10_000
	key, keyID := e.issueRated(t, ledger.Rate{TPM: capacity})
	b := ratelimit.Bucket{Layer: "key", ID: strconv.FormatInt(keyID, 10), Metric: "tpm", Limit: capacity}
	before, err := rdb.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	r := e.sendWith(t, key, `{"model":"fake-overuse","max_tokens":100}`)
	if r.status != 200 {
		t.Fatalf("overuse request: %d %s", r.status, r.body)
	}
	c := e.call(t, requestID(t, r))
	if c.status.String != "SETTLED" || c.usageTotal.Int64 <= fakeupstream.OveruseTokens {
		t.Fatalf("call %s, usage %d: want SETTLED at the actual usage", c.status.String, c.usageTotal.Int64)
	}
	h, err := rdb.HMGet(context.Background(), lim.Key(b), "tokens", "ms").Result()
	if err != nil || h[0] == nil {
		t.Fatalf("bucket: %v %v", h, err)
	}
	tokens, _ := strconv.ParseFloat(h[0].(string), 64)
	ms, _ := strconv.ParseInt(h[1].(string), 10, 64)
	// 准入后是 L − 估算，修正再扣 L：不低于 −估算；往上最多是两次写之间补的
	low := -float64(c.reserved)
	high := low + float64(ms-before.UnixMilli())*capacity/60000
	if tokens < low || tokens > high {
		t.Fatalf("key/tpm holds %v tokens after the overuse, want %v to %v (a charge of at most L = %d)", tokens, low, high, capacity)
	}
}

// 估算超过桶的容量 L 的请求，桶满时照样放行（否则它永远进不来），准入后桶里是 L − 估算（负数）。上游实际用得很少：
// 修正要把“估算 − 实际”全部加回，不能像再扣那样截到 L（“一次最多再扣 L”只作用于再扣），否则这个桶被压在负数上更久。
// 修正之后令牌数是 L − 实际，补充按 Redis 的时间算；截到 −L 的话只加回 L，令牌数会落在负数上。
func TestRefundNotCappedAtCapacity(t *testing.T) {
	lim, rdb := redisLimiter(t)
	e := newDBEnvLimited(t, 10_000_000, nil, lim)
	const capacity = 500
	key, keyID := e.issueRated(t, ledger.Rate{TPM: capacity})
	b := ratelimit.Bucket{Layer: "key", ID: strconv.FormatInt(keyID, 10), Metric: "tpm", Limit: capacity}
	before, err := rdb.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	r := e.sendWith(t, key, `{"model":"shared","max_tokens":1000}`)
	if r.status != 200 {
		t.Fatalf("request: %d %s", r.status, r.body)
	}
	c := e.call(t, requestID(t, r))
	est, used := c.reserved, c.usageTotal.Int64
	if c.status.String != "SETTLED" || est <= 2*capacity || used <= 0 || used >= capacity {
		t.Fatalf("call %s: reserved %d, used %d; want SETTLED, reserved above 2L (a refund of more than L) and used below L = %d",
			c.status.String, est, used, capacity)
	}
	h, err := rdb.HMGet(context.Background(), lim.Key(b), "tokens", "ms").Result()
	if err != nil || h[0] == nil {
		t.Fatalf("bucket: %v %v", h, err)
	}
	tokens, _ := strconv.ParseFloat(h[0].(string), 64)
	ms, _ := strconv.ParseInt(h[1].(string), 10, 64)
	// 准入后是 L − 估算，退还 估算 − 实际：不低于 L − 实际；往上最多是两次写之间补的，封顶到 L
	low := float64(capacity - used)
	high := min(float64(capacity), low+float64(ms-before.UnixMilli())*capacity/60000)
	if tokens < low || tokens > high {
		t.Fatalf("key/tpm holds %v tokens after the refund, want %v to %v (all of estimate − actual = %d refunded)", tokens, low, high, est-used)
	}
}

// 预留失败（额度不够）：RPM 不退，TPM 全部加回。
func TestReserveFailureKeepsRPMRefundsTPM(t *testing.T) {
	lim, rdb := redisLimiter(t)
	e := newDBEnvLimited(t, 0, nil, lim)
	key, id := e.issueRated(t, ledger.Rate{RPM: 10, TPM: 100_000})
	if r := e.sendWith(t, key, `{"model":"shared"}`); r.status != 429 || errorCode(t, r) != "quota_exhausted" {
		t.Fatalf("status = %d %s", r.status, r.body)
	}
	tokens := func(metric string) float64 {
		b := ratelimit.Bucket{Layer: "key", ID: strconv.FormatInt(id, 10), Metric: metric}
		v, err := rdb.HGet(context.Background(), lim.Key(b), "tokens").Float64()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := tokens("rpm"); v < 9 || v >= 9.5 {
		t.Fatalf("rpm tokens = %v, want about 9: the request still counts", v)
	}
	if v := tokens("tpm"); v != 100_000 {
		t.Fatalf("tpm tokens = %v, want the full 100000 back", v)
	}
}

// Redis 接受连接却不回答：请求在 0.5 秒加少量余量内得到 503，不预留、不调上游。
func TestRedisHangsGives503(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	}()
	lim, err := ratelimit.Open("redis://"+ln.Addr().String()+"/0", "x:")
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	tr := &callOrder{}
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates, trace: tr}, lim)
	start := time.Now()
	r := h.chat(t, `{"model":"shared"}`)
	took := time.Since(start)
	if r.status != http.StatusServiceUnavailable || errorCode(t, r) != "rate_limiter_unavailable" || took > config.RateLimitTimeout+500*time.Millisecond {
		t.Fatalf("got %d %s after %v", r.status, r.body, took)
	}
	if len(tr.get()) != 0 || h.fake.Stats().ChatCompletions != 0 {
		t.Fatalf("reserved %v, upstream calls %d", tr.get(), h.fake.Stats().ChatCompletions)
	}
}
