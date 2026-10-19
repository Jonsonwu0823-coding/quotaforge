package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// 换上游和熔断经过网关的测试。fo- 开头的别名都是“第一个目标 + fake-b 的备用目标（权重 0）”：
// 第一个目标拨号失败时应当换到 fake-b，别的失败一律不换（fake-b 一个请求都收不到）。
func addFailoverModels(addTargets func(alias, purpose string, spec ...any)) {
	addTargets("fo-dead-b", "chat", "dead", "whatever-dead", 1, "fake-b", "fake-chat", 0)
	addTargets("fo-primary", "chat", "fake", "fake-chat", 1, "fake-b", "fake-chat", 0)
	// 同一个上游的两个目标：试过一次 dead 之后，另一个 dead 目标也跳过
	addTargets("fo-twice", "chat", "dead", "a", 1, "dead", "b", 1, "fake-b", "fake-chat", 0)
	for _, m := range []string{"fake-hang", "fake-500", "fake-not-json", "fake-401", "fake-429", "fake-stream-cut"} {
		addTargets("fo-"+m, "chat", "fake", m, 1, "fake-b", "fake-chat", 0)
	}
	for _, m := range []string{"redirect", "status-400", "hangup"} {
		addTargets("fo-"+m, "chat", "scripted", m, 1, "fake-b", "fake-chat", 0)
	}
	addTargets("fo-emb", "embeddings", "fake", "fake-embed", 1, "fake-b", "fake-embed", 0)
	addTargets("fo-emb-hold", "embeddings", "fake", "fake-hold", 1, "fake-b", "fake-embed", 0)
	addTargets("fo-emb-dead", "embeddings", "dead", "whatever-dead", 1, "fake-b", "fake-embed", 0)
}

// breakAfter 让熔断在连续 n 次失败后打开：harness 的配置是 1000000 次，碰不到。在发请求之前调。
func (h *harness) breakAfter(n int) {
	for _, b := range h.g.router.breakers {
		b.settings.Failures = n
	}
}

// clock 是可以拨动的时钟，换掉 router 的 now。
type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }
func (h *harness) useClock() *clock {
	c := &clock{}
	c.ns.Store(t0.UnixNano())
	h.g.router.now = c.now
	return c
}
func (h *harness) state(up string) circuit {
	b := h.g.router.breakers[up]
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// limitUpstream 给 harness 配置里的上游设 RPM、TPM 限额（配置里没写，0 是不限）。在发请求之前调。
func (h *harness) limitUpstream(name string, rpm, tpm int64) {
	for _, u := range h.cfg.Upstreams() {
		if u.Name == name {
			u.RPM, u.TPM = rpm, tpm
		}
	}
}

// 第一个目标拨号失败：换到 fake-b，客户端拿到它的回答。发送意图只落一次；账本改道一次；日志的上游是 fake-b、
// failover_from 是 dead；每次尝试一个 upstream span；指标按尝试计。
func TestFailoverOnDialFailure(t *testing.T) {
	l := newAllowAll()
	h := newHarnessWith(t, l)
	r := h.chat(t, `{"model":"fo-dead-b","messages":[{"role":"user","content":"hi"}]}`)
	if r.status != http.StatusOK || !strings.Contains(string(r.body), `"model":"fake-chat-20260101"`) {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	if b, a := h.fakeB.Stats().ChatCompletions, h.fake.Stats().ChatCompletions; b != 1 || a != 0 {
		t.Fatalf("fake-b got %d, fake got %d; want 1, 0", b, a)
	}
	id := requestID(t, r)
	if res := l.lastReservation(t); res.Upstream != "dead" || res.UpstreamModel != "whatever-dead" {
		t.Fatalf("reserved for %s/%s, want the first pick dead/whatever-dead", res.Upstream, res.UpstreamModel)
	}
	l.mu.Lock()
	sent, reroutes := l.sent, slices.Clone(l.reroutes)
	l.mu.Unlock()
	if sent != 1 || !slices.Equal(reroutes, []string{"dead/whatever-dead -> fake-b/fake-chat"}) {
		t.Fatalf("send intents %d, reroutes %v", sent, reroutes)
	}
	line := h.logLine(t, id)
	if line["upstream"] != "fake-b" || line["outcome"] != "SETTLED" || line["status"] != float64(200) ||
		!slices.Equal(anyStrings(line["failover_from"]), []string{"dead"}) || line["circuit_open"] != nil {
		t.Fatalf("log = %v", line)
	}
	root, children := h.rootSpan(t, id)
	var names, ups []string
	for _, c := range children {
		names = append(names, c.Name())
		if c.Name() == "upstream" {
			ups = append(ups, attr(c, "qf.upstream")+"/"+attr(c, "error.type"))
		}
	}
	if !slices.Equal(names, []string{"authenticate", "reserve", "mark_sent", "upstream", "reroute", "upstream", "finish"}) ||
		!slices.Equal(ups, []string{"dead/upstream_unavailable", "fake-b/"}) {
		t.Fatalf("children = %v, upstream spans = %v", names, ups)
	}
	if attr(root, "qf.upstream") != "fake-b" || attr(root, "qf.failover_from") != `["dead"]` {
		t.Fatalf("root: qf.upstream %q, qf.failover_from %q", attr(root, "qf.upstream"), attr(root, "qf.failover_from"))
	}
	m := scrape(t, h)
	for _, want := range []string{
		`qf_upstream_requests_total{endpoint="chat",result="upstream_unavailable",upstream="dead"} 1`,
		`qf_upstream_requests_total{endpoint="chat",result="ok",upstream="fake-b"} 1`,
		`qf_upstream_failovers_total{endpoint="chat",upstream="dead"} 1`,
		`qf_upstream_circuit_state{upstream="dead"} 0`,
		`qf_usage_tokens_total{endpoint="chat",type="total",upstream="fake-b"}`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func anyStrings(v any) []string {
	list, _ := v.([]any)
	var out []string
	for _, x := range list {
		s, _ := x.(string)
		out = append(out, s)
	}
	return out
}

// 换上游时请求体按新目标的上游模型名从客户端的原始请求体重新改写：流式的 include_usage 照旧，其余字节和单个目标时一样。
func TestFailoverRewritesTheClientBody(t *testing.T) {
	h := newStreamHarness(t, newAllowAll())
	body := `{ "stream" : true, "model": MODEL, "messages":[{"role":"user","content":"<hi> & 你好"}], "max_tokens":16 }`
	readAll(t, h.openStream(t, clientKey, withModel(body, `"fo-dead-b"`)))
	h.mu.Lock()
	viaFailover := string(h.upBBody)
	h.mu.Unlock()
	readAll(t, h.openStream(t, clientKey, withModel(body, `"shared"`))) // shared 也是 fake-chat，只有一个目标
	direct, _, _ := h.upstreamSaw()
	if viaFailover != string(direct) {
		t.Fatalf("fake-b got\n%s\nthe single-target path sends\n%s", viaFailover, direct)
	}
	// 和客户端发的相比只改了两处：model 换成 fake-chat（不是第一个目标的 whatever-dead），开头加上 QF 替客户端要的 usage
	want := `{"stream_options":{"include_usage":true}, "stream" : true, "model": "fake-chat", "messages":[{"role":"user","content":"<hi> & 你好"}], "max_tokens":16 }`
	if viaFailover != want {
		t.Fatalf("fake-b got\n%s\nwant\n%s", viaFailover, want)
	}
}

// readAll 读完一个流，它必须正常结束（[DONE] 之后关闭）。
func readAll(t *testing.T, s *clientStream) {
	t.Helper()
	events, err := s.rest(t)
	if !errors.Is(err, io.EOF) || len(events) == 0 || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("stream ended with %v after %d events", err, len(events))
	}
}

// deadlineRecorder 记下每次发给上游的请求的期限，再交给原来的 Transport。
type deadlineRecorder struct {
	next      http.RoundTripper
	mu        sync.Mutex
	deadlines []time.Time
}

func (d *deadlineRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	dl, _ := req.Context().Deadline()
	d.mu.Lock()
	d.deadlines = append(d.deadlines, dl)
	d.mu.Unlock()
	return d.next.RoundTrip(req)
}

// 两次尝试共用一个期限：两次发给上游的请求的期限相同（每次重新算的话，第二次晚一点）。不靠睡眠。
func TestFailoverSharesDeadline(t *testing.T) {
	h := newHarness(t)
	rec := &deadlineRecorder{next: h.g.client.Transport}
	h.g.client.Transport = rec
	if r := h.chat(t, `{"model":"fo-dead-b"}`); r.status != http.StatusOK {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.deadlines) != 2 || rec.deadlines[0].IsZero() || !rec.deadlines[0].Equal(rec.deadlines[1]) {
		t.Fatalf("upstream request deadlines = %v, want two equal ones", rec.deadlines)
	}
}

// stepDeadlines 记下换上游时账本改道和限流准入拿到的 ctx 期限。
type stepDeadlines struct {
	mu             sync.Mutex
	reroute, admit []time.Time
}

func (d *stepDeadlines) add(list *[]time.Time, ctx context.Context) {
	dl, _ := ctx.Deadline()
	d.mu.Lock()
	defer d.mu.Unlock()
	*list = append(*list, dl)
}

type deadlineLedger struct {
	Ledger
	d *stepDeadlines
}

func (l deadlineLedger) Reroute(ctx context.Context, r ledger.Reservation, up, model string) error {
	l.d.add(&l.d.reroute, ctx)
	return l.Ledger.Reroute(ctx, r, up, model)
}

type deadlineLimiter struct {
	*stubLimiter
	d *stepDeadlines
}

func (l deadlineLimiter) Admit(ctx context.Context, bs []ratelimit.Bucket) (*ratelimit.Denial, error) {
	l.d.add(&l.d.admit, ctx)
	return l.stubLimiter.Admit(ctx, bs)
}

// 换上游之间的准入和改道也受所有尝试共用的期限限制：它们的 ctx 期限不晚于发给上游的请求的期限
// （harness 的 chat 超时 0.5 秒，比账本操作的 2 秒、限流的 0.5 秒都短或相等，所以就等于它）。
func TestFailoverStepsShareDeadline(t *testing.T) {
	d := &stepDeadlines{}
	h := newHarnessLimited(t, deadlineLedger{Ledger: newAllowAll(), d: d}, deadlineLimiter{stubLimiter: &stubLimiter{}, d: d})
	h.limitUpstream("dead", 0, 9000)
	h.limitUpstream("fake-b", 0, 8000)
	rec := &deadlineRecorder{next: h.g.client.Transport}
	h.g.client.Transport = rec
	if r := h.chat(t, `{"model":"fo-dead-b"}`); r.status != http.StatusOK {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	rec.mu.Lock()
	up := rec.deadlines
	rec.mu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(up) != 2 || len(d.reroute) != 1 || len(d.admit) != 2 {
		t.Fatalf("upstream %v, reroute %v, admit %v", up, d.reroute, d.admit)
	}
	if !d.reroute[0].Equal(up[0]) || !d.admit[1].Equal(up[0]) {
		t.Fatalf("reroute deadline %v, second admission %v; want the shared upstream deadline %v", d.reroute[0], d.admit[1], up[0])
	}
}

// 限流：Key、账户两层只准入一次；换上游时只给新上游准入上游层；换走的上游的 TPM 收尾时全部加回，
// 最后那个上游按结局修正（结算：退“估算 − 实际”）。
func TestFailoverMovesUpstreamBucket(t *testing.T) {
	lim := &stubLimiter{}
	l := newAllowAll()
	h := newHarnessLimited(t, &rated{Ledger: l, key: bothRates}, lim)
	h.limitUpstream("dead", 0, 9000)
	h.limitUpstream("fake-b", 0, 8000)
	r := h.chat(t, `{"model":"fo-dead-b"}`)
	if r.status != http.StatusOK {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	amount := l.lastReservation(t).Amount()
	usage := int64(h.logLine(t, requestID(t, r))["usage"].(map[string]any)["total_tokens"].(float64))
	admits, adjusts := lim.calls()
	names := func(bs []ratelimit.Bucket) (out []string) {
		for _, b := range bs {
			out = append(out, b.Layer+":"+b.ID+":"+b.Metric)
		}
		return out
	}
	if len(admits) != 2 || !slices.Equal(names(admits[0]), []string{"key:1:rpm", "key:1:tpm", "upstream:dead:tpm"}) ||
		!slices.Equal(names(admits[1]), []string{"upstream:fake-b:tpm"}) || admits[1][0].Cost != amount {
		t.Fatalf("admits = %+v", admits)
	}
	if len(adjusts) != 1 {
		t.Fatalf("adjusts = %+v", adjusts)
	}
	// 逐个桶比较（层、编号、指标和修正量），同一个桶出现两次（例如换走的上游退两次）也看得出来。
	var got []string
	for _, b := range adjusts[0] {
		got = append(got, fmt.Sprintf("%s:%s:%s=%d", b.Layer, b.ID, b.Metric, b.Cost))
	}
	slices.Sort(got)
	want := []string{fmt.Sprintf("key:1:tpm=%d", usage-amount), fmt.Sprintf("upstream:dead:tpm=%d", -amount),
		fmt.Sprintf("upstream:fake-b:tpm=%d", usage-amount)}
	if !slices.Equal(got, want) {
		t.Fatalf("adjusted %v, want %v", got, want)
	}
}

// denyUpstream 是限流器替身：检查上游 id 那一层时拒绝（err 不为 nil 时报错），其余放行；照 stubLimiter 记下每次调用。
type denyUpstream struct {
	*stubLimiter
	id  string
	err error
}

func (d *denyUpstream) Admit(ctx context.Context, bs []ratelimit.Bucket) (*ratelimit.Denial, error) {
	_, _ = d.stubLimiter.Admit(ctx, bs)
	for _, b := range bs {
		if b.Layer == "upstream" && b.ID == d.id {
			if d.err != nil {
				return nil, d.err
			}
			return &ratelimit.Denial{Bucket: b, RetryAfter: 3}, nil
		}
	}
	return nil, nil
}

// 新上游准入不过（被限，或 Redis 出错）：不换，返回前一次的错误，确定没发出、释放；fake-b 没收到；第一个上游的桶全部加回。
func TestFailoverDeniedByRateLimit(t *testing.T) {
	for _, admitErr := range []error{nil, errors.New("redis down")} {
		lim := &denyUpstream{stubLimiter: &stubLimiter{}, id: "fake-b", err: admitErr}
		l := newAllowAll()
		h := newHarnessLimited(t, l, lim)
		h.limitUpstream("dead", 0, 9000)
		h.limitUpstream("fake-b", 0, 8000)
		r := h.chat(t, `{"model":"fo-dead-b"}`)
		if r.status != http.StatusBadGateway || errorCode(t, r) != "upstream_unavailable" {
			t.Fatalf("%v: got %d %s", admitErr, r.status, r.body)
		}
		line := h.logLine(t, requestID(t, r))
		if line["outcome"] != "CANCELLED" || line["upstream"] != "dead" || line["failover_from"] != nil {
			t.Fatalf("%v: log = %v", admitErr, line)
		}
		if admitErr == nil && line["rate_limited_by"] != "upstream/tpm" {
			t.Fatalf("log = %v, want the denying bucket", line)
		}
		if admitErr != nil && line["rate_limiter_error"] == nil {
			t.Fatalf("log = %v, want the limiter error", line)
		}
		if n := h.fakeB.Stats().ChatCompletions; n != 0 {
			t.Fatalf("fake-b got %d requests", n)
		}
		_, adjusts := lim.calls()
		amount := l.lastReservation(t).Amount()
		if len(adjusts) != 1 || len(adjusts[0]) != 1 || adjusts[0][0].ID != "dead" || adjusts[0][0].Cost != -amount {
			t.Fatalf("%v: adjusts = %+v", admitErr, adjusts)
		}
	}
}

// 换上游时新上游拿到的是半开的探测名额、上游层的准入却被拒：不换，名额要还回去。熔断回到开着（不卡在半开），
// 下一次选目标还能探测它。
func TestFailoverDeniedReturnsProbe(t *testing.T) {
	h := newHarnessLimited(t, newAllowAll(), &denyUpstream{stubLimiter: &stubLimiter{}, id: "fake-b"})
	h.limitUpstream("dead", 0, 9000)
	h.limitUpstream("fake-b", 0, 8000)
	clk := h.useClock()
	b := h.g.router.breakers["fake-b"]
	b.mu.Lock()
	b.move(open, clk.now())
	b.mu.Unlock()
	clk.advance(b.settings.Cooldown)

	r := h.chat(t, `{"model":"fo-dead-b"}`)
	if r.status != http.StatusBadGateway || errorCode(t, r) != "upstream_unavailable" {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	if line := h.logLine(t, requestID(t, r)); line["rate_limited_by"] != "upstream/tpm" || line["outcome"] != "CANCELLED" {
		t.Fatalf("log = %v", line)
	}
	if s := h.state("fake-b"); s != open {
		t.Fatalf("fake-b is %v after its probe was denied by the rate limiter; want open (the probe returned)", s)
	}
	if target, p := h.g.router.pick(h.cfg.Model("chat", "fo-dead-b").Targets[1:], nil); target == nil || p == nil || !p.probe {
		t.Fatal("the next pick cannot probe fake-b: the half-open slot leaked")
	}
}

// rerouteFails 是改道总失败的账本。
type rerouteFails struct {
	Ledger
	err error
}

func (r rerouteFails) Reroute(context.Context, ledger.Reservation, string, string) error {
	return r.err
}

// 改道没改成（被回收了，或者账本出错）：不发，fake-b 没收到；新上游刚扣的 TPM 和第一个上游的一起加回；返回前一次的错误。
func TestFailoverRerouteFails(t *testing.T) {
	for _, err := range []error{ledger.ErrNotInFlight, errors.New("db down")} {
		lim := &stubLimiter{}
		l := newAllowAll()
		h := newHarnessLimited(t, rerouteFails{Ledger: l, err: err}, lim)
		h.limitUpstream("dead", 0, 9000)
		h.limitUpstream("fake-b", 0, 8000)
		r := h.chat(t, `{"model":"fo-dead-b"}`)
		if r.status != http.StatusBadGateway || errorCode(t, r) != "upstream_unavailable" {
			t.Fatalf("%v: got %d %s", err, r.status, r.body)
		}
		line := h.logLine(t, requestID(t, r))
		if line["outcome"] != "CANCELLED" || line["upstream"] != "dead" || !strings.Contains(line["ledger_error"].(string), err.Error()) {
			t.Fatalf("%v: log = %v", err, line)
		}
		if n := h.fakeB.Stats().ChatCompletions; n != 0 {
			t.Fatalf("fake-b got %d requests", n)
		}
		admits, adjusts := lim.calls()
		amount := l.lastReservation(t).Amount()
		refunded := map[string]int64{}
		for _, b := range adjusts[0] {
			refunded[b.ID] = b.Cost
		}
		if len(admits) != 2 || len(adjusts) != 1 || refunded["dead"] != -amount || refunded["fake-b"] != -amount || len(refunded) != 2 {
			t.Fatalf("%v: admits %+v, adjusts %+v", err, admits, adjusts)
		}
	}
}

// 可能已经发出的失败一律不换：超时、5xx、正文不能用、3xx、4xx、429、发出后断开、流已经开始又中途失败。
// fake-b 一个请求都收不到，只有一个 upstream span，结局照旧。
func TestNoFailoverAfterSent(t *testing.T) {
	cases := []struct {
		alias, body, code, outcome string
	}{
		{"fo-fake-hang", "", "upstream_timeout", "USAGE_UNKNOWN"},
		{"fo-fake-500", "", "upstream_error", "USAGE_UNKNOWN"},
		{"fo-fake-not-json", "", "upstream_invalid_response", "USAGE_UNKNOWN"},
		{"fo-redirect", "", "upstream_invalid_response", "USAGE_UNKNOWN"},
		{"fo-hangup", "", "upstream_error", "USAGE_UNKNOWN"},
		{"fo-status-400", "", "upstream_bad_request", "CANCELLED"},
		{"fo-fake-401", "", "upstream_auth_failed", "CANCELLED"},
		{"fo-fake-429", "", "upstream_rate_limited", "CANCELLED"},
		{"fo-fake-stream-cut", streamOf("fo-fake-stream-cut"), "upstream_error", "USAGE_UNKNOWN"},
	}
	h := newHarness(t)
	for _, tc := range cases {
		t.Run(tc.alias, func(t *testing.T) {
			body := tc.body
			if body == "" {
				body = `{"model":"` + tc.alias + `"}`
			}
			id := requestID(t, h.chat(t, body))
			line := h.logLine(t, id)
			if line["error_code"] != tc.code || line["outcome"] != tc.outcome || line["failover_from"] != nil {
				t.Fatalf("log = %v", line)
			}
			_, children := h.rootSpan(t, id)
			if n := len(slices.DeleteFunc(children, func(s sdktrace.ReadOnlySpan) bool { return s.Name() != "upstream" })); n != 1 {
				t.Fatalf("%d upstream spans", n)
			}
			if n := h.fakeB.Stats().ChatCompletions; n != 0 {
				t.Fatalf("fake-b got %d requests", n)
			}
		})
	}
}

// 一次调用每个上游最多试一次：两个目标都在 dead 上，试过一次就都跳过，直接去 fake-b。
func TestEachUpstreamOnce(t *testing.T) {
	h := newHarness(t)
	r := h.chat(t, `{"model":"fo-twice"}`)
	if r.status != http.StatusOK {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	if line := h.logLine(t, requestID(t, r)); !slices.Equal(anyStrings(line["failover_from"]), []string{"dead"}) {
		t.Fatalf("log = %v", line)
	}
	if m := scrape(t, h); !strings.Contains(m, `qf_upstream_requests_total{endpoint="chat",result="upstream_unavailable",upstream="dead"} 1`) {
		t.Fatal("dead must be tried exactly once")
	}
}

// 只有一个目标的别名也受熔断：连续 5 次连不上之后，第 6 次快速失败，502 upstream_unavailable，在准入和预留之前：
// 不碰限流器和账本，不调上游，没有 upstream span；日志有 circuit_open，指标计一次。
func TestCircuitOpenFastFails(t *testing.T) {
	lim := &stubLimiter{}
	l := newAllowAll()
	h := newHarnessLimited(t, &rated{Ledger: l, key: bothRates}, lim)
	h.breakAfter(5)
	for i := range 5 {
		if r := h.chat(t, `{"model":"dead"}`); r.status != http.StatusBadGateway || errorCode(t, r) != "upstream_unavailable" {
			t.Fatalf("request %d: %d %s", i+1, r.status, r.body)
		}
	}
	if h.state("dead") != open {
		t.Fatalf("state %v after 5 dial failures", h.state("dead"))
	}
	admits, _ := lim.calls()
	reserved := len(l.reserved)
	r := h.chat(t, `{"model":"dead"}`)
	if r.status != http.StatusBadGateway || errorCode(t, r) != "upstream_unavailable" {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	id := requestID(t, r)
	after, _ := lim.calls()
	l.mu.Lock()
	reservedAfter := len(l.reserved)
	l.mu.Unlock()
	if len(after) != len(admits) || reservedAfter != reserved {
		t.Fatalf("admits %d → %d, reservations %d → %d: a fast failure must not admit or reserve", len(admits), len(after), reserved, reservedAfter)
	}
	line := h.logLine(t, id)
	if line["circuit_open"] != true || line["reserved_tokens"] != nil || line["outcome"] != nil || line["upstream"] != nil {
		t.Fatalf("log = %v", line)
	}
	_, children := h.rootSpan(t, id)
	if n := len(slices.DeleteFunc(children, func(s sdktrace.ReadOnlySpan) bool { return s.Name() != "upstream" })); n != 0 {
		t.Fatalf("%d upstream spans", n)
	}
	m := scrape(t, h)
	for _, want := range []string{`qf_upstream_circuit_rejections_total{endpoint="chat"} 1`, `qf_upstream_circuit_state{upstream="dead"} 2`,
		`qf_upstream_requests_total{endpoint="chat",result="upstream_unavailable",upstream="dead"} 5`} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

// 熔断按上游：别的别名把 fake 熔断之后，主目标在 fake 上的别名直接去备用，不再试 fake，也不算换上游。
// 4xx、429 不算失败，打不开熔断。
func TestCircuitOpenSkipsTarget(t *testing.T) {
	h := newHarness(t)
	h.breakAfter(5)
	for range 5 {
		h.chat(t, `{"model":"fake-401"}`)
		h.chat(t, `{"model":"fake-429"}`)
	}
	if h.state("fake") != closed {
		t.Fatal("4xx and 429 opened the circuit")
	}
	for range 5 {
		h.chat(t, `{"model":"fake-500"}`)
	}
	before := h.fake.Stats().ChatCompletions
	r := h.chat(t, `{"model":"fo-primary"}`)
	if r.status != http.StatusOK || h.fake.Stats().ChatCompletions != before || h.fakeB.Stats().ChatCompletions != 1 {
		t.Fatalf("got %d; fake %d → %d, fake-b %d", r.status, before, h.fake.Stats().ChatCompletions, h.fakeB.Stats().ChatCompletions)
	}
	if line := h.logLine(t, requestID(t, r)); line["upstream"] != "fake-b" || line["failover_from"] != nil {
		t.Fatalf("log = %v", line)
	}
}

// 半开：冷却到了之后只放一个探测。探测挂着时，别的请求去备用；探测成功之后熔断关上，又回到主目标。
func TestHalfOpenThroughGateway(t *testing.T) {
	h := newHarness(t)
	h.breakAfter(5)
	clk := h.useClock()
	for range 5 {
		h.chat(t, `{"model":"fake-500"}`)
	}
	emb := func(alias string) reply {
		return h.post(context.Background(), "/v1/embeddings", clientKey, `{"model":"`+alias+`","input":["a"]}`)
	}
	if r := emb("fo-emb"); r.status != http.StatusOK || h.fakeB.Stats().Embeddings != 1 {
		t.Fatalf("open: got %d, fake-b embeddings %d", r.status, h.fakeB.Stats().Embeddings)
	}
	clk.advance(30*time.Second - time.Nanosecond)
	if r := emb("fo-emb"); r.status != http.StatusOK || h.fakeB.Stats().Embeddings != 2 {
		t.Fatal("probed before the cooldown")
	}
	clk.advance(time.Nanosecond)
	probe := make(chan reply, 1)
	go func() { probe <- emb("fo-emb-hold") }()
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 })
	if h.state("fake") != halfOpen {
		t.Fatalf("state %v while the probe is in flight", h.state("fake"))
	}
	if r := emb("fo-emb"); r.status != http.StatusOK || h.fakeB.Stats().Embeddings != 3 {
		t.Fatal("a second request went to the half-open upstream")
	}
	h.fake.Release()
	if r := <-probe; r.status != http.StatusOK {
		t.Fatalf("probe: %d %s", r.status, r.body)
	}
	if h.state("fake") != closed {
		t.Fatalf("state %v after a successful probe", h.state("fake"))
	}
	before := h.fake.Stats().Embeddings
	if r := emb("fo-emb"); r.status != http.StatusOK || h.fake.Stats().Embeddings != before+1 {
		t.Fatal("not back on the primary after closing")
	}
}

// markSentFails 让发送意图失败，直到 fail 被清掉。
type markSentFails struct {
	Ledger
	fail atomic.Bool
}

func (m *markSentFails) MarkSent(ctx context.Context, r ledger.Reservation) error {
	if m.fail.Load() {
		return errors.New("db down")
	}
	return m.Ledger.MarkSent(ctx, r)
}

// 拿到探测名额的请求没发出去（这里是发送意图失败）：名额还回去，下一个请求照样能探测，熔断不会卡在半开。
func TestProbeReleasedOnEarlyExit(t *testing.T) {
	l := &markSentFails{Ledger: newAllowAll()}
	h := newHarnessWith(t, l)
	h.breakAfter(5)
	clk := h.useClock()
	for range 5 {
		h.chat(t, `{"model":"fake-500"}`)
	}
	clk.advance(30 * time.Second)
	l.fail.Store(true)
	if r := h.chat(t, `{"model":"fo-primary"}`); r.status != http.StatusServiceUnavailable || errorCode(t, r) != "ledger_unavailable" {
		t.Fatalf("got %d %s", r.status, r.body)
	}
	if h.state("fake") != open {
		t.Fatalf("state %v after the probe was not sent", h.state("fake"))
	}
	l.fail.Store(false)
	before := h.fake.Stats().ChatCompletions
	if r := h.chat(t, `{"model":"fo-primary"}`); r.status != http.StatusOK || h.fake.Stats().ChatCompletions != before+1 {
		t.Fatal("the next request did not probe")
	}
	if h.state("fake") != closed {
		t.Fatalf("state %v after the probe succeeded", h.state("fake"))
	}
}

// 用真账本：换上游成功的调用账本里的上游是 fake-b，结算；账单只在 fake-b 那边。带幂等键的照常：同一个键再来是重放。
// 连不上、没有别的目标可换时（只有一个目标的别名 dead）确定没发出、释放，同一个键再来照常执行。
func TestFailoverLedgerAndIdempotency(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"fo-dead-b","messages":[{"role":"user","content":"hi"}]}`
	first := e.sendKey(t, "/v1/chat/completions", "fo-key", body)
	if first.status != http.StatusOK {
		t.Fatalf("got %d %s", first.status, first.body)
	}
	id := requestID(t, first)
	e.expect(t, id, "SETTLED", []string{"reserve", "settle"}, audit{sent: true, resolvedBy: "gateway", evidence: "response"})
	var up, model string
	if err := e.admin.QueryRow("SELECT upstream, upstream_model FROM calls WHERE id = ?", id).Scan(&up, &model); err != nil {
		t.Fatal(err)
	}
	if up != "fake-b" || model != "fake-chat" {
		t.Fatalf("ledger upstream %s/%s, want fake-b/fake-chat", up, model)
	}
	if billRow(e.bill(t), id) != nil || !strings.Contains(string(e.fakeB.Bill()), id+",yes,") {
		t.Fatal("only fake-b may bill the call")
	}
	wantReplay(t, e.sendKey(t, "/v1/chat/completions", "fo-key", body), first)
	if n := e.fakeB.Stats().ChatCompletions; n != 1 {
		t.Fatalf("fake-b got %d requests, want 1 (the second was a replay)", n)
	}

	r := e.sendKey(t, "/v1/chat/completions", "dead-key", `{"model":"dead"}`)
	e.expect(t, requestID(t, r), "CANCELLED", []string{"reserve", "release"}, audit{sent: true, resolvedBy: "gateway", evidence: "not_sent"})
	again := e.sendKey(t, "/v1/chat/completions", "dead-key", `{"model":"dead"}`)
	if again.status != http.StatusBadGateway || requestID(t, again) == requestID(t, r) {
		t.Fatalf("the same key after a call that was not sent must run again: %d", again.status)
	}
	dbtest.Verify(t, e.store)
}
