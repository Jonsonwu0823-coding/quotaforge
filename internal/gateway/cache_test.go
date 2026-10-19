package gateway

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/embcache"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

// memCache 是嵌入缓存的替身：一个内存里的表。可以让读、写出错，记下每次写的键和过期时长；
// getHook、setHook 不为 nil 时，在读、写之前调用（测试用它挡住领头的、让它 panic）。写的时候 context 已经取消了就返回它的错误，
// 和真的 Redis 一样：写缓存要是跟着客户端取消，测试看得出来。
type memCache struct {
	mu             sync.Mutex
	m              map[string][]byte
	gets, sets     int
	setKeys        []string
	ttls           []time.Duration
	getErr, setErr error
	getHook        func()
	setHook        func()
}

func newMemCache() *memCache { return &memCache{m: map[string][]byte{}} }

func (c *memCache) Get(_ context.Context, key string) ([]byte, error) {
	if c.getHook != nil {
		c.getHook()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	if c.getErr != nil {
		return nil, c.getErr
	}
	return bytes.Clone(c.m[key]), nil
}

func (c *memCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.setHook != nil {
		c.setHook()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	if c.setErr != nil {
		return c.setErr
	}
	c.m[key] = bytes.Clone(value)
	c.setKeys, c.ttls = append(c.setKeys, key), append(c.ttls, ttl)
	return nil
}

// counts 是到现在为止读、写的次数。
func (c *memCache) counts() (gets, sets int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.sets
}

// corrupt 把缓存里的每一条都换成 v。
func (c *memCache) corrupt(v string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		c.m[k] = []byte(v)
	}
}

const cacheTTL = time.Hour

// withCache 装上嵌入缓存，过期时长 1 小时，抖动固定为 0.5（正好 1 倍）。
func withCache(c Cache) func(*gateway) {
	return func(g *gateway) {
		g.cache, g.cacheTTL, g.jitter = c, cacheTTL, func() float64 { return 0.5 }
	}
}

// newCacheHarness 起一个开着嵌入缓存的 QF，限流器是 lim。
// 测试结束时（在关服务器之前）取消客户端的请求、放开假上游挂住的请求：测试中途失败时，挂着的请求不会让关闭一直等。
func newCacheHarness(t *testing.T, l Ledger, lim Limiter, c Cache) *harness {
	t.Helper()
	h := newHarnessSetup(t, l, lim, tracedAt(1), chatTimeout, withCache(c))
	ctx, cancel := context.WithCancel(context.Background())
	h.clientCtx = ctx
	t.Cleanup(func() {
		cancel()
		h.fake.Release()
		h.fakeB.Release()
	})
	return h
}

// emb 用 key 发一个嵌入请求。
func (h *harness) emb(t *testing.T, key, body string, headers ...string) reply {
	t.Helper()
	r := h.post(context.Background(), "/v1/embeddings", key, body, headers...)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

// flightCount 是合并的表里还有的 flight 数。
func (h *harness) flightCount() int {
	h.g.flights.mu.Lock()
	defer h.g.flights.mu.Unlock()
	return len(h.g.flights.m)
}

func (h *harness) waiting() int { return int(testutil.ToFloat64(h.metrics.CacheWaiting)) }

func (h *harness) embeddings() int64 { return h.fake.Stats().Embeddings }

const embBody = `{"model":"shared","input":["一二三"],"dimensions":4}`

// wantCache 核对一次嵌入请求的结果：状态码、Cache-Status，以及日志里的 cache 字段。
func (h *harness) wantCache(t *testing.T, r reply, status int, header, logged string) map[string]any {
	t.Helper()
	l := h.logLine(t, requestID(t, r))
	if r.status != status || r.header.Get("Cache-Status") != header || l["cache"] != nilIfEmpty(logged) {
		t.Fatalf("got %d, Cache-Status %q, log cache %v; want %d, %q, %q (body %s)", r.status, r.header.Get("Cache-Status"),
			l["cache"], status, header, logged, r.body)
	}
	return l
}

const (
	statusHit       = "quotaforge; hit"
	statusCollapsed = "quotaforge; fwd=miss; collapsed"
	statusMiss      = "quotaforge; fwd=miss"
)

// 命中：第二次不到上游、正文逐字节相同（正文里的 id 是第一次的）、x-request-id 是新号；不预留、不落发送意图，
// 记一条 CACHED（指向第一次、用量照抄）；日志有 cache、cached_from，call_status 和 outcome 都是 CACHED，没有用量和预留量。
func TestEmbeddingCacheHit(t *testing.T) {
	a := newAllowAll()
	mc := newMemCache()
	h := newCacheHarness(t, a, &stubLimiter{}, mc)

	first := h.emb(t, clientKey, embBody, "X-Client-Request-Id", "crid-2")
	l := h.wantCache(t, first, 200, statusMiss, cacheMiss)
	if l["call_status"] != "SETTLED" || l["cached_from"] != nil || h.embeddings() != 1 {
		t.Fatalf("first: %v, upstream got %d", l, h.embeddings())
	}
	if _, sets := mc.counts(); sets != 1 || mc.ttls[0] != cacheTTL {
		t.Fatalf("writes %d, ttl %v", sets, mc.ttls)
	}

	second := h.emb(t, clientKey, embBody, "X-Client-Request-Id", "crid-2", "X-Run-Id", "run-1")
	l = h.wantCache(t, second, 200, statusHit, cacheHit)
	firstID, secondID := requestID(t, first), requestID(t, second)
	if !bytes.Equal(second.body, first.body) || second.header.Get("Content-Type") != first.header.Get("Content-Type") ||
		secondID == firstID || h.embeddings() != 1 {
		t.Fatalf("hit: body equal %v, ids %s %s, upstream got %d", bytes.Equal(second.body, first.body), firstID, secondID, h.embeddings())
	}
	if reserved, cached := a.counts(); reserved != 1 || cached != 1 || a.sent != 1 {
		t.Fatalf("reserved %d, cached %d, sent %d; want 1, 1, 1", reserved, cached, a.sent)
	}
	want := ledger.CachedCall{CallID: secondID, AccountID: 1, KeyID: 1, Purpose: "embeddings", Alias: "shared",
		Upstream: "fake", UpstreamModel: "fake-embed", ClientRequestID: "crid-2", RunID: "run-1", CachedFrom: firstID,
		Usage: ledger.Usage{Prompt: 3, Total: 3}, ResponseID: "fakeemb-1", ResponseModel: "fake-embed" + fakeupstream.ModelVersionSuffix}
	if a.cached[0] != want {
		t.Fatalf("CACHED = %+v\nwant     %+v", a.cached[0], want)
	}
	for k, v := range map[string]any{"call_status": "CACHED", "outcome": "CACHED", "cached_from": firstID, "status": 200.0,
		"upstream_model": "fake-embed" + fakeupstream.ModelVersionSuffix, "upstream_response_id": "fakeemb-1"} {
		if l[k] != v {
			t.Errorf("log %s = %v, want %v", k, l[k], v)
		}
	}
	for _, k := range []string{"usage", "reserved_tokens", "upstream_status", "cache_error"} {
		if _, ok := l[k]; ok {
			t.Errorf("a hit logged %s: %v", k, l[k])
		}
	}
	m := h.metrics
	if testutil.ToFloat64(m.Calls.WithLabelValues("embeddings", "cached")) != 1 || testutil.ToFloat64(m.CacheLookups.WithLabelValues("hit")) != 1 ||
		testutil.ToFloat64(m.CacheLookups.WithLabelValues("miss")) != 1 || testutil.ToFloat64(m.CacheWrites.WithLabelValues("ok")) != 1 {
		t.Fatal("metrics of a miss and a hit")
	}
	if h.flightCount() != 0 {
		t.Fatalf("%d flights left", h.flightCount())
	}
}

// 用真的账本和真的 Redis：命中不动额度和账本行，CACHED 那一行查询接口查得到，Verify 干净；额度用完的账户照样命中。
func TestEmbeddingCacheHitLedger(t *testing.T) {
	cache, err := embcache.Open(dbtest.RedisURL(t), dbtest.RedisPrefix())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	e := newDBEnvWith(t, 1000, nil, func(l Ledger) *harness { return newCacheHarness(t, l, &stubLimiter{}, cache) })
	first := e.send(t, "/v1/embeddings", embBody)
	_, reserved, consumed := e.budget(t)
	if first.status != 200 || reserved != 0 || consumed != 3 {
		t.Fatalf("first: %d, reserved %d, consumed %d", first.status, reserved, consumed)
	}
	// 额度用完：没缓存过的请求 429，缓存过的照样命中
	if _, err := e.admin.Exec("UPDATE budgets b JOIN accounts a ON a.id = b.account_id SET b.limit_tokens = b.consumed_tokens WHERE a.name = ?", e.account); err != nil {
		t.Fatal(err)
	}
	if r := e.send(t, "/v1/embeddings", `{"model":"shared","input":["new"]}`); r.status != 429 || errorCode(t, r) != "quota_exhausted" {
		t.Fatalf("an uncached request with no quota left: %d %s", r.status, r.body)
	}
	second := e.send(t, "/v1/embeddings", embBody)
	if second.status != 200 || second.header.Get("Cache-Status") != statusHit || !bytes.Equal(second.body, first.body) {
		t.Fatalf("hit: %d %q", second.status, second.header.Get("Cache-Status"))
	}
	if _, r2, c2 := e.budget(t); r2 != reserved || c2 != consumed {
		t.Fatalf("budget moved: %d/%d -> %d/%d", reserved, consumed, r2, c2)
	}
	id := requestID(t, second)
	c := e.expect(t, id, "CACHED", nil, audit{resolvedBy: "gateway", evidence: "cache"})
	if c.reserved != 0 || c.httpStatus.Int64 != 200 || c.usageTotal.Int64 != 3 || c.usageCompletion.Valid {
		t.Fatalf("CACHED row = %+v", c)
	}
	var info struct {
		Status     string
		Resolution struct{ By, Evidence string }
		Usage      struct {
			Total int64 `json:"total_tokens"`
		}
	}
	r := e.get("/v1/calls/"+id, e.key)
	if err := json.Unmarshal(r.body, &info); err != nil || r.status != 200 || info.Status != "CACHED" ||
		info.Resolution.By != "gateway" || info.Resolution.Evidence != "cache" || info.Usage.Total != 3 {
		t.Fatalf("lookup: %d %s", r.status, r.body)
	}
	dbtest.Verify(t, e.store)
}

// 命中照样过准入：三层的 RPM 都扣；修正时 TPM 全部退回（refund 的“其余”那一支，和重放一样），
// 上游那一层的 RPM 退 1（命中不发给上游），Key、账户两层的 RPM 不退。没命中的那次 RPM 一个都不退。
func TestEmbeddingCacheHitRateLimits(t *testing.T) {
	lim := &stubLimiter{}
	h := newCacheHarness(t, &rated{Ledger: newAllowAll(), key: bothRates, account: ledger.Rate{RPM: 50}}, lim, newMemCache())
	h.limitUpstream("fake", 30, 0)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit) // 日志写完时修正已经做完
	admits, adjusts := lim.calls()
	if len(admits) != 2 || len(adjusts) != 2 {
		t.Fatalf("%d admits, %d adjusts", len(admits), len(adjusts))
	}
	var rpm, tpm int64
	for _, b := range admits[1] {
		switch b.Metric {
		case "rpm":
			rpm += b.Cost
		case "tpm":
			tpm = b.Cost
		}
	}
	if rpm != 3 || tpm <= 0 {
		t.Fatalf("hit admitted %v, want an RPM token from each of the three layers", admits[1])
	}
	for _, b := range adjusts[0] {
		if b.Metric == "rpm" {
			t.Fatalf("miss adjusted %v: no RPM comes back for a call that went upstream", adjusts[0])
		}
	}
	want := []ratelimit.Bucket{
		{Layer: "key", ID: "1", Metric: "tpm", Limit: bothRates.TPM, Cost: -tpm},
		{Layer: "upstream", ID: "fake", Metric: "rpm", Limit: 30, Cost: -1},
	}
	if !slices.Equal(adjusts[1], want) {
		t.Fatalf("hit adjusted %v, want the whole estimate and the upstream request back %v", adjusts[1], want)
	}
}

// 上游那一层每分钟只有 2 个请求：一次回源、三次命中之后，另一个要回源的请求照样放行（命中不占上游的 RPM），
// 再下一个才在上游那一层被拒（被拒的一个桶都不扣）。Key 那一层放行的五个请求都算。
func TestEmbeddingCacheHitFreesUpstreamRPM(t *testing.T) {
	lim, rdb := redisLimiter(t)
	h := newCacheHarness(t, &rated{Ledger: newAllowAll(), key: ledger.Rate{RPM: 100}}, lim, newMemCache())
	h.limitUpstream("fake", 2, 0)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
	for range 3 {
		h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit)
	}
	other := strings.Replace(embBody, "一二三", "四五六", 1)
	h.wantCache(t, h.emb(t, clientKey, other), 200, statusMiss, cacheMiss)
	third := strings.Replace(embBody, "一二三", "七八九", 1)
	r := h.emb(t, clientKey, third)
	if l := h.logLine(t, requestID(t, r)); r.status != 429 || l["rate_limited_by"] != "upstream/rpm" {
		t.Fatalf("third miss: %d, rate_limited_by %v; want 429 upstream/rpm", r.status, l["rate_limited_by"])
	}
	tokens := func(layer, id string) float64 {
		v, err := rdb.HGet(context.Background(), lim.Key(ratelimit.Bucket{Layer: layer, ID: id, Metric: "rpm"}), "tokens").Float64()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := tokens("key", "1"); v < 95 || v >= 95.5 {
		t.Fatalf("key/rpm holds %v tokens, want about 95: the five admitted requests count, hits included", v)
	}
	if v := tokens("upstream", "fake"); v < 0 || v >= 0.5 {
		t.Fatalf("upstream/rpm holds %v tokens, want about 0: only the two misses count", v)
	}
}

// 合并的跟随者结局也是 CACHED，同样退回上游那一层的 RPM：10 个相同请求并发，1 个领头的回源，
// 9 个跟随的拿它的结果。上游那一层只留领头的 1 个；Key 那一层 10 个都算。
func TestEmbeddingCacheCollapsedFollowersFreeUpstreamRPM(t *testing.T) {
	lim, rdb := redisLimiter(t)
	h := newCacheHarness(t, &rated{Ledger: newAllowAll(), key: ledger.Rate{RPM: 1000}}, lim, newMemCache())
	h.limitUpstream("fake", 50, 0)
	const n = 10
	replies := h.embAsync(h.clientCtx, clientKey, `{"model":"emb-hold","input":["collapsed"]}`, n)
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 && h.waiting() == n-1 })
	h.fake.Release()
	groups := byCacheStatus(collect(t, replies, n, 5*time.Second))
	if len(groups[statusMiss]) != 1 || len(groups[statusCollapsed]) != n-1 {
		t.Fatalf("%d miss, %d collapsed; want 1, %d", len(groups[statusMiss]), len(groups[statusCollapsed]), n-1)
	}
	for _, r := range groups[statusCollapsed] {
		if l := h.logLine(t, requestID(t, r)); l["call_status"] != "CACHED" { // 日志在修正之后写
			t.Fatalf("follower log %v", l)
		}
	}
	tokens := func(layer, id string) float64 {
		v, err := rdb.HGet(context.Background(), lim.Key(ratelimit.Bucket{Layer: layer, ID: id, Metric: "rpm"}), "tokens").Float64()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := tokens("upstream", "fake"); v < 49 || v >= 49.5 {
		t.Fatalf("upstream/rpm holds %v tokens, want about 49: only the leader counts, every follower gets its token back", v)
	}
	if v := tokens("key", "1"); v < 990 || v >= 990.5 {
		t.Fatalf("key/rpm holds %v tokens, want about 990: all ten requests count", v)
	}
}

// 隔离：别的账户、同一上游的别的上游模型名、别的上游、请求体不同都不命中；别名不同但目标相同、
// 只差空白和键的顺序的请求体会命中。键是“账户 id:64 位十六进制”，没有明文。
func TestEmbeddingCacheIsolation(t *testing.T) {
	mc := newMemCache()
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
	cases := []struct {
		name, key, body, header, logged string
	}{
		{"another account", limitedKey, embBody, statusMiss, cacheMiss},
		{"another upstream model", clientKey, `{"model":"emb-model2","input":["一二三"],"dimensions":4}`, statusMiss, cacheMiss},
		{"another upstream", clientKey, `{"model":"emb-b","input":["一二三"],"dimensions":4}`, statusMiss, cacheMiss},
		{"another input", clientKey, `{"model":"shared","input":["一二四"],"dimensions":4}`, statusMiss, cacheMiss},
		{"another alias of the same target", clientKey, `{"model":"emb-alias2","input":["一二三"],"dimensions":4}`, statusHit, cacheHit},
		{"the same body canonically", clientKey, "{ \"dimensions\" : 4,\n\"input\":[\"\\u4e00二三\"], \"model\":\"shared\"}", statusHit, cacheHit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h.wantCache(t, h.emb(t, tc.key, tc.body), 200, tc.header, tc.logged)
		})
	}
	key := regexp.MustCompile(`^[12]:[0-9a-f]{64}$`)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for k := range mc.m {
		if !key.MatchString(k) {
			t.Errorf("cache key %q", k)
		}
	}
}

// 不缓存的：带幂等键的不读也不写（bypass、没有 Cache-Status）；聊天不查；上游错误、上游 4xx、用量未知、正文超过 1 MiB、
// 收尾失败的都不写；规范化不了的请求体不走缓存，照常转发。正好 1 MiB 的照常写。
func TestEmbeddingCacheSkips(t *testing.T) {
	cases := []struct {
		name       string
		ledger     func() Ledger
		path, body string
		headers    []string
		status     int
		header     string
		logged     string
		gets, sets int // 两次请求之后
	}{
		{"idempotency key", nil, "/v1/embeddings", embBody, []string{"Idempotency-Key", "k-1"}, 200, "", cacheBypass, 0, 0},
		{"chat", nil, "/v1/chat/completions", `{"model":"shared"}`, nil, 200, "", "", 0, 0},
		{"upstream error", nil, "/v1/embeddings", `{"model":"emb-500","input":"x"}`, nil, 502, statusMiss, cacheMiss, 2, 0},
		{"upstream 4xx", nil, "/v1/embeddings", `{"model":"emb-400","input":"x"}`, nil, 400, statusMiss, cacheMiss, 2, 0},
		{"usage unknown", nil, "/v1/embeddings", `{"model":"emb-no-usage","input":"x"}`, nil, 200, statusMiss, cacheMiss, 2, 0},
		{"over 1 MiB", nil, "/v1/embeddings", `{"model":"size-1048577"}`, nil, 200, statusMiss, cacheMiss, 2, 0},
		{"exactly 1 MiB", nil, "/v1/embeddings", `{"model":"size-1048576"}`, nil, 200, statusMiss, cacheMiss, 2, 1},
		{"a 201 with usage", nil, "/v1/embeddings", `{"model":"emb-201","input":"x"}`, nil, 201, statusMiss, cacheMiss, 2, 0},
		{"finish failed", func() Ledger { return &faulty{Ledger: newAllowAll(), finishErr: errors.New("db down")} },
			"/v1/embeddings", embBody, nil, 200, statusMiss, cacheMiss, 2, 0},
		{"not canonical", nil, "/v1/embeddings", `{"model":"shared","input":"\ud800"}`, nil, 200, "", cacheBypass, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var l Ledger = newAllowAll()
			if tc.ledger != nil {
				l = tc.ledger()
			}
			mc := newMemCache()
			h := newCacheHarness(t, l, &stubLimiter{}, mc)
			for range 2 {
				r := h.post(context.Background(), tc.path, clientKey, tc.body, tc.headers...)
				if r.err != nil {
					t.Fatal(r.err)
				}
				h.wantCache(t, r, tc.status, tc.header, tc.logged)
				if tc.sets == 1 { // 写进去了：第二次就命中
					tc.status, tc.header, tc.logged = 200, statusHit, cacheHit
				}
			}
			if gets, sets := mc.counts(); gets != tc.gets || sets != tc.sets {
				t.Fatalf("cache gets %d, sets %d", gets, sets)
			}
		})
	}
}

// 换上游：主目标拨号失败、换到 fake-b 回答的结果写在 fake-b 的键下（同一个请求体发给 fake-b 的别名会命中），
// 不共享给等着主目标那个键的跟随的；熔断打开之后直接选 fake-b 的请求命中。
func TestEmbeddingCacheFailover(t *testing.T) {
	mc := newMemCache()
	g := newGate()
	mc.getHook = g.hook
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	t.Cleanup(g.open)
	body := `{"model":"fo-emb-dead","input":["fail over"]}`
	replies := h.embAsync(h.clientCtx, clientKey, body, 2)
	g.wait(t)
	waitFor(t, func() bool { return h.waiting() == 1 })
	g.open()
	for _, r := range collect(t, replies, 2, 5*time.Second) {
		if r.status != 200 || r.header.Get("Cache-Status") != statusMiss {
			t.Fatalf("got %d %q: the follower must not share a result from another target", r.status, r.header.Get("Cache-Status"))
		}
	}
	if n := h.fakeB.Stats().Embeddings; n != 2 {
		t.Fatalf("fake-b got %d, want 2", n)
	}
	h.wantCache(t, h.emb(t, clientKey, `{"model":"emb-b","input":["fail over"]}`), 200, statusHit, cacheHit)

	h.breakAfter(1)
	h.emb(t, clientKey, body) // dead 再失败一次，熔断打开
	if h.state("dead") != open {
		t.Fatalf("dead is %v", h.state("dead"))
	}
	h.wantCache(t, h.emb(t, clientKey, body), 200, statusHit, cacheHit)
	if n := h.fakeB.Stats().Embeddings; n != 3 {
		t.Fatalf("fake-b got %d, want 3", n)
	}
}

// 冷却到了、拿到探测名额的请求命中缓存：没发给上游，名额按“不算”交回，熔断回到开着；下一个没命中的请求照常探测。
func TestEmbeddingCacheHitReturnsTheProbe(t *testing.T) {
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, newMemCache())
	h.breakAfter(1)
	clk := h.useClock()
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
	h.chat(t, `{"model":"fake-500"}`) // fake 的熔断打开
	clk.advance(31 * time.Second)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit)
	if h.state("fake") != open || h.embeddings() != 1 {
		t.Fatalf("after a hit with the probe: %v, upstream got %d", h.state("fake"), h.embeddings())
	}
	h.wantCache(t, h.emb(t, clientKey, `{"model":"shared","input":"probe"}`), 200, statusMiss, cacheMiss)
	if h.state("fake") != closed {
		t.Fatalf("after the probe: %v", h.state("fake"))
	}
}

// span：没命中的有 cache_lookup、cache_store，命中的有 cache_lookup、record_cached；根 span 有 qf.cache、qf.cached_from。
// 用量导出：命中的行用量三列为空、cached_from 是原来的调用号。
func TestEmbeddingCacheObservability(t *testing.T) {
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, newMemCache())
	miss := requestID(t, h.emb(t, clientKey, embBody))
	hit := requestID(t, h.emb(t, clientKey, embBody))
	root, children := h.rootSpan(t, miss)
	if attr(root, "qf.cache") != cacheMiss || attr(root, "qf.cached_from") != "" ||
		!slices.Equal(names(children), []string{"authenticate", "cache_lookup", "reserve", "mark_sent", "upstream", "finish", "cache_store"}) {
		t.Fatalf("miss: %v %v", root.Attributes(), names(children))
	}
	root, children = h.rootSpan(t, hit)
	if attr(root, "qf.cache") != cacheHit || attr(root, "qf.cached_from") != miss || attr(root, "qf.call_status") != "CACHED" ||
		attr(root, "qf.outcome") != "CACHED" || attr(root, "qf.reserved_tokens") != "" || attr(root, "qf.usage.known") != "" ||
		!slices.Equal(names(children), []string{"authenticate", "cache_lookup", "record_cached"}) {
		t.Fatalf("hit: %v %v", root.Attributes(), names(children))
	}

	var out bytes.Buffer
	if _, err := ExportUsage(strings.NewReader(h.logs.String()), &out); err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(&out).ReadAll()
	if err != nil || len(records) != 3 {
		t.Fatalf("%d records, %v", len(records), err)
	}
	row := map[string]string{}
	for i, col := range usageColumns {
		row[col] = records[2][i]
	}
	if row["request_id"] != hit || row["status"] != "200" || row["cached_from"] != miss || row["replay_of"] != "" ||
		row["prompt_tokens"]+row["completion_tokens"]+row["total_tokens"] != "" {
		t.Fatalf("hit row %v", row)
	}
	if records[1][len(usageColumns)-1] != "" {
		t.Fatalf("miss row %v has cached_from", records[1])
	}
}
