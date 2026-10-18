package gateway

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// gate 挡住第 at 次调用（从 0 数，默认第一次）：进来时关上 entered，等 open 之后才往下走（then 不为 nil 时再执行它，
// 例如 panic）。别的调用直接过去。
type gate struct {
	at              int64
	calls           atomic.Int64
	openOnce        sync.Once
	entered, opened chan struct{}
	then            func()
}

func newGate() *gate { return &gate{entered: make(chan struct{}), opened: make(chan struct{})} }

func (g *gate) hook() {
	if g.calls.Add(1)-1 != g.at {
		return
	}
	close(g.entered)
	<-g.opened
	if g.then != nil {
		g.then()
	}
}

// open 放开第一次调用；可以调多次。测试在起好 harness 之后用 t.Cleanup(g.open) 兜底：中途失败时挡住的请求也会被放开。
func (g *gate) open() { g.openOnce.Do(func() { close(g.opened) }) }

// wait 等第一次调用进来，截止时间只用来判失败。
func (g *gate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("nothing reached the gate within 3s")
	}
}

// embAsync 并发发 n 个相同的嵌入请求，结果按到达的先后放进返回的 channel。
func (h *harness) embAsync(ctx context.Context, key, body string, n int) <-chan reply {
	out := make(chan reply, n)
	for range n {
		go func() { out <- h.post(ctx, "/v1/embeddings", key, body) }()
	}
	return out
}

// collect 收 n 个结果，截止时间只用来判失败。
func collect(t *testing.T, replies <-chan reply, n int, within time.Duration) []reply {
	t.Helper()
	var out []reply
	deadline := time.After(within)
	for len(out) < n {
		select {
		case r := <-replies:
			if r.err != nil {
				t.Fatal(r.err)
			}
			out = append(out, r)
		case <-deadline:
			t.Fatalf("got %d of %d replies within %v", len(out), n, within)
		}
	}
	return out
}

// byCacheStatus 按 Cache-Status 分组。
func byCacheStatus(rs []reply) map[string][]reply {
	out := map[string][]reply{}
	for _, r := range rs {
		s := r.header.Get("Cache-Status")
		out[s] = append(out[s], r)
	}
	return out
}

// 并发的 N 个相同请求：等到 1 个挂在上游、N−1 个在等（qf_embedding_cache_waiting），再放开。上游只收到 1 个；
// 其余的拿到领头的结果（collapsed），各记一条指向它的 CACHED，正文相同。结束之后合并的表是空的。
func TestEmbeddingCacheCoalesces(t *testing.T) {
	a := newAllowAll()
	mc := newMemCache()
	h := newCacheHarness(t, a, &stubLimiter{}, mc)
	const n = 10
	replies := h.embAsync(h.clientCtx, clientKey, `{"model":"emb-hold","input":["held"]}`, n)
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 && h.waiting() == n-1 })
	h.fake.Release()
	groups := byCacheStatus(collect(t, replies, n, 5*time.Second))
	leaders, followers := groups[statusMiss], groups[statusCollapsed]
	if len(leaders) != 1 || len(followers) != n-1 || h.embeddings() != 1 {
		t.Fatalf("%d leaders, %d collapsed, upstream got %d; want 1, %d, 1", len(leaders), len(followers), h.embeddings(), n-1)
	}
	leader := requestID(t, leaders[0])
	for _, r := range followers {
		if r.status != 200 || !bytes.Equal(r.body, leaders[0].body) {
			t.Fatalf("follower: %d %s", r.status, r.body)
		}
		if l := h.logLine(t, requestID(t, r)); l["cache"] != cacheCoalesced || l["cached_from"] != leader || l["call_status"] != "CACHED" {
			t.Fatalf("follower log %v", l)
		}
	}
	reserved, cached := a.counts()
	if reserved != 1 || cached != n-1 || slices.ContainsFunc(a.cached, func(c ledger.CachedCall) bool { return c.CachedFrom != leader }) {
		t.Fatalf("reserved %d, cached %d %+v", reserved, cached, a.cached)
	}
	if gets, sets := mc.counts(); gets != 1 || sets != 1 {
		t.Fatalf("cache gets %d, sets %d; want 1, 1", gets, sets)
	}
	if h.flightCount() != 0 || h.waiting() != 0 {
		t.Fatalf("%d flights, %d waiting left", h.flightCount(), h.waiting())
	}
	if got := testutil.ToFloat64(h.metrics.CacheLookups.WithLabelValues(cacheCoalesced)); got != n-1 {
		t.Fatalf("coalesced lookups %v", got)
	}
}

// 领头的没给出结果（上游出错、收尾失败）：跟随的不共享它的失败，各自照常回源。
func TestEmbeddingCacheLeaderFails(t *testing.T) {
	cases := []struct {
		name   string
		ledger Ledger
		body   string
		status int
	}{
		{"upstream error", newAllowAll(), `{"model":"emb-500","input":"x"}`, 502},
		{"finish failed", &faulty{Ledger: newAllowAll(), finishErr: errors.New("db down")}, embBody, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mc := newMemCache()
			g := newGate()
			mc.getHook = g.hook
			h := newCacheHarness(t, tc.ledger, &stubLimiter{}, mc)
			t.Cleanup(g.open)
			const n = 4
			replies := h.embAsync(h.clientCtx, clientKey, tc.body, n)
			g.wait(t)
			waitFor(t, func() bool { return h.waiting() == n-1 })
			g.open()
			for _, r := range collect(t, replies, n, 5*time.Second) {
				if r.status != tc.status || r.header.Get("Cache-Status") != statusMiss {
					t.Fatalf("got %d %q, want %d and a miss", r.status, r.header.Get("Cache-Status"), tc.status)
				}
			}
			if h.embeddings() != n {
				t.Fatalf("upstream got %d, want %d: every follower went on its own", h.embeddings(), n)
			}
			if gets, sets := mc.counts(); gets != 1 || sets != 0 {
				t.Fatalf("cache gets %d, sets %d", gets, sets)
			}
			if h.flightCount() != 0 {
				t.Fatal("a flight was left")
			}
		})
	}
}

// 领头的上游超时（504、用量未知）：跟随的各自回源，最坏的延迟就是等领头的超时、再加自己的一次。不靠睡眠：
// 用挂起的场景，领头的超时之后跟随的到了上游，再放开它们。
func TestEmbeddingCacheLeaderTimesOut(t *testing.T) {
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, newMemCache())
	const n = 3
	replies := h.embAsync(h.clientCtx, clientKey, `{"model":"emb-hold","input":["slow"]}`, n)
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 && h.waiting() == n-1 })
	first := collect(t, replies, 1, embeddingsTimeout+3*time.Second)[0]
	if first.status != http.StatusGatewayTimeout || errorCode(t, first) != "upstream_timeout" || first.header.Get("Cache-Status") != statusMiss {
		t.Fatalf("leader: %d %s", first.status, first.body)
	}
	// 跟随的随后各自到上游、被挂住；放开（可能要放几次：跟随的到的先后不定），截止时间只用来判失败。
	var rest []reply
	deadline := time.Now().Add(3 * time.Second)
	for len(rest) < n-1 && time.Now().Before(deadline) {
		h.fake.Release()
		select {
		case r := <-replies:
			rest = append(rest, r)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(rest) != n-1 {
		t.Fatalf("%d followers answered", len(rest))
	}
	for _, r := range rest {
		if r.err != nil || r.status != 200 || r.header.Get("Cache-Status") != statusMiss {
			t.Fatalf("follower: %v %d %q", r.err, r.status, r.header.Get("Cache-Status"))
		}
	}
	if h.embeddings() != n {
		t.Fatalf("upstream got %d, want %d", h.embeddings(), n)
	}
}

// 跟随的在等的时候客户端断开：不再等，不预留、不留 CACHED，日志是 client_canceled；领头的照常完成。
func TestEmbeddingCacheFollowerCanceled(t *testing.T) {
	a := newAllowAll()
	mc := newMemCache()
	g := newGate()
	mc.getHook = g.hook
	h := newCacheHarness(t, a, &stubLimiter{}, mc)
	t.Cleanup(g.open)
	leader := h.embAsync(h.clientCtx, clientKey, embBody, 1)
	g.wait(t)
	ctx, cancel := context.WithCancel(h.clientCtx)
	follower := h.embAsync(ctx, clientKey, embBody, 1)
	waitFor(t, func() bool { return h.waiting() == 1 })
	cancel()
	if r := <-follower; r.err == nil {
		t.Fatalf("the canceled follower got %d", r.status)
	}
	waitFor(t, func() bool { return h.waiting() == 0 })
	var line map[string]any
	waitFor(t, func() bool {
		for _, l := range h.logs.lines(t) {
			if l["error_code"] == "client_canceled" {
				line = l
			}
		}
		return line != nil
	})
	if _, ok := line["call_status"]; ok || line["cache"] != cacheMiss {
		t.Fatalf("follower log %v", line)
	}
	if reserved, cached := a.counts(); reserved != 0 || cached != 0 {
		t.Fatalf("reserved %d, cached %d before the leader went on", reserved, cached)
	}
	g.open()
	if r := collect(t, leader, 1, 3*time.Second)[0]; r.status != 200 {
		t.Fatalf("leader: %d", r.status)
	}
	if reserved, cached := a.counts(); reserved != 1 || cached != 0 || h.flightCount() != 0 {
		t.Fatalf("reserved %d, cached %d, flights %d", reserved, cached, h.flightCount())
	}
}

// 领头的 panic：跟随的照样被放开（自己回源），键从合并的表里删掉，之后的请求正常。
func TestEmbeddingCacheLeaderPanics(t *testing.T) {
	mc := newMemCache()
	g := newGate()
	g.then = func() { panic("cache client bug") }
	mc.getHook = g.hook
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	t.Cleanup(g.open)
	leader := h.embAsync(h.clientCtx, clientKey, embBody, 1)
	g.wait(t)
	follower := h.embAsync(h.clientCtx, clientKey, embBody, 1)
	waitFor(t, func() bool { return h.waiting() == 1 })
	g.open()
	if r := <-leader; r.err == nil {
		t.Fatalf("the panicking leader answered %d", r.status)
	}
	if r := collect(t, follower, 1, 3*time.Second)[0]; r.status != 200 || r.header.Get("Cache-Status") != statusMiss {
		t.Fatalf("follower: %d %q", r.status, r.header.Get("Cache-Status"))
	}
	if h.flightCount() != 0 {
		t.Fatal("the panicking leader left its flight")
	}
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit) // 跟随的结算后写进了缓存
}

// 领头的写缓存之前不删键：它还在写的时候来的相同请求拿到它的结果（collapsed），不再回源。
func TestEmbeddingCacheFlightOutlivesTheWrite(t *testing.T) {
	mc := newMemCache()
	g := newGate()
	mc.setHook = g.hook
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	t.Cleanup(g.open)
	leader := h.embAsync(h.clientCtx, clientKey, embBody, 1)
	g.wait(t) // 领头的已经结算、写回，正在写缓存
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusCollapsed, cacheCoalesced)
	g.open()
	if r := collect(t, leader, 1, 3*time.Second)[0]; r.status != 200 || h.embeddings() != 1 {
		t.Fatalf("leader %d, upstream got %d; want 200, 1", r.status, h.embeddings())
	}
}

// 缓存坏了只算没命中：读出错、条目坏了都照常回源（200，不是 503），记日志和缓存自己的指标，这次不再写；
// 写出错照样 200、记日志和指标。缓存的错误不计进 qf_dependency_errors_total。
func TestEmbeddingCacheFailsOpen(t *testing.T) {
	t.Run("read error", func(t *testing.T) {
		mc := newMemCache()
		mc.getErr = errors.New("cache-host:6379: connection refused")
		h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
		l := h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheError)
		if l["cache_error"] != mc.getErr.Error() || l["call_status"] != "SETTLED" {
			t.Fatalf("log %v", l)
		}
		if _, sets := mc.counts(); sets != 0 {
			t.Fatal("wrote after a failed read")
		}
		if testutil.ToFloat64(h.metrics.CacheLookups.WithLabelValues(cacheError)) != 1 || testutil.CollectAndCount(h.metrics.DependencyErrors) != 0 {
			t.Fatal("metrics of a read error")
		}
	})
	t.Run("write error", func(t *testing.T) {
		mc := newMemCache()
		mc.setErr = errors.New("cache-host:6379: i/o timeout")
		h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
		l := h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
		if l["cache_error"] != mc.setErr.Error() {
			t.Fatalf("log %v", l)
		}
		h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
		if testutil.ToFloat64(h.metrics.CacheWrites.WithLabelValues("error")) != 2 || testutil.CollectAndCount(h.metrics.DependencyErrors) != 0 {
			t.Fatal("metrics of a write error")
		}
	})
	for name, entry := range map[string]string{
		"garbage":       "not an entry",
		"no usage":      `{"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA","content_type":["application/json"]}` + "\n{}",
		"bad usage":     `{"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA","usage":{"Prompt":-1,"Total":1}}` + "\n{}",
		"bad call id":   `{"call_id":"x","usage":{"Prompt":1,"Total":1}}` + "\n{}",
		"no line break": `{"call_id":"qf-AAAAAAAAAAAAAAAAAAAAAAAAAA","usage":{"Prompt":1,"Total":1}}`,
	} {
		t.Run("malformed entry: "+name, func(t *testing.T) {
			mc := newMemCache()
			h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
			h.emb(t, clientKey, embBody)
			mc.corrupt(entry)
			l := h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheError)
			// Redis 是好的，坏条目被这次结算的新条目覆盖：再发一次命中，上游只多了坏条目那一次
			h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit)
			if l["cache_error"] != errBadEntry.Error() || h.embeddings() != 2 {
				t.Fatalf("log %v, upstream got %d", l, h.embeddings())
			}
		})
	}
}

// 写 CACHED 失败：503 ledger_unavailable，不给缓存里的正文（QF 返回的调用号都要查得到），也不带 Cache-Status（不是来自缓存的响应）。
func TestEmbeddingCacheRecordFails(t *testing.T) {
	h := newCacheHarness(t, &faulty{Ledger: newAllowAll(), cachedErr: errors.New("db down")}, &stubLimiter{}, newMemCache())
	first := h.emb(t, clientKey, embBody)
	r := h.emb(t, clientKey, embBody)
	l := h.logLine(t, requestID(t, r))
	if r.status != 503 || errorCode(t, r) != "ledger_unavailable" || bytes.Equal(r.body, first.body) ||
		r.header.Get("Cache-Status") != "" || l["ledger_error"] != "db down" || l["cache"] != cacheHit {
		t.Fatalf("got %d %s, log %v", r.status, r.body, l)
	}
	if _, ok := l["call_status"]; ok {
		t.Fatalf("a failed CACHED logged call_status: %v", l)
	}
}

// 过期时长：配置的时长乘以 0.9 到 1.1 之间的随机数。注入的随机数是 0、0.5、接近 1；默认的随机源都落在范围里。
func TestEmbeddingCacheTTLJitter(t *testing.T) {
	mc := newMemCache()
	draws := []float64{0, 0.5, 0.999999}
	var mu sync.Mutex
	h := newHarnessSetup(t, newAllowAll(), &stubLimiter{}, tracedAt(1), chatTimeout, func(g *gateway) {
		withCache(mc)(g)
		g.jitter = func() float64 {
			mu.Lock()
			defer mu.Unlock()
			d := draws[0]
			draws = draws[1:]
			return d
		}
	})
	for i := range 3 {
		h.emb(t, clientKey, `{"model":"shared","input":"`+strings.Repeat("x", i+1)+`"}`)
	}
	want := []time.Duration{54 * time.Minute, 60 * time.Minute, 66 * time.Minute} // 最后一个比 66 分钟少不到 1 毫秒
	for i, ttl := range mc.ttls {
		if d := want[i] - ttl; d < 0 || d > time.Millisecond {
			t.Errorf("ttl %d = %v, want %v", i, ttl, want[i])
		}
	}
	if len(mc.ttls) != 3 {
		t.Fatalf("%d writes", len(mc.ttls))
	}

	mc2 := newMemCache()
	h2 := newHarnessSetup(t, newAllowAll(), &stubLimiter{}, tracedAt(1), chatTimeout, func(g *gateway) {
		g.cache, g.cacheTTL = mc2, cacheTTL // 默认的随机源
	})
	for i := range 50 {
		h2.emb(t, clientKey, `{"model":"shared","input":"`+strings.Repeat("y", i+1)+`"}`)
	}
	distinct := map[time.Duration]bool{}
	for _, ttl := range mc2.ttls {
		if ttl < cacheTTL*9/10 || ttl >= cacheTTL*11/10 {
			t.Errorf("ttl %v outside [0.9, 1.1) of %v", ttl, cacheTTL)
		}
		distinct[ttl] = true
	}
	if len(mc2.ttls) != 50 || len(distinct) < 40 {
		t.Fatalf("%d writes, %d distinct ttls: not jittered", len(mc2.ttls), len(distinct))
	}
}

// stuckWriter 是一个不读响应的客户端：写正文时挡住，直到测试放开。直接调处理函数用它，不靠内核缓冲的大小。
type stuckWriter struct {
	header        http.Header
	writing       chan struct{} // 开始写正文时关上
	release       chan struct{}
	once, relOnce sync.Once
}

func (w *stuckWriter) Header() http.Header { return w.header }
func (w *stuckWriter) WriteHeader(int)     {}
func (w *stuckWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.writing) })
	<-w.release
	return len(b), nil
}

// stuckLeader 直接调 QF 的处理函数发一个嵌入请求，客户端是 stuckWriter。测试结束时放开它、等处理函数返回。
func (h *harness) stuckLeader(t *testing.T, body string) *stuckWriter {
	t.Helper()
	w := &stuckWriter{header: http.Header{}, writing: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+clientKey)
	req.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.qf.Config.Handler.ServeHTTP(w, req)
	}()
	t.Cleanup(func() {
		w.relOnce.Do(func() { close(w.release) })
		select {
		case <-done:
		case <-time.After(embeddingsTimeout + 3*time.Second):
			t.Error("the stuck leader's handler did not return")
		}
	})
	return w
}

// waitWriting 等领头的开始写回（卡在那里），截止时间只用来判失败。
func (w *stuckWriter) waitWriting(t *testing.T) {
	t.Helper()
	select {
	case <-w.writing:
	case <-time.After(5 * time.Second):
		t.Fatal("the leader never started to write its response")
	}
}

// 领头的客户端不读、领头的卡在写回时，跟随的不等它：合并只包住读缓存、回源、收尾、写缓存，领头的在写回之前就离开合并的表。
func TestEmbeddingCacheStuckLeader(t *testing.T) {
	// 不能缓存的结果（超过 1 MiB 的 200）：已经在等的跟随的、之后来的相同请求，都在几秒内自己回源、拿到 200。
	t.Run("uncacheable", func(t *testing.T) {
		mc := newMemCache()
		g := newGate()
		mc.getHook = g.hook
		h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
		t.Cleanup(g.open)
		const body = `{"model":"size-1048577"}`
		leader := h.stuckLeader(t, body)
		g.wait(t)
		follower := h.embAsync(h.clientCtx, clientKey, body, 1)
		waitFor(t, func() bool { return h.waiting() == 1 })
		g.open()
		leader.waitWriting(t)
		if r := collect(t, follower, 1, 5*time.Second)[0]; r.status != 200 || r.header.Get("Cache-Status") != statusMiss {
			t.Fatalf("follower: %d %q", r.status, r.header.Get("Cache-Status"))
		}
		if r := collect(t, h.embAsync(h.clientCtx, clientKey, body, 1), 1, 5*time.Second)[0]; r.status != 200 || r.header.Get("Cache-Status") != statusMiss {
			t.Fatalf("a later request: %d %q", r.status, r.header.Get("Cache-Status"))
		}
		if h.flightCount() != 0 {
			t.Fatalf("%d flights left while the leader is stuck", h.flightCount())
		}
	})
	// 能缓存的：领头的卡在写回时，缓存里已经有这一条、合并的表是空的，之后来的相同请求是 hit。
	t.Run("cacheable", func(t *testing.T) {
		mc := newMemCache()
		h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
		h.stuckLeader(t, embBody).waitWriting(t)
		if _, sets := mc.counts(); sets != 1 || h.flightCount() != 0 {
			t.Fatalf("while the leader is stuck: cache writes %d, flights %d; want 1, 0", sets, h.flightCount())
		}
		h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit)
	})
	// 命中的：领头的读到条目、交给跟随的之后就离开；它卡在写回时，合并的表是空的，之后来的相同请求是 hit。
	t.Run("hit", func(t *testing.T) {
		h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, newMemCache())
		h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
		h.stuckLeader(t, embBody).waitWriting(t)
		if h.flightCount() != 0 {
			t.Fatalf("%d flights left while the hitting leader is stuck", h.flightCount())
		}
		h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusHit, cacheHit)
	})
}

// 查完缓存、预留出错（429）的领头的：在写错误之前就离开合并的表，它的客户端不读时，之后来的相同请求不等它。
func TestEmbeddingCacheFailedLeaderLeavesBeforeWriting(t *testing.T) {
	h := newCacheHarness(t, &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrQuotaExhausted}, &stubLimiter{}, newMemCache())
	h.stuckLeader(t, embBody).waitWriting(t)
	if h.flightCount() != 0 {
		t.Fatalf("%d flights left while the failed leader is stuck", h.flightCount())
	}
	if r := collect(t, h.embAsync(h.clientCtx, clientKey, embBody, 1), 1, 3*time.Second)[0]; r.status != 429 || r.header.Get("Cache-Status") != statusMiss {
		t.Fatalf("a later request: %d %q", r.status, r.header.Get("Cache-Status"))
	}
}

// 写缓存不随客户端取消：请求发出之后客户端断开，领头的照样结算（有界结算）、照样写缓存，下一个相同请求命中。
func TestEmbeddingCacheWriteOutlivesTheClient(t *testing.T) {
	mc := newMemCache()
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	const body = `{"model":"emb-hold","input":["gone"]}`
	ctx, cancel := context.WithCancel(h.clientCtx)
	r := h.embAsync(ctx, clientKey, body, 1)
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 })
	cancel()
	if got := <-r; got.err == nil {
		t.Fatalf("the canceled client got %d", got.status)
	}
	h.fake.Release()
	waitFor(t, func() bool { _, sets := mc.counts(); return sets == 1 })
	h.wantCache(t, h.emb(t, clientKey, body), 200, statusHit, cacheHit)
	if h.embeddings() != 1 {
		t.Fatalf("upstream got %d, want 1", h.embeddings())
	}
}

// 领头的命中时把条目交给跟随的：热的键上同时到的相同请求，跟随的都是 collapsed，上游不多一次。
func TestEmbeddingCacheHitIsShared(t *testing.T) {
	mc := newMemCache()
	g := newGate()
	g.at = 1 // 第一次读（写进缓存的那个请求）直接过去，挡住第二次：会命中的领头的
	mc.getHook = g.hook
	h := newCacheHarness(t, newAllowAll(), &stubLimiter{}, mc)
	t.Cleanup(g.open)
	h.wantCache(t, h.emb(t, clientKey, embBody), 200, statusMiss, cacheMiss)
	const n = 5
	replies := h.embAsync(h.clientCtx, clientKey, embBody, n)
	g.wait(t)
	waitFor(t, func() bool { return h.waiting() == n-1 })
	g.open()
	groups := byCacheStatus(collect(t, replies, n, 5*time.Second))
	if len(groups[statusHit]) != 1 || len(groups[statusCollapsed]) != n-1 || h.embeddings() != 1 {
		t.Fatalf("%d hit, %d collapsed, upstream got %d; want 1, %d, 1", len(groups[statusHit]), len(groups[statusCollapsed]), h.embeddings(), n-1)
	}
}
