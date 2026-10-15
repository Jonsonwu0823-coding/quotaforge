package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// 熔断和选目标的单元测试：时钟和随机数都是注入的，不靠睡眠。

var t0 = time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

const testCooldown = 30 * time.Second

// testBreaker 是阈值 5、冷却 30 秒的熔断器；gauge 记下最后设的值。
func testBreaker() (*breaker, *float64) {
	gauge := new(float64)
	return &breaker{settings: config.Breaker{Failures: 5, Cooldown: testCooldown}, setGauge: func(v float64) { *gauge = v }}, gauge
}

// try 放行一次、马上交回结果；返回有没有放行、是不是探测。
func try(b *breaker, now time.Time, v verdict) (ok, probe bool) {
	p, ok := b.acquire(now)
	if ok {
		b.report(p, v, now)
	}
	return ok, ok && p.probe
}

// tripped 是连续失败到阈值、刚在 now 打开的熔断器。
func tripped(t *testing.T, now time.Time) (*breaker, *float64) {
	t.Helper()
	b, gauge := testBreaker()
	for range 5 {
		if ok, _ := try(b, now, failure); !ok {
			t.Fatal("refused before the threshold")
		}
	}
	if b.available(now) || *gauge != float64(open) {
		t.Fatalf("not open after 5 failures: state %v, gauge %v", b.state, *gauge)
	}
	return b, gauge
}

func TestBreakerOpensAtThreshold(t *testing.T) {
	b, gauge := testBreaker()
	for i := range 4 {
		try(b, t0, failure)
		if !b.available(t0) || b.state != closed {
			t.Fatalf("open after %d failures", i+1)
		}
	}
	try(b, t0, success) // 中间一次成功就清零
	for range 4 {
		try(b, t0, failure)
	}
	// 不算的（429、没发出）不动计数
	for range 10 {
		try(b, t0, neutral)
	}
	if b.state != closed || *gauge != float64(closed) {
		t.Fatalf("state %v after a success, 4 failures and neutral results", b.state)
	}
	try(b, t0, failure)
	if b.state != open || *gauge != float64(open) {
		t.Fatalf("state %v after 5 consecutive failures", b.state)
	}
	if _, ok := b.acquire(t0); ok {
		t.Fatal("an open circuit let a request through")
	}
}

// 冷却时间到了才放探测，差 1 纳秒也不放。
func TestBreakerCooldown(t *testing.T) {
	b, _ := tripped(t, t0)
	before := t0.Add(testCooldown - time.Nanosecond)
	if b.available(before) {
		t.Fatal("available before the cooldown")
	}
	if _, ok := b.acquire(before); ok {
		t.Fatal("a probe before the cooldown")
	}
	p, ok := b.acquire(t0.Add(testCooldown))
	if !ok || !p.probe {
		t.Fatalf("at the cooldown: ok %v, permit %+v", ok, p)
	}
}

// 半开时只放一个探测：探测在飞时别的请求当它开着；并发时也只有一个拿到。
func TestBreakerOneProbe(t *testing.T) {
	b, gauge := tripped(t, t0)
	at := t0.Add(testCooldown)
	if _, ok := b.acquire(at); !ok {
		t.Fatal("no probe")
	}
	if *gauge != float64(halfOpen) || b.available(at.Add(time.Hour)) {
		t.Fatalf("half-open: gauge %v, available %v", *gauge, b.available(at.Add(time.Hour)))
	}
	if _, ok := b.acquire(at.Add(time.Hour)); ok {
		t.Fatal("a second probe while the first is in flight")
	}

	b, _ = tripped(t, t0)
	var granted atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 64 {
		wg.Go(func() {
			<-start
			if _, ok := b.acquire(at); ok {
				granted.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if granted.Load() != 1 {
		t.Fatalf("%d concurrent requests got through a half-open circuit, want 1", granted.Load())
	}
}

// 探测的结果：成功关上（计数从 0 开始），失败重新打开、从失败的时间重新计时，不算的让下一个请求重新探测。
func TestBreakerProbeResult(t *testing.T) {
	at := t0.Add(testCooldown)

	b, gauge := tripped(t, t0)
	if ok, probe := try(b, at, success); !ok || !probe {
		t.Fatal("no probe")
	}
	if b.state != closed || *gauge != float64(closed) {
		t.Fatalf("state %v after a successful probe", b.state)
	}
	for range 4 {
		try(b, at, failure)
	}
	if b.state != closed {
		t.Fatal("the failure count was not reset by closing")
	}

	b, _ = tripped(t, t0)
	failedAt := at.Add(time.Second)
	p, _ := b.acquire(at)
	b.report(p, failure, failedAt)
	if b.state != open || b.available(failedAt.Add(testCooldown-time.Nanosecond)) || !b.available(failedAt.Add(testCooldown)) {
		t.Fatalf("after a failed probe: state %v, the new cooldown must start at the failure", b.state)
	}

	b, _ = tripped(t, t0)
	p, _ = b.acquire(at)
	b.report(p, neutral, at)
	if ok, probe := try(b, at, success); !ok || !probe || b.state != closed {
		t.Fatalf("after a neutral probe the next request must probe: ok %v, probe %v, state %v", ok, probe, b.state)
	}
}

// 打开之前就放行、打开之后才回来的结果不算数：不会把半开的熔断关上，也不会让开着的重新计时。
func TestBreakerStaleReport(t *testing.T) {
	b, _ := testBreaker()
	old, _ := b.acquire(t0)
	for range 5 {
		try(b, t0, failure)
	}
	b.report(old, success, t0)
	if b.state != open {
		t.Fatalf("a stale success closed the circuit: %v", b.state)
	}
	b.report(old, failure, t0.Add(testCooldown-time.Second))
	if !b.available(t0.Add(testCooldown)) {
		t.Fatal("a stale failure restarted the cooldown")
	}
	probe, _ := b.acquire(t0.Add(testCooldown))
	b.report(old, success, t0.Add(testCooldown))
	if b.state != halfOpen {
		t.Fatalf("a stale success ended the probe: %v", b.state)
	}
	b.report(probe, success, t0.Add(testCooldown))
	if b.state != closed {
		t.Fatalf("the probe's success did not close: %v", b.state)
	}
}

func TestVerdict(t *testing.T) {
	cases := []struct {
		e    *apiError
		want verdict
	}{
		{nil, success},
		{errUpstreamAuthFailed, success},
		{errUpstreamBadRequest, success},
		{errUpstreamRateLimited, neutral},
		{errUpstreamRateLimited.withRetryAfter("7"), neutral}, // 带上游 Retry-After 的是副本
		{errUpstreamUnavailable, failure},
		{errUpstreamTimeout, failure},
		{errUpstreamError, failure},
		{errUpstreamInvalidResponse, failure},
	}
	for _, tc := range cases {
		if got := verdictOf(tc.e); got != tc.want {
			t.Errorf("verdictOf(%v) = %v, want %v", tc.e, got, tc.want)
		}
	}
}

// testRouter 的随机源依次返回 seq 里的数。
func testRouter(seq func(n int) int, upstreams ...string) *router {
	r := &router{breakers: map[string]*breaker{}, now: func() time.Time { return t0 }, intn: seq}
	for _, u := range upstreams {
		r.breakers[u], _ = testBreaker()
	}
	return r
}

func tgt(upstream string, weight int) config.Target {
	return config.Target{Upstream: &config.Upstream{Name: upstream}, UpstreamModel: upstream + "-model", Weight: weight}
}

// 权重：随机源把 [0, 总权重) 每个值各给一次，每个目标被选中的次数正好等于它的权重；权重 0 的从不选。
func TestPickWeights(t *testing.T) {
	targets := []config.Target{tgt("a", 9), tgt("standby", 0), tgt("b", 1), tgt("c", 5)}
	next := 0
	r := testRouter(func(n int) int {
		if n != 15 {
			t.Fatalf("intn(%d), want the total weight 15", n)
		}
		next++
		return (next - 1) % n
	}, "a", "standby", "b", "c")
	got := map[string]int{}
	for range 15 {
		target, p := r.pick(targets, nil)
		got[target.Upstream.Name]++
		r.breakers[target.Upstream.Name].report(p, success, t0)
	}
	if got["a"] != 9 || got["b"] != 1 || got["c"] != 5 || got["standby"] != 0 {
		t.Fatalf("picks = %v, want a 9, b 1, c 5, standby 0", got)
	}
}

// 备用（权重 0）只在权重大于 0 的目标都用不了时选：试过了，或者熔断开着。多个备用按配置顺序。
// 试过的是上游：同一个上游的另一个目标也跳过。
func TestPickStandby(t *testing.T) {
	targets := []config.Target{tgt("a", 1), {Upstream: &config.Upstream{Name: "a"}, UpstreamModel: "a-other", Weight: 1},
		tgt("s1", 0), tgt("s2", 0)}
	r := testRouter(func(int) int { return 0 }, "a", "s1", "s2")
	name := func(tried ...string) string {
		target, p := r.pick(targets, tried)
		if target == nil {
			return ""
		}
		r.breakers[target.Upstream.Name].report(p, neutral, t0)
		return target.Upstream.Name + "/" + target.UpstreamModel
	}
	if got := name(); got != "a/a-model" {
		t.Fatalf("first pick = %s", got)
	}
	if got := name("a"); got != "s1/s1-model" {
		t.Fatalf("after trying a = %s, want the first standby", got)
	}
	if got := name("a", "s1"); got != "s2/s2-model" {
		t.Fatalf("after trying a and s1 = %s", got)
	}
	if got := name("a", "s1", "s2"); got != "" {
		t.Fatalf("after trying all = %s, want none", got)
	}
	for range 5 {
		try(r.breakers["a"], t0, failure)
	}
	if got := name(); got != "s1/s1-model" {
		t.Fatalf("with a open = %s, want the standby", got)
	}
	for range 5 {
		try(r.breakers["s1"], t0, failure)
		try(r.breakers["s2"], t0, failure)
	}
	if got := name(); got != "" {
		t.Fatalf("with every circuit open = %s, want none", got)
	}
}

// 冷却到了的上游可以选，但只有一个请求拿到探测：并发选目标时，其余的去下一个能用的目标，不会因为抢输了而失败。
func TestPickProbeRace(t *testing.T) {
	targets := []config.Target{tgt("a", 1), tgt("b", 0)}
	r := testRouter(func(int) int { return 0 }, "a", "b")
	for range 5 {
		try(r.breakers["a"], t0, failure)
	}
	r.now = func() time.Time { return t0.Add(testCooldown) }
	var probes, standby atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 64 {
		wg.Go(func() {
			<-start
			switch target, _ := r.pick(targets, nil); {
			case target == nil:
				t.Error("no target")
			case target.Upstream.Name == "a":
				probes.Add(1)
			default:
				standby.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if probes.Load() != 1 || standby.Load() != 63 {
		t.Fatalf("probes %d, standby %d; want 1 and 63", probes.Load(), standby.Load())
	}
}

// 判断“能用”之后、拿名额之前，探测名额被别的请求拿走：pick 跳过这个上游重选，拿到备用，不当成没有目标。
// 确定地走到重选那一支：choose 先问 usable、再调 intn，pick 最后才 acquire；测试的 intn 在返回之前替“别的请求”
// 把主目标的探测拿走。TestPickProbeRace 守的是并发下只有一个探测，这里守的是抢输之后的重选。
func TestPickStolenProbe(t *testing.T) {
	targets := []config.Target{tgt("a", 1), tgt("b", 0)}
	var r *router
	stolen := false
	r = testRouter(func(n int) int {
		if !stolen {
			stolen = true
			if _, ok := r.breakers["a"].acquire(r.now()); !ok {
				t.Fatal("the other request could not take the probe")
			}
		}
		return 0
	}, "a", "b")
	for range 5 {
		try(r.breakers["a"], t0, failure)
	}
	r.now = func() time.Time { return t0.Add(testCooldown) }
	target, p := r.pick(targets, nil)
	if !stolen || target == nil || target.Upstream.Name != "b" || p == nil || p.probe {
		t.Fatalf("pick = %+v, %+v; want the standby b after the probe of a was taken", target, p)
	}
	if r.breakers["a"].state != halfOpen {
		t.Fatalf("a is %v, want half-open (its probe is in flight)", r.breakers["a"].state)
	}
}
