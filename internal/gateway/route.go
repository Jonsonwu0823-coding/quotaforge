package gateway

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// 选目标和熔断。熔断按上游、每个实例在内存里各记各的：它只决定“试不试这个上游”，不影响额度和账本的正确性，
// 两个实例短时间不一致（一个开着、一个还关着）可以接受，各自的失败和探测会收敛（docs/scaling.md）。

// circuit 是一个上游的熔断状态。取值就是 qf_upstream_circuit_state 的值。
type circuit int

const (
	closed   circuit = iota // 放行；记连续失败的次数
	halfOpen                // 冷却到了，放出去的一个探测请求还没有结果；别的请求当它开着
	open                    // 不放行，直到冷却时间到了
)

// verdict 是一次尝试对熔断的意义（见 verdictOf）。
type verdict int

const (
	neutral verdict = iota
	success
	failure
)

// breaker 是一个上游的熔断器。每次状态改变 gen 加 1：放行时发出的 permit 记着当时的 gen，结果回来时 gen 已经变了的
// （打开之前就发出、打开之后才回来的请求；半开时别的代的请求）不算数，只有当前这一代的结果能改变状态。
type breaker struct {
	settings config.Breaker
	setGauge func(float64)

	mu       sync.Mutex
	state    circuit
	failures int       // closed 时连续失败的次数
	openedAt time.Time // 最近一次打开的时间
	gen      uint64
}

// permit 是熔断放行的一次尝试，结果要交回 report。probe 表示它是半开时放出的那一个探测。
type permit struct {
	b     *breaker
	gen   uint64
	probe bool
}

// available 报告现在能不能试这个上游，不改状态：关着，或者开着但冷却已经到了（下一个请求会是探测）。
func (b *breaker) available(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == closed || (b.state == open && b.cooled(now))
}

// acquire 放行一次尝试。开着、冷却到了时转成半开，只放这一个探测；半开时不放。
func (b *breaker) acquire(now time.Time) (*permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.state == closed:
		return &permit{b: b, gen: b.gen}, true
	case b.state == open && b.cooled(now):
		b.move(halfOpen, now)
		return &permit{b: b, gen: b.gen, probe: true}, true
	}
	return nil, false
}

func (b *breaker) cooled(now time.Time) bool { return !now.Before(b.openedAt.Add(b.settings.Cooldown)) }

// report 交回一次尝试的结果。关着时：失败累计，到阈值打开；成功清零；不算的不动。
// 探测的结果：成功关上；失败重新打开、重新计时；不算的回到开着、保留原来打开的时间，下一个请求重新探测。
func (b *breaker) report(p *permit, v verdict, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.gen != b.gen {
		return
	}
	switch {
	case b.state == halfOpen && v == success:
		b.move(closed, now)
	case b.state == halfOpen && v == failure:
		b.move(open, now)
	case b.state == halfOpen:
		b.move(open, b.openedAt)
	case v == success:
		b.failures = 0
	case v == failure:
		if b.failures++; b.failures >= b.settings.Failures {
			b.move(open, now)
		}
	}
}

func (b *breaker) move(s circuit, now time.Time) {
	b.state, b.failures, b.gen = s, 0, b.gen+1
	if s == open {
		b.openedAt = now
	}
	b.setGauge(float64(s))
}

// verdictOf 是一次发出去的尝试对熔断的意义（docs/observability.md 的表）：
//   - 失败：连接没建立、超时、5xx、发出后断开、流中途失败、3xx 和不能用的正文；
//   - 不算：上游 429（它在限流，是背压不是故障）；
//   - 成功：其余的回答（2xx，401、403 和其它 4xx：上游活着，只是拒绝了这个请求）。
//
// 根本没发出去的尝试（准入、预留、发送意图失败，客户端已经断开）不经过这里，按“不算”交回（见 router.release）。
func verdictOf(e *apiError) verdict {
	switch {
	case e == nil, e.code == errUpstreamAuthFailed.code, e.code == errUpstreamBadRequest.code:
		return success
	case e.code == errUpstreamRateLimited.code:
		return neutral
	}
	return failure
}

// router 给每个请求选目标，持有每个上游的熔断器。breakers 在启动时按配置建好，之后只读。
type router struct {
	breakers map[string]*breaker
	now      func() time.Time
	intn     func(int) int // 返回 [0, n) 里的一个数；测试换成固定的
}

func newRouter(cfg *config.Config, m *telemetry.Metrics) *router {
	r := &router{breakers: map[string]*breaker{}, now: time.Now, intn: rand.IntN}
	for _, u := range cfg.Upstreams() {
		set := m.CircuitState.WithLabelValues(u.Name).Set
		set(float64(closed))
		r.breakers[u.Name] = &breaker{settings: cfg.Breaker(), setGauge: set}
	}
	return r
}

// pick 选这次调用的下一个目标，跳过 tried 里的上游和熔断不放行的上游：
//   - 剩下的权重大于 0 的目标里按权重随机选；
//   - 一个都没有时，按配置顺序取第一个剩下的权重 0 的目标（备用）；
//   - 还没有就返回 nil。
//
// 首次选目标和换上游都用它。选中的上游放行了才返回，permit 要交回 report 或 release。
func (r *router) pick(targets []config.Target, tried []string) (*config.Target, *permit) {
	now := r.now()
	skip := slices.Clone(tried)
	for {
		t := r.choose(targets, func(t *config.Target) bool {
			return !slices.Contains(skip, t.Upstream.Name) && r.breakers[t.Upstream.Name].available(now)
		})
		if t == nil {
			return nil, nil
		}
		if p, ok := r.breakers[t.Upstream.Name].acquire(now); ok {
			return t, p
		}
		skip = append(skip, t.Upstream.Name) // 判断之后被别的请求抢先拿走了探测
	}
}

// choose 在 usable 的目标里按权重选一个，没有权重大于 0 的就取第一个备用。usable 对每个目标只问一次。
func (r *router) choose(targets []config.Target, usable func(*config.Target) bool) *config.Target {
	var weighted []*config.Target
	var standby *config.Target
	total := 0
	for i := range targets {
		t := &targets[i]
		switch {
		case !usable(t):
		case t.Weight > 0:
			weighted, total = append(weighted, t), total+t.Weight
		case standby == nil:
			standby = t
		}
	}
	if total == 0 {
		return standby
	}
	n := r.intn(total)
	for _, t := range weighted {
		if n < t.Weight {
			return t
		}
		n -= t.Weight
	}
	panic("unreachable: n < total")
}

// report 交回调用手上那次尝试的结果。
func (r *router) report(c *call, v verdict) {
	if c.permit != nil {
		c.permit.b.report(c.permit, v, r.now())
		c.permit = nil
	}
}

// release 在调用结束时兜底：拿到了放行、却没有发出去的尝试按“不算”交回。半开时放出的探测名额一定还回去，
// 熔断不会卡在半开。
func (r *router) release(c *call) { r.report(c, neutral) }
