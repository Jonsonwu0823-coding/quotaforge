package gateway

import (
	"context"
	"slices"
	"strconv"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

// Limiter 是网关用到的限流操作：生产里是 *ratelimit.Limiter（Redis）。Admit 放行时返回 nil, nil；
// 返回错误就当成限流器不可用。
type Limiter interface {
	Admit(ctx context.Context, buckets []ratelimit.Bucket) (*ratelimit.Denial, error)
	Adjust(ctx context.Context, buckets []ratelimit.Bucket) error
}

// limiterContext 给一次限流操作用：不随客户端断开而取消，只受 config.RateLimitTimeout 限制。
func limiterContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), config.RateLimitTimeout)
}

// buckets 是这次请求要检查的桶：Key、账户和选中的目标的上游三层里限额不为 0 的那些。RPM 桶花 1，TPM 桶花预留量（估算）。
func buckets(c *call) []ratelimit.Bucket {
	out := layerBuckets("key", strconv.FormatInt(c.key.ID, 10), c.key.Rate.RPM, c.key.Rate.TPM, c.planned.Amount())
	out = append(out, layerBuckets("account", strconv.FormatInt(c.key.AccountID, 10), c.key.AccountRate.RPM, c.key.AccountRate.TPM, c.planned.Amount())...)
	return append(out, upstreamBuckets(c.target.Upstream, c.planned.Amount())...)
}

// upstreamBuckets 是一个上游的那一层桶。
func upstreamBuckets(up *config.Upstream, amount int64) []ratelimit.Bucket {
	return layerBuckets("upstream", up.Name, up.RPM, up.TPM, amount)
}

func layerBuckets(layer, id string, rpm, tpm, amount int64) []ratelimit.Bucket {
	var out []ratelimit.Bucket
	if rpm > 0 {
		out = append(out, ratelimit.Bucket{Layer: layer, ID: id, Metric: "rpm", Limit: rpm, Cost: 1})
	}
	if tpm > 0 {
		out = append(out, ratelimit.Bucket{Layer: layer, ID: id, Metric: "tpm", Limit: tpm, Cost: amount})
	}
	return out
}

// admit 是准入，在预留之前：被限流的请求不碰账本。一个桶都没有就不碰 Redis。
// 被拒是 429（带 Retry-After），Redis 出错或超时是 503：不放行。
func (g *gateway) admit(ctx context.Context, c *call) *apiError {
	lctx, cancel := limiterContext(ctx)
	defer cancel()
	return g.admitBuckets(ctx, lctx, c, buckets(c))
}

// admitUpstream 是换上游时新上游的准入：只准入上游层（Key、账户两层只在调用开始时准入一次）。lctx 从所有尝试共用的期限
// 派生。不放行（被限、Redis 出错）就不换；被拒的桶、Redis 的错误照常记进日志和指标，返回给客户端的仍是上一次尝试的错误。
func (g *gateway) admitUpstream(ctx, upCtx context.Context, c *call, up *config.Upstream) bool {
	lctx, cancel := context.WithTimeout(upCtx, config.RateLimitTimeout)
	defer cancel()
	return g.admitBuckets(ctx, lctx, c, upstreamBuckets(up, c.planned.Amount())) == nil
}

// admitBuckets 用 lctx 检查一组桶，放行时把它们记进 c.admitted。
func (g *gateway) admitBuckets(ctx, lctx context.Context, c *call, bs []ratelimit.Bucket) (e *apiError) {
	if len(bs) == 0 {
		return nil
	}
	span := g.step(ctx, "admit")
	defer func() { endStep(span, e) }()
	denial, err := g.limiter.Admit(lctx, bs)
	switch {
	case err != nil:
		g.limiterFailed(c, "admit", err)
		return errRateLimiterUnavailable
	case denial != nil:
		c.deniedBy = &denial.Bucket
		return errRateLimited.withRetryAfter(strconv.Itoa(denial.RetryAfter))
	}
	c.admitted = append(c.admitted, bs...)
	return nil
}

// releaseUpstream 把准入时给上游 name 扣过的桶从 c.admitted 挪到 c.released：这个上游确定没收到请求，
// 收尾时它的 TPM 全部加回，不按结局修正。
func (c *call) releaseUpstream(name string) {
	c.admitted = slices.DeleteFunc(c.admitted, func(b ratelimit.Bucket) bool {
		mine := b.Layer == "upstream" && b.ID == name
		if mine {
			c.released = append(c.released, b)
		}
		return mine
	})
}

// refund 是调用结束后要加回每个 TPM 桶的令牌数，按网关自己判定的结局，和账本写没写成功无关：
//   - SETTLED：加回“估算 − 实际”，实际比估算多时是负数，就是再扣；
//   - USAGE_UNKNOWN：不动，上游可能已经计了费，按估算算是保守的；
//   - 其余（没预留、重放、幂等的拒绝、发送意图没落成、没发出、上游 4xx）：全部加回。
func refund(c *call) int64 {
	switch c.outcome {
	case ledger.Settled:
		return c.planned.Amount() - c.usage.Total
	case ledger.UsageUnknown:
		return 0
	default:
		return c.planned.Amount()
	}
}

// adjust 按结局修正准入时扣过的 TPM 桶。在 endpoint 的 defer 里、写日志之前执行。
// 实际比估算多时再扣，但一个桶一次最多再扣它的容量 L：上游报的用量离谱时，桶最多被压到 −L、一分钟补回，
// 而不是被压成很大的负数、挡住这一层（上游那层就是所有账户）几个小时。账本不受影响，照旧按实际结算。
// 换上游时换走的上游（c.released）确定没收到请求，它的 TPM 不论结局都全部加回。
// RPM 只在嵌入缓存命中（CACHED）时退上游那一层的 1 个：命中不发给上游，不该占着上游的每分钟请求数；
// Key、账户两层照扣，命中也是客户端的一次请求。别的结局 RPM 都不退。加回之后不超过容量（见 bucket.lua）。
// 失败只记日志，不影响已经得到的结果，也不重试：桶里多扣着估算，偏保守。
func (g *gateway) adjust(ctx context.Context, c *call) {
	var bs []ratelimit.Bucket
	add := func(b ratelimit.Bucket, n int64) {
		if n != 0 {
			b.Cost = min(-n, b.Limit)
			bs = append(bs, b)
		}
	}
	for _, b := range c.admitted {
		switch {
		case b.Metric == "tpm":
			add(b, refund(c))
		case b.Layer == "upstream" && c.outcome == ledger.Cached:
			add(b, 1)
		}
	}
	for _, b := range c.released {
		if b.Metric == "tpm" {
			add(b, c.planned.Amount())
		}
	}
	if len(bs) == 0 {
		return
	}
	span := g.step(ctx, "adjust")
	lctx, cancel := limiterContext(ctx)
	defer cancel()
	if err := g.limiter.Adjust(lctx, bs); err != nil {
		g.limiterFailed(c, "adjust", err)
		endStep(span, errRateLimiterUnavailable)
		return
	}
	endStep(span, nil)
}
