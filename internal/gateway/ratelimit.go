package gateway

import (
	"context"
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

// buckets 是这次请求要检查的桶：Key、账户、上游三层里限额不为 0 的那些。RPM 桶花 1，TPM 桶花预留量（估算）。
func buckets(c *call) []ratelimit.Bucket {
	var out []ratelimit.Bucket
	add := func(layer, id string, rpm, tpm int64) {
		if rpm > 0 {
			out = append(out, ratelimit.Bucket{Layer: layer, ID: id, Metric: "rpm", Limit: rpm, Cost: 1})
		}
		if tpm > 0 {
			out = append(out, ratelimit.Bucket{Layer: layer, ID: id, Metric: "tpm", Limit: tpm, Cost: c.planned.Amount()})
		}
	}
	add("key", strconv.FormatInt(c.key.ID, 10), c.key.Rate.RPM, c.key.Rate.TPM)
	add("account", strconv.FormatInt(c.key.AccountID, 10), c.key.AccountRate.RPM, c.key.AccountRate.TPM)
	up := c.model.Upstream
	add("upstream", up.Name, up.RPM, up.TPM)
	return out
}

// admit 是准入，在预留之前：被限流的请求不碰账本。一个桶都没有就不碰 Redis。
// 被拒是 429（带 Retry-After），Redis 出错或超时是 503：不放行。
func (g *gateway) admit(ctx context.Context, c *call) (e *apiError) {
	bs := buckets(c)
	if len(bs) == 0 {
		return nil
	}
	span := g.step(ctx, "admit")
	defer func() { endStep(span, e) }()
	lctx, cancel := limiterContext(ctx)
	defer cancel()
	denial, err := g.limiter.Admit(lctx, bs)
	switch {
	case err != nil:
		g.limiterFailed(c, "admit", err)
		return errRateLimiterUnavailable
	case denial != nil:
		c.deniedBy = &denial.Bucket
		return errRateLimited.withRetryAfter(strconv.Itoa(denial.RetryAfter))
	}
	c.admitted = bs
	return nil
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

// adjust 按结局修正准入时扣过的 TPM 桶，RPM 不退。在 endpoint 的 defer 里、写日志之前执行。
// 实际比估算多时再扣，但一个桶一次最多再扣它的容量 L：上游报的用量离谱时，桶最多被压到 −L、一分钟补回，
// 而不是被压成很大的负数、挡住这一层（上游那层就是所有账户）几个小时。账本不受影响，照旧按实际结算。
// 失败只记日志，不影响已经得到的结果，也不重试：桶里多扣着估算，偏保守。
func (g *gateway) adjust(ctx context.Context, c *call) {
	n := refund(c)
	if n == 0 {
		return
	}
	var tpm []ratelimit.Bucket
	for _, b := range c.admitted {
		if b.Metric == "tpm" {
			b.Cost = min(-n, b.Limit)
			tpm = append(tpm, b)
		}
	}
	if len(tpm) == 0 {
		return
	}
	span := g.step(ctx, "adjust")
	lctx, cancel := limiterContext(ctx)
	defer cancel()
	if err := g.limiter.Adjust(lctx, tpm); err != nil {
		g.limiterFailed(c, "adjust", err)
		endStep(span, errRateLimiterUnavailable)
		return
	}
	endStep(span, nil)
}
