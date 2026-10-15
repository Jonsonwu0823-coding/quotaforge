package gateway

import (
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// ledgerContext 给一次账本操作用：不随客户端断开而取消，只受 config.LedgerTimeout 限制，超了就是 503。
// 否则客户端一断开，提交可能被打断在半路，释放和结算也做不成。
func ledgerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), config.LedgerTimeout)
}

// estimate 是预留量的保守上界。输入取请求体的字节数：一个 token 一般至少对应一个字节，也不用解析消息内容。
// 输出：chat 取请求的 max_tokens，没写就用模型配置的默认值；embeddings 没有输出。
func estimate(m *config.Model, body []byte, maxTokens *int64) (input, output int64) {
	switch {
	case m.Purpose == config.Embeddings:
		output = 0
	case maxTokens != nil:
		output = *maxTokens
	default:
		output = m.DefaultMaxTokens
	}
	return int64(len(body)), output
}

// retryAfterSeconds 是 quota_exhausted 的 Retry-After：较大的上游超时，向上取整到秒。
// 不经管理员改上限，额度回来的唯一途径是进行中的调用结算或释放，它们都在一个上游超时内结束。
func retryAfterSeconds(cfg *config.Config) string {
	d := max(cfg.Timeout(config.Chat), cfg.Timeout(config.Embeddings))
	return strconv.Itoa(int(math.Ceil(d.Seconds())))
}

// plan 建好这次调用要预留的内容，估算按改写过 model 的请求体算。prepare 的最后一步：
// 准入的 TPM 花费和预留量都取它的 Amount，同一个数只算一次。
func plan(c *call, body []byte, maxTokens *int64) ledger.Reservation {
	input, output := estimate(c.model, body, maxTokens)
	r := ledger.Reservation{
		CallID:          c.id,
		AccountID:       c.key.AccountID,
		KeyID:           c.key.ID,
		Purpose:         string(c.purpose),
		Alias:           c.alias,
		Upstream:        c.target.Upstream.Name,
		UpstreamModel:   c.target.UpstreamModel,
		EstInput:        input,
		EstOutput:       output,
		ClientRequestID: checked(c.clientRequestID),
		RunID:           checked(c.runID),
	}
	if c.fingerprint != nil { // prepare 校验过键才会算指纹
		r.IdempotencyKey, r.Fingerprint = c.idempotencyKey, c.fingerprint
	}
	return r
}

// reserve 在账本里预留这次调用的额度。额度不够是 429，账本出错是 503，都不调上游。
// 带幂等键时，账本按这个键已有的记录判断：可能返回要重放的快照，或者 409、422，这些也都不调上游。
func (g *gateway) reserve(ctx context.Context, c *call) (replay *ledger.Replay, e *apiError) {
	span := g.step(ctx, "reserve")
	defer func() { endStep(span, e) }()
	lctx, cancel := ledgerContext(ctx)
	defer cancel()
	replay, err := g.ledger.Reserve(lctx, c.planned)
	switch {
	case errors.Is(err, ledger.ErrQuotaExhausted):
		return nil, g.quotaExhausted
	case errors.Is(err, ledger.ErrIdempotencyKeyReused):
		return nil, errIdempotencyKeyReused
	case errors.Is(err, ledger.ErrIdempotencyInProgress):
		return nil, errIdempotencyInProgress
	case errors.Is(err, ledger.ErrIdempotencyOutcomeUnknown):
		return nil, errIdempotencyOutcomeUnknown
	case errors.Is(err, ledger.ErrIdempotencyResponseTooLarge):
		return nil, errIdempotencyResponseTooLarge
	case errors.Is(err, ledger.ErrIdempotencyResponseUnavailable):
		return nil, errIdempotencyResponseUnavailable
	case err != nil:
		g.ledgerFailed(c, "reserve", err)
		return nil, errLedgerUnavailable
	case replay != nil:
		return replay, nil
	}
	c.reservation, c.callStatus = &c.planned, ledger.Reserved
	return nil, nil
}

// markSent 落发送意图：调用改成 IN_FLIGHT 并提交之后，才向上游发请求。没落成（账本出错，或者调用已被回收释放）就不发，
// 回 503，也不收尾：调用留给回收。
func (g *gateway) markSent(ctx context.Context, c *call) (e *apiError) {
	span := g.step(ctx, "mark_sent")
	defer func() { endStep(span, e) }()
	lctx, cancel := ledgerContext(ctx)
	defer cancel()
	if err := g.ledger.MarkSent(lctx, *c.reservation); err != nil {
		g.ledgerFailed(c, "mark_sent", err)
		return errLedgerUnavailable
	}
	c.callStatus = ledger.InFlight
	return nil
}

// reroute 换上游时在账本里改道（条件更新，见 ledger.Store.Reroute），改成了才能发给新的上游。账本操作的上限从所有尝试共用的
// 期限派生。没改成（被回收了、账本出错）返回 false：不发，调用照上一次尝试的结果收尾。
func (g *gateway) reroute(ctx, upCtx context.Context, c *call, t *config.Target) bool {
	span := g.step(ctx, "reroute")
	lctx, cancel := context.WithTimeout(upCtx, config.LedgerTimeout)
	defer cancel()
	if err := g.ledger.Reroute(lctx, c.planned, t.Upstream.Name, t.UpstreamModel); err != nil {
		g.ledgerFailed(c, "reroute", err)
		endStep(span, errLedgerUnavailable)
		return false
	}
	endStep(span, nil)
	c.planned.Upstream, c.planned.UpstreamModel = t.Upstream.Name, t.UpstreamModel
	return true
}

// finish 在账本里收尾。失败时不改已经拿到的上游结果：调用留在原来的状态，由回收接手，原因记进日志。
// 非流式拿到 2xx 时把响应交给账本：带幂等键的调用存成快照。流式不存快照，结算了的只交一个标记。
func (g *gateway) finish(ctx context.Context, c *call, res *ledger.Response, e *apiError) {
	result := ledger.Result{
		Status:             c.outcome,
		Evidence:           c.evidence,
		Usage:              c.usage,
		UpstreamXRequestID: checked(c.upstreamXRequestID),
		UpstreamResponseID: checked(c.upstreamResponseID),
		ResponseModel:      checked(c.upstreamModel),
	}
	switch {
	case e != nil && c.streamed: // 响应头和 200 已经写给客户端；错误码是补给客户端的错误事件里的那个
		result.HTTPStatus, result.ErrorCode = c.status, e.code
	case e != nil:
		result.HTTPStatus, result.ErrorCode = e.status, e.code
	case c.stream: // 流式不存快照：内容边读边写，QF 没有留下。结算了的只记一个标记，同一个键再来是 409（docs/api.md 的幂等）
		result.HTTPStatus = res.Status
		if c.outcome == ledger.Settled {
			result.Response = &ledger.Response{Status: res.Status, Streamed: true}
		}
	default:
		result.HTTPStatus, result.Response = res.Status, res
	}
	span := g.step(ctx, "finish")
	lctx, cancel := ledgerContext(ctx)
	defer cancel()
	if err := g.ledger.Finish(lctx, *c.reservation, result); err != nil {
		g.ledgerFailed(c, "finish", err)
		endStep(span, errLedgerUnavailable)
		return
	}
	endStep(span, nil)
	c.callStatus = result.Status
}

// notSent 报告上游的这次尝试确定没收到请求：连接没建立（拨号失败，见 transportError）。outcome 判“确定没发出”、
// send 判能不能换上游，都用这一条：换走了，前一个上游就一定算没收到；一个调用号最多到达一个可能计费的上游。
func notSent(e *apiError) bool { return e == errUpstreamUnavailable }

// outcome 是网关自己对一次调用结局的判定，和账本写没写成功无关；docs/ledger.md 的“上游结果怎么收尾”就是这张表。
//   - 确定没发出（发出前失败、发送意图没落成、连接没建立）：释放，凭 NotSent；
//   - 上游明确拒绝（4xx）：释放，凭上游的回答；
//   - 2xx 且 usage 合法：结算；
//   - 其余（2xx 缺 usage、正文不能用、3xx、5xx、超时、发出后连接断开）：用量未知，继续占着预留。
//     3xx 不保证请求没执行（例如 303 就是“处理完了，去别处取结果”），所以不当成拒绝。
func outcome(c *call, res *ledger.Response, e *apiError) (ledger.Status, ledger.Evidence) {
	switch {
	case !c.upstreamCalled || notSent(e):
		return ledger.Cancelled, ledger.NotSent
	case c.upstreamStatus >= 400 && c.upstreamStatus < 500:
		return ledger.Cancelled, ledger.Answered
	case res != nil && c.usage != nil:
		return ledger.Settled, ledger.Answered
	default:
		return ledger.UsageUnknown, ""
	}
}
