package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// 上游响应体的上限。嵌入一次几十条、上千维，几 MiB；32 MiB 留足余量，又不会被异常上游拖垮内存。
const maxUpstreamBody = 32 << 20

// 每个上游最多留这么多空闲连接。Go 默认只留 2 个：同时调同一个上游的请求多于 2 个时，多出来的连接用完就关，
// 下一次重新建连接，关掉的一方（QF）留下 TIME_WAIT。压测（docs/scaling.md；16 个账户、64 并发、约 350 次/秒）里，
// 单个实例 30 秒积了约 9400 个；照这个速度，几百次/秒就会用光本机的临时端口。100 和默认的 MaxIdleConns（所有主机合计）一样。
const upstreamIdleConns = 100

// newUpstreamClient 用默认 Transport 的副本（代理变量、拨号和 TLS 的超时都照旧），只放宽每个上游的空闲连接数。
func newUpstreamClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = upstreamIdleConns
	return &http.Client{
		Transport: tr,
		// 不跟随重定向：跳转后的地址不在配置里，请求还带着上游 Key。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// upstreamRequest 建好发给目标 t 的请求：按 t 的上游模型名改写的请求体、上游的 Key，以及这次调用的 QF 调用号
// （X-Client-Request-Id，上游超时、拿不到它的请求号时凭这个对账单；换过上游也是同一个号，只有一个上游可能收到它）。
// 客户端发来的 X-Client-Request-Id、X-Run-Id 不转发。在落发送意图（换上游时是改道）之前建：建不起来时确定没发出。
func (g *gateway) upstreamRequest(c *call, t *config.Target, rq request) (*http.Request, *apiError) {
	up := t.Upstream
	req, err := http.NewRequest(http.MethodPost, up.URL(c.purpose), bytes.NewReader(rq.bodyFor(t)))
	if err != nil {
		return nil, errUpstreamError
	}
	req.Header.Set("Authorization", "Bearer "+up.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	req.Header.Set(headerClientRequestID, c.id)
	return req, nil
}

// forward 是对上游的一次尝试：包一个客户端 span，记这次尝试的指标，累计等上游的时间（QF 自身耗时 = 总耗时 − 它）。
// 流式时 span 和这段时间包住整个流，含写给客户端的时间。只把 ctx 用于 span：发给上游的请求头在 upstreamRequest 里定好，
// 不注入 traceparent。
func (g *gateway) forward(ctx, upCtx context.Context, w http.ResponseWriter, c *call, req *http.Request) (*ledger.Response, *apiError) {
	up := c.target.Upstream.Name
	span := g.step(ctx, "upstream", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrUpstream.String(up), semconv.HTTPRequestMethodPost))
	start := time.Now()
	res, e := g.exchange(ctx, upCtx, w, c, req)
	d := time.Since(start)
	c.upstreamDuration += d
	if c.upstreamStatus != 0 {
		span.SetAttributes(semconv.HTTPResponseStatusCode(c.upstreamStatus))
	}
	endStep(span, e)
	result := "ok"
	if e != nil {
		result = e.code
	}
	endpoint := string(c.purpose)
	g.tel.Metrics.UpstreamRequests.WithLabelValues(up, endpoint, result).Inc()
	g.tel.Metrics.UpstreamDuration.WithLabelValues(up, endpoint).Observe(d.Seconds())
	return res, e
}

// exchange 把请求发给上游，不重试。upCtx 是这次调用所有尝试共用的期限，不跟着客户端取消（有界结算）：请求一旦发出，
// 就等到上游回答或超时，照常收尾，客户端断开了也一样。上游的请求号、模型名和用量记到 c 上。
// 2xx 的应答原样交给客户端：非流式整个读进来，由 deliver 在收尾之后写回；流式由 relay 边读边写给 w。
func (g *gateway) exchange(ctx, upCtx context.Context, w http.ResponseWriter, c *call, req *http.Request) (*ledger.Response, *apiError) {
	c.upstreamCalled = true
	resp, err := g.client.Do(req.WithContext(upCtx))
	if err != nil {
		return nil, transportError(upCtx, err)
	}
	defer resp.Body.Close()
	c.upstreamStatus = resp.StatusCode
	c.upstreamXRequestID = resp.Header.Get(headerRequestID)
	if e := fromUpstreamStatus(resp.StatusCode); e != nil {
		return nil, e.withRetryAfter(resp.Header.Get(headerRetryAfter))
	}
	if c.stream {
		return g.relay(ctx, upCtx, w, c, resp)
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody+1))
	if err != nil {
		return nil, transportError(upCtx, err)
	}
	if len(respBody) > maxUpstreamBody || !c.recordResponse(respBody) {
		return nil, errUpstreamInvalidResponse
	}
	return &ledger.Response{Status: resp.StatusCode, ContentType: resp.Header.Values("Content-Type"), Body: respBody}, nil
}

// failover 在上一次尝试确定没发出（拨号失败）之后换到下一个目标：选目标 → 建请求 → 新上游准入（只准入上游层）
// → 账本改道 → 换走的上游层的桶在收尾时全部加回。准入和改道受所有尝试共用的期限限制，期限已经到了就不换。
// 任何一步不成就不换，返回 nil：调用照上一次尝试的结果收尾（确定没发出，释放），准入过的桶（新上游的也在里面）
// 在收尾时按这个结局全部加回。
func (g *gateway) failover(ctx, upCtx context.Context, c *call, rq request) *http.Request {
	if upCtx.Err() != nil {
		return nil
	}
	from := c.target.Upstream.Name
	next, p := g.router.pick(c.model.Targets, append(slices.Clone(c.failoverFrom), from))
	if next == nil {
		return nil
	}
	c.permit = p
	req, e := g.upstreamRequest(c, next, rq)
	if e != nil || !g.admitUpstream(ctx, upCtx, c, next.Upstream) || !g.reroute(ctx, upCtx, c, next) {
		g.router.release(c)
		return nil
	}
	c.releaseUpstream(from)
	c.failoverFrom = append(c.failoverFrom, from)
	c.target = next
	g.tel.Metrics.Failovers.WithLabelValues(string(c.purpose), from).Inc()
	return req
}

// transportError 区分上游超时、连接没建立和其它传输错误。
func transportError(upCtx context.Context, err error) *apiError {
	var op *net.OpError
	switch {
	case upCtx.Err() != nil:
		return errUpstreamTimeout
	case errors.As(err, &op) && op.Op == "dial": // 连接被拒绝、DNS 失败
		return errUpstreamUnavailable
	default:
		return errUpstreamError
	}
}

// recordResponse 从上游 2xx 的正文里取出请求号、实际模型名和用量记到 c 上。
// 正文不是 JSON 对象时返回 false；缺字段不算错，QF 不校验 OpenAI 的结构。
func (c *call) recordResponse(body []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return false
	}
	_ = json.Unmarshal(fields["id"], &c.upstreamResponseID)
	_ = json.Unmarshal(fields["model"], &c.upstreamModel)
	c.usage = parseUsage(fields["usage"], c.purpose)
	return true
}
