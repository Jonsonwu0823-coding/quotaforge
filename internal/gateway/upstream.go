package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

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

// upstreamRequest 建好发给上游的请求：改写过 model 的请求体、上游的 Key，以及这次调用的 QF 调用号（X-Client-Request-Id，
// 上游超时、拿不到它的请求号时凭这个对账单）。客户端发来的 X-Client-Request-Id、X-Run-Id 不转发。
// 在落发送意图之前建：建不起来时调用还是 RESERVED，确定没发出。
func (g *gateway) upstreamRequest(c *call, body []byte) (*http.Request, *apiError) {
	up := c.model.Upstream
	req, err := http.NewRequest(http.MethodPost, up.URL(c.purpose), bytes.NewReader(body))
	if err != nil {
		return nil, errUpstreamError
	}
	req.Header.Set("Authorization", "Bearer "+up.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Client-Request-Id", c.id)
	return req, nil
}

// forward 把请求发给上游，包一个客户端 span，记下等上游的时间（QF 自身耗时 = 总耗时 − 它）。
// 只把 ctx 用于 span：发给上游的请求头在 upstreamRequest 里定好，不注入 traceparent。
func (g *gateway) forward(ctx context.Context, c *call, req *http.Request) (*ledger.Response, *apiError) {
	span := g.step(ctx, "upstream", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrUpstream.String(c.model.Upstream.Name), semconv.HTTPRequestMethodPost))
	start := time.Now()
	res, e := g.exchange(ctx, c, req)
	c.upstreamDuration = time.Since(start)
	if c.upstreamStatus != 0 {
		span.SetAttributes(semconv.HTTPResponseStatusCode(c.upstreamStatus))
	}
	endStep(span, e)
	return res, e
}

// exchange 把请求发给上游，不重试。超时从 ctx 派生，但不跟着客户端取消（有界结算）：请求一旦发出，
// 就等到上游回答或超时，照常收尾，客户端断开了也一样。上游的请求号、模型名和用量记到 c 上。
// 2xx 的应答原样交给客户端。
func (g *gateway) exchange(ctx context.Context, c *call, req *http.Request) (*ledger.Response, *apiError) {
	upCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.Timeout(c.purpose))
	defer cancel()
	c.upstreamCalled = true
	resp, err := g.client.Do(req.WithContext(upCtx))
	if err != nil {
		return nil, transportError(upCtx, err)
	}
	defer resp.Body.Close()
	c.upstreamStatus = resp.StatusCode
	c.upstreamXRequestID = resp.Header.Get("x-request-id")
	if e := fromUpstreamStatus(resp.StatusCode); e != nil {
		return nil, e.withRetryAfter(resp.Header.Get("Retry-After"))
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
