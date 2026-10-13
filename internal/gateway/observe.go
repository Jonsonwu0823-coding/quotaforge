package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// 追踪和指标都在这里收口：每次调用一个根 span（服务端），每次对外部依赖的调用（账本、Redis、上游）一个子 span，
// 都是根的直接子 span；调用结束时 record 用同一个总耗时写日志、记指标、结束根 span。
// span 属性和指标标签的值只取固定集合（错误码、状态、结局、桶名）或配置里的名字（别名、上游名），客户端给的原始字符串
// 一律不进：没有 Key、账户、幂等键、run、客户端请求号和正文；这些要查就按 trace_id 回到日志。
// 出错的 span 只记错误码，不记底层错误的原文（驱动、Redis、网络的错误文字可能带地址），所以不用 RecordError。

var (
	attrRequestID      = attribute.Key("qf.request_id")
	attrEndpoint       = attribute.Key("qf.endpoint")
	attrModelAlias     = attribute.Key("qf.model_alias")
	attrUpstream       = attribute.Key("qf.upstream")
	attrCallStatus     = attribute.Key("qf.call_status")
	attrOutcome        = attribute.Key("qf.outcome")
	attrReplayOf       = attribute.Key("qf.replay_of")
	attrCallID         = attribute.Key("qf.call_id")
	attrReservedTokens = attribute.Key("qf.reserved_tokens")
	attrUsageKnown     = attribute.Key("qf.usage.known")
	attrPromptTokens   = attribute.Key("qf.usage.prompt_tokens")
	attrCompletion     = attribute.Key("qf.usage.completion_tokens")
	attrTotalTokens    = attribute.Key("qf.usage.total_tokens")
	attrDisconnected   = attribute.Key("qf.client_disconnected")
	attrRateLimitedBy  = attribute.Key("qf.rate_limited_by")
	attrStream         = attribute.Key("qf.stream")
	attrFirstEventMS   = attribute.Key("qf.stream.first_event_ms")
	attrWriteTimeout   = attribute.Key("qf.stream.write_timeout")
)

// startTrace 在鉴权之后建根 span：起点回填成 c.start（和日志的 duration_ms 同一个起点），鉴权的子 span 也按显式的起止时间补上。
// 鉴权没过（401）或者出错（账本不可用的 503）都不建：没有 Key 的请求不能借 traceparent 让 QF 产生 span。日志和指标照常有。
// 有上游的 traceparent 时根 span 接在它下面。返回带着根 span 的 ctx，之后的每一步都从它开子 span。
func (g *gateway) startTrace(r *http.Request, c *call, route string, authErr *apiError) context.Context {
	if authErr != nil {
		c.span = trace.SpanFromContext(r.Context()) // 空实现
		return r.Context()
	}
	authEnd := time.Now() // 鉴权刚做完
	ctx := g.tel.Propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	if p := trace.SpanContextFromContext(ctx); p.IsRemote() { // 只有合法的 traceparent 才会被取出来
		c.parentSpanID = p.SpanID()
	}
	ctx, c.span = g.tel.Tracer.Start(ctx, r.Method+" "+route, trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(c.start),
		trace.WithAttributes(semconv.HTTPRequestMethodKey.String(r.Method), semconv.HTTPRoute(route), attrRequestID.String(c.id)))
	_, auth := g.tel.Tracer.Start(ctx, "authenticate", trace.WithTimestamp(c.start))
	auth.End(trace.WithTimestamp(authEnd))
	return ctx
}

// step 开一个子 span，包住一次对外部依赖的调用。
func (g *gateway) step(ctx context.Context, name string, opts ...trace.SpanStartOption) trace.Span {
	_, span := g.tel.Tracer.Start(ctx, name, opts...)
	return span
}

// endStep 结束子 span：e 不为 nil 时标成 Error，描述和 error.type 都是错误码。
func endStep(span trace.Span, e *apiError) {
	if e != nil {
		span.SetAttributes(semconv.ErrorTypeKey.String(e.code))
		span.SetStatus(codes.Error, e.code)
	}
	span.End()
}

// ledgerFailed、limiterFailed 记下一次依赖操作失败：错误文字进日志（不含连接串、URL），次数进 qf_dependency_errors_total。
func (g *gateway) ledgerFailed(c *call, op string, err error) {
	c.ledgerError = err.Error()
	g.tel.Metrics.DependencyErrors.WithLabelValues("ledger", op).Inc()
}

func (g *gateway) limiterFailed(c *call, op string, err error) {
	c.rateLimiterError = err.Error()
	g.tel.Metrics.DependencyErrors.WithLabelValues("rate_limiter", op).Inc()
}

// record 是一次调用（或查询）结束时的记录，在 handler 的最后一个 defer 里：算一次总耗时，写日志、记指标、结束根 span。
func (g *gateway) record(c *call, endpoint string, logLine func(*call, time.Duration)) {
	d := time.Since(c.start)
	logLine(c, d)
	g.observe(c, endpoint, d)
	g.endTrace(c, endpoint, d)
}

// observe 记这次调用的指标。
func (g *gateway) observe(c *call, endpoint string, d time.Duration) {
	m := g.tel.Metrics
	m.Requests.WithLabelValues(endpoint, strconv.Itoa(c.status), c.errorCode).Inc()
	m.RequestDuration.WithLabelValues(endpoint).Observe(d.Seconds())
	m.Overhead.WithLabelValues(endpoint).Observe((d - c.upstreamDuration).Seconds())
	if c.replayOf != "" {
		m.Replays.WithLabelValues(endpoint).Inc()
	}
	if c.deniedBy != nil {
		m.RateLimited.WithLabelValues(c.deniedBy.Layer, c.deniedBy.Metric).Inc()
	}
	if c.upstreamCalled {
		up := c.model.Upstream.Name
		result := c.errorCode // 调过上游之后，错误码只会来自上游这一步
		if result == "" {
			result = "ok"
		}
		m.UpstreamRequests.WithLabelValues(up, endpoint, result).Inc()
		m.UpstreamDuration.WithLabelValues(up, endpoint).Observe(c.upstreamDuration.Seconds())
	}
	if c.reservation != nil {
		m.Calls.WithLabelValues(endpoint, strings.ToLower(string(c.outcome))).Inc()
	}
	if c.stream && c.upstreamStatus >= 200 && c.upstreamStatus < 300 { // 上游开始了流的调用
		m.Streams.WithLabelValues(streamResult(c), streamClient(c)).Inc()
	}
	if c.firstEvent != 0 {
		m.StreamFirstEvent.WithLabelValues(endpoint).Observe(c.firstEvent.Seconds())
	}
	if c.outcome == ledger.Settled { // 结算的一定有用量；未知用量的不计成 0，数在 qf_calls_total 里
		up := c.model.Upstream.Name
		m.UsageTokens.WithLabelValues(endpoint, up, "prompt").Add(float64(c.usage.Prompt))
		if c.usage.HasCompletion {
			m.UsageTokens.WithLabelValues(endpoint, up, "completion").Add(float64(c.usage.Completion))
		}
		m.UsageTokens.WithLabelValues(endpoint, up, "total").Add(float64(c.usage.Total))
	}
}

// streamResult 是 qf_streams_total 的 result：ok，或者流没有正常结束时的错误码（upstream_*，取值固定）。
func streamResult(c *call) string {
	if c.errorCode == "" {
		return "ok"
	}
	return c.errorCode
}

// streamClient 是 qf_streams_total 的 client：connected、disconnected，或者 write_timeout（客户端不读，写超过了期限）。
func streamClient(c *call) string {
	switch {
	case c.writeTimeout:
		return "write_timeout"
	case c.clientDisconnected:
		return "disconnected"
	default:
		return "connected"
	}
}

// endTrace 给根 span 设结果的属性并结束它，终点和日志的 duration_ms 一致。只有 5xx 标成 Error（HTTP 服务端 span 的惯例），
// 错误码照记。
func (g *gateway) endTrace(c *call, endpoint string, d time.Duration) {
	if !c.span.IsRecording() {
		return
	}
	c.span.SetAttributes(rootAttrs(c, endpoint)...)
	if c.status >= 500 {
		c.span.SetStatus(codes.Error, c.errorCode)
	}
	c.span.End(trace.WithTimestamp(c.start.Add(d)))
}

// rootAttrs 是根 span 上结果的属性：有的才写。别名只在查到模型之后记：查不到的是客户端随便写的字符串。
func rootAttrs(c *call, endpoint string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attrEndpoint.String(endpoint)}
	if c.status != 0 {
		attrs = append(attrs, semconv.HTTPResponseStatusCode(c.status))
	}
	if c.errorCode != "" {
		attrs = append(attrs, semconv.ErrorTypeKey.String(c.errorCode))
	}
	if c.model != nil {
		attrs = append(attrs, attrModelAlias.String(c.model.Alias), attrUpstream.String(c.model.Upstream.Name))
	}
	if c.reservation != nil {
		attrs = append(attrs, attrCallStatus.String(string(c.callStatus)), attrOutcome.String(string(c.outcome)),
			attrReservedTokens.Int64(c.reservation.Amount()))
	}
	if c.replayOf != "" {
		attrs = append(attrs, attrReplayOf.String(c.replayOf))
	}
	if c.lookupOf != "" {
		attrs = append(attrs, attrCallID.String(c.lookupOf))
	}
	if c.upstreamCalled {
		attrs = append(attrs, usageAttrs(c.usage)...)
	}
	if c.clientDisconnected {
		attrs = append(attrs, attrDisconnected.Bool(true))
	}
	if c.deniedBy != nil {
		attrs = append(attrs, attrRateLimitedBy.String(c.deniedBy.Name()))
	}
	if c.stream {
		attrs = append(attrs, attrStream.Bool(true))
	}
	if c.firstEvent != 0 {
		attrs = append(attrs, attrFirstEventMS.Int64(c.firstEvent.Milliseconds()))
	}
	if c.writeTimeout {
		attrs = append(attrs, attrWriteTimeout.Bool(true))
	}
	return attrs
}

// usageAttrs 是调过上游的调用的用量：未知时只有 qf.usage.known=false，不补 0。
func usageAttrs(u *ledger.Usage) []attribute.KeyValue {
	if u == nil {
		return []attribute.KeyValue{attrUsageKnown.Bool(false)}
	}
	attrs := []attribute.KeyValue{attrUsageKnown.Bool(true), attrPromptTokens.Int64(u.Prompt), attrTotalTokens.Int64(u.Total)}
	if u.HasCompletion {
		attrs = append(attrs, attrCompletion.Int64(u.Completion))
	}
	return attrs
}

// traceAttrs 加上日志和 trace 关联的字段：trace_id、根 span 的 span_id，请求带了合法的 traceparent 时还有 parent_span_id
// （调用方那一次尝试的 span id）。写不写看 QF 自己的根 span 是不是在记录，不看 traceparent 带来的 sampled 标志：
// 不记录的 span 可能带着调用方的 span context，那样会把调用方的 span id 写成 QF 的。
// 日志里有 trace_id，Jaeger 里就有这条 trace（导出失败时除外）。记录日志在结束根 span 之前（见 record）。
func traceAttrs(attrs []slog.Attr, c *call) []slog.Attr {
	if !c.span.IsRecording() {
		return attrs
	}
	sc := c.span.SpanContext()
	attrs = append(attrs, slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	if c.parentSpanID.IsValid() {
		attrs = append(attrs, slog.String("parent_span_id", c.parentSpanID.String()))
	}
	return attrs
}
