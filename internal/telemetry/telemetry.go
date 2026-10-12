// Package telemetry 是 QF 的追踪（OpenTelemetry，以 OTLP/JSON 导出）和指标（Prometheus）。
package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// FlushTimeout 是退出时把还没导出的 span 发完的上限。它在在途请求都处理完之后才开始，不占它们的时间；
// 到点没发完的 span 丢掉，账本在这之前已经收完尾。
const FlushTimeout = 2 * time.Second

// batchExportTimeout 是批处理器给一次导出的上限，比 exporter 自己的 HTTP 超时长，只是兜底。
const batchExportTimeout = 5 * time.Second

const serviceName = "quotaforge"

// Telemetry 是网关用到的追踪和指标。
type Telemetry struct {
	Tracer     trace.Tracer
	Propagator propagation.TextMapPropagator
	Metrics    *Metrics
}

// Disabled 是不追踪的 Telemetry：Tracer 是空实现，Propagator 什么都不取，所以也不读请求里的 traceparent。
func Disabled(m *Metrics) Telemetry {
	return Telemetry{Tracer: noop.NewTracerProvider().Tracer(""), Propagator: propagation.NewCompositeTextMapPropagator(), Metrics: m}
}

// NewTracerProvider 是 QF 的追踪：span 交给 sp；有上游的 traceparent 时服从它的 sampled 标志（整条 trace 由最上游决定采不采），
// 没有时按 ratio 采样。生产里 sp 是批量处理器加 OTLP 导出器（Setup），测试里是内存里的记录器。
func NewTracerProvider(sp sdktrace.SpanProcessor, ratio float64, instance string) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName), semconv.ServiceInstanceID(instance))),
	)
}

// Enabled 是追踪开着的 Telemetry。只取 W3C 的 traceparent 和 tracestate，不取 baggage：它可能带客户端的任意数据。
func Enabled(tp trace.TracerProvider, m *Metrics) Telemetry {
	return Telemetry{
		Tracer:     tp.Tracer("github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"),
		Propagator: propagation.TraceContext{},
		Metrics:    m,
	}
}

// Setup 按配置建追踪。没配 OTLP 的地址时不建导出器、不追踪。返回的 shutdown 在退出时调用：
// 把队列里还没导出的 span 发完，最多 FlushTimeout。
//
// 导出是批量、异步的：span 结束时只是入队（队列有界，满了丢弃，不阻塞请求）。SDK 报的错误和它自己的日志
// 都接到 log（见 problems），不写 stderr。
func Setup(cfg config.Telemetry, m *Metrics, log *slog.Logger) (Telemetry, func() error) {
	if cfg.OTLPEndpoint == "" {
		return Disabled(m), func() error { return nil }
	}
	p := &problems{log: log, errors: m.TraceErrors, now: time.Now}
	otel.SetErrorHandler(p)
	otel.SetLogger(logr.New(sdkLog{p}))
	bsp := sdktrace.NewBatchSpanProcessor(newExporter(cfg.OTLPEndpoint), sdktrace.WithExportTimeout(batchExportTimeout))
	tp := NewTracerProvider(bsp, cfg.SampleRatio, cfg.Instance)
	return Enabled(tp, m), func() error {
		ctx, cancel := context.WithTimeout(context.Background(), FlushTimeout)
		defer cancel()
		return tp.Shutdown(ctx)
	}
}

// problems 把 SDK 报的错误（主要是导出失败）记成日志：第一次立即记一行，之后每分钟最多一行，带上这期间的次数。
// 收集端不可用时每个批次都会失败，不能每批一行。错误还计进 qf_trace_export_errors_total。
// 日志行是 msg=telemetry，字段 problems（次数）、error（最后一次的文字），不和调用日志的字段重名。
type problems struct {
	log    *slog.Logger
	errors prometheus.Counter
	now    func() time.Time

	mu      sync.Mutex
	next    time.Time // 这之前不再记日志
	pending int       // 上一行之后的次数
}

// Handle 实现 otel.ErrorHandler。
func (p *problems) Handle(err error) {
	p.errors.Inc()
	p.report(err.Error())
}

func (p *problems) report(text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending++
	now := p.now()
	if now.Before(p.next) {
		return
	}
	p.log.LogAttrs(context.Background(), slog.LevelWarn, "telemetry", slog.Int("problems", p.pending), slog.String("error", text))
	p.pending, p.next = 0, now.Add(time.Minute)
}

// sdkLog 是交给 otel.SetLogger 的 logr 接收端：SDK 的错误和警告（V(1)）进 problems，Info、Debug 不记。
// 键值对不记，只记消息文字。
type sdkLog struct{ p *problems }

func (sdkLog) Init(logr.RuntimeInfo)              {}
func (sdkLog) Enabled(level int) bool             { return level <= 1 }
func (s sdkLog) Info(_ int, msg string, _ ...any) { s.p.report(msg) }
func (s sdkLog) WithValues(...any) logr.LogSink   { return s }
func (s sdkLog) WithName(string) logr.LogSink     { return s }
func (s sdkLog) Error(err error, msg string, _ ...any) {
	if err != nil {
		msg += ": " + err.Error()
	}
	s.p.report(msg)
}
