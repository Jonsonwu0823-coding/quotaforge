package telemetry

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

func newMetrics() *Metrics { return NewMetrics(func() sql.DBStats { return sql.DBStats{} }) }

func mustTraceID(t *testing.T, s string) trace.TraceID {
	t.Helper()
	id, err := trace.TraceIDFromHex(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustSpanID(t *testing.T, s string) trace.SpanID {
	t.Helper()
	id, err := trace.SpanIDFromHex(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// fixedSpans 是两个固定的 span：一个服务端根 span（父 span 是远端的，带 tracestate），一个出错的客户端子 span。
func fixedSpans(t *testing.T) []sdktrace.ReadOnlySpan {
	tid := mustTraceID(t, "0af7651916cd43dd8448eb211c80319c")
	ts, err := trace.ParseTraceState("qs=1")
	if err != nil {
		t.Fatal(err)
	}
	root := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: mustSpanID(t, "00f067aa0ba902b7"),
		TraceFlags: trace.FlagsSampled, TraceState: ts})
	child := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: mustSpanID(t, "b7ad6b7169203331"), TraceFlags: trace.FlagsSampled})
	remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: mustSpanID(t, "a3ce929d0e0e4736"), Remote: true})
	res := resource.NewSchemaless(attribute.String("service.name", "quotaforge"), attribute.String("service.instance.id", "qf-a"))
	scope := instrumentation.Scope{Name: "qf"}
	start := time.Unix(1_700_000_000, 123_456_789)
	stubs := tracetest.SpanStubs{
		{Name: "POST /v1/chat/completions", SpanContext: root, Parent: remote, SpanKind: trace.SpanKindServer,
			StartTime: start, EndTime: start.Add(1500 * time.Millisecond), Resource: res, InstrumentationScope: scope,
			Attributes: []attribute.KeyValue{attribute.String("qf.request_id", "qf-x"), attribute.Int("http.response.status_code", 200),
				attribute.Bool("qf.usage.known", true), attribute.Int64("qf.usage.total_tokens", 1<<40)}},
		{Name: "upstream", SpanContext: child, Parent: root, SpanKind: trace.SpanKindClient,
			StartTime: start.Add(time.Millisecond), EndTime: start.Add(time.Second), Resource: res, InstrumentationScope: scope,
			Status: sdktrace.Status{Code: codes.Error, Description: "upstream_error"}},
	}
	return stubs.Snapshots()
}

// OTLP/JSON：十六进制的 id，64 位整数写成十进制字符串，kind 和 status 是 OTLP 的编号（ERROR 是 2，不是 SDK 的 1）。
func TestOTLPJSONEncoding(t *testing.T) {
	got, err := json.Marshal(encodeSpans(fixedSpans(t)))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"resourceSpans":[{"resource":{"attributes":[` +
		`{"key":"service.instance.id","value":{"stringValue":"qf-a"}},{"key":"service.name","value":{"stringValue":"quotaforge"}}]},` +
		`"scopeSpans":[{"scope":{"name":"qf"},"spans":[` +
		`{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","traceState":"qs=1","parentSpanId":"a3ce929d0e0e4736",` +
		`"name":"POST /v1/chat/completions","kind":2,"startTimeUnixNano":"1700000000123456789","endTimeUnixNano":"1700000001623456789",` +
		`"attributes":[{"key":"qf.request_id","value":{"stringValue":"qf-x"}},{"key":"http.response.status_code","value":{"intValue":"200"}},` +
		`{"key":"qf.usage.known","value":{"boolValue":true}},{"key":"qf.usage.total_tokens","value":{"intValue":"1099511627776"}}],` +
		`"status":{}},` +
		`{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","parentSpanId":"00f067aa0ba902b7",` +
		`"name":"upstream","kind":3,"startTimeUnixNano":"1700000000124456789","endTimeUnixNano":"1700000001123456789",` +
		`"status":{"code":2,"message":"upstream_error"}}]}]}]}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// collector 是一个假的 OTLP 收集端：记下收到的请求，按 status 回答。
type collector struct {
	*httptest.Server
	mu     sync.Mutex
	paths  []string
	types  []string
	bodies [][]byte
}

func newCollector(t *testing.T, status int) *collector {
	c := &collector{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.paths, c.types, c.bodies = append(c.paths, r.Method+" "+r.URL.Path), append(c.types, r.Header.Get("Content-Type")), append(c.bodies, body)
		c.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte("collector says SECRET-COLLECTOR-BODY"))
	}))
	t.Cleanup(c.Close)
	return c
}

func TestExporterPostsJSON(t *testing.T) {
	c := newCollector(t, http.StatusOK)
	if err := newExporter(c.URL).ExportSpans(context.Background(), fixedSpans(t)); err != nil {
		t.Fatal(err)
	}
	if len(c.paths) != 1 || c.paths[0] != "POST /v1/traces" || c.types[0] != "application/json" {
		t.Fatalf("requests %v, content types %v", c.paths, c.types)
	}
	var req otlpRequest
	if err := json.Unmarshal(c.bodies[0], &req); err != nil {
		t.Fatal(err)
	}
	if spans := req.ResourceSpans[0].ScopeSpans[0].Spans; len(spans) != 2 || spans[1].ParentSpanID != spans[0].SpanID {
		t.Fatalf("spans = %+v", spans)
	}
}

// 非 2xx 算失败，这一批丢掉、不重试；收集端的响应正文不进错误。
func TestExporterRejectsNon2xx(t *testing.T) {
	c := newCollector(t, http.StatusServiceUnavailable)
	err := newExporter(c.URL).ExportSpans(context.Background(), fixedSpans(t))
	if err == nil || !strings.Contains(err.Error(), "503") || strings.Contains(err.Error(), "SECRET-COLLECTOR-BODY") {
		t.Fatalf("err = %v", err)
	}
	if len(c.paths) != 1 {
		t.Fatalf("%d requests, want 1 (no retry)", len(c.paths))
	}
}

// 收集端挂住时，导出服从传进来的 ctx。
func TestExporterObeysContext(t *testing.T) {
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := newExporter(hang.URL).ExportSpans(ctx, fixedSpans(t)); err == nil {
		t.Fatal("export to a hanging collector succeeded")
	}
	if d := time.Since(start); d >= exportTimeout {
		t.Fatalf("export took %v, the context allowed 50ms", d)
	}
	if exportTimeout >= batchExportTimeout {
		t.Fatalf("the HTTP timeout %v must be shorter than the batch processor's %v", exportTimeout, batchExportTimeout)
	}
}

// 收集端收下连接却一直不回：先到的是导出器自己的 HTTP 超时，不是传进来的 ctx。批处理器给的 ctx 是它自己的 5 秒导出上限，
// 靠它兜底就等于没有这一层超时。ctx 给得比 exportTimeout 长得多，返回时它必须还没到期。
func TestExporterHTTPClientTimesOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := newExporter(hangingCollector(t)).ExportSpans(ctx, fixedSpans(t))
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("err = %v, want the HTTP client's timeout", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the export returned only when the context expired: the HTTP client has no timeout of its own")
	}
}

// hangingCollector 收到请求就挂住，直到测试结束。
func hangingCollector(t *testing.T) string {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv.URL
}

// 收集端挂住时，结束 span 不等导出（批量、异步）。同步发送时第一个 End 就会挂住。
func TestEndDoesNotWaitForCollector(t *testing.T) {
	tel, flush := Setup(config.Telemetry{OTLPEndpoint: hangingCollector(t), SampleRatio: 1, Instance: "t"}, newMetrics(),
		slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer func() { _ = flush() }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			_, span := tel.Tracer.Start(context.Background(), "s")
			span.End()
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ending spans waited for the hanging collector")
	}
}

// 退出时冲刷有上限：收集端挂住、队列里有好几批时，最多 FlushTimeout 左右就返回（没设上限时要一批一批等 HTTP 超时）。
func TestShutdownIsBounded(t *testing.T) {
	tel, flush := Setup(config.Telemetry{OTLPEndpoint: hangingCollector(t), SampleRatio: 1, Instance: "t"}, newMetrics(),
		slog.New(slog.NewJSONHandler(io.Discard, nil)))
	for range 2000 { // 4 批
		_, span := tel.Tracer.Start(context.Background(), "s")
		span.End()
	}
	start := time.Now()
	if err := flush(); err == nil {
		t.Fatal("flushing to a hanging collector succeeded")
	}
	if d := time.Since(start); d > FlushTimeout+time.Second {
		t.Fatalf("shutdown took %v, the cap is %v", d, FlushTimeout)
	}
}

// 收集端连不上时，错误进 slog（JSON、有节制）和计数，不写 stderr。
func TestExportErrorsGoToTheLog(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + ln.Addr().String()
	ln.Close()
	var logs bytes.Buffer
	m := newMetrics()
	tel, flush := Setup(config.Telemetry{OTLPEndpoint: dead, SampleRatio: 1, Instance: "t"}, m, slog.New(slog.NewJSONHandler(&logs, nil)))
	for range 3 {
		_, span := tel.Tracer.Start(context.Background(), "s")
		span.End()
	}
	_ = flush()
	if testutil.ToFloat64(m.TraceErrors) < 1 {
		t.Fatal("the failed export was not counted")
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	var l struct {
		Msg      string
		Problems int
		Error    string
	}
	if len(lines) != 1 || json.Unmarshal([]byte(lines[0]), &l) != nil || l.Msg != "telemetry" || l.Problems != 1 || l.Error == "" {
		t.Fatalf("logs = %q", logs.String())
	}
}

// 第一次立即记一行，之后每分钟最多一行，带上这期间的次数。SDK 的错误和警告也走这里，Info、Debug 不记。
func TestExportErrorsAreLoggedSparingly(t *testing.T) {
	var logs bytes.Buffer
	m := newMetrics()
	now := time.Unix(1_700_000_000, 0)
	p := &problems{log: slog.New(slog.NewJSONHandler(&logs, nil)), errors: m.TraceErrors, now: func() time.Time { return now }}
	sdk := logr.New(sdkLog{p})
	for range 5 {
		p.Handle(io.ErrUnexpectedEOF)
	}
	sdk.Error(io.EOF, "sdk error")
	sdk.V(1).Info("sdk warning")
	sdk.V(4).Info("sdk info")
	now = now.Add(59 * time.Second)
	p.Handle(io.ErrUnexpectedEOF)
	now = now.Add(2 * time.Second)
	p.Handle(io.ErrClosedPipe)
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var l struct {
			Problems int
			Error    string
		}
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s x%d", l.Error, l.Problems))
	}
	// 第二行：上一行之后被压下的 4 次导出错误、SDK 的错误和警告各 1 次、59 秒时的 1 次，加上它自己
	want := []string{"unexpected EOF x1", "io: read/write on closed pipe x8"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("log lines %q, want %q", got, want)
	}
	if n := testutil.ToFloat64(m.TraceErrors); n != 7 {
		t.Fatalf("counted %v errors, want 7 (SDK log messages are not export errors)", n)
	}
}

// 没配地址：空实现，不读 traceparent，什么都不导出。
func TestDisabled(t *testing.T) {
	tel, flush := Setup(config.Telemetry{}, newMetrics(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err := flush(); err != nil {
		t.Fatal(err)
	}
	h := http.Header{"Traceparent": {"00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"}}
	ctx := tel.Propagator.Extract(context.Background(), propagation.HeaderCarrier(h))
	_, span := tel.Tracer.Start(ctx, "s")
	if span.IsRecording() || span.SpanContext().IsValid() {
		t.Fatalf("disabled tracing produced span %v", span.SpanContext())
	}
}

// 指标的名字、帮助和类型符合 Prometheus 的约定；自身耗时的桶里正好有 SLO 的门槛。
func TestMetricsLint(t *testing.T) {
	m := newMetrics()
	m.Overhead.WithLabelValues("chat").Observe(0.1)
	problems, err := testutil.GatherAndLint(m.reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("%s: %s", p.Metric, p.Text)
	}
	if !strings.Contains(scrape(t, m), `qf_overhead_seconds_bucket{endpoint="chat",le="0.25"} 1`) {
		t.Fatal("0.25 is not a bucket boundary of qf_overhead_seconds")
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// 导出器不走 HTTP_PROXY 这类代理变量：span 只发给同一网络里的收集端。环境里配了代理也一样。
func TestExporterIgnoresProxyVariables(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://proxy.invalid:3128")
	t.Setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
	tr, ok := newExporter("http://qf-jaeger:4318").client.Transport.(*http.Transport)
	if !ok || tr.Proxy != nil {
		t.Fatalf("the exporter's transport uses a proxy function (ok %v)", ok)
	}
}
