package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// exportTimeout 是一次导出（一个 HTTP 请求）的上限，比批处理器自己的导出上限（batchExportTimeout）短。
const exportTimeout = 2 * time.Second

// exporter 把一批 span 编成 OTLP/JSON，一批一次 POST 到 <地址>/v1/traces。
//
// 自己写而不用官方的 otlptracehttp：官方的经共用的 proto 包带进 gRPC，QF 不用 gRPC。
// 不重试、不压缩：失败（含非 2xx）这一批就丢掉，错误交给 SDK 的错误处理器；追踪不能影响请求，也不值得排队重发。
// 只编码 QF 用到的：没有 events、links，属性是字符串、整数、布尔或浮点。
type exporter struct {
	url    string
	client *http.Client
}

func newExporter(endpoint string) *exporter {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil // span 只发给同一网络里的收集端，不能被容器里注入的 HTTP_PROXY 送去代理
	return &exporter{url: endpoint + "/v1/traces", client: &http.Client{Transport: tr, Timeout: exportTimeout}}
}

// ExportSpans 实现 sdktrace.SpanExporter。响应正文读完丢掉，不进日志。
func (e *exporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	body, err := json.Marshal(encodeSpans(spans))
	if err != nil {
		return fmt.Errorf("encode spans: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("export spans: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("export spans: %w", err) // 错误里有地址；地址经过校验，不带凭据
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("export spans: the collector answered HTTP %d", resp.StatusCode)
	}
	return nil
}

func (e *exporter) Shutdown(context.Context) error {
	e.client.CloseIdleConnections()
	return nil
}

// 下面是 OTLP 的 JSON 编码（opentelemetry-proto 的 ExportTraceServiceRequest）：字段名小驼峰；trace id、span id
// 是十六进制，不是 protobuf JSON 默认的 base64；64 位整数（时间、整数属性）写成十进制字符串；kind、status 是整数。
type otlpRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeSpans struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type otlpSpan struct {
	TraceID           string         `json:"traceId"`
	SpanID            string         `json:"spanId"`
	TraceState        string         `json:"traceState,omitempty"`
	ParentSpanID      string         `json:"parentSpanId,omitempty"`
	Name              string         `json:"name"`
	Kind              int            `json:"kind"`
	StartTimeUnixNano string         `json:"startTimeUnixNano"`
	EndTimeUnixNano   string         `json:"endTimeUnixNano"`
	Attributes        []otlpKeyValue `json:"attributes,omitempty"`
	Status            otlpStatus     `json:"status"`
}

type otlpStatus struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type otlpKeyValue struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
	IntValue    string   `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
}

// otlpStatusCode 把 SDK 的状态换成 OTLP 的编号：两边的顺序不同（SDK：Unset 0、Error 1、Ok 2；OTLP：UNSET 0、OK 1、ERROR 2）。
var otlpStatusCode = map[codes.Code]int{codes.Unset: 0, codes.Ok: 1, codes.Error: 2}

// encodeSpans 编码一批 span。QF 只有一个 TracerProvider，一批里的 span 资源相同，资源取第一个的；按 scope 分组。
// span 的种类编号和 OTLP 的相同（INTERNAL 1、SERVER 2、CLIENT 3……），直接用。
func encodeSpans(spans []sdktrace.ReadOnlySpan) otlpRequest {
	if len(spans) == 0 {
		return otlpRequest{ResourceSpans: []otlpResourceSpans{}}
	}
	rs := otlpResourceSpans{Resource: otlpResource{Attributes: keyValues(spans[0].Resource().Attributes())}}
	scopes := map[string]int{} // scope 名 → 在 rs.ScopeSpans 里的位置
	for _, s := range spans {
		scope := s.InstrumentationScope()
		i, ok := scopes[scope.Name]
		if !ok {
			i = len(rs.ScopeSpans)
			scopes[scope.Name] = i
			rs.ScopeSpans = append(rs.ScopeSpans, otlpScopeSpans{Scope: otlpScope{Name: scope.Name, Version: scope.Version}})
		}
		rs.ScopeSpans[i].Spans = append(rs.ScopeSpans[i].Spans, encodeSpan(s))
	}
	return otlpRequest{ResourceSpans: []otlpResourceSpans{rs}}
}

func encodeSpan(s sdktrace.ReadOnlySpan) otlpSpan {
	sc := s.SpanContext()
	out := otlpSpan{
		TraceID:           sc.TraceID().String(),
		SpanID:            sc.SpanID().String(),
		TraceState:        sc.TraceState().String(),
		Name:              s.Name(),
		Kind:              int(s.SpanKind()),
		StartTimeUnixNano: strconv.FormatInt(s.StartTime().UnixNano(), 10),
		EndTimeUnixNano:   strconv.FormatInt(s.EndTime().UnixNano(), 10),
		Attributes:        keyValues(s.Attributes()),
		Status:            otlpStatus{Code: otlpStatusCode[s.Status().Code], Message: s.Status().Description},
	}
	if p := s.Parent(); p.SpanID().IsValid() {
		out.ParentSpanID = p.SpanID().String()
	}
	return out
}

func keyValues(attrs []attribute.KeyValue) []otlpKeyValue {
	out := make([]otlpKeyValue, 0, len(attrs))
	for _, a := range attrs {
		var v otlpValue
		switch a.Value.Type() {
		case attribute.BOOL:
			b := a.Value.AsBool()
			v.BoolValue = &b
		case attribute.INT64:
			v.IntValue = strconv.FormatInt(a.Value.AsInt64(), 10)
		case attribute.FLOAT64:
			f := a.Value.AsFloat64()
			v.DoubleValue = &f
		default: // 字符串；QF 不用切片类型的属性
			s := a.Value.String()
			v.StringValue = &s
		}
		out = append(out, otlpKeyValue{Key: string(a.Key), Value: v})
	}
	return out
}
