package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

const (
	parentTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	parentSpanID  = "00f067aa0ba902b7"
	sampledParent = "00-" + parentTraceID + "-" + parentSpanID + "-01"
	unsampled     = "00-" + parentTraceID + "-" + parentSpanID + "-00"
)

// rootSpan 等到调用号为 id 的根 span 结束，返回它和它的子 span（按开始时间排序）。根 span 在 handler 的最后一步结束。
func (h *harness) rootSpan(t *testing.T, id string) (sdktrace.ReadOnlySpan, []sdktrace.ReadOnlySpan) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		spans := h.spans.Ended()
		for _, s := range spans {
			if attr(s, "qf.request_id") != id {
				continue
			}
			var children []sdktrace.ReadOnlySpan
			for _, c := range spans {
				if c.Parent().SpanID() == s.SpanContext().SpanID() {
					children = append(children, c)
				}
			}
			slices.SortFunc(children, func(a, b sdktrace.ReadOnlySpan) int { return a.StartTime().Compare(b.StartTime()) })
			return s, children
		}
		if time.Now().After(deadline) {
			t.Fatalf("no root span for %s", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// attr 返回 span 的属性值的文字；没有时为空。
func attr(s sdktrace.ReadOnlySpan, key string) string {
	for _, a := range s.Attributes() {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	var out []string
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}

// 一次调用是一棵两层的树：根是服务端 span，接在请求的 traceparent 下面；每次对外部依赖的调用是它的直接子 span。
// 日志行的 trace_id、span_id 就是根 span 的。
func TestSpanTree(t *testing.T) {
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, &stubLimiter{})
	r := h.chat(t, withModel(toolsBody, `"shared"`), "traceparent", sampledParent, "tracestate", "qs=run7")
	if r.status != http.StatusOK {
		t.Fatalf("status %d", r.status)
	}
	id := requestID(t, r)
	root, children := h.rootSpan(t, id)
	sc := root.SpanContext()
	if root.Name() != "POST /v1/chat/completions" || root.SpanKind() != oteltrace.SpanKindServer ||
		sc.TraceID().String() != parentTraceID || root.Parent().SpanID().String() != parentSpanID || !root.Parent().IsRemote() ||
		sc.TraceState().String() != "qs=run7" {
		t.Fatalf("root %q kind %v trace %s parent %s remote %v tracestate %q", root.Name(), root.SpanKind(), sc.TraceID(),
			root.Parent().SpanID(), root.Parent().IsRemote(), sc.TraceState())
	}
	want := []string{"authenticate", "admit", "reserve", "mark_sent", "upstream", "finish", "adjust"}
	if got := names(children); !slices.Equal(got, want) {
		t.Fatalf("children %v, want %v", got, want)
	}
	if len(h.spans.Ended()) != len(want)+1 {
		t.Fatalf("%d spans, want %d: a step hangs somewhere else in the tree", len(h.spans.Ended()), len(want)+1)
	}
	checkChildren(t, root, children)
	if got := attr(root, "http.response.status_code") + " " + attr(root, "qf.outcome") + " " + attr(root, "qf.model_alias"); got != "200 SETTLED shared" {
		t.Fatalf("root attributes: %s", got)
	}
	line := h.logLine(t, id)
	if line["trace_id"] != parentTraceID || line["span_id"] != sc.SpanID().String() || line["parent_span_id"] != parentSpanID {
		t.Fatalf("log trace_id %v span_id %v parent_span_id %v, root span %s", line["trace_id"], line["span_id"], line["parent_span_id"], sc.SpanID())
	}
	// 根 span 的起止和日志的 duration_ms 是同一个总耗时
	if ms := root.EndTime().Sub(root.StartTime()).Milliseconds(); float64(ms) != line["duration_ms"] {
		t.Fatalf("root span lasted %d ms, log says %v", ms, line["duration_ms"])
	}
}

// checkChildren：子 span 都在根 span 的 trace 里、时间在根 span 之内；upstream 是 CLIENT，其余是 INTERNAL。
func checkChildren(t *testing.T, root sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan) {
	t.Helper()
	for _, c := range children {
		if c.SpanContext().TraceID() != root.SpanContext().TraceID() || c.StartTime().Before(root.StartTime()) || c.EndTime().After(root.EndTime()) {
			t.Errorf("%s is not inside the root span", c.Name())
		}
		want := oteltrace.SpanKindInternal
		if c.Name() == "upstream" {
			want = oteltrace.SpanKindClient
		}
		if c.SpanKind() != want {
			t.Errorf("%s has kind %v, want %v", c.Name(), c.SpanKind(), want)
		}
	}
}

// 没有 traceparent 时新起一条 trace。
func TestSpanTreeNewTrace(t *testing.T) {
	h := newHarness(t)
	id := requestID(t, h.chat(t, `{"model":"shared"}`))
	root, children := h.rootSpan(t, id)
	if root.Parent().IsValid() || root.SpanContext().TraceID().String() == parentTraceID {
		t.Fatalf("root has parent %v", root.Parent())
	}
	if got := names(children); !slices.Equal(got, []string{"authenticate", "reserve", "mark_sent", "upstream", "finish"}) {
		t.Fatalf("children %v (no rate limits: no admit, no adjust)", got)
	}
}

// 出错的那一步是 Error，描述和 error.type 是错误码；没有 event，底层错误的原文不进 span。之后的步骤没有 span。
// 根 span 只有 5xx 是 Error。
func TestSpanTreeOnErrors(t *testing.T) {
	driverErr := errors.New("dial tcp 10.9.8.7:3306: DRIVER-TEXT")
	cases := []struct {
		name     string
		ledger   Ledger
		limiter  *stubLimiter
		body     string
		status   int
		children []string
		failed   string // 出错的子 span
		code     string
		depErr   string // qf_dependency_errors_total 里该是 1 的那一项（依赖/操作）；空：一项都没有
	}{
		{"quota exhausted", &faulty{Ledger: newAllowAll(), reserveErr: ledger.ErrQuotaExhausted}, &stubLimiter{}, `{"model":"shared"}`,
			429, []string{"authenticate", "reserve"}, "reserve", "quota_exhausted", ""},
		{"rate limiter down", &rated{Ledger: newAllowAll(), key: bothRates}, &stubLimiter{admitErr: driverErr}, `{"model":"shared"}`,
			503, []string{"authenticate", "admit"}, "admit", "rate_limiter_unavailable", "rate_limiter/admit"},
		{"upstream 500", newAllowAll(), &stubLimiter{}, `{"model":"fake-500"}`,
			502, []string{"authenticate", "reserve", "mark_sent", "upstream", "finish"}, "upstream", "upstream_error", ""},
		{"intent not recorded", &faulty{Ledger: newAllowAll(), markSentErr: driverErr}, &stubLimiter{}, `{"model":"shared"}`,
			503, []string{"authenticate", "reserve", "mark_sent"}, "mark_sent", "ledger_unavailable", "ledger/mark_sent"},
		{"finish failed", &faulty{Ledger: newAllowAll(), finishErr: driverErr}, &stubLimiter{}, `{"model":"shared"}`,
			200, []string{"authenticate", "reserve", "mark_sent", "upstream", "finish"}, "finish", "ledger_unavailable", "ledger/finish"},
		{"adjust failed", &rated{Ledger: newAllowAll(), key: bothRates}, &stubLimiter{adjustErr: driverErr}, `{"model":"shared"}`,
			200, []string{"authenticate", "admit", "reserve", "mark_sent", "upstream", "finish", "adjust"}, "adjust", "rate_limiter_unavailable", "rate_limiter/adjust"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarnessLimited(t, tc.ledger, tc.limiter)
			r := h.chat(t, tc.body)
			if r.status != tc.status {
				t.Fatalf("status %d, want %d", r.status, tc.status)
			}
			root, children := h.rootSpan(t, requestID(t, r))
			if got := names(children); !slices.Equal(got, tc.children) {
				t.Fatalf("children %v, want %v", got, tc.children)
			}
			checkErrorStatus(t, root, children, tc.failed, tc.code, tc.status)
			checkDependencyErrors(t, h.metrics, tc.depErr)
			if tc.failed == "upstream" { // 调过上游、用量未知
				checkUsageUnknown(t, root)
			}
		})
	}
}

// checkErrorStatus：只有出错的那一步是 Error，描述和 error.type 是错误码；根 span 只有 5xx 是 Error。
// 哪个 span 都没有 event，属性和状态里都没有底层错误的原文。
func checkErrorStatus(t *testing.T, root sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan, failed, code string, status int) {
	t.Helper()
	for _, s := range append(children, root) {
		if len(s.Events()) != 0 || strings.Contains(fmt.Sprint(s.Attributes(), s.Status()), "DRIVER-TEXT") {
			t.Errorf("%s carries the underlying error: %v %v %v", s.Name(), s.Events(), s.Attributes(), s.Status())
		}
	}
	for _, s := range children {
		isFailed := s.Name() == failed
		if isFailed != (s.Status().Code == codes.Error) {
			t.Errorf("%s status %v", s.Name(), s.Status())
		}
		if isFailed && (s.Status().Description != code || attr(s, "error.type") != code) {
			t.Errorf("%s status %q error.type %q, want %s", s.Name(), s.Status().Description, attr(s, "error.type"), code)
		}
	}
	if (root.Status().Code == codes.Error) != (status >= 500) {
		t.Errorf("root status %v for HTTP %d", root.Status(), status)
	}
}

// checkUsageUnknown：用量未知时根 span 只有 qf.usage.known=false，没有补 0 的 token 数。
func checkUsageUnknown(t *testing.T, root sdktrace.ReadOnlySpan) {
	t.Helper()
	if got := attr(root, "qf.usage.known"); got != "false" {
		t.Errorf("qf.usage.known = %q, want false", got)
	}
	for _, k := range []string{"qf.usage.prompt_tokens", "qf.usage.completion_tokens", "qf.usage.total_tokens"} {
		if got := attr(root, k); got != "" {
			t.Errorf("%s = %s, want no attribute", k, got)
		}
	}
}

// checkDependencyErrors：qf_dependency_errors_total 里正好有 want（“依赖/操作”）这一项、值是 1；want 为空时一项都没有。
func checkDependencyErrors(t *testing.T, m *telemetry.Metrics, want string) {
	t.Helper()
	series := testutil.CollectAndCount(m.DependencyErrors)
	dep, op, ok := strings.Cut(want, "/")
	if !ok {
		if series != 0 {
			t.Errorf("qf_dependency_errors_total has %d series, want none", series)
		}
		return
	}
	if n := testutil.ToFloat64(m.DependencyErrors.WithLabelValues(dep, op)); n != 1 || series != 1 {
		t.Errorf("qf_dependency_errors_total{%s} = %v in %d series, want 1 in 1", want, n, series)
	}
}

// 没通过鉴权、或者鉴权这一步就出错的请求没有 span，带了 traceparent 也没有；日志、指标照常。
func TestUnauthenticatedIsNotTraced(t *testing.T) {
	h := newHarnessLimited(t, &faulty{Ledger: newAllowAll()}, &stubLimiter{})
	r := h.post(context.Background(), "/v1/chat/completions", "qfk_wrong", `{"model":"shared"}`, "traceparent", sampledParent)
	if r.status != http.StatusUnauthorized {
		t.Fatalf("status %d", r.status)
	}
	if _, ok := h.logLine(t, requestID(t, r))["trace_id"]; ok {
		t.Fatal("the 401 log line has a trace_id")
	}
	down := newHarnessLimited(t, &faulty{Ledger: newAllowAll(), keyErr: errors.New("db down")}, &stubLimiter{})
	r = down.chat(t, `{"model":"shared"}`, "traceparent", sampledParent)
	if r.status != http.StatusServiceUnavailable {
		t.Fatalf("status %d", r.status)
	}
	down.logLine(t, requestID(t, r))
	if n := len(h.spans.Ended()) + len(down.spans.Ended()); n != 0 {
		t.Fatalf("%d spans for requests that did not pass authentication", n)
	}
	if n := testutil.ToFloat64(h.metrics.Requests.WithLabelValues("chat", "401", "invalid_api_key")); n != 1 {
		t.Fatalf("401 counted %v times", n)
	}
	if n := testutil.ToFloat64(down.metrics.DependencyErrors.WithLabelValues("ledger", "authenticate")); n != 1 {
		t.Fatalf("ledger error at authentication counted %v times", n)
	}
}

// 有 traceparent 时服从它的 sampled 标志，没有时按本地比例；没采样的调用日志里没有 trace_id。
func TestSamplingFollowsParent(t *testing.T) {
	cases := []struct {
		name        string
		ratio       float64
		traceparent string
		sampled     bool
	}{
		{"ratio 0, no parent", 0, "", false},
		{"ratio 0, sampled parent", 0, sampledParent, true},
		{"ratio 1, unsampled parent", 1, unsampled, false},
		{"ratio 1, no parent", 1, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarnessTelemetry(t, newAllowAll(), &stubLimiter{}, tracedAt(tc.ratio))
			var headers []string
			if tc.traceparent != "" {
				headers = []string{"traceparent", tc.traceparent}
			}
			line := h.logLine(t, requestID(t, h.chat(t, `{"model":"shared"}`, headers...)))
			_, logged := line["trace_id"]
			_, parent := line["parent_span_id"]
			if got := len(h.spans.Ended()) > 0; got != tc.sampled || logged != tc.sampled || parent != (tc.sampled && tc.traceparent != "") {
				t.Fatalf("spans %v, trace_id logged %v, parent_span_id logged %v; want %v", got, logged, parent, tc.sampled)
			}
		})
	}
}

// 没配导出地址：不读 traceparent。带着 sampled 的 traceparent 时，日志行也没有 trace_id、span_id、parent_span_id，
// 其余字段和不带 traceparent 时一样。
func TestUntracedLogHasNoTraceFields(t *testing.T) {
	h := newHarnessTelemetry(t, newAllowAll(), &stubLimiter{}, untraced)
	with := h.logLine(t, requestID(t, h.chat(t, `{"model":"shared"}`, "traceparent", sampledParent)))
	without := h.logLine(t, requestID(t, h.chat(t, `{"model":"shared"}`)))
	if got, want := keys(with), keys(without); !slices.Equal(got, want) {
		t.Fatalf("log fields with a traceparent %v, without %v", got, want)
	}
	for _, k := range []string{"trace_id", "span_id", "parent_span_id"} {
		if _, ok := with[k]; ok {
			t.Fatalf("log line has %s: %v", k, with)
		}
	}
}

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		if k != "time" && k != "duration_ms" && k != "request_id" { // 每次都不同的值，键都在
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// 写不写 trace 的字段看根 span 在不在记录，不看 span context 的 sampled 标志：不记录的 span（例如空实现）可能带着
// 调用方的 span context，sampled 是 01，trace id、span id 都是调用方的。
func TestTraceFieldsNeedARecordingSpan(t *testing.T) {
	tid, _ := oteltrace.TraceIDFromHex(parentTraceID)
	sid, _ := oteltrace.SpanIDFromHex(parentSpanID)
	remote := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: oteltrace.FlagsSampled, Remote: true})
	c := &call{span: oteltrace.SpanFromContext(oteltrace.ContextWithRemoteSpanContext(context.Background(), remote)), parentSpanID: sid}
	if !c.span.SpanContext().IsSampled() || c.span.IsRecording() {
		t.Fatal("the test needs a sampled span context on a span that does not record")
	}
	if attrs := traceAttrs(nil, c); len(attrs) != 0 {
		t.Fatalf("trace fields for a span that does not record: %v", attrs)
	}
}

// 不合法的 traceparent 照常处理：起新的 trace，日志没有 parent_span_id，不回 400。
func TestInvalidTraceparentStartsANewTrace(t *testing.T) {
	h := newHarness(t)
	for _, tp := range []string{
		"garbage",
		"00-" + parentTraceID + "-" + parentSpanID,                          // 少 flags
		"00-" + strings.ToUpper(parentTraceID) + "-" + parentSpanID + "-01", // 大写的十六进制
		"00-" + parentTraceID + "-" + strings.ToUpper(parentSpanID) + "-01",
		"00-00000000000000000000000000000000-" + parentSpanID + "-01", // trace-id 全 0
		"00-" + parentTraceID + "-0000000000000000-01",                // parent-id 全 0
		"ff-" + parentTraceID + "-" + parentSpanID + "-01",            // 不允许的版本
		"00-" + parentTraceID + "-" + parentSpanID + "-01-extra",      // 版本 00 不许有多余的部分
	} {
		r := h.chat(t, `{"model":"shared"}`, "traceparent", tp)
		if r.status != http.StatusOK {
			t.Fatalf("%q: status %d", tp, r.status)
		}
		id := requestID(t, r)
		root, _ := h.rootSpan(t, id)
		line := h.logLine(t, id)
		if root.Parent().IsValid() || root.SpanContext().TraceID().String() == parentTraceID {
			t.Errorf("%q: root span has parent %v, trace %s", tp, root.Parent(), root.SpanContext().TraceID())
		}
		if _, ok := line["parent_span_id"]; ok || line["trace_id"] != root.SpanContext().TraceID().String() {
			t.Errorf("%q: log line %v", tp, line)
		}
	}
}

func TestLookupSpans(t *testing.T) {
	h := newHarness(t)
	asked := requestID(t, h.chat(t, `{"model":"shared"}`))
	r := h.get("/v1/calls/"+asked, clientKey)
	if r.status != http.StatusNotFound { // allowAll 不存调用
		t.Fatalf("status %d", r.status)
	}
	root, children := h.rootSpan(t, requestID(t, r))
	if root.Name() != "GET /v1/calls/{id}" || attr(root, "qf.call_id") != asked || attr(root, "qf.endpoint") != "calls" ||
		!slices.Equal(names(children), []string{"authenticate", "lookup_call"}) {
		t.Fatalf("root %q call_id %q children %v", root.Name(), attr(root, "qf.call_id"), names(children))
	}
	if line := h.logLine(t, requestID(t, r)); line["trace_id"] != root.SpanContext().TraceID().String() {
		t.Fatalf("lookup log line %v", line)
	}
}

// QF 不往上游发 traceparent、tracestate：客户端的不转发，自己的也不注入。
func TestUpstreamGetsNoTraceparent(t *testing.T) {
	h := newHarness(t)
	h.chat(t, `{"model":"shared"}`, "traceparent", sampledParent, "tracestate", "qs=1", "baggage", "k=v")
	_, header, _ := h.upstreamSaw()
	for _, k := range []string{"Traceparent", "Tracestate", "Baggage"} {
		if v, ok := header[k]; ok {
			t.Errorf("the upstream got %s: %v", k, v)
		}
	}
}

// span 的属性只有这些键；别名只在查到模型时记（查不到的是客户端随便写的字符串）。
func TestSpanAttributesAllowList(t *testing.T) {
	allowed := []string{"http.request.method", "http.route", "http.response.status_code", "error.type", "qf.request_id",
		"qf.endpoint", "qf.model_alias", "qf.upstream", "qf.call_status", "qf.outcome", "qf.replay_of", "qf.call_id",
		"qf.reserved_tokens", "qf.usage.known", "qf.usage.prompt_tokens", "qf.usage.completion_tokens", "qf.usage.total_tokens",
		"qf.client_disconnected", "qf.rate_limited_by"}
	h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, &stubLimiter{})
	h.chat(t, withModel(toolsBody, `"fake-tool-call"`), "X-Run-Id", "RUN-MARKER", "X-Client-Request-Id", "CRID-MARKER")
	h.chat(t, `{"model":"fake-no-usage"}`, "Idempotency-Key", "IDEM-MARKER")
	unknown := requestID(t, h.chat(t, `{"model":"ALIAS-MARKER"}`))
	denied := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates},
		&stubLimiter{denial: &ratelimit.Denial{Bucket: ratelimit.Bucket{Layer: "key", ID: "1", Metric: "rpm"}, RetryAfter: 1}})
	limited := requestID(t, denied.chat(t, `{"model":"shared"}`))
	replayed := newHarnessLimited(t, replaying{newAllowAll()}, &stubLimiter{})
	replay := replayed.chat(t, `{"model":"shared"}`, "Idempotency-Key", "k")

	root, _ := h.rootSpan(t, unknown)
	if attr(root, "error.type") != "model_not_found" || attr(root, "qf.model_alias") != "" {
		t.Fatalf("unknown model: %v", root.Attributes())
	}
	if root, _ := denied.rootSpan(t, limited); attr(root, "qf.rate_limited_by") != "key/rpm" {
		t.Fatalf("rate limited: %v", root.Attributes())
	}
	// 重放的 x-request-id 是原来的调用号，这次调用自己的号只在日志里；按 replay_of 找根 span。
	if spans := replayed.spans.Ended(); len(spans) == 0 || !slices.ContainsFunc(spans, func(s sdktrace.ReadOnlySpan) bool {
		return attr(s, "qf.replay_of") == replay.header.Get("x-request-id")
	}) {
		t.Fatal("no root span with qf.replay_of")
	}
	for _, spans := range [][]sdktrace.ReadOnlySpan{h.spans.Ended(), denied.spans.Ended(), replayed.spans.Ended()} {
		checkAllowedAttributes(t, spans, allowed)
	}
}

// checkAllowedAttributes：span 的属性键都在 allowed 里，值里没有客户端给的标记（MARKER）。
func checkAllowedAttributes(t *testing.T, spans []sdktrace.ReadOnlySpan, allowed []string) {
	t.Helper()
	for _, s := range spans {
		for _, a := range s.Attributes() {
			if !slices.Contains(allowed, string(a.Key)) {
				t.Errorf("%s has attribute %s", s.Name(), a.Key)
			}
		}
		if strings.Contains(fmt.Sprint(s.Attributes()), "MARKER") {
			t.Errorf("%s carries a client string: %v", s.Name(), s.Attributes())
		}
	}
}

// 指标按调用的结果计；未知用量的调用不把 token 数计成 0；QF 自身耗时扣掉等上游的时间。
func TestRequestMetrics(t *testing.T) {
	h := newHarness(t)
	m := h.metrics
	h.chat(t, `{"model":"shared"}`)
	total := testutil.ToFloat64(m.UsageTokens.WithLabelValues("chat", "fake", "total"))
	h.chat(t, `{"model":"fake-no-usage"}`)
	h.chat(t, `{"model":"fake-500"}`)
	h.chat(t, `{"model":"nope"}`)
	slow := h.post(context.Background(), "/v1/embeddings", clientKey, `{"model":"slow","input":"x"}`)
	if slow.status != http.StatusOK {
		t.Fatalf("slow embeddings: %d", slow.status)
	}
	h.logLine(t, requestID(t, slow)) // 等它记完
	counts := map[string]float64{
		"requests chat 200":            testutil.ToFloat64(m.Requests.WithLabelValues("chat", "200", "")),
		"requests chat 502":            testutil.ToFloat64(m.Requests.WithLabelValues("chat", "502", "upstream_error")),
		"requests chat 404":            testutil.ToFloat64(m.Requests.WithLabelValues("chat", "404", "model_not_found")),
		"calls settled":                testutil.ToFloat64(m.Calls.WithLabelValues("chat", "settled")),
		"calls usage_unknown":          testutil.ToFloat64(m.Calls.WithLabelValues("chat", "usage_unknown")),
		"upstream ok":                  testutil.ToFloat64(m.UpstreamRequests.WithLabelValues("fake", "chat", "ok")),
		"upstream upstream_error":      testutil.ToFloat64(m.UpstreamRequests.WithLabelValues("fake", "chat", "upstream_error")),
		"embeddings upstream ok":       testutil.ToFloat64(m.UpstreamRequests.WithLabelValues("fake", "embeddings", "ok")),
		"embeddings settled":           testutil.ToFloat64(m.Calls.WithLabelValues("embeddings", "settled")),
		"embeddings completion tokens": testutil.ToFloat64(m.UsageTokens.WithLabelValues("embeddings", "fake", "completion")),
	}
	want := map[string]float64{"requests chat 200": 2, "requests chat 502": 1, "requests chat 404": 1, "calls settled": 1,
		"calls usage_unknown": 2, "upstream ok": 2, "upstream upstream_error": 1, "embeddings upstream ok": 1, "embeddings settled": 1,
		"embeddings completion tokens": 0}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("%s = %v, want %v", k, counts[k], v)
		}
	}
	if total <= 0 || testutil.ToFloat64(m.UsageTokens.WithLabelValues("chat", "fake", "total")) != total {
		t.Errorf("chat total tokens %v, then %v: unknown usage must not add", total, testutil.ToFloat64(m.UsageTokens.WithLabelValues("chat", "fake", "total")))
	}
	// 上游 1 秒才回答：总耗时 ≥ 1 秒，自身耗时在 SLO 的门槛之内
	text := scrape(t, h)
	for _, series := range []string{`qf_request_duration_seconds_bucket{endpoint="embeddings",le="0.5"} 0`,
		`qf_overhead_seconds_bucket{endpoint="embeddings",le="0.25"} 1`, `qf_upstream_duration_seconds_bucket{endpoint="embeddings",upstream="fake",le="0.5"} 0`} {
		if !strings.Contains(text, series+"\n") {
			t.Errorf("/metrics lacks %s", series)
		}
	}
}

func scrape(t *testing.T, h *harness) string {
	t.Helper()
	srv := h.metrics.Handler()
	req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
	rec := &recorder{header: http.Header{}}
	srv.ServeHTTP(rec, req)
	return rec.body.String()
}

type recorder struct {
	header http.Header
	body   strings.Builder
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recorder) WriteHeader(int)             {}

// 限流拒绝按层和种类计；依赖出错按操作计；重放单独计。
func TestRateLimitAndDependencyMetrics(t *testing.T) {
	denied := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates},
		&stubLimiter{denial: &ratelimit.Denial{Bucket: ratelimit.Bucket{Layer: "account", ID: "1", Metric: "tpm"}, RetryAfter: 2}})
	denied.chat(t, `{"model":"shared"}`)
	down := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, &stubLimiter{adjustErr: errors.New("redis down")})
	down.logLine(t, requestID(t, down.chat(t, `{"model":"shared"}`)))
	replayed := newHarnessLimited(t, replaying{newAllowAll()}, &stubLimiter{})
	replayed.chat(t, `{"model":"shared"}`, "Idempotency-Key", "k")
	waitFor(t, func() bool { return len(denied.logs.lines(t)) == 1 && len(replayed.logs.lines(t)) == 1 })
	for name, got := range map[string]float64{
		"rate limited":  testutil.ToFloat64(denied.metrics.RateLimited.WithLabelValues("account", "tpm")),
		"adjust failed": testutil.ToFloat64(down.metrics.DependencyErrors.WithLabelValues("rate_limiter", "adjust")),
		"replayed":      testutil.ToFloat64(replayed.metrics.Replays.WithLabelValues("chat")),
	} {
		if got != 1 {
			t.Errorf("%s counted %v times, want 1", name, got)
		}
	}
}

// 400 invalid_request 在日志里有固定的原因，不是 encoding/json 的错误原文（原文会带上请求体里的字符）。
func TestInvalidRequestReason(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		body    string
		headers []string
		reason  invalidReason
	}{
		{`{"model":"shared","messages":[REQ-MARKER]}`, nil, reasonMalformedJSON},
		{`["REQ-MARKER"]`, nil, reasonNotObject},
		{`{"model":"shared","max_tokens":"REQ-MARKER"}`, nil, reasonMaxTokensInvalid},
		{`{"model":"shared","x":"\ud800 REQ-MARKER"}`, []string{"Idempotency-Key", "k1"}, reasonFingerprintFailed},
	}
	for _, tc := range cases {
		r := h.chat(t, tc.body, tc.headers...)
		line := h.logLine(t, requestID(t, r))
		if r.status != http.StatusBadRequest || line["error_code"] != "invalid_request" || line["invalid_reason"] != string(tc.reason) {
			t.Errorf("%s: status %d, log %v", tc.body, r.status, line)
		}
	}
	// 分块编码坏了：连接还在，仍是 400，原因是读请求体失败
	resp := rawRequest(t, h, "POST /v1/chat/completions HTTP/1.1\r\nHost: qf\r\nAuthorization: Bearer "+clientKey+
		"\r\nTransfer-Encoding: chunked\r\n\r\nzz-REQ-MARKER\r\n", false)
	if !strings.HasPrefix(resp, "HTTP/1.1 400") {
		t.Fatalf("bad chunked body: %q", resp)
	}
	waitFor(t, func() bool { return strings.Contains(h.logs.String(), string(reasonUnreadableBody)) })
	if strings.Contains(h.logs.String(), "REQ-MARKER") {
		t.Fatal("the request body reached the log")
	}
}

// 请求体没发完客户端就断开：和写回之前断开一样，日志没有 status，有 client_disconnected，指标计在 status 0 下。
func TestBodyCutOffByClient(t *testing.T) {
	h := newHarness(t)
	rawRequest(t, h, "POST /v1/chat/completions HTTP/1.1\r\nHost: qf\r\nAuthorization: Bearer "+clientKey+
		"\r\nContent-Length: 100\r\n\r\n{\"model\":", true)
	waitFor(t, func() bool { return len(h.logs.lines(t)) == 1 })
	line := h.logs.lines(t)[0]
	if _, ok := line["status"]; ok || line["client_disconnected"] != true || line["error_code"] != "client_canceled" || line["invalid_reason"] != nil {
		t.Fatalf("log line %v", line)
	}
	if n := testutil.ToFloat64(h.metrics.Requests.WithLabelValues("chat", "0", "client_canceled")); n != 1 {
		t.Fatalf("counted %v times under status 0", n)
	}
}

// rawRequest 在一条 TCP 连接上发原始的请求。closeAfter 时发完就关连接，不读响应；否则返回响应的第一行。
func rawRequest(t *testing.T, h *harness, req string, closeAfter bool) string {
	t.Helper()
	u, err := url.Parse(h.qf.URL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	if closeAfter {
		return ""
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return line
}
