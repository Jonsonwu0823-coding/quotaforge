package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// 三个出口（日志、导出的 span、/metrics）里都没有 Key、Authorization 的值、请求正文、响应正文和上游 Key。
// 走完整的调用，用真的账本：成功、带键的重放、上游出错、额度不足。请求和响应里放能认出来的标记：
// 请求正文里的 REQ-MARKER 和“查询_lookup”，脚本上游的响应正文里的 RESP-MARKER，假上游的回答 fake reply。
// span 和指标另外不许有客户端给的原始字符串（幂等键、run、客户端请求号），日志里有它们是对的。
func TestTelemetryHasNoSecrets(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	poor := newDBEnv(t, 1, nil) // 额度只有 1：每个请求都是 quota_exhausted
	tp, tps := "traceparent", sampledParent
	clientHeaders := []string{tp, tps, "X-Run-Id", "RUN-MARKER", "X-Client-Request-Id", "CRID-MARKER"}

	ok := e.send(t, "/v1/chat/completions", withModel(toolsBody, `"shared"`), clientHeaders...)
	resp := e.send(t, "/v1/chat/completions", `{"model":"missing-fields","messages":[{"content":"REQ-MARKER-2"}]}`, clientHeaders...)
	first := e.send(t, "/v1/chat/completions", withModel(toolsBody, `"fake-tool-call"`), "Idempotency-Key", "IDEM-MARKER", tp, tps)
	replay := e.send(t, "/v1/chat/completions", withModel(toolsBody, `"fake-tool-call"`), "Idempotency-Key", "IDEM-MARKER", tp, tps)
	failed := e.send(t, "/v1/chat/completions", `{"model":"fake-500","messages":[{"content":"REQ-MARKER-3"}]}`, clientHeaders...)
	exhausted := poor.send(t, "/v1/embeddings", `{"model":"shared","input":["REQ-MARKER-4"]}`, clientHeaders...)
	// 流式：流里的事件（假上游的内容含 "fake reply"）也是正文；正常结束的、中途断开的、上游报错的各一个。
	streamed := e.send(t, "/v1/chat/completions", streamOf("shared"), clientHeaders...)
	cut := e.send(t, "/v1/chat/completions", streamOf("fake-stream-cut"), "Idempotency-Key", "IDEM-MARKER-S", tp, tps)
	upErr := e.send(t, "/v1/chat/completions", streamOf("fake-stream-error"), clientHeaders...)
	for _, r := range []struct {
		r    reply
		want int
	}{{ok, 200}, {resp, 200}, {first, 200}, {replay, 200}, {failed, 502}, {exhausted, 429}, {streamed, 200}, {cut, 200}, {upErr, 200}} {
		if r.r.status != r.want {
			t.Fatalf("status %d, want %d", r.r.status, r.want)
		}
	}
	if replay.header.Get("Idempotent-Replayed") != "true" || !strings.Contains(string(resp.body), "RESP-MARKER") ||
		!strings.Contains(string(streamed.body), "fake reply") || !strings.Contains(string(upErr.body), "fake upstream stream error") {
		t.Fatal("the replay, the response marker or the stream content did not happen")
	}
	waitFor(t, func() bool { return len(e.logs.lines(t)) == 8 && len(poor.logs.lines(t)) == 1 })

	secrets := []string{e.key, poor.key, "Bearer", upstreamKey, scriptedKey, "REQ-MARKER", "RESP-MARKER", "fake reply", "查询_lookup",
		"fake upstream stream error", "fake_stream_error"}
	clientStrings := []string{"IDEM-MARKER", "RUN-MARKER", "CRID-MARKER"}
	for _, env := range []*dbEnv{e, poor} {
		checkOutputs(t, env.harness, secrets, clientStrings)
	}
}

// checkOutputs：日志、span、/metrics 三个出口都不为空，都没有 secrets；span 和指标另外没有客户端给的原始字符串。
func checkOutputs(t *testing.T, h *harness, secrets, clientStrings []string) {
	t.Helper()
	outputs := map[string]string{"logs": h.logs.String(), "spans": dumpSpans(h), "metrics": scrape(t, h)}
	if outputs["spans"] == "" || !strings.Contains(outputs["metrics"], "qf_requests_total") {
		t.Fatal("nothing was traced or measured")
	}
	for name, text := range outputs {
		forbidden := secrets
		if name != "logs" {
			forbidden = append(slices.Clone(secrets), clientStrings...)
		}
		for _, s := range forbidden {
			if strings.Contains(text, s) {
				t.Errorf("%s contain %q", name, s)
			}
		}
	}
}

// dumpSpans 把记下的 span 写成文字：名字、属性、状态、event、资源和 tracestate，什么都不漏。
func dumpSpans(h *harness) string {
	var b strings.Builder
	for _, s := range h.spans.Ended() {
		fmt.Fprintf(&b, "%s %v %v %v %v %v %s\n", s.Name(), s.Attributes(), s.Status(), s.Events(), s.Links(),
			s.Resource().Attributes(), s.SpanContext().TraceState())
	}
	return b.String()
}

// 收集端挂住不回时，请求照常完成：span 结束时只是入队，导出在后台。导出改成同步时请求会一直等着。
func TestRequestsDoNotWaitForTraceExport(t *testing.T) {
	release := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(collector.Close)
	t.Cleanup(func() { close(release) })
	var flush func() error
	h := newHarnessTelemetry(t, newAllowAll(), &stubLimiter{}, func(_ sdktrace.SpanProcessor, m *telemetry.Metrics) telemetry.Telemetry {
		var tel telemetry.Telemetry
		tel, flush = telemetry.Setup(config.Telemetry{OTLPEndpoint: collector.URL, SampleRatio: 1, Instance: "t"}, m,
			slog.New(slog.NewJSONHandler(io.Discard, nil)))
		return tel
	})
	defer func() { _ = flush() }()
	done := make(chan error, 1)
	go func() {
		for range 20 {
			r := h.post(context.Background(), "/v1/chat/completions", clientKey, `{"model":"shared"}`, "traceparent", sampledParent)
			if r.err != nil || r.status != http.StatusOK {
				done <- fmt.Errorf("status %d, %v", r.status, r.err)
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("requests waited for the hanging trace collector")
	}
}

// 指纹只看接口和请求体：同一个 Idempotency-Key、同样的请求体，换一个 traceparent（调用方每次重试一个新的 parent-id）
// 第二次是重放，不是 422；上游只收到一次。
func TestReplayIgnoresTraceparent(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	body := `{"model":"shared","messages":[{"content":"retry"}]}`
	first := e.send(t, "/v1/chat/completions", body, "Idempotency-Key", "retry-1",
		"traceparent", "00-"+parentTraceID+"-1111111111111111-01")
	second := e.send(t, "/v1/chat/completions", body, "Idempotency-Key", "retry-1",
		"traceparent", "00-"+parentTraceID+"-2222222222222222-01")
	if first.status != http.StatusOK || second.status != http.StatusOK || second.header.Get("Idempotent-Replayed") != "true" ||
		second.header.Get("x-request-id") != first.header.Get("x-request-id") || string(second.body) != string(first.body) {
		t.Fatalf("first %d, second %d replayed %q", first.status, second.status, second.header.Get("Idempotent-Replayed"))
	}
	if n := e.upstreamCalls(); n != 1 {
		t.Fatalf("the upstream got %d calls, want 1", n)
	}
}
