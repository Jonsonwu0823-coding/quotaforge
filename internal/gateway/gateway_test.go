package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// 用库的测试按 dbtest 的规则跳过或强制；只用替身的测试不需要 MySQL。
func TestMain(m *testing.M) { dbtest.Main(m) }

const (
	clientKey         = "qfk_client-test-key-AAAA"
	limitedKey        = "qfk_client-test-key-BBBB" // 只允许 shared
	upstreamKey       = "upstream-test-secret-CCCC"
	scriptedKey       = "upstream-test-secret-DDDD"
	chatTimeout       = 500 * time.Millisecond
	embeddingsTimeout = 5 * time.Second
	slowUpstreamMS    = 1000 // 比 chat 超时长、比 embeddings 超时短
)

// harness 起一个假上游（外面包一层，记下上游收到的请求和发出的响应）、一个脚本上游
// （按模型名返回任意状态、超大正文、重定向）和 QF。
type harness struct {
	qf       *httptest.Server
	up       *httptest.Server
	fake     *fakeupstream.Server
	logs     *syncBuffer
	spans    *tracetest.SpanRecorder
	metrics  *telemetry.Metrics
	mu       sync.Mutex
	upBody   []byte
	upHeader http.Header
	upResp   []byte
}

// newHarness 的账本是只放行的替身（allowAll），Key 是 clientKey 和 limitedKey。
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWith(t, newAllowAll())
}

// newHarnessWith 用给定的账本起 QF。限流器是 stubLimiter：配置里的上游和替身账本的 Key 都没有限额，碰不到它。
func newHarnessWith(t *testing.T, l Ledger) *harness {
	t.Helper()
	return newHarnessLimited(t, l, &stubLimiter{})
}

// newHarnessLimited 用给定的账本和限流器起 QF。追踪开着、全量采样：结束的 span 在 h.spans 里，指标在 h.metrics 里。
func newHarnessLimited(t *testing.T, l Ledger, lim Limiter) *harness {
	t.Helper()
	return newHarnessTelemetry(t, l, lim, tracedAt(1))
}

// tracedAt 是追踪开着、没有上游 traceparent 时按 ratio 采样的 Telemetry，和生产用同一个 TracerProvider。
func tracedAt(ratio float64) func(sdktrace.SpanProcessor, *telemetry.Metrics) telemetry.Telemetry {
	return func(sp sdktrace.SpanProcessor, m *telemetry.Metrics) telemetry.Telemetry {
		return telemetry.Enabled(telemetry.NewTracerProvider(sp, ratio, "test-instance"), m)
	}
}

// untraced 是没配导出地址时的 Telemetry。
func untraced(_ sdktrace.SpanProcessor, m *telemetry.Metrics) telemetry.Telemetry {
	return telemetry.Disabled(m)
}

// newHarnessTelemetry 用给定的账本、限流器和 Telemetry 起 QF。
func newHarnessTelemetry(t *testing.T, l Ledger, lim Limiter, tel func(sdktrace.SpanProcessor, *telemetry.Metrics) telemetry.Telemetry) *harness {
	t.Helper()
	h := &harness{fake: fakeupstream.New(), logs: &syncBuffer{}, spans: tracetest.NewSpanRecorder(),
		metrics: telemetry.NewMetrics(func() sql.DBStats { return sql.DBStats{} })}
	h.up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.fake.ServeHTTP(rec, r)
		h.mu.Lock()
		h.upBody, h.upHeader, h.upResp = body, r.Header.Clone(), rec.Body.Bytes()
		h.mu.Unlock()
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.Header().Set("X-Upstream-Marker", "upstream-only")
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(h.up.Close)
	scripted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Model string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		scriptedResponse(w, req.Model, h.up.URL)
	}))
	t.Cleanup(scripted.Close)
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadURL := "http://" + dead.Addr().String() + "/v1"
	dead.Close()

	cfg := loadConfig(t, h.up.URL+"/v1", scripted.URL+"/v1", deadURL)
	h.qf = httptest.NewServer(WithRequestID(New(cfg, l, lim, slog.New(slog.NewJSONHandler(h.logs, nil)), tel(h.spans, h.metrics))))
	t.Cleanup(h.qf.Close)
	return h
}

func scriptedResponse(w http.ResponseWriter, model, fakeURL string) {
	status, statusErr := strconv.Atoi(strings.TrimPrefix(model, "status-"))
	switch {
	case model == "redirect":
		w.Header().Set("Location", fakeURL+"/v1/chat/completions")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = w.Write([]byte(`{"redirect":true}`)) // 正文是 JSON 对象，确认 3xx 不会被当成成功透传
	case model == "missing-fields":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"chat.completion","marker":"RESP-MARKER-缺字段"}`))
	case model == "no-completion":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","usage":{"prompt_tokens":3,"total_tokens":3}}`))
	case model == "no-content-type":
		w.Header()["Content-Type"] = nil // 不让 net/http 给这个响应嗅探出 Content-Type
		_, _ = w.Write([]byte(`{"id":"no-ct"}`))
	case model == "null":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`null`))
	case model == "array":
		_, _ = w.Write([]byte(`[1,2,3]`))
	case model == "empty":
		w.WriteHeader(http.StatusOK)
	case model == "at-limit", model == "over-limit":
		size := 32 << 20
		if model == "over-limit" {
			size++
		}
		w.Header().Set("Content-Type", "application/json")
		prefix := `{"pad":"`
		_, _ = w.Write([]byte(prefix))
		_, _ = w.Write(bytes.Repeat([]byte("a"), size-len(prefix)-2))
		_, _ = w.Write([]byte(`"}`))
	case strings.HasPrefix(model, "size-"): // 正好 n 字节的 2xx 正文，带合法的嵌入 usage
		n, _ := strconv.Atoi(strings.TrimPrefix(model, "size-"))
		prefix := `{"usage":{"prompt_tokens":1,"total_tokens":1},"pad":"`
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(prefix))
		_, _ = w.Write(bytes.Repeat([]byte("a"), n-len(prefix)-2))
		_, _ = w.Write([]byte(`"}`))
	case strings.HasPrefix(model, "status-") && statusErr == nil:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"scripted upstream original text"}}`))
	default:
		w.WriteHeader(http.StatusTeapot)
	}
}

// defaultMaxTokens 是测试配置里 chat 模型的 default_max_tokens。
const defaultMaxTokens = 64

// allAliases 是测试配置里的全部别名，clientKey 都能用。
var allAliases []string

func loadConfig(t *testing.T, fakeURL, scriptedURL, deadURL string) *config.Config {
	t.Helper()
	var models []map[string]any
	var aliases []string
	add := func(alias, purpose, upstream, upstreamModel string) {
		m := map[string]any{"alias": alias, "purpose": purpose, "upstream": upstream, "upstream_model": upstreamModel}
		if purpose == "chat" {
			m["default_max_tokens"] = defaultMaxTokens
		}
		models = append(models, m)
		if !slices.Contains(aliases, alias) {
			aliases = append(aliases, alias)
		}
	}
	// 同一个别名分别配给 chat 和 embeddings
	add("shared", "chat", "fake", "fake-chat")
	add("shared", "embeddings", "fake", "fake-embed")
	for _, s := range []string{"fake-tool-call", "fake-no-usage", "fake-hang", "fake-500", "fake-429", "fake-401", "fake-not-json", "fake-overuse", "fake-billed-hang"} {
		add(s, "chat", "fake", s)
	}
	add("slow", "chat", "fake", fmt.Sprintf("fake-slow-%d", slowUpstreamMS))
	add("quick", "chat", "fake", "fake-slow-150") // 在 chat 的超时之内回答：给“客户端中途断开”用
	add("slow", "embeddings", "fake", fmt.Sprintf("fake-slow-%d", slowUpstreamMS))
	add("emb-hang", "embeddings", "fake", "fake-hang")
	add("emb-no-usage", "embeddings", "fake", "fake-no-usage")
	add("emb-hold", "embeddings", "fake", "fake-hold") // embeddings 的超时长，挂住的请求等得起
	for _, s := range []string{"redirect", "missing-fields", "array", "null", "no-content-type", "empty", "no-completion", "status-201", "status-400", "status-404", "status-422", "status-403", "status-503"} {
		add(s, "chat", "scripted", s)
	}
	// 32 MiB 的正文在 -race 下要传一阵，放在超时更长的 embeddings 上。
	add("at-limit", "embeddings", "scripted", "at-limit")
	// 快照上限附近的正文：1 MiB、正好 8 MiB、8 MiB 加 1 字节、32 MiB（上游正文的上限）。
	for _, n := range []int{1 << 20, 8 << 20, 8<<20 + 1, 32 << 20} {
		add(fmt.Sprintf("size-%d", n), "embeddings", "scripted", fmt.Sprintf("size-%d", n))
	}
	add("over-limit", "embeddings", "scripted", "over-limit")
	add("dead", "chat", "dead", "whatever")
	if allAliases == nil { // 每次生成的都一样；只写一次，免得和进行中的请求竞争
		allAliases = aliases
	}

	cfg := map[string]any{
		"timeouts": map[string]any{"chat_ms": chatTimeout.Milliseconds(), "embeddings_ms": embeddingsTimeout.Milliseconds()},
		"upstreams": []any{
			map[string]any{"name": "fake", "base_url": fakeURL, "api_key_env": "UP_KEY"},
			map[string]any{"name": "scripted", "base_url": scriptedURL, "api_key_env": "SCRIPTED_KEY"},
			map[string]any{"name": "dead", "base_url": deadURL, "api_key_env": "UP_KEY"},
		},
		"models":   models,
		"database": map[string]any{"dsn_env": "TEST_DSN"}, // 网关只经 Ledger 用库，这个值不会被连接
		"redis":    map[string]any{"url_env": "TEST_DSN"}, // 网关只经 Limiter 用 Redis，这个值也不会被连接
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"UP_KEY": upstreamKey, "SCRIPTED_KEY": scriptedKey, "TEST_DSN": "unused:unused@tcp(127.0.0.1:1)/unused"}
	c, err := config.Load(path, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type reply struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func (h *harness) post(ctx context.Context, path, key, body string, headers ...string) reply {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.qf.URL+path, strings.NewReader(body))
	if err != nil {
		return reply{err: err}
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, header: resp.Header, body: b, err: err}
}

func (h *harness) chat(t *testing.T, body string, headers ...string) reply {
	t.Helper()
	r := h.post(context.Background(), "/v1/chat/completions", clientKey, body, headers...)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

func (h *harness) upstreamSaw() (body []byte, header http.Header, resp []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.upBody, h.upHeader, h.upResp
}

// logLine 等到调用号为 id 的日志行出现。响应写回去之后日志才写，所以要等。
func (h *harness) logLine(t *testing.T, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var found []map[string]any
		for _, l := range h.logs.lines(t) {
			if l["request_id"] == id {
				found = append(found, l)
			}
		}
		if len(found) > 1 {
			t.Fatalf("%d log lines for %s, want 1", len(found), id)
		}
		if len(found) == 1 {
			return found[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no log line for %s", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		out = append(out, m)
	}
	return out
}

func requestID(t *testing.T, r reply) string {
	t.Helper()
	id := r.header.Get("x-request-id")
	if !strings.HasPrefix(id, "qf-") || len(id) < 20 {
		t.Fatalf("x-request-id = %q", id)
	}
	return id
}

// tools 里放中文、<>&、\u 转义、大数字、多余空白、深层嵌套和特殊的键序；MODEL 处填别名或上游模型名。
const toolsBody = `{ "messages" : [ {"role":"system","content":"你好 <b>&amp; é \"q\" REQ-MARKER-请求正文"},{"role":"user","content":"hi"} ],
  "model"  :   MODEL ,
  "temperature":0, "max_tokens":256, "stream":false,
  "tools":[{"type":"function","function":{"name":"查询_lookup","description":"a<b && c>d   \/",
    "parameters":{"z":1,"type":"object","a":{"b":{"c":{"d":[1, 2.50, {"e":123456789012345678901234567890}]}}},
      "properties":{"x":{"type":"string","enum":["<", ">", "&"]}}}}}],
  "tool_choice":"auto", "parallel_tool_calls":false }
`

func withModel(body, model string) string { return strings.Replace(body, "MODEL", model, 1) }

func TestForwardIsByteExact(t *testing.T) {
	cases := []struct {
		name, path, body, alias, upstreamModel string
	}{
		{"chat", "/v1/chat/completions", `{"model":MODEL,"messages":[{"role":"user","content":"<hi> & 你好"}],"temperature":0,"max_tokens":16,"stream":false}`, `"shared"`, `"fake-chat"`},
		{"chat with tools", "/v1/chat/completions", toolsBody, `"fake-tool-call"`, `"fake-tool-call"`},
		{"escaped alias", "/v1/chat/completions", `{"messages":[],"model":"shared"}`, ``, ``},
		{"embeddings", "/v1/embeddings", "{\n  \"input\": [\"一\", \"<two> & three\"],\n  \"model\":MODEL,\n  \"dimensions\": 4\n}", `"shared"`, `"fake-embed"`},
	}
	h := newHarness(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sent, want := withModel(tc.body, tc.alias), withModel(tc.body, tc.upstreamModel)
			if tc.name == "escaped alias" {
				sent, want = tc.body, strings.Replace(tc.body, `"shared"`, `"fake-chat"`, 1)
			}
			r := h.post(context.Background(), tc.path, clientKey, sent,
				"X-Client-Request-Id", "0b7f6d3e-2f7c-4c1e-9a55-1f3e5c8d9a10", "X-Run-Id", "run-0b7f6d3e-2f7c-4c1e-9a55-1f3e5c8d9a10")
			if r.err != nil || r.status != http.StatusOK {
				t.Fatalf("status = %d, err = %v, body = %s", r.status, r.err, r.body)
			}
			upBody, upHeader, upResp := h.upstreamSaw()
			if !bytes.Equal(upBody, []byte(want)) {
				t.Fatalf("upstream body differs\n got: %s\nwant: %s", upBody, want)
			}
			if got := upHeader.Get("Authorization"); got != "Bearer "+upstreamKey {
				t.Fatalf("upstream Authorization = %q", got)
			}
			if upHeader.Get("Content-Type") != "application/json" || upHeader.Get("Accept") != "application/json" {
				t.Fatalf("upstream Content-Type/Accept = %q/%q", upHeader.Get("Content-Type"), upHeader.Get("Accept"))
			}
			for name, values := range upHeader {
				for _, v := range values {
					if strings.Contains(v, clientKey) {
						t.Fatalf("client key forwarded in header %s", name)
					}
				}
			}
			// 上游收到的 X-Client-Request-Id 是 QF 的调用号（对账用），不是客户端发的值；X-Run-Id 不转发。
			if got := upHeader.Get("X-Client-Request-Id"); got != requestID(t, r) || upHeader.Get("X-Run-Id") != "" {
				t.Fatalf("upstream X-Client-Request-Id %q (want the QF call id %q), X-Run-Id %q",
					got, requestID(t, r), upHeader.Get("X-Run-Id"))
			}

			if !bytes.Equal(r.body, upResp) {
				t.Fatalf("response body differs from upstream\n got: %s\nwant: %s", r.body, upResp)
			}
			if ct := r.header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
			requestID(t, r)
			var resp struct {
				Model string
			}
			if err := json.Unmarshal(r.body, &resp); err != nil {
				t.Fatal(err)
			}
			if wantModel := strings.Trim(tc.upstreamModel, `"`) + fakeupstream.ModelVersionSuffix; tc.name != "escaped alias" && resp.Model != wantModel {
				t.Fatalf("response model = %q, want the upstream's %q", resp.Model, wantModel)
			}
		})
	}
}

func TestToolCallsPassThrough(t *testing.T) {
	h := newHarness(t)
	r := h.chat(t, withModel(toolsBody, `"fake-tool-call"`))
	if r.status != http.StatusOK {
		t.Fatalf("status = %d", r.status)
	}
	var resp struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					ID       string
					Function struct{ Name, Arguments string }
				} `json:"tool_calls"`
			}
			FinishReason string `json:"finish_reason"`
		}
	}
	if err := json.Unmarshal(r.body, &resp); err != nil {
		t.Fatal(err)
	}
	c := resp.Choices[0]
	if c.FinishReason != "tool_calls" || len(c.Message.ToolCalls) != 1 || c.Message.ToolCalls[0].ID == "" ||
		c.Message.ToolCalls[0].Function.Name != "查询_lookup" || c.Message.ToolCalls[0].Function.Arguments != "{}" {
		t.Fatalf("choice = %+v", c)
	}
}

// 第 2.4 节的映射表逐行一条；错误响应也要有 x-request-id，message 不带上游原文。
func TestErrorMapping(t *testing.T) {
	big := `{"model":"shared","pad":"` + strings.Repeat("a", 2<<20) + `"}`
	cases := []struct {
		name, path, key, body string
		status                int
		code                  string
	}{
		{"missing key", "/v1/chat/completions", "", `{"model":"shared"}`, 401, "invalid_api_key"},
		{"wrong key", "/v1/chat/completions", "qfk_wrong", `{"model":"shared"}`, 401, "invalid_api_key"},
		{"too large", "/v1/chat/completions", clientKey, big, 413, "request_too_large"},
		{"not json", "/v1/chat/completions", clientKey, `model=shared`, 400, "invalid_request"},
		{"not an object", "/v1/chat/completions", clientKey, `["shared"]`, 400, "invalid_request"},
		{"no model", "/v1/chat/completions", clientKey, `{"messages":[]}`, 400, "invalid_request"},
		{"empty model", "/v1/chat/completions", clientKey, `{"model":""}`, 400, "invalid_request"},
		{"model not a string", "/v1/chat/completions", clientKey, `{"model":42}`, 400, "invalid_request"},
		{"model null", "/v1/chat/completions", clientKey, `{"model":null}`, 400, "invalid_request"},
		{"duplicate model", "/v1/chat/completions", clientKey, `{"model":"shared","model":"fake-500"}`, 400, "invalid_request"},
		{"duplicate stream", "/v1/chat/completions", clientKey, `{"model":"shared","stream":true,"stream":false}`, 400, "invalid_request"},
		{"model case variant", "/v1/chat/completions", clientKey, `{"model":"shared","MODEL":"fake-500"}`, 400, "invalid_request"},
		{"only a case variant of model", "/v1/chat/completions", clientKey, `{"Model":"shared"}`, 400, "invalid_request"},
		{"stream fold variant", "/v1/chat/completions", clientKey, `{"model":"shared","stream":false,"ſtream":true}`, 400, "invalid_request"},
		{"trailing data", "/v1/chat/completions", clientKey, `{"model":"shared"} {}`, 400, "invalid_request"},
		{"broken nested json", "/v1/chat/completions", clientKey, `{"model":"shared","tools":[{]}`, 400, "invalid_request"},
		{"stream not bool", "/v1/chat/completions", clientKey, `{"model":"shared","stream":"yes"}`, 400, "invalid_request"},
		{"stream", "/v1/chat/completions", clientKey, `{"model":"shared","stream":true}`, 400, "stream_not_supported"},
		{"unknown alias", "/v1/chat/completions", clientKey, `{"model":"gpt-x"}`, 404, "model_not_found"},
		{"alias only for chat", "/v1/embeddings", clientKey, `{"model":"fake-tool-call","input":["a"]}`, 404, "model_not_found"},
		{"alias only for embeddings", "/v1/chat/completions", clientKey, `{"model":"emb-hang"}`, 404, "model_not_found"},
		{"model not allowed", "/v1/chat/completions", limitedKey, `{"model":"fake-500"}`, 403, "model_not_allowed"},
		{"upstream 400", "/v1/chat/completions", clientKey, `{"model":"status-400"}`, 400, "upstream_bad_request"},
		{"upstream 404", "/v1/chat/completions", clientKey, `{"model":"status-404"}`, 400, "upstream_bad_request"},
		{"upstream 422", "/v1/chat/completions", clientKey, `{"model":"status-422"}`, 400, "upstream_bad_request"},
		{"upstream 401", "/v1/chat/completions", clientKey, `{"model":"fake-401"}`, 502, "upstream_auth_failed"},
		{"upstream 403", "/v1/chat/completions", clientKey, `{"model":"status-403"}`, 502, "upstream_auth_failed"},
		{"upstream 429", "/v1/chat/completions", clientKey, `{"model":"fake-429"}`, 503, "upstream_rate_limited"},
		{"upstream 500", "/v1/chat/completions", clientKey, `{"model":"fake-500"}`, 502, "upstream_error"},
		{"upstream 503", "/v1/chat/completions", clientKey, `{"model":"status-503"}`, 502, "upstream_error"},
		{"connection refused", "/v1/chat/completions", clientKey, `{"model":"dead"}`, 502, "upstream_unavailable"},
		{"upstream timeout", "/v1/chat/completions", clientKey, `{"model":"slow"}`, 504, "upstream_timeout"},
		{"upstream not json", "/v1/chat/completions", clientKey, `{"model":"fake-not-json"}`, 502, "upstream_invalid_response"},
		{"upstream json array", "/v1/chat/completions", clientKey, `{"model":"array"}`, 502, "upstream_invalid_response"},
		{"upstream json null", "/v1/chat/completions", clientKey, `{"model":"null"}`, 502, "upstream_invalid_response"},
		{"upstream empty body", "/v1/chat/completions", clientKey, `{"model":"empty"}`, 502, "upstream_invalid_response"},
		{"upstream redirect", "/v1/chat/completions", clientKey, `{"model":"redirect"}`, 502, "upstream_invalid_response"},
		{"upstream body over 32 MiB", "/v1/embeddings", clientKey, `{"model":"over-limit"}`, 502, "upstream_invalid_response"},
	}
	h := newHarness(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := h.fake.Stats()
			r := h.post(context.Background(), tc.path, tc.key, tc.body)
			if r.err != nil {
				t.Fatal(r.err)
			}
			// QF 自己拒绝的请求（不是 upstream_ 开头的错误码）不能发到上游。
			if !strings.HasPrefix(tc.code, "upstream_") {
				if after := h.fake.Stats(); after.ChatCompletions != before.ChatCompletions || after.Embeddings != before.Embeddings {
					t.Fatalf("request reached the upstream: %+v -> %+v", before, after)
				}
			}
			if r.status != tc.status {
				t.Fatalf("status = %d, want %d (body %s)", r.status, tc.status, r.body)
			}
			var e struct {
				Error struct {
					Message, Type, Code string
					Param               *string
				}
			}
			if err := json.Unmarshal(r.body, &e); err != nil {
				t.Fatalf("error body is not JSON: %s", r.body)
			}
			if e.Error.Code != tc.code || e.Error.Message == "" || e.Error.Type == "" || e.Error.Param != nil {
				t.Fatalf("error = %+v, want code %q", e.Error, tc.code)
			}
			if !bytes.Contains(r.body, []byte(`"param":null`)) {
				t.Fatalf("error body has no param:null: %s", r.body)
			}
			if strings.Contains(e.Error.Message, "upstream error") || strings.Contains(e.Error.Message, "original text") {
				t.Fatalf("message leaks upstream text: %q", e.Error.Message)
			}
			if r.header.Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type = %q", r.header.Get("Content-Type"))
			}
			id := requestID(t, r)
			if got := h.logLine(t, id)["error_code"]; got != tc.code {
				t.Fatalf("logged error_code = %v, want %s", got, tc.code)
			}
		})
	}
}

func TestUpstreamRateLimitedKeepsRetryAfter(t *testing.T) {
	h := newHarness(t)
	r := h.chat(t, `{"model":"fake-429"}`)
	if r.status != http.StatusServiceUnavailable || r.header.Get("Retry-After") != "7" {
		t.Fatalf("status = %d, Retry-After = %q", r.status, r.header.Get("Retry-After"))
	}
}

// 同时调同一个上游的请求，连接用完留着复用：20 个并发请求连发两轮，第二轮不再新建连接。
// Go 默认每个主机只留 2 个空闲连接，那样第二轮要新建 18 个，用完再关掉（QF 这边留下 TIME_WAIT）。
func TestUpstreamConnectionsAreReused(t *testing.T) {
	const n = 20
	var opened atomic.Int64
	arrived := make(chan struct{}, n)
	var release atomic.Pointer[chan struct{}] // 每一轮一个新的
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate := *release.Load()
		arrived <- struct{}{}
		<-gate
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	client := newUpstreamClient()
	for round := range 2 {
		gate := make(chan struct{})
		release.Store(&gate)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				resp, err := client.Get(srv.URL)
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			})
		}
		for range n { // 全部在途之后才放行：n 个请求同时占着 n 个连接
			<-arrived
		}
		close(gate)
		wg.Wait()
		if got := opened.Load(); got != n {
			t.Fatalf("after round %d: %d connections opened, want %d", round+1, got, n)
		}
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	h := newHarness(t)
	h.chat(t, `{"model":"redirect"}`)
	if n := h.fake.Stats().ChatCompletions; n != 0 {
		t.Fatalf("redirect was followed: fake upstream got %d requests", n)
	}
}

// 正好 2 MiB 的请求体能过，多 1 字节就是 413（见 TestErrorMapping）。
func TestRequestBodyAtLimit(t *testing.T) {
	prefix, suffix := `{"model":"shared","pad":"`, `"}`
	body := prefix + strings.Repeat("a", 2<<20-len(prefix)-len(suffix)) + suffix
	h := newHarness(t)
	if r := h.chat(t, body); r.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.status)
	}
}

// 正好 32 MiB 的上游正文照常透传，多 1 字节就是 502（见 TestErrorMapping）。
func TestUpstreamBodyAtLimit(t *testing.T) {
	h := newHarness(t)
	r := h.post(context.Background(), "/v1/embeddings", clientKey, `{"model":"at-limit"}`)
	if r.err != nil || r.status != http.StatusOK || len(r.body) != 32<<20 {
		t.Fatalf("status = %d, len = %d", r.status, len(r.body))
	}
}

// 正文是 JSON 对象、只是缺字段：原样透传，用量记 unknown。
func TestMissingFieldsPassThrough(t *testing.T) {
	h := newHarness(t)
	r := h.chat(t, `{"model":"missing-fields"}`)
	if r.status != http.StatusOK || string(r.body) != `{"object":"chat.completion","marker":"RESP-MARKER-缺字段"}` {
		t.Fatalf("status = %d, body = %s", r.status, r.body)
	}
	if got := h.logLine(t, requestID(t, r))["usage"]; got != "unknown" {
		t.Fatalf("usage = %v, want unknown", got)
	}
}

// 超时按端点用途取：同一个慢上游，chat（500ms）超时，embeddings（5s）不超时。
func TestTimeoutPerPurpose(t *testing.T) {
	h := newHarness(t)
	start := time.Now()
	if r := h.chat(t, `{"model":"slow"}`); r.status != http.StatusGatewayTimeout {
		t.Fatalf("chat status = %d, want 504", r.status)
	}
	// 上界不卡：504 本身已经说明没等到慢上游（1s）回答。
	if d := time.Since(start); d < chatTimeout {
		t.Fatalf("chat returned after %v, before the %v timeout", d, chatTimeout)
	}
	r := h.post(context.Background(), "/v1/embeddings", clientKey, `{"model":"slow","input":["a"]}`)
	if r.err != nil || r.status != http.StatusOK {
		t.Fatalf("embeddings status = %d, err = %v", r.status, r.err)
	}
}

// 上游超时后，QF 取消上游请求，假上游看得到取消；也没有重试。
func TestTimeoutCancelsUpstream(t *testing.T) {
	h := newHarness(t)
	if r := h.chat(t, `{"model":"fake-hang"}`); r.status != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", r.status)
	}
	waitFor(t, func() bool { return h.fake.Stats().Canceled == 1 })
	if n := h.fake.Stats().ChatCompletions; n != 1 {
		t.Fatalf("upstream got %d requests, want 1 (no retry)", n)
	}
}

func TestClientDisconnectDoesNotCancelUpstream(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reply, 1)
	go func() { done <- h.post(ctx, "/v1/embeddings", clientKey, `{"model":"emb-hold","input":["a"]}`) }()
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 })
	cancel()
	if r := <-done; r.err == nil {
		t.Fatalf("request finished with status %d after cancel", r.status)
	}
	// 有界结算：请求发出之后不跟着客户端取消。给“跟着取消”的写法一点时间暴露出来（假上游会记一次 canceled），
	// 这一段只是给反例留的窗口，正确与否看下面放开之后的确定事件。
	stays(t, func() bool { return h.fake.Stats().Canceled == 0 && h.fake.Stats().Held == 1 }, 200*time.Millisecond)
	h.fake.Release()
	var line map[string]any
	waitFor(t, func() bool {
		for _, l := range h.logs.lines(t) {
			if l["client_disconnected"] == true {
				line = l
			}
		}
		return line != nil
	})
	if line["outcome"] != "SETTLED" || line["status"] != nil || line["usage"] == "unknown" || h.fake.Stats().Canceled != 0 {
		t.Fatalf("log = %v, canceled %d", line, h.fake.Stats().Canceled)
	}
}

// stays 要求 cond 在 d 之内一直成立：给“不该发生的事”一个发生的窗口。
func stays(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if !cond() {
			t.Fatal("condition stopped holding")
		}
	}
}

func TestNoRetryOnUpstreamError(t *testing.T) {
	h := newHarness(t)
	if r := h.chat(t, `{"model":"fake-500"}`); r.status != http.StatusBadGateway {
		t.Fatalf("status = %d", r.status)
	}
	if n := h.fake.Stats().ChatCompletions; n != 1 {
		t.Fatalf("upstream got %d requests, want 1", n)
	}
}

func TestConcurrentForwarding(t *testing.T) {
	const n = 20
	h := newHarness(t)
	start := make(chan struct{})
	replies := make([]reply, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// 每个请求的内容长度不同，假上游按字符数算 prompt_tokens，借此确认拿到的是自己的响应。
			body := fmt.Sprintf(`{"model":"shared","messages":[{"role":"user","content":%q}]}`, strings.Repeat("x", i+1))
			replies[i] = h.post(context.Background(), "/v1/chat/completions", clientKey, body)
		}()
	}
	close(start)
	wg.Wait()

	ids := map[string]bool{}
	for i, r := range replies {
		if r.err != nil || r.status != http.StatusOK {
			t.Fatalf("request %d: status = %d, err = %v", i, r.status, r.err)
		}
		var resp struct {
			Usage struct {
				PromptTokens int `json:"prompt_tokens"`
			}
		}
		if err := json.Unmarshal(r.body, &resp); err != nil || resp.Usage.PromptTokens != i+1 {
			t.Fatalf("request %d got someone else's response: %s", i, r.body)
		}
		ids[requestID(t, r)] = true
		h.logLine(t, r.header.Get("x-request-id"))
	}
	if len(ids) != n {
		t.Fatalf("%d distinct request ids, want %d", len(ids), n)
	}
	if got := len(h.logs.lines(t)); got != n {
		t.Fatalf("%d log lines, want %d", got, n)
	}
	if got := h.fake.Stats().ChatCompletions; got != n {
		t.Fatalf("upstream got %d requests, want %d", got, n)
	}
}

func TestCallLog(t *testing.T) {
	h := newHarness(t)
	runID := "run-0b7f6d3e-2f7c-4c1e-9a55-1f3e5c8d9a10"
	clientReqID := "0b7f6d3e-2f7c-4c1e-9a55-1f3e5c8d9a10"
	r := h.chat(t, withModel(toolsBody, `"shared"`), "X-Run-Id", runID, "X-Client-Request-Id", clientReqID)
	l := h.logLine(t, requestID(t, r))
	want := map[string]any{
		"msg":                   "call",
		"key_id":                float64(1),
		"account":               "acct-full",
		"endpoint":              "chat",
		"model_alias":           "shared",
		"upstream":              "fake",
		"upstream_status":       float64(200),
		"client_request_id":     clientReqID,
		"run_id":                runID,
		"upstream_x_request_id": "fake-chat-1",
		"upstream_response_id":  "fakecmpl-1",
		"upstream_model":        "fake-chat" + fakeupstream.ModelVersionSuffix,
	}
	for k, v := range want {
		if l[k] != v {
			t.Errorf("log %s = %v, want %v", k, l[k], v)
		}
	}
	if _, ok := l["duration_ms"].(float64); !ok {
		t.Errorf("log duration_ms = %v", l["duration_ms"])
	}
	if _, ok := l["error_code"]; ok {
		t.Errorf("successful call logged error_code %v", l["error_code"])
	}
	var resp struct{ Usage map[string]any }
	if err := json.Unmarshal(r.body, &resp); err != nil {
		t.Fatal(err)
	}
	u, _ := l["usage"].(map[string]any)
	for _, f := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
		if u[f] != resp.Usage[f] {
			t.Errorf("log usage.%s = %v, upstream said %v", f, u[f], resp.Usage[f])
		}
	}
}

func TestCallLogUsage(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name, path, body string
		want             any
	}{
		{"chat without usage", "/v1/chat/completions", `{"model":"fake-no-usage"}`, "unknown"},
		{"embeddings without usage", "/v1/embeddings", `{"model":"emb-no-usage","input":["a"]}`, "unknown"},
		{"upstream not json", "/v1/chat/completions", `{"model":"fake-not-json"}`, "unknown"},
		{"upstream error", "/v1/chat/completions", `{"model":"fake-500"}`, "unknown"},
		{"chat usage without completion_tokens", "/v1/chat/completions", `{"model":"no-completion"}`, "unknown"},
		{"embeddings", "/v1/embeddings", `{"model":"shared","input":["一二三","ab"]}`, map[string]any{"prompt_tokens": float64(5), "total_tokens": float64(5)}},
		{"not sent upstream", "/v1/chat/completions", `{"model":"gpt-x"}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := h.post(context.Background(), tc.path, clientKey, tc.body)
			if r.err != nil {
				t.Fatal(r.err)
			}
			got := h.logLine(t, requestID(t, r))["usage"]
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("usage = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCallLogRejectsBadClientHeaders(t *testing.T) {
	h := newHarness(t)
	long := "run-" + strings.Repeat("a", 125) // 129 个字符
	r := h.chat(t, `{"model":"shared"}`, "X-Run-Id", long, "X-Client-Request-Id", "bad\tvalue")
	if r.status != http.StatusOK {
		t.Fatalf("status = %d: bad client headers must not fail the request", r.status)
	}
	l := h.logLine(t, requestID(t, r))
	if _, ok := l["run_id"]; ok || l["run_id_invalid"] != true {
		t.Fatalf("run_id = %v, run_id_invalid = %v", l["run_id"], l["run_id_invalid"])
	}
	if _, ok := l["client_request_id"]; ok || l["client_request_id_invalid"] != true {
		t.Fatalf("client_request_id = %v, invalid = %v", l["client_request_id"], l["client_request_id_invalid"])
	}
}

// 各种调用之后，日志里搜不到客户端 Key、上游 Key、请求正文和响应正文。
func TestCallLogHasNoSecretsOrBodies(t *testing.T) {
	h := newHarness(t)
	h.chat(t, withModel(toolsBody, `"shared"`))
	h.chat(t, withModel(toolsBody, `"fake-tool-call"`))
	h.chat(t, `{"model":"missing-fields"}`)
	h.chat(t, `{"model":"fake-500","messages":[{"content":"REQ-MARKER-请求正文"}]}`)
	h.post(context.Background(), "/v1/chat/completions", "qfk_wrong-"+clientKey, `{"model":"shared"}`)
	h.post(context.Background(), "/v1/embeddings", clientKey, `{"model":"shared","input":["REQ-MARKER-请求正文"]}`)
	waitFor(t, func() bool { return len(h.logs.lines(t)) == 6 })
	logs := h.logs.String()
	for _, secret := range []string{clientKey, upstreamKey, scriptedKey, "REQ-MARKER", "RESP-MARKER", "fake reply", "查询_lookup", "embedding\""} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs contain %q", secret)
		}
	}
}

func TestLoggable(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"run-0b7f6d3e-2f7c-4c1e-9a55-1f3e5c8d9a10", true},
		{strings.Repeat("中", 128), true},
		{strings.Repeat("中", 129), false},
		{strings.Repeat("a", 129), false},
		{"a\x01b", false},
		{"a\x7fb", false},
		{"a\nb", false},
		{"a\u0085b", false},
		{"\xff\xfe", false},
	}
	for _, tc := range cases {
		if got := loggable(tc.s); got != tc.want {
			t.Errorf("loggable(%q) = %v, want %v", tc.s, got, tc.want)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 3s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 客户端已经断开：不写任何响应（状态码、正文都不写），只在日志里记 client_canceled。
func TestClientCanceledWritesNothing(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"shared"}`))
	req.Header.Set("Authorization", "Bearer "+clientKey)
	rec := httptest.NewRecorder()
	h.qf.Config.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Fatalf("wrote a response after the client left: %d %q", rec.Code, rec.Body)
	}
	l := h.logLine(t, rec.Header().Get("x-request-id"))
	if l["error_code"] != "client_canceled" {
		t.Fatalf("error_code = %v", l["error_code"])
	}
	if st, ok := l["status"]; ok {
		t.Fatalf("status = %v, want no status: nothing was written back", st)
	}
}

// 日志的 status 是写回客户端的状态：成功、QF 自己拒绝、上游映射来的错误都要记，而且和响应一致。
func TestCallLogStatus(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name, path, key, body string
		want                  int
	}{
		{"success", "/v1/chat/completions", clientKey, `{"model":"shared"}`, 200},
		{"embeddings success", "/v1/embeddings", clientKey, `{"model":"shared","input":["a"]}`, 200},
		{"upstream 201 passes through", "/v1/chat/completions", clientKey, `{"model":"status-201"}`, 201},
		{"invalid key", "/v1/chat/completions", "qfk_wrong", `{"model":"shared"}`, 401},
		{"model not found", "/v1/chat/completions", clientKey, `{"model":"gpt-x"}`, 404},
		{"upstream 500", "/v1/chat/completions", clientKey, `{"model":"fake-500"}`, 502},
		{"upstream 429", "/v1/chat/completions", clientKey, `{"model":"fake-429"}`, 503},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := h.post(context.Background(), tc.path, tc.key, tc.body)
			if r.err != nil || r.status != tc.want {
				t.Fatalf("response = %d %v, want %d", r.status, r.err, tc.want)
			}
			if got := h.logLine(t, requestID(t, r))["status"]; got != float64(tc.want) {
				t.Fatalf("log status = %v, want %d", got, tc.want)
			}
		})
	}
}

// 方案必须是 Bearer（不区分大小写）：合法的 Key 配别的方案也是 401。
func TestAuthRequiresBearerScheme(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		name, scheme string // 失败信息只打 name，不打头的值（里面有 Key）
		status       int
	}{
		{"basic", "Basic ", http.StatusUnauthorized},
		{"token", "Token ", http.StatusUnauthorized},
		{"no scheme", "", http.StatusUnauthorized},
		{"lowercase bearer", "bearer ", http.StatusOK},
		{"uppercase bearer", "BEARER ", http.StatusOK},
	} {
		r := h.post(context.Background(), "/v1/chat/completions", "", `{"model":"shared"}`, "Authorization", tc.scheme+clientKey)
		if r.err != nil || r.status != tc.status {
			t.Fatalf("%s: status = %d, want %d", tc.name, r.status, tc.status)
		}
		requestID(t, r)
	}
}

// Key 只允许 shared：请求里另带一个大小写变体的 MODEL，不能借它调到白名单之外的上游模型。
func TestCaseVariantCannotBypassModelAllowList(t *testing.T) {
	h := newHarness(t)
	if r := h.post(context.Background(), "/v1/chat/completions", limitedKey, `{"model":"fake-tool-call"}`); r.status != http.StatusForbidden {
		t.Fatalf("baseline: status = %d, want 403", r.status)
	}
	r := h.post(context.Background(), "/v1/chat/completions", limitedKey, `{"model":"shared","MODEL":"fake-tool-call"}`)
	if r.err != nil || r.status != http.StatusBadRequest || !bytes.Contains(r.body, []byte(`"code":"invalid_request"`)) {
		t.Fatalf("status = %d, body = %s, want 400 invalid_request", r.status, r.body)
	}
	if n := h.fake.Stats().ChatCompletions; n != 0 {
		t.Fatalf("upstream got %d requests, want 0", n)
	}
}

// 客户端传来的 X-Request-Id 不能当成调用号：调用号只由 QF 生成。
func TestClientCannotChooseRequestID(t *testing.T) {
	h := newHarness(t)
	const chosen = "qf-CLIENTCHOSENREQUESTID000"
	r := h.chat(t, `{"model":"shared"}`, "X-Request-Id", chosen)
	id := requestID(t, r)
	if id == chosen {
		t.Fatal("x-request-id is the client's value")
	}
	if got := h.logLine(t, id)["request_id"]; got != id {
		t.Fatalf("logged request_id = %v, want %s", got, id)
	}
}

// 不转发上游的其它响应头：成功和失败的响应里都没有上游的标记头，x-request-id 是 QF 的。
func TestUpstreamHeadersAreNotForwarded(t *testing.T) {
	h := newHarness(t)
	for _, body := range []string{`{"model":"shared"}`, `{"model":"fake-500"}`} {
		r := h.chat(t, body)
		if v := r.header.Get("X-Upstream-Marker"); v != "" {
			t.Fatalf("%s: upstream header forwarded: %q", body, v)
		}
		requestID(t, r) // 上游自己的 x-request-id（fake-chat-N）也不能盖掉 QF 的
	}
}

// 上游 2xx 没给 Content-Type 时，QF 的响应也不带 Content-Type，不让 net/http 嗅探。
func TestNoContentTypeIsNotSniffed(t *testing.T) {
	h := newHarness(t)
	r := h.chat(t, `{"model":"no-content-type"}`)
	if r.status != http.StatusOK || string(r.body) != `{"id":"no-ct"}` {
		t.Fatalf("status = %d, body = %s", r.status, r.body)
	}
	if v, ok := r.header["Content-Type"]; ok {
		t.Fatalf("Content-Type = %q, want none", v)
	}
}
