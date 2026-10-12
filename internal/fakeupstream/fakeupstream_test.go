package fakeupstream

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func post(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body is not JSON: %s", rec.Body)
	}
	return m
}

func TestChat(t *testing.T) {
	s := New()
	rec := post(t, s, "/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"你好ab"}]}`)
	m := decode(t, rec)
	if rec.Code != 200 || m["model"] != "m"+ModelVersionSuffix || m["id"] == "" || rec.Header().Get("x-request-id") == "" {
		t.Fatalf("code %d, body %s", rec.Code, rec.Body)
	}
	c := m["choices"].([]any)[0].(map[string]any)
	if c["finish_reason"] != "stop" || c["message"].(map[string]any)["content"] != "fake reply" {
		t.Fatalf("choice = %v", c)
	}
	u := m["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(4) || u["completion_tokens"] != float64(10) || u["total_tokens"] != float64(14) || u["fake"] != true {
		t.Fatalf("usage = %v", u)
	}
}

func TestChatToolCall(t *testing.T) {
	m := decode(t, post(t, New(), "/v1/chat/completions", `{"model":"fake-tool-call","tools":[{"type":"function","function":{"name":"search"}}]}`))
	c := m["choices"].([]any)[0].(map[string]any)
	call := c["message"].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)
	if c["finish_reason"] != "tool_calls" || call["id"] == "" || call["function"].(map[string]any)["name"] != "search" {
		t.Fatalf("choice = %v", c)
	}
}

func TestScenarios(t *testing.T) {
	cases := []struct {
		model string
		code  int
	}{
		{"fake-500", 500}, {"fake-429", 429}, {"fake-401", 401}, {"fake-slow-x", 400},
	}
	for _, tc := range cases {
		for _, path := range []string{"/v1/chat/completions", "/v1/embeddings"} {
			rec := post(t, New(), path, `{"model":"`+tc.model+`","input":"a"}`)
			if rec.Code != tc.code {
				t.Errorf("%s %s: code %d, want %d", path, tc.model, rec.Code, tc.code)
			}
		}
	}
	if rec := post(t, New(), "/v1/chat/completions", `{"model":"fake-429"}`); rec.Header().Get("Retry-After") != "7" {
		t.Error("fake-429 has no Retry-After")
	}
	rec := post(t, New(), "/v1/chat/completions", `{"model":"fake-not-json"}`)
	if rec.Code != 200 || json.Valid(rec.Body.Bytes()) {
		t.Errorf("fake-not-json: code %d, body %s", rec.Code, rec.Body)
	}
	if _, ok := decode(t, post(t, New(), "/v1/chat/completions", `{"model":"fake-no-usage"}`))["usage"]; ok {
		t.Error("fake-no-usage has usage")
	}
	start := time.Now()
	if rec := post(t, New(), "/v1/chat/completions", `{"model":"fake-slow-50"}`); rec.Code != 200 || time.Since(start) < 50*time.Millisecond {
		t.Errorf("fake-slow-50: code %d after %v", rec.Code, time.Since(start))
	}
}

func TestHangCountsCancel(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"fake-hang"}`)).WithContext(ctx))
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if got := s.Stats(); got.ChatCompletions != 1 || got.Canceled != 1 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestEmbeddings(t *testing.T) {
	s := New()
	m := decode(t, post(t, s, "/v1/embeddings", `{"model":"e","input":["一二","abc"],"dimensions":5}`))
	data := m["data"].([]any)
	if len(data) != 2 || m["id"] == "" || m["model"] != "e"+ModelVersionSuffix {
		t.Fatalf("body = %v", m)
	}
	for i, d := range data {
		d := d.(map[string]any)
		vec := d["embedding"].([]any)
		if d["index"] != float64(i) || len(vec) != 5 {
			t.Fatalf("data[%d] = %v", i, d)
		}
	}
	u := m["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(5) || u["total_tokens"] != float64(5) || u["fake"] != true {
		t.Fatalf("usage = %v", u)
	}
	// 确定性：同样的输入得到同样的向量，不同输入不同。
	again := decode(t, post(t, s, "/v1/embeddings", `{"model":"e","input":"一二","dimensions":5}`))
	if v1, v2 := data[0].(map[string]any)["embedding"], again["data"].([]any)[0].(map[string]any)["embedding"]; !equal(v1, v2) || equal(v1, data[1].(map[string]any)["embedding"]) {
		t.Fatal("vectors are not deterministic per input")
	}
	def := decode(t, post(t, s, "/v1/embeddings", `{"model":"e","input":"x"}`))
	if n := len(def["data"].([]any)[0].(map[string]any)["embedding"].([]any)); n != defaultDimensions {
		t.Fatalf("default dimensions = %d", n)
	}
	for _, x := range Vector("x", 64) {
		if x < -1 || x > 1 {
			t.Fatalf("component %v out of [-1, 1]", x)
		}
	}
}

func equal(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestStats(t *testing.T) {
	s := New()
	post(t, s, "/v1/chat/completions", `{"model":"m"}`)
	post(t, s, "/v1/chat/completions", `{"model":"fake-500"}`)
	post(t, s, "/v1/embeddings", `{"model":"m","input":"a"}`)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_fake/stats", nil))
	var got Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != (Stats{ChatCompletions: 2, Embeddings: 1}) {
		t.Fatalf("stats = %+v", got)
	}
}

func TestOveruse(t *testing.T) {
	s := New()
	u := decode(t, post(t, s, "/v1/chat/completions", `{"model":"fake-overuse","messages":[{"content":"ab"}]}`))["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(OveruseTokens) || u["total_tokens"] != float64(OveruseTokens+10) {
		t.Fatalf("chat usage = %v", u)
	}
	u = decode(t, post(t, s, "/v1/embeddings", `{"model":"fake-overuse","input":"ab"}`))["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(OveruseTokens) || u["total_tokens"] != float64(OveruseTokens) {
		t.Fatalf("embeddings usage = %v", u)
	}
}

// fake-hold 挂住，直到 Release（或 POST /_fake/release）放开当时挂着的全部；之后来的等下一次放开。被取消的计数。
func TestHold(t *testing.T) {
	s := New()
	srv := httptest.NewServer(s)
	defer srv.Close()
	send := func(ctx context.Context) <-chan int {
		done := make(chan int, 1)
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"fake-hold"}`))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- 0
				return
			}
			resp.Body.Close()
			done <- resp.StatusCode
		}()
		return done
	}
	waitHeld := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for s.Stats().Held != n {
			if time.Now().After(deadline) {
				t.Fatalf("held = %d, want %d", s.Stats().Held, n)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	a, b := send(context.Background()), send(context.Background())
	waitHeld(2)
	s.Release()
	if <-a != 200 || <-b != 200 {
		t.Fatal("released requests did not get 200")
	}
	waitHeld(0)

	c := send(context.Background())
	waitHeld(1)
	resp, err := http.Post(srv.URL+"/_fake/release", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /_fake/release: %v %v", resp, err)
	}
	resp.Body.Close()
	if <-c != 200 {
		t.Fatal("request released over HTTP did not get 200")
	}

	ctx, cancel := context.WithCancel(context.Background())
	d := send(ctx)
	waitHeld(1)
	cancel()
	<-d
	waitHeld(0)
	if s.Stats().Canceled != 1 {
		t.Fatalf("canceled = %d", s.Stats().Canceled)
	}
}

// billed 发一个带调用号的请求；ctx 被取消时返回（挂住的场景）。
func billed(s *Server, ctx context.Context, id, path, body string) {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	if id != "" {
		req.Header.Set("X-Client-Request-Id", id)
	}
	s.ServeHTTP(httptest.NewRecorder(), req)
}

// 账单：每个调用号恰好一行，按 QF 的账单格式（ledger.ReadBill 能读）。回答了的记 yes 和用量（包括不把用量放进响应的
// fake-no-usage），错误、不是 JSON、被取消的记 no；fake-billed-hang 收到就记 yes，被取消时不再记；没有调用号的不记。
func TestBill(t *testing.T) {
	s := New()
	ctx := context.Background()
	id := func(n int) string { return "qf-" + strings.Repeat(string(rune('A'+n)), 26) }
	chat := "/v1/chat/completions"
	billed(s, ctx, id(0), chat, `{"model":"fake-chat","messages":[{"role":"user","content":"hello"}]}`)
	billed(s, ctx, id(1), "/v1/embeddings", `{"model":"fake-embed","input":["abc","de"]}`)
	billed(s, ctx, id(2), chat, `{"model":"fake-no-usage","messages":[{"role":"user","content":"abc"}]}`)
	billed(s, ctx, id(3), chat, `{"model":"fake-500"}`)
	billed(s, ctx, id(4), chat, `{"model":"fake-not-json"}`)
	billed(s, ctx, id(0), chat, `{"model":"fake-500"}`) // 同一个调用号再来：只记第一次
	billed(s, ctx, "", chat, `{"model":"fake-chat"}`)   // 没有调用号：不记
	for n, model := range map[int]string{5: "fake-hang", 6: "fake-billed-hang"} {
		hctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		calls := s.Stats().ChatCompletions
		go func() {
			billed(s, hctx, id(n), chat, `{"model":"`+model+`","messages":[{"role":"user","content":"xy"}]}`)
			close(done)
		}()
		for s.Stats().ChatCompletions == calls { // 等它进到处理里再取消
			time.Sleep(time.Millisecond)
		}
		cancel()
		<-done
	}
	proofs, err := ledger.ReadBill(bytes.NewReader(s.Bill()))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, p := range proofs {
		got[p.CallID] = p.String()
	}
	want := map[string]string{
		id(0): "bill: prompt=5 completion=10 total=15",
		id(1): "bill: prompt=5 total=5",
		id(2): "bill: prompt=3 completion=10 total=13",
		id(3): "bill: not consumed",
		id(4): "bill: not consumed",
		id(5): "bill: not consumed",
		id(6): "bill: prompt=2 completion=10 total=12",
	}
	if len(proofs) != len(want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("bill = %v, want %v", got, want)
	}
	if !slices.Equal(billHeader, ledger.BillHeader) {
		t.Fatalf("header %v differs from QF's %v", billHeader, ledger.BillHeader)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_fake/bill", nil))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "text/csv" || !bytes.Equal(rec.Body.Bytes(), s.Bill()) {
		t.Fatalf("GET /_fake/bill = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
}
