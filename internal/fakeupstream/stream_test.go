package fakeupstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// openStream 经真的 HTTP 发一个流式请求（带调用号），返回响应。
func openStream(t *testing.T, srv *httptest.Server, ctx context.Context, id, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("X-Client-Request-Id", id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// dataLines 读完整个流，返回每个事件的 data（不含前缀）。
func dataLines(t *testing.T, r io.Reader) ([]string, error) {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 4<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			out = append(out, v)
		}
	}
	return out, sc.Err()
}

// 流式回答的形状照百炼：role 块、10 个内容块、finish 块；要了 usage 时每块带 "usage":null，末尾多一个 choices 为空、
// 带 usage 的块；最后 [DONE]。账单按实际生成的记，和 usage 块里的数相同。
func TestStream(t *testing.T) {
	s := New()
	srv := httptest.NewServer(s)
	defer srv.Close()
	resp := openStream(t, srv, context.Background(), "qf-AAAAAAAAAAAAAAAAAAAAAAAAAA",
		`{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"你好ab"}]}`)
	data, err := dataLines(t, resp.Body)
	if err != nil || resp.Header.Get("Content-Type") != "text/event-stream; charset=utf-8" || len(data) != 14 || data[13] != "[DONE]" {
		t.Fatalf("err %v, content type %q, data %q", err, resp.Header.Get("Content-Type"), data)
	}
	var usageChunk struct {
		Choices []any
		Usage   map[string]any
		Model   string
	}
	if err := json.Unmarshal([]byte(data[12]), &usageChunk); err != nil || usageChunk.Choices == nil || len(usageChunk.Choices) != 0 ||
		usageChunk.Usage["prompt_tokens"] != float64(4) || usageChunk.Usage["completion_tokens"] != float64(10) || usageChunk.Model != "m"+ModelVersionSuffix {
		t.Fatalf("usage chunk %s", data[12])
	}
	for _, d := range data[:12] {
		if !strings.Contains(d, `"usage":null`) || !strings.Contains(d, `"object":"chat.completion.chunk"`) {
			t.Fatalf("chunk %s", d)
		}
	}
	if !strings.Contains(data[1], "fake reply 1") || !strings.Contains(data[11], `"finish_reason":"stop"`) {
		t.Fatalf("chunks %q", data)
	}
	if st := s.Stats(); st.StreamEvents != 14 || st.ChatCompletions != 1 {
		t.Fatalf("stats %+v", st)
	}
	proofs, err := ledger.ReadBill(bytes.NewReader(s.Bill()))
	if err != nil || len(proofs) != 1 || proofs[0].String() != "bill: prompt=4 completion=10 total=14" {
		t.Fatalf("bill %v %v", proofs, err)
	}
}

// 不要 usage 的、fake-no-usage 的流没有 usage 块，块里也没有 usage 字段；场景照模型名。
func TestStreamScenarios(t *testing.T) {
	s := New()
	srv := httptest.NewServer(s)
	defer srv.Close()
	cases := map[string]func(data []string, err error) bool{
		`{"model":"m","stream":true}`: func(d []string, err error) bool {
			return err == nil && len(d) == 13 && !strings.Contains(strings.Join(d, ""), "usage")
		},
		`{"model":"fake-no-usage","stream":true,"stream_options":{"include_usage":true}}`: func(d []string, err error) bool {
			return err == nil && len(d) == 13 && !strings.Contains(strings.Join(d, ""), `"usage":{`)
		},
		`{"model":"fake-stream-no-done","stream":true,"stream_options":{"include_usage":true}}`: func(d []string, err error) bool {
			return err == nil && len(d) == 13 && strings.Contains(d[12], `"usage":{`)
		},
		`{"model":"fake-stream-error","stream":true}`: func(d []string, err error) bool {
			return err == nil && len(d) == 4 && strings.Contains(d[2], `"error":`) && d[3] == "[DONE]"
		},
		`{"model":"fake-stream-cut","stream":true}`: func(d []string, err error) bool {
			return err != nil && len(d) == 2 // 连接断开，没有分块编码的结尾
		},
		`{"model":"fake-stream-huge","stream":true}`: func(d []string, err error) bool {
			return err == nil && len(d) == 13 && len(d[1]) > 1<<20
		},
		`{"model":"fake-stream-big","stream":true}`: func(d []string, err error) bool {
			return err == nil && len(d) == 67 && len(d[5]) > 256<<10
		},
		`{"model":"fake-stream-bytewise","stream":true}`: func(d []string, err error) bool {
			return err == nil && len(d) == 13 && d[12] == "[DONE]"
		},
		`{"model":"fake-500","stream":true}`: func(d []string, err error) bool { return len(d) == 0 },
	}
	for body, ok := range cases {
		resp := openStream(t, srv, context.Background(), "", body)
		if d, err := dataLines(t, resp.Body); !ok(d, err) {
			t.Errorf("%s: %d events, err %v", body, len(d), err)
		}
	}
}

// fake-stream-step 每个事件之前等一次 Release；调用方中途走了，账单只记已经生成的内容块。
func TestStreamStepAndCancel(t *testing.T) {
	s := New()
	srv := httptest.NewServer(s)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 后进先出，在 srv.Close 之前跑：中途 t.Fatalf 时 handler 照样能结束，不卡住整个包
	resp := openStream(t, srv, ctx, "qf-BBBBBBBBBBBBBBBBBBBBBBBBBB", `{"model":"fake-stream-step","stream":true,"messages":[{"content":"ab"}]}`)
	r := bufio.NewReader(resp.Body)
	for n := int64(1); n <= 3; n++ {
		waitHeld(t, s, 1)
		s.Release()
		if line, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data: ") {
			t.Fatalf("event %d: %q %v", n, line, err)
		}
		_, _ = r.ReadString('\n') // 空行
		if got := s.Stats().StreamEvents; got != n {
			t.Fatalf("read %d events, sent %d", n, got)
		}
	}
	waitHeld(t, s, 1)
	cancel()
	waitHeld(t, s, 0)
	for range 3000 {
		if s.Stats().Canceled == 1 && len(s.Bill()) > len("call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	proofs, err := ledger.ReadBill(bytes.NewReader(s.Bill()))
	if err != nil || len(proofs) != 1 || proofs[0].String() != "bill: prompt=2 completion=2 total=4" {
		t.Fatalf("bill %v %v", proofs, err)
	}
}

func waitHeld(t *testing.T, s *Server, n int64) {
	t.Helper()
	for range 3000 {
		if s.Stats().Held == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("held %d, want %d", s.Stats().Held, n)
}
