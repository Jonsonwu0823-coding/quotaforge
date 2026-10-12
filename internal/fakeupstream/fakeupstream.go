// Package fakeupstream 是一个 OpenAI 兼容的假上游，给测试和本机联调用，不调真实模型。
// 行为按请求里的模型名选（QF 不转发客户端的头）：
//
//	fake-tool-call   返回一个 tool_calls，finish_reason=tool_calls
//	fake-no-usage    200，没有 usage
//	fake-slow-<毫秒>  延迟后再正常回答
//	fake-hang        一直不回，直到请求被取消
//	fake-500、fake-429（带 Retry-After: 7）、fake-401   对应状态的错误
//	fake-not-json    200，正文不是 JSON
//	fake-overuse     正常回答，但 usage 的 prompt_tokens 是 OveruseTokens（远超任何请求能预留的量）
//	fake-hold        挂住，直到 POST /_fake/release（或 Release）放开，再正常回答；被取消就不回
//	fake-billed-hang 收到就按正常回答的用量记账，然后一直不回，直到请求被取消：上游照样计费，调用方却拿不到结果
//	其它任意名        正常回答
//
// usage 按字符数算，并带 "fake": true 标明是假的数。GET /_fake/stats 返回计数和当前挂住的请求数。响应里的 model 是请求的名字加上版本后缀，
// 模拟上游返回带版本号的实际模型名。
//
// 账单：带 X-Client-Request-Id（QF 的调用号）的请求，每个调用号恰好记一行，是假上游自己的“真相”。
// 回答了的（包括 fake-no-usage：照算用量，只是不放进响应）记 yes 和用量；回错误状态的、fake-not-json、
// 被取消的记 no；fake-billed-hang 在收到时就记 yes。GET /_fake/bill 按 QF 的账单格式（ledger.BillHeader）导出 CSV。
package fakeupstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// ModelVersionSuffix 加在请求的模型名后面，作为响应里的实际模型名。
const ModelVersionSuffix = "-20260101"

const defaultDimensions = 8

// OveruseTokens 是 fake-overuse 报的 prompt_tokens。
const OveruseTokens = 1_000_000

// Stats 是收到的请求数、在处理中途被取消的请求数，以及当前被 fake-hold 挂住的请求数。
type Stats struct {
	ChatCompletions int64 `json:"chat_completions"`
	Embeddings      int64 `json:"embeddings"`
	Canceled        int64 `json:"canceled"`
	Held            int64 `json:"held"`
}

type Server struct {
	mux                              *http.ServeMux
	chat, embeddings, canceled, held atomic.Int64

	mu      sync.Mutex
	release chan struct{} // fake-hold 等它关闭；Release 关掉它并换一个新的
	bill    [][]string    // 账单的行，按记账的先后
	billed  map[string]bool
}

func New() *Server {
	s := &Server{mux: http.NewServeMux(), release: make(chan struct{}), billed: map[string]bool{}}
	s.mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	s.mux.HandleFunc("POST /v1/embeddings", s.handleEmbeddings)
	s.mux.HandleFunc("GET /_fake/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.Stats())
	})
	s.mux.HandleFunc("POST /_fake/release", func(w http.ResponseWriter, _ *http.Request) {
		s.Release()
		w.WriteHeader(http.StatusNoContent)
	})
	s.mux.HandleFunc("GET /_fake/bill", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(s.Bill())
	})
	return s
}

// billHeader 是 QF 的账单格式的表头，和 ledger.BillHeader 相同（测试核对）；假上游不依赖账本的包。
var billHeader = []string{"call_id", "consumed", "prompt_tokens", "completion_tokens", "total_tokens"}

// Bill 按 QF 的账单格式导出到现在为止的账单。
func (s *Server) Bill() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	_ = w.Write(billHeader)
	_ = w.WriteAll(s.bill) // 写到内存，不会失败
	return b.Bytes()
}

// usage 是一次回答的用量。嵌入没有 completion_tokens。
type usage struct {
	prompt, completion int
	chat               bool
}

func (u usage) total() int { return u.prompt + u.completion }

// charge 给这个调用号记一行账：u 为 nil 表示没有消费。没有调用号的不记；同一个调用号只记第一次。
func (s *Server) charge(r *http.Request, u *usage) {
	id := r.Header.Get("X-Client-Request-Id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" || s.billed[id] {
		return
	}
	s.billed[id] = true
	row := []string{id, "no", "", "", ""}
	if u != nil {
		row = []string{id, "yes", strconv.Itoa(u.prompt), "", strconv.Itoa(u.total())}
		if u.chat {
			row[3] = strconv.Itoa(u.completion)
		}
	}
	s.bill = append(s.bill, row)
}

// serve 按场景处理一个已经解析好的请求，并记账：fake-billed-hang 先记 yes 再挂住；错误场景和被取消的记 no；
// 其余记 yes 之后由 answer 正常回答。
func (s *Server) serve(w http.ResponseWriter, r *http.Request, model string, u usage, answer func()) {
	switch {
	case model == "fake-billed-hang":
		s.charge(r, &u)
		<-r.Context().Done()
		s.canceled.Add(1)
	case s.scenario(w, r, model):
		s.charge(r, nil)
	default:
		s.charge(r, &u)
		answer()
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) Stats() Stats {
	return Stats{ChatCompletions: s.chat.Load(), Embeddings: s.embeddings.Load(), Canceled: s.canceled.Load(), Held: s.held.Load()}
}

// Release 放开当前被 fake-hold 挂住的全部请求；之后来的 fake-hold 请求等下一次 Release。
func (s *Server) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.release)
	s.release = make(chan struct{})
}

type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Tools []struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tools"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	n := s.chat.Add(1)
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Model == "" {
		s.charge(r, nil)
		writeFakeError(w, http.StatusBadRequest)
		return
	}
	prompt := 0
	for _, m := range req.Messages {
		var text string
		if json.Unmarshal(m.Content, &text) != nil {
			text = string(m.Content) // 多段内容：按原始 JSON 的字符数算
		}
		prompt += utf8.RuneCountInString(text)
	}
	if req.Model == "fake-overuse" {
		prompt = OveruseTokens
	}
	u := usage{prompt: prompt, completion: 10, chat: true} // 固定规则：每次回答记 10 个
	s.serve(w, r, req.Model, u, func() {
		message := map[string]any{"role": "assistant", "content": "fake reply"}
		finish := "stop"
		if req.Model == "fake-tool-call" {
			name := "lookup"
			if len(req.Tools) > 0 && req.Tools[0].Function.Name != "" {
				name = req.Tools[0].Function.Name
			}
			message = map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
				"id": "call_fake_1", "type": "function",
				"function": map[string]any{"name": name, "arguments": "{}"},
			}}}
			finish = "tool_calls"
		}
		resp := map[string]any{
			"id":      fmt.Sprintf("fakecmpl-%d", n),
			"object":  "chat.completion",
			"created": 1767225600,
			"model":   req.Model + ModelVersionSuffix,
			"choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}},
		}
		if req.Model != "fake-no-usage" {
			resp["usage"] = map[string]any{"prompt_tokens": u.prompt, "completion_tokens": u.completion, "total_tokens": u.total(), "fake": true}
		}
		w.Header().Set("x-request-id", fmt.Sprintf("fake-chat-%d", n))
		writeJSON(w, http.StatusOK, resp)
	})
}

type embeddingsRequest struct {
	Model      string          `json:"model"`
	Input      json.RawMessage `json:"input"`
	Dimensions int             `json:"dimensions"`
}

func (s *Server) handleEmbeddings(w http.ResponseWriter, r *http.Request) {
	n := s.embeddings.Add(1)
	var req embeddingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Model == "" || req.Dimensions < 0 {
		s.charge(r, nil)
		writeFakeError(w, http.StatusBadRequest)
		return
	}
	var inputs []string
	if json.Unmarshal(req.Input, &inputs) != nil {
		var one string
		if json.Unmarshal(req.Input, &one) != nil {
			s.charge(r, nil)
			writeFakeError(w, http.StatusBadRequest)
			return
		}
		inputs = []string{one}
	}
	prompt := 0
	for _, in := range inputs {
		prompt += utf8.RuneCountInString(in)
	}
	if req.Model == "fake-overuse" {
		prompt = OveruseTokens
	}
	u := usage{prompt: prompt}
	s.serve(w, r, req.Model, u, func() {
		dims := req.Dimensions
		if dims == 0 {
			dims = defaultDimensions
		}
		data := make([]any, len(inputs))
		for i, in := range inputs {
			data[i] = map[string]any{"object": "embedding", "index": i, "embedding": Vector(in, dims)}
		}
		resp := map[string]any{
			"id":     fmt.Sprintf("fakeemb-%d", n),
			"object": "list",
			"model":  req.Model + ModelVersionSuffix,
			"data":   data,
		}
		if req.Model != "fake-no-usage" {
			resp["usage"] = map[string]any{"prompt_tokens": u.prompt, "total_tokens": u.total(), "fake": true}
		}
		w.Header().Set("x-request-id", fmt.Sprintf("fake-emb-%d", n))
		writeJSON(w, http.StatusOK, resp)
	})
}

// Vector 是输入文本确定的嵌入向量，每一维在 [-1, 1] 内。
func Vector(input string, dims int) []float64 {
	v := make([]float64, dims)
	for i := range v {
		sum := sha256.Sum256(fmt.Appendf(nil, "%d\x00%s", i, input))
		v[i] = float64(binary.BigEndian.Uint32(sum[:4]))/math.MaxUint32*2 - 1
	}
	return v
}

// scenario 处理按模型名选的特殊场景（fake-billed-hang 除外，见 serve）。已经写好错误响应（或请求被取消）时返回 true，
// 这些都不计费；返回 false 时照常回答。
func (s *Server) scenario(w http.ResponseWriter, r *http.Request, model string) bool {
	switch {
	case model == "fake-hang":
		<-r.Context().Done()
		s.canceled.Add(1)
		return true
	case model == "fake-hold":
		return s.hold(r.Context())
	case strings.HasPrefix(model, "fake-slow-"):
		ms, err := strconv.Atoi(strings.TrimPrefix(model, "fake-slow-"))
		if err != nil || ms < 0 {
			writeFakeError(w, http.StatusBadRequest)
			return true
		}
		return s.wait(r.Context(), time.Duration(ms)*time.Millisecond)
	case model == "fake-500":
		writeFakeError(w, http.StatusInternalServerError)
	case model == "fake-429":
		w.Header().Set("Retry-After", "7")
		writeFakeError(w, http.StatusTooManyRequests)
	case model == "fake-401":
		writeFakeError(w, http.StatusUnauthorized)
	case model == "fake-not-json":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("fake upstream: this is not json"))
	default:
		return false
	}
	return true
}

// hold 等到 Release；请求先被取消就计数并返回 true。
func (s *Server) hold(ctx context.Context) bool {
	s.mu.Lock()
	release := s.release
	s.mu.Unlock()
	s.held.Add(1)
	defer s.held.Add(-1)
	select {
	case <-release:
		return false
	case <-ctx.Done():
		s.canceled.Add(1)
		return true
	}
}

// wait 等 d；请求先被取消就计数并返回 true。
func (s *Server) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return false
	case <-ctx.Done():
		s.canceled.Add(1)
		return true
	}
}

func writeFakeError(w http.ResponseWriter, status int) {
	writeJSON(w, status, map[string]any{"error": map[string]any{
		"message": "fake upstream error " + strconv.Itoa(status), "type": "fake_error", "param": nil, "code": "fake_" + strconv.Itoa(status),
	}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
