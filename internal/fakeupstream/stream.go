package fakeupstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 流式回答（chat 带 "stream": true），形状照百炼的 OpenAI 兼容模式：一个带 role 的块、若干内容块（每块记 1 个 completion token，
// 内容都含 "fake reply"）、一个带 finish_reason 的块；请求写了 stream_options.include_usage: true 时，每个块带 "usage": null，
// 末尾再多一个 choices 为空数组、带 usage 的块；最后是 data: [DONE]。每个事件写完就刷新。
// 账单按实际生成的记：handler 结束时（包括被取消、中途断开）记 yes，completion_tokens 是已经发出的内容块数。
//
//	fake-no-usage            不发 usage 的块
//	fake-stream-slow-<毫秒>   每两个事件之间等这么久
//	fake-stream-step         每个事件之前等一次 Release（和 fake-hold 同一个开关），挂住时计入 Held
//	fake-stream-cut          发完 role 块和第一个内容块，连接直接断开
//	fake-stream-error        发完 role 块和第一个内容块，发一个 {"error":…} 事件，再发 [DONE]
//	fake-stream-huge         第一个内容块超过 1 MiB
//	fake-stream-no-done      照常发完（含 usage），但不发 [DONE]
//	fake-stream-big          64 个约 256 KiB 的内容块，共 16 MiB，比内核的收发缓冲大（慢客户端的测试用）
//	fake-stream-bytewise     一次只写一个字节并刷新（事件跨越多次读）

const streamContentEvents = 10

// streamer 写一个流式回答。
type streamer struct {
	s       *Server
	w       http.ResponseWriter
	ctx     context.Context
	model   string
	head    map[string]any // 每个块共有的字段
	content int            // 已经发出的内容块数
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, req chatRequest, n int64, prompt int) {
	st := &streamer{s: s, w: w, ctx: r.Context(), model: req.Model, head: map[string]any{
		"id": fmt.Sprintf("fakecmpl-%d", n), "object": "chat.completion.chunk", "created": 1767225600, "model": req.Model + ModelVersionSuffix,
	}}
	withUsage := req.StreamOptions.IncludeUsage
	if withUsage {
		st.head["usage"] = nil
	}
	defer func() { s.charge(r, &usage{prompt: prompt, completion: st.content, chat: true}) }()
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("x-request-id", fmt.Sprintf("fake-chat-%d", n))
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	pieces := contentPieces(req.Model)
	if !st.chunk(map[string]any{"role": "assistant", "content": ""}, nil) || !st.chunk(map[string]any{"content": pieces[0]}, nil) {
		return
	}
	st.content = 1
	switch req.Model {
	case "fake-stream-cut":
		panic(http.ErrAbortHandler) // net/http 直接关掉连接，不写分块编码的结尾
	case "fake-stream-error":
		if st.event(`{"error":{"message":"fake upstream stream error","type":"fake_error","param":null,"code":"fake_stream_error"}}`) {
			st.event("[DONE]")
		}
		return
	}
	for _, p := range pieces[1:] {
		if !st.chunk(map[string]any{"content": p}, nil) {
			return
		}
		st.content++
	}
	stop := "stop"
	if !st.chunk(map[string]any{}, &stop) {
		return
	}
	if withUsage && req.Model != "fake-no-usage" {
		u := map[string]any{"prompt_tokens": prompt, "completion_tokens": st.content, "total_tokens": prompt + st.content, "fake": true}
		if !st.send(map[string]any{"choices": []any{}, "usage": u}) {
			return
		}
	}
	if req.Model != "fake-stream-no-done" {
		st.event("[DONE]")
	}
}

// contentPieces 是各个内容块的文字。
func contentPieces(model string) []string {
	n, size := streamContentEvents, 0
	switch model {
	case "fake-stream-huge":
		size = 1 << 20
	case "fake-stream-big":
		n, size = 64, 256<<10
	}
	pieces := make([]string, n)
	for i := range pieces {
		pieces[i] = "fake reply " + strconv.Itoa(i+1) + strings.Repeat(".", size)
	}
	return pieces
}

// chunk 发一个 choices 只有一项的块。
func (st *streamer) chunk(delta map[string]any, finish *string) bool {
	return st.send(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}})
}

// send 发一个 JSON 块：fields 加上共有的字段。
func (st *streamer) send(fields map[string]any) bool {
	for k, v := range st.head {
		if _, ok := fields[k]; !ok {
			fields[k] = v
		}
	}
	data, _ := json.Marshal(fields)
	return st.event(string(data))
}

// event 先按场景等（延迟、等 Release），再写一个 data 事件并刷新，计入 StreamEvents。请求被取消了返回 false。
// 计数在写成功之后、事件的最后一个字节能到客户端之前：先写除最后一个字节以外的部分（fake-stream-bytewise 一个字节一个字节地
// 写并刷新），计数，再写最后一个字节、刷新。最后一个字节一定留在 ResponseWriter 自己的缓冲里，刷新之前到不了客户端，
// 所以客户端读完这个事件时计数一定已经加上，事件再大（前面的部分写穿缓冲）也一样。写失败的不计数。
func (st *streamer) event(data string) bool {
	if !st.pause() {
		return false
	}
	b := []byte("data: " + data + "\n\n")
	head, last := b[:len(b)-1], b[len(b)-1:]
	if !st.write(head) {
		return false
	}
	st.s.sent.Add(1)
	if _, err := st.w.Write(last); err != nil {
		return false
	}
	st.w.(http.Flusher).Flush()
	if st.ctx.Err() != nil {
		st.s.canceled.Add(1)
		return false
	}
	return true
}

// write 写一段字节；fake-stream-bytewise 一次只写一个字节并刷新（事件跨越多次读）。
func (st *streamer) write(b []byte) bool {
	if st.model != "fake-stream-bytewise" {
		_, err := st.w.Write(b)
		return err == nil
	}
	for i := range b {
		if _, err := st.w.Write(b[i : i+1]); err != nil {
			return false
		}
		st.w.(http.Flusher).Flush()
	}
	return true
}

// pause 是事件之前的等待：fake-stream-slow-<毫秒> 等这么久，fake-stream-step 等 Release。被取消时计数并返回 false。
func (st *streamer) pause() bool {
	switch {
	case st.model == "fake-stream-step":
		return !st.s.hold(st.ctx)
	case strings.HasPrefix(st.model, "fake-stream-slow-"):
		ms, _ := strconv.Atoi(strings.TrimPrefix(st.model, "fake-stream-slow-"))
		return !st.s.wait(st.ctx, time.Duration(ms)*time.Millisecond)
	}
	return true
}
