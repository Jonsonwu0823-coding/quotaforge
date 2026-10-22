package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// 流式（chat 带 stream: true）：上游回 2xx 之后，把它的 SSE 事件逐个原样转发给客户端，每个事件写完就刷新。
// 整个流受上游超时管（和非流式同一个口径：从发出上游请求到读完），所以一次流式调用活着的时间和非流式一样有界，
// 回收的上界和退出的上限都盖得住它（config.ReclaimAfter）。流结束之后才收尾。

// maxStreamEvent 是上游单个事件（到空行为止，含行尾）的上限。正常的块几百字节；一次给全的工具调用参数也远小于它。
// 超了就是上游的响应不能用。每个流最多在内存里攒一个事件，慢客户端不会让它变大。
const maxStreamEvent = 1 << 20

// writeGrace 是写客户端的上限比上游超时的到期时间多出的部分：读在到期时间截断，写最多再给这么久。到点时 bufio 里
// 已经读到的事件、刚读到的事件和补的错误事件都要在那之后写，写的上限要是就停在到期时间，它们写不出去，响应也不能正常结束。
// 一次流式调用最多比非流式多活这么久，回收上界的余量（5 秒）和退出上限都盖得住（TestStreamWorstCaseWithinBounds）。
const writeGrace = time.Second

var errEventTooLarge = errors.New("stream event exceeds the size limit")

// relay 转发上游的事件流，返回和 exchange 一样的结果：流完整（见到 [DONE]、没有错误事件、每个事件都能用）时返回
// 不带正文的 Response，c.usage 是流里最后一个合法的 usage（没有就是未知）；否则返回错误，c.usage 保持未知（不补 0），
// outcome 记成 USAGE_UNKNOWN。QF 自己发现的中途失败（超时、上游断开、事件超大或不是 JSON）给客户端补一个 OpenAI 格式的
// 错误事件，不发 [DONE]；上游自己的错误事件已经原样转发，不再补。
func (g *gateway) relay(ctx, upCtx context.Context, w http.ResponseWriter, c *call, resp *http.Response) (*ledger.Response, *apiError) {
	contentType := resp.Header.Values("Content-Type")
	if mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || mt != "text/event-stream" {
		return nil, errUpstreamInvalidResponse
	}
	deadline, _ := upCtx.Deadline()
	s := &stream{ctx: ctx, w: w, rc: http.NewResponseController(w), c: c, writeLimit: deadline.Add(writeGrace)}
	s.open(resp.StatusCode, contentType)

	usage, e := s.pump(upCtx, resp.Body)
	switch {
	case e == nil && s.upstreamError:
		return nil, errUpstreamError
	case e != nil:
		if !s.upstreamError {
			s.send(errorEvent(e))
		}
		return nil, e
	}
	c.usage = usage
	return &ledger.Response{Status: resp.StatusCode, ContentType: contentType}, nil
}

// stream 是一次流式转发里写客户端的一侧。
type stream struct {
	ctx           context.Context // 客户端请求的 context：取消了就是客户端走了
	w             http.ResponseWriter
	rc            *http.ResponseController
	c             *call
	writeLimit    time.Time // 写的上限：上游超时的到期时间加 writeGrace
	writing       bool      // 还在往客户端写
	upstreamError bool      // 上游发了错误事件
}

// open 写响应头：上游的状态码和 Content-Type，加上 Cache-Control: no-cache，不转发上游的其它头。客户端已经走了就不写。
func (s *stream) open(status int, contentType []string) {
	if s.ctx.Err() != nil {
		s.c.clientDisconnected = true
		return
	}
	s.w.Header()["Content-Type"] = contentType
	s.w.Header().Set("Cache-Control", "no-cache")
	s.w.WriteHeader(status)
	s.c.status, s.c.streamed, s.writing = status, true, true
	s.send(nil) // 先把头刷出去：客户端马上知道流开始了
}

// pump 一个一个地读上游的事件，记下用量和上游给的请求号、模型名，转发给客户端，读到 [DONE] 为止。
// 客户端走了也接着读（有界结算），只是不再写。返回流里最后一个合法的 usage。
func (s *stream) pump(upCtx context.Context, body io.Reader) (*ledger.Usage, *apiError) {
	r := eventReader{r: bufio.NewReader(body)}
	var usage *ledger.Usage
	for {
		raw, err := r.next()
		if err != nil {
			return nil, streamReadError(upCtx, err)
		}
		if s.c.firstEventAt.IsZero() {
			s.c.firstEventAt = time.Now()
		}
		ev, ok := s.c.recordEvent(raw)
		if !ok {
			return nil, errUpstreamInvalidResponse
		}
		if ev.usage != nil {
			usage = parseUsage(ev.usage, s.c.purpose) // 后来的覆盖先来的；不合法的就是未知
		}
		s.upstreamError = s.upstreamError || ev.upstreamError
		// QF 替客户端要的 usage：choices 为空、只带 usage 的那个事件不转发，客户端看到的和它没开 include_usage 时一样。
		if !(ev.usageOnly && !s.c.wantsUsage) {
			s.send(raw)
		}
		if ev.done {
			// 把 [DONE] 之后到正文结束的几个字节读掉，上游的连接才能复用；上游不结束时受上游超时管。
			_, _ = io.Copy(io.Discard, io.LimitReader(body, maxStreamEvent))
			return usage, nil
		}
	}
}

// send 写一段字节并刷新。每次写的期限只有一条规则：min(现在 + writeTimeout, 写的上限)，普通事件和补的错误事件都一样。
// 写不出去就不再写：到了写的上限还写不完，是上游超时之后的事（读早已截断），不算客户端的；
// 否则按 call.wrote 记：客户端走了，写超时另外记下。客户端不读时接着读完上游、照常结算，不让上游因为 QF 不读而停下。
func (s *stream) send(b []byte) {
	if !s.writing {
		return
	}
	if s.ctx.Err() != nil {
		s.writing, s.c.clientDisconnected = false, true
		return
	}
	until := time.Now().Add(writeTimeout)
	if s.writeLimit.Before(until) {
		until = s.writeLimit
	}
	_ = s.rc.SetWriteDeadline(until)
	_, err := s.w.Write(b)
	if err == nil {
		err = s.rc.Flush()
	}
	if err == nil {
		return
	}
	s.writing = false
	if !(errors.Is(err, os.ErrDeadlineExceeded) && until.Equal(s.writeLimit)) {
		s.c.wrote(err)
	}
}

// streamReadError 把读事件的错误映射成错误码：上游超时、事件超大，其余（中途断开、没有 [DONE] 就结束）是上游出错。
func streamReadError(upCtx context.Context, err error) *apiError {
	if errors.Is(err, errEventTooLarge) {
		return errUpstreamInvalidResponse
	}
	return transportError(upCtx, err)
}

// errorEvent 是 QF 补给客户端的错误事件，正文和 writeError 的错误体是同一个形状。
func errorEvent(e *apiError) []byte {
	var b errorBody
	b.Error.Message, b.Error.Type, b.Error.Code = e.message, e.typ, e.code
	data, _ := json.Marshal(b)
	return append(append([]byte("data: "), data...), "\n\n"...)
}

// eventReader 按事件读 SSE：一行一行读，读到空行（\n 或 \r\n）为止是一个事件，返回原始字节（含行尾），和网络怎么分包无关。
// 只用 \r 作行尾的流读不到空行，会长到上限，按不能用处理。
type eventReader struct {
	r   *bufio.Reader
	buf []byte
}

// next 返回下一个事件，下一次调用之前有效。在两个事件之间读完是 io.EOF，在事件中途读完是 io.ErrUnexpectedEOF。
func (er *eventReader) next() ([]byte, error) {
	er.buf = er.buf[:0]
	lineStart := 0
	for {
		chunk, err := er.r.ReadSlice('\n')
		if len(er.buf)+len(chunk) > maxStreamEvent {
			return nil, errEventTooLarge
		}
		er.buf = append(er.buf, chunk...)
		switch {
		case err == bufio.ErrBufferFull: // 一行比 bufio 的缓冲长，接着读这一行
			continue
		case err == io.EOF && len(er.buf) == 0:
			return nil, io.EOF
		case err == io.EOF:
			return nil, io.ErrUnexpectedEOF
		case err != nil:
			return nil, err
		}
		if line := er.buf[lineStart:]; string(line) == "\n" || string(line) == "\r\n" {
			return er.buf, nil
		}
		lineStart = len(er.buf)
	}
}

// streamEvent 是从一个事件里取出的、QF 需要的东西。
type streamEvent struct {
	done          bool            // data 是 [DONE]
	usage         json.RawMessage // 不为 null 的 usage 成员
	usageOnly     bool            // choices 是空数组、带着 usage：开了 include_usage 才会有的那个事件
	upstreamError bool            // 顶层带 error 的对象
}

// recordEvent 解析一个事件的 data（多个 data 行用 \n 连起来；注释和别的字段不看），第一次见到的请求号、模型名记到 c 上。
// 没有 data 的事件（注释、心跳）照转发。data 既不是 [DONE] 也不是 JSON 对象时返回 false。缺字段不算错，QF 不校验 chunk 的结构。
func (c *call) recordEvent(raw []byte) (ev streamEvent, ok bool) {
	data, hasData := eventData(raw)
	switch {
	case !hasData:
		return ev, true
	case string(data) == "[DONE]":
		ev.done = true
		return ev, true
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return ev, false
	}
	if c.upstreamResponseID == "" {
		_ = json.Unmarshal(fields["id"], &c.upstreamResponseID)
	}
	if c.upstreamModel == "" {
		_ = json.Unmarshal(fields["model"], &c.upstreamModel)
	}
	if u := fields["usage"]; u != nil && string(u) != "null" {
		var choices []json.RawMessage
		ev.usage = u
		ev.usageOnly = json.Unmarshal(fields["choices"], &choices) == nil && choices != nil && len(choices) == 0
	}
	ev.upstreamError = fields["error"] != nil && string(fields["error"]) != "null"
	return ev, true
}

// eventData 取出事件的 data：每个 data: 行去掉前缀和一个可选的空格，多行用 \n 连起来。
func eventData(raw []byte) (data []byte, ok bool) {
	for line := range bytes.Lines(raw) {
		v, found := bytes.CutPrefix(bytes.TrimRight(line, "\r\n"), []byte("data:"))
		if !found {
			continue
		}
		if ok {
			data = append(data, '\n')
		}
		data, ok = append(data, bytes.TrimPrefix(v, []byte(" "))...), true
	}
	return data, ok
}
