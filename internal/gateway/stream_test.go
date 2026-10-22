package gateway

import (
	"bufio"
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
	"net/http/httptrace"
	"os"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// 流式的测试不靠睡眠判对错：等确定的事件（假上游已经发出第 n 个事件、客户端已经读到第 n 个事件、日志行出现），
// 截止时间只用来判失败。要在一个流里来回几步的测试用 streamChat 这么长的 chat 超时。
const streamChat = 10 * time.Second

const streamBody = `{"model":MODEL,"stream":true,"messages":[{"role":"user","content":"REQ-MARKER-stream"}]}`

// 正常流的事件：role 块、10 个内容块、finish 块、usage 块、[DONE]。
const fakeStreamEvents = 14

func streamOf(model string) string { return withModel(streamBody, `"`+model+`"`) }

func newStreamHarness(t *testing.T, l Ledger) *harness {
	t.Helper()
	return newHarnessChat(t, l, &stubLimiter{}, tracedAt(1), streamChat)
}

// clientStream 是客户端这一侧打开的一个流：一个 goroutine 按事件读，送进 events；读完（或出错）时关掉 events，结果在 end。
type clientStream struct {
	resp   *http.Response
	events chan string
	end    chan error
	cancel context.CancelFunc
}

func (h *harness) openStream(t *testing.T, key, body string, headers ...string) *clientStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.qf.URL+chatRoute, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	s := &clientStream{resp: resp, events: make(chan string, 256), end: make(chan error, 1), cancel: cancel}
	t.Cleanup(func() { cancel(); resp.Body.Close() })
	go func() {
		r := eventReader{r: bufio.NewReader(resp.Body)}
		for {
			raw, err := r.next()
			if err != nil {
				close(s.events)
				s.end <- err
				return
			}
			s.events <- string(raw)
		}
	}()
	return s
}

// next 等客户端读到下一个事件。
func (s *clientStream) next(t *testing.T) string {
	t.Helper()
	select {
	case ev, ok := <-s.events:
		if !ok {
			t.Fatal("the stream ended")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event within 5s")
	}
	return ""
}

// rest 读完剩下的事件，返回它们和流怎么结束的（正常关闭是 io.EOF）。
func (s *clientStream) rest(t *testing.T) ([]string, error) {
	t.Helper()
	var out []string
	timeout := time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return out, <-s.end
			}
			out = append(out, ev)
		case <-timeout:
			t.Fatal("the stream did not end within 15s")
		}
	}
}

// step 放出 fake-stream-step 的下一个事件，等假上游把它写出去。
func (h *harness) step(t *testing.T) {
	t.Helper()
	waitFor(t, func() bool { return h.fake.Stats().Held == 1 })
	sent := h.fake.Stats().StreamEvents
	h.fake.Release()
	waitFor(t, func() bool { return h.fake.Stats().StreamEvents > sent })
}

// splitEvents 把一段 SSE 字节按空行切成事件（只用于 \n 的流）。
func splitEvents(b []byte) []string {
	var out []string
	for _, e := range strings.SplitAfter(string(b), "\n\n") {
		if e != "" {
			out = append(out, e)
		}
	}
	return out
}

func isUsageOnly(ev string) bool {
	return strings.Contains(ev, `"choices":[]`) && strings.Contains(ev, `"usage":{`)
}

// 正常的流：事件原样转发，只去掉 QF 替客户端要的 usage 那个事件；上游收到的请求体只多了 stream_options；
// 响应头是 SSE；结局是结算，用量就是上游 usage 事件里的数。
func TestStreamForwardsEventsAsIs(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("shared"))
	events, end := s.rest(t)
	if end != io.EOF || s.resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, end %v", s.resp.StatusCode, end)
	}
	if ct := s.resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" || s.resp.Header.Get("Cache-Control") != "no-cache" ||
		s.resp.Header.Get("X-Upstream-Marker") != "" {
		t.Fatalf("headers %v", s.resp.Header)
	}
	sentBody, upHeader, upResp := h.upstreamSaw()
	want := `{"stream_options":{"include_usage":true},"model":"fake-chat","stream":true,"messages":[{"role":"user","content":"REQ-MARKER-stream"}]}`
	if string(sentBody) != want || upHeader.Get("Accept") != "text/event-stream" {
		t.Fatalf("upstream got %s (Accept %q)", sentBody, upHeader.Get("Accept"))
	}
	upEvents := splitEvents(upResp)
	wantEvents := slices.DeleteFunc(slices.Clone(upEvents), isUsageOnly)
	if len(upEvents) != fakeStreamEvents || len(wantEvents) != fakeStreamEvents-1 || !slices.Equal(events, wantEvents) {
		t.Fatalf("client got %d events, upstream sent %d:\n%q\n%q", len(events), len(upEvents), events, upEvents)
	}
	// 其它事件里的 "usage":null 不去掉；模型名是上游的实际模型名
	if !strings.Contains(events[1], `"usage":null`) || !strings.Contains(events[1], `"model":"fake-chat-20260101"`) || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("events %q", events)
	}
	line := h.logLine(t, s.resp.Header.Get("x-request-id"))
	u, _ := line["usage"].(map[string]any)
	if line["outcome"] != "SETTLED" || line["status"] != float64(200) || line["stream"] != true || line["first_event_ms"] == nil ||
		u["completion_tokens"] != float64(10) || line["upstream_model"] != "fake-chat-20260101" || line["upstream_response_id"] == nil ||
		line["error_code"] != nil || line["client_disconnected"] != nil {
		t.Fatalf("log = %v", line)
	}
}

// 客户端自己要了 usage：全部原样转发，请求体只换了模型名。
func TestStreamClientWantsUsage(t *testing.T) {
	h := newHarness(t)
	body := `{"stream":true, "stream_options" : { "x":1, "include_usage" : true },"model":"shared"}`
	s := h.openStream(t, clientKey, body)
	events, end := s.rest(t)
	sentBody, _, upResp := h.upstreamSaw()
	if end != io.EOF || string(sentBody) != strings.Replace(body, `"shared"`, `"fake-chat"`, 1) ||
		strings.Join(events, "") != string(upResp) || !slices.ContainsFunc(events, isUsageOnly) {
		t.Fatalf("end %v, upstream got %s, client got %q", end, sentBody, events)
	}
}

// 上游把事件分成一个一个字节发：事件的边界只看空行，和怎么分包无关。
func TestStreamEventsAcrossReads(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("fake-stream-bytewise"))
	events, end := s.rest(t)
	_, _, upResp := h.upstreamSaw()
	if end != io.EOF || !slices.Equal(events, slices.DeleteFunc(splitEvents(upResp), isUsageOnly)) || len(events) != fakeStreamEvents-1 {
		t.Fatalf("end %v, events %q", end, events)
	}
	if line := h.logLine(t, s.resp.Header.Get("x-request-id")); line["outcome"] != "SETTLED" {
		t.Fatalf("log = %v", line)
	}
}

// \r\n 的行尾、注释行、分成两个 data 行的 usage：照样认出 usage 事件（去掉）、照样结算。
func TestStreamCRLFAndMultilineData(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("sse-crlf"))
	events, end := s.rest(t)
	want := slices.DeleteFunc(strings.SplitAfter(sseCRLF, "\r\n\r\n"), func(e string) bool { return e == "" || strings.Contains(e, `"choices":[]`) })
	if end != io.EOF || !slices.Equal(events, want) {
		t.Fatalf("end %v, events %q, want %q", end, events, want)
	}
	line := h.logLine(t, s.resp.Header.Get("x-request-id"))
	u, _ := line["usage"].(map[string]any)
	if line["outcome"] != "SETTLED" || u["total_tokens"] != float64(3) || line["upstream_response_id"] != "crlf-1" || line["upstream_model"] != "crlf-model" {
		t.Fatalf("log = %v", line)
	}
}

// 每个事件写完就刷新：客户端读到第 n 个事件时，上游还挂着第 n+1 个。
func TestStreamFlushesEachEvent(t *testing.T) {
	h := newStreamHarness(t, newAllowAll())
	s := h.openStream(t, clientKey, streamOf("fake-stream-step"))
	for n := int64(1); n <= 3; n++ {
		h.step(t)
		s.next(t)
		waitFor(t, func() bool { return h.fake.Stats().Held == 1 }) // 第 n+1 个还没放
		if got := h.fake.Stats().StreamEvents; got != n {
			t.Fatalf("client read event %d while the upstream sent %d", n, got)
		}
	}
	for range fakeStreamEvents - 3 {
		h.step(t)
	}
	if events, end := s.rest(t); end != io.EOF || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("end %v, events %q", end, events)
	}
}

// 流结束之后才收尾：客户端已经读到 usage 之前的全部事件、上游还没发 [DONE] 时，账本里调用还是 IN_FLIGHT，
// 预留还占着；[DONE] 之后才结算。
func TestStreamSettlesOnlyAfterTheEnd(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	s := e.openStream(t, e.key, streamOf("fake-stream-step"))
	id := s.resp.Header.Get("x-request-id")
	for range fakeStreamEvents - 1 { // 除了 [DONE]，usage 事件也已经发出
		e.step(t)
	}
	for range fakeStreamEvents - 2 {
		s.next(t)
	}
	if c := e.call(t, id); c.status.String != "IN_FLIGHT" || c.usageTotal.Valid {
		t.Fatalf("before [DONE]: call %s, usage %v", c.status.String, c.usageTotal)
	}
	e.step(t)
	if ev := s.next(t); ev != "data: [DONE]\n\n" {
		t.Fatalf("last event %q", ev)
	}
	line := e.logLine(t, id)
	c := e.call(t, id)
	u, _ := line["usage"].(map[string]any)
	if c.status.String != "SETTLED" || c.usageTotal.Int64 != int64(u["total_tokens"].(float64)) || c.httpStatus.Int64 != 200 || c.errorCode.Valid ||
		!slices.Equal(c.kinds, []string{"reserve", "settle"}) {
		t.Fatalf("call %+v, log %v", c, line)
	}
	if _, reserved, consumed := e.budget(t); reserved != 0 || consumed != c.usageTotal.Int64 {
		t.Fatalf("reserved %d consumed %d", reserved, consumed)
	}
	dbtest.Verify(t, e.store)
}

// 客户端在流的中途断开：不取消上游、接着读完，照常结算；日志有 client_disconnected，状态码是已经写出的 200。
func TestStreamClientDisconnectKeepsReading(t *testing.T) {
	h := newStreamHarness(t, newAllowAll())
	s := h.openStream(t, clientKey, streamOf("fake-stream-step"))
	id := s.resp.Header.Get("x-request-id")
	h.step(t)
	s.next(t)
	s.cancel()
	for range fakeStreamEvents - 1 { // 断开之后上游还在等 QF 读：一个一个放完
		h.step(t)
	}
	line := h.logLine(t, id)
	if line["outcome"] != "SETTLED" || line["client_disconnected"] != true || line["write_timeout"] != nil || line["status"] != float64(200) ||
		line["usage"] == "unknown" || h.fake.Stats().Canceled != 0 || h.fake.Stats().StreamEvents != fakeStreamEvents {
		t.Fatalf("log %v, stats %+v", line, h.fake.Stats())
	}
}

// 客户端不读：写一个事件超过期限后不再写，接着读完上游、照常结算；日志分得出这是写超时。
func TestStreamSlowClient(t *testing.T) {
	h := newStreamHarness(t, newAllowAll())
	line := h.unreadStream(t, "fake-stream-big")
	if line["outcome"] != "SETTLED" || line["client_disconnected"] != true || line["write_timeout"] != true || line["status"] != float64(200) ||
		h.fake.Stats().StreamEvents != 68 || h.fake.Stats().Canceled != 0 {
		t.Fatalf("log %v, stats %+v", line, h.fake.Stats())
	}
	if got := testutil.ToFloat64(h.metrics.Streams.WithLabelValues("ok", "write_timeout")); got != 1 {
		t.Fatalf("qf_streams_total{ok,write_timeout} = %v", got)
	}
}

// 客户端不读、上游超时先到：最后一次写的期限就是上游超时的到期时间，写不完是上游超时，不算客户端的事。
// 日志没有 client_disconnected、write_timeout，错误码是 upstream_timeout，指标的 client 是 connected。
func TestStreamSlowClientUpstreamTimesOutFirst(t *testing.T) {
	h := newHarness(t) // chat 超时 500 毫秒，比写的期限（5 秒）短
	line := h.unreadStream(t, "fake-stream-big")
	if line["outcome"] != "USAGE_UNKNOWN" || line["error_code"] != "upstream_timeout" || line["client_disconnected"] != nil ||
		line["write_timeout"] != nil || line["status"] != float64(200) {
		t.Fatalf("log %v, stats %+v", line, h.fake.Stats())
	}
	if got := testutil.ToFloat64(h.metrics.Streams.WithLabelValues("upstream_timeout", "connected")); got != 1 {
		t.Fatalf("qf_streams_total{upstream_timeout,connected} = %v", got)
	}
}

// unreadStream 用一个读完请求就不再读的客户端发一个流式请求（见 unread）。
func (h *harness) unreadStream(t *testing.T, model string) map[string]any {
	t.Helper()
	return h.unread(t, chatRoute, streamOf(model))
}

// unread 用一个接收缓冲很小、发完请求就不再读的原始 TCP 客户端发请求，等到这次调用的日志行出现（测试里只有这一个请求）。
func (h *harness) unread(t *testing.T, route, body string) map[string]any {
	t.Helper()
	conn, err := net.Dial("tcp", h.qf.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.(*net.TCPConn).SetReadBuffer(4096)
	fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: qf\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		route, clientKey, len(body), body)
	for start := time.Now(); time.Since(start) < writeTimeout+10*time.Second; time.Sleep(20 * time.Millisecond) {
		if lines := h.logs.lines(t); len(lines) > 0 {
			return lines[0]
		}
	}
	t.Fatalf("no log line; upstream stats %+v", h.fake.Stats())
	return nil
}

// 非流式的客户端不读：整个响应（32 MiB）超过写的期限还没写完时 QF 不再写，handler 返回，日志记 client_disconnected 和
// write_timeout；收尾在写回之前，账本照常结算。没有写的期限时 handler 一直挂在写上，日志行不会出现。
// 不在 Windows 上跑：回环的缓冲装得下整个响应，写不会阻塞，测不到写的期限（期限设没设由 TestNonStreamWritesHaveADeadline 守着）。
func TestUnreadResponseTimesOut(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Windows loopback buffers the whole response, so the write never blocks")
	}
	h := newHarness(t)
	line := h.unread(t, embeddingsRoute, `{"model":"size-33554432"}`)
	if line["outcome"] != "SETTLED" || line["status"] != float64(200) || line["client_disconnected"] != true || line["write_timeout"] != true {
		t.Fatalf("log %v", line)
	}
}

// writeDeadlines 记下处理函数每次设的写期限，和第一次写（写头或者正文）的时候已经设了几次、是什么时候。
type writeDeadlines struct {
	*httptest.ResponseRecorder
	deadlines   []time.Time
	wrote       bool
	beforeWrite int
	firstWrite  time.Time
}

func (w *writeDeadlines) SetWriteDeadline(d time.Time) error {
	w.deadlines = append(w.deadlines, d)
	return nil
}

func (w *writeDeadlines) WriteHeader(code int) {
	w.noteWrite()
	w.ResponseRecorder.WriteHeader(code)
}

func (w *writeDeadlines) Write(b []byte) (int, error) {
	w.noteWrite()
	return w.ResponseRecorder.Write(b)
}

func (w *writeDeadlines) noteWrite() {
	if !w.wrote {
		w.wrote, w.beforeWrite, w.firstWrite = true, len(w.deadlines), time.Now()
	}
}

// failingWrites 的每次写都返回 err：客户端那一侧出了事（对方重置连接、写超过期限）。
type failingWrites struct {
	*httptest.ResponseRecorder
	err error
}

func (w *failingWrites) Write([]byte) (int, error) { return 0, w.err }

// 非流式的写回失败都记 client_disconnected；只有写超过期限（os.ErrDeadlineExceeded）才另记 write_timeout，
// 别的写错误（例如对方重置了连接）不记。和平台无关，不靠真的不读的客户端。
func TestNonStreamWriteFailures(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		err     error
		timeout any
	}{
		{syscall.ECONNRESET, nil},
		{io.ErrClosedPipe, nil},
		{os.ErrDeadlineExceeded, true},
	} {
		w := &failingWrites{ResponseRecorder: httptest.NewRecorder(), err: tc.err}
		req := httptest.NewRequest(http.MethodPost, chatRoute, strings.NewReader(`{"model":"shared"}`))
		req.Header.Set("Authorization", "Bearer "+clientKey)
		h.qf.Config.Handler.ServeHTTP(w, req)
		l := h.logLine(t, w.Header().Get("x-request-id"))
		if l["status"] != float64(http.StatusOK) || l["client_disconnected"] != true || l["write_timeout"] != tc.timeout {
			t.Fatalf("%v: status %v, client_disconnected %v, write_timeout %v; want 200, true, %v",
				tc.err, l["status"], l["client_disconnected"], l["write_timeout"], tc.timeout)
		}
	}
}

// knownCall 的调用状态查询总能查到这次调用（allowAll 不存调用，查询是 404）。
type knownCall struct{ Ledger }

func (knownCall) Call(_ context.Context, _ int64, id string) (*ledger.CallInfo, error) {
	return &ledger.CallInfo{ID: id, Purpose: "chat", Alias: "shared", Status: ledger.Settled, CreatedAt: "2026-10-18T00:00:00.000000Z"}, nil
}

// 非流式的三种写回（上游的响应、错误响应、调用状态查询）都在第一次写之前设了写期限，期限在“第一次写 + writeTimeout”以内。
// 和平台无关；客户端真的不读时期限起作用，由 TestUnreadResponseTimesOut 在 Linux 上守着。
func TestNonStreamWritesHaveADeadline(t *testing.T) {
	h := newHarnessWith(t, knownCall{Ledger: newAllowAll()})
	asked := requestID(t, h.chat(t, `{"model":"shared"}`))
	for _, tc := range []struct {
		name, method, path, body, key string
		status                        int
	}{
		{"upstream response", http.MethodPost, chatRoute, `{"model":"shared"}`, clientKey, http.StatusOK},
		{"error response", http.MethodPost, chatRoute, `{"model":"shared"}`, "", http.StatusUnauthorized},
		{"call lookup", http.MethodGet, "/v1/calls/" + asked, "", clientKey, http.StatusOK},
	} {
		w := &writeDeadlines{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.key != "" {
			req.Header.Set("Authorization", "Bearer "+tc.key)
		}
		h.qf.Config.Handler.ServeHTTP(w, req)
		if w.Code != tc.status || !w.wrote || w.beforeWrite == 0 {
			t.Fatalf("%s: status %d, %d write deadlines before the first write", tc.name, w.Code, w.beforeWrite)
		}
		if d := w.deadlines[w.beforeWrite-1]; !d.After(w.firstWrite) || d.After(w.firstWrite.Add(writeTimeout)) {
			t.Fatalf("%s: write deadline %v, first write at %v", tc.name, d, w.firstWrite)
		}
	}
}

// 流式的结尾有写的期限：收尾之后、handler 返回之前设成现在 + writeTimeout，net/http 在 handler 返回之后写分块编码的结尾时
// 还在期限里。收尾慢到过了写的上限（上游超时的到期时间加 writeGrace）：留着最后一个事件的期限，结尾写不出去；
// 清掉的话，客户端不读时这次写没有上限。
func TestStreamTailHasAWriteDeadline(t *testing.T) {
	h := newHarnessWith(t, slowFinish{Ledger: newAllowAll(), d: chatTimeout + writeGrace})
	w := &writeDeadlines{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, chatRoute, strings.NewReader(streamOf("shared")))
	req.Header.Set("Authorization", "Bearer "+clientKey)
	h.qf.Config.Handler.ServeHTTP(w, req)
	returned := time.Now()
	if w.Code != http.StatusOK || len(w.deadlines) == 0 {
		t.Fatalf("status %d, %d write deadlines", w.Code, len(w.deadlines))
	}
	if last := w.deadlines[len(w.deadlines)-1]; !last.After(returned) || last.After(returned.Add(writeTimeout)) {
		t.Fatalf("the last write deadline is %v, the handler returned at %v", last, returned)
	}
}

// 整个流受 chat 的上游超时管（从发出上游请求算起）：事件之间慢的流在超时时截断，客户端收到 QF 补的错误事件、没有 [DONE]，
// 结局是用量未知。一次流式调用最长活 chat 超时再加几次账本操作，回收上界（chat 超时 + 3 次账本操作 + 余量）盖得住它。
func TestStreamLivesWithinTheChatTimeout(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("stream-slow"))
	events, end := s.rest(t)
	if end != io.EOF || slices.Contains(events, "data: [DONE]\n\n") {
		t.Fatalf("end %v, events %q", end, events)
	}
	checkErrorEvent(t, events[len(events)-1], "upstream_timeout")
	line := h.logLine(t, s.resp.Header.Get("x-request-id"))
	if line["outcome"] != "USAGE_UNKNOWN" || line["error_code"] != "upstream_timeout" || line["usage"] != "unknown" || line["status"] != float64(200) {
		t.Fatalf("log = %v", line)
	}
	timeout := h.cfg.Timeout(config.Chat)
	if d := time.Duration(line["duration_ms"].(float64)) * time.Millisecond; d > timeout+timeout/2 {
		t.Fatalf("the stream lived %v, the chat timeout is %v", d, timeout)
	}
	if timeout+3*2*time.Second >= h.cfg.ReclaimAfter() || h.cfg.ReclaimAfter() > h.cfg.ShutdownTimeout() {
		t.Fatalf("reclaim after %v, shutdown %v", h.cfg.ReclaimAfter(), h.cfg.ShutdownTimeout())
	}
}

// 一次流式调用最坏的情况：4 次账本操作、2 次限流操作、上游超时（整个流），再加补给客户端的错误事件的写期限，
// 也在退出上限之内；停在 RESERVED、IN_FLIGHT 的部分（预留事务剩下的、发送意图、整个流、错误事件、收尾）在回收上界之内。
// 默认配置测一次，上游超时没有上限，再用 24 小时测一次。
func TestStreamWorstCaseWithinBounds(t *testing.T) {
	for _, cfg := range []*config.Config{testConfig(t, 0, 0), testConfig(t, 24*3600*1000, 1000)} {
		chat := cfg.Timeout(config.Chat)
		if worst := 4*config.LedgerTimeout + 2*config.RateLimitTimeout + chat + writeGrace; worst > cfg.ShutdownTimeout() {
			t.Errorf("chat timeout %v: the worst stream takes %v, longer than the shutdown cap %v", chat, worst, cfg.ShutdownTimeout())
		}
		if alive := 3*config.LedgerTimeout + chat + writeGrace; alive >= cfg.ReclaimAfter() {
			t.Errorf("chat timeout %v: a stream stays open %v, the reclaim bound is %v", chat, alive, cfg.ReclaimAfter())
		}
	}
	if writeTimeout >= testConfig(t, 0, 0).Timeout(config.Chat) {
		t.Error("the write deadline must be shorter than the stream's limit")
	}
}

// 流在中途失败：QF 自己发现的（上游断开、没有 [DONE]、事件超大、data 不是 JSON）补一个错误事件、不发 [DONE]；
// 上游自己发的错误事件原样转发，不再补。都是用量未知，不补 0。流完整但没有 usage 的也是未知，没有错误码。
func TestStreamFailuresMidway(t *testing.T) {
	cases := []struct {
		model, code, last string // last 为空：最后一个事件是 QF 补的错误事件
	}{
		{"fake-stream-cut", "upstream_error", ""},
		{"fake-stream-no-done", "upstream_error", ""},
		{"fake-stream-huge", "upstream_invalid_response", ""},
		{"sse-not-json", "upstream_invalid_response", ""},
		{"fake-stream-error", "upstream_error", "data: [DONE]\n\n"},
		{"fake-no-usage", "", "data: [DONE]\n\n"},
	}
	h := newHarness(t)
	for _, tc := range cases {
		t.Run(tc.model, func(t *testing.T) {
			s := h.openStream(t, clientKey, streamOf(tc.model))
			events, end := s.rest(t)
			all := strings.Join(events, "")
			if end != io.EOF || len(events) == 0 {
				t.Fatalf("end %v, events %q", end, events)
			}
			checkStreamEnd(t, events, tc.code, tc.last)
			if !forwardedRight(tc.model, all) {
				t.Fatalf("events %q", events)
			}
			line := h.logLine(t, s.resp.Header.Get("x-request-id"))
			if line["outcome"] != "USAGE_UNKNOWN" || line["usage"] != "unknown" || line["status"] != float64(200) || line["error_code"] != nilIfEmpty(tc.code) {
				t.Fatalf("log = %v", line)
			}
		})
	}
}

// forwardedRight：不能用的事件（不是 JSON 的、超大的）没有转发；上游自己的错误事件照转发。
func forwardedRight(model, all string) bool {
	unusable := strings.Contains(all, "RESP-MARKER-sse") || strings.Contains(all, strings.Repeat(".", 1000))
	return !unusable && (model == "fake-stream-error") == strings.Contains(all, `"code":"fake_stream_error"`)
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// checkStreamEnd：last 为空时，最后一个事件是 QF 补的错误事件（错误码 code），流里只有这一个；否则最后一个事件是 last，
// 流里没有 QF 补的错误事件。
func checkStreamEnd(t *testing.T, events []string, code, last string) {
	t.Helper()
	qfErrors := strings.Count(strings.Join(events, ""), `"type":"upstream_error"`)
	switch {
	case last == "":
		checkErrorEvent(t, events[len(events)-1], code)
		if qfErrors != 1 {
			t.Fatalf("%d QF error events: %q", qfErrors, events)
		}
	case events[len(events)-1] != last || qfErrors != 0:
		t.Fatalf("want %q last and no QF error event, got %q", last, events)
	}
}

// checkErrorEvent 按字面检查 QF 补给客户端的错误事件：一个 data 行加一个空行，data 是 OpenAI 格式的错误对象。
// 期望值不经 errorEvent 生成，格式改了这里会发现。
func checkErrorEvent(t *testing.T, ev, code string) {
	t.Helper()
	var body struct {
		Error struct {
			Message, Type, Code string
			Param               *string
		}
	}
	data, ok := strings.CutPrefix(ev, `data: {"error":{`)
	if !ok || !strings.HasSuffix(ev, "}}\n\n") || strings.Count(ev, "\n") != 2 ||
		json.Unmarshal([]byte(strings.TrimPrefix(ev, "data: ")), &body) != nil ||
		body.Error.Type != "upstream_error" || body.Error.Code != code || body.Error.Message == "" || body.Error.Param != nil {
		t.Fatalf("QF's error event %q (data %q), want code %s", ev, data, code)
	}
}

// 流中途失败时，账本（和调用状态查询）记的 http_status 是已经写给客户端的 200，error_code 是补给客户端的错误事件里的那个。
func TestStreamFailureRecordedAsWritten(t *testing.T) {
	e := newDBEnv(t, 100000, nil) // chat 超时 500 毫秒：stream-slow 会超时
	for model, code := range map[string]string{"fake-stream-cut": "upstream_error", "stream-slow": "upstream_timeout"} {
		s := e.openStream(t, e.key, streamOf(model))
		s.rest(t)
		id := s.resp.Header.Get("x-request-id")
		e.logLine(t, id)
		r := e.get("/v1/calls/"+id, e.key)
		var got struct {
			Status     string `json:"status"`
			HTTPStatus int    `json:"http_status"`
			ErrorCode  string `json:"error_code"`
		}
		if err := json.Unmarshal(r.body, &got); err != nil || r.status != http.StatusOK ||
			got.Status != "USAGE_UNKNOWN" || got.HTTPStatus != 200 || got.ErrorCode != code {
			t.Fatalf("%s: lookup %d %s", model, r.status, r.body)
		}
	}
	dbtest.Verify(t, e.store)
}

// 事件密集的流到了上游超时：到点时读缓冲里还有已经读到的事件，它们和补的错误事件都要写得出去。客户端最后收到
// upstream_timeout 的错误事件，响应正常结束；日志是用量未知、upstream_timeout，不算客户端断开或写超时。跑几遍。
func TestStreamDenseAtTheDeadline(t *testing.T) {
	h := newHarness(t)
	for range 5 {
		s := h.openStream(t, clientKey, streamOf("sse-dense"))
		events, end := s.rest(t)
		if end != io.EOF || len(events) < 2 {
			t.Fatalf("end %v after %d events", end, len(events))
		}
		checkErrorEvent(t, events[len(events)-1], "upstream_timeout")
		line := h.logLine(t, s.resp.Header.Get("x-request-id"))
		if line["outcome"] != "USAGE_UNKNOWN" || line["error_code"] != "upstream_timeout" || line["client_disconnected"] != nil || line["write_timeout"] != nil {
			t.Fatalf("log = %v", line)
		}
	}
}

// 流的上限从发出上游请求算起，不是从收到响应头算起：上游停 300 毫秒才回响应头、之后慢慢发，流在请求之后约 chat 超时
// （500 毫秒）就以 upstream_timeout 截断；从响应头算起的话要到约 800 毫秒。
func TestStreamLimitCountsFromTheRequest(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("sse-late"))
	events, end := s.rest(t)
	if end != io.EOF || len(events) == 0 {
		t.Fatalf("end %v, %d events", end, len(events))
	}
	checkErrorEvent(t, events[len(events)-1], "upstream_timeout")
	line := h.logLine(t, s.resp.Header.Get("x-request-id"))
	if d := time.Duration(line["duration_ms"].(float64)) * time.Millisecond; d > chatTimeout+150*time.Millisecond || line["error_code"] != "upstream_timeout" {
		t.Fatalf("the stream lived %v (chat timeout %v), log %v", d, chatTimeout, line)
	}
}

// usage 放在带内容的事件里、choices 为空但 usage 为 null 的事件、再一个只带 usage 的事件：只有最后这种是 QF 替客户端要的、
// 客户端没要时去掉；前两个照转发。用量取最后一个合法的 usage，客户端要不要都一样。
func TestStreamUsageEvents(t *testing.T) {
	h := newHarness(t)
	upstream := splitEvents([]byte(sseUsageTwice))
	for body, want := range map[string][]string{
		streamOf("sse-usage-twice"): {upstream[0], upstream[1], upstream[3]},
		`{"model":"sse-usage-twice","stream":true,"stream_options":{"include_usage":true}}`: upstream,
	} {
		s := h.openStream(t, clientKey, body)
		events, end := s.rest(t)
		if end != io.EOF || !slices.Equal(events, want) {
			t.Fatalf("%s: end %v, events %q", body, end, events)
		}
		line := h.logLine(t, s.resp.Header.Get("x-request-id"))
		if u, _ := line["usage"].(map[string]any); line["outcome"] != "SETTLED" || u["total_tokens"] != float64(11) || u["prompt_tokens"] != float64(5) {
			t.Fatalf("%s: log %v", body, line)
		}
	}
}

// 流开始之前就失败的（上游非 2xx、上游超时、2xx 却不是 SSE）：和非流式一样，返回 JSON 错误。
func TestStreamFailuresBeforeTheStream(t *testing.T) {
	h := newHarness(t)
	for model, want := range map[string]string{"fake-500": "upstream_error", "fake-401": "upstream_auth_failed", "fake-429": "upstream_rate_limited",
		"fake-hang": "upstream_timeout", "sse-json-type": "upstream_invalid_response", "sse-no-type": "upstream_invalid_response", "dead": "upstream_unavailable"} {
		r := h.chat(t, streamOf(model))
		if errorCode(t, r) != want || r.header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: status %d, body %s", model, r.status, r.body)
		}
	}
}

// 带幂等键的流式：照常记键，不存快照，结算了的只记一个标记。下面几个测试按情形分开，每个最后都校验账本没有违例。

// 完整的流结算了：同一个键再来是 409 idempotency_response_unavailable，不再调上游，账本不变。
func TestStreamIdempotencyComplete(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	body := streamOf("shared")
	first := e.openStream(t, e.key, body, "Idempotency-Key", "stream-1")
	if _, end := first.rest(t); end != io.EOF {
		t.Fatal(end)
	}
	e.logLine(t, first.resp.Header.Get("x-request-id"))
	_, reserved, consumed := e.budget(t)
	e.retryGets(t, body, "stream-1", "idempotency_response_unavailable")
	if _, r2, c2 := e.budget(t); r2 != reserved || c2 != consumed {
		t.Fatalf("the retry changed the budget: reserved %d → %d, consumed %d → %d", reserved, r2, consumed, c2)
	}
	if n := e.fake.Stats().ChatCompletions; n != 1 {
		t.Fatalf("the upstream got %d requests", n)
	}
	dbtest.Verify(t, e.store)
}

// 流还没结束：同一个键再来是 409 idempotency_in_progress。
func TestStreamIdempotencyInProgress(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	held := e.openStream(t, e.key, streamOf("fake-stream-step"), "Idempotency-Key", "stream-2")
	e.step(t)
	held.next(t)
	e.retryGets(t, streamOf("fake-stream-step"), "stream-2", "idempotency_in_progress")
	for range fakeStreamEvents - 1 {
		e.step(t)
	}
	held.rest(t)
	if n := e.fake.Stats().ChatCompletions; n != 1 {
		t.Fatalf("the upstream got %d requests", n)
	}
	dbtest.Verify(t, e.store)
}

// 截断的流（用量未知）不写标记：重试是 409 idempotency_outcome_unknown。上游 4xx 的（确定没生成）可以重新执行。
func TestStreamIdempotencyCutAndRejected(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	cut := e.openStream(t, e.key, streamOf("fake-stream-cut"), "Idempotency-Key", "stream-5")
	cut.rest(t)
	e.logLine(t, cut.resp.Header.Get("x-request-id"))
	e.retryGets(t, streamOf("fake-stream-cut"), "stream-5", "idempotency_outcome_unknown")
	rejected := streamOf("fake-401")
	e.retryGets(t, rejected, "stream-3", "upstream_auth_failed")
	e.retryGets(t, rejected, "stream-3", "upstream_auth_failed")
	if n := e.fake.Stats().ChatCompletions; n != 3 {
		t.Fatalf("the upstream got %d requests, want 3", n)
	}
	dbtest.Verify(t, e.store)
}

// 正常结束、但没有 usage 的流：用量未知，不写标记，重试是 409 idempotency_outcome_unknown，上游只收到一次。
func TestStreamIdempotencyNoUsage(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	noUsage := e.openStream(t, e.key, streamOf("fake-no-usage"), "Idempotency-Key", "stream-6")
	if events, end := noUsage.rest(t); end != io.EOF || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("no-usage stream: end %v", end)
	}
	if l := e.logLine(t, noUsage.resp.Header.Get("x-request-id")); l["outcome"] != "USAGE_UNKNOWN" || l["error_code"] != nil {
		t.Fatalf("no-usage stream: log %v", l)
	}
	e.retryGets(t, streamOf("fake-no-usage"), "stream-6", "idempotency_outcome_unknown")
	if n := e.fake.Stats().ChatCompletions; n != 1 {
		t.Fatalf("the upstream got %d requests", n)
	}
	dbtest.Verify(t, e.store)
}

// 客户端在流开始之前就走了：QF 照样读完、结算，也只记标记，重试是 409 idempotency_response_unavailable，不会得到空的重放。
func TestStreamIdempotencyClientGoneBeforeTheStream(t *testing.T) {
	e := newStreamDBEnv(t, 100000, nil)
	ctx, cancel := context.WithCancel(context.Background())
	gone := make(chan reply, 1)
	go func() { gone <- e.post(ctx, chatRoute, e.key, streamOf("slow"), "Idempotency-Key", "stream-4") }()
	waitFor(t, func() bool { return e.fake.Stats().ChatCompletions == 1 })
	cancel()
	<-gone
	waitFor(t, func() bool {
		return slices.ContainsFunc(e.logs.lines(t), func(l map[string]any) bool { return l["idempotency_key"] == "stream-4" && l["outcome"] == "SETTLED" })
	})
	e.retryGets(t, streamOf("slow"), "stream-4", "idempotency_response_unavailable")
	dbtest.Verify(t, e.store)
}

// retryGets 用幂等键 key 发 body，要求得到错误码 code。
func (e *dbEnv) retryGets(t *testing.T, body, key, code string) {
	t.Helper()
	if r := e.send(t, chatRoute, body, "Idempotency-Key", key); errorCode(t, r) != code {
		t.Fatalf("key %s: %d %s, want %s", key, r.status, r.body, code)
	}
}

// 收尾失败：客户端已经收完了整个流，看不出区别；调用留在 IN_FLIGHT 给回收，日志有结局和用量。
func TestStreamFinishFailure(t *testing.T) {
	e := newStreamDBEnv(t, 100000, func(l Ledger) Ledger { return &faulty{Ledger: l, finishErr: errors.New("commit: invalid connection")} })
	s := e.openStream(t, e.key, streamOf("shared"))
	events, end := s.rest(t)
	if end != io.EOF || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("end %v, events %q", end, events)
	}
	id := s.resp.Header.Get("x-request-id")
	line := e.logLine(t, id)
	if c := e.call(t, id); c.status.String != "IN_FLIGHT" || line["outcome"] != "SETTLED" || line["call_status"] != "IN_FLIGHT" || line["usage"] == "unknown" {
		t.Fatalf("call %s, log %v", c.status.String, line)
	}
}

// 写的期限不带到下一个请求（net/http 在请求结束时清掉它）：同一条 keep-alive 连接上，等上游超时的到期时间过了再发一个普通请求，照常成功。
func TestStreamClearsTheWriteDeadline(t *testing.T) {
	h := newHarness(t)
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	send := func(body string) (*http.Response, bool) {
		reused := false
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.qf.URL+chatRoute, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+clientKey)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp, reused
	}
	start := time.Now()
	if resp, _ := send(streamOf("shared")); resp.StatusCode != http.StatusOK {
		t.Fatal(resp.Status)
	}
	time.Sleep(time.Until(start.Add(chatTimeout + 200*time.Millisecond))) // 流里最后一次写的期限不晚于这个时刻
	resp, reused := send(`{"model":"shared"}`)
	if resp.StatusCode != http.StatusOK || !reused {
		t.Fatalf("status %d, reused %v", resp.StatusCode, reused)
	}
}

// 收尾之后重新设写的期限：最后几个事件的写期限就是上游超时的到期时间；收尾慢、handler 过了那个时刻才返回时，
// net/http 还要在这条连接上写分块编码的结尾，旧的期限留着就写不出去，客户端收到了 [DONE]，流却没有正常结束。
func TestStreamEndsCleanlyAfterASlowFinish(t *testing.T) {
	h := newHarnessWith(t, slowFinish{Ledger: newAllowAll(), d: chatTimeout})
	s := h.openStream(t, clientKey, streamOf("stream-quick"))
	events, end := s.rest(t)
	if end != io.EOF || len(events) == 0 || events[len(events)-1] != "data: [DONE]\n\n" {
		t.Fatalf("end %v, %d events", end, len(events))
	}
}

// slowFinish 的收尾先等 d 再做。
type slowFinish struct {
	Ledger
	d time.Duration
}

func (s slowFinish) Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error {
	time.Sleep(s.d)
	return s.Ledger.Finish(ctx, r, res)
}

// 流式的 TPM 修正和非流式一样按结局：结算的加回“估算 − 实际”，未知的不动。
func TestStreamAdjust(t *testing.T) {
	for model, settled := range map[string]bool{"shared": true, "fake-stream-cut": false} {
		lim := &stubLimiter{}
		h := newHarnessLimited(t, &rated{Ledger: newAllowAll(), key: bothRates}, lim)
		s := h.openStream(t, clientKey, streamOf(model))
		s.rest(t)
		line := h.logLine(t, s.resp.Header.Get("x-request-id"))
		admits, adjusts := lim.calls()
		if !settled {
			if len(adjusts) != 0 {
				t.Fatalf("%s: adjusted %v", model, adjusts)
			}
			continue
		}
		u := line["usage"].(map[string]any)
		want := admits[0][1].Cost - int64(u["total_tokens"].(float64))
		if len(adjusts) != 1 || adjusts[0][0].Cost != -want {
			t.Fatalf("%s: adjusts %+v, want a refund of %d", model, adjusts, want)
		}
	}
}

// 流式的观测：根 span 和日志标明流式、第一个事件的时间；upstream 子 span 包住整个流；QF 自身耗时不含流的时间；
// 两个流式的指标。
func TestStreamTelemetry(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, withModel(streamBody, `"fake-stream-cut"`))
	s.rest(t)
	id := s.resp.Header.Get("x-request-id")
	h.logLine(t, id)
	slow := h.openStream(t, clientKey, streamOf("stream-slow"))
	slow.rest(t)
	slowID := slow.resp.Header.Get("x-request-id")
	h.logLine(t, slowID)

	root, children := h.rootSpan(t, slowID)
	if attr(root, "qf.stream") != "true" || attr(root, "qf.stream.first_event_ms") == "" || attr(root, "error.type") != "upstream_timeout" {
		t.Fatalf("root attributes %v", root.Attributes())
	}
	up := children[slices.IndexFunc(children, func(c sdktrace.ReadOnlySpan) bool { return c.Name() == "upstream" })]
	// 两个时间戳各取一次时钟，粗的时钟（约 1 毫秒一跳）上相减可能比超时短不到一跳：留 2 毫秒。
	if d := up.EndTime().Sub(up.StartTime()); d < chatTimeout-2*time.Millisecond {
		t.Fatalf("the upstream span lasted %v, shorter than the stream", d)
	}
	m := h.metrics
	if got := testutil.ToFloat64(m.Streams.WithLabelValues("upstream_error", "connected")); got != 1 {
		t.Errorf("streams{upstream_error,connected} = %v", got)
	}
	if got := testutil.ToFloat64(m.Streams.WithLabelValues("upstream_timeout", "connected")); got != 1 {
		t.Errorf("streams{upstream_timeout,connected} = %v", got)
	}
	text := scrape(t, h)
	for _, series := range []string{`qf_overhead_seconds_bucket{endpoint="chat",le="0.25"} 2`, `qf_stream_first_event_seconds_count{endpoint="chat"} 2`,
		`qf_requests_total{code="upstream_timeout",endpoint="chat",status="200"} 1`} {
		if !strings.Contains(text, series+"\n") {
			t.Errorf("/metrics lacks %s", series)
		}
	}
}

// 第一个事件和调用开始落在单调时钟的同一跳里（时长为 0）：照样算收到了，指标、根 span 的属性、日志字段都记 0。
func TestStreamFirstEventAtTheStart(t *testing.T) {
	m := telemetry.NewMetrics(func() sql.DBStats { return sql.DBStats{} })
	g := &gateway{tel: telemetry.Disabled(m)}
	start := time.Now()
	c := &call{start: start, stream: true, firstEventAt: start}
	g.observe(c, "chat", 0)
	if n := testutil.CollectAndCount(m.StreamFirstEvent); n != 1 {
		t.Errorf("qf_stream_first_event_seconds has %d series, want 1", n)
	}
	if attrs := rootAttrs(c, "chat"); !slices.Contains(attrs, attrFirstEventMS.Int64(0)) {
		t.Errorf("root attributes %v lack qf.stream.first_event_ms=0", attrs)
	}
	if attrs := clientLogAttrs(nil, c); !slices.ContainsFunc(attrs, func(a slog.Attr) bool { return a.Equal(slog.Int64("first_event_ms", 0)) }) {
		t.Errorf("log attributes %v lack first_event_ms=0", attrs)
	}
}

// first_event_ms 是第一个事件的时刻，不被后面的事件覆盖：stream-quick 每个事件之前等 20 毫秒，第一个事件之后
// 至少还有 12 个间隔（240 毫秒）才结束，所以整个调用的时长比 first_event_ms 至少长 200 毫秒。
func TestStreamFirstEventIsTheFirst(t *testing.T) {
	h := newHarness(t)
	s := h.openStream(t, clientKey, streamOf("stream-quick"))
	s.rest(t)
	line := h.logLine(t, s.resp.Header.Get("x-request-id"))
	first, total := line["first_event_ms"].(float64), line["duration_ms"].(float64)
	if total-first < 200 {
		t.Fatalf("first_event_ms %v, duration_ms %v: the first event's time was overwritten by a later one", first, total)
	}
}

// 非流式请求里的 stream_options 不读、不改，写成什么样都原样转发（流式时会拒绝的变体也一样）。
func TestNonStreamIgnoresStreamOptions(t *testing.T) {
	h := newHarness(t)
	body := `{"model":"shared","stream":false,"stream_options":{"include_usage":false},"Stream_Options":{"x":1}}`
	if r := h.chat(t, body); r.status != http.StatusOK {
		t.Fatalf("status %d %s", r.status, r.body)
	}
	if sent, _, _ := h.upstreamSaw(); string(sent) != strings.Replace(body, `"shared"`, `"fake-chat"`, 1) {
		t.Fatalf("upstream got %s", sent)
	}
}

// newStreamDBEnv 是用真账本、chat 超时为 streamChat 的 QF。
func newStreamDBEnv(t *testing.T, limit int64, wrap func(Ledger) Ledger) *dbEnv {
	t.Helper()
	return newDBEnvWith(t, limit, wrap, func(l Ledger) *harness { return newStreamHarness(t, l) })
}

// 按事件读：到空行为止，\n 和 \r\n 都认；多行 data 用 \n 连起来，注释和别的字段不进 data；事件正好 1 MiB 可以，多一个字节不行；
// 在两个事件之间读完是 io.EOF，在事件中途读完是 io.ErrUnexpectedEOF。
func TestEventReader(t *testing.T) {
	fill := func(total int) string { return "data: " + strings.Repeat("x", total-len("data: ")-2) + "\n\n" }
	cases := []struct {
		in     string
		events []string
		data   []string
		end    error
	}{
		{"data: a\n\ndata: [DONE]\n\n", []string{"data: a\n\n", "data: [DONE]\n\n"}, []string{"a", "[DONE]"}, io.EOF},
		{"data: a\r\n\r\n", []string{"data: a\r\n\r\n"}, []string{"a"}, io.EOF},
		{": c\nevent: x\ndata: {\"a\":\ndata:1}\nid: 7\n\n", []string{": c\nevent: x\ndata: {\"a\":\ndata:1}\nid: 7\n\n"}, []string{"{\"a\":\n1}"}, io.EOF},
		{": keep-alive\n\n", []string{": keep-alive\n\n"}, []string{""}, io.EOF},
		{"data: a\n", nil, nil, io.ErrUnexpectedEOF},
		{"data: a\r", nil, nil, io.ErrUnexpectedEOF},
		{fill(maxStreamEvent), []string{fill(maxStreamEvent)}, nil, io.EOF},
		{fill(maxStreamEvent + 1), nil, nil, errEventTooLarge},
		{"data: a\rdata: b\r\r", nil, nil, io.ErrUnexpectedEOF}, // 只用 \r 的行尾读不到空行
	}
	for i, tc := range cases {
		r := eventReader{r: bufio.NewReader(strings.NewReader(tc.in))}
		var events []string
		var end error
		for {
			raw, err := r.next()
			if err != nil {
				end = err
				break
			}
			events = append(events, string(raw))
		}
		if !slices.Equal(events, tc.events) || end != tc.end {
			t.Errorf("case %d: %d events, end %v", i, len(events), end)
			continue
		}
		for j, d := range tc.data {
			if got, _ := eventData([]byte(events[j])); string(got) != d {
				t.Errorf("case %d event %d: data %q, want %q", i, j, got, d)
			}
		}
	}
}
