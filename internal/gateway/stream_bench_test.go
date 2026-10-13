package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
)

// 流式的开销：QF 处理一个已经到手的上游应答要多少 CPU 和内存，不含网络和上游。流式是 relay（按事件读、解析、
// 去掉 usage 块、逐个写出并刷新），非流式是 exchange 里读完正文之后的部分（整个读进来、取 id、model、usage）。
// 两边的上游应答都是假上游的同一个回答（10 个内容块），写给客户端的一侧丢弃。

func fakeAnswer(b *testing.B, body string) []byte {
	b.Helper()
	rec := httptest.NewRecorder()
	fakeupstream.New().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	return rec.Body.Bytes()
}

type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(int)             {}
func (d *discardWriter) Flush()                      {}

func BenchmarkRelay(b *testing.B) {
	answer := fakeAnswer(b, `{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"content":"hello"}]}`)
	g := &gateway{}
	ctx := context.Background()
	b.SetBytes(int64(len(answer)))
	b.ReportAllocs()
	for b.Loop() {
		upCtx, cancel := context.WithTimeout(ctx, time.Minute)
		c := &call{purpose: config.Chat, start: time.Now(), stream: true}
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}},
			Body: io.NopCloser(bytes.NewReader(answer))}
		if _, e := g.relay(ctx, upCtx, &discardWriter{h: http.Header{}}, c, resp); e != nil || c.usage == nil {
			b.Fatal(e)
		}
		cancel()
	}
}

func BenchmarkNonStreamResponse(b *testing.B) {
	answer := fakeAnswer(b, `{"model":"m","messages":[{"content":"hello"}]}`)
	b.SetBytes(int64(len(answer)))
	b.ReportAllocs()
	for b.Loop() {
		c := &call{purpose: config.Chat}
		body, err := io.ReadAll(io.LimitReader(bytes.NewReader(answer), maxUpstreamBody+1))
		if err != nil || !c.recordResponse(body) || c.usage == nil {
			b.Fatal("no usage")
		}
	}
}
