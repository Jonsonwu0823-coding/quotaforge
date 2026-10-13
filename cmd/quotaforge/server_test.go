package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// 两个服务的设置：读请求头 5 秒、读整个请求 30 秒、空闲连接 120 秒；不设全局的写期限（会掐断长流）。
func TestServerTimeouts(t *testing.T) {
	s := newServer(http.NotFoundHandler())
	if s.ReadHeaderTimeout != 5*time.Second || s.ReadTimeout != 30*time.Second || s.IdleTimeout != 120*time.Second || s.WriteTimeout != 0 {
		t.Fatalf("read header %v, read %v, idle %v, write %v", s.ReadHeaderTimeout, s.ReadTimeout, s.IdleTimeout, s.WriteTimeout)
	}
}

// serveShort 用读、空闲上限都改短的 newServer 提供 h，返回地址。
func serveShort(t *testing.T, h http.Handler, read, idle time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := newServer(h)
	srv.ReadTimeout, srv.IdleTimeout = read, idle
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, ln, srv, 10*time.Second) }()
	t.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String()
}

// closedWithin：服务端在 d 之内关掉连接（客户端读到 EOF 或连接被重置）。
func closedWithin(t *testing.T, conn net.Conn, d time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("the server kept the connection open for %v", d)
	}
}

// 只发一半请求体的客户端：在读整个请求的上限到点时被断开，不能一直占住连接。
func TestServerCutsHalfSentBody(t *testing.T) {
	addr := serveShort(t, newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
	})), 300*time.Millisecond, time.Minute)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: qf\r\nContent-Length: 100\r\n\r\n{\"model\":")
	closedWithin(t, conn, 5*time.Second)
}

// keep-alive 的连接空闲超过上限就被关掉。
func TestServerClosesIdleConnections(t *testing.T) {
	addr := serveShort(t, newHandler(http.NotFoundHandler()), time.Minute, 300*time.Millisecond)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "GET /healthz HTTP/1.1\r\nHost: qf\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil || resp.StatusCode != http.StatusOK || resp.Close {
		t.Fatalf("response %v, err %v", resp, err)
	}
	closedWithin(t, conn, 5*time.Second)
}

// gatewayConfig 写一份接假上游的配置（chat 超时 10 秒）：stream 按步走的流，slow 600 毫秒才回答，flow 事件之间各 100 毫秒；
// 建账户、发 Key，返回配置文件的路径、配置、Key 和假上游。
func gatewayConfig(t *testing.T) (string, *config.Config, string, *fakeupstream.Server) {
	t.Helper()
	fake := fakeupstream.New()
	up := httptest.NewServer(fake)
	t.Cleanup(up.Close)
	path := filepath.Join(t.TempDir(), "config.json")
	data := fmt.Sprintf(`{"timeouts":{"chat_ms":10000},"upstreams":[{"name":"fake","base_url":%q,"api_key_env":"QF_KILL_UPSTREAM_KEY"}],
		"models":[{"alias":"step","purpose":"chat","upstream":"fake","upstream_model":"fake-stream-step","default_max_tokens":16},
			{"alias":"slow","purpose":"chat","upstream":"fake","upstream_model":"fake-slow-600","default_max_tokens":16},
			{"alias":"flow","purpose":"chat","upstream":"fake","upstream_model":"fake-stream-slow-100","default_max_tokens":16}],
		"database":{"dsn_env":"QF_KILL_DSN"},"redis":{"url_env":"QF_KILL_REDIS"}}`, up.URL+"/v1")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"QF_KILL_UPSTREAM_KEY": "test-only-upstream-key", "QF_KILL_DSN": dbtest.AppDSN(t), "QF_KILL_REDIS": dbtest.RedisURL(t)}
	for k, v := range env {
		t.Setenv(k, v)
	}
	cfg, err := config.Load(path, os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	store := dbtest.Store(t)
	issued, err := store.IssueKey(context.Background(), dbtest.Account(t, store, 1_000_000), []string{"step", "slow", "flow"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	return path, cfg, issued.Secret, fake
}

// readTimeout 是 serveGateway 的读整个请求的上限：比下面的长调用都短。
const readTimeout = 300 * time.Millisecond

// serveGateway 用真的网关和账本、读整个请求的上限改成 readTimeout 的服务器提供 API，返回地址、Key、假上游和日志。
func serveGateway(t *testing.T) (string, string, *fakeupstream.Server, *syncLog) {
	t.Helper()
	_, cfg, key, fake := gatewayConfig(t)
	limiter, err := ratelimit.Open(dbtest.RedisURL(t), "qf:test:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { limiter.Close() })
	logs := &syncLog{}
	h := newHandler(gateway.New(cfg, dbtest.Store(t), limiter, slog.New(slog.NewJSONHandler(logs, nil)), telemetry.Disabled(telemetry.NewMetrics(noDBStats))))
	return serveShort(t, h, readTimeout, time.Minute), key, fake, logs
}

func postChat(t *testing.T, client *http.Client, ctx context.Context, addr, key, model string, stream bool) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"stream":%v}`, model, stream)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// stepFake 放出 fake-stream-step 的下一个事件，等假上游把它写出去（再放下一个之前，它一定已经在等新的 Release）。
func stepFake(t *testing.T, fake *fakeupstream.Server) {
	t.Helper()
	waitUntil(t, 10*time.Second, func() bool { return fake.Stats().Held == 1 })
	sent := fake.Stats().StreamEvents
	fake.Release()
	waitUntil(t, 10*time.Second, func() bool { return fake.Stats().StreamEvents > sent })
}

// 读整个请求的上限只管读请求：网关读完请求体就清掉它留在连接上的读期限。比上限长的流和比上限久的非流式调用都照常完成，
// 同一条连接接着用。
func TestServerReadTimeoutSparesLongCalls(t *testing.T) {
	addr, key, _, logs := serveGateway(t)
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	start := time.Now()
	resp := postChat(t, client, context.Background(), addr, key, "flow", true)
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || !strings.HasSuffix(string(b), "data: [DONE]\n\n") || time.Since(start) < 2*readTimeout {
		t.Fatalf("stream: err %v, took %v, ends %q", err, time.Since(start), b[max(0, len(b)-40):])
	}
	start = time.Now()
	resp = postChat(t, client, context.Background(), addr, key, "slow", false)
	b, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "fake reply") || time.Since(start) < 2*readTimeout {
		t.Fatalf("slow call: %d, err %v, took %v", resp.StatusCode, err, time.Since(start))
	}
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(l, "client_disconnected") || !strings.Contains(l, `"outcome":"SETTLED"`) {
			t.Fatalf("log %s", l)
		}
	}
}

// 清掉读期限之后，客户端真的在流的中途走了，照样发现（日志 client_disconnected），接着读完上游、照常结算。
func TestServerStillNoticesDisconnects(t *testing.T) {
	addr, key, fake, logs := serveGateway(t)
	ctx, cancel := context.WithCancel(context.Background())
	resp := postChat(t, http.DefaultClient, ctx, addr, key, "step", true)
	defer resp.Body.Close()
	id := resp.Header.Get("x-request-id")
	r := bufio.NewReader(resp.Body)
	stepFake(t, fake)
	if line, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data: ") {
		t.Fatalf("first event %q %v", line, err)
	}
	cancel()
	for range 13 {
		stepFake(t, fake)
	}
	waitUntil(t, 5*time.Second, func() bool { return strings.Contains(logs.String(), id) })
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, id) && (!strings.Contains(l, `"client_disconnected":true`) || !strings.Contains(l, `"outcome":"SETTLED"`)) {
			t.Fatalf("log %s", l)
		}
	}
}

// 流的中途收到 SIGTERM：退出时等这个流读完、收尾，客户端收到 [DONE]，调用结算，进程正常退出。
// 不在 Windows 上跑：Go 在那里发不了 SIGTERM。
func TestProcessFinishesStreamOnSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go cannot send SIGTERM on Windows")
	}
	cfgPath, _, key, fake := gatewayConfig(t)
	p := startQF(t, cfgPath)
	req, _ := http.NewRequest(http.MethodPost, "http://"+p.addr+"/v1/chat/completions", strings.NewReader(`{"model":"step","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	id := resp.Header.Get("x-request-id")
	r := bufio.NewReader(resp.Body)
	stepFake(t, fake)
	if line, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(line, "data: ") {
		t.Fatalf("first event %q %v", line, err)
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, func() bool { return !listening(p.addr) })
	for range 13 {
		stepFake(t, fake)
	}
	rest, err := io.ReadAll(r)
	if err != nil || !strings.HasSuffix(string(rest), "data: [DONE]\n\n") {
		t.Fatalf("rest of the stream: err %v, ends %q", err, rest[max(0, len(rest)-40):])
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("QF exited with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("QF did not exit")
	}
	if s := callStatus(t, dbtest.Admin(t), id); s != "SETTLED" {
		t.Fatalf("call %s", s)
	}
}
