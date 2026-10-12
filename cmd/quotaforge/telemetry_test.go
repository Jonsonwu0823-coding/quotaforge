package main

import (
	"bufio"
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
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

func noDBStats() sql.DBStats { return sql.DBStats{} }

// 对外的监听地址上没有 /metrics；健康检查、404、405 都不建 span。
func TestMetricsAreNotOnTheAPIListener(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tel := telemetry.Enabled(telemetry.NewTracerProvider(spans, 1, "t"), telemetry.NewMetrics(noDBStats))
	h := newHandler(gateway.New(ledgerConfig(t, "unused"), nil, nil, slog.New(slog.NewJSONHandler(io.Discard, nil)), tel))
	for _, tc := range []struct {
		method, path string
		code         int
	}{
		{http.MethodGet, "/metrics", http.StatusNotFound},
		{http.MethodGet, "/v1/metrics", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		h.ServeHTTP(rec, req)
		if rec.Code != tc.code || strings.Contains(rec.Body.String(), "qf_requests_total") {
			t.Errorf("%s %s: status %d", tc.method, tc.path, rec.Code)
		}
	}
	if n := len(spans.Ended()); n != 0 {
		t.Fatalf("%d spans for health checks and unknown paths", n)
	}
}

// 指标在自己的监听地址上，只有 GET /metrics。
func TestServeMetrics(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := telemetry.NewMetrics(noDBStats)
	m.Requests.WithLabelValues("chat", "200", "").Inc()
	stop := serveMetrics(ln, m)
	base := "http://" + ln.Addr().String()
	body := get(t, base+"/metrics", http.StatusOK)
	if !strings.Contains(body, `qf_requests_total{code="",endpoint="chat",status="200"} 1`) || !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics:\n%s", body)
	}
	get(t, base+"/healthz", http.StatusNotFound)
	stop()
	if _, err := http.Get(base + "/metrics"); err == nil {
		t.Fatal("the metrics listener is still serving after stop")
	}
}

func get(t *testing.T, url string, want int) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("GET %s: %d, want %d", url, resp.StatusCode, want)
	}
	return string(b)
}

// 退出的总时长：在途请求最多 ShutdownTimeout，之后冲刷 span 最多 FlushTimeout；两者之和要小于 Compose 的宽限期，
// 否则 docker compose stop 会在冲刷时把 QF 杀掉。
func TestStopGracePeriodCoversShutdown(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`stop_grace_period: (\d+)s`).FindSubmatch(data)
	if m == nil {
		t.Fatal("compose.yaml has no stop_grace_period")
	}
	grace, _ := strconv.Atoi(string(m[1]))
	if total := ledgerConfig(t, "unused").ShutdownTimeout() + telemetry.FlushTimeout; total >= time.Duration(grace)*time.Second {
		t.Fatalf("shutdown can take %v, the grace period is %ds", total, grace)
	}
}

// 后台循环的指标：间隔；三件都成功的一轮记下时间；出错按任务计，不算成功。
func TestBackgroundMetrics(t *testing.T) {
	store := dbtest.Store(t)
	m := telemetry.NewMetrics(store.Stats)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		background(ctx, store, time.Hour, 20*time.Millisecond, slog.New(slog.NewJSONHandler(io.Discard, nil)), m)
	}()
	waitUntil(t, 10*time.Second, func() bool { return testutil.ToFloat64(m.BackgroundLastSuccess) > 0 })
	cancel()
	<-done
	if got := testutil.ToFloat64(m.BackgroundInterval); got != 0.02 {
		t.Fatalf("interval %v", got)
	}
	if age := time.Since(time.Unix(int64(testutil.ToFloat64(m.BackgroundLastSuccess)), 0)); age > time.Minute {
		t.Fatalf("last success %v ago", age)
	}

	closed := dbtest.Store(t)
	closed.Close()
	broken := telemetry.NewMetrics(closed.Stats)
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		defer close(done)
		background(ctx, closed, time.Hour, 20*time.Millisecond, slog.New(slog.NewJSONHandler(io.Discard, nil)), broken)
	}()
	waitUntil(t, 10*time.Second, func() bool {
		return testutil.ToFloat64(broken.BackgroundErrors.WithLabelValues("unknown_scan")) > 0
	})
	cancel()
	<-done
	for _, task := range []string{"reclaim", "purge", "unknown_scan"} {
		if testutil.ToFloat64(broken.BackgroundErrors.WithLabelValues(task)) == 0 {
			t.Errorf("the failed %s was not counted", task)
		}
	}
	if testutil.ToFloat64(broken.BackgroundLastSuccess) != 0 {
		t.Fatal("a failed round counted as a success")
	}
}

// 整个进程：配了指标地址和 OTLP 地址时，指标在自己的地址上；调用的 span 导出到收集端，退出时冲刷；日志行带 trace_id。
// 不在 Windows 上发 SIGTERM（Go 不支持），那里等批处理器到点导出，再杀掉进程。
func TestProcessWithTelemetry(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	collector, exported := spanSink(t)
	cfgPath, key, _ := processConfig(t)
	p := startObservedQF(t, cfgPath, collector)
	api, metricsAddr := p.addr(t, "listening"), p.addr(t, "metrics")

	req, _ := http.NewRequest(http.MethodPost, "http://"+api+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	get(t, "http://"+api+"/metrics", http.StatusNotFound)
	if body := get(t, "http://"+metricsAddr+"/metrics", http.StatusOK); !strings.Contains(body, `qf_requests_total{code="",endpoint="chat",status="200"} 1`) {
		t.Fatalf("/metrics:\n%s", body)
	}

	p.stop(t, func() bool { return strings.Contains(exported(), traceID) })
	for _, want := range []string{`"name":"POST /v1/chat/completions"`, `"kind":2`, `"stringValue":"qf-process-test"`, `"name":"upstream"`} {
		if !strings.Contains(exported(), want) {
			t.Errorf("exported spans lack %s", want)
		}
	}
	for _, l := range p.logLines() {
		if _, bad := l["not_json"]; bad {
			t.Errorf("a log line is not JSON: %v", l)
		}
		if l["msg"] == "call" && l["trace_id"] != traceID {
			t.Errorf("call log line %v", l)
		}
	}
}

// 退出的顺序：收到信号时有请求在途，先等它处理完，再冲刷 span；这个请求的根 span 和 upstream 都要导出。
// 先冲刷（和排空同时进行）的话，在途请求的 span 在批处理器关掉之后才结束，会丢。
//
// 全程等确定的事件：假上游挂住了这个请求（它确实在途）；信号之后监听已关（QF 进入排空）；然后再放开上游。
// 唯一一处限时的等待是“收集端有没有在放开之前收到过东西”：先冲刷的实现几毫秒内就会发出第一批，限时只决定能不能抓到它，
// 正确的实现不管等多久都通过。不在 Windows 上跑：Go 在那里发不了 SIGTERM。
func TestProcessDrainsInFlightRequestBeforeFlushing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go cannot send SIGTERM on Windows")
	}
	collector, exported := spanSink(t)
	cfgPath, key, upstream := processConfig(t)
	p := startObservedQF(t, cfgPath, collector)
	api := p.addr(t, "listening")

	status := chatStatus(api, key, "hold")
	waitUntil(t, 20*time.Second, func() bool { return upstream.Stats().Held == 1 })

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 20*time.Second, func() bool { return !listening(api) })
	for deadline := time.Now().Add(2 * time.Second); exported() == "" && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
	}
	upstream.Release()

	select {
	case code := <-status:
		if code != http.StatusOK {
			t.Fatalf("the in-flight request got %d, want 200", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the in-flight request did not finish")
	}
	p.waitExit(t)
	for _, want := range []string{`"name":"POST /v1/chat/completions"`, `"name":"upstream"`} {
		if !strings.Contains(exported(), want) {
			t.Errorf("the spans exported on exit lack %s", want)
		}
	}
}

// chatStatus 在另一个 goroutine 里发一个 chat 请求，把响应的状态码送进返回的通道（请求失败是 -1）。
func chatStatus(api, key, model string) <-chan int {
	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+api+"/v1/chat/completions", strings.NewReader(`{"model":"`+model+`"}`))
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- -1
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	return status
}

// listening 说 addr 上现在连得上吗。
func listening(addr string) bool {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// spanSink 是一个记下收到的正文的假收集端；返回它的地址和读出已收到内容的函数。
func spanSink(t *testing.T) (url string, received func() string) {
	var mu sync.Mutex
	var bodies []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b...)
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() string {
		mu.Lock()
		defer mu.Unlock()
		return string(bodies)
	}
}

// processConfig 写一份接假上游的配置（别名 m 正常回答，hold 被假上游挂住直到 Release），建账户、发 Key；
// 返回配置文件的路径、Key 和假上游。
func processConfig(t *testing.T) (cfgPath, key string, upstream *fakeupstream.Server) {
	upstream = fakeupstream.New()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	cfgPath = filepath.Join(t.TempDir(), "config.json")
	cfgJSON := fmt.Sprintf(`{"upstreams":[{"name":"fake","base_url":%q,"api_key_env":"QF_KILL_UPSTREAM_KEY"}],
		"models":[{"alias":"m","purpose":"chat","upstream":"fake","upstream_model":"fake-chat","default_max_tokens":16},
			{"alias":"hold","purpose":"chat","upstream":"fake","upstream_model":"fake-hold","default_max_tokens":16}],
		"database":{"dsn_env":"QF_KILL_DSN"},"redis":{"url_env":"QF_KILL_REDIS"}}`, up.URL+"/v1")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QF_KILL_UPSTREAM_KEY", "test-only-upstream-key")
	store := dbtest.Store(t)
	issued, err := store.IssueKey(context.Background(), dbtest.Account(t, store, 1_000_000), []string{"m", "hold"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	return cfgPath, issued.Secret, upstream
}

// observedQF 是开着追踪和指标的 QF 子进程，记下它的每一行日志（不是 JSON 的记成 not_json）。
type observedQF struct {
	cmd    *exec.Cmd
	exited chan error
	mu     sync.Mutex
	lines  []map[string]any
}

func startObservedQF(t *testing.T, cfgPath, collector string) *observedQF {
	p := &observedQF{cmd: exec.Command(os.Args[0]), exited: make(chan error, 1)}
	p.cmd.Env = append(os.Environ(), "QUOTAFORGE_TEST_SERVE=1", "QUOTAFORGE_CONFIG="+cfgPath, "QUOTAFORGE_ADDR=127.0.0.1:0",
		"QUOTAFORGE_METRICS_ADDR=127.0.0.1:0", "QUOTAFORGE_OTLP_ENDPOINT="+collector, "QUOTAFORGE_INSTANCE=qf-process-test",
		"QF_KILL_DSN="+dbtest.AppDSN(t), "QF_KILL_REDIS="+dbtest.RedisURL(t))
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = os.Stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.cmd.Process.Kill() })
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var l map[string]any
			if json.Unmarshal(sc.Bytes(), &l) != nil {
				l = map[string]any{"not_json": sc.Text()}
			}
			p.mu.Lock()
			p.lines = append(p.lines, l)
			p.mu.Unlock()
		}
		p.exited <- p.cmd.Wait()
	}()
	return p
}

func (p *observedQF) logLines() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.lines)
}

// addr 等到 msg 那一行日志出现，返回它的 addr 字段。
func (p *observedQF) addr(t *testing.T, msg string) string {
	var addr string
	waitUntil(t, 20*time.Second, func() bool {
		for _, l := range p.logLines() {
			if l["msg"] == msg {
				addr, _ = l["addr"].(string)
			}
		}
		return addr != ""
	})
	return addr
}

// waitExit 等 QF 退出，退出码必须是 0。
func (p *observedQF) waitExit(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.exited:
		if err != nil {
			t.Fatalf("QF exited with %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("QF did not exit")
	}
}

// stop 让 QF 退出，之后 flushed 必须成立。Linux 上发 SIGTERM：批处理器 5 秒才到点，这么快退出还导出了，就是退出时冲刷的；
// Windows 上 Go 发不了 SIGTERM，等 flushed 成立再杀掉进程。
func (p *observedQF) stop(t *testing.T, flushed func() bool) {
	if runtime.GOOS == "windows" {
		waitUntil(t, 20*time.Second, flushed)
		_ = p.cmd.Process.Kill()
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(t)
	if !flushed() {
		t.Fatal("the spans were not flushed on exit")
	}
}
