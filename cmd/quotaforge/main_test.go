package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newHandler(http.NotFoundHandler()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"status":"ok"}` {
		t.Fatalf("body = %q", body)
	}
}

func TestHealthzRejectsPost(t *testing.T) {
	rec := httptest.NewRecorder()
	newHandler(http.NotFoundHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// 取消 ctx 后，run 要等进行中的请求处理完才返回，且该请求拿到完整响应。
func TestRunWaitsForInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte("done"))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- run(ctx, ln, newServer(h), 10*time.Second) }()

	type result struct {
		code int
		body string
		err  error
	}
	respCh := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err != nil {
			respCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		respCh <- result{code: resp.StatusCode, body: string(body), err: err}
	}()

	<-started
	cancel()
	select {
	case err := <-runDone:
		t.Fatalf("run returned %v while a request was still in flight", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if r := <-respCh; r.err != nil || r.code != http.StatusOK || r.body != "done" {
		t.Fatalf("response = %+v", r)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run = %v, want nil", err)
	}
}

// 监听器已关闭时 Serve 立刻出错，run 要把错误返回，而不是等 ctx 取消。
func TestRunReturnsServeError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()

	runDone := make(chan error, 1)
	go func() {
		runDone <- run(context.Background(), ln, newServer(newHandler(http.NotFoundHandler())), 10*time.Second)
	}()

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("run returned nil, want the serve error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return within 2s after the listener failed")
	}
}

// 关闭上限到了、请求还没结束时，run 要返回 Shutdown 的错误，不能吞掉。
func TestRunReturnsShutdownTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- run(ctx, ln, newServer(h), 50*time.Millisecond) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String()); err == nil {
			resp.Body.Close()
		}
	}()

	<-started
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after the shutdown timeout")
	}
}

// 未注册的路径、错误的方法、健康检查：响应都带 x-request-id。
func TestEveryResponseHasRequestID(t *testing.T) {
	h := newHandler(http.NotFoundHandler())
	for _, tc := range []struct {
		method, path string
		code         int
	}{
		{http.MethodGet, "/nope", http.StatusNotFound},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/v1/unknown", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusOK},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != tc.code {
			t.Errorf("%s %s: status = %d, want %d", tc.method, tc.path, rec.Code, tc.code)
		}
		if id := rec.Header().Get("x-request-id"); !strings.HasPrefix(id, "qf-") {
			t.Errorf("%s %s: x-request-id = %q", tc.method, tc.path, id)
		}
	}
}

// keygen 已删掉：Key 只由 qfctl key issue 发放并入库。
func TestKeygenIsGone(t *testing.T) {
	if code := command("keygen"); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

// TestMain：环境变量 QUOTAFORGE_TEST_SERVE=1 时，这个测试二进制就是 QF 本身（跑 main，给被杀进程的测试当子进程），
// 不进 dbtest.Main：那里要拿库的锁，而父进程正拿着它。
func TestMain(m *testing.M) {
	if os.Getenv("QUOTAFORGE_TEST_SERVE") == "1" {
		main()
		return
	}
	dbtest.Main(m)
}

// ledgerConfig 是连接串取自环境变量 DSN 的最小配置。
func ledgerConfig(t *testing.T, dsn string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	data := `{"upstreams":[{"name":"u","base_url":"http://127.0.0.1:1/v1","api_key_env":"K"}],
		"models":[{"alias":"m","purpose":"chat","targets":[{"upstream":"u","upstream_model":"m","weight":1}],"default_max_tokens":1}],
		"database":{"dsn_env":"DSN"},"redis":{"url_env":"K"}}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path, func(k string) string { return map[string]string{"K": "k", "DSN": dsn}[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// 配置没开嵌入缓存时，交给网关的是值为 nil 的接口（不查缓存）；开了就连它的 Redis（不立即拨号），地址不对时报错不带地址。
func TestOpenCache(t *testing.T) {
	cache, closeCache, err := openCache(ledgerConfig(t, "unused"))
	if err != nil || cache != nil {
		t.Fatalf("without embedding_cache: %v, %v", cache, err)
	}
	closeCache()
	load := func(url string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.json")
		data := `{"upstreams":[{"name":"u","base_url":"http://127.0.0.1:1/v1","api_key_env":"K"}],
			"models":[{"alias":"m","purpose":"embeddings","targets":[{"upstream":"u","upstream_model":"m","weight":1}]}],
			"database":{"dsn_env":"K"},"redis":{"url_env":"K"},"embedding_cache":{"url_env":"C"}}`
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path, func(k string) string { return map[string]string{"K": "k", "C": url}[k] })
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	cache, closeCache, err = openCache(load("redis://127.0.0.1:1/0"))
	if err != nil || cache == nil {
		t.Fatalf("with embedding_cache: %v, %v", cache, err)
	}
	closeCache()
	if _, _, err := openCache(load("redis://:password@[::1")); err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "::1") {
		t.Fatalf("bad URL: %v", err)
	}
}

// 启动时数据库的版本和程序要求的不一致（或连不上），QF 拒绝启动，报错里说明原因、不带连接串。
func TestOpenLedgerChecksSchema(t *testing.T) {
	store, err := openLedger(context.Background(), ledgerConfig(t, dbtest.AppDSN(t)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	admin := dbtest.Admin(t)
	if _, err := admin.Exec("INSERT INTO schema_migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", ledger.SchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DELETE FROM schema_migrations WHERE version = ?", ledger.SchemaVersion+1); err != nil {
			t.Error(err)
		}
	})
	if _, err := openLedger(context.Background(), ledgerConfig(t, dbtest.AppDSN(t))); err == nil || !strings.Contains(err.Error(), "this build needs") {
		t.Fatalf("openLedger on a newer schema = %v", err)
	}

	const secret = "pw-SECRET-MARKER"
	_, err = openLedger(context.Background(), ledgerConfig(t, "qf_app:"+secret+"@tcp(127.0.0.1:1)/x?timeout=1s"))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("openLedger on a closed port = %v", err)
	}
}

// 拼错的子命令要报错退出，而不是当成没有子命令去启动服务。
func TestUnknownCommand(t *testing.T) {
	if code := command("healtcheck"); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

// runCommand 跑子命令 name：标准输入是 stdin，标准输出和错误写进临时文件，返回退出码和两者的内容。
func runCommand(t *testing.T, name, stdin string) (code int, stdout, stderr string) {
	t.Helper()
	dir := t.TempDir()
	inPath := filepath.Join(dir, "stdin")
	if err := os.WriteFile(inPath, []byte(stdin), 0o600); err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(inPath)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	errOut, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	oldIn, oldOut, oldErr := os.Stdin, os.Stdout, os.Stderr
	os.Stdin, os.Stdout, os.Stderr = in, out, errOut
	code = command(name)
	os.Stdin, os.Stdout, os.Stderr = oldIn, oldOut, oldErr
	// 先关再读：Windows 上打开着的文件删不掉，TempDir 清理会失败。
	for _, f := range []*os.File{in, out, errOut} {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	read := func(n string) string {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	return code, read("stdout"), read("stderr")
}

// usage-export 子命令：解析不了的行在 stderr 报数，能解析的行照常写完，退出码 1；没有非法行时退出码 0、stderr 为空。
func TestUsageExportCommand(t *testing.T) {
	const call = `{"time":"t","level":"INFO","msg":"call","request_id":"qf-1","endpoint":"chat","status":200,"usage":"unknown","duration_ms":3}`
	const listening = `{"time":"t","level":"INFO","msg":"listening","addr":"0.0.0.0:8080"}`
	const header = "time,request_id,client_request_id,run_id,endpoint,model_alias,upstream_model,status,error_code,prompt_tokens,completion_tokens,total_tokens,duration_ms,replay_of,cached_from\n"
	const row = "t,qf-1,,,chat,,,200,,unknown,unknown,unknown,3,,\n"
	cases := []struct {
		name, stdin, stderr string
		code                int
	}{
		{"unparseable lines are counted", listening + "\nnot json\n" + call + "\n{\"msg\":\"call\",\n", "usage-export: skipped 2 unparseable lines\n", 1},
		{"clean input", listening + "\n" + call + "\n", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(t, "usage-export", tc.stdin)
			if code != tc.code || stderr != tc.stderr || stdout != header+row {
				t.Fatalf("code = %d, stderr = %q, stdout = %q", code, stderr, stdout)
			}
		})
	}
}
