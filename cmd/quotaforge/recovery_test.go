package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// qfProcess 是一个当 QF 跑起来的子进程（就是这个测试二进制，见 TestMain）。
type qfProcess struct {
	cmd  *exec.Cmd
	addr string
	done chan error
}

// startQF 用 configPath 起一个 QF 子进程，等它打出 listening 那一行，读出地址。
func startQF(t *testing.T, configPath string) *qfProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0]) // 不带参数：main 把第一个参数当子命令
	cmd.Env = append(os.Environ(), "QUOTAFORGE_TEST_SERVE=1", "QUOTAFORGE_CONFIG="+configPath, "QUOTAFORGE_ADDR=127.0.0.1:0",
		"QF_KILL_DSN="+dbtest.AppDSN(t), "QF_KILL_REDIS="+dbtest.RedisURL(t))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &qfProcess{cmd: cmd, done: make(chan error, 1)}
	addr := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var l struct{ Msg, Addr string }
			if json.Unmarshal(sc.Bytes(), &l) == nil && l.Msg == "listening" {
				addr <- l.Addr
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
		p.done <- cmd.Wait()
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	select {
	case p.addr = <-addr:
	case err := <-p.done:
		t.Fatalf("QF exited before listening: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("QF did not start listening")
	}
	return p
}

// kill 直接杀掉进程（Linux 上是 SIGKILL，Windows 上是 TerminateProcess）：不给它收尾、关连接的机会。
func (p *qfProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-p.done
}

// 被杀点 3：请求已经发给上游、还没拿到响应时，真的杀掉 QF 进程。上游照样计费（fake-billed-hang 收到就记账）。
// 库里留下 IN_FLIGHT；重启之后，新进程的后台回收在上界之后把它标成未知；用假上游的账单对账：预演不改，执行结算，
// 再执行一遍什么都不改；账本校验通过。
func TestKillWhileUpstreamHasTheRequest(t *testing.T) {
	ctx := context.Background()
	fake := fakeupstream.New()
	up := httptest.NewServer(fake)
	t.Cleanup(up.Close)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	cfgJSON := fmt.Sprintf(`{"upstreams":[{"name":"fake","base_url":%q,"api_key_env":"QF_KILL_UPSTREAM_KEY"}],
		"models":[{"alias":"billed","purpose":"chat","targets":[{"upstream":"fake","upstream_model":"fake-billed-hang","weight":1}],"default_max_tokens":16}],
		"database":{"dsn_env":"QF_KILL_DSN"},"redis":{"url_env":"QF_KILL_REDIS"}}`, up.URL+"/v1")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QF_KILL_UPSTREAM_KEY", "test-only-upstream-key")
	cfg, err := config.Load(cfgPath, func(k string) string {
		return map[string]string{"QF_KILL_UPSTREAM_KEY": "x", "QF_KILL_DSN": "x", "QF_KILL_REDIS": "x"}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	store, admin := dbtest.Store(t), dbtest.Admin(t)
	account := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, account, []string{"billed"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}

	qf := startQF(t, cfgPath)
	sent := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+qf.addr+"/v1/chat/completions",
			strings.NewReader(`{"model":"billed","messages":[{"role":"user","content":"kill me"}]}`))
		req.Header.Set("Authorization", "Bearer "+issued.Secret)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			err = fmt.Errorf("got a response %d", resp.StatusCode)
		}
		sent <- err
	}()
	var bill []ledger.Proof // 等上游收到请求、记了账：这时账本里已经是 IN_FLIGHT（发送意图先于第一个字节）
	waitUntil(t, 20*time.Second, func() bool {
		bill, err = ledger.ReadBill(bytes.NewReader(fake.Bill()))
		return err == nil && len(bill) == 1
	})
	id := bill[0].CallID
	if got := callStatus(t, admin, id); got != "IN_FLIGHT" {
		t.Fatalf("before the kill: %s", got)
	}
	qf.kill(t)
	if err := <-sent; err == nil {
		t.Fatal("the client got an answer from a killed QF")
	}
	if got := callStatus(t, admin, id); got != "IN_FLIGHT" {
		t.Fatalf("after the kill: %s", got)
	}

	// 重启。真等上界太慢：把 created_at 往前改到上界之前，新进程的后台循环启动时就跑一轮回收。
	if _, err := admin.Exec("UPDATE calls SET created_at = UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND WHERE id = ?",
		(cfg.ReclaimAfter() + time.Second).Microseconds(), id); err != nil {
		t.Fatal(err)
	}
	restarted := startQF(t, cfgPath)
	waitUntil(t, 20*time.Second, func() bool { return callStatus(t, admin, id) == "USAGE_UNKNOWN" })
	restarted.kill(t)

	_, reserved, consumed := dbtest.Budget(t, admin, account)
	if items := store.Reconcile(ctx, bill, false); items[0].Verdict != ledger.Settle || items[0].Applied {
		t.Fatalf("dry run: %+v", items[0])
	}
	if _, r, c := dbtest.Budget(t, admin, account); r != reserved || c != consumed || callStatus(t, admin, id) != "USAGE_UNKNOWN" {
		t.Fatal("the dry run changed the ledger")
	}
	if items := store.Reconcile(ctx, bill, true); items[0].Verdict != ledger.Settle || !items[0].Applied {
		t.Fatalf("apply: %+v", items[0])
	}
	if items := store.Reconcile(ctx, bill, true); items[0].Verdict != ledger.Consistent || items[0].Applied {
		t.Fatalf("second apply: %+v", items[0])
	}
	var total int64
	if err := admin.QueryRow("SELECT usage_total_tokens FROM calls WHERE id = ? AND status = 'SETTLED' AND resolved_by = 'reconcile'", id).Scan(&total); err != nil ||
		total != bill[0].Usage.Total {
		t.Fatalf("settled usage %d, bill %d: %v", total, bill[0].Usage.Total, err)
	}
	if _, r, c := dbtest.Budget(t, admin, account); r != 0 || c != total {
		t.Fatalf("reserved, consumed = %d, %d", r, c)
	}
	dbtest.Verify(t, store)
}

// 后台循环：启动就跑一轮，回收超过上界的调用、清理过期的幂等记录；ctx 取消后返回，不留半截事务。
func TestBackgroundLoop(t *testing.T) {
	ctx := context.Background()
	store, admin := dbtest.Store(t), dbtest.Admin(t)
	account := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, account, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil {
		t.Fatal(err)
	}
	reserve := func(idemKey string) ledger.Reservation {
		r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID, Purpose: "chat",
			Alias: "m", Upstream: "u", UpstreamModel: "um", EstInput: 5, EstOutput: 5}
		if idemKey != "" {
			r.IdempotencyKey, r.Fingerprint = idemKey, bytes.Repeat([]byte{1}, 32)
		}
		if _, err := store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	const after = 36 * time.Second
	stale, inFlight, keyed := reserve(""), reserve(""), reserve("expired")
	if err := store.MarkSent(ctx, inFlight); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, keyed, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"UPDATE calls SET created_at = UTC_TIMESTAMP(6) - INTERVAL 37 SECOND WHERE id IN ('" + stale.CallID + "', '" + inFlight.CallID + "')",
		"UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE call_id = '" + keyed.CallID + "'",
	} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	var logs syncLog
	loopCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		background(loopCtx, store, after, time.Hour, slog.New(slog.NewJSONHandler(&logs, nil)), telemetry.NewMetrics(store.Stats))
		close(done)
	}()
	waitUntil(t, 10*time.Second, func() bool {
		var records int
		_ = admin.QueryRow("SELECT COUNT(*) FROM idempotency_keys WHERE call_id = ?", keyed.CallID).Scan(&records)
		return callStatus(t, admin, stale.CallID) == "CANCELLED" && callStatus(t, admin, inFlight.CallID) == "USAGE_UNKNOWN" && records == 0
	})
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the background loop did not stop")
	}
	// 这个测试里没有请求等过连接，所以没有 db_pool 这一行
	if !strings.Contains(logs.String(), `"msg":"background"`) || strings.Contains(logs.String(), "_error") || strings.Contains(logs.String(), "db_pool") {
		t.Fatalf("background log: %s", logs.String())
	}
	dbtest.Verify(t, store)
}

// 一轮清理的期限（一个间隔）到了不是错误：间隔只有 1 纳秒时，每一轮的期限在第一个事务之前就到了，
// 一条都没删，日志里也没有 purge_error；换成长间隔以后，下一轮从头接着把积压删完。
func TestBackgroundPurgeDeadlineIsNotAnError(t *testing.T) {
	ctx := context.Background()
	store, admin := dbtest.Store(t), dbtest.Admin(t)
	account := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, account, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil {
		t.Fatal(err)
	}
	const backlog = 5
	for i := range backlog {
		r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID, Purpose: "chat",
			Alias: "m", Upstream: "u", UpstreamModel: "um", EstInput: 5, EstOutput: 5,
			IdempotencyKey: fmt.Sprintf("backlog-%d", i), Fingerprint: bytes.Repeat([]byte{1}, 32)}
		if _, err := store.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(ctx, r, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec("UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE account_id = ?", key.AccountID); err != nil {
		t.Fatal(err)
	}
	left := func() (n int) {
		if err := admin.QueryRow("SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", key.AccountID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	var logs syncLog
	rounds, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	background(rounds, store, time.Hour, time.Nanosecond, slog.New(slog.NewJSONHandler(&logs, nil)), telemetry.NewMetrics(store.Stats))
	if n := left(); n != backlog {
		t.Fatalf("%d keys left after rounds whose deadline had passed, want all %d untouched", n, backlog)
	}
	if strings.Contains(logs.String(), "purge_error") {
		t.Fatalf("a round that hit its deadline logged an error: %s", logs.String())
	}

	var next syncLog
	loopCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		background(loopCtx, store, time.Hour, time.Hour, slog.New(slog.NewJSONHandler(&next, nil)), telemetry.NewMetrics(store.Stats))
		close(done)
	}()
	waitUntil(t, 10*time.Second, func() bool { return left() == 0 })
	stop()
	<-done
	var purged int64 // 共用库里别的账户也可能有刚过期的键，所以只要求不少于这 5 个
	for _, l := range strings.Split(strings.TrimSpace(next.String()), "\n") {
		var line struct {
			Msg    string
			Purged int64
		}
		if err := json.Unmarshal([]byte(l), &line); err == nil && line.Msg == "background" {
			purged += line.Purged
		}
	}
	if purged < backlog || strings.Contains(next.String(), "_error") {
		t.Fatalf("purged %d, want at least %d, no error: %s", purged, backlog, next.String())
	}
	dbtest.Verify(t, store)
}

// 连接池只有 1 个连接时：占着它，另一个查询只能等。先等一次，再取统计、再等一次：poolWaits 报的是这期间新增的 1 次，
// 不是累计的 2 次。没有新的等待时返回 nil（不记日志）。
func TestPoolWaits(t *testing.T) {
	db := dbtest.App(t)
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if s := db.Stats(); poolWaits(s, db.Stats()) != nil {
		t.Fatal("reported waits without any")
	}
	wait := func() { // 制造一次等连接
		t.Helper()
		start := db.Stats().WaitCount
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		queried := make(chan error, 1)
		go func() { queried <- db.QueryRow("SELECT 1").Scan(new(int)) }()
		waitUntil(t, 5*time.Second, func() bool { return db.Stats().WaitCount > start })
		conn.Close()
		if err := <-queried; err != nil {
			t.Fatal(err)
		}
	}
	wait()
	before := db.Stats()
	wait()
	after := db.Stats()
	got := map[string]int64{}
	for _, a := range poolWaits(before, after) {
		got[a.Key] = a.Value.Int64()
	}
	if before.WaitCount != 1 || got["waits"] != 1 || got["max_open"] != 1 || got["open"] != 1 || got["in_use"] != 0 || got["wait_ms"] < 0 {
		t.Fatalf("waits before %d; poolWaits = %v, want 1 new wait", before.WaitCount, got)
	}
	if poolWaits(after, db.Stats()) != nil {
		t.Fatal("reported the same wait twice")
	}
}

// 后台循环的 db_pool 这一行：额度行被锁住时，比连接池大小多一个的预留同时进来，前 32 个各占一个连接等锁，
// 多出的那个等连接；放锁后让后台循环每 50 毫秒一轮、跑 1 秒：等待只发生过一次，所以正好一行 db_pool，等待次数至少 1。
// 每轮都要记下这一轮的统计，否则之后每一轮都会把同一次等待再报一遍。
func TestBackgroundLogsPoolWaits(t *testing.T) {
	ctx := context.Background()
	store, admin := dbtest.Store(t), dbtest.Admin(t)
	account := dbtest.Account(t, store, 1_000_000)
	issued, err := store.IssueKey(ctx, account, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil {
		t.Fatal(err)
	}
	release := dbtest.LockBudget(t, admin, key.AccountID)
	n := store.Stats().MaxOpenConnections + 1
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := store.Reserve(ctx, ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: key.AccountID, KeyID: key.ID,
				Purpose: "chat", Alias: "m", Upstream: "u", UpstreamModel: "um", EstInput: 5, EstOutput: 5})
			errs <- err
		}()
	}
	waitUntil(t, 5*time.Second, func() bool { return store.Stats().WaitCount > 0 })
	release()
	for range n {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var logs syncLog
	rounds, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	background(rounds, store, time.Hour, 50*time.Millisecond, slog.New(slog.NewJSONHandler(&logs, nil)), telemetry.NewMetrics(store.Stats))
	var pool []int64 // 每一行 db_pool 的等待次数
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var line struct {
			Msg   string
			Waits int64
		}
		if err := json.Unmarshal([]byte(l), &line); err == nil && line.Msg == "db_pool" {
			pool = append(pool, line.Waits)
		}
	}
	if len(pool) != 1 || pool[0] < 1 {
		t.Fatalf("db_pool lines with waits %v, want exactly one line with at least 1: %s", pool, logs.String())
	}
	dbtest.Verify(t, store)
}

func callStatus(t *testing.T, admin *sql.DB, id string) string {
	t.Helper()
	var s string
	if err := admin.QueryRow("SELECT status FROM calls WHERE id = ?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// waitUntil 等确定的事件（库里的状态、假上游的账单），有截止时间。
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
	}
}

// syncLog 是可以并发写的日志缓冲。
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
