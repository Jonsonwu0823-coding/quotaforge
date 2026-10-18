package ledger_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/go-sql-driver/mysql"
)

// events 是这个调用在 outbox 里的行数。
func (f *fixture) events(t *testing.T, id string) int {
	t.Helper()
	return f.count(t, "SELECT COUNT(*) FROM outbox WHERE call_id = ?", id)
}

// sameAsCall 报告 outbox 行抄的内容和调用行逐列相同。
func (f *fixture) sameAsCall(t *testing.T, id string) bool {
	t.Helper()
	return f.count(t, `SELECT COUNT(*) FROM outbox o JOIN calls c ON c.id = o.call_id WHERE o.call_id = ?
		AND o.account_id = c.account_id AND o.purpose = c.purpose AND o.model_alias = c.model_alias AND o.upstream = c.upstream
		AND o.upstream_model = c.upstream_model AND o.status = c.status AND o.usage_prompt_tokens <=> c.usage_prompt_tokens
		AND o.usage_completion_tokens <=> c.usage_completion_tokens AND o.usage_total_tokens <=> c.usage_total_tokens
		AND o.resolved_at = c.resolved_at AND o.cached_from <=> c.cached_from`, id) == 1
}

// hold 在管理连接的一个事务里执行 query（加锁的读或写），一直不提交；返回放开（回滚）它的函数，测试结束时也会放。
func (f *fixture) hold(t *testing.T, query string, args ...any) func() {
	t.Helper()
	return dbtest.Hold(t, f.admin, query, args...)
}

// 四处结清都写一行、只写一行，内容和调用行相同：收尾的结算和取消（从 RESERVED、IN_FLIGHT 出发）、回收的释放、
// 对账的结算和释放、缓存命中。回收标成未知的不写；未知之后迟到的结果结清时写。
func TestEventPerResolution(t *testing.T) {
	f := newFixture(t, 1<<40)
	settled, cancelledInFlight := f.reserve(t, 100), f.reserve(t, 100)
	f.finish(t, settled, ledger.Settled, 42)
	f.finish(t, cancelledInFlight, ledger.Cancelled, 0)
	cancelledEarly := f.reserve(t, 100)
	if err := f.s.Finish(ctx, cancelledEarly, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent}); err != nil {
		t.Fatal(err)
	}

	reclaimed, unknownLate := f.reserve(t, 100), f.reserve(t, 100)
	f.sent(t, unknownLate)
	f.age(t, reclaimed, reclaimAfter+time.Second)
	f.age(t, unknownLate, reclaimAfter+time.Second)
	f.reclaim(t)
	if f.events(t, unknownLate.CallID) != 0 {
		t.Fatal("a call marked USAGE_UNKNOWN by the reclaim has an event")
	}
	late := ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200, Usage: chatUsage(5, 1)}
	if err := f.s.Finish(ctx, unknownLate, late); err != nil {
		t.Fatal(err)
	}

	toSettle, toRelease, stillUnknown := f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100)
	f.timedOut(t, toSettle, toRelease, stillUnknown)
	proofs := []ledger.Proof{
		{CallID: toSettle.CallID, Status: ledger.Settled, Usage: chatUsage(30, 7), Line: 2, Source: ledger.Bill},
		{CallID: toRelease.CallID, Status: ledger.Cancelled, Line: 3, Source: ledger.Bill},
	}
	for _, it := range f.s.Reconcile(ctx, proofs, true) {
		if !it.Applied {
			t.Fatalf("reconcile %s: %v %s", it.CallID, it.Verdict, it.Reason)
		}
	}
	cached := f.recordCached(t, f.settledEmbedding(t).CallID)

	for _, id := range []string{settled.CallID, cancelledInFlight.CallID, cancelledEarly.CallID, reclaimed.CallID,
		unknownLate.CallID, toSettle.CallID, toRelease.CallID, cached.CallID} {
		if n := f.events(t, id); n != 1 || !f.sameAsCall(t, id) {
			t.Errorf("%s (%s): %d outbox rows, same content %v", id, f.state(t, id).status, n, f.sameAsCall(t, id))
		}
	}
	if n := f.events(t, stillUnknown.CallID); n != 0 {
		t.Errorf("USAGE_UNKNOWN call has %d outbox rows", n)
	}
	dbtest.Verify(t, f.s)
}

// 和结清同一个事务：outbox 写不进去（开关行被锁住，等锁超时），结清整个回滚；结清之后的语句（写账本那一行，
// 撞上预先插好的同键的行）失败，outbox 也没有；缓存命中的 outbox 写不进去，CACHED 那一行也没有。
func TestEventInSameTransaction(t *testing.T) {
	f := newFixture(t, 1000)
	t.Cleanup(func() { deleteAccount(t, f) }) // 预先插的账本行不留给后面以 Verify 收尾的测试
	rowBroken, ledgerBroken := f.reserve(t, 100), f.reserve(t, 100)
	f.sent(t, rowBroken)
	f.sent(t, ledgerBroken)
	exec(t, f.admin, "INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('"+
		ledgerBroken.CallID+"', 'settle', 0, 0, UTC_TIMESTAMP(6))")
	before := f.snapshot(t)
	settle := ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200, Usage: chatUsage(10, 2)}
	release := f.hold(t, "SELECT 1 FROM usage_events WHERE id = 1 FOR UPDATE")
	if err := f.s.Finish(ctx, rowBroken, settle); err == nil {
		t.Fatal("Finish succeeded although the outbox row could not be written")
	}
	c := f.cachedCall("qf-" + rand.Text()) // 写 outbox 时就失败了，用不到原来那次的内容
	if err := f.s.RecordCached(ctx, c); err == nil {
		t.Fatal("RecordCached succeeded although the outbox row could not be written")
	}
	release()
	if err := f.s.Finish(ctx, ledgerBroken, settle); err == nil {
		t.Fatal("Finish succeeded despite the conflicting ledger row")
	}
	for _, id := range []string{rowBroken.CallID, ledgerBroken.CallID, c.CallID} {
		if n := f.events(t, id); n != 0 {
			t.Errorf("%s: a rolled back transaction left %d outbox rows", id, n)
		}
	}
	if n := f.count(t, "SELECT COUNT(*) FROM calls WHERE id = ?", c.CallID); n != 0 {
		t.Error("the CACHED call was written without its outbox row")
	}
	if after := f.snapshot(t); after != before {
		t.Fatalf("a failed settle changed the ledger:\n%s\nvs\n%s", before, after)
	}
}

// restoreSwitch 在测试结束时把开关的 enabled_at 恢复成现在的值（DATETIME(6) 的文本，原样写回）。
func restoreSwitch(t *testing.T, f *fixture) {
	t.Helper()
	var was sql.NullString
	if err := f.admin.QueryRow("SELECT DATE_FORMAT(enabled_at, '%Y-%m-%d %H:%i:%s.%f') FROM usage_events WHERE id = 1").Scan(&was); err != nil || !was.Valid {
		t.Fatalf("usage events switch = %v, %v; the test database should have it on", was, err)
	}
	t.Cleanup(func() { exec(t, f.admin, "UPDATE usage_events SET enabled_at = '"+was.String+"' WHERE id = 1") })
}

// switchOff 把开关关上，测试结束时恢复原来的 enabled_at；关着时结清的调用没有事件，结束前要删掉（deleteAccount），
// 否则恢复之后它们落在完整性的窗口里。
func switchOff(t *testing.T, f *fixture) {
	t.Helper()
	restoreSwitch(t, f)
	exec(t, f.admin, "UPDATE usage_events SET enabled_at = NULL WHERE id = 1")
}

// 开关关着时什么都不写（给 QS 用的部署就是这样）；打开之后才写。
func TestEventsSwitchedOff(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	t.Cleanup(func() { deleteAccount(t, f) }) // 先于恢复开关执行
	off := f.reserve(t, 100)
	f.finish(t, off, ledger.Settled, 5)
	c := f.recordCached(t, f.settledEmbedding(t).CallID)
	if n := f.events(t, off.CallID) + f.events(t, c.CallID); n != 0 {
		t.Fatalf("%d outbox rows written while usage events are off", n)
	}
	if err := f.s.EnableUsageEvents(ctx); err != nil {
		t.Fatal(err)
	}
	on := f.reserve(t, 100)
	f.finish(t, on, ledger.Settled, 5)
	if n := f.events(t, on.CallID); n != 1 {
		t.Fatalf("%d outbox rows after enabling, want 1", n)
	}
}

// 打开开关要等正在结清、已经读到“关”的事务提交：管理连接锁住幂等记录那一行，把一次带键的收尾卡在存快照
// （读过开关之后）；这时去打开，打开会等着。放开之后，打开时记下的时间晚于这次结清，它没有事件不违反完整性；
// 之后结清的调用都有事件，Verify 干净。打开过之后再打开，enabled_at 不变（读到“开”直接返回）；再从“关”出发
// 同时打开两次，锁里再看一次，也不变（racingEnables）。
func TestEnableWaitsForSettlingTransactions(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	t.Cleanup(func() { deleteAccount(t, f) })
	stuck := f.keyed(100, "stuck", "request")
	if _, err := f.s.Reserve(ctx, stuck); err != nil {
		t.Fatal(err)
	}
	f.sent(t, stuck)
	release := f.hold(t, "SELECT 1 FROM idempotency_keys WHERE account_id = ? AND idem_key = 'stuck' FOR UPDATE", f.key.AccountID)
	settled := make(chan error, 1)
	go func() {
		settled <- f.s.Finish(ctx, stuck, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200,
			Usage: chatUsage(3, 1), Response: &ledger.Response{Status: 200, Body: []byte("{}")}})
	}()
	waitBlocked(t, "UPDATE idempotency_keys%", 1)
	enabled := make(chan error, 1)
	go func() { enabled <- f.s.EnableUsageEvents(ctx) }()
	waitBlocked(t, "%usage_events%", 1) // 打开的语句到了开关行上（不管它是怎么写的）
	// 进程列表里看到语句不等于它在等锁，正在执行也算：再给 200 毫秒，这期间打开返回了，就是没在等。
	select {
	case err := <-enabled:
		t.Fatalf("enabling did not wait for the settling transaction: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if err := <-enabled; err != nil {
		t.Fatal(err)
	}
	if n := f.count(t, `SELECT COUNT(*) FROM calls c JOIN usage_events e ON e.id = 1
		WHERE c.id = ? AND c.resolved_at < e.enabled_at`, stuck.CallID); n != 1 {
		t.Fatal("enabled_at is not after the settle that read the switch as off")
	}
	after := f.reserve(t, 100)
	f.finish(t, after, ledger.Settled, 5)
	if f.events(t, stuck.CallID) != 0 || f.events(t, after.CallID) != 1 {
		t.Fatalf("events: stuck %d (want 0), after %d (want 1)", f.events(t, stuck.CallID), f.events(t, after.CallID))
	}
	enabledAt := func() string {
		var at string
		if err := f.admin.QueryRow("SELECT DATE_FORMAT(enabled_at, '%Y-%m-%d %H:%i:%s.%f') FROM usage_events").Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	first := enabledAt()
	if err := f.s.EnableUsageEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if again := enabledAt(); again != first {
		t.Fatalf("enabled_at changed on a second enable: %q -> %q", first, again)
	}
	racingEnables(t, f)
	if got := rules(t, f); len(got) != 0 {
		t.Fatalf("Verify: %v", got)
	}
}

// 打开时开关行被一个结清事务拿着共享锁超过等锁的上限（2 秒，1205）：在 ctx 的期限里重试，放锁之后照常打开。
// 锁一直不放的话，到 ctx 的期限返回错误，开关还是关的。
func TestEnableRetriesLockWaitTimeouts(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	release := f.hold(t, "SELECT 1 FROM usage_events WHERE id = 1 FOR SHARE")
	time.AfterFunc(3*time.Second, release)
	ctx10, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := f.s.EnableUsageEvents(ctx10); err != nil {
		t.Fatalf("enable while the switch row was held for 3s: %v", err)
	}
	if f.count(t, "SELECT COUNT(*) FROM usage_events WHERE id = 1 AND enabled_at IS NOT NULL") != 1 {
		t.Fatal("the switch is still off")
	}

	exec(t, f.admin, "UPDATE usage_events SET enabled_at = NULL WHERE id = 1")
	f.hold(t, "SELECT 1 FROM usage_events WHERE id = 1 FOR SHARE")
	ctx5, cancel5 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel5()
	if err := f.s.EnableUsageEvents(ctx5); err == nil {
		t.Fatal("enable succeeded while the switch row stayed locked")
	}
	if f.count(t, "SELECT COUNT(*) FROM usage_events WHERE id = 1 AND enabled_at IS NULL") != 1 {
		t.Fatal("the switch was turned on")
	}
}

// 别的错误（这里是连接池已经关掉）不重试：立即返回，不等到 ctx 的期限。
func TestEnableReturnsOtherErrorsAtOnce(t *testing.T) {
	s, err := ledger.Open(dbtest.AppDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	ctx5, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := s.EnableUsageEvents(ctx5); err == nil || ctx5.Err() != nil || time.Since(start) > time.Second {
		t.Fatalf("enable on a closed pool = %v after %v", err, time.Since(start))
	}
}

// isMySQLError 只认编号相同的 MySQL 错误（包装过的也认）：打开开关只重试 1205，CreateAccount 只把 1062 当成重名。
func TestIsMySQLError(t *testing.T) {
	lockWait := &mysql.MySQLError{Number: 1205}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{lockWait, true},
		{fmt.Errorf("lock usage events switch: %w", lockWait), true},
		{&mysql.MySQLError{Number: 1062}, false},
		{errors.New("Error 1205: lock wait timeout"), false},
		{nil, false},
	} {
		if got := ledger.IsMySQLError(tc.err, 1205); got != tc.want {
			t.Errorf("IsMySQLError(%v, 1205) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// readCommittedStore 打开一个会话隔离级别是读已提交的 Store：连接串参数只影响它自己的连接，不改共用测试库的全局设置。
// 结清事务用服务器默认的隔离级别，有的服务器默认就是读已提交。
func readCommittedStore(t *testing.T) *ledger.Store {
	t.Helper()
	cfg, err := mysql.ParseDSN(dbtest.AppDSN(t))
	if err != nil {
		t.Fatal("parse test DSN: invalid")
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["transaction_isolation"] = "'READ-COMMITTED'"
	dsn := cfg.FormatDSN()
	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var level string
	if err := raw.QueryRow("SELECT @@SESSION.transaction_isolation").Scan(&level); err != nil || level != "READ-COMMITTED" {
		t.Fatalf("session isolation %q, %v; want READ-COMMITTED", level, err)
	}
	s, err := ledger.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// 打开在前、结清在后，结清事务用读已提交：管理连接锁住开关行、写了 enabled_at、还没提交；这时结清，写事件的语句
// 要等开关行的锁（显式 FOR SHARE），打开提交之后读到“开”，写一行。不加 FOR SHARE 的话，读已提交下它读到已提交的
// “关”、不等，这个 resolved_at 晚于 enabled_at 的调用就没有事件。
func TestEnableFirstThenSettleUnderReadCommitted(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	t.Cleanup(func() { deleteAccount(t, f) })
	rc := readCommittedStore(t)
	r := f.reserve(t, 100)
	f.sent(t, r)
	enable, err := f.admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = enable.Rollback() }()
	for _, q := range []string{"SELECT 1 FROM usage_events WHERE id = 1 FOR UPDATE",
		"UPDATE usage_events SET enabled_at = UTC_TIMESTAMP(6) WHERE id = 1"} {
		if _, err := enable.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	settled := make(chan error, 1)
	go func() {
		settled <- rc.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200, Usage: chatUsage(3, 1)})
	}()
	waitBlocked(t, "INSERT INTO outbox%", 1) // 结清等在开关行上
	if err := enable.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if f.events(t, r.CallID) != 1 || !f.sameAsCall(t, r.CallID) {
		t.Fatalf("%d outbox rows for a call settled after enabling, want 1 matching the call", f.events(t, r.CallID))
	}
	if got := rules(t, f); len(got) != 0 {
		t.Fatalf("Verify: %v", got)
	}
}

// 开关已经开着时，打开只做一次不加锁的读就返回：另一个连接拿着开关行的共享锁（正在结清的事务就是这样）时也不等。
// 要是照样去排开关行的排他锁，它要等到共享锁放开（这里放不开，等锁超时报错），排队期间所有实例的结清都排在它后面。
func TestEnableWhenOnDoesNotWait(t *testing.T) {
	f := newFixture(t, 1000)
	if f.count(t, "SELECT COUNT(*) FROM usage_events WHERE id = 1 AND enabled_at IS NOT NULL") != 1 {
		t.Fatal("usage events are off in the shared test database")
	}
	f.hold(t, "SELECT 1 FROM usage_events WHERE id = 1 FOR SHARE")
	start := time.Now()
	if err := f.s.EnableUsageEvents(ctx); err != nil {
		t.Fatalf("enabling an enabled switch: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("enabling an enabled switch took %v", d)
	}
}

// racingEnables 把开关关上，让两次打开同时从“关”出发（两个实例同时启动）：都先读到“关”，都去排开关行的排他锁。
// 管理连接先拿着锁、自己打开并提交，两次打开轮到时都要在锁里再看一次、看到“开”就不动，enabled_at 还是第一次的时间。
func racingEnables(t *testing.T, f *fixture) {
	t.Helper()
	exec(t, f.admin, "UPDATE usage_events SET enabled_at = NULL WHERE id = 1")
	first, err := f.admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback() }()
	for _, q := range []string{"SELECT 1 FROM usage_events WHERE id = 1 FOR UPDATE",
		"UPDATE usage_events SET enabled_at = UTC_TIMESTAMP(6) WHERE id = 1"} {
		if _, err := first.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	enabled := make(chan error, 2)
	for range 2 {
		go func() { enabled <- f.s.EnableUsageEvents(ctx) }()
	}
	waitBlocked(t, "SELECT enabled_at IS NOT NULL FROM usage_events%FOR UPDATE", 2) // 都读到了“关”，在等锁
	var at string
	if err := first.QueryRow("SELECT DATE_FORMAT(enabled_at, '%Y-%m-%d %H:%i:%s.%f') FROM usage_events WHERE id = 1").Scan(&at); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-enabled; err != nil {
			t.Fatal(err)
		}
	}
	if n := f.count(t, "SELECT COUNT(*) FROM usage_events WHERE id = 1 AND enabled_at = ?", at); n != 1 {
		t.Fatalf("enabled_at moved from %s after two more enables", at)
	}
}

// collector 是假的发送方：记下发给它的事件；fail 不为 nil 时返回它、什么都不记。
type collector struct {
	mu     sync.Mutex
	got    []ledger.UsageEvent
	fail   error
	during func()
}

func (c *collector) send(_ context.Context, events []ledger.UsageEvent) error {
	if c.during != nil {
		c.during()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.got = append(c.got, events...)
	return nil
}

// of 是发给它的、属于 ids 的事件的调用号，按发送的顺序。
func (c *collector) of(ids ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.got {
		if slices.Contains(ids, e.CallID) {
			out = append(out, e.CallID)
		}
	}
	return out
}

func (c *collector) event(id string) (ledger.UsageEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.got {
		if e.CallID == id {
			return e, true
		}
	}
	return ledger.UsageEvent{}, false
}

func (f *fixture) unsent(t *testing.T, id string) bool {
	t.Helper()
	return f.count(t, "SELECT COUNT(*) FROM outbox WHERE call_id = ? AND sent_at IS NULL", id) == 1
}

// 转发按 seq 发、发成功才标记；内容和调用行一样；标记过的不再发。发送失败时一条都不标记，下一轮补发。
func TestForwardOutbox(t *testing.T) {
	f := newFixture(t, 1000)
	a, b := f.reserve(t, 100), f.reserve(t, 100)
	f.finish(t, a, ledger.Settled, 42)
	f.finish(t, b, ledger.Cancelled, 0)

	down := &collector{fail: errors.New("queue unavailable")}
	if _, err := f.s.ForwardOutbox(ctx, down.send); err == nil {
		t.Fatal("ForwardOutbox succeeded with a failing sender")
	}
	if !f.unsent(t, a.CallID) || !f.unsent(t, b.CallID) {
		t.Fatal("rows were marked sent although sending failed")
	}

	up := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, up.send); err != nil {
		t.Fatal(err)
	}
	if got := up.of(a.CallID, b.CallID); !slices.Equal(got, []string{a.CallID, b.CallID}) {
		t.Fatalf("sent %v, want both in order", got)
	}
	if f.unsent(t, a.CallID) || f.unsent(t, b.CallID) {
		t.Fatal("rows not marked sent after a successful send")
	}
	e, _ := up.event(a.CallID)
	resolved := f.resolvedAt(t, a.CallID)
	if e.Status != ledger.Settled || e.AccountID != f.key.AccountID || e.Purpose != "chat" || e.Alias != "m" || e.Upstream != "u" ||
		e.UpstreamModel != "um" || e.Usage == nil || e.Usage.Total != 42 || !e.ResolvedAt.Equal(resolved) || e.CachedFrom != "" {
		t.Fatalf("event %+v (resolved at %v)", e, resolved)
	}
	again := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, again.send); err != nil {
		t.Fatal(err)
	}
	if got := again.of(a.CallID, b.CallID); len(got) != 0 {
		t.Fatalf("sent again: %v", got)
	}
}

// 转发正在发一批时，另一个结清事务提交了一行新的 outbox：这一轮只标记发出的那一批，新的一行还是没发出，
// 下一轮才发。先把共用测试库里没发出的行都发掉，这一轮的第一批就只有 a（不满一批，这一轮发完就停）。
func TestForwardMarksOnlyWhatItSent(t *testing.T) {
	f := newFixture(t, 1000)
	if _, err := f.s.ForwardOutbox(ctx, (&collector{}).send); err != nil {
		t.Fatal(err)
	}
	a := f.reserve(t, 100)
	f.finish(t, a, ledger.Settled, 1)
	var b ledger.Reservation
	first := &collector{}
	first.during = func() {
		first.during = nil
		b = f.reserve(t, 100)
		f.finish(t, b, ledger.Settled, 2)
	}
	if _, err := f.s.ForwardOutbox(ctx, first.send); err != nil {
		t.Fatal(err)
	}
	if got := first.of(a.CallID, b.CallID); !slices.Equal(got, []string{a.CallID}) {
		t.Fatalf("first round sent %v, want only a", got)
	}
	if f.unsent(t, a.CallID) || !f.unsent(t, b.CallID) {
		t.Fatalf("after the first round: a unsent %v (want false), b unsent %v (want true)", f.unsent(t, a.CallID), f.unsent(t, b.CallID))
	}
	second := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, second.send); err != nil {
		t.Fatal(err)
	}
	if got := second.of(a.CallID, b.CallID); !slices.Equal(got, []string{b.CallID}) || f.unsent(t, b.CallID) {
		t.Fatalf("second round sent %v, want only b, marked", got)
	}
}

func (f *fixture) resolvedAt(t *testing.T, id string) time.Time {
	t.Helper()
	var s string
	if err := f.admin.QueryRow("SELECT DATE_FORMAT(resolved_at, '%Y-%m-%dT%H:%i:%s.%fZ') FROM calls WHERE id = ?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// 发出了、还没标记（标记失败，等于进程在两步之间死了）：下一轮再发一遍；消费者按调用号去重，汇总只算一次。
// 标记失败的造法：管理连接锁住这一行 outbox，标记等锁超时。
func TestForwardSentButNotMarked(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 100)
	f.finish(t, r, ledger.Settled, 42)
	release := f.hold(t, "SELECT 1 FROM outbox WHERE call_id = ? FOR UPDATE", r.CallID)
	first := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, first.send); err == nil {
		t.Fatal("ForwardOutbox succeeded although marking failed")
	}
	// 客户端的期限和数据库的等锁上限都是 2 秒：客户端先放弃时，数据库那边的 UPDATE 还在等锁，这时放锁它就照样提交了
	// （“出错但其实已经提交”，这一批只发了一次，也是对的）。这里要的是标记确实没成，所以等它在数据库那边超时结束再放锁。
	waitNoSession(t, "UPDATE outbox SET sent_at%")
	release()
	second := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, second.send); err != nil {
		t.Fatal(err)
	}
	e1, ok1 := first.event(r.CallID)
	e2, ok2 := second.event(r.CallID)
	if !ok1 || !ok2 {
		t.Fatalf("delivered first %v, second %v; want both", ok1, ok2)
	}
	for i, e := range []ledger.UsageEvent{e1, e2} {
		applied, err := f.s.ApplyUsageEvent(ctx, e)
		if err != nil || applied != (i == 0) {
			t.Fatalf("delivery %d: applied %v, %v", i+1, applied, err)
		}
	}
	if got := f.daily(t); got != "chat 1 42/0/42 0/0 0" {
		t.Fatalf("daily totals %q", got)
	}
}

// daily 是这个账户今天的汇总，一行一种用途：用途 结算数 prompt/completion/total 缓存数/省下的 取消数。
func (f *fixture) daily(t *testing.T) string {
	t.Helper()
	rows, err := f.admin.Query(`SELECT CONCAT_WS(' ', purpose, settled_calls,
			CONCAT_WS('/', prompt_tokens, completion_tokens, total_tokens), CONCAT_WS('/', cached_calls, cached_tokens), cancelled_calls)
		FROM usage_daily WHERE account_id = ? ORDER BY day, purpose`, f.key.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return strings.Join(out, "; ")
}

// 两个实例同时转发：同一时刻只有拿到命名锁的那个在发，每条只发一次。发送里停一下，拉长两边重叠的时间。
func TestForwardTwoInstances(t *testing.T) {
	f := newFixture(t, 1<<40)
	var ids []string
	for range 30 {
		r := f.reserve(t, 100)
		f.finish(t, r, ledger.Settled, 1)
		ids = append(ids, r.CallID)
	}
	stores := []*ledger.Store{f.s, dbtest.Store(t)}
	sink := &collector{during: func() { time.Sleep(20 * time.Millisecond) }}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range stores {
		wg.Go(func() {
			<-start
			deadline := time.Now().Add(20 * time.Second)
			for f.count(t, "SELECT COUNT(*) FROM outbox WHERE account_id = ? AND sent_at IS NULL", f.key.AccountID) > 0 {
				if time.Now().After(deadline) {
					t.Error("rows still unsent at the deadline")
					return
				}
				if _, err := s.ForwardOutbox(ctx, sink.send); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	got := sink.of(ids...)
	slices.Sort(got)
	want := slices.Clone(ids)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("sent %d events for %d calls (each must be sent exactly once)", len(got), len(ids))
	}
}

// drain 把 outbox 里没发出的行全部标成已发出（共用的测试库里别的测试留下的也算），之后的积压只有本测试造的。
func (f *fixture) drain(t *testing.T) {
	t.Helper()
	if _, err := f.s.ForwardOutbox(ctx, (&collector{}).send); err != nil {
		t.Fatal(err)
	}
	if n := f.count(t, "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL"); n != 0 {
		t.Fatalf("%d rows left unsent", n)
	}
}

// 积压：没发出的条数，和最早一条（seq 最小的）从写入到现在的秒数；发完之后都是 0。
func TestOutboxBacklog(t *testing.T) {
	f := newFixture(t, 1000)
	f.drain(t)
	if n, age, err := f.s.OutboxBacklog(ctx); err != nil || n != 0 || age != 0 {
		t.Fatalf("empty backlog = %d, %d, %v", n, age, err)
	}
	a, b := f.reserve(t, 100), f.reserve(t, 100)
	f.finish(t, a, ledger.Settled, 1)
	f.finish(t, b, ledger.Settled, 1)
	exec(t, f.admin, "UPDATE outbox SET created_at = UTC_TIMESTAMP(6) - INTERVAL 1 HOUR WHERE call_id = '"+a.CallID+"'")
	n, age, err := f.s.OutboxBacklog(ctx)
	if err != nil || n != 2 || age < 3600 || age > 3700 {
		t.Fatalf("backlog = %d, %d s, %v; want 2 rows, about an hour", n, age, err)
	}
	f.drain(t)
	if n, age, err := f.s.OutboxBacklog(ctx); err != nil || n != 0 || age != 0 {
		t.Fatalf("backlog after forwarding = %d, %d, %v", n, age, err)
	}
}

// 清理只删发出超过 7 天的；没发出的再老也留着，发出没满 7 天的也留着。两边各差 5 秒：7 天多 5 秒的删，差 5 秒满 7 天的留着。
func TestPurgeOutbox(t *testing.T) {
	f := newFixture(t, 1000)
	t.Cleanup(func() { deleteAccount(t, f) }) // 删掉了 outbox 行的调用不留给后面以 Verify 收尾的测试
	old, unsent, recent := f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100)
	for _, r := range []ledger.Reservation{old, unsent, recent} {
		f.finish(t, r, ledger.Settled, 1)
	}
	exec(t, f.admin,
		"UPDATE outbox SET sent_at = UTC_TIMESTAMP(6) - INTERVAL 7 DAY - INTERVAL 5 SECOND WHERE call_id = '"+old.CallID+"'",
		"UPDATE outbox SET created_at = UTC_TIMESTAMP(6) - INTERVAL 30 DAY WHERE call_id = '"+unsent.CallID+"'",
		"UPDATE outbox SET sent_at = UTC_TIMESTAMP(6) - INTERVAL 7 DAY + INTERVAL 5 SECOND WHERE call_id = '"+recent.CallID+"'")
	if _, err := f.s.PurgeOutbox(ctx, opTimeout); err != nil {
		t.Fatal(err)
	}
	if f.events(t, old.CallID) != 0 || f.events(t, unsent.CallID) != 1 || f.events(t, recent.CallID) != 1 {
		t.Fatalf("after purge: old %d (want 0), unsent %d (want 1), recent %d (want 1)",
			f.events(t, old.CallID), f.events(t, unsent.CallID), f.events(t, recent.CallID))
	}
}

// 改坏 outbox，或者删掉窗口里结清的调用的那一行，Verify 能报出来；打开之前、7 天之前结清的调用没有行不报。
func TestVerifyFindsOutboxTampering(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(f *fixture, settled, unknown ledger.Reservation) []string
		want   []string
	}{
		{"usage differs", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"UPDATE outbox SET usage_total_tokens = usage_total_tokens + 1 WHERE call_id = '" + s.CallID + "'"}
		}, []string{"outbox"}},
		{"status differs", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"UPDATE outbox SET status = 'CANCELLED' WHERE call_id = '" + s.CallID + "'"}
		}, []string{"outbox"}},
		{"resolved_at differs", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"UPDATE outbox SET resolved_at = resolved_at - INTERVAL 1 DAY WHERE call_id = '" + s.CallID + "'"}
		}, []string{"outbox"}},
		{"row for an unresolved call", func(_ *fixture, s, u ledger.Reservation) []string {
			return []string{`INSERT INTO outbox (call_id, account_id, purpose, model_alias, upstream, upstream_model, status,
				resolved_at, created_at) SELECT id, account_id, purpose, model_alias, upstream, upstream_model, status,
				UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) FROM calls WHERE id = '` + u.CallID + "'"}
		}, []string{"outbox"}},
		{"row missing", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"DELETE FROM outbox WHERE call_id = '" + s.CallID + "'"}
		}, []string{"outbox_complete"}},
		{"resolved before enabling", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"DELETE FROM outbox WHERE call_id = '" + s.CallID + "'",
				"UPDATE calls c JOIN usage_events e ON e.id = 1 SET c.resolved_at = e.enabled_at - INTERVAL 1 SECOND WHERE c.id = '" + s.CallID + "'"}
		}, nil},
		// 窗口的两边（7 天减 5 秒在里面、加 5 秒在外面）：开关打开得更早（30 天前），只有 7 天的窗口决定查不查
		{"resolved just inside the window", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"DELETE FROM outbox WHERE call_id = '" + s.CallID + "'",
				"UPDATE calls SET resolved_at = UTC_TIMESTAMP(6) - INTERVAL 7 DAY + INTERVAL 5 SECOND WHERE id = '" + s.CallID + "'",
				"UPDATE usage_events SET enabled_at = UTC_TIMESTAMP(6) - INTERVAL 30 DAY WHERE id = 1"}
		}, []string{"outbox_complete"}},
		{"resolved just before the window", func(_ *fixture, s, _ ledger.Reservation) []string {
			return []string{"DELETE FROM outbox WHERE call_id = '" + s.CallID + "'",
				"UPDATE calls SET resolved_at = UTC_TIMESTAMP(6) - INTERVAL 7 DAY - INTERVAL 5 SECOND WHERE id = '" + s.CallID + "'",
				"UPDATE usage_events SET enabled_at = UTC_TIMESTAMP(6) - INTERVAL 30 DAY WHERE id = 1"}
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1000)
			restoreSwitch(t, f) // 有的用例把开关的时间往前改
			s, u := f.reserve(t, 100), f.reserve(t, 100)
			f.finish(t, s, ledger.Settled, 12)
			f.finish(t, u, ledger.UsageUnknown, 0)
			if got := rules(t, f); len(got) != 0 {
				t.Fatalf("before tampering: %v", got)
			}
			exec(t, f.admin, tc.tamper(f, s, u)...)
			if got := rules(t, f); !slices.Equal(got, tc.want) {
				t.Errorf("rules = %v, want %v", got, tc.want)
			}
			deleteAccount(t, f)
		})
	}
}

// unmigrate6 回到版本 5 的表结构：去掉 0006 建的四张表。
var unmigrate6 = []string{
	"DROP TABLE outbox", "DROP TABLE usage_events", "DROP TABLE usage_seen", "DROP TABLE usage_daily",
	"DELETE FROM schema_migrations WHERE version = 6",
}

// 迁移 0006：版本 5 的库里已有的调用原样保留，迁两遍都成功；开关是关的，打开之前结清不写事件，打开之后才写；
// Verify 一直干净（打开之前结清的调用不要求有事件）。
func TestMigration6UsageEvents(t *testing.T) {
	dropAll(t)
	dsn := dbtest.MigrateDSN(t)
	if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, 1000)
	old := f.reserve(t, 100)
	f.finish(t, old, ledger.Settled, 5)
	exec(t, f.admin, unmigrate6...)
	before := f.snapshot(t)
	for range 2 {
		from, to, err := ledger.Migrate(ctx, dsn)
		if err != nil || to != ledger.SchemaVersion {
			t.Fatalf("Migrate = %d, %d, %v", from, to, err)
		}
	}
	wantAllVersions(t)
	if after := f.snapshot(t); after != before {
		t.Fatalf("existing rows changed by the migration:\n%s\nvs\n%s", before, after)
	}
	if n := f.count(t, "SELECT COUNT(*) FROM usage_events WHERE id = 1 AND enabled_at IS NULL"); n != 1 {
		t.Fatal("usage events are not off after the migration")
	}
	offSettled := f.reserve(t, 100)
	f.finish(t, offSettled, ledger.Settled, 5)
	dbtest.Verify(t, f.s)
	if err := f.s.EnableUsageEvents(ctx); err != nil {
		t.Fatal(err)
	}
	on := f.reserve(t, 100)
	f.finish(t, on, ledger.Settled, 5)
	if f.events(t, old.CallID)+f.events(t, offSettled.CallID) != 0 || f.events(t, on.CallID) != 1 {
		t.Fatal("events written before enabling, or not after")
	}
	dbtest.Verify(t, f.s)
}

// waitNoSession 等到服务的账号没有正在执行、语句以 pattern 开头的线程（截止时间只用来判失败）。
func waitNoSession(t *testing.T, pattern string) {
	t.Helper()
	app := dbtest.App(t)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := app.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE ?", pattern).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions still run %q", n, pattern)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
