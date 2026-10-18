package ledger_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql" // waitBlocked 用服务的账号另开连接；坏行的测试认它的错误号

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// reclaimAfter 是测试用的回收上界，和默认配置算出来的一样（25 + 3 × 2 + 5 秒）；opTimeout 是每个事务的上限。
const (
	reclaimAfter = 36 * time.Second
	opTimeout    = 2 * time.Second
)

// age 把调用的 created_at 改成数据库现在的时间往前 d：造出“已经过了多久”，不用真的等。
func (f *fixture) age(t *testing.T, r ledger.Reservation, d time.Duration) {
	t.Helper()
	if _, err := f.admin.Exec("UPDATE calls SET created_at = UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND WHERE id = ?",
		d.Microseconds(), r.CallID); err != nil {
		t.Fatal(err)
	}
}

// callState 是一个调用在库里的样子：状态、审计列、发送意图、错误码、账本行。
type callState struct {
	status, resolvedBy, evidence, errorCode string
	sent, resolvedAt, finished              bool
	httpStatus                              sql.NullInt64
	usage                                   [3]sql.NullInt64
	kinds                                   []string
}

func (f *fixture) state(t *testing.T, id string) callState {
	t.Helper()
	var c callState
	var by, ev, code sql.NullString
	if err := f.admin.QueryRow(`SELECT status, resolved_by, resolution_evidence, error_code, sent_at IS NOT NULL,
			resolved_at IS NOT NULL, finished_at IS NOT NULL, http_status, usage_prompt_tokens, usage_completion_tokens, usage_total_tokens
		FROM calls WHERE id = ?`, id).Scan(&c.status, &by, &ev, &code, &c.sent, &c.resolvedAt, &c.finished, &c.httpStatus,
		&c.usage[0], &c.usage[1], &c.usage[2]); err != nil {
		t.Fatal(err)
	}
	c.resolvedBy, c.evidence, c.errorCode = by.String, ev.String, code.String
	rows, err := f.admin.Query("SELECT kind FROM ledger WHERE call_id = ? ORDER BY id", id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		c.kinds = append(c.kinds, k)
	}
	return c
}

func (f *fixture) reclaim(t *testing.T) ledger.ReclaimStats {
	t.Helper()
	stats, err := f.s.Reclaim(ctx, reclaimAfter, opTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

// 上界两侧：差一秒不回收，超过一秒就回收。RESERVED 是确定没发出，释放；IN_FLIGHT 可能已经发出，标成未知、继续占着预留。
func TestReclaimBound(t *testing.T) {
	f := newFixture(t, 1000)
	youngR, oldR := f.reserve(t, 10), f.reserve(t, 20)
	youngI, oldI := f.reserve(t, 30), f.reserve(t, 40)
	f.sent(t, youngI)
	f.sent(t, oldI)
	// 回收是全局的：先清掉共用测试库里别的测试留下的、超过上界的调用。否则一轮要处理很多批，
	// 后面的批次扫描时已经过了一秒多，“差一秒”的调用也会越过上界。
	f.reclaim(t)
	f.age(t, youngR, reclaimAfter-time.Second)
	f.age(t, youngI, reclaimAfter-time.Second)
	f.age(t, oldR, reclaimAfter+time.Second)
	f.age(t, oldI, reclaimAfter+time.Second)
	f.reclaim(t)

	if c := f.state(t, youngR.CallID); c.status != "RESERVED" || c.sent {
		t.Errorf("young RESERVED = %+v", c)
	}
	if c := f.state(t, youngI.CallID); c.status != "IN_FLIGHT" {
		t.Errorf("young IN_FLIGHT = %+v", c)
	}
	want := callState{status: "CANCELLED", resolvedBy: "reclaim", evidence: "not_sent", errorCode: "reclaimed_before_send",
		resolvedAt: true, kinds: []string{"reserve", "release"}}
	if c := f.state(t, oldR.CallID); !equalState(c, want) {
		t.Errorf("old RESERVED = %+v, want %+v", c, want)
	}
	want = callState{status: "USAGE_UNKNOWN", errorCode: "reclaimed_in_flight", sent: true, kinds: []string{"reserve"}}
	if c := f.state(t, oldI.CallID); !equalState(c, want) {
		t.Errorf("old IN_FLIGHT = %+v, want %+v", c, want)
	}
	if _, reserved, consumed := f.budget(t); reserved != 10+30+40 || consumed != 0 {
		t.Fatalf("reserved, consumed = %d, %d; want 80, 0 (only the old RESERVED released)", reserved, consumed)
	}
	// 再跑一遍什么都不改
	f.reclaim(t)
	if c := f.state(t, oldR.CallID); !slices.Equal(c.kinds, []string{"reserve", "release"}) {
		t.Fatalf("second reclaim: %v", c.kinds)
	}
	dbtest.Verify(t, f.s)
}

func equalState(a, b callState) bool {
	return a.status == b.status && a.resolvedBy == b.resolvedBy && a.evidence == b.evidence && a.errorCode == b.errorCode &&
		a.sent == b.sent && a.resolvedAt == b.resolvedAt && slices.Equal(a.kinds, b.kinds)
}

// 转换表之外的转换都被拒绝，什么都不改。
func TestTransitionsNotAllowed(t *testing.T) {
	f := newFixture(t, 1000)
	settle := ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, Usage: &ledger.Usage{Prompt: 1, Completion: 1, Total: 2, HasCompletion: true}}

	reserved := f.reserve(t, 10) // 没落发送意图就结算：网关不可能拿到结果
	if err := f.s.Finish(ctx, reserved, settle); !errors.Is(err, ledger.ErrNotAllowed) {
		t.Errorf("RESERVED → SETTLED = %v", err)
	}
	if err := f.s.Finish(ctx, reserved, ledger.Result{Status: ledger.UsageUnknown}); !errors.Is(err, ledger.ErrNotAllowed) {
		t.Errorf("RESERVED → USAGE_UNKNOWN = %v", err)
	}
	inFlight := f.reserve(t, 10) // 结算凭“没发出”
	f.sent(t, inFlight)
	if err := f.s.Finish(ctx, inFlight, ledger.Result{Status: ledger.Settled, Evidence: ledger.NotSent, Usage: settle.Usage}); !errors.Is(err, ledger.ErrNotAllowed) {
		t.Errorf("SETTLED on not_sent = %v", err)
	}
	unknown := f.reserve(t, 10) // 迟到的结果也是未知：不变
	f.finish(t, unknown, ledger.UsageUnknown, 0)
	if err := f.s.Finish(ctx, unknown, ledger.Result{Status: ledger.UsageUnknown}); !errors.Is(err, ledger.ErrNotAllowed) {
		t.Errorf("USAGE_UNKNOWN → USAGE_UNKNOWN = %v", err)
	}
	if err := f.s.MarkSent(ctx, unknown); !errors.Is(err, ledger.ErrNotReserved) { // 发送意图只能从 RESERVED 落
		t.Errorf("MarkSent on USAGE_UNKNOWN = %v", err)
	}
	if c := f.state(t, reserved.CallID); c.status != "RESERVED" || len(c.kinds) != 1 {
		t.Errorf("reserved: %+v", c)
	}
	if c := f.state(t, inFlight.CallID); c.status != "IN_FLIGHT" || len(c.kinds) != 1 {
		t.Errorf("in flight: %+v", c)
	}
	if c := f.state(t, unknown.CallID); c.status != "USAGE_UNKNOWN" || len(c.kinds) != 1 || c.httpStatus.Int64 != 200 {
		t.Errorf("unknown: %+v", c)
	}
	// reconcile 不碰 IN_FLIGHT：报 in-progress，不改
	items := f.s.Reconcile(ctx, []ledger.Proof{{CallID: inFlight.CallID, Status: ledger.Settled, Usage: settle.Usage, Source: ledger.Bill}}, true)
	if items[0].Verdict != ledger.InProgress || items[0].Applied {
		t.Errorf("reconcile IN_FLIGHT = %+v", items[0])
	}
	if c := f.state(t, inFlight.CallID); c.status != "IN_FLIGHT" {
		t.Errorf("in flight after reconcile: %+v", c)
	}
	dbtest.Verify(t, f.s)
}

// 迟到的结果：回收先把 IN_FLIGHT 标成未知，原来的请求随后拿到 2xx 和用量，照常结算，证据记 late_response；
// 写回了 2xx、带键的照常存快照，之后同一个键重放。
func TestLateResult(t *testing.T) {
	f := newFixture(t, 1<<40)
	r := f.keyed(10, "late", "req")
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.sent(t, r)
	f.age(t, r, reclaimAfter+time.Second)
	f.reclaim(t)
	if c := f.state(t, r.CallID); c.status != "USAGE_UNKNOWN" {
		t.Fatalf("after reclaim: %+v", c)
	}
	resp := ledger.Response{Status: 200, ContentType: []string{"application/json"}, Body: []byte(`{"late":true}`)}
	if err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200,
		Usage: &ledger.Usage{Prompt: 3, Completion: 4, Total: 7, HasCompletion: true}, Response: &resp}); err != nil {
		t.Fatal(err)
	}
	if c := f.state(t, r.CallID); c.status != "SETTLED" || c.resolvedBy != "gateway" || c.evidence != "late_response" ||
		!slices.Equal(c.kinds, []string{"reserve", "settle"}) || c.usage[2].Int64 != 7 || c.httpStatus.Int64 != 200 {
		t.Fatalf("late result: %+v", c)
	}
	if _, reserved, consumed := f.budget(t); reserved != 0 || consumed != 7 {
		t.Fatalf("reserved, consumed = %d, %d", reserved, consumed)
	}
	replay, err := f.s.Reserve(ctx, f.keyed(10, "late", "req"))
	if err != nil || replay == nil || replay.CallID != r.CallID || string(replay.Body) != `{"late":true}` {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	// 迟到的结果是释放（上游 4xx）也照常
	r2 := f.reserve(t, 10)
	f.sent(t, r2)
	f.age(t, r2, reclaimAfter+time.Second)
	f.reclaim(t)
	if err := f.s.Finish(ctx, r2, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.Answered, HTTPStatus: 400}); err != nil {
		t.Fatal(err)
	}
	if c := f.state(t, r2.CallID); c.status != "CANCELLED" || c.evidence != "late_response" {
		t.Fatalf("late release: %+v", c)
	}
	dbtest.Verify(t, f.s)
}

// waitBlocked 等到 n 个 QF 服务账号的会话停在 INFO 像 pattern 的语句上（在等锁）。测试账号没有 PROCESS 权限，
// 但能看到同一个账号自己的线程；服务的连接都用这个账号，而拿着锁的管理连接是另一个账号，看不到也不算进来。
// 查 PROCESSLIST 的这条语句自己也匹配以 % 开头的模式（比如 "%usage_events%"），所以排除自己的连接。
func waitBlocked(t *testing.T, pattern string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dbtest.AppDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got int
		if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID() AND INFO LIKE ?", pattern).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions blocked on %q, want %d", got, pattern, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const lockBudgetSQL = "SELECT 1 FROM budgets WHERE account_id = % FOR UPDATE"

// holdBudget 用管理连接锁住这个账户的额度行，返回放开它的函数。
func (f *fixture) holdBudget(t *testing.T) func() {
	t.Helper()
	holder, err := f.admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec("SELECT 1 FROM budgets WHERE account_id = ? FOR UPDATE", f.key.AccountID); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = holder.Rollback() }) }
	t.Cleanup(release)
	return release
}

// 两个回收同时碰到同一批调用（两个实例）：每个调用只转换一次，没有死锁。
func TestConcurrentReclaims(t *testing.T) {
	f := newFixture(t, 1000)
	r, i := f.reserve(t, 10), f.reserve(t, 20)
	f.sent(t, i)
	f.age(t, r, reclaimAfter+time.Second)
	f.age(t, i, reclaimAfter+time.Second)
	release := f.holdBudget(t)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
			errs <- err
		}()
	}
	waitBlocked(t, lockBudgetSQL, 2)
	release()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err) // 包括 1213
		}
	}
	if c := f.state(t, r.CallID); c.status != "CANCELLED" || !slices.Equal(c.kinds, []string{"reserve", "release"}) {
		t.Errorf("RESERVED: %+v", c)
	}
	if c := f.state(t, i.CallID); c.status != "USAGE_UNKNOWN" || !slices.Equal(c.kinds, []string{"reserve"}) {
		t.Errorf("IN_FLIGHT: %+v", c)
	}
	dbtest.Verify(t, f.s)
}

// 回收和收尾同时碰到同一个调用：谁先拿到锁都只有一个结果。收尾先：照常结算，回收跳过；回收先：标未知，收尾按迟到的结果结算。
func TestReclaimAndFinish(t *testing.T) {
	for round := range 3 {
		f := newFixture(t, 1000)
		r := f.reserve(t, 10)
		f.sent(t, r)
		f.age(t, r, reclaimAfter+time.Second)
		release := f.holdBudget(t)
		errs := make(chan error, 2)
		go func() {
			_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
			errs <- err
		}()
		go func() {
			errs <- f.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200,
				Usage: &ledger.Usage{Prompt: 5, Completion: 1, Total: 6, HasCompletion: true}})
		}()
		waitBlocked(t, lockBudgetSQL, 2)
		release()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		c := f.state(t, r.CallID)
		if c.status != "SETTLED" || !slices.Equal(c.kinds, []string{"reserve", "settle"}) || (c.evidence != "response" && c.evidence != "late_response") {
			t.Fatalf("round %d: %+v", round, c)
		}
		t.Logf("round %d: settled on %s", round, c.evidence)
		dbtest.Verify(t, f.s)
	}
}

// 回收和对账同时进行：对账只碰 USAGE_UNKNOWN，回收只碰 RESERVED、IN_FLIGHT。对账读到还在 IN_FLIGHT 的报 in-progress，
// 等回收标成未知之后再跑一遍就能结清；每个调用只转换一次。
func TestReclaimAndReconcile(t *testing.T) {
	f := newFixture(t, 1000)
	inFlight, unknown := f.reserve(t, 10), f.reserve(t, 20)
	f.sent(t, inFlight)
	f.age(t, inFlight, reclaimAfter+time.Second)
	f.finish(t, unknown, ledger.UsageUnknown, 0)
	u := &ledger.Usage{Prompt: 2, Completion: 3, Total: 5, HasCompletion: true}
	proofs := []ledger.Proof{
		{CallID: inFlight.CallID, Line: 2, Status: ledger.Settled, Usage: u, Source: ledger.Bill},
		{CallID: unknown.CallID, Line: 3, Status: ledger.Settled, Usage: u, Source: ledger.Bill},
	}
	release := f.holdBudget(t)
	errs := make(chan error, 1)
	items := make(chan []ledger.Item, 1)
	go func() {
		_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
		errs <- err
	}()
	go func() { items <- f.s.Reconcile(ctx, proofs, true) }()
	waitBlocked(t, lockBudgetSQL, 2) // 回收在等 inFlight，对账在等 unknown
	release()
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	got := <-items
	if got[0].Verdict != ledger.InProgress || got[1].Verdict != ledger.Settle || !got[1].Applied {
		t.Fatalf("first reconcile: %+v", got)
	}
	got = f.s.Reconcile(ctx, proofs, true)
	if got[0].Verdict != ledger.Settle || !got[0].Applied || got[1].Verdict != ledger.Consistent {
		t.Fatalf("second reconcile: %+v", got)
	}
	for _, r := range []ledger.Reservation{inFlight, unknown} {
		if c := f.state(t, r.CallID); c.status != "SETTLED" || c.resolvedBy != "reconcile" || !slices.Equal(c.kinds, []string{"reserve", "settle"}) {
			t.Errorf("%s: %+v", r.CallID, c)
		}
	}
	dbtest.Verify(t, f.s)
}

// 发送意图和回收同时碰到同一个超过上界的 RESERVED：发送意图不锁额度行，只锁调用行。
//   - 回收拿着额度行在等时，发送意图先提交：回收锁住后读到 IN_FLIGHT，按读到的状态标未知，不能释放（扫描时它还是 RESERVED）；
//   - 两边都在等调用行时：谁先都只有一个结果，发送意图成功 ⇔ 调用被标成未知，失败（ErrNotReserved）⇔ 被释放。
func TestMarkSentAndReclaim(t *testing.T) {
	t.Run("intent commits while reclaim waits", func(t *testing.T) {
		f := newFixture(t, 1000)
		r := f.reserve(t, 10)
		f.age(t, r, reclaimAfter+time.Second)
		release := f.holdBudget(t)
		errs := make(chan error, 1)
		go func() {
			_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
			errs <- err
		}()
		waitBlocked(t, lockBudgetSQL, 1)
		f.sent(t, r) // 不等额度行
		release()
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if c := f.state(t, r.CallID); c.status != "USAGE_UNKNOWN" || c.errorCode != "reclaimed_in_flight" || len(c.kinds) != 1 {
			t.Fatalf("call: %+v", c)
		}
		dbtest.Verify(t, f.s)
	})
	t.Run("both wait on the call row", func(t *testing.T) {
		for round := range 3 {
			f := newFixture(t, 1000)
			r := f.reserve(t, 10)
			f.age(t, r, reclaimAfter+time.Second)
			holder, err := f.admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := holder.Exec("SELECT 1 FROM calls WHERE id = ? FOR UPDATE", r.CallID); err != nil {
				t.Fatal(err)
			}
			reclaimErr, sentErr := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
				reclaimErr <- err
			}()
			go func() { sentErr <- f.s.MarkSent(ctx, r) }()
			waitBlocked(t, "SELECT status, purpose, reserved_tokens%FOR UPDATE", 1)
			waitBlocked(t, "UPDATE calls SET status = 'IN_FLIGHT'%", 1)
			_ = holder.Rollback()
			if err := <-reclaimErr; err != nil {
				t.Fatal(err)
			}
			err = <-sentErr
			c := f.state(t, r.CallID)
			switch {
			case err == nil && c.status == "USAGE_UNKNOWN" && len(c.kinds) == 1:
			case errors.Is(err, ledger.ErrNotReserved) && c.status == "CANCELLED" && slices.Equal(c.kinds, []string{"reserve", "release"}):
			default:
				t.Fatalf("round %d: MarkSent %v, call %+v", round, err, c)
			}
			t.Logf("round %d: MarkSent %v → %s", round, err, c.status)
			dbtest.Verify(t, f.s)
		}
	})
}

// 发送意图在测试库上的耗时：只报数（p50、p95），不断言，避免不稳定。
func TestMarkSentLatency(t *testing.T) {
	f := newFixture(t, 1<<40)
	const n = 300
	rs := make([]ledger.Reservation, n)
	for i := range rs {
		rs[i] = f.reserve(t, 1)
	}
	d := make([]time.Duration, n)
	for i, r := range rs {
		start := time.Now()
		f.sent(t, r)
		d[i] = time.Since(start)
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	t.Logf("MarkSent over %d calls: p50 %v, p95 %v, max %v", n, d[n/2], d[n*95/100], d[n-1])
}

// 改坏未知用量恢复加的列（版本 3），Verify 都能报出对应的规则；迁移前的调用不要求 sent_at。
func TestVerifyFindsRecoveryTampering(t *testing.T) {
	type calls struct{ reserved, inFlight, settled, reclaimed, unknown, reconciled ledger.Reservation }
	cases := []struct {
		name   string
		tamper func(c calls) []string
		want   []string
	}{
		{"in flight released", func(c calls) []string {
			return []string{
				"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) SELECT '" + c.inFlight.CallID + "', 'release', -20, 0, UTC_TIMESTAMP(6) FROM DUAL WHERE ? > 0",
				"UPDATE budgets SET reserved_tokens = reserved_tokens - 20 WHERE account_id = ?",
			}
		}, []string{"open"}},
		{"reserved with sent_at", func(c calls) []string { return setCall(c.reserved, "sent_at = UTC_TIMESTAMP(6)") }, []string{"sent"}},
		{"in flight without sent_at", func(c calls) []string { return setCall(c.inFlight, "sent_at = NULL") }, []string{"sent"}},
		{"settled without sent_at", func(c calls) []string { return setCall(c.settled, "sent_at = NULL") }, []string{"sent"}},
		{"reclaim released with sent_at", func(c calls) []string { return setCall(c.reclaimed, "sent_at = UTC_TIMESTAMP(6)") }, []string{"sent"}},
		{"settled without resolution", func(c calls) []string { return setCall(c.settled, "resolved_by = NULL") }, []string{"resolution"}},
		{"settled without resolved_at", func(c calls) []string { return setCall(c.settled, "resolved_at = NULL") },
			[]string{"outbox", "resolution"}}, // outbox 里抄的是结清的时间
		{"unknown with resolution", func(c calls) []string {
			return setCall(c.unknown, "resolved_by = 'gateway', resolution_evidence = 'response', resolved_at = UTC_TIMESTAMP(6)")
		}, []string{"resolution"}},
		{"reclaim on a response", func(c calls) []string { return setCall(c.reclaimed, "resolution_evidence = 'response'") }, []string{"resolution", "sent"}},
		{"reconcile on not_sent", func(c calls) []string { return setCall(c.reconciled, "resolution_evidence = 'not_sent'") }, []string{"resolution"}},
		{"gateway settled without a snapshot", func(c calls) []string {
			return setCall(c.reconciled, "resolved_by = 'gateway', resolution_evidence = 'response'")
		}, []string{"idem_settled_snapshot"}},
		{"pre-migration call without sent_at", func(c calls) []string {
			return setCall(c.settled, "sent_at = NULL, created_at = '2000-01-01'")
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1<<40)
			t.Cleanup(func() { deleteAccount(t, f) })
			var c calls
			c.reserved, c.inFlight, c.settled, c.reclaimed, c.unknown = f.reserve(t, 10), f.reserve(t, 20), f.reserve(t, 30), f.reserve(t, 40), f.reserve(t, 50)
			f.sent(t, c.inFlight)
			f.finish(t, c.settled, ledger.Settled, 30)
			f.finish(t, c.unknown, ledger.UsageUnknown, 0)
			f.age(t, c.reclaimed, reclaimAfter+time.Second)
			f.reclaim(t)
			// 往前改过的 created_at 可能早于版本 3 的迁移时间（别的测试刚重跑过迁移），那样它会被当成迁移前的调用、
			// 不检查 sent_at。改回现在。
			f.age(t, c.reclaimed, 0)
			c.reconciled = f.keyed(60, "reconciled", "req")
			if _, err := f.s.Reserve(ctx, c.reconciled); err != nil {
				t.Fatal(err)
			}
			f.finish(t, c.reconciled, ledger.UsageUnknown, 0)
			u := &ledger.Usage{Prompt: 1, Completion: 1, Total: 2, HasCompletion: true}
			if it := f.s.Reconcile(ctx, []ledger.Proof{{CallID: c.reconciled.CallID, Status: ledger.Settled, Usage: u, Source: ledger.Bill}}, true); !it[0].Applied {
				t.Fatalf("reconcile: %+v", it)
			}
			if got := rules(t, f); len(got) != 0 {
				t.Fatalf("before tampering: %v", got)
			}
			for _, q := range tc.tamper(c) {
				res, err := f.admin.Exec(q, f.key.AccountID)
				if err != nil {
					t.Fatal(err)
				}
				if n, _ := res.RowsAffected(); n != 1 {
					t.Fatalf("tampering changed %d rows: %s", n, q)
				}
			}
			if got := rules(t, f); !slices.Equal(got, tc.want) {
				t.Errorf("rules = %v, want %v", got, tc.want)
			}
		})
	}
}

func setCall(r ledger.Reservation, assignment string) []string {
	return []string{"UPDATE calls SET " + assignment + " WHERE id = '" + r.CallID + "' AND ? > 0"}
}

// 清理和“重新使用正被清理的过期键”同时进行（曾经会死锁）：30 个账户各 10 个过期的键，清理和 300 次重新执行同时出发。
// 代码里没有任何 1213 重试，死锁一出现就是错误（清理返回错误、预留失败），所以这里任何错误都判失败：
// 测试分得出“没有死锁”和“死锁被重试救回来了”。跑 -count=5。
func TestPurgeConcurrentWithKeyReuse(t *testing.T) {
	const accounts, keys, rounds = 30, 10, 10
	fs := make([]*fixture, accounts)
	finishCancelled := func(f *fixture, r ledger.Reservation) error {
		return f.s.Finish(ctx, r, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent})
	}
	for i := range fs {
		fs[i] = newFixture(t, 1<<40)
		for k := range keys {
			r := fs[i].keyed(1, fmt.Sprintf("s%d", k), "req")
			if _, err := fs[i].s.Reserve(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err := finishCancelled(fs[i], r); err != nil {
				t.Fatal(err)
			}
		}
	}
	admin := fs[0].admin
	for round := range rounds {
		for _, f := range fs {
			if _, err := admin.Exec(`UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE account_id = ?`, f.key.AccountID); err != nil {
				t.Fatal(err)
			}
		}
		var mu sync.Mutex
		var errs []string
		fail := func(what string, err error) {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, fmt.Sprintf("%s: %v", what, err))
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			if _, err := fs[0].s.PurgeIdempotency(ctx, 10*time.Second); err != nil {
				fail("purge", err)
			}
		})
		for _, f := range fs {
			wg.Go(func() {
				<-start
				for k := range keys {
					r := f.keyed(1, fmt.Sprintf("s%d", k), "req")
					if _, err := f.s.Reserve(ctx, r); err != nil {
						fail("reserve", err)
						continue
					}
					if err := finishCancelled(f, r); err != nil {
						fail("finish", err)
					}
				}
			})
		}
		close(start)
		wg.Wait()
		if len(errs) > 0 {
			t.Fatalf("round %d: %d errors (1213 is a deadlock): %s", round, len(errs), strings.Join(errs, "; "))
		}
	}
	dbtest.Verify(t, fs[0].s)
}

// 锁、坏行测试里每个事务的上限：被锁住的候选各等这么久，别的账户的事务远用不到。
const shortTimeout = time.Second

// within 在限定时间内跑完 fn；没跑完就判失败（“一轮一定结束”的测试用它，不靠睡眠判断对错）。
func within(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("did not finish within %v", limit)
	}
}

// callsIn 数这个账户里处在 status 的调用。回收是全局的，共用库里别的账户可能有刚越过上界的调用，
// 下面两个测试只断言自己的调用和账户，不断言整库的 ReclaimStats。
func (f *fixture) callsIn(t *testing.T, status string) int {
	t.Helper()
	return f.count(t, "SELECT COUNT(*) FROM calls WHERE account_id = ? AND status = ?", f.key.AccountID, status)
}

// 一个账户的额度行被锁住（超过 opTimeout）：它的候选出错，别的账户超过上界的调用照常处理，
// 错误带出错的个数和锁等待的原因；放开之后下一轮把被挡住的也处理掉。
func TestReclaimBlockedAccount(t *testing.T) {
	stuck, other := newFixture(t, 1000), newFixture(t, 1000)
	s := stuck.reserve(t, 10)
	stuck.age(t, s, 10*time.Hour) // 最早的候选
	oR, oI := other.reserve(t, 10), other.reserve(t, 20)
	other.sent(t, oI)
	other.age(t, oR, reclaimAfter+time.Second)
	other.age(t, oI, reclaimAfter+time.Second)

	release := stuck.holdBudget(t)
	_, err := stuck.s.Reclaim(ctx, reclaimAfter, shortTimeout)
	if err == nil || !strings.HasPrefix(err.Error(), "reclaim: 1 calls failed, first: ") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reclaim while the budget row is locked = %v, want 1 failed call, wrapping the lock wait", err)
	}
	if c := other.state(t, oR.CallID); c.status != "CANCELLED" || c.errorCode != "reclaimed_before_send" {
		t.Errorf("other RESERVED: %+v", c)
	}
	if c := other.state(t, oI.CallID); c.status != "USAGE_UNKNOWN" || c.errorCode != "reclaimed_in_flight" {
		t.Errorf("other IN_FLIGHT: %+v", c)
	}
	if c := stuck.state(t, s.CallID); c.status != "RESERVED" || stuck.callsIn(t, "RESERVED") != 1 {
		t.Errorf("blocked call: %+v, want untouched", c)
	}
	if other.callsIn(t, "CANCELLED") != 1 || other.callsIn(t, "USAGE_UNKNOWN") != 1 || other.callsIn(t, "RESERVED")+other.callsIn(t, "IN_FLIGHT") != 0 {
		t.Errorf("other account: CANCELLED %d, USAGE_UNKNOWN %d, want 1 and 1 and nothing open", other.callsIn(t, "CANCELLED"), other.callsIn(t, "USAGE_UNKNOWN"))
	}

	release()
	stuck.reclaim(t)
	if c := stuck.state(t, s.CallID); c.status != "CANCELLED" || !slices.Equal(c.kinds, []string{"reserve", "release"}) || stuck.callsIn(t, "RESERVED") != 0 {
		t.Errorf("blocked call after release: %+v", c)
	}
	dbtest.Verify(t, stuck.s)
}

// 坏行（超过上界的 RESERVED，账本里却已经有它的 release，结清时撞唯一键 1062）一直出错：
// 它们比一批还多，也不挡住后面的调用，整轮会结束；错误带个数和第一个错误；坏行修好之后下一轮照常处理。
func TestReclaimBadCalls(t *testing.T) {
	const bad = 101 // 多于一批（reclaimBatch = 100）：一批全是出错的，还得往后翻
	f, other := newFixture(t, 1<<40), newFixture(t, 1000)
	var ids []string
	unpoison := func() { // 只删坏行（变动量都是 0）；修好之后回收写的 release 不动，所以修好后再调一次也无妨
		for _, id := range ids {
			if _, err := f.admin.Exec("DELETE FROM ledger WHERE call_id = ? AND kind = 'release' AND reserved_delta = 0", id); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(unpoison) // 测试中途失败也不把坏行留在共用库里
	for range bad {
		r := f.reserve(t, 1)
		f.age(t, r, 10*time.Hour)
		if _, err := f.admin.Exec(`INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at)
			VALUES (?, 'release', 0, 0, UTC_TIMESTAMP(6))`, r.CallID); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.CallID)
	}
	good := other.reserve(t, 10) // 排在所有坏行后面
	other.age(t, good, reclaimAfter+time.Second)

	var err error
	within(t, 30*time.Second, func() { _, err = f.s.Reclaim(ctx, reclaimAfter, opTimeout) })
	var dup *mysql.MySQLError
	if err == nil || !strings.HasPrefix(err.Error(), fmt.Sprintf("reclaim: %d calls failed, first: ", bad)) || !errors.As(err, &dup) || dup.Number != 1062 {
		t.Fatalf("Reclaim with %d bad calls = %v, want their count and the first duplicate-key error", bad, err)
	}
	if c := other.state(t, good.CallID); c.status != "CANCELLED" {
		t.Errorf("good call behind the bad ones: %+v", c)
	}
	if n := f.callsIn(t, "RESERVED"); n != bad {
		t.Errorf("%d bad calls still RESERVED, want all %d (nothing of theirs can be released)", n, bad)
	}

	unpoison()
	f.reclaim(t)
	if n, left := f.callsIn(t, "CANCELLED"), f.callsIn(t, "RESERVED"); n != bad || left != 0 {
		t.Fatalf("after repair: %d CANCELLED, %d RESERVED, want %d and 0", n, left, bad)
	}
	dbtest.Verify(t, f.s)
}

// UnknownBacklog 数全部账户里还占着预留的未知用量调用，给出最早一条的年龄；查询走 calls_status_created，不扫全表。
// 库是各个测试共用的（以前跑的这个测试也会留下更老的）：只能断言“至少有自己这一条”“最早的不比自己这条新”。
func TestUnknownBacklog(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 10)
	f.finish(t, r, ledger.UsageUnknown, 0)
	const age = 10 * 365 * 24 * time.Hour // 比别的测试造的都老
	f.age(t, r, age)
	other := f.reserve(t, 10)
	f.finish(t, other, ledger.Settled, 5)
	f.age(t, other, 2*age) // 已经结算的，再老也不算

	n, oldest, err := f.s.UnknownBacklog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 上界：结算过的那条（两倍年龄）不能算进来；以前留下的未知调用也只比 age 老几天
	if n < 1 || oldest < int64(age.Seconds())-60 || oldest > int64(1.5*age.Seconds()) {
		t.Fatalf("backlog %d calls, oldest %d s; want at least 1 and about %d s", n, oldest, int64(age.Seconds()))
	}
	if key := explainKey(t, f.admin, ledger.UnknownBacklogQuery); key != "calls_status_created" {
		t.Fatalf("the query uses index %q, want calls_status_created", key)
	}
	dbtest.Verify(t, f.s)
}

// explainKey 是 EXPLAIN 给出的查询所用的索引（key 列）；没有用索引时为空。
func explainKey(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN " + query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	key := ""
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		if i := slices.Index(cols, "key"); i >= 0 && vals[i] != nil {
			key = string(vals[i])
		}
	}
	return key
}

// upstreamOf 是调用在库里的上游和上游模型名。
func (f *fixture) upstreamOf(t *testing.T, id string) [2]string {
	t.Helper()
	var u [2]string
	if err := f.admin.QueryRow("SELECT upstream, upstream_model FROM calls WHERE id = ?", id).Scan(&u[0], &u[1]); err != nil {
		t.Fatal(err)
	}
	return u
}

// 改道只在 IN_FLIGHT、上游还是预留里那个时改上游两列；状态、额度、账本行都不动。别的状态、上游对不上，都是 ErrNotInFlight，什么都不改。
func TestReroute(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 10)
	if err := f.s.Reroute(ctx, r, "u2", "um2"); !errors.Is(err, ledger.ErrNotInFlight) { // 还没落发送意图
		t.Fatalf("reroute on RESERVED = %v", err)
	}
	f.sent(t, r)
	before := f.state(t, r.CallID)
	_, reserved, _ := f.budget(t)
	stale := r
	stale.Upstream = "other"
	if err := f.s.Reroute(ctx, stale, "u2", "um2"); !errors.Is(err, ledger.ErrNotInFlight) {
		t.Fatalf("reroute from a different upstream = %v", err)
	}
	stale = r
	stale.UpstreamModel = "other"
	if err := f.s.Reroute(ctx, stale, "u2", "um2"); !errors.Is(err, ledger.ErrNotInFlight) {
		t.Fatalf("reroute from a different upstream model = %v", err)
	}
	if got := f.upstreamOf(t, r.CallID); got != [2]string{"u", "um"} {
		t.Fatalf("upstream after refused reroutes = %v", got)
	}
	if err := f.s.Reroute(ctx, r, "u2", "um2"); err != nil {
		t.Fatal(err)
	}
	after := f.state(t, r.CallID)
	if got := f.upstreamOf(t, r.CallID); got != [2]string{"u2", "um2"} {
		t.Fatalf("upstream = %v", got)
	}
	if _, res, _ := f.budget(t); res != reserved || !slices.Equal(after.kinds, before.kinds) || after.status != "IN_FLIGHT" || !after.sent {
		t.Fatalf("reroute changed more than the upstream: %+v → %+v, reserved %d → %d", before, after, reserved, res)
	}
	// 改过一次之后，旧的上游已经对不上
	if err := f.s.Reroute(ctx, r, "u3", "um3"); !errors.Is(err, ledger.ErrNotInFlight) {
		t.Fatalf("reroute from the old upstream again = %v", err)
	}

	// 结清的、未知的都不能改道
	for _, status := range []ledger.Status{ledger.Settled, ledger.Cancelled, ledger.UsageUnknown} {
		r := f.reserve(t, 10)
		f.sent(t, r)
		res := ledger.Result{Status: status, Evidence: ledger.Answered, HTTPStatus: 200}
		if status == ledger.Settled {
			res.Usage = &ledger.Usage{Prompt: 1, Completion: 1, HasCompletion: true, Total: 2}
		}
		if status == ledger.UsageUnknown {
			res.Evidence = ""
		}
		if err := f.s.Finish(ctx, r, res); err != nil {
			t.Fatal(err)
		}
		if err := f.s.Reroute(ctx, r, "u2", "um2"); !errors.Is(err, ledger.ErrNotInFlight) {
			t.Fatalf("reroute on %s = %v", status, err)
		}
		if got := f.upstreamOf(t, r.CallID); got != [2]string{"u", "um"} {
			t.Fatalf("%s: upstream = %v", status, got)
		}
	}
	dbtest.Verify(t, f.s)
}

// 改道和回收同时碰到同一个超过上界的 IN_FLIGHT：谁先拿到调用行都只有一个结果。
// 改道先提交 ⇔ 回收把它标成未知、上游是新的；回收先 ⇔ 改道 ErrNotInFlight、上游还是旧的（网关不发）。
func TestRerouteAndReclaim(t *testing.T) {
	for round := range 3 {
		f := newFixture(t, 1000)
		r := f.reserve(t, 10)
		f.sent(t, r)
		f.age(t, r, reclaimAfter+time.Second)
		holder, err := f.admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := holder.Exec("SELECT 1 FROM calls WHERE id = ? FOR UPDATE", r.CallID); err != nil {
			t.Fatal(err)
		}
		reclaimErr, rerouteErr := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := f.s.Reclaim(ctx, reclaimAfter, 10*time.Second)
			reclaimErr <- err
		}()
		go func() { rerouteErr <- f.s.Reroute(ctx, r, "u2", "um2") }()
		waitBlocked(t, "SELECT status, purpose, reserved_tokens%FOR UPDATE", 1)
		waitBlocked(t, "UPDATE calls SET upstream = %", 1)
		_ = holder.Rollback()
		if err := <-reclaimErr; err != nil {
			t.Fatal(err)
		}
		err = <-rerouteErr
		c, up := f.state(t, r.CallID), f.upstreamOf(t, r.CallID)
		if c.status != "USAGE_UNKNOWN" || c.errorCode != "reclaimed_in_flight" || len(c.kinds) != 1 {
			t.Fatalf("round %d: call %+v", round, c)
		}
		switch {
		case err == nil && up == [2]string{"u2", "um2"}:
		case errors.Is(err, ledger.ErrNotInFlight) && up == [2]string{"u", "um"}:
		default:
			t.Fatalf("round %d: Reroute %v, upstream %v", round, err, up)
		}
		t.Logf("round %d: Reroute %v → upstream %v", round, err, up)
		dbtest.Verify(t, f.s)
	}
}
