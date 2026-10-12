package ledger_test

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func chatUsage(p, c int64) *ledger.Usage {
	return &ledger.Usage{Prompt: p, Completion: c, Total: p + c, HasCompletion: true}
}

// snapshot 是一个账户在库里的全部状态：调用的各列、账本行、额度行、幂等记录。预演前后逐项比对。
func (f *fixture) snapshot(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, q := range []string{
		`SELECT id, status, http_status, error_code, usage_prompt_tokens, usage_completion_tokens, usage_total_tokens,
			sent_at, finished_at, resolved_by, resolution_evidence, resolved_at FROM calls WHERE account_id = ? ORDER BY id`,
		`SELECT l.call_id, l.kind, l.reserved_delta, l.consumed_delta FROM ledger l JOIN calls c ON c.id = l.call_id
			WHERE c.account_id = ? ORDER BY l.id`,
		`SELECT reserved_tokens, consumed_tokens FROM budgets WHERE account_id = ?`,
		`SELECT idem_key, call_id, response_status FROM idempotency_keys WHERE account_id = ? ORDER BY idem_key`,
	} {
		rows, err := f.admin.Query(q, f.key.AccountID)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(&b, vals)
		}
		rows.Close()
	}
	return b.String()
}

// 对账：预演什么都不改；执行按证据结算和释放；同样的证据再执行一遍什么都不改，干净地报告“一致”；
// 一致、不一致、还在进行中、账本里没有的，各自如实报告。
func TestReconcile(t *testing.T) {
	f := newFixture(t, 1<<40)
	settleMe, releaseMe, answered, badUsage := f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100)
	for _, r := range []ledger.Reservation{settleMe, releaseMe, badUsage} {
		f.sent(t, r)
		if err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.UsageUnknown, HTTPStatus: 504, ErrorCode: "upstream_timeout"}); err != nil {
			t.Fatal(err)
		}
	}
	f.finish(t, answered, ledger.UsageUnknown, 0) // QF 写回过 200（2xx 缺 usage）
	settled, cancelled, inFlight := f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100)
	f.finish(t, settled, ledger.Settled, 42)
	f.finish(t, cancelled, ledger.Cancelled, 0)
	f.sent(t, inFlight)
	missing := "qf-" + rand.Text()

	proofs := []ledger.Proof{
		{CallID: settleMe.CallID, Status: ledger.Settled, Usage: chatUsage(30, 7)},
		{CallID: releaseMe.CallID, Status: ledger.Cancelled},
		{CallID: answered.CallID, Status: ledger.Cancelled},                                          // QF 拿到过 2xx：不能当成没有消费
		{CallID: badUsage.CallID, Status: ledger.Settled, Usage: &ledger.Usage{Prompt: 5, Total: 5}}, // chat 缺 completion_tokens
		{CallID: settled.CallID, Status: ledger.Settled, Usage: &ledger.Usage{Prompt: 42, Total: 42}},
		{CallID: cancelled.CallID, Status: ledger.Cancelled},
		{CallID: inFlight.CallID, Status: ledger.Settled, Usage: chatUsage(1, 1)},
		{CallID: missing, Status: ledger.Cancelled},
	}
	for i := range proofs {
		proofs[i].Line, proofs[i].Source = i+2, ledger.Bill
	}
	want := []ledger.Verdict{ledger.Settle, ledger.Release, ledger.Inconsistent, ledger.Inconsistent,
		ledger.Consistent, ledger.Consistent, ledger.InProgress, ledger.NotFound}
	verdicts := func(items []ledger.Item) (out []ledger.Verdict, applied int) {
		for _, it := range items {
			out = append(out, it.Verdict)
			if it.Applied {
				applied++
			}
		}
		return out, applied
	}

	before := f.snapshot(t)
	got, applied := verdicts(f.s.Reconcile(ctx, proofs, false))
	if !slices.Equal(got, want) || applied != 0 {
		t.Fatalf("dry run: %v (%d applied), want %v", got, applied, want)
	}
	if after := f.snapshot(t); after != before {
		t.Fatalf("the dry run changed the ledger:\n%s\n→\n%s", before, after)
	}

	items := f.s.Reconcile(ctx, proofs, true)
	got, applied = verdicts(items)
	if !slices.Equal(got, want) || applied != 2 {
		t.Fatalf("apply: %v (%d applied), want %v", got, applied, want)
	}
	if items[0].Account != f.account || items[0].Before != ledger.UsageUnknown || items[7].Account != "" {
		t.Fatalf("items: %+v", items)
	}
	c := f.state(t, settleMe.CallID)
	if c.status != "SETTLED" || c.resolvedBy != "reconcile" || c.evidence != "bill" || c.usage[0].Int64 != 30 || c.usage[1].Int64 != 7 ||
		c.usage[2].Int64 != 37 || c.httpStatus.Int64 != 504 || c.errorCode != "upstream_timeout" || !slices.Equal(c.kinds, []string{"reserve", "settle"}) {
		t.Fatalf("settled on the bill: %+v", c)
	}
	if c := f.state(t, releaseMe.CallID); c.status != "CANCELLED" || c.resolvedBy != "reconcile" || !slices.Equal(c.kinds, []string{"reserve", "release"}) {
		t.Fatalf("released on the bill: %+v", c)
	}
	for _, r := range []ledger.Reservation{answered, badUsage} {
		if c := f.state(t, r.CallID); c.status != "USAGE_UNKNOWN" || len(c.kinds) != 1 {
			t.Fatalf("inconsistent evidence changed %s: %+v", r.CallID, c)
		}
	}
	if _, reserved, consumed := f.budget(t); reserved != 300 || consumed != 42+37 {
		t.Fatalf("reserved, consumed = %d, %d", reserved, consumed)
	}

	// 同样的证据再执行一遍：已经结清的两个现在是“一致”，什么都不改，也不靠唯一约束挡
	before = f.snapshot(t)
	want[0], want[1] = ledger.Consistent, ledger.Consistent
	got, applied = verdicts(f.s.Reconcile(ctx, proofs, true))
	if !slices.Equal(got, want) || applied != 0 {
		t.Fatalf("second apply: %v (%d applied), want %v", got, applied, want)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("the second apply changed the ledger")
	}
	// 证据和已经结清的调用不一致：报出来，不改。total 相同、拆分不同的也算不一致
	other := []ledger.Proof{{CallID: settleMe.CallID, Status: ledger.Settled, Usage: chatUsage(30, 8), Source: ledger.Bill},
		{CallID: releaseMe.CallID, Status: ledger.Settled, Usage: chatUsage(1, 1), Source: ledger.Bill},
		{CallID: settled.CallID, Status: ledger.Cancelled, Source: ledger.Bill},
		{CallID: settleMe.CallID, Status: ledger.Settled, Usage: chatUsage(31, 6), Source: ledger.Bill}}
	if got, _ := verdicts(f.s.Reconcile(ctx, other, true)); !slices.Equal(got, []ledger.Verdict{ledger.Inconsistent, ledger.Inconsistent, ledger.Inconsistent, ledger.Inconsistent}) {
		t.Fatalf("inconsistent evidence: %v", got)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("inconsistent evidence changed the ledger")
	}
	dbtest.Verify(t, f.s)
}

// 对账和幂等键：按账单结算、写回过 2xx 的（有快照）照样重放，调用的 http_status 保留，verify 通过；
// 超时之后按账单结算的（没有快照）不再执行；按账单释放的，键空出来，重新执行。
func TestReconcileKeyedCalls(t *testing.T) {
	f := newFixture(t, 1<<40)
	withSnapshot, timedOut, released := f.keyed(10, "snap", "r"), f.keyed(10, "timeout", "r"), f.keyed(10, "free", "r")
	for _, r := range []ledger.Reservation{withSnapshot, timedOut, released} {
		if _, err := f.s.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		f.sent(t, r)
	}
	resp := ledger.Response{Status: 200, Body: []byte(`{"no":"usage"}`)}
	if err := f.s.Finish(ctx, withSnapshot, ledger.Result{Status: ledger.UsageUnknown, HTTPStatus: 200, Response: &resp}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []ledger.Reservation{timedOut, released} {
		if err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.UsageUnknown, HTTPStatus: 504, ErrorCode: "upstream_timeout"}); err != nil {
			t.Fatal(err)
		}
	}
	items := f.s.Reconcile(ctx, []ledger.Proof{
		{CallID: withSnapshot.CallID, Status: ledger.Settled, Usage: chatUsage(3, 4), Source: ledger.Bill},
		{CallID: timedOut.CallID, Status: ledger.Settled, Usage: chatUsage(5, 6), Source: ledger.CallLog},
		{CallID: released.CallID, Status: ledger.Cancelled, Source: ledger.Bill},
	}, true)
	for _, it := range items {
		if !it.Applied {
			t.Fatalf("not applied: %+v", it)
		}
	}
	dbtest.Verify(t, f.s) // 有快照的调用 http_status 保留，没有快照的 SETTLED 由 reconcile 结清，都成立

	replay, err := f.s.Reserve(ctx, f.keyed(10, "snap", "r"))
	if err != nil || replay == nil || string(replay.Body) != `{"no":"usage"}` {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if _, err := f.s.Reserve(ctx, f.keyed(10, "timeout", "r")); !errors.Is(err, ledger.ErrIdempotencyResponseUnavailable) {
		t.Fatalf("settled without a snapshot: %v", err)
	}
	if replay, err := f.s.Reserve(ctx, f.keyed(10, "free", "r")); replay != nil || err != nil {
		t.Fatalf("released key: %v, %v", replay, err)
	}
}

// 账单的格式：表头、调用号、yes 的用量、no 不带数、重复的调用号，任何一处不对整份都不用。
func TestReadBill(t *testing.T) {
	id1, id2 := "qf-"+rand.Text(), "qf-"+rand.Text()
	head := "call_id,consumed,prompt_tokens,completion_tokens,total_tokens\n"
	proofs, err := ledger.ReadBill(strings.NewReader(head + id1 + ",yes,120,10,130\n" + id2 + ",yes,64,,64\n" + "qf-" + rand.Text() + ",no,,,\n"))
	if err != nil || len(proofs) != 3 {
		t.Fatalf("ReadBill = %v, %v", proofs, err)
	}
	if p := proofs[0]; p.CallID != id1 || p.Line != 2 || p.Status != ledger.Settled || p.Source != ledger.Bill || *p.Usage != *chatUsage(120, 10) {
		t.Fatalf("chat row = %+v %+v", p, p.Usage)
	}
	if p := proofs[1]; *p.Usage != (ledger.Usage{Prompt: 64, Total: 64}) {
		t.Fatalf("embeddings row = %+v", p.Usage)
	}
	if p := proofs[2]; p.Status != ledger.Cancelled || p.Usage != nil {
		t.Fatalf("not consumed row = %+v", p)
	}
	for name, body := range map[string]string{
		"empty":            "",
		"header":           "call_id,consumed,prompt,completion,total\n",
		"extra column":     head + id1 + ",yes,1,1,2,x\n",
		"bad call id":      head + "req-1,yes,1,1,2\n",
		"consumed maybe":   head + id1 + ",maybe,1,1,2\n",
		"yes without sums": head + id1 + ",yes,,,\n",
		"negative":         head + id1 + ",yes,-1,1,0\n",
		"not a number":     head + id1 + ",yes,1.5,1,2\n",
		"no with numbers":  head + id1 + ",no,0,0,0\n",
		"duplicate":        head + id1 + ",yes,1,1,2\n" + id1 + ",no,,,\n",
		"bare quote":       head + id1 + "\",yes,1,1,2\n",
		"unclosed quote":   head + "\"" + id1 + ",yes,1,1,2\n",
	} {
		if _, err := ledger.ReadBill(strings.NewReader(body)); !errors.Is(err, ledger.ErrBadEvidence) {
			t.Errorf("%s: %v, want ErrBadEvidence", name, err)
		}
	}
}

// 迁移 0003 只能部分完成（ALTER 做完了、回填和版本号还没写）时再跑一遍，结果和一次跑完相同。
// 迁移前的调用按已有列回填：RESERVED 改成 IN_FLIGHT（发送意图的时间取 created_at），结清的都记网关收尾、证据按错误码推，
// 推不出的 sent_at 留空；回填之后 Verify 成立。
func TestMigration3Backfill(t *testing.T) {
	dropAll(t)
	dsn := dbtest.MigrateDSN(t)
	admin := dbtest.Admin(t)
	run := func(partial bool) string {
		t.Helper()
		for _, tbl := range tables {
			if _, err := admin.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
			t.Fatal(err)
		}
		exec(t, admin, unmigrate3...) // 回到版本 2 的表结构
		exec(t, admin, legacyRows...)
		if partial {
			exec(t, admin, alter3)
		}
		from, to, err := ledger.Migrate(ctx, dsn)
		if err != nil || from != 2 || to != ledger.SchemaVersion {
			t.Fatalf("Migrate = %d, %d, %v", from, to, err)
		}
		var b strings.Builder
		// 时间在 SQL 里转成字符串：各处测试连接串的 parseTime 不一样。
		rows, err := admin.Query(`SELECT CONCAT_WS('|', id, status, COALESCE(CAST(sent_at AS CHAR(19)), 'NULL'),
				COALESCE(resolved_by, 'NULL'), COALESCE(resolution_evidence, 'NULL'), COALESCE(CAST(resolved_at AS CHAR(19)), 'NULL'),
				COALESCE(CAST(finished_at AS CHAR(19)), 'NULL'))
			FROM calls ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(&b, line)
		}
		return b.String()
	}
	once, rerun := run(false), run(true)
	if once != rerun {
		t.Fatalf("partial migration then rerun differs:\n%s\nvs\n%s", once, rerun)
	}
	// id|status|sent_at|resolved_by|resolution_evidence|resolved_at|finished_at
	want := []string{
		"qf-legacy-cancelled-4xx|CANCELLED|NULL|gateway|response|2026-01-01 00:00:02|2026-01-01 00:00:02",
		"qf-legacy-cancelled-early|CANCELLED|NULL|gateway|not_sent|2026-01-01 00:00:02|2026-01-01 00:00:02",
		"qf-legacy-reserved|IN_FLIGHT|2026-01-01 00:00:01|NULL|NULL|NULL|NULL",
		"qf-legacy-settled|SETTLED|NULL|gateway|response|2026-01-01 00:00:02|2026-01-01 00:00:02",
		"qf-legacy-unknown|USAGE_UNKNOWN|NULL|NULL|NULL|NULL|2026-01-01 00:00:02",
	}
	if got := strings.Split(strings.TrimSpace(once), "\n"); !reflect.DeepEqual(got, want) {
		t.Fatalf("backfill:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	s := dbtest.Store(t)
	dbtest.Verify(t, s)
	// 回填成 IN_FLIGHT 的旧调用由回收标成未知，不会被当成没发出而释放
	if _, err := s.Reclaim(ctx, reclaimAfter, opTimeout); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := admin.QueryRow("SELECT status FROM calls WHERE id = 'qf-legacy-reserved'").Scan(&status); err != nil || status != "USAGE_UNKNOWN" {
		t.Fatalf("legacy RESERVED after reclaim: %s, %v", status, err)
	}
	dbtest.Verify(t, s)
}

func exec(t *testing.T, db *sql.DB, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%v: %s", err, q)
		}
	}
}

// unmigrate3 把版本 3 的表结构退回版本 2，测试用来造“迁移前的库”。
var unmigrate3 = []string{
	`ALTER TABLE calls DROP INDEX calls_status_created, DROP COLUMN sent_at, DROP COLUMN resolved_by,
		DROP COLUMN resolution_evidence, DROP COLUMN resolved_at,
		MODIFY COLUMN status ENUM('RESERVED', 'SETTLED', 'CANCELLED', 'USAGE_UNKNOWN') NOT NULL`,
	"DELETE FROM schema_migrations WHERE version >= 3", // 之后的迁移也随之重跑：0004 的列已经在了，跳过
}

// alter3 是迁移 0003 里的那条 ALTER（和迁移文件相同）。
const alter3 = `ALTER TABLE calls
	MODIFY COLUMN status ENUM('RESERVED', 'SETTLED', 'CANCELLED', 'USAGE_UNKNOWN', 'IN_FLIGHT') NOT NULL,
	ADD COLUMN sent_at DATETIME(6) NULL,
	ADD COLUMN resolved_by ENUM('gateway', 'reclaim', 'reconcile') NULL,
	ADD COLUMN resolution_evidence ENUM('response', 'not_sent', 'late_response', 'call_log', 'bill') NULL,
	ADD COLUMN resolved_at DATETIME(6) NULL,
	ADD KEY calls_status_created (status, created_at)`

// legacyRows 是迁移前（版本 2）的一个账户和五个调用，账本和额度行都对得上。
var legacyRows = func() []string {
	q := []string{
		"INSERT INTO accounts (id, name, created_at) VALUES (1, 'legacy', '2026-01-01')",
		"INSERT INTO budgets (account_id, limit_tokens, reserved_tokens, consumed_tokens, updated_at) VALUES (1, 1000, 20, 7, '2026-01-01')",
		"INSERT INTO api_keys (id, account_id, sha256, prefix, models, status, created_at) VALUES (1, 1, REPEAT('a', 64), 'qfk_legacy', '[\"m\"]', 'active', '2026-01-01')",
	}
	call := func(id, status, errorCode string, finished bool, usage string) {
		fin := "NULL"
		if finished {
			fin = "'2026-01-01 00:00:02'"
		}
		q = append(q, fmt.Sprintf(`INSERT INTO calls (id, account_id, key_id, status, purpose, model_alias, upstream, upstream_model,
			est_input_tokens, est_output_tokens, reserved_tokens, usage_prompt_tokens, usage_total_tokens, error_code, created_at, finished_at)
			VALUES ('%s', 1, 1, '%s', 'embeddings', 'm', 'u', 'um', 10, 0, 10, %s, %s, %s, '2026-01-01 00:00:01', %s)`,
			id, status, usage, usage, errorCode, fin),
			fmt.Sprintf("INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('%s', 'reserve', 10, 0, '2026-01-01')", id))
	}
	call("qf-legacy-reserved", "RESERVED", "NULL", false, "NULL")
	call("qf-legacy-unknown", "USAGE_UNKNOWN", "'upstream_timeout'", true, "NULL")
	call("qf-legacy-settled", "SETTLED", "NULL", true, "7")
	call("qf-legacy-cancelled-4xx", "CANCELLED", "'upstream_rate_limited'", true, "NULL")
	call("qf-legacy-cancelled-early", "CANCELLED", "'client_canceled'", true, "NULL")
	return append(q,
		"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('qf-legacy-settled', 'settle', -10, 7, '2026-01-01')",
		"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('qf-legacy-cancelled-4xx', 'release', -10, 0, '2026-01-01')",
		"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES ('qf-legacy-cancelled-early', 'release', -10, 0, '2026-01-01')",
	)
}()
