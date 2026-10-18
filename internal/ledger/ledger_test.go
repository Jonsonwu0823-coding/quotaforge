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

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

func TestMain(m *testing.M) { dbtest.Main(m) }

var ctx = context.Background()

var tables = []string{"idempotency_keys", "ledger", "outbox", "calls", "api_keys", "budgets", "accounts", "schema_migrations",
	"usage_events", "usage_seen", "usage_daily"}

// dropAll 删掉全部表，得到一个空库。测试结束时迁移回最新，别的测试照常用。
func dropAll(t *testing.T) {
	t.Helper()
	admin := dbtest.Admin(t)
	for _, tbl := range tables {
		if _, err := admin.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatal(err)
		}
	}
	dsn := dbtest.MigrateDSN(t)
	t.Cleanup(func() {
		if err := dbtest.Migrate(ctx, dsn); err != nil { // 开关表也是重建的，迁移之后重新打开
			t.Errorf("re-migrate: %v", err)
		}
	})
}

func versions(t *testing.T) []int {
	t.Helper()
	rows, err := dbtest.Admin(t).Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func wantAllVersions(t *testing.T) {
	t.Helper()
	var want []int
	for v := 1; v <= ledger.SchemaVersion; v++ {
		want = append(want, v)
	}
	if got := versions(t); !slices.Equal(got, want) {
		t.Fatalf("schema_migrations = %v, want %v", got, want)
	}
}

func TestMigrateEmptyDatabase(t *testing.T) {
	dropAll(t)
	from, to, err := ledger.Migrate(ctx, dbtest.MigrateDSN(t))
	if err != nil || from != 0 || to != ledger.SchemaVersion {
		t.Fatalf("Migrate = %d, %d, %v", from, to, err)
	}
	wantAllVersions(t)
	if err := dbtest.Store(t).CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateAgainDoesNothing(t *testing.T) {
	dsn := dbtest.MigrateDSN(t)
	if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	from, to, err := ledger.Migrate(ctx, dsn)
	if err != nil || from != ledger.SchemaVersion || to != ledger.SchemaVersion {
		t.Fatalf("second Migrate = %d, %d, %v", from, to, err)
	}
	wantAllVersions(t)
}

// 两个进程同时迁移空库：都成功，只有一个真正执行了迁移，每个版本恰好记一次。
// 每个 Migrate 自己开连接池，等同于两个进程各自的会话。
func TestConcurrentMigrate(t *testing.T) {
	dsn := dbtest.MigrateDSN(t)
	for round := range 5 {
		dropAll(t)
		start := make(chan struct{})
		froms := make([]int, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i := range 2 {
			wg.Go(func() {
				<-start
				froms[i], _, errs[i] = ledger.Migrate(ctx, dsn)
			})
		}
		close(start)
		wg.Wait()
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: errors %v, %v", round, errs[0], errs[1])
		}
		slices.Sort(froms)
		if froms[0] != 0 || froms[1] != ledger.SchemaVersion {
			t.Fatalf("round %d: from versions %v, want one 0 and one %d", round, froms, ledger.SchemaVersion)
		}
		wantAllVersions(t)
	}
}

func TestCheckSchema(t *testing.T) {
	s := dbtest.Store(t)
	admin := dbtest.Admin(t)
	if _, err := admin.Exec("INSERT INTO schema_migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", ledger.SchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	removed := false
	remove := func() {
		if !removed {
			removed = true
			if _, err := admin.Exec("DELETE FROM schema_migrations WHERE version = ?", ledger.SchemaVersion+1); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(remove)
	err := s.CheckSchema(ctx)
	want := fmt.Sprintf("schema version is %d, this build needs %d", ledger.SchemaVersion+1, ledger.SchemaVersion)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("CheckSchema on a newer database = %v, want %q", err, want)
	}
	if _, _, err := ledger.Migrate(ctx, dbtest.MigrateDSN(t)); err == nil || !strings.Contains(err.Error(), "newer than this build") {
		t.Fatalf("Migrate on a newer database = %v", err)
	}
	remove()
	if err := s.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// 版本表在、但一行都没有（建了版本表，0001 中途失败）：版本 0，拒绝。之后迁移回最新：0001 可以重跑。
	dsn := dbtest.MigrateDSN(t)
	restore := func() {
		if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
			t.Errorf("re-migrate: %v", err)
		}
	}
	t.Cleanup(restore)
	if _, err := admin.Exec("DELETE FROM schema_migrations"); err != nil {
		t.Fatal(err)
	}
	want = fmt.Sprintf("schema version is 0, this build needs %d", ledger.SchemaVersion)
	if err := s.CheckSchema(ctx); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("CheckSchema with an empty schema_migrations = %v, want %q", err, want)
	}
	restore()
	wantAllVersions(t)
	if err := s.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}

	dropAll(t)
	if err := s.CheckSchema(ctx); err == nil || !strings.Contains(err.Error(), "not migrated") {
		t.Fatalf("CheckSchema on an empty database = %v", err)
	}
}

// QF 服务的账号只能增删改查：迁移不了，也建不了表。
func TestAppAccountCannotMigrate(t *testing.T) {
	if _, _, err := ledger.Migrate(ctx, dbtest.AppDSN(t)); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("Migrate with the app account = %v, want a permission error", err)
	}
}

func TestOpenErrorsHideDSN(t *testing.T) {
	const secret = "pw-SECRET-MARKER"
	for _, dsn := range []string{"user:" + secret + "@tcp(127.0.0.1:1)", "user:" + secret + "@bogus(x)/db", "user:" + secret + "@tcp(127.0.0.1:1)/db?parseTime=maybe"} {
		_, err := ledger.Open(dsn)
		if err == nil {
			continue // 格式对的连接串不会立即连接
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error contains the password: %v", err)
		}
	}
	_, _, err := ledger.Migrate(ctx, "user:"+secret+"@tcp(127.0.0.1:1)/db?timeout=1s")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("Migrate to a closed port: %v", err)
	}
}

// fixture 是一个只属于当前测试的账户和它的一把 Key。
type fixture struct {
	s       *ledger.Store
	admin   *sql.DB
	account string
	key     *ledger.Key
}

func newFixture(t *testing.T, limit int64) *fixture {
	t.Helper()
	f := &fixture{s: dbtest.Store(t), admin: dbtest.Admin(t)}
	f.account = dbtest.Account(t, f.s, limit)
	f.key = dbtest.Key(t, f.s, f.account)
	return f
}

// reservation 是一次预留；调用号和网关生成的格式相同，查询和对账要认它。
func (f *fixture) reservation(amount int64) ledger.Reservation {
	return ledger.Reservation{
		CallID: "qf-" + rand.Text(), AccountID: f.key.AccountID, KeyID: f.key.ID,
		Purpose: "chat", Alias: "m", Upstream: "u", UpstreamModel: "um", EstInput: amount - 1, EstOutput: 1,
	}
}

// timedOut 让这几个调用落了发送意图之后等上游超时：收尾成 USAGE_UNKNOWN、504 upstream_timeout。
func (f *fixture) timedOut(t *testing.T, rs ...ledger.Reservation) {
	t.Helper()
	for _, r := range rs {
		f.sent(t, r)
		if err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.UsageUnknown, HTTPStatus: 504, ErrorCode: "upstream_timeout"}); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) reserve(t *testing.T, amount int64) ledger.Reservation {
	t.Helper()
	r := f.reservation(amount)
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	return r
}

// sent 落发送意图：RESERVED → IN_FLIGHT。
func (f *fixture) sent(t *testing.T, r ledger.Reservation) {
	t.Helper()
	if err := f.s.MarkSent(ctx, r); err != nil {
		t.Fatal(err)
	}
}

// finish 照网关的顺序收尾一个 RESERVED 的调用：先落发送意图，再按拿到的上游回答收尾（释放算上游 4xx）。
func (f *fixture) finish(t *testing.T, r ledger.Reservation, status ledger.Status, total int64) {
	t.Helper()
	f.sent(t, r)
	res := ledger.Result{Status: status, HTTPStatus: 200}
	switch status {
	case ledger.Settled:
		res.Usage, res.Evidence = &ledger.Usage{Prompt: total, Total: total}, ledger.Answered
	case ledger.Cancelled:
		res.HTTPStatus, res.Evidence = 400, ledger.Answered
	}
	if err := f.s.Finish(ctx, r, res); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) budget(t *testing.T) (limit, reserved, consumed int64) {
	t.Helper()
	return dbtest.Budget(t, f.admin, f.account)
}

func (f *fixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReserveIsConditional(t *testing.T) {
	f := newFixture(t, 100)
	f.reserve(t, 60)
	r := f.reservation(41)
	if _, err := f.s.Reserve(ctx, r); !errors.Is(err, ledger.ErrQuotaExhausted) {
		t.Fatalf("Reserve over the limit = %v, want ErrQuotaExhausted", err)
	}
	// 被拒的预留什么都不写
	if n := f.count(t, "SELECT COUNT(*) FROM calls WHERE id = ?", r.CallID); n != 0 {
		t.Fatalf("rejected reservation wrote %d calls rows", n)
	}
	f.reserve(t, 40) // 正好用完
	if _, reserved, _ := f.budget(t); reserved != 100 {
		t.Fatalf("reserved = %d, want 100", reserved)
	}
	dbtest.Verify(t, f.s)
}

func TestFinish(t *testing.T) {
	f := newFixture(t, 1000)
	settled, cancelled, unknown := f.reserve(t, 100), f.reserve(t, 200), f.reserve(t, 300)
	f.finish(t, settled, ledger.Settled, 150) // 实际用量可以超过预留
	f.finish(t, cancelled, ledger.Cancelled, 0)
	f.finish(t, unknown, ledger.UsageUnknown, 0)
	if _, reserved, consumed := f.budget(t); reserved != 300 || consumed != 150 {
		t.Fatalf("reserved, consumed = %d, %d, want 300, 150 (unknown keeps its reservation)", reserved, consumed)
	}
	// 已经结清的调用不能再收尾，什么都不改。USAGE_UNKNOWN 不在这里：网关迟到的结果可以结清它（见 recovery_test.go）。
	for _, r := range []ledger.Reservation{settled, cancelled} {
		err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, Usage: &ledger.Usage{Total: 1}})
		if !errors.Is(err, ledger.ErrNotAllowed) {
			t.Fatalf("finishing %s twice = %v, want ErrNotAllowed", r.CallID, err)
		}
	}
	if _, reserved, consumed := f.budget(t); reserved != 300 || consumed != 150 {
		t.Fatalf("after duplicate finishes: reserved, consumed = %d, %d", reserved, consumed)
	}
	dbtest.Verify(t, f.s)
}

// 唯一约束是第二道防线：即使绕过转换表，同一个调用也插不进第二条 settle。
func TestLedgerCallKindIsUnique(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 10)
	f.finish(t, r, ledger.Settled, 10)
	if _, err := f.admin.Exec("INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) VALUES (?, 'settle', 0, 1, UTC_TIMESTAMP(6))", r.CallID); err == nil {
		t.Fatal("inserted a second settle entry for the same call")
	}
}

func TestAccounts(t *testing.T) {
	s := dbtest.Store(t)
	name := dbtest.Account(t, s, 5)
	if err := s.CreateAccount(ctx, name, 5, ledger.Rate{}); !errors.Is(err, ledger.ErrAccountExists) {
		t.Fatalf("duplicate account = %v", err)
	}
	for _, bad := range []string{"", "Upper", "-x", "has space", strings.Repeat("a", 65)} {
		if err := s.CreateAccount(ctx, bad, 1, ledger.Rate{}); err == nil {
			t.Fatalf("account name %q accepted", bad)
		}
	}
	if err := s.CreateAccount(ctx, "neg-"+name, -1, ledger.Rate{}); err == nil {
		t.Fatal("negative limit accepted")
	}
	if err := s.SetLimit(ctx, name, 7); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLimit(ctx, name, 7); err != nil { // 值不变也算成功
		t.Fatal(err)
	}
	if err := s.SetLimit(ctx, "no-such-"+name, 7); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("SetLimit on a missing account = %v", err)
	}
	u, err := s.Usage(ctx, name)
	if err != nil || len(u) != 1 || u[0].Limit != 7 || u[0].Remaining != 7 {
		t.Fatalf("Usage = %+v, %v", u, err)
	}
	if _, err := s.Usage(ctx, "no-such-"+name); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("Usage on a missing account = %v", err)
	}
}

func TestKeys(t *testing.T) {
	s := dbtest.Store(t)
	name := dbtest.Account(t, s, 5)
	for _, models := range [][]string{nil, {""}, {"a", "a"}, {strings.Repeat("m", 256)}} {
		if _, err := s.IssueKey(ctx, name, models, ledger.Rate{}); err == nil {
			t.Fatalf("models %q accepted", models)
		}
	}
	if _, err := s.IssueKey(ctx, "no-such-"+name, []string{"a"}, ledger.Rate{}); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("IssueKey on a missing account = %v", err)
	}
	k, err := s.IssueKey(ctx, name, []string{"a", "b"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(k.Secret, "qfk_") || len(k.Secret) != 47 || !strings.HasPrefix(k.Secret, k.Prefix) || len(k.Prefix) != 12 {
		t.Fatalf("issued key has the wrong shape (prefix %q)", k.Prefix)
	}
	got, err := s.KeyByHash(ctx, apikey.Hash(k.Secret))
	if err != nil || got == nil || got.ID != k.ID || got.Account != name || !got.Allows("b") || got.Allows("c") {
		t.Fatalf("KeyByHash = %+v, %v", got, err)
	}
	// 库里只有哈希和前缀
	var stored string
	if err := dbtest.Admin(t).QueryRow("SELECT CONCAT(sha256, prefix, models) FROM api_keys WHERE id = ?", k.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, k.Secret) {
		t.Fatal("api_keys stores the key itself")
	}
	list, err := s.ListKeys(ctx, name)
	if err != nil || len(list) != 1 || list[0].Status != "active" || !slices.Equal(list[0].Models, []string{"a", "b"}) {
		t.Fatalf("ListKeys = %+v, %v", list, err)
	}
	if err := s.RevokeKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := s.KeyByHash(ctx, apikey.Hash(k.Secret)); got != nil || err != nil {
		t.Fatalf("revoked key: KeyByHash = %+v, %v", got, err)
	}
	if err := s.RevokeKey(ctx, k.ID); !errors.Is(err, ledger.ErrKeyNotFound) {
		t.Fatalf("revoking twice = %v", err)
	}
	if got, err := s.KeyByHash(ctx, apikey.Hash("qfk_never-issued")); got != nil || err != nil {
		t.Fatalf("unknown key: KeyByHash = %+v, %v", got, err)
	}
}

// models 列解不出别名数组（库里是合法的 JSON，但不是数组）时，按哈希查 Key 和列出 Key 都报错，不当成没有别名。
func TestKeyWithBadModels(t *testing.T) {
	s := dbtest.Store(t)
	name := dbtest.Account(t, s, 5)
	k, err := s.IssueKey(ctx, name, []string{"a"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	exec(t, dbtest.Admin(t), fmt.Sprintf(`UPDATE api_keys SET models = '{"a": 1}' WHERE id = %d`, k.ID))
	if got, err := s.KeyByHash(ctx, apikey.Hash(k.Secret)); got != nil || err == nil || !strings.Contains(err.Error(), "models column") {
		t.Fatalf("KeyByHash = %+v, %v", got, err)
	}
	if list, err := s.ListKeys(ctx, name); list != nil || err == nil || !strings.Contains(err.Error(), "models column") {
		t.Fatalf("ListKeys = %+v, %v", list, err)
	}
}

// 改坏额度行或账本行，Verify 都能报出对应的规则；改坏的账户在测试结束时整个删掉。
func TestVerifyFindsTampering(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(f *fixture, settled, cancelled, unknown ledger.Reservation) []string // 执行的 SQL
		want   []string                                                                  // 必须报出的规则
		only   bool                                                                      // 只报这些规则
	}{
		{"budget reserved", func(f *fixture, _, _, _ ledger.Reservation) []string {
			return []string{"UPDATE budgets SET reserved_tokens = reserved_tokens + 1 WHERE account_id = ?"}
		}, []string{"budget_reserved"}, true},
		{"budget consumed", func(f *fixture, _, _, _ ledger.Reservation) []string {
			return []string{"UPDATE budgets SET consumed_tokens = consumed_tokens - 1 WHERE account_id = ?"}
		}, []string{"budget_consumed"}, true},
		{"reserved negative", func(f *fixture, _, _, _ ledger.Reservation) []string {
			return []string{"UPDATE budgets SET reserved_tokens = -1 WHERE account_id = ?"}
		}, []string{"budget_reserved", "reserved_not_negative"}, true},
		{"reserve entry", func(f *fixture, s, _, _ ledger.Reservation) []string {
			return []string{"UPDATE ledger SET reserved_delta = reserved_delta + 1 WHERE kind = 'reserve' AND call_id = '" + s.CallID + "' AND ? > 0"}
		}, []string{"reserve_entry", "settled", "budget_reserved"}, true},
		{"settled consumed", func(f *fixture, s, _, _ ledger.Reservation) []string {
			return []string{
				"UPDATE ledger SET consumed_delta = consumed_delta + 1 WHERE kind = 'settle' AND call_id = '" + s.CallID + "' AND ? > 0",
				"UPDATE budgets SET consumed_tokens = consumed_tokens + 1 WHERE account_id = ?",
			}
		}, []string{"settled"}, true},
		// 已释放的调用少释放 1，额度行同样多占 1：两边仍然相等，只有“已释放的预留变动合计为 0”能发现。
		{"cancelled reserved", func(f *fixture, _, c, _ ledger.Reservation) []string {
			return []string{
				"UPDATE ledger SET reserved_delta = reserved_delta + 1 WHERE kind = 'release' AND call_id = '" + c.CallID + "' AND ? > 0",
				"UPDATE budgets SET reserved_tokens = reserved_tokens + 1 WHERE account_id = ?",
			}
		}, []string{"cancelled"}, true},
		{"cancelled turned into settle", func(f *fixture, _, c, _ ledger.Reservation) []string {
			return []string{"UPDATE ledger SET kind = 'settle' WHERE kind = 'release' AND call_id = '" + c.CallID + "' AND ? > 0"}
		}, []string{"cancelled"}, true},
		{"unknown released", func(f *fixture, _, _, u ledger.Reservation) []string {
			return []string{
				"INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at) SELECT '" + u.CallID + "', 'release', -30, 0, UTC_TIMESTAMP(6) FROM DUAL WHERE ? > 0",
				"UPDATE budgets SET reserved_tokens = reserved_tokens - 30 WHERE account_id = ?",
			}
		}, []string{"open"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1000)
			t.Cleanup(func() { deleteAccount(t, f) })
			settled, cancelled, unknown := f.reserve(t, 10), f.reserve(t, 20), f.reserve(t, 30)
			f.finish(t, settled, ledger.Settled, 12)
			f.finish(t, cancelled, ledger.Cancelled, 0)
			f.finish(t, unknown, ledger.UsageUnknown, 0)
			if got := rules(t, f); len(got) != 0 {
				t.Fatalf("before tampering: %v", got)
			}
			for _, q := range tc.tamper(f, settled, cancelled, unknown) {
				if _, err := f.admin.Exec(q, f.key.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			got := rules(t, f)
			for _, w := range tc.want {
				if !slices.Contains(got, w) {
					t.Errorf("rule %s not reported; got %v", w, got)
				}
			}
			if tc.only && len(got) != len(tc.want) {
				t.Errorf("rules = %v, want exactly %v", got, tc.want)
			}
		})
	}
}

// rules 返回这个账户上不成立的规则名（去重、排序）。
// tamperRules 先确认账本干净，再执行改坏数据的语句（每条用 ? 带上这个账户的编号、正好改一行），最后核对 Verify 报出的规则。
func (f *fixture) tamperRules(t *testing.T, tamper, want []string) {
	t.Helper()
	if got := rules(t, f); len(got) != 0 {
		t.Fatalf("before tampering: %v", got)
	}
	for _, q := range tamper {
		res, err := f.admin.Exec(q, f.key.AccountID)
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Fatalf("tampering changed %d rows: %s", n, q)
		}
	}
	if got := rules(t, f); !slices.Equal(got, want) {
		t.Errorf("rules = %v, want %v", got, want)
	}
}

func rules(t *testing.T, f *fixture) []string {
	t.Helper()
	v, err := f.s.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, x := range v {
		if x.Account == f.account && !slices.Contains(out, x.Rule) {
			out = append(out, x.Rule)
		}
	}
	slices.Sort(out)
	return out
}

func deleteAccount(t *testing.T, f *fixture) {
	t.Helper()
	id := f.key.AccountID
	for _, q := range []string{
		"DELETE FROM idempotency_keys WHERE account_id = ?",
		"DELETE l FROM ledger l JOIN calls c ON c.id = l.call_id WHERE c.account_id = ?",
		"DELETE FROM outbox WHERE account_id = ?",
		"DELETE FROM usage_seen WHERE account_id = ?",
		"DELETE FROM usage_daily WHERE account_id = ?",
		"DELETE FROM calls WHERE account_id = ?",
		"DELETE FROM api_keys WHERE account_id = ?",
		"DELETE FROM budgets WHERE account_id = ?",
		"DELETE FROM accounts WHERE id = ?",
	} {
		if _, err := f.admin.Exec(q, id); err != nil {
			t.Error(err)
		}
	}
}

// 收尾先锁额度行、再动调用行：另一个事务占着额度行时，Finish 在等，调用行还没被它锁住（NOWAIT 拿得到）。
// 正确的顺序下这里永远不会失败；顺序反了，Finish 一开始就锁住调用行，NOWAIT 立刻失败。
func TestFinishLocksBudgetFirst(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 10)
	f.sent(t, r)
	holder, err := f.admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec("SELECT 1 FROM budgets WHERE account_id = ? FOR UPDATE", f.key.AccountID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- f.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, Usage: &ledger.Usage{Total: 10}})
	}()
	for range 100 {
		select {
		case err := <-done:
			t.Fatalf("Finish returned while the budget row was locked: %v", err)
		default:
		}
		probe, err := f.admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = probe.Exec("SELECT id FROM calls WHERE id = ? FOR UPDATE NOWAIT", r.CallID)
		_ = probe.Rollback()
		if err != nil {
			t.Fatalf("Finish locked the call row before the budget row: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	dbtest.Verify(t, f.s)
}

// unmigrate4 回到版本 3 的表结构：去掉 0004 加的四列。
var unmigrate4 = []string{
	"ALTER TABLE api_keys DROP COLUMN rpm_limit, DROP COLUMN tpm_limit",
	"ALTER TABLE accounts DROP COLUMN rpm_limit, DROP COLUMN tpm_limit",
	"DELETE FROM schema_migrations WHERE version >= 4", // 0005 随之重跑：它的列已经在了，跳过
}

// 迁移 0004：版本 3 的库里已有的 Key 和账户，迁移后限流限额都是 0（不限）；只做了一张表的 ALTER 就中断时，重跑能补完。
func TestMigration4RateLimits(t *testing.T) {
	dropAll(t)
	dsn := dbtest.MigrateDSN(t)
	admin := dbtest.Admin(t)
	if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	exec(t, admin, unmigrate4...)
	exec(t, admin,
		"INSERT INTO accounts (id, name, created_at) VALUES (1, 'legacy', '2026-01-01')",
		"INSERT INTO budgets (account_id, limit_tokens, updated_at) VALUES (1, 1000, '2026-01-01')",
		"INSERT INTO api_keys (id, account_id, sha256, prefix, models, status, created_at) VALUES (1, 1, REPEAT('a', 64), 'qfk_legacy', '[\"m\"]', 'active', '2026-01-01')")
	limits := func() string {
		t.Helper()
		var s string
		if err := admin.QueryRow(`SELECT CONCAT_WS('|', k.rpm_limit, k.tpm_limit, a.rpm_limit, a.tpm_limit)
			FROM api_keys k JOIN accounts a ON a.id = k.account_id WHERE k.id = 1`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, partial := range []bool{false, true} {
		if partial { // 中断在两条 ALTER 之间：api_keys 已经加了列，accounts 还没有，版本还是 3
			exec(t, admin, unmigrate4...)
			exec(t, admin, "ALTER TABLE api_keys ADD COLUMN rpm_limit BIGINT NOT NULL DEFAULT 0, ADD COLUMN tpm_limit BIGINT NOT NULL DEFAULT 0")
		}
		from, to, err := ledger.Migrate(ctx, dsn)
		if err != nil || from != 3 || to != ledger.SchemaVersion {
			t.Fatalf("partial=%v: Migrate = %d, %d, %v", partial, from, to, err)
		}
		if got := limits(); got != "0|0|0|0" {
			t.Fatalf("partial=%v: limits after migrating = %s, want all 0", partial, got)
		}
	}
	k, err := dbtest.Store(t).KeyByHash(ctx, strings.Repeat("a", 64))
	if err != nil || k == nil || k.Rate != (ledger.Rate{}) || k.AccountRate != (ledger.Rate{}) {
		t.Fatalf("legacy key after migrating = %+v, %v", k, err)
	}
	wantAllVersions(t)
}
