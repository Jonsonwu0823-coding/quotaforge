package ledger_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// dailyRows 是这个账户的全部汇总，一行一个（日期、用途）：日期 用途 结算数 prompt/completion/total 缓存数/省下的 取消数。
func (f *fixture) dailyRows(t *testing.T) []string {
	t.Helper()
	rows, err := f.admin.Query(`SELECT CONCAT_WS(' ', DATE_FORMAT(day, '%Y-%m-%d'), purpose, settled_calls,
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
	return out
}

func at(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// 消费者写库：结算、缓存命中、取消各自算对；同一个事件第二次是重复，汇总不变。按结清时间的 UTC 日期分天：
// 23:59:59.999999 归当天，00:00:00 归第二天；进程的本地时区（这里设成 UTC+8）不影响。
func TestApplyUsageEvent(t *testing.T) {
	defer func(l *time.Location) { time.Local = l }(time.Local)
	time.Local = time.FixedZone("UTC+8", 8*3600)
	f := newFixture(t, 1000)
	t.Cleanup(func() { deleteAccount(t, f) })
	late, midnight := at(t, "2026-10-16T23:59:59.999999Z"), at(t, "2026-10-17T00:00:00Z")
	base := ledger.UsageEvent{AccountID: f.key.AccountID, Alias: "m", Upstream: "u", UpstreamModel: "um"}
	event := func(purpose string, status ledger.Status, u *ledger.Usage, resolved time.Time) ledger.UsageEvent {
		e := base
		e.CallID, e.Purpose, e.Status, e.Usage, e.ResolvedAt = "qf-"+rand.Text(), purpose, status, u, resolved
		return e
	}
	events := []ledger.UsageEvent{
		event("chat", ledger.Settled, chatUsage(10, 3), late),
		event("embeddings", ledger.Settled, &ledger.Usage{Prompt: 7, Total: 7}, late),
		event("embeddings", ledger.Cached, &ledger.Usage{Prompt: 7, Total: 7}, late),
		event("chat", ledger.Cancelled, nil, midnight),
	}
	for _, e := range events {
		if applied, err := f.s.ApplyUsageEvent(ctx, e); err != nil || !applied {
			t.Fatalf("apply %s %s: %v, %v", e.Purpose, e.Status, applied, err)
		}
	}
	want := []string{"2026-10-16 chat 1 10/3/13 0/0 0", "2026-10-16 embeddings 1 7/0/7 1/7 0", "2026-10-17 chat 0 0/0/0 0/0 1"}
	if got := f.dailyRows(t); !slices.Equal(got, want) {
		t.Fatalf("daily totals:\n%v\nwant\n%v", got, want)
	}
	for _, e := range events {
		if applied, err := f.s.ApplyUsageEvent(ctx, e); err != nil || applied {
			t.Fatalf("second delivery of %s: applied %v, %v", e.CallID, applied, err)
		}
	}
	if got := f.dailyRows(t); !slices.Equal(got, want) {
		t.Fatalf("daily totals changed by duplicates:\n%v\nwant\n%v", got, want)
	}
}

// consumeAll 把 outbox 里没发出的行发给假的发送方，再把这个账户的事件交给消费者写库。
func (f *fixture) consumeAll(t *testing.T) {
	t.Helper()
	c := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, c.send); err != nil {
		t.Fatal(err)
	}
	f.apply(t, c)
}

func (f *fixture) apply(t *testing.T, c *collector) {
	t.Helper()
	for _, e := range c.got {
		if e.AccountID != f.key.AccountID {
			continue
		}
		if _, err := f.s.ApplyUsageEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
}

// report 是这个账户的对账：每行“日期 用途 结论”，加上对不上的调用“种类 调用号”。
func (f *fixture) report(t *testing.T, day string) (rows, gaps []string) {
	t.Helper()
	r, err := f.s.ReconcileUsage(ctx, day, f.account)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range r.Days {
		rows = append(rows, d.Day+" "+d.Purpose+" "+d.Result())
	}
	for _, g := range r.Gaps {
		gaps = append(gaps, g.Kind+" "+g.CallID)
	}
	return rows, gaps
}

func (f *fixture) today(t *testing.T) string { return dbtest.Now(t, f.admin).Format(time.DateOnly) }

// 对账：两边完全相同；还没发的、发了没消费的算在路上（结论 in-transit）；消费完又相同。参与的有收尾的结算和取消、
// 缓存命中，和对账结清的调用（它没有 finished_at：按 resolved_at 分天）。
func TestReconcileUsage(t *testing.T) {
	f := newFixture(t, 1<<40)
	settled, cancelled, unknown := f.reserve(t, 100), f.reserve(t, 100), f.reserve(t, 100)
	f.finish(t, settled, ledger.Settled, 42)
	f.finish(t, cancelled, ledger.Cancelled, 0)
	f.finish(t, unknown, ledger.UsageUnknown, 0)
	f.recordCached(t, f.settledEmbedding(t).CallID)
	for _, it := range f.s.Reconcile(ctx, []ledger.Proof{{CallID: unknown.CallID, Status: ledger.Settled, Usage: chatUsage(30, 7),
		Line: 2, Source: ledger.Bill}}, true) {
		if !it.Applied {
			t.Fatalf("reconcile: %v %s", it.Verdict, it.Reason)
		}
	}
	f.consumeAll(t)
	today := f.today(t)
	same := []string{today + " chat same", today + " embeddings same"}
	if rows, gaps := f.report(t, ""); !slices.Equal(rows, same) || len(gaps) != 0 {
		t.Fatalf("after consuming everything: %v %v", rows, gaps)
	}

	pending := f.reserve(t, 100)
	f.finish(t, pending, ledger.Settled, 5)
	if rows, gaps := f.report(t, today); !slices.Contains(rows, today+" chat in-transit") ||
		!slices.Equal(gaps, []string{"unsent " + pending.CallID}) {
		t.Fatalf("unsent: %v %v", rows, gaps)
	}
	c := &collector{}
	if _, err := f.s.ForwardOutbox(ctx, c.send); err != nil {
		t.Fatal(err)
	}
	if rows, gaps := f.report(t, today); !slices.Contains(rows, today+" chat in-transit") ||
		!slices.Equal(gaps, []string{"unconsumed " + pending.CallID}) {
		t.Fatalf("sent, not consumed: %v %v", rows, gaps)
	}
	f.apply(t, c)
	if rows, gaps := f.report(t, today); !slices.Equal(rows, same) || len(gaps) != 0 {
		t.Fatalf("after consuming the rest: %v %v", rows, gaps)
	}
}

// 对不上的几种：丢了（结清了、没有 outbox 行、没消费）、多了（消费者记了一个账本里没有的调用）、汇总被改、
// 记错了日期。结论都是 MISMATCH。
func TestReconcileUsageMismatches(t *testing.T) {
	cases := []struct {
		name     string
		break_   func(t *testing.T, f *fixture, r ledger.Reservation) // 造出不一致
		wantGaps func(r ledger.Reservation) []string
	}{
		{"lost", func(t *testing.T, f *fixture, r ledger.Reservation) {
			exec(t, f.admin, "DELETE FROM outbox WHERE call_id = '"+r.CallID+"'")
		}, func(r ledger.Reservation) []string { return []string{"LOST " + r.CallID} }},
		{"extra", func(t *testing.T, f *fixture, r ledger.Reservation) {
			f.consumeAll(t)
			exec(t, f.admin, fmt.Sprintf(`INSERT INTO usage_seen (call_id, account_id, day, purpose, consumed_at)
				VALUES ('qf-NOSUCHCALL', %d, UTC_DATE(), 'chat', UTC_TIMESTAMP(6))`, f.key.AccountID))
		}, func(ledger.Reservation) []string { return []string{"extra qf-NOSUCHCALL"} }},
		{"totals changed", func(t *testing.T, f *fixture, r ledger.Reservation) {
			f.consumeAll(t)
			exec(t, f.admin, fmt.Sprintf("UPDATE usage_daily SET total_tokens = total_tokens + 1 WHERE account_id = %d", f.key.AccountID))
		}, func(ledger.Reservation) []string { return nil }},
		{"misfiled", func(t *testing.T, f *fixture, r ledger.Reservation) {
			c := &collector{}
			if _, err := f.s.ForwardOutbox(ctx, c.send); err != nil {
				t.Fatal(err)
			}
			for i := range c.got {
				c.got[i].ResolvedAt = c.got[i].ResolvedAt.AddDate(0, 0, 1) // 记到第二天（在对账的范围之外）
			}
			f.apply(t, c)
		}, func(r ledger.Reservation) []string { return []string{"MISFILED " + r.CallID} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1000)
			t.Cleanup(func() { deleteAccount(t, f) })
			r := f.reserve(t, 100)
			f.finish(t, r, ledger.Settled, 42)
			tc.break_(t, f, r)
			rows, gaps := f.report(t, "")
			if !slices.Equal(rows, []string{f.today(t) + " chat MISMATCH"}) || !slices.Equal(gaps, tc.wantGaps(r)) {
				t.Fatalf("report: %v %v", rows, gaps)
			}
		})
	}
}

// 半夜：结清在前一天的 23:59:59.999999（UTC），收尾的时间在第二天；事件和对账都按 resolved_at 归到前一天。
func TestReconcileUsageAtMidnight(t *testing.T) {
	f := newFixture(t, 1000)
	r := f.reserve(t, 100)
	f.finish(t, r, ledger.Settled, 42)
	yesterday := dbtest.Now(t, f.admin).AddDate(0, 0, -1).Format(time.DateOnly)
	exec(t, f.admin,
		"UPDATE calls SET resolved_at = '"+yesterday+" 23:59:59.999999', finished_at = UTC_TIMESTAMP(6) WHERE id = '"+r.CallID+"'",
		"UPDATE outbox SET resolved_at = '"+yesterday+" 23:59:59.999999' WHERE call_id = '"+r.CallID+"'")
	f.consumeAll(t)
	if got := f.dailyRows(t); len(got) != 1 || !strings.HasPrefix(got[0], yesterday+" chat 1 ") {
		t.Fatalf("daily totals %v, want one row on %s", got, yesterday)
	}
	if rows, gaps := f.report(t, yesterday); !slices.Equal(rows, []string{yesterday + " chat same"}) || len(gaps) != 0 {
		t.Fatalf("report for %s: %v %v", yesterday, rows, gaps)
	}
	if rows, gaps := f.report(t, f.today(t)); len(rows)+len(gaps) != 0 {
		t.Fatalf("report for today: %v %v", rows, gaps)
	}
}

// 打开之前结清、没有事件的调用不在对账的范围里：关着时结清一个，打开，再结清一个，消费完，
// 这一天的结论是 same，不是 LOST 或 MISMATCH。
func TestReconcileUsageSkipsCallsBeforeEnabling(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	t.Cleanup(func() { deleteAccount(t, f) }) // 先于恢复开关执行
	before := f.reserve(t, 100)
	f.finish(t, before, ledger.Settled, 7)
	if err := f.s.EnableUsageEvents(ctx); err != nil {
		t.Fatal(err)
	}
	after := f.reserve(t, 100)
	f.finish(t, after, ledger.Settled, 5)
	if f.events(t, before.CallID) != 0 || f.events(t, after.CallID) != 1 {
		t.Fatalf("events: before %d (want 0), after %d (want 1)", f.events(t, before.CallID), f.events(t, after.CallID))
	}
	f.consumeAll(t)
	today := f.today(t)
	for _, day := range []string{"", today} {
		if rows, gaps := f.report(t, day); !slices.Equal(rows, []string{today + " chat same"}) || len(gaps) != 0 {
			t.Fatalf("report for %q: %v %v", day, rows, gaps)
		}
	}
	if got := f.dailyRows(t); len(got) != 1 || !strings.HasPrefix(got[0], today+" chat 1 ") {
		t.Fatalf("daily totals %v, want only the call settled after enabling", got)
	}
}

// 开关没打开过时，对账报 ErrUsageEventsOff。
func TestReconcileUsageOff(t *testing.T) {
	f := newFixture(t, 1000)
	switchOff(t, f)
	if _, err := f.s.ReconcileUsage(ctx, "", ""); !errors.Is(err, ledger.ErrUsageEventsOff) {
		t.Fatalf("ReconcileUsage with events off = %v", err)
	}
}
