package gateway

import (
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
)

// 额度行被别的事务一直锁着：预留在账本操作的 2 秒上限处放弃，返回 503 ledger_unavailable（映射不变）；
// 数据库那边也不留下这个请求的等锁线程，它在会话级的 2 秒上限处被放弃（没有会话设置时要等 50 秒）。
// 锁一直不放，所以线程消失只能是数据库自己放弃了。放锁后同一把 Key 照常可用，账本校验通过。
func TestLockedBudgetRowGives503AndLeavesNoWaiter(t *testing.T) {
	e := newDBEnv(t, 1_000_000, nil)
	app := dbtest.App(t)
	var accountID int64
	if err := e.admin.QueryRow("SELECT id FROM accounts WHERE name = ?", e.account).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	release := dbtest.LockBudget(t, e.admin, accountID)
	start := dbtest.Now(t, e.admin)
	r := e.send(t, "/v1/chat/completions", `{"model":"shared","max_tokens":16}`)
	failed := dbtest.Now(t, e.admin)
	if r.status != 503 || errorCode(t, r) != "ledger_unavailable" {
		t.Fatalf("reservation on a locked budget row: %d %s, want 503 ledger_unavailable", r.status, r.body)
	}
	if d := failed.Sub(start); d < 2*time.Second || d > 3*time.Second {
		t.Fatalf("the request took %v, want 2s to 3s", d)
	}
	deadline := failed.Add(3 * time.Second)
	for dbtest.BudgetWaiters(t, app) > 0 {
		if dbtest.Now(t, e.admin).After(deadline) {
			t.Fatal("the database is still waiting for the budget row lock 3s after the request failed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	release()
	if r := e.send(t, "/v1/chat/completions", `{"model":"shared","max_tokens":16}`); r.status != 200 {
		t.Fatalf("after the lock was released: %d %s", r.status, r.body)
	}
	dbtest.Verify(t, e.store)
}
