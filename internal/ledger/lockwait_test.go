package ledger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
)

// 等行锁的上限是会话级的 innodb_lock_wait_timeout（2 秒），不是 context：context 给 10 秒，
// 额度行被别的事务锁住时，预留在数据库时间 2 秒多一点返回 1205，整个事务回滚；放锁后同一个连接池照常可用。
// 去掉会话设置时要等到 context 的 10 秒。
func TestLockWaitIsCappedBySession(t *testing.T) {
	f := newFixture(t, 1000)
	release := dbtest.LockBudget(t, f.admin, f.key.AccountID)
	r := f.reservation(10)
	start := dbtest.Now(t, f.admin)
	ctx10, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := f.s.Reserve(ctx10, r)
	waited := dbtest.Now(t, f.admin).Sub(start)
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1205 {
		t.Fatalf("Reserve on a locked budget row = %v, want MySQL error 1205", err)
	}
	if waited < 2*time.Second || waited > 3500*time.Millisecond {
		t.Fatalf("waited %v for the lock, want 2s to 3.5s", waited)
	}
	release()
	if _, reserved, _ := f.budget(t); reserved != 0 {
		t.Fatalf("reserved = %d after the failed reservation, want 0", reserved)
	}
	if n := f.count(t, "SELECT COUNT(*) FROM calls WHERE id = ?", r.CallID); n != 0 {
		t.Fatalf("the failed reservation left %d calls rows", n)
	}
	f.reserve(t, 10)
	dbtest.Verify(t, f.s)
}
