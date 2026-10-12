package ledger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// reserveForever 在被锁住的额度行上预留，用永不结束的 context：能让它返回的只有数据库的等锁上限（会话级的 2 秒）。
// 返回等了多久（数据库的时间）和错误。数据库时间超过 3.5 秒还没返回就判失败，不干等默认的 50 秒；
// 失败后测试结束时放锁，被卡住的预留随之完成。
func (f *fixture) reserveForever(t *testing.T, r ledger.Reservation) (time.Duration, error) {
	t.Helper()
	start := dbtest.Now(t, f.admin)
	done := make(chan error, 1)
	go func() {
		_, err := f.s.Reserve(context.Background(), r)
		done <- err
	}()
	for {
		select {
		case err := <-done:
			return dbtest.Now(t, f.admin).Sub(start), err
		case <-time.After(50 * time.Millisecond):
			if dbtest.Now(t, f.admin).Sub(start) > 3500*time.Millisecond {
				t.Fatal("the reservation still waits for the locked budget row 3.5 s after it started (database time)")
			}
		}
	}
}

// inTx 出错时回滚事务，连接干干净净回到连接池。
//
// 账本操作的调用方都用会结束的 context，context 结束时 database/sql 自己会回滚事务，所以 inTx 里那一行回滚平时看不出来；
// 只有 context 永不结束时，它才是唯一的保障。这个测试把连接池缩成 1 个连接，用 context.Background() 预留一个被锁住的额度行
// （普通预留和带键预留，后者是读已提交），等锁超时后，下一条语句只能复用同一个连接：
// 回滚没做，这个连接还停在事务里，会话状态查询会超时；会话级的等锁上限没设，预留要等默认的 50 秒。
func TestFailedTransactionLeavesConnectionClean(t *testing.T) {
	f := newFixture(t, 1000)
	ledger.OneConnection(f.s)
	session := func(step string) (int64, string) {
		t.Helper()
		conn, lockWait, autocommit, isolation, err := ledger.SessionState(f.s)
		if err != nil {
			t.Fatalf("%s: the only connection is not usable: %v", step, err)
		}
		if lockWait != 2 || autocommit != 1 {
			t.Fatalf("%s: innodb_lock_wait_timeout %d, autocommit %d; want 2 and 1", step, lockWait, autocommit)
		}
		return conn, isolation
	}
	conn0, isolation0 := session("before")

	release := dbtest.LockBudget(t, f.admin, f.key.AccountID)
	plain, keyed := f.reservation(10), f.keyed(10, "held", "req")
	for _, step := range []struct {
		name string
		r    ledger.Reservation
	}{{"plain reservation", plain}, {"keyed reservation (read committed)", keyed}} {
		waited, err := f.reserveForever(t, step.r)
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1205 || waited < 2*time.Second || waited > 3500*time.Millisecond {
			t.Fatalf("%s: Reserve = %v after %v, want error 1205 after 2 to 3.5 s", step.name, err, waited)
		}
		if conn, isolation := session("after the " + step.name); conn != conn0 || isolation != isolation0 {
			t.Fatalf("after the %s: connection %d, isolation %s; want the same connection %d, back at %s", step.name, conn, isolation, conn0, isolation0)
		}
	}

	release()
	if _, reserved, _ := f.budget(t); reserved != 0 {
		t.Fatalf("reserved = %d after two failed reservations, want 0", reserved)
	}
	for _, r := range []ledger.Reservation{plain, keyed} {
		if n := f.count(t, "SELECT COUNT(*) FROM calls WHERE id = ?", r.CallID); n != 0 {
			t.Fatalf("the failed reservation %s left %d calls rows", r.CallID, n)
		}
	}
	if n := f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", f.key.AccountID); n != 0 {
		t.Fatalf("the failed keyed reservation left %d idempotency records", n)
	}
	f.reserve(t, 10)
	if _, reserved, _ := f.budget(t); reserved != 10 {
		t.Fatalf("reserved = %d after one good reservation on the same pool, want 10", reserved)
	}
	dbtest.Verify(t, f.s)
}
