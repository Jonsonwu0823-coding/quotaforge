// Package dbtest 给要 MySQL、Redis 的测试用：连接串、跳过或强制的规则、包之间的互斥、每个测试独立的账户和 key 前缀。
//
// 连接串从 QUOTAFORGE_TEST_MYSQL_MIGRATE_DSN（能建表）和 QUOTAFORGE_TEST_MYSQL_DSN（只能增删改查）读，
// Redis 的地址从 QUOTAFORGE_TEST_REDIS_ADDR 读，Kafka 的地址从 QUOTAFORGE_TEST_KAFKA_ADDR 读。没设时跳过；
// QUOTAFORGE_TEST_REQUIRE_DB=1（CI）时缺了就判失败。
//
// 测试库里用量事件的开关一直开着（Main 迁移之后就打开）：所有包跑在同一种状态上，每个以 Verify 收尾的测试都会查
// outbox 的完整性。测试里直接写库造出来的终态调用，要么补 outbox 行，要么在测试结束前删掉。
//
// 几个测试包同时用同一个 Redis：每个测试只用自己的 key 前缀（RedisPrefix），不清空整个库。
//
// go test ./... 会让多个包同时跑，而它们共用一个测试库，迁移测试还要删表重建。所以每个用库的测试包
// 在 TestMain 里调用 Main：拿到 MySQL 的命名锁才开始跑，各包依次使用这个库。
package dbtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

const (
	migrateEnv = "QUOTAFORGE_TEST_MYSQL_MIGRATE_DSN"
	appEnv     = "QUOTAFORGE_TEST_MYSQL_DSN"
	redisEnv   = "QUOTAFORGE_TEST_REDIS_ADDR"
	kafkaEnv   = "QUOTAFORGE_TEST_KAFKA_ADDR"
	requireEnv = "QUOTAFORGE_TEST_REQUIRE_DB"
	lockName   = "quotaforge_test"
)

// Main 用在测试包的 TestMain 里。有连接串时先拿库的锁、迁移到最新，再跑测试。
func Main(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	migrate, app := os.Getenv(migrateEnv), os.Getenv(appEnv)
	if migrate == "" || app == "" {
		if os.Getenv(requireEnv) == "1" {
			fmt.Fprintf(os.Stderr, "%s=1 but %s or %s is not set\n", requireEnv, migrateEnv, appEnv)
			return 1
		}
		return m.Run() // 用库的测试各自跳过
	}
	ctx := context.Background()
	db, err := openRaw(migrate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dbtest:", err)
		return 1
	}
	defer db.Close()
	// 锁属于会话：拿锁和释放都在这一个连接上，整个包跑完才释放。
	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dbtest: connect:", err)
		return 1
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 900)", lockName).Scan(&got); err != nil || got.Int64 != 1 {
		fmt.Fprintln(os.Stderr, "dbtest: could not lock the test database:", err)
		return 1
	}
	defer func() { _, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", lockName) }()
	if err := Migrate(ctx, migrate); err != nil {
		fmt.Fprintln(os.Stderr, "dbtest:", err)
		return 1
	}
	return m.Run()
}

// Migrate 把测试库迁到最新，并打开用量事件的开关（已经开着就不变）。删表重建的测试结束时也用它。
func Migrate(ctx context.Context, dsn string) error {
	if _, _, err := ledger.Migrate(ctx, dsn); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	s, err := ledger.Open(dsn)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.EnableUsageEvents(ctx)
}

func dsn(t testing.TB, env string) string {
	t.Helper()
	v := os.Getenv(env)
	if v == "" {
		if os.Getenv(requireEnv) == "1" {
			t.Fatalf("%s is not set", env)
		}
		t.Skipf("%s is not set; skipping a test that needs it", env)
	}
	return v
}

// AppDSN 是只能增删改查的账号（QF 服务用的那种）的连接串。没设就跳过当前测试。
func AppDSN(t testing.TB) string { return dsn(t, appEnv) }

// MigrateDSN 是能建表的账号的连接串。没设就跳过当前测试。
func MigrateDSN(t testing.TB) string { return dsn(t, migrateEnv) }

// RedisURL 是测试用 Redis 的地址（redis://主机:端口/0）。没设就跳过当前测试。
func RedisURL(t testing.TB) string { return "redis://" + dsn(t, redisEnv) + "/0" }

// KafkaAddr 是测试用 Kafka 的地址（主机:端口）。没设就跳过当前测试。
func KafkaAddr(t testing.TB) string { return dsn(t, kafkaEnv) }

// RedisPrefix 是只属于当前测试的 key 前缀，带随机串。
func RedisPrefix() string { return "qf-test:" + strings.ToLower(rand.Text()[:12]) + ":" }

// Store 用 QF 服务的账号打开账本，测试结束时关闭。
func Store(t testing.TB) *ledger.Store {
	t.Helper()
	s, err := ledger.Open(AppDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Admin 用能建表的账号直接连库，给测试读表、改坏数据用。
func Admin(t testing.TB) *sql.DB {
	t.Helper()
	db, err := openRaw(MigrateDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func openRaw(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse test DSN: invalid")
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("parse test DSN: invalid")
	}
	return sql.OpenDB(connector), nil
}

// Account 建一个只属于当前测试的账户，名字带随机后缀，返回名字。
func Account(t testing.TB, s *ledger.Store, limit int64) string {
	t.Helper()
	name := "t-" + strings.ToLower(rand.Text()[:12])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.CreateAccount(ctx, name, limit, ledger.Rate{}); err != nil {
		t.Fatal(err)
	}
	return name
}

// Key 给账户发一把只允许别名 m、不限流的 Key，返回按哈希查到的它（鉴权时网关拿到的那个）。
func Key(t testing.TB, s *ledger.Store, account string) *ledger.Key {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	issued, err := s.IssueKey(ctx, account, []string{"m"}, ledger.Rate{})
	if err != nil {
		t.Fatal(err)
	}
	k, err := s.KeyByHash(ctx, apikey.Hash(issued.Secret))
	if err != nil || k == nil {
		t.Fatalf("KeyByHash = %v, %v", k, err)
	}
	return k
}

// Budget 读账户的额度行：上限、已预留、已消费。
func Budget(t testing.TB, db *sql.DB, account string) (limit, reserved, consumed int64) {
	t.Helper()
	err := db.QueryRow(`SELECT b.limit_tokens, b.reserved_tokens, b.consumed_tokens
		FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = ?`, account).Scan(&limit, &reserved, &consumed)
	if err != nil {
		t.Fatal(err)
	}
	return limit, reserved, consumed
}

// Verify 校验整个账本，有不成立的条目就让测试失败。
func Verify(t testing.TB, s *ledger.Store) {
	t.Helper()
	v, err := s.Verify(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range v {
		t.Errorf("ledger invariant: %s", x)
	}
}

// App 用 QF 服务的账号直接连库（不经 ledger.Open，所以没有会话级的等锁上限），给测试看本账号的线程用。
func App(t testing.TB) *sql.DB {
	t.Helper()
	db, err := openRaw(AppDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Now 是数据库的当前时间（UTC）。和时间有关的断言用它，不用测试进程的时钟。
func Now(t testing.TB, db *sql.DB) time.Time {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')").Scan(&s); err != nil {
		t.Fatal(err)
	}
	now, err := time.Parse("2006-01-02 15:04:05.000000", s)
	if err != nil {
		t.Fatal(err)
	}
	return now
}

// Hold 在另一个事务里执行 query（例如 SELECT … FOR UPDATE）、拿着它的锁，返回放锁的函数：回滚这个事务，只回滚一次，
// 测试结束时也会放。
func Hold(t testing.TB, db *sql.DB, query string, args ...any) (release func()) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(query, args...); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = tx.Rollback() }) }
	t.Cleanup(release)
	return release
}

// LockBudget 在另一个事务里锁住账户的额度行，返回放锁的函数（测试结束时也会放）。
func LockBudget(t testing.TB, db *sql.DB, accountID int64) (release func()) {
	t.Helper()
	return Hold(t, db, "SELECT 1 FROM budgets WHERE account_id = ? FOR UPDATE", accountID)
}

// BudgetWaiters 是 app（App 的连接）这个账号在本库里正在执行、语句以 UPDATE budgets 开头的线程数。
// 测试账号没有 PROCESS 权限，读不了锁等待表，但 processlist 里看得到自己账号的全部线程。
func BudgetWaiters(t testing.TB, app *sql.DB) int {
	t.Helper()
	var n int
	if err := app.QueryRow(`SELECT COUNT(*) FROM information_schema.processlist
		WHERE user = SUBSTRING_INDEX(CURRENT_USER(), '@', 1) AND db = DATABASE() AND info LIKE 'UPDATE budgets%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
