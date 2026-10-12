package ledger

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version int
	sql     string
}

// migrations 是内嵌的迁移文件，按版本号排好。文件名以四位版本号开头，版本号从 1 起连续。
var migrations = mustLoadMigrations()

// SchemaVersion 是这个程序要求的数据库版本，即最大的迁移文件号。
var SchemaVersion = len(migrations)

func mustLoadMigrations() []migration {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		panic(err)
	}
	var out []migration
	for i, name := range names { // Glob 按文件名排序
		base := strings.TrimPrefix(name, "migrations/")
		v, err := strconv.Atoi(base[:min(4, len(base))])
		if err != nil || v != i+1 {
			panic(fmt.Sprintf("migration %s: file names must start with consecutive 4-digit versions from 0001", base))
		}
		data, err := migrationFiles.ReadFile(name)
		if err != nil {
			panic(err)
		}
		out = append(out, migration{version: v, sql: string(data)})
	}
	return out
}

const migrateLock = "quotaforge_migrate"

// Migrate 把 dsn 指向的库迁到 SchemaVersion，返回迁移前后的版本。已是最新时什么都不做。
//
// 两个进程同时迁移时靠 MySQL 的命名锁串行：后来的等前一个做完，读到的已是最新版本。
// 命名锁属于会话（连接），所以加锁、迁移、释放必须在同一个 *sql.Conn 上；走连接池的话，
// RELEASE_LOCK 可能落到另一个连接上而释放不了，迁移语句也可能在没拿到锁的连接上执行。
func Migrate(ctx context.Context, dsn string) (from, to int, err error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return 0, 0, err
	}
	cfg.MultiStatements = true // 只用于执行内嵌的迁移文件
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return 0, 0, errInvalidDSN
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", migrateLock).Scan(&got); err != nil {
		return 0, 0, fmt.Errorf("get migration lock: %w", err)
	}
	if got.Int64 != 1 {
		return 0, 0, errors.New("get migration lock: another migration is still running after 30s")
	}
	defer func() {
		// 释放失败也无妨：db.Close 断开这个会话时锁随之释放。
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DO RELEASE_LOCK(?)", migrateLock)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INT NOT NULL PRIMARY KEY, applied_at DATETIME(6) NOT NULL) ENGINE=InnoDB`); err != nil {
		return 0, 0, fmt.Errorf("create schema_migrations: %w", err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&from); err != nil {
		return 0, 0, fmt.Errorf("read schema version: %w", err)
	}
	if from > SchemaVersion {
		return from, from, fmt.Errorf("database schema version %d is newer than this build (%d)", from, SchemaVersion)
	}
	for _, m := range migrations[from:] {
		if _, err := conn.ExecContext(ctx, m.sql); err != nil {
			return from, m.version - 1, fmt.Errorf("migration %d: %w", m.version, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", m.version); err != nil {
			return from, m.version - 1, fmt.Errorf("record migration %d: %w", m.version, err)
		}
	}
	return from, SchemaVersion, nil
}
