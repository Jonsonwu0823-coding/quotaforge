// Package ledger 是 MySQL 里的额度账本：账户、额度、Key、调用记录和追加式账本。
//
// 额度的正确性只靠数据库保证：预留是一条带条件的 UPDATE（可用量够才扣），账本 (call_id, kind) 唯一，
// 调用的每次状态转换都在锁住之后按转换表判断（见 recovery.go）。不用进程内的锁。
//
// 锁的顺序：每个改动额度的事务都先锁这个账户的额度行（budgets），再按主键动 calls 和 ledger。
// 顺序统一，并发的预留、收尾、回收和对账就不会互相等成死锁；同一账户的事务在额度行上排队。
// 发送意图（MarkSent）和改道（Reroute）是不锁额度行的两个写：它们只锁调用这一行，持锁时不等别的锁，构不成等待环。
package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// 连接池：一个实例最多 32 个连接；空闲连接也留 32 个，否则并发上来时会反复建连（默认只留 2 个）。
const (
	maxConns        = 32
	connMaxLifetime = 5 * time.Minute
)

// ErrQuotaExhausted 表示账户的可用额度不够这次预留。
var ErrQuotaExhausted = errors.New("quota exhausted")

// errInvalidDSN 不带驱动的原文：原文里可能有连接串的片段。
var errInvalidDSN = errors.New("database DSN is invalid (want user:password@tcp(host:port)/dbname)")

type Status string

// 调用的状态。RESERVED：预留成功，还没落发送意图；IN_FLIGHT：发送意图已落库，请求可能已经发给上游；
// SETTLED：按可信的用量结算；CANCELLED：确定上游没有消费，预留已释放；USAGE_UNKNOWN：可能已被上游计费，用量不知道，占着预留。
const (
	Reserved     Status = "RESERVED"
	InFlight     Status = "IN_FLIGHT"
	Settled      Status = "SETTLED"
	Cancelled    Status = "CANCELLED"
	UsageUnknown Status = "USAGE_UNKNOWN"
)

// Key 是一把可用的 Key，带着它自己和它所属账户的限流限额。
type Key struct {
	ID          int64
	AccountID   int64
	Account     string
	Models      []string
	Rate        Rate
	AccountRate Rate
}

// Rate 是一层的限流限额：每分钟的请求数和 token 数，0 表示不限。
type Rate struct{ RPM, TPM int64 }

// Allows 报告这把 Key 能否使用某个别名；一个别名允许了，chat 和 embeddings 两种用途都允许。
func (k *Key) Allows(alias string) bool { return slices.Contains(k.Models, alias) }

// Reservation 是一次调用的预留：调用号、归属、调用的模型和估算。字符串为空的存 NULL。
// IdempotencyKey 为空表示请求没带幂等键；带键时 Fingerprint 是请求指纹（SHA-256）。
type Reservation struct {
	CallID          string
	AccountID       int64
	KeyID           int64
	Purpose         string
	Alias           string
	Upstream        string
	UpstreamModel   string
	EstInput        int64
	EstOutput       int64
	ClientRequestID string
	RunID           string
	IdempotencyKey  string
	Fingerprint     []byte
}

// Amount 是预留量：估算的输入加输出。
func (r Reservation) Amount() int64 { return r.EstInput + r.EstOutput }

// Usage 是上游给的用量，三个数原样用上游的。嵌入没有 completion_tokens。
type Usage struct {
	Prompt, Completion, Total int64
	HasCompletion             bool
}

// Valid 报告用量对这个接口是否可信：三个数都不为负；chat 必须有 completion_tokens，嵌入可以没有。
// 上游的响应和对账的证据都按这一条判断，不合格的当作未知，不补 0。
func (u Usage) Valid(purpose string) bool {
	return u.Prompt >= 0 && u.Total >= 0 && u.Completion >= 0 && (u.HasCompletion || purpose != "chat")
}

// Result 是网关对一次调用的收尾。Status 是 Settled 时 Usage 必须有，结算按 Usage.Total。字符串为空、HTTPStatus 为 0 的存 NULL。
// Evidence 是结清时凭的证据（Answered 或 NotSent）；调用已被回收标成未知时，Finish 改记 LateResponse。
// Response 是 QF 写回给客户端的上游 2xx，没写回时为 nil；带幂等键的调用把它存成快照。
type Result struct {
	Status             Status
	Evidence           Evidence
	Usage              *Usage
	HTTPStatus         int
	ErrorCode          string
	UpstreamXRequestID string
	UpstreamResponseID string
	ResponseModel      string
	Response           *Response
}

type Store struct{ db *sql.DB }

// Open 按 dsn（go-sql-driver 的格式）打开连接池，不立即连接。错误信息不带 dsn。
func Open(dsn string) (*Store, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errInvalidDSN
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	return &Store{db: db}, nil
}

func parseDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errInvalidDSN
	}
	// RowsAffected 报匹配到的行数而不是改了的行数：条件更新匹配到了就算成功，不管值是否恰好没变。
	cfg.ClientFoundRows = true
	// 等行锁的上限和账本操作一样（驱动在每个新连接上 SET）。没有它，context 到期时驱动关掉连接，
	// 数据库那边的语句却还在等锁，要等到默认的 50 秒才放弃；并发一高，等锁的线程越堆越多。
	// 这里是所有连接的入口：QF、qfctl 和迁移都带上（迁移的 DDL 等的是元数据锁，不归它管）。
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["innodb_lock_wait_timeout"] = strconv.Itoa(int(config.LedgerTimeout / time.Second))
	return cfg, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Stats 是连接池的统计，给后台循环判断连接池是否打满（db_pool 日志）。
func (s *Store) Stats() sql.DBStats { return s.db.Stats() }

// CheckSchema 确认数据库已迁移到 SchemaVersion。QF 启动时调用，不一致就拒绝启动。
func (s *Store) CheckSchema(ctx context.Context) error {
	var v int
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&v)
	var me *mysql.MySQLError
	switch {
	case errors.As(err, &me) && me.Number == 1146: // ER_NO_SUCH_TABLE
		return errors.New("database is not migrated; run qfctl migrate")
	case err != nil:
		return fmt.Errorf("read schema version: %w", err)
	case v != SchemaVersion:
		return fmt.Errorf("database schema version is %d, this build needs %d; run qfctl migrate with the matching build", v, SchemaVersion)
	}
	return nil
}

// KeyByHash 按 Key 的 SHA-256 查可用的 Key，一并读出 Key 和账户的限流限额。不存在或已吊销返回 nil, nil。
func (s *Store) KeyByHash(ctx context.Context, hash string) (*Key, error) {
	var k Key
	var models []byte
	err := s.db.QueryRowContext(ctx, `SELECT k.id, k.account_id, a.name, k.models, k.rpm_limit, k.tpm_limit, a.rpm_limit, a.tpm_limit
		FROM api_keys k JOIN accounts a ON a.id = k.account_id
		WHERE k.sha256 = ? AND k.status = 'active'`, hash).Scan(&k.ID, &k.AccountID, &k.Account, &models,
		&k.Rate.RPM, &k.Rate.TPM, &k.AccountRate.RPM, &k.AccountRate.TPM)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("look up key: %w", err)
	}
	if err := json.Unmarshal(models, &k.Models); err != nil {
		return nil, fmt.Errorf("key %d: models column: %w", k.ID, err)
	}
	return &k, nil
}

// Reserve 预留一次调用的额度。可用量不够时什么都不写，返回 ErrQuotaExhausted。
// 带幂等键时先按这个键已有的记录判断（见 reserveWithKey）：可能返回要重放的快照，或者幂等的几种拒绝，
// 这两种情况下什么都不写。返回 nil, nil 表示已经预留，照常调用上游。
func (s *Store) Reserve(ctx context.Context, r Reservation) (*Replay, error) {
	if r.IdempotencyKey == "" {
		return nil, s.inTx(ctx, nil, func(tx *sql.Tx) error { return reserveIn(ctx, tx, r) })
	}
	var replay *Replay
	err := s.inTx(ctx, readCommitted, func(tx *sql.Tx) error {
		var err error
		replay, err = reserveWithKey(ctx, tx, r)
		return err
	})
	return replay, err
}

// reserveIn 在 tx 里扣预留：先对额度行做条件更新（可用量够才扣），再插调用记录和账本的 reserve。
func reserveIn(ctx context.Context, tx *sql.Tx, r Reservation) error {
	res, err := tx.ExecContext(ctx, `UPDATE budgets
		SET reserved_tokens = reserved_tokens + ?, updated_at = UTC_TIMESTAMP(6)
		WHERE account_id = ? AND limit_tokens - reserved_tokens - consumed_tokens >= ?`,
		r.Amount(), r.AccountID, r.Amount())
	if err != nil {
		return fmt.Errorf("reserve: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("reserve: %w", err)
	}
	if n == 0 {
		return ErrQuotaExhausted
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO calls (id, account_id, key_id, status, purpose, model_alias,
		upstream, upstream_model, est_input_tokens, est_output_tokens, reserved_tokens,
		client_request_id, run_id, idempotency_key, created_at)
		VALUES (?, ?, ?, 'RESERVED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))`,
		r.CallID, r.AccountID, r.KeyID, r.Purpose, r.Alias, r.Upstream, r.UpstreamModel,
		r.EstInput, r.EstOutput, r.Amount(), nullString(r.ClientRequestID), nullString(r.RunID),
		nullString(r.IdempotencyKey)); err != nil {
		return fmt.Errorf("insert call: %w", err)
	}
	return insertEntry(ctx, tx, r.CallID, "reserve", r.Amount(), 0)
}

// ErrNotReserved 是发送意图没落成：调用已经不是 RESERVED（超过上界，被回收释放了）。这时不能发请求。
var ErrNotReserved = errors.New("call is no longer RESERVED: it was reclaimed before the request was sent")

// MarkSent 落发送意图：调用还是 RESERVED 时改成 IN_FLIGHT，记下时间。只有它成功提交之后，网关才向上游发第一个字节。
// 一条自动提交的条件更新，只锁这一行；回收先改掉了的话影响 0 行，返回 ErrNotReserved。
func (s *Store) MarkSent(ctx context.Context, r Reservation) error {
	res, err := s.db.ExecContext(ctx, `UPDATE calls SET status = 'IN_FLIGHT', sent_at = UTC_TIMESTAMP(6)
		WHERE id = ? AND status = 'RESERVED'`, r.CallID)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	if n == 0 {
		return ErrNotReserved
	}
	return nil
}

// ErrNotInFlight 是改道没改成：调用已经不是 IN_FLIGHT（超过上界，被回收标成了未知），或者上游已经不是 r 里的那个。
// 这时不能把请求发给新的上游。
var ErrNotInFlight = errors.New("call is no longer IN_FLIGHT on the expected upstream: it was not rerouted")

// Reroute 是网关换上游时的改道：调用还是 IN_FLIGHT、上游和上游模型名还是 r 里的时，改成新的。状态不变，额度和账本行不动。
// 只在前一个上游确定没收到请求（拨号失败）之后、发给新上游之前调用；提交之后才发。和 MarkSent 一样是一条自动提交的
// 条件更新，只锁这一行；回收先把调用标成未知的话影响 0 行，返回 ErrNotInFlight。
// 改道之后、发出之前进程死了：回收把它标成未知，上游是新的那个（偏保守：新上游其实没收到）。
func (s *Store) Reroute(ctx context.Context, r Reservation, upstream, upstreamModel string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE calls SET upstream = ?, upstream_model = ?
		WHERE id = ? AND status = 'IN_FLIGHT' AND upstream = ? AND upstream_model = ?`,
		upstream, upstreamModel, r.CallID, r.Upstream, r.UpstreamModel)
	if err != nil {
		return fmt.Errorf("reroute: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("reroute: %w", err)
	}
	if n == 0 {
		return ErrNotInFlight
	}
	return nil
}

// Finish 是网关的收尾，一个事务：按转换表从当前状态转到 res.Status，写这次的全部结果，结算、释放或者额度不动（未知）。
// 调用已被回收标成未知时（迟到的结果），结算和释放照常，证据记 LateResponse。转换表不允许时返回 ErrNotAllowed，什么都不改。
// 带幂等键、写回了 2xx 的调用，在同一个事务里存快照：别的请求看到的，要么还在处理，要么收尾和快照都在。
func (s *Store) Finish(ctx context.Context, r Reservation, res Result) error {
	return s.inTx(ctx, nil, func(tx *sql.Tx) error {
		c, err := lockCall(ctx, tx, r.AccountID, r.CallID)
		if err != nil {
			return err
		}
		if err := checkTransition(Gateway, c, res.Status); err != nil {
			return err
		}
		evidence := res.Evidence
		if c.status == UsageUnknown {
			evidence = LateResponse
		}
		var prompt, completion, total sql.NullInt64
		if u := res.Usage; u != nil {
			prompt = sql.NullInt64{Int64: u.Prompt, Valid: true}
			completion = sql.NullInt64{Int64: u.Completion, Valid: u.HasCompletion}
			total = sql.NullInt64{Int64: u.Total, Valid: true}
		}
		httpStatus := sql.NullInt64{Int64: int64(res.HTTPStatus), Valid: res.HTTPStatus != 0}
		if err := setStatus(ctx, tx, r.CallID, c.status, res.Status, Gateway, evidence, `usage_prompt_tokens = ?, usage_completion_tokens = ?,
			usage_total_tokens = ?, http_status = ?, error_code = ?, upstream_x_request_id = ?, upstream_response_id = ?,
			response_model = ?, finished_at = UTC_TIMESTAMP(6)`, prompt, completion, total, httpStatus, nullString(res.ErrorCode),
			nullString(res.UpstreamXRequestID), nullString(res.UpstreamResponseID), nullString(res.ResponseModel)); err != nil {
			return err
		}
		if err := closeBudget(ctx, tx, r.AccountID, r.CallID, res.Status, c.reserved, res.Usage); err != nil {
			return err
		}
		if r.IdempotencyKey == "" || res.Response == nil {
			return nil
		}
		return saveResponse(ctx, tx, r, *res.Response)
	})
}

// lockBudget 锁住账户的额度行。所有改额度或幂等记录的事务都先做这一步。
func lockBudget(ctx context.Context, tx *sql.Tx, accountID int64) error {
	if _, err := tx.ExecContext(ctx, "SELECT 1 FROM budgets WHERE account_id = ? FOR UPDATE", accountID); err != nil {
		return fmt.Errorf("lock budget: %w", err)
	}
	return nil
}

// moveBudget 按变动量改额度行，并在账本里记同样的变动。
func moveBudget(ctx context.Context, tx *sql.Tx, accountID int64, callID, kind string, reservedDelta, consumedDelta int64) error {
	if _, err := tx.ExecContext(ctx, `UPDATE budgets SET reserved_tokens = reserved_tokens + ?,
		consumed_tokens = consumed_tokens + ?, updated_at = UTC_TIMESTAMP(6) WHERE account_id = ?`,
		reservedDelta, consumedDelta, accountID); err != nil {
		return fmt.Errorf("%s: %w", kind, err)
	}
	return insertEntry(ctx, tx, callID, kind, reservedDelta, consumedDelta)
}

func insertEntry(ctx context.Context, tx *sql.Tx, callID, kind string, reservedDelta, consumedDelta int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at)
		VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6))`, callID, kind, reservedDelta, consumedDelta); err != nil {
		return fmt.Errorf("ledger %s: %w", kind, err)
	}
	return nil
}

// readCommitted 用于带幂等键的预留和清理：不对不存在的行加间隙锁，每条语句读最新提交的数据。
var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// inTx 在一个事务里执行 fn：fn 返回 nil 才提交，否则回滚。opts 为 nil 时用默认的可重复读。
func (s *Store) inTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 已提交时是空操作
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }
