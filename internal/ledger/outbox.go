package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// 用量事件：调用结清（SETTLED、CANCELLED、CACHED）时，在把它改成终态的同一个事务里往 outbox 写一行（writeEvent）。
// 事务回滚，这一行也没有；提交了，这一行一定在。后台的转发把它发到消息队列（ForwardOutbox），发成功才标记已发出；
// 消费者按调用号去重、按天汇总（ApplyUsageEvent）。投递是至少一次，靠消费者去重，不靠消息队列或生产者的幂等。
//
// 开关在数据库里（usage_events 一行），不在配置里：结清调用的不只是 QF 实例，还有 qfctl reconcile，它们读同一个开关。

// outboxRetentionDays 是已发出的 outbox 行保留的天数，和消息队列里事件的保留期一样。清理按它删，Verify 的完整性
// 也只查这个窗口里结清的调用：窗口里的调用，outbox 行一定还没被清理（sent_at 不早于 resolved_at）。
const outboxRetentionDays = 7

// forwardBatch 是转发一次取的行数，也是发给消息队列的一批。
const forwardBatch = 100

// forwardLock 是转发的命名锁：几个实例同时转发时，同一时刻只有拿到它的那个在发。
const forwardLock = "quotaforge_outbox"

// ErrUsageEventsOff 表示用量事件的开关没打开过。
var ErrUsageEventsOff = errors.New("usage events are not enabled")

// UsageEvent 是一个调用结清的事件，内容是结清时调用行的样子。Usage 为 nil 表示没有用量（例如取消的调用）。
// ResolvedAt 是 UTC 的结清时间（calls.resolved_at），按它的日期分天。
type UsageEvent struct {
	CallID        string
	AccountID     int64
	Purpose       string
	Alias         string
	Upstream      string
	UpstreamModel string
	Status        Status
	Usage         *Usage
	ResolvedAt    time.Time
	CachedFrom    string
}

// Day 是事件归属的日期：结清时间的 UTC 日期，和账本那边的 DATE(resolved_at) 是同一列、同一个值。
func (e UsageEvent) Day() string { return e.ResolvedAt.UTC().Format(time.DateOnly) }

// writeEvent 在结清事务里写这个调用的 outbox 行，内容从刚改好的调用行抄。结清的写法都调它：setStatus（收尾、回收、
// 对账）和 RecordCached（缓存命中），QF 和 qfctl 一样。开关关着时 JOIN 不出行，什么都不写。
//
// 完整性靠显式的 FOR SHARE，不靠隔离级别（结清事务用服务器默认的隔离级别）：这条语句读开关行时加共享锁，读的是
// 最新提交的值；打开开关（EnableUsageEvents）要先拿这一行的排他锁、再取时间。
//   - 正在打开时（排他锁已拿、还没提交）结清：这条语句等锁，打开提交之后读到“开”，写事件。
//   - 开关关着时结清：读到“关”时的共享锁也拿到提交（可重复读、读已提交的会话都是这样，MySQL 8.4 上看过），打开
//     要等这个结清提交之后才取时间，所以 resolved_at < enabled_at，不要求有事件。假设这把锁读完就放开，也不会丢：
//     打开取时间在这条语句读到“关”之后，而结清的 resolved_at 在这条语句之前就写好了。
//
// 两种隔离级别下，enabled_at 之后结清（resolved_at ≥ enabled_at）的调用都一定有一行。锁的顺序：额度行 → 调用行
// → 开关行（共享）；打开只锁开关行，构不成等待环。
func writeEvent(ctx context.Context, tx *sql.Tx, callID string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox (call_id, account_id, purpose, model_alias, upstream, upstream_model,
			status, usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, resolved_at, cached_from, created_at)
		SELECT c.id, c.account_id, c.purpose, c.model_alias, c.upstream, c.upstream_model,
			c.status, c.usage_prompt_tokens, c.usage_completion_tokens, c.usage_total_tokens, c.resolved_at, c.cached_from,
			UTC_TIMESTAMP(6)
		FROM calls c JOIN usage_events e ON e.id = 1 AND e.enabled_at IS NOT NULL WHERE c.id = ? FOR SHARE`, callID); err != nil {
		return fmt.Errorf("write usage event: %w", err)
	}
	return nil
}

// EnableUsageEvents 打开用量事件，只开不关；已经开着就什么都不改（enabled_at 不随重启变）。
// 先不加锁地读：读到“开”就一直是开的，直接返回，不去排开关行的排他锁（排队时所有实例的结清都排在它后面）。
// 关着才锁住开关行再看一次，还是关的才记下现在的时间。时间在拿到锁之后的另一条语句里取（见 writeEvent）。
//
// 等开关行的排他锁超过会话的上限（1205，2 秒：有结清事务拿着它的共享锁不放）时，在 ctx 的期限里重试，别的错误不重试。
// 代价：等锁的这段时间，新的结清也排在它后面（每个最多等 2 秒）；只在第一次打开时发生。
func (s *Store) EnableUsageEvents(ctx context.Context) error {
	for {
		err := s.enableUsageEvents(ctx)
		if !isMySQLError(err, 1205) || ctx.Err() != nil { // ER_LOCK_WAIT_TIMEOUT
			return err
		}
	}
}

func (s *Store) enableUsageEvents(ctx context.Context) error {
	var on bool
	if err := s.db.QueryRowContext(ctx, "SELECT enabled_at IS NOT NULL FROM usage_events WHERE id = 1").Scan(&on); err != nil {
		return fmt.Errorf("read usage events switch: %w", err)
	}
	if on {
		return nil
	}
	return s.inTx(ctx, nil, func(tx *sql.Tx) error {
		var on bool
		if err := tx.QueryRowContext(ctx, "SELECT enabled_at IS NOT NULL FROM usage_events WHERE id = 1 FOR UPDATE").Scan(&on); err != nil {
			return fmt.Errorf("lock usage events switch: %w", err)
		}
		if on {
			return nil
		}
		if _, err := tx.ExecContext(ctx, "UPDATE usage_events SET enabled_at = UTC_TIMESTAMP(6) WHERE id = 1"); err != nil {
			return fmt.Errorf("enable usage events: %w", err)
		}
		return nil
	})
}

// ForwardOutbox 把 outbox 里还没发出的行按 seq 一批一批交给 send，send 成功才把这一批标成已发出，返回标记的条数。
//
// 只有拿到命名锁的实例发：锁在一个专用连接上，拿不到（另一个实例正在发）就什么都不做，返回 0。连接断了锁自动放开，
// 下一轮另一个实例接手。读没发出的行不加锁、标记按主键，所以转发不会挡住结清事务往 outbox 插新行。
//
// 至少一次：send 成功之后、标记之前出错或者进程死了，这一批下一轮再发一遍，由消费者按调用号去重。send 失败时整批
// 都不标记。seq 是插入的顺序、不是提交的顺序，晚提交的行下一批或下一轮照样读得到（每批都从没发出的里重新取）。
//
// 每批的数据库操作最多 config.LedgerTimeout，从 ctx 脱开；ctx 取消时只在两批之间停下。send 自己限住等待的时间。
func (s *Store) ForwardOutbox(ctx context.Context, send func(context.Context, []UsageEvent) error) (int, error) {
	conn, locked, err := s.lockForward(ctx)
	if err != nil || !locked {
		return 0, err
	}
	defer unlockForward(conn)
	total := 0
	for ctx.Err() == nil {
		seqs, events, err := unsentEvents(ctx, conn)
		if err != nil || len(events) == 0 {
			return total, err
		}
		if err := send(ctx, events); err != nil {
			return total, fmt.Errorf("send usage events: %w", err)
		}
		if err := markSent(ctx, conn, seqs); err != nil {
			return total, err
		}
		total += len(events)
		if len(events) < forwardBatch {
			break
		}
	}
	return total, nil
}

// lockForward 在一个专用连接上不等待地拿转发的命名锁。拿到时返回这个连接（锁属于它）；没拿到时连接已经还回池里。
func (s *Store) lockForward(ctx context.Context) (*sql.Conn, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.LedgerTimeout)
	defer cancel()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("forward lock: %w", err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", forwardLock).Scan(&got); err != nil || got.Int64 != 1 {
		conn.Close()
		if err != nil {
			return nil, false, fmt.Errorf("forward lock: %w", err)
		}
		return nil, false, nil
	}
	return conn, true, nil
}

// unlockForward 放开命名锁、把连接还回池里。放不掉时（连接出错）不把这个会话放回池里：关掉连接，锁随之释放。
func unlockForward(conn *sql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), config.LedgerTimeout)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", forwardLock); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	conn.Close()
}

// unsentEvents 不加锁地取一批还没发出的行，按 seq。
func unsentEvents(ctx context.Context, conn *sql.Conn) ([]any, []UsageEvent, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.LedgerTimeout)
	defer cancel()
	rows, err := conn.QueryContext(ctx, `SELECT seq, call_id, account_id, purpose, model_alias, upstream, upstream_model, status,
			usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, `+formatted("resolved_at")+`, COALESCE(cached_from, '')
		FROM outbox WHERE sent_at IS NULL ORDER BY seq LIMIT ?`, forwardBatch)
	if err != nil {
		return nil, nil, fmt.Errorf("read outbox: %w", err)
	}
	defer rows.Close()
	var seqs []any
	var events []UsageEvent
	for rows.Next() {
		var seq int64
		var e UsageEvent
		var prompt, completion, total sql.NullInt64
		var resolved string
		if err := rows.Scan(&seq, &e.CallID, &e.AccountID, &e.Purpose, &e.Alias, &e.Upstream, &e.UpstreamModel, &e.Status,
			&prompt, &completion, &total, &resolved, &e.CachedFrom); err != nil {
			return nil, nil, fmt.Errorf("read outbox: %w", err)
		}
		if e.ResolvedAt, err = time.Parse(time.RFC3339Nano, resolved); err != nil {
			return nil, nil, fmt.Errorf("outbox %s: resolved_at: %w", e.CallID, err)
		}
		e.Usage = scanUsage(prompt, completion, total)
		seqs, events = append(seqs, seq), append(events, e)
	}
	return seqs, events, rows.Err()
}

// markSent 按主键把这一批标成已发出（自动提交，只锁这几行）。
func markSent(ctx context.Context, conn *sql.Conn, seqs []any) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.LedgerTimeout)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "UPDATE outbox SET sent_at = UTC_TIMESTAMP(6) WHERE seq IN ("+placeholders(len(seqs))+")",
		seqs...); err != nil {
		return fmt.Errorf("mark usage events sent: %w", err)
	}
	return nil
}

func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?, ", n), ", ") }

// OutboxBacklog 是还没发出的 outbox 行：条数，和最早的一条（seq 最小的）从写入到现在的秒数（没有时为 0），
// 按数据库的时钟算。给指标用，转发每轮查一次。走 outbox_sent，只扫没发出的行。
func (s *Store) OutboxBacklog(ctx context.Context) (n, oldestAgeSeconds int64, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(TIMESTAMPDIFF(SECOND,
			(SELECT created_at FROM outbox WHERE sent_at IS NULL ORDER BY seq LIMIT 1), UTC_TIMESTAMP(6)), 0)
		FROM outbox WHERE sent_at IS NULL`).Scan(&n, &oldestAgeSeconds)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox backlog: %w", err)
	}
	return n, oldestAgeSeconds, nil
}

// purgeOutboxBatch 是清理 outbox 一次取的行数。
const purgeOutboxBatch = 1000

// PurgeOutbox 删掉发出超过 outboxRetentionDays 天的 outbox 行，返回删掉的条数；没发出的再老也不删。
// 先不加锁地取一批候选，再按主键删（删时再判断一次）：不用按 sent_at 范围的 DELETE，那样在可重复读下会锁住
// sent_at 为 NULL 的区域后面的间隙，挡住结清事务插新的（没发出的）行。ctx 到期时在两批之间停下，返回 ErrPurgeStopped。
func (s *Store) PurgeOutbox(ctx context.Context, opTimeout time.Duration) (int64, error) {
	var total int64
	for {
		if ctx.Err() != nil {
			return total, fmt.Errorf("%w: %w", ErrPurgeStopped, ctx.Err())
		}
		n, more, err := s.purgeOutboxBatch(ctx, opTimeout)
		total += n
		if err != nil || !more {
			return total, err
		}
	}
}

func (s *Store) purgeOutboxBatch(ctx context.Context, opTimeout time.Duration) (n int64, more bool, err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT seq FROM outbox
		WHERE sent_at < UTC_TIMESTAMP(6) - INTERVAL ? DAY ORDER BY sent_at, seq LIMIT ?`, outboxRetentionDays, purgeOutboxBatch)
	if err != nil {
		return 0, false, fmt.Errorf("find old outbox rows: %w", err)
	}
	var seqs []any
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			return 0, false, err
		}
		seqs = append(seqs, seq)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(seqs) == 0 {
		return 0, false, err
	}
	res, err := s.db.ExecContext(ctx, "DELETE FROM outbox WHERE seq IN ("+placeholders(len(seqs))+
		") AND sent_at < UTC_TIMESTAMP(6) - INTERVAL ? DAY", append(seqs, outboxRetentionDays)...)
	if err != nil {
		return 0, false, fmt.Errorf("purge outbox: %w", err)
	}
	n, err = res.RowsAffected()
	return n, len(seqs) == purgeOutboxBatch, err
}
