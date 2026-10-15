package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Actor 是转换调用状态的一方，记进 calls.resolved_by。
type Actor string

const (
	Gateway   Actor = "gateway"   // 网关：发送意图、收尾、迟到的结果
	Reclaim   Actor = "reclaim"   // 后台回收：超过上界还没结束的调用
	Reconcile Actor = "reconcile" // qfctl reconcile：凭证据结清未知用量
)

// Evidence 是结清凭的证据，记进 calls.resolution_evidence。
type Evidence string

const (
	Answered     Evidence = "response"      // 网关及时拿到的上游回答：2xx 带合法 usage，或者 4xx
	NotSent      Evidence = "not_sent"      // 确定没发出：发出前失败、拨号失败、回收释放超过上界的 RESERVED
	LateResponse Evidence = "late_response" // 调用已被回收标成未知之后，网关自己拿到的结果
	CallLog      Evidence = "call_log"      // QF 调用日志里确定的结局（账本里的收尾没写成）
	Bill         Evidence = "bill"          // 导入的上游按调用的账单
)

// transitions 是允许的状态转换：谁、从哪个状态、到哪些状态。其余一律不允许，SETTLED、CANCELLED 是终态。
// RESERVED → IN_FLIGHT（发送意图）只有 MarkSent 一条路，不经过这里。网关换上游时的改道（Reroute）只在 IN_FLIGHT 上
// 改上游两列、状态不变，也不经过这里；Verify 不读这两列。
var transitions = map[Actor]map[Status][]Status{
	Gateway:   {Reserved: {Cancelled}, InFlight: {Settled, Cancelled, UsageUnknown}, UsageUnknown: {Settled, Cancelled}},
	Reclaim:   {Reserved: {Cancelled}, InFlight: {UsageUnknown}},
	Reconcile: {UsageUnknown: {Settled, Cancelled}},
}

// resolutions 是结清时合法的证据：谁把调用结清成什么状态，可以凭哪些证据。转换时按它填值，Verify 按它检查。
var resolutions = map[Actor]map[Status][]Evidence{
	Gateway:   {Settled: {Answered, LateResponse}, Cancelled: {Answered, NotSent, LateResponse}},
	Reclaim:   {Cancelled: {NotSent}},
	Reconcile: {Settled: {CallLog, Bill}, Cancelled: {CallLog, Bill}},
}

func terminal(s Status) bool { return s == Settled || s == Cancelled }

// ErrNotAllowed 表示转换表不允许这次转换（包括调用已经结清过）。什么都没改。
var ErrNotAllowed = errors.New("transition not allowed")

// ErrCallNotFound 表示账本里没有这个调用（或者不属于这个账户）。
var ErrCallNotFound = errors.New("call not found")

// lockedCall 是锁住之后读到的调用。
type lockedCall struct {
	id        string
	accountID int64
	status    Status
	purpose   string
	reserved  int64
	http      sql.NullInt64
	usage     *Usage
}

// lockCall 先锁账户的额度行，再按主键锁住调用这一行，读出它现在的状态。所有改调用状态的事务都从这里开始。
func lockCall(ctx context.Context, tx *sql.Tx, accountID int64, callID string) (*lockedCall, error) {
	if err := lockBudget(ctx, tx, accountID); err != nil {
		return nil, err
	}
	c := lockedCall{id: callID, accountID: accountID}
	var prompt, completion, total sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT status, purpose, reserved_tokens, http_status,
			usage_prompt_tokens, usage_completion_tokens, usage_total_tokens
		FROM calls WHERE id = ? AND account_id = ? FOR UPDATE`, callID, accountID).Scan(
		&c.status, &c.purpose, &c.reserved, &c.http, &prompt, &completion, &total)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("call %s: %w", callID, ErrCallNotFound)
	case err != nil:
		return nil, fmt.Errorf("lock call: %w", err)
	}
	c.usage = scanUsage(prompt, completion, total)
	return &c, nil
}

func scanUsage(prompt, completion, total sql.NullInt64) *Usage {
	if !prompt.Valid || !total.Valid {
		return nil
	}
	return &Usage{Prompt: prompt.Int64, Completion: completion.Int64, Total: total.Int64, HasCompletion: completion.Valid}
}

// checkTransition 按转换表判断 by 能不能把调用从现在的状态改成 to。
func checkTransition(by Actor, c *lockedCall, to Status) error {
	if !slices.Contains(transitions[by][c.status], to) {
		return fmt.Errorf("%w: call %s is %s, %s cannot make it %s", ErrNotAllowed, c.id, c.status, by, to)
	}
	return nil
}

// setStatus 按主键、带着锁住时读到的状态做条件更新：status 改成 to，结清（终态）时写审计三列；
// set 和 args 是这一方自己要写的其它列。改到 0 行说明状态变了（不该发生，调用行已经锁住），返回错误让事务回滚。
func setStatus(ctx context.Context, tx *sql.Tx, callID string, from, to Status, by Actor, evidence Evidence, set string, args ...any) error {
	var resolvedBy, resolution any // nil 存 NULL
	resolvedAt := "NULL"
	if terminal(to) {
		if !slices.Contains(resolutions[by][to], evidence) {
			return fmt.Errorf("%w: %s cannot make call %s %s on evidence %q", ErrNotAllowed, by, callID, to, evidence)
		}
		resolvedBy, resolution, resolvedAt = by, evidence, "UTC_TIMESTAMP(6)"
	}
	q := `UPDATE calls SET status = ?, resolved_by = ?, resolution_evidence = ?, resolved_at = ` + resolvedAt
	if set != "" {
		q += ", " + set
	}
	q += " WHERE id = ? AND status = ?"
	res, err := tx.ExecContext(ctx, q, slices.Concat([]any{to, resolvedBy, resolution}, args, []any{callID, from})...)
	if err != nil {
		return fmt.Errorf("update call: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("update call %s: status is no longer %s (%v)", callID, from, err)
	}
	return nil
}

// closeBudget 按转换的目标改额度和账本：结算释放预留、记实际用量（可以超过预留）；释放只放预留；
// 用量未知时额度和账本都不动，继续占着预留。
func closeBudget(ctx context.Context, tx *sql.Tx, accountID int64, callID string, to Status, reserved int64, u *Usage) error {
	switch to {
	case Settled:
		return moveBudget(ctx, tx, accountID, callID, "settle", -reserved, u.Total)
	case Cancelled:
		return moveBudget(ctx, tx, accountID, callID, "release", -reserved, 0)
	}
	return nil
}

// 回收写进 error_code 的原因。
const (
	reclaimedBeforeSend = "reclaimed_before_send"
	reclaimedInFlight   = "reclaimed_in_flight"
)

// reclaimBatch 是回收一次取的候选数。
const reclaimBatch = 100

// ReclaimStats 是一次回收的结果：释放的 RESERVED 数、标成未知的 IN_FLIGHT 数。
type ReclaimStats struct{ Released, Unknown int }

// Reclaim 回收从预留（created_at）算起超过 after 还没结束的调用：RESERVED 是确定没发出，释放；IN_FLIGHT 标成未知。
// after 由调用方按超时算出（见 config.ReclaimAfter），比一次活着的调用停在这两个状态的最长时间还长。时间都用数据库的。
//
// 候选用不加锁的读取出，每个候选一个事务，走和收尾一样的锁顺序：先锁额度行，再按主键锁调用行、重新读状态，按读到的状态转换。
// 不用一条按索引范围的 UPDATE 批量改：那样没法先锁额度行，而且先锁二级索引、再锁主键行，和按主键改的收尾顺序相反。
// 多个实例同时回收、回收和收尾或对账同时碰到同一个调用时，后拿到锁的看到的已经不是原来的状态，每个调用只转换一次。
//
// 一个候选的事务出错（额度行被长时间锁住、坏行）只算它自己：记下来，接着处理下一个，不挡住别的账户。
// 一批一批地取，下一批按 (created_at, id) 接在上一批最后一个之后（键集分页），所以一轮里每个候选最多处理一次，
// 一批全出错也照样往后翻，一轮一定结束。出错的下一轮从头取，会再试。整轮结束时有出错，返回错误：出错的个数和第一个错误。
// 没有出错的候选照常计入 ReclaimStats。
//
// 每个事务最多 opTimeout，从 ctx 脱开：ctx 取消时只在两个事务之间停下，不打断进行中的事务。
// 一个账户的额度行一直被锁住时，一轮在它的每个候选上各等 opTimeout；不在出错之后跳过这个账户剩下的候选。
func (s *Store) Reclaim(ctx context.Context, after, opTimeout time.Duration) (ReclaimStats, error) {
	var stats ReclaimStats
	var failed int
	var first error
	cursor := candidate{createdAt: minDatetime}
	for {
		batch, err := s.reclaimCandidates(ctx, after, opTimeout, cursor)
		if err != nil {
			return stats, err
		}
		for _, c := range batch {
			if ctx.Err() != nil {
				return stats, ctx.Err()
			}
			to, err := s.reclaimOne(ctx, c.account, c.id, opTimeout)
			switch {
			case err != nil:
				if failed++; first == nil {
					first = err
				}
			case to == Cancelled:
				stats.Released++
			case to == UsageUnknown:
				stats.Unknown++
			}
		}
		if len(batch) < reclaimBatch {
			break
		}
		cursor = batch[len(batch)-1]
	}
	if failed > 0 {
		return stats, fmt.Errorf("reclaim: %d calls failed, first: %w", failed, first)
	}
	return stats, nil
}

// minDatetime 比任何 created_at 都早（DATETIME 的最小值），是第一批的游标。
const minDatetime = "1000-01-01 00:00:00.000000"

// candidate 是一个回收候选，也是翻页的游标：createdAt 是 DATETIME(6) 的文本，原样传回数据库比较。
type candidate struct {
	id        string
	account   int64
	createdAt string
}

// reclaimCandidates 取游标之后的一批候选，按 (created_at, id) 排序。回收的条件里有两个状态，
// 排序用不上 calls_status_created 的顺序，要 filesort；候选只有超过上界的那些，量小。
func (s *Store) reclaimCandidates(ctx context.Context, after, opTimeout time.Duration, cursor candidate) ([]candidate, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, account_id, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f') FROM calls
		WHERE status IN ('RESERVED', 'IN_FLIGHT') AND created_at <= UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
			AND (created_at, id) > (?, ?)
		ORDER BY created_at, id LIMIT ?`, after.Microseconds(), cursor.createdAt, cursor.id, reclaimBatch)
	if err != nil {
		return nil, fmt.Errorf("reclaim candidates: %w", err)
	}
	defer rows.Close()
	var out []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.account, &c.createdAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// reclaimOne 回收一个调用，返回它转到的状态；锁住后已经不是 RESERVED、IN_FLIGHT 的（已收尾、已被别的实例回收）不动，返回空。
func (s *Store) reclaimOne(ctx context.Context, accountID int64, callID string, opTimeout time.Duration) (Status, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	var to Status
	err := s.inTx(ctx, nil, func(tx *sql.Tx) error {
		c, err := lockCall(ctx, tx, accountID, callID)
		if err != nil {
			return err
		}
		reason := reclaimedBeforeSend
		switch c.status { // 目标按锁住之后读到的状态定：扫描之后才落了发送意图的，标未知，不能释放
		case Reserved:
			to = Cancelled
		case InFlight:
			to, reason = UsageUnknown, reclaimedInFlight
		default:
			return nil
		}
		if err := checkTransition(Reclaim, c, to); err != nil {
			return err
		}
		if err := setStatus(ctx, tx, callID, c.status, to, Reclaim, NotSent, "error_code = ?", reason); err != nil {
			return err
		}
		return closeBudget(ctx, tx, accountID, callID, to, c.reserved, nil)
	})
	if err != nil {
		return "", fmt.Errorf("reclaim %s: %w", callID, err)
	}
	return to, nil
}
