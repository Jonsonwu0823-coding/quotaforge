package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/go-sql-driver/mysql"
)

// 消费者那一边：按调用号去重、按账户按天汇总（usage_seen、usage_daily），以及拿汇总和账本对账。
// 真实部署里这两张表在下游自己的库里；这里和账本同一个库，方便对账。

// UsageTotals 是一天、一个账户、一种用途的汇总：结算的调用数和用量（就是这一天的已消费）、缓存命中的调用数和
// 省下的 total、取消的调用数。
type UsageTotals struct {
	Settled, Prompt, Completion, Total int64
	Cached, CachedTokens               int64
	Cancelled                          int64
}

// add 把一个结清的调用记进汇总。用量缺的项按 0 加（嵌入没有 completion_tokens；取消的调用没有用量）。
func (t *UsageTotals) add(status Status, u *Usage) {
	var v Usage
	if u != nil {
		v = *u
	}
	switch status {
	case Settled:
		t.Settled++
		t.Prompt += v.Prompt
		t.Completion += v.Completion
		t.Total += v.Total
	case Cached:
		t.Cached++
		t.CachedTokens += v.Total
	case Cancelled:
		t.Cancelled++
	}
}

// ApplyUsageEvent 把一个事件记进按天的汇总，一个事务（读已提交，不加间隙锁）：先记下调用号，主键冲突就是处理过的
// 重复，什么都不改，返回 false；否则把这次调用加到 (账户, 日期, 用途) 那一行上，返回 true。
// 去重和汇总在同一个事务里，所以一个调用只会被算一次：要么两样都写了，要么都没有。
func (s *Store) ApplyUsageEvent(ctx context.Context, e UsageEvent) (bool, error) {
	var t UsageTotals
	t.add(e.Status, e.Usage)
	applied := true
	err := s.inTx(ctx, readCommitted, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO usage_seen (call_id, account_id, day, purpose, consumed_at)
			VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6))`, e.CallID, e.AccountID, e.Day(), e.Purpose)
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 { // ER_DUP_ENTRY：处理过了
			applied = false
			return nil
		}
		if err != nil {
			return fmt.Errorf("record usage event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO usage_daily (account_id, day, purpose, settled_calls, prompt_tokens,
				completion_tokens, total_tokens, cached_calls, cached_tokens, cancelled_calls)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS n
			ON DUPLICATE KEY UPDATE settled_calls = usage_daily.settled_calls + n.settled_calls,
				prompt_tokens = usage_daily.prompt_tokens + n.prompt_tokens,
				completion_tokens = usage_daily.completion_tokens + n.completion_tokens,
				total_tokens = usage_daily.total_tokens + n.total_tokens,
				cached_calls = usage_daily.cached_calls + n.cached_calls,
				cached_tokens = usage_daily.cached_tokens + n.cached_tokens,
				cancelled_calls = usage_daily.cancelled_calls + n.cancelled_calls`,
			e.AccountID, e.Day(), e.Purpose, t.Settled, t.Prompt, t.Completion, t.Total, t.Cached, t.CachedTokens, t.Cancelled); err != nil {
			return fmt.Errorf("add usage event to the daily totals: %w", err)
		}
		return nil
	})
	return applied && err == nil, err
}

// 对账里一个调用的去向。
const (
	GapUnsent     = "unsent"     // 在 outbox 里，还没发出（在路上）
	GapUnconsumed = "unconsumed" // 发出了，消费者还没记（在路上）
	GapLost       = "LOST"       // 账本里结清了，没有 outbox 行，消费者也没有
	GapMisfiled   = "MISFILED"   // 消费者记了，但记在别的账户、日期或用途下（那一行另报成 extra）
	GapExtra      = "extra"      // 消费者记了，账本的范围里没有对得上的调用
)

// UsageDay 是对账的一行：一天、一个账户、一种用途，两边的汇总。Consumed 是账本里已被消费者记下的那些调用的汇总，
// 必须和 Consumer 逐项相等；Ledger 和 Consumer 的差只能来自在路上的调用。
type UsageDay struct {
	Day, Account, Purpose        string
	Ledger, Consumed, Consumer   UsageTotals
	Unsent, Unconsumed, Problems int64
}

// Result 是这一行的结论：same（完全相同）、in-transit（只差在路上的）、MISMATCH。
func (d UsageDay) Result() string {
	switch {
	case d.Problems > 0 || d.Consumed != d.Consumer:
		return "MISMATCH"
	case d.Unsent+d.Unconsumed > 0:
		return "in-transit"
	}
	return "same"
}

// UsageGap 是一个对不上的调用：账本有、消费者没有的（Kind 是在路上的两种或 LOST），或者消费者多出来的（extra）。
type UsageGap struct {
	Kind, CallID, Day, Account, Purpose string
	AgeSeconds                          int64 // 在路上的：从结清到现在的秒数
}

// UsageReport 是对账的结果，按日期、账户、用途排序。
type UsageReport struct {
	Days []UsageDay
	Gaps []UsageGap
}

// ReconcileUsage 拿消费者的汇总和账本对账。day 为空时查从打开开关那天到今天（UTC）的每一天；account 为空时查全部账户。
//
// 账本那边的范围：这些天里结清的调用（按 resolved_at 的 UTC 日期，和事件同一列），resolved_at 不早于开关打开的时间，
// 或者已经有事件（打开的那一刻正在结清的调用可能也写了）。在一个只读、可重复读的事务里查，两边是同一个快照。
// 开关没打开过时返回 ErrUsageEventsOff。
func (s *Store) ReconcileUsage(ctx context.Context, day, account string) (*UsageReport, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var from, to sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT DATE(enabled_at), DATE(UTC_TIMESTAMP(6)) FROM usage_events WHERE id = 1`).Scan(&from, &to); err != nil {
		return nil, fmt.Errorf("read usage events switch: %w", err)
	}
	if !from.Valid {
		return nil, ErrUsageEventsOff
	}
	if day != "" {
		from.String, to.String = day, day
	}
	days := map[usageKey]*UsageDay{}
	r := &UsageReport{}
	seen, err := ledgerSide(ctx, tx, from.String, to.String, account, days, r)
	if err != nil {
		return nil, err
	}
	if err := consumerSide(ctx, tx, from.String, to.String, account, seen, days, r); err != nil {
		return nil, err
	}
	for _, d := range days {
		r.Days = append(r.Days, *d)
	}
	sort.Slice(r.Days, func(i, j int) bool { return r.Days[i].key().less(r.Days[j].key()) })
	sort.SliceStable(r.Gaps, func(i, j int) bool { return r.Gaps[i].key().less(r.Gaps[j].key()) })
	return r, nil
}

type usageKey struct{ day, account, purpose string }

func (k usageKey) less(o usageKey) bool {
	if k.day != o.day {
		return k.day < o.day
	}
	if k.account != o.account {
		return k.account < o.account
	}
	return k.purpose < o.purpose
}

func (d UsageDay) key() usageKey { return usageKey{d.Day, d.Account, d.Purpose} }
func (g UsageGap) key() usageKey { return usageKey{g.Day, g.Account, g.Purpose} }

func (k usageKey) row(days map[usageKey]*UsageDay) *UsageDay {
	d := days[k]
	if d == nil {
		d = &UsageDay{Day: k.day, Account: k.account, Purpose: k.purpose}
		days[k] = d
	}
	return d
}

// ledgerSide 逐个读账本范围里的调用，记进 Ledger；消费者按同样的账户、日期、用途记下了的，也记进 Consumed；
// 没记下的按 outbox 分成在路上的两种和 LOST，记错了地方的是 MISFILED。返回范围里被消费者记下的调用号（找 extra 用）。
func ledgerSide(ctx context.Context, tx *sql.Tx, from, to, account string, days map[usageKey]*UsageDay, r *UsageReport) (map[string]bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.id, a.name, DATE_FORMAT(c.resolved_at, '%Y-%m-%d'), c.purpose, c.status,
			c.usage_prompt_tokens, c.usage_completion_tokens, c.usage_total_tokens,
			s.call_id IS NOT NULL AND s.account_id = c.account_id AND s.day = DATE(c.resolved_at) AND s.purpose = c.purpose,
			s.call_id IS NOT NULL, o.call_id IS NOT NULL, o.sent_at IS NOT NULL,
			TIMESTAMPDIFF(SECOND, c.resolved_at, UTC_TIMESTAMP(6))
		FROM calls c JOIN accounts a ON a.id = c.account_id JOIN usage_events e ON e.id = 1
			LEFT JOIN usage_seen s ON s.call_id = c.id LEFT JOIN outbox o ON o.call_id = c.id
		WHERE c.status IN ('SETTLED', 'CANCELLED', 'CACHED')
			AND c.resolved_at >= ? AND c.resolved_at < DATE(?) + INTERVAL 1 DAY AND (? = '' OR a.name = ?)
			AND (c.resolved_at >= e.enabled_at OR s.call_id IS NOT NULL OR o.call_id IS NOT NULL)`, from, to, account, account)
	if err != nil {
		return nil, fmt.Errorf("reconcile usage: read the ledger: %w", err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var g UsageGap
		var status Status
		var prompt, completion, total sql.NullInt64
		var consumed, anySeen, inOutbox, sent bool
		if err := rows.Scan(&g.CallID, &g.Account, &g.Day, &g.Purpose, &status, &prompt, &completion, &total,
			&consumed, &anySeen, &inOutbox, &sent, &g.AgeSeconds); err != nil {
			return nil, err
		}
		d := g.key().row(days)
		u := scanUsage(prompt, completion, total)
		d.Ledger.add(status, u)
		switch {
		case consumed:
			d.Consumed.add(status, u)
			seen[g.CallID] = true
			continue
		case anySeen:
			g.Kind = GapMisfiled
			d.Problems++
		case inOutbox && sent:
			g.Kind = GapUnconsumed
			d.Unconsumed++
		case inOutbox:
			g.Kind = GapUnsent
			d.Unsent++
		default:
			g.Kind = GapLost
			d.Problems++
		}
		r.Gaps = append(r.Gaps, g)
	}
	return seen, rows.Err()
}

// consumerSide 读消费者的汇总记进 Consumer，再找出消费者记了、账本范围里没对上的调用（extra）。
func consumerSide(ctx context.Context, tx *sql.Tx, from, to, account string, seen map[string]bool, days map[usageKey]*UsageDay, r *UsageReport) error {
	rows, err := tx.QueryContext(ctx, `SELECT DATE_FORMAT(u.day, '%Y-%m-%d'), COALESCE(a.name, CONCAT('#', u.account_id)), u.purpose,
			u.settled_calls, u.prompt_tokens, u.completion_tokens, u.total_tokens, u.cached_calls, u.cached_tokens, u.cancelled_calls
		FROM usage_daily u LEFT JOIN accounts a ON a.id = u.account_id
		WHERE u.day BETWEEN ? AND ? AND (? = '' OR a.name = ?)`, from, to, account, account)
	if err != nil {
		return fmt.Errorf("reconcile usage: read the daily totals: %w", err)
	}
	for rows.Next() {
		var k usageKey
		var t UsageTotals
		if err := rows.Scan(&k.day, &k.account, &k.purpose, &t.Settled, &t.Prompt, &t.Completion, &t.Total,
			&t.Cached, &t.CachedTokens, &t.Cancelled); err != nil {
			rows.Close()
			return err
		}
		k.row(days).Consumer = t
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT s.call_id, DATE_FORMAT(s.day, '%Y-%m-%d'), COALESCE(a.name, CONCAT('#', s.account_id)), s.purpose
		FROM usage_seen s LEFT JOIN accounts a ON a.id = s.account_id
		WHERE s.day BETWEEN ? AND ? AND (? = '' OR a.name = ?)`, from, to, account, account)
	if err != nil {
		return fmt.Errorf("reconcile usage: read the consumed calls: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		g := UsageGap{Kind: GapExtra}
		if err := rows.Scan(&g.CallID, &g.Day, &g.Account, &g.Purpose); err != nil {
			return err
		}
		if !seen[g.CallID] {
			g.key().row(days).Problems++
			r.Gaps = append(r.Gaps, g)
		}
	}
	return rows.Err()
}
