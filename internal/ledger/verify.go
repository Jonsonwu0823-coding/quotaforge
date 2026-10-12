package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
)

// Violation 是一条不成立的不变量：哪一条规则、哪个账户、哪个调用（账户级的规则没有调用）。
type Violation struct {
	Rule    string
	Account string
	Call    string
	Detail  string
}

func (v Violation) String() string {
	s := v.Rule + " account=" + v.Account
	if v.Call != "" {
		s += " call=" + v.Call
	}
	return s + ": " + v.Detail
}

// Verify 检查账本的不变量，返回所有不成立的条目（全部成立时为空）。
// 在一个只读、可重复读的事务里查，读到的是同一个快照，服务运行中也能跑。
//
// 账户级：
//   - budget_reserved：额度行的已预留 = 账本里预留变动之和；
//   - budget_consumed：额度行的已消费 = 账本里消费变动之和；
//   - reserved_not_negative：已预留不为负。
//
// 调用级：
//   - reserve_entry：恰有一条 reserve，预留变动 = 调用的预留量、消费变动为 0；
//   - settled：有 settle、没有 release，预留变动合计 0，消费变动合计 = usage 的 total；
//   - cancelled：有 release、没有 settle，预留和消费变动合计都是 0；
//   - open：RESERVED、IN_FLIGHT、USAGE_UNKNOWN 只有 reserve，继续占着自己的预留；
//   - sent：发送意图的时间。RESERVED 和回收释放的（确定没发出）没有 sent_at；IN_FLIGHT、USAGE_UNKNOWN、SETTLED，
//     以及凭 response、late_response 或由 reconcile 结清的，都有 sent_at。迁移前的调用（created_at 早于版本 3 的迁移时间）
//     推不出发送的时间，迁移时没有补，不要求有；
//   - resolution：SETTLED、CANCELLED 的审计三列（谁、凭什么、什么时候）都有，而且是 resolutions 表里合法的组合；
//     其余状态三列都是空的。
//
// 幂等记录级（见 verifyIdempotency）：
//   - idem_call：记录指向的调用属于同一个账户，而且那次调用记的键就是这个键；
//   - idem_snapshot：有快照的记录，调用是 SETTLED 或 USAGE_UNKNOWN，状态码是 2xx 且等于调用的 http_status，
//     正文长度对得上：不超过上限的存了正文，超过的正文为空；没有快照的记录，快照的几列都是空的；
//   - idem_settled_snapshot：指向 SETTLED 调用的记录有快照（或者标明正文超过上限）；
//     只有凭证据由 reconcile 结算的调用可以没有（QF 没拿到或没存下响应）。
func (s *Store) Verify(ctx context.Context) ([]Violation, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	out, err := verifyBudgets(ctx, tx)
	if err != nil {
		return nil, err
	}
	calls, err := verifyCalls(ctx, tx)
	if err != nil {
		return nil, err
	}
	records, err := verifyIdempotency(ctx, tx)
	if err != nil {
		return nil, err
	}
	return append(append(out, calls...), records...), nil
}

func verifyBudgets(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT a.name, b.reserved_tokens, b.consumed_tokens,
			COALESCE(SUM(l.reserved_delta), 0), COALESCE(SUM(l.consumed_delta), 0)
		FROM budgets b JOIN accounts a ON a.id = b.account_id
		LEFT JOIN calls c ON c.account_id = b.account_id LEFT JOIN ledger l ON l.call_id = c.id
		GROUP BY b.account_id, a.name, b.reserved_tokens, b.consumed_tokens ORDER BY a.name`)
	if err != nil {
		return nil, fmt.Errorf("verify budgets: %w", err)
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var account string
		var reserved, consumed, ledgerReserved, ledgerConsumed int64
		if err := rows.Scan(&account, &reserved, &consumed, &ledgerReserved, &ledgerConsumed); err != nil {
			return nil, err
		}
		if reserved != ledgerReserved {
			out = append(out, Violation{"budget_reserved", account, "", fmt.Sprintf("budget %d, ledger %d", reserved, ledgerReserved)})
		}
		if consumed != ledgerConsumed {
			out = append(out, Violation{"budget_consumed", account, "", fmt.Sprintf("budget %d, ledger %d", consumed, ledgerConsumed)})
		}
		if reserved < 0 {
			out = append(out, Violation{"reserved_not_negative", account, "", fmt.Sprintf("reserved %d", reserved)})
		}
	}
	return out, rows.Err()
}

// callEntries 是一个调用的账本汇总和审计列。
type callEntries struct {
	account, id                 string
	status                      Status
	amount                      int64
	usageTotal                  sql.NullInt64
	reserves, settles, releases int64
	reserveReserved             int64 // reserve 行的变动
	reserveConsumed             int64
	sumReserved, sumConsumed    int64 // 所有行的变动之和
	sentNull, premigration      bool
	resolvedBy, evidence        sql.NullString
	resolvedAtNull              bool
}

func verifyCalls(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT a.name, c.id, c.status, c.reserved_tokens, c.usage_total_tokens,
			COALESCE(SUM(l.kind = 'reserve'), 0), COALESCE(SUM(l.kind = 'settle'), 0), COALESCE(SUM(l.kind = 'release'), 0),
			COALESCE(SUM(CASE WHEN l.kind = 'reserve' THEN l.reserved_delta END), 0),
			COALESCE(SUM(CASE WHEN l.kind = 'reserve' THEN l.consumed_delta END), 0),
			COALESCE(SUM(l.reserved_delta), 0), COALESCE(SUM(l.consumed_delta), 0),
			c.sent_at IS NULL, COALESCE(c.created_at < (SELECT applied_at FROM schema_migrations WHERE version = 3), FALSE),
			c.resolved_by, c.resolution_evidence, c.resolved_at IS NULL
		FROM calls c JOIN accounts a ON a.id = c.account_id LEFT JOIN ledger l ON l.call_id = c.id
		GROUP BY c.id, a.name, c.status, c.reserved_tokens, c.usage_total_tokens ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("verify calls: %w", err)
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var e callEntries
		if err := rows.Scan(&e.account, &e.id, &e.status, &e.amount, &e.usageTotal, &e.reserves, &e.settles, &e.releases,
			&e.reserveReserved, &e.reserveConsumed, &e.sumReserved, &e.sumConsumed,
			&e.sentNull, &e.premigration, &e.resolvedBy, &e.evidence, &e.resolvedAtNull); err != nil {
			return nil, err
		}
		out = append(out, e.check()...)
	}
	return out, rows.Err()
}

func (e callEntries) check() []Violation {
	var out []Violation
	add := func(rule, format string, args ...any) {
		out = append(out, Violation{rule, e.account, e.id, fmt.Sprintf(format, args...)})
	}
	if e.reserves != 1 || e.reserveReserved != e.amount || e.reserveConsumed != 0 {
		add("reserve_entry", "%d reserve entries with deltas %d/%d, reserved amount %d", e.reserves, e.reserveReserved, e.reserveConsumed, e.amount)
	}
	switch e.status {
	case Settled:
		if e.settles != 1 || e.releases != 0 || e.sumReserved != 0 || !e.usageTotal.Valid || e.sumConsumed != e.usageTotal.Int64 {
			add("settled", "settle %d, release %d, reserved delta %d, consumed delta %d, usage total %v",
				e.settles, e.releases, e.sumReserved, e.sumConsumed, e.usageTotal)
		}
	case Cancelled:
		if e.releases != 1 || e.settles != 0 || e.sumReserved != 0 || e.sumConsumed != 0 {
			add("cancelled", "release %d, settle %d, reserved delta %d, consumed delta %d", e.releases, e.settles, e.sumReserved, e.sumConsumed)
		}
	default: // RESERVED、IN_FLIGHT、USAGE_UNKNOWN
		if e.settles != 0 || e.releases != 0 || e.sumReserved != e.amount || e.sumConsumed != 0 {
			add("open", "%s: settle %d, release %d, reserved delta %d (amount %d), consumed delta %d",
				e.status, e.settles, e.releases, e.sumReserved, e.amount, e.sumConsumed)
		}
	}
	by, evidence := Actor(e.resolvedBy.String), Evidence(e.evidence.String)
	if terminal(e.status) {
		if !e.resolvedBy.Valid || !e.evidence.Valid || e.resolvedAtNull || !slices.Contains(resolutions[by][e.status], evidence) {
			add("resolution", "%s resolved by %q on evidence %q, resolved_at set %v", e.status, by, evidence, !e.resolvedAtNull)
		}
	} else if e.resolvedBy.Valid || e.evidence.Valid || !e.resolvedAtNull {
		add("resolution", "%s call has resolution columns set", e.status)
	}
	mustBeSent := e.status == InFlight || e.status == UsageUnknown || e.status == Settled ||
		evidence == Answered || evidence == LateResponse || by == Reconcile
	neverSent := e.status == Reserved || by == Reclaim
	if (mustBeSent && e.sentNull && !e.premigration) || (neverSent && !e.sentNull) {
		add("sent", "%s call (resolved by %q on %q), sent_at set %v", e.status, by, evidence, !e.sentNull)
	}
	return out
}

// idemEntry 是一条幂等记录和它指向的调用，不含正文。
type idemEntry struct {
	account, call, key        string
	sameAccount, sameKey      bool
	callStatus                Status
	resolvedBy                sql.NullString
	callHTTPStatus            sql.NullInt64
	status, bytes, bodyLen    sql.NullInt64
	contentTypeNull, bodyNull bool
}

func verifyIdempotency(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT COALESCE(a.name, CONCAT('#', k.account_id)), k.call_id, k.idem_key,
			k.account_id = c.account_id, c.idempotency_key <=> k.idem_key, c.status, c.resolved_by, c.http_status,
			k.response_status, k.response_bytes, LENGTH(k.response_body),
			k.response_content_type IS NULL, k.response_body IS NULL
		FROM idempotency_keys k JOIN calls c ON c.id = k.call_id LEFT JOIN accounts a ON a.id = k.account_id
		ORDER BY k.account_id, k.idem_key`)
	if err != nil {
		return nil, fmt.Errorf("verify idempotency records: %w", err)
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var e idemEntry
		if err := rows.Scan(&e.account, &e.call, &e.key, &e.sameAccount, &e.sameKey, &e.callStatus, &e.resolvedBy, &e.callHTTPStatus,
			&e.status, &e.bytes, &e.bodyLen, &e.contentTypeNull, &e.bodyNull); err != nil {
			return nil, err
		}
		out = append(out, e.check()...)
	}
	return out, rows.Err()
}

func (e idemEntry) check() []Violation {
	var out []Violation
	add := func(rule, format string, args ...any) {
		out = append(out, Violation{rule, e.account, e.call, fmt.Sprintf("key %q: ", e.key) + fmt.Sprintf(format, args...)})
	}
	if !e.sameAccount || !e.sameKey {
		add("idem_call", "call in the same account %v, call has this key %v", e.sameAccount, e.sameKey)
	}
	if !e.status.Valid {
		if e.bytes.Valid || !e.contentTypeNull || !e.bodyNull {
			add("idem_snapshot", "snapshot columns set without a status")
		}
		if e.callStatus == Settled && Actor(e.resolvedBy.String) != Reconcile {
			add("idem_settled_snapshot", "SETTLED call without a snapshot, resolved by %q", e.resolvedBy.String)
		}
		return out
	}
	kept := !e.bodyNull && e.bytes.Valid && e.bodyLen.Int64 == e.bytes.Int64 && e.bytes.Int64 <= MaxSnapshotBody
	tooLarge := e.bodyNull && e.bytes.Valid && e.bytes.Int64 > MaxSnapshotBody
	if (e.callStatus != Settled && e.callStatus != UsageUnknown) || e.status.Int64 < 200 || e.status.Int64 > 299 ||
		e.callHTTPStatus != e.status || !(kept || tooLarge) {
		add("idem_snapshot", "%s call (http %s), snapshot status %d, bytes %s, body length %s",
			e.callStatus, nullInt(e.callHTTPStatus), e.status.Int64, nullInt(e.bytes), nullInt(e.bodyLen))
	}
	return out
}

func nullInt(v sql.NullInt64) string {
	if !v.Valid {
		return "NULL"
	}
	return fmt.Sprint(v.Int64)
}
