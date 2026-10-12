package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// timeFormat 把 DATETIME(6)（UTC）写成 RFC 3339、微秒、带 Z。在 SQL 里格式化，不依赖连接串的 parseTime。
const timeFormat = "'%Y-%m-%dT%H:%i:%s.%fZ'"

func formatted(col string) string { return "DATE_FORMAT(" + col + ", " + timeFormat + ")" }

// CallInfo 是一个调用的公开信息，给调用状态查询用：没有请求体、响应体、快照、幂等键和 Key。
// 时间是 RFC 3339 的字符串；空串、0、nil 表示库里是 NULL。
type CallInfo struct {
	ID, Purpose, Alias string
	Status             Status
	CreatedAt          string
	SentAt, FinishedAt string
	Usage              *Usage
	HTTPStatus         int
	ErrorCode          string
	ResolvedBy         Actor
	Evidence           Evidence
	ResolvedAt         string
}

// Call 按调用号查一个属于 accountID 的调用。不存在、或者属于别的账户，都返回 nil, nil：调用方不能分辨这两种情况。
func (s *Store) Call(ctx context.Context, accountID int64, id string) (*CallInfo, error) {
	var c CallInfo
	var sent, finished, resolvedAt, errorCode, resolvedBy, evidence sql.NullString
	var prompt, completion, total, httpStatus sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT id, purpose, model_alias, status, `+formatted("created_at")+`, `+formatted("sent_at")+`,
			`+formatted("finished_at")+`, usage_prompt_tokens, usage_completion_tokens, usage_total_tokens, http_status, error_code,
			resolved_by, resolution_evidence, `+formatted("resolved_at")+`
		FROM calls WHERE id = ? AND account_id = ?`, id, accountID).Scan(
		&c.ID, &c.Purpose, &c.Alias, &c.Status, &c.CreatedAt, &sent, &finished, &prompt, &completion, &total, &httpStatus,
		&errorCode, &resolvedBy, &evidence, &resolvedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("look up call: %w", err)
	}
	c.SentAt, c.FinishedAt, c.ResolvedAt, c.ErrorCode = sent.String, finished.String, resolvedAt.String, errorCode.String
	c.ResolvedBy, c.Evidence = Actor(resolvedBy.String), Evidence(evidence.String)
	c.Usage = scanUsage(prompt, completion, total)
	c.HTTPStatus = int(httpStatus.Int64)
	return &c, nil
}

// UnknownCall 是一个用量未知、还占着预留的调用：运维拿这张单去向上游要账单。
type UnknownCall struct {
	ID, Account, CreatedAt  string
	Purpose, Alias          string
	Upstream, UpstreamModel string
	Reserved                int64
	ErrorCode               string
}

// UnknownCalls 列出 USAGE_UNKNOWN 的调用，按创建时间排序；account 为空时列出全部账户的。
func (s *Store) UnknownCalls(ctx context.Context, account string) ([]UnknownCall, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT c.id, a.name, `+formatted("c.created_at")+`, c.purpose, c.model_alias,
			c.upstream, c.upstream_model, c.reserved_tokens, COALESCE(c.error_code, '')
		FROM calls c JOIN accounts a ON a.id = c.account_id
		WHERE c.status = 'USAGE_UNKNOWN' AND (? = '' OR a.name = ?) ORDER BY c.created_at, c.id`, account, account)
	if err != nil {
		return nil, fmt.Errorf("unknown calls: %w", err)
	}
	defer rows.Close()
	var out []UnknownCall
	for rows.Next() {
		var u UnknownCall
		if err := rows.Scan(&u.ID, &u.Account, &u.CreatedAt, &u.Purpose, &u.Alias, &u.Upstream, &u.UpstreamModel,
			&u.Reserved, &u.ErrorCode); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
