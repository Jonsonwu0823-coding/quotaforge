package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
)

// 管理操作给 qfctl 用。账户名、额度、别名在这里校验一次：这是管理员输入进库的边界。
// 限流的限额（Rate）由 qfctl 按 config.ValidRate 校验，和配置文件是同一条规则，这里不再校验。

var accountName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// 别名进 calls.model_alias（VARCHAR(255)），和配置里的别名同一个上限。
const maxAliasLen = 255

// keyPrefixLen 是存下来用于辨认的前缀长度：qfk_ 加 8 个字符，远不够猜出 Key。
const keyPrefixLen = 12

var (
	ErrAccountNotFound = errors.New("account not found")
	ErrAccountExists   = errors.New("account already exists")
	ErrKeyNotFound     = errors.New("key not found or already revoked")
)

func checkAccount(name string) error {
	if !accountName.MatchString(name) {
		return fmt.Errorf("account name must match %s", accountName)
	}
	return nil
}

func checkLimit(limit int64) error {
	if limit < 0 {
		return errors.New("limit must not be negative")
	}
	return nil
}

// CreateAccount 建账户和它的额度行，连同账户的限流限额。
func (s *Store) CreateAccount(ctx context.Context, name string, limit int64, rate Rate) error {
	if err := checkAccount(name); err != nil {
		return err
	}
	if err := checkLimit(limit); err != nil {
		return err
	}
	return s.inTx(ctx, nil, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO accounts (name, rpm_limit, tpm_limit, created_at)
			VALUES (?, ?, ?, UTC_TIMESTAMP(6))`, name, rate.RPM, rate.TPM)
		if isMySQLError(err, 1062) { // ER_DUP_ENTRY
			return ErrAccountExists
		}
		if err != nil {
			return fmt.Errorf("insert account: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO budgets (account_id, limit_tokens, updated_at) VALUES (?, ?, UTC_TIMESTAMP(6))", id, limit)
		return err
	})
}

// SetLimit 改账户的额度上限。已预留、已消费不变；新上限低于已用量时，之后的预留都会被拒。
func (s *Store) SetLimit(ctx context.Context, name string, limit int64) error {
	if err := checkLimit(limit); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE budgets b JOIN accounts a ON a.id = b.account_id
		SET b.limit_tokens = ?, b.updated_at = UTC_TIMESTAMP(6) WHERE a.name = ?`, limit, name)
	return oneRow(res, err, "set limit", ErrAccountNotFound)
}

// SetAccountRate 改账户的限流限额，下一个请求就按新值。
func (s *Store) SetAccountRate(ctx context.Context, name string, rate Rate) error {
	res, err := s.db.ExecContext(ctx, "UPDATE accounts SET rpm_limit = ?, tpm_limit = ? WHERE name = ?", rate.RPM, rate.TPM, name)
	return oneRow(res, err, "set account rate", ErrAccountNotFound)
}

// IssuedKey 是刚发出的 Key。Secret 只在这里出现一次，库里只存哈希和前缀。
type IssuedKey struct {
	ID     int64
	Prefix string
	Secret string
}

// IssueKey 给账户发一把新 Key，允许使用 models 里的别名，连同 Key 的限流限额。
// 别名不对照 QF 的配置：写错的别名只会让请求得到 404。
func (s *Store) IssueKey(ctx context.Context, account string, models []string, rate Rate) (*IssuedKey, error) {
	if len(models) == 0 {
		return nil, errors.New("at least one model alias is required")
	}
	seen := map[string]bool{}
	for _, m := range models {
		if m == "" || utf8.RuneCountInString(m) > maxAliasLen || seen[m] {
			return nil, fmt.Errorf("model aliases must be non-empty, unique and at most %d characters", maxAliasLen)
		}
		seen[m] = true
	}
	list, err := json.Marshal(models)
	if err != nil {
		return nil, err
	}
	key := apikey.Generate()
	res, err := s.db.ExecContext(ctx, `INSERT INTO api_keys (account_id, sha256, prefix, models, status, rpm_limit, tpm_limit, created_at)
		SELECT id, ?, ?, ?, 'active', ?, ?, UTC_TIMESTAMP(6) FROM accounts WHERE name = ?`,
		apikey.Hash(key), key[:keyPrefixLen], list, rate.RPM, rate.TPM, account)
	if err := oneRow(res, err, "insert key", ErrAccountNotFound); err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return &IssuedKey{ID: id, Prefix: key[:keyPrefixLen], Secret: key}, nil
}

// RevokeKey 吊销一把 Key，立即生效（鉴权每次都查库）。
func (s *Store) RevokeKey(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE api_keys SET status = 'revoked', revoked_at = UTC_TIMESTAMP(6)
		WHERE id = ? AND status = 'active'`, id)
	return oneRow(res, err, "revoke key", ErrKeyNotFound)
}

// SetKeyRate 改一把可用的 Key 的限流限额，下一个请求就按新值。不存在或已吊销返回 ErrKeyNotFound。
func (s *Store) SetKeyRate(ctx context.Context, id int64, rate Rate) error {
	res, err := s.db.ExecContext(ctx, "UPDATE api_keys SET rpm_limit = ?, tpm_limit = ? WHERE id = ? AND status = 'active'",
		rate.RPM, rate.TPM, id)
	return oneRow(res, err, "set key rate", ErrKeyNotFound)
}

// decodeModels 解出 api_keys.models 列：Key 允许的别名，JSON 数组。
func decodeModels(keyID int64, raw []byte) ([]string, error) {
	var models []string
	if err := json.Unmarshal(raw, &models); err != nil {
		return nil, fmt.Errorf("key %d: models column: %w", keyID, err)
	}
	return models, nil
}

// KeyInfo 是 Key 的公开信息，不含 Key 本身。
type KeyInfo struct {
	ID     int64
	Prefix string
	Status string
	Models []string
	Rate   Rate
}

// ListKeys 列出账户的全部 Key。
func (s *Store) ListKeys(ctx context.Context, account string) ([]KeyInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT k.id, k.prefix, k.status, k.models, k.rpm_limit, k.tpm_limit
		FROM api_keys k JOIN accounts a ON a.id = k.account_id WHERE a.name = ? ORDER BY k.id`, account)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer rows.Close()
	var out []KeyInfo
	for rows.Next() {
		var k KeyInfo
		var models []byte
		if err := rows.Scan(&k.ID, &k.Prefix, &k.Status, &models, &k.Rate.RPM, &k.Rate.TPM); err != nil {
			return nil, err
		}
		if k.Models, err = decodeModels(k.ID, models); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// AccountUsage 是一个账户的额度、各状态的调用数和账户的限流限额。Remaining 可以是负数（超用）。
type AccountUsage struct {
	Account                   string
	Limit, Reserved, Consumed int64
	Remaining                 int64
	Calls                     map[Status]int64
	Rate                      Rate
}

// Usage 返回账户的用量；account 为空时返回全部账户，按名字排序。
func (s *Store) Usage(ctx context.Context, account string) ([]AccountUsage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.name, b.limit_tokens, b.reserved_tokens, b.consumed_tokens, a.rpm_limit, a.tpm_limit,
			COALESCE(SUM(c.status = 'RESERVED'), 0), COALESCE(SUM(c.status = 'SETTLED'), 0),
			COALESCE(SUM(c.status = 'CANCELLED'), 0), COALESCE(SUM(c.status = 'USAGE_UNKNOWN'), 0),
				COALESCE(SUM(c.status = 'IN_FLIGHT'), 0), COALESCE(SUM(c.status = 'CACHED'), 0)
		FROM accounts a JOIN budgets b ON b.account_id = a.id LEFT JOIN calls c ON c.account_id = a.id
		WHERE ? = '' OR a.name = ?
		GROUP BY a.id, a.name, b.limit_tokens, b.reserved_tokens, b.consumed_tokens, a.rpm_limit, a.tpm_limit ORDER BY a.name`, account, account)
	if err != nil {
		return nil, fmt.Errorf("usage: %w", err)
	}
	defer rows.Close()
	var out []AccountUsage
	for rows.Next() {
		var u AccountUsage
		var reserved, settled, cancelled, unknown, inFlight, cached int64
		if err := rows.Scan(&u.Account, &u.Limit, &u.Reserved, &u.Consumed, &u.Rate.RPM, &u.Rate.TPM, &reserved, &settled, &cancelled, &unknown, &inFlight, &cached); err != nil {
			return nil, err
		}
		u.Remaining = u.Limit - u.Reserved - u.Consumed
		u.Calls = map[Status]int64{Reserved: reserved, InFlight: inFlight, Settled: settled, Cancelled: cancelled, UsageUnknown: unknown, Cached: cached}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if account != "" && len(out) == 0 {
		return nil, ErrAccountNotFound
	}
	return out, nil
}
