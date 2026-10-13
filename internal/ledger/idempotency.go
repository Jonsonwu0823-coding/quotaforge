package ledger

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// 幂等记录：一个账户里一个 Idempotency-Key 一行，指向这个键最近一次真正执行的调用，带着那次写回的响应快照。
//
// 并发的正确性：写记录的有带键的预留（upsert）、收尾（存快照）和清理（删过期的），三者都先锁账户的额度行，
// 所以同一账户里“读记录、判断、写记录”是串行的。预留用读已提交：查记录的普通读在拿到锁之后才开始，
// 看得到所有已提交的写；也不对不存在的行加锁。改记录的语句都按主键定位，先锁主键、再改二级索引（expires_at），
// 顺序一致。不同账户的事务不共用额度行，也不碰对方的主键行；实测（30 个账户 × 10 个过期键，清理和重新使用
// 同时进行，5 轮）没有死锁，见 PurgeIdempotency。

// IdempotencyRetention 是记录的保留期，从这个键最近一次真正执行算起。
const IdempotencyRetention = 24 * time.Hour

// MaxSnapshotBody 是快照存下的正文上限。更大的正文写入要好几秒，会超过账本操作的时限；
// 这时只记状态码和长度，同一个键再来时返回 ErrIdempotencyResponseTooLarge。
const MaxSnapshotBody = 8 << 20

// expired 是“记录已过期”的条件，查记录和清理共用。时间用数据库的。
const expired = "expires_at <= UTC_TIMESTAMP(6)"

// ErrPurgeStopped 是 PurgeIdempotency 在 ctx 取消或到期时没做完、又没有事务出错的返回：不是故障，没删完的下一轮接着删。
var ErrPurgeStopped = errors.New("purge idempotency stopped before the round was done")

// 带幂等键的预留不执行的几种情况。
var (
	ErrIdempotencyKeyReused        = errors.New("idempotency key reused with a different request")
	ErrIdempotencyInProgress       = errors.New("idempotency key: the call is still in progress")
	ErrIdempotencyOutcomeUnknown   = errors.New("idempotency key: the call's outcome is unknown")
	ErrIdempotencyResponseTooLarge = errors.New("idempotency key: the call succeeded but its response was too large to keep")
	// 上一次已经结清为 SETTLED（计过费），但没有快照：凭账单或日志结算的调用，QF 没有拿到或没有存下响应。
	ErrIdempotencyResponseUnavailable = errors.New("idempotency key: the call was settled but no response was kept")
)

// Response 是 QF 写回给客户端的上游 2xx：状态码、Content-Type 的值（上游没给时为 nil）和正文。
// Streamed 表示这是一个结算了的流式回答：内容边读边写给了客户端，QF 没有留下，只记状态码当标记，
// ContentType 和 Body 不用。正文为空的非流式 2xx 也是合法的快照，所以要显式标明，不靠正文为空来判断。
type Response struct {
	Status      int
	ContentType []string
	Body        []byte
	Streamed    bool
}

// Replay 是要重放的快照和原来的调用号。
type Replay struct {
	CallID string
	Response
}

// record 是查到的一条幂等记录。response 几列都是 NULL 表示没有写回 2xx；status 有值、body 为 NULL 时：bytes 有值是正文超过上限，
// bytes 也是 NULL 是结算了的流（流式的标记，见 Response.Streamed）。
type record struct {
	fingerprint []byte
	callID      string
	expired     bool
	callStatus  Status
	status      sql.NullInt64
	bytes       sql.NullInt64
	contentType sql.Null[[]byte]
	body        sql.Null[[]byte]
}

// reserveWithKey 按这个键已有的记录判断，要执行时预留，并让记录指向这次调用。
func reserveWithKey(ctx context.Context, tx *sql.Tx, r Reservation) (*Replay, error) {
	if err := lockBudget(ctx, tx, r.AccountID); err != nil {
		return nil, err
	}
	rec, err := readRecord(ctx, tx, r.AccountID, r.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if replay, err := rec.decide(r.Fingerprint); replay != nil || err != nil {
		return replay, err
	}
	if err := reserveIn(ctx, tx, r); err != nil {
		return nil, err
	}
	return nil, pointRecord(ctx, tx, r)
}

// readRecord 返回这个键的记录，没有时返回 nil。
func readRecord(ctx context.Context, tx *sql.Tx, accountID int64, key string) (*record, error) {
	var rec record
	err := tx.QueryRowContext(ctx, `SELECT k.fingerprint, k.call_id, k.`+expired+`, c.status,
			k.response_status, k.response_bytes, k.response_content_type, k.response_body
		FROM idempotency_keys k JOIN calls c ON c.id = k.call_id
		WHERE k.account_id = ? AND k.idem_key = ?`, accountID, key).Scan(
		&rec.fingerprint, &rec.callID, &rec.expired, &rec.callStatus, &rec.status, &rec.bytes, &rec.contentType, &rec.body)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read idempotency record: %w", err)
	}
	return &rec, nil
}

// decide 是判断表：返回 nil, nil 表示照常执行。指纹不同先于一切状态；还在处理的不看快照。
func (rec *record) decide(fingerprint []byte) (*Replay, error) {
	switch {
	case rec == nil || rec.expired:
		return nil, nil
	case !bytes.Equal(rec.fingerprint, fingerprint):
		return nil, ErrIdempotencyKeyReused
	case rec.callStatus == Reserved || rec.callStatus == InFlight: // 还在处理，或者收尾失败了、等回收
		return nil, ErrIdempotencyInProgress
	case rec.status.Valid && rec.body.Valid:
		return &Replay{CallID: rec.callID, Response: Response{
			Status: int(rec.status.Int64), ContentType: decodeContentType(rec.contentType), Body: rec.body.V}}, nil
	case rec.status.Valid && !rec.bytes.Valid: // 结算了的流：已经计费，内容没有留下
		return nil, ErrIdempotencyResponseUnavailable
	case rec.status.Valid:
		return nil, ErrIdempotencyResponseTooLarge
	case rec.callStatus == Cancelled: // 确定上游没有生成结果
		return nil, nil
	case rec.callStatus == Settled: // 已经计费，响应没有保存（凭账单或日志结算的）
		return nil, ErrIdempotencyResponseUnavailable
	default: // 发出后结果未知：上一次可能已经被上游执行并计费
		return nil, ErrIdempotencyOutcomeUnknown
	}
}

// pointRecord 让记录指向这次调用：新建，或者覆盖过期、CANCELLED 的旧记录并清掉快照。
// 用 upsert：过期的旧记录可能刚被清理删掉。这是预留事务的最后一条语句，调用行已经插好（外键）。
func pointRecord(ctx context.Context, tx *sql.Tx, r Reservation) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO idempotency_keys (account_id, idem_key, fingerprint, call_id, expires_at)
		VALUES (?, ?, ?, ?, UTC_TIMESTAMP(6) + INTERVAL ? SECOND) AS n
		ON DUPLICATE KEY UPDATE fingerprint = n.fingerprint, call_id = n.call_id, expires_at = n.expires_at,
			response_status = NULL, response_bytes = NULL, response_content_type = NULL, response_body = NULL`,
		r.AccountID, r.IdempotencyKey, r.Fingerprint, r.CallID, int64(IdempotencyRetention/time.Second))
	if err != nil {
		return fmt.Errorf("write idempotency record: %w", err)
	}
	return nil
}

// saveResponse 把写回的 2xx 存成快照（结算了的流只记状态码，见 Response.Streamed），是收尾事务的最后一条语句。WHERE 带上调用号：调用卡住期间记录过期、
// 被新请求改指向的话，旧调用的收尾影响 0 行，不会把快照写到新的记录上。
func saveResponse(ctx context.Context, tx *sql.Tx, r Reservation, resp Response) error {
	var size, contentType, body any = len(resp.Body), nil, nil // nil 存 NULL
	if resp.ContentType != nil {
		contentType = strings.Join(resp.ContentType, "\n")
	}
	switch {
	case resp.Streamed: // 只记状态码，其余三列 NULL
		size, contentType = nil, nil
	case len(resp.Body) > MaxSnapshotBody: // 只记状态码和长度
	case resp.Body == nil:
		body = "" // nil 的 []byte 会存成 NULL
	default:
		body = resp.Body
	}
	_, err := tx.ExecContext(ctx, `UPDATE idempotency_keys
		SET response_status = ?, response_bytes = ?, response_content_type = ?, response_body = ?
		WHERE account_id = ? AND idem_key = ? AND call_id = ?`,
		resp.Status, size, contentType, body, r.AccountID, r.IdempotencyKey, r.CallID)
	if err != nil {
		return fmt.Errorf("save idempotency snapshot: %w", err)
	}
	return nil
}

func decodeContentType(v sql.Null[[]byte]) []string {
	if !v.Valid {
		return nil
	}
	return strings.Split(string(v.V), "\n")
}

// purgeBatch 是清理一次取出的过期记录数。删的时候按账户分组，一个账户一个事务，持有额度行锁的时间限在这一批之内。
const purgeBatch = 100

// PurgeIdempotency 删掉过期的记录（连同快照），返回删掉的条数。可以重复执行，也可以和服务同时运行。
//
// 先用不加锁的读取出一批过期记录的主键，按账户分组；每个账户一个事务（读已提交）：先锁额度行，再逐个按主键删，
// 条件里再判断一次过期。这样清理和带键的预留、收尾同一个锁顺序（额度行 → 主键 → 二级索引），和同一账户的预留串行：
// 预留先改了指向的，清理按最新提交的版本判断，已不过期就不删。
//
// 为什么逐个按主键删（实测）：原来按 expires_at 范围一条 DELETE，或者 account_id 加 idem_key IN (…)，
// 优化器都会走 expires_at 二级索引，先锁二级索引项（其中有别的账户的）、再锁主键行，和改指向的 upsert 顺序相反，
// 清理和别的账户重新使用过期的键同时进行时会死锁（1213）。主键等值删除总是先锁主键。
// 不对 1213 重试：重试会把锁顺序的错误藏起来，检查锁顺序的测试也就失去了意义。
//
// 一个账户的事务出错（额度行被长时间锁住）只算它自己：记下来，接着处理下一个账户，不挡住别的账户。
// 一批一批地取，按 expires_at 二级索引的顺序 (expires_at, account_id, idem_key) 翻批（键集分页，见 expiredRecords），
// 下一批接在上一批最后一条之后，所以一轮里每条记录最多处理一次，一个账户一直出错也照样往后翻；
// 候选只取一轮开始时（数据库时间 t0）已经过期的：之后过期的、被重新使用的（过期时间往后挪了 24 小时）都不进这一轮，
// 候选集合只会变小，一轮一定结束。出错的下一轮从头取，会再试。整轮结束时有出错，返回错误：
// 出错的事务个数和第一个错误；已经删掉的条数照常返回。出错的事务整个回滚，它的条数不计入。
//
// 每个事务最多 opTimeout，从 ctx 脱开：ctx 取消或到期时只在两个事务之间停下，正在做的那个事务做完，后面的候选不动。
// 没做完就停下，而且没有事务出错：返回 ErrPurgeStopped（同时包着 ctx.Err()），已经删掉的条数照常返回，下一轮从头取、接着删。
// 停下之前已经有事务出错的：返回的是上一段说的那个错误，出错的报告不因为停下而丢掉。
// 一个账户的额度行一直被锁住时，一轮在它的每一批上各等 opTimeout。
func (s *Store) PurgeIdempotency(ctx context.Context, opTimeout time.Duration) (int64, error) {
	t0, err := s.now(ctx, opTimeout)
	if err != nil {
		return 0, err
	}
	var total int64
	var failed int
	var first error
	stopped := false
	cursor := expiredKey{expires: minDatetime} // 比任何记录都早：第一批从头取
	for !stopped {
		batch, err := s.expiredRecords(ctx, opTimeout, t0, cursor)
		if err != nil {
			return total, err
		}
		for _, g := range byAccount(batch) {
			if ctx.Err() != nil {
				stopped = true
				break
			}
			n, err := s.purgeAccount(ctx, g, opTimeout)
			if err != nil {
				if failed++; first == nil {
					first = err
				}
				continue
			}
			total += n
		}
		if len(batch) < purgeBatch {
			break
		}
		cursor = batch[len(batch)-1]
	}
	switch {
	case failed > 0:
		return total, fmt.Errorf("purge idempotency: %d transactions failed, first: %w", failed, first)
	case stopped:
		return total, fmt.Errorf("%w: %w", ErrPurgeStopped, ctx.Err())
	}
	return total, nil
}

// now 是数据库的当前时间，DATETIME(6) 的文本，原样传回数据库比较。过期按数据库的时间判断，不用 QF 的时钟。
func (s *Store) now(ctx context.Context, opTimeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	var now string
	if err := s.db.QueryRowContext(ctx, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')").Scan(&now); err != nil {
		return "", fmt.Errorf("read database time: %w", err)
	}
	return now, nil
}

// expiredKey 是一条过期记录在 expires_at 索引里的位置（过期时间和主键），也是翻页的游标。
// expires 是 DATETIME(6) 的文本，原样传回数据库比较。
type expiredKey struct {
	expires string
	account int64
	key     string
}

// expiredKeys 是一个账户的一批过期记录。
type expiredKeys struct {
	account int64
	keys    []string
}

// expiredRecords 不加锁地取出游标之后、t0 之前过期的一批记录，按 expires_at 索引的顺序。
// 二级索引的项里带着主键，所以这个索引的顺序就是 (expires_at, account_id, idem_key)：只读过期的项、不用排序，
// 没过期的再多也读不到（实测）。比较要展开成 OR：行构造器 (a, b, c) > (?, ?, ?) 只被当成 a 上的范围，
// 每批都要从头扫过游标之前的项；按主键 (account_id, idem_key) 翻批时，过期的多就从主键开头扫，过期的少就把全部过期项排一遍。
func (s *Store) expiredRecords(ctx context.Context, opTimeout time.Duration, t0 string, after expiredKey) ([]expiredKey, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT DATE_FORMAT(expires_at, '%Y-%m-%d %H:%i:%s.%f'), account_id, idem_key
		FROM idempotency_keys
		WHERE expires_at <= ? AND (expires_at > ? OR (expires_at = ? AND (account_id > ? OR (account_id = ? AND idem_key > ?))))
		ORDER BY expires_at, account_id, idem_key LIMIT ?`,
		t0, after.expires, after.expires, after.account, after.account, after.key, purgeBatch)
	if err != nil {
		return nil, fmt.Errorf("find expired idempotency records: %w", err)
	}
	defer rows.Close()
	var out []expiredKey
	for rows.Next() {
		var k expiredKey
		if err := rows.Scan(&k.expires, &k.account, &k.key); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// byAccount 把一批记录按账户分组，组内按键排序：每个账户一个事务，逐个按主键删，顺序和主键一致。
func byAccount(batch []expiredKey) []expiredKeys {
	sorted := slices.Clone(batch)
	slices.SortFunc(sorted, func(a, b expiredKey) int {
		return cmp.Or(cmp.Compare(a.account, b.account), strings.Compare(a.key, b.key))
	})
	var out []expiredKeys
	for _, k := range sorted {
		if len(out) == 0 || out[len(out)-1].account != k.account {
			out = append(out, expiredKeys{account: k.account})
		}
		out[len(out)-1].keys = append(out[len(out)-1].keys, k.key)
	}
	return out
}

// purgeAccount 在一个事务里删掉一个账户的这批过期记录：先锁额度行，再逐个按主键删。
func (s *Store) purgeAccount(ctx context.Context, batch expiredKeys, opTimeout time.Duration) (int64, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	var n int64
	err := s.inTx(ctx, readCommitted, func(tx *sql.Tx) error {
		if err := lockBudget(ctx, tx, batch.account); err != nil {
			return err
		}
		for _, key := range batch.keys {
			res, err := tx.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE account_id = ? AND idem_key = ? AND "+expired,
				batch.account, key)
			if err != nil {
				return fmt.Errorf("purge idempotency records: %w", err)
			}
			m, err := res.RowsAffected()
			if err != nil {
				return err
			}
			n += m
		}
		return nil
	})
	return n, err
}
