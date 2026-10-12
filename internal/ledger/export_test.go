package ledger

import (
	"context"
	"strconv"
	"time"
)

// UnknownBacklogQuery 给外部测试看 UnknownBacklog 用的查询（核对它走的索引）。
const UnknownBacklogQuery = unknownBacklogQuery

// PurgeBatches 给外部测试看清理怎么翻批：一轮开始时的前两批候选，每条是“账户号/键”。
func (s *Store) PurgeBatches(ctx context.Context) (first, second []string, err error) {
	t0, err := s.now(ctx, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	cursor := expiredKey{expires: minDatetime}
	for i, out := range []*[]string{&first, &second} {
		batch, err := s.expiredRecords(ctx, 2*time.Second, t0, cursor)
		if err != nil || len(batch) == 0 {
			return first, second, err
		}
		*out = append(*out, names(batch)...)
		if i == 0 {
			cursor = batch[len(batch)-1]
		}
	}
	return first, second, nil
}

// PurgeFirstBatch 把清理的第一批拆成两步，给测试在“取到候选”和“删除”之间插入别的操作：
// 返回这一批的“账户号/键”，和删掉这一批的函数（每个账户一个事务，和 PurgeIdempotency 一样）。
func (s *Store) PurgeFirstBatch(ctx context.Context) ([]string, func() (int64, error), error) {
	t0, err := s.now(ctx, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	batch, err := s.expiredRecords(ctx, 2*time.Second, t0, expiredKey{expires: minDatetime})
	if err != nil {
		return nil, nil, err
	}
	purge := func() (int64, error) {
		var total int64
		for _, g := range byAccount(batch) {
			n, err := s.purgeAccount(ctx, g, 2*time.Second)
			if err != nil {
				return total, err
			}
			total += n
		}
		return total, nil
	}
	return names(batch), purge, nil
}

func names(batch []expiredKey) []string {
	out := make([]string, len(batch))
	for i, k := range batch {
		out[i] = strconv.FormatInt(k.account, 10) + "/" + k.key
	}
	return out
}

// OneConnection 把连接池缩成 1 个连接：一条语句失败之后，下一条语句只能复用出错的那个连接。
func OneConnection(s *Store) {
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
}

// SessionState 读连接池里那个连接的会话状态。唯一的连接还被没回滚的事务占着时，查询会超时，而不是一直等。
func SessionState(s *Store) (conn int64, lockWait, autocommit int, isolation string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = s.db.QueryRowContext(ctx, "SELECT CONNECTION_ID(), @@session.innodb_lock_wait_timeout, @@autocommit, @@session.transaction_isolation").
		Scan(&conn, &lockWait, &autocommit, &isolation)
	return
}
