package ledger_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// keyed 是带幂等键的预留；指纹由 request 算出，测试里只需要“相同”和“不同”。
func (f *fixture) keyed(amount int64, key, request string) ledger.Reservation {
	r := f.reservation(amount)
	sum := sha256.Sum256([]byte(request))
	r.IdempotencyKey, r.Fingerprint = key, sum[:]
	return r
}

// settleWith 照网关的顺序结算一个 RESERVED 的调用，写回 resp：先落发送意图，再收尾。
func (f *fixture) settleWith(t *testing.T, r ledger.Reservation, resp ledger.Response) {
	t.Helper()
	f.sent(t, r)
	err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, Usage: &ledger.Usage{Prompt: 1, Total: 1},
		HTTPStatus: resp.Status, Response: &resp})
	if err != nil {
		t.Fatal(err)
	}
}

// expire 把这个账户的记录改成已过期。
func (f *fixture) expire(t *testing.T, key string) {
	t.Helper()
	if _, err := f.admin.Exec(`UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
		WHERE account_id = ? AND idem_key = ?`, f.key.AccountID, key); err != nil {
		t.Fatal(err)
	}
}

// 快照原样存取：Content-Type 没有、空值、多个值；正文为空、二进制、非法 UTF-8、正好上限。
func TestSnapshotRoundTrip(t *testing.T) {
	f := newFixture(t, 1<<40)
	for i, resp := range []ledger.Response{
		{Status: 200, ContentType: nil, Body: []byte(`{"a":1}`)},
		{Status: 200, ContentType: []string{""}, Body: []byte{}},
		{Status: 201, ContentType: []string{"application/json", "text/plain; charset=gbk"}, Body: []byte{0, 0xff, 0xfe, '\n', 0x80}},
		{Status: 299, ContentType: []string{"application/json"}, Body: bytes.Repeat([]byte("x"), ledger.MaxSnapshotBody)},
	} {
		key := "round-trip-" + string(rune('a'+i))
		r := f.keyed(10, key, "req")
		if replay, err := f.s.Reserve(ctx, r); replay != nil || err != nil {
			t.Fatalf("first Reserve = %v, %v", replay, err)
		}
		f.settleWith(t, r, resp)
		replay, err := f.s.Reserve(ctx, f.keyed(10, key, "req"))
		if err != nil || replay == nil {
			t.Fatalf("%d: replay = %v, %v", i, replay, err)
		}
		if replay.CallID != r.CallID || replay.Status != resp.Status || !slices.Equal(replay.ContentType, resp.ContentType) ||
			(replay.ContentType == nil) != (resp.ContentType == nil) || !bytes.Equal(replay.Body, resp.Body) {
			t.Fatalf("%d: replay = %s %d %q (%d bytes), want %d %q (%d bytes)", i, replay.CallID, replay.Status,
				replay.ContentType, len(replay.Body), resp.Status, resp.ContentType, len(resp.Body))
		}
	}
	dbtest.Verify(t, f.s)
}

// 正文超过上限：照常结算，只记状态码和长度；同一个键再来返回 ErrIdempotencyResponseTooLarge。
func TestSnapshotTooLarge(t *testing.T) {
	f := newFixture(t, 1<<40)
	r := f.keyed(10, "big", "req")
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.settleWith(t, r, ledger.Response{Status: 200, Body: bytes.Repeat([]byte("x"), ledger.MaxSnapshotBody+1)})
	var status, size int64
	var bodyNull bool
	if err := f.admin.QueryRow(`SELECT response_status, response_bytes, response_body IS NULL FROM idempotency_keys
		WHERE account_id = ? AND idem_key = 'big'`, f.key.AccountID).Scan(&status, &size, &bodyNull); err != nil {
		t.Fatal(err)
	}
	if status != 200 || size != ledger.MaxSnapshotBody+1 || !bodyNull {
		t.Fatalf("record = %d %d body null %v", status, size, bodyNull)
	}
	if _, err := f.s.Reserve(ctx, f.keyed(10, "big", "req")); !errors.Is(err, ledger.ErrIdempotencyResponseTooLarge) {
		t.Fatalf("retry = %v", err)
	}
	if _, reserved, consumed := f.budget(t); reserved != 0 || consumed != 1 {
		t.Fatalf("reserved, consumed = %d, %d, want 0, 1", reserved, consumed)
	}
	dbtest.Verify(t, f.s)
}

// 结算了的流：记录里只记状态码当标记（其余三列 NULL），不存正文；同一个键再来返回 ErrIdempotencyResponseUnavailable，
// 不执行、不预留；账本校验认这个标记。
func TestStreamedMarker(t *testing.T) {
	f := newFixture(t, 1<<40)
	r := f.keyed(10, "stream", "req")
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.settleWith(t, r, ledger.Response{Status: 200, ContentType: []string{"text/event-stream"}, Body: []byte("ignored"), Streamed: true})
	var status int64
	var nulls int
	if err := f.admin.QueryRow(`SELECT response_status, (response_bytes IS NULL) + (response_content_type IS NULL) + (response_body IS NULL)
		FROM idempotency_keys WHERE account_id = ? AND idem_key = 'stream'`, f.key.AccountID).Scan(&status, &nulls); err != nil {
		t.Fatal(err)
	}
	if status != 200 || nulls != 3 {
		t.Fatalf("record: status %d, %d of the other three columns are NULL", status, nulls)
	}
	if _, err := f.s.Reserve(ctx, f.keyed(10, "stream", "req")); !errors.Is(err, ledger.ErrIdempotencyResponseUnavailable) {
		t.Fatalf("retry = %v", err)
	}
	if _, reserved, consumed := f.budget(t); reserved != 0 || consumed != 1 {
		t.Fatalf("reserved, consumed = %d, %d, want 0, 1", reserved, consumed)
	}
	dbtest.Verify(t, f.s)
}

// 调用卡在 RESERVED 期间记录过期、被新请求改指向：旧调用收尾时不能把快照写到新的记录上。
func TestLateFinishDoesNotOverwriteRepointedRecord(t *testing.T) {
	f := newFixture(t, 1<<40)
	old := f.keyed(10, "k", "old request")
	if _, err := f.s.Reserve(ctx, old); err != nil {
		t.Fatal(err)
	}
	f.expire(t, "k")
	next := f.keyed(10, "k", "new request")
	if replay, err := f.s.Reserve(ctx, next); replay != nil || err != nil {
		t.Fatalf("Reserve after expiry = %v, %v", replay, err)
	}
	f.settleWith(t, next, ledger.Response{Status: 200, Body: []byte("new response")})
	f.settleWith(t, old, ledger.Response{Status: 200, Body: []byte("old response")})
	replay, err := f.s.Reserve(ctx, f.keyed(10, "k", "new request"))
	if err != nil || replay == nil || replay.CallID != next.CallID || string(replay.Body) != "new response" {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if n := f.count(t, "SELECT COUNT(*) FROM calls WHERE account_id = ? AND idempotency_key = 'k' AND status = 'SETTLED'", f.key.AccountID); n != 2 {
		t.Fatalf("%d settled calls with the key, want 2", n)
	}
	dbtest.Verify(t, f.s)
}

// 保留期是 24 小时，从最近一次真正执行算起，用数据库的时间：新建的记录和过期后改指向的记录都是。
func TestIdempotencyRetention(t *testing.T) {
	f := newFixture(t, 1<<40)
	remaining := func() int64 {
		t.Helper()
		var s int64
		if err := f.admin.QueryRow(`SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), expires_at) FROM idempotency_keys
			WHERE account_id = ? AND idem_key = 'k'`, f.key.AccountID).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	const day = int64(24 * 60 * 60)
	if _, err := f.s.Reserve(ctx, f.keyed(1, "k", "req")); err != nil {
		t.Fatal(err)
	}
	if s := remaining(); s < day-60 || s > day {
		t.Fatalf("new record expires in %d s, want 24 h", s)
	}
	f.expire(t, "k")
	if _, err := f.s.Reserve(ctx, f.keyed(1, "k", "req")); err != nil {
		t.Fatal(err)
	}
	if s := remaining(); s < day-60 || s > day {
		t.Fatalf("re-pointed record expires in %d s, want 24 h", s)
	}
}

// 清理只删过期的；一条 DELETE 最多删一批，多于一批也全删掉；再跑一遍删 0 条。
func TestPurgeIdempotency(t *testing.T) {
	f := newFixture(t, 1<<40)
	const expiredN = 250
	for i := range expiredN + 3 {
		r := f.keyed(1, fmt.Sprintf("k%03d", i), "req")
		if _, err := f.s.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		f.settleWith(t, r, ledger.Response{Status: 200, Body: []byte("x")})
	}
	if _, err := f.admin.Exec(`UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
		WHERE account_id = ? ORDER BY idem_key LIMIT ?`, f.key.AccountID, expiredN); err != nil {
		t.Fatal(err)
	}
	// 别的测试可能留下过期记录：只核对这个账户的。
	n, err := f.s.PurgeIdempotency(ctx, 2*time.Second)
	if err != nil || n < expiredN {
		t.Fatalf("purge = %d, %v, want at least %d", n, err, expiredN)
	}
	if left := f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", f.key.AccountID); left != 3 {
		t.Fatalf("%d records left, want the 3 unexpired", left)
	}
	if n, err := f.s.PurgeIdempotency(ctx, 2*time.Second); err != nil || n != 0 {
		t.Fatalf("second purge = %d, %v", n, err)
	}
	dbtest.Verify(t, f.s)
}

// 清理按 expires_at 索引的顺序 (expires_at, account_id, idem_key) 翻批：150 个过期的键，过期顺序和主键顺序相反，
// 第一批正好是最早过期的 100 个，第二批接着是剩下的 50 个（按主键翻批的老查询取到的是主键最小的 100 个）。
// 过期时间放在 2001 年：比别的测试留下的过期键都早，前两批里先出现的就是这个账户的。之后一轮全部删掉。
func TestPurgePagesInExpiryOrder(t *testing.T) {
	f := newFixture(t, 1<<40)
	f.expiredKeys(t, 150)
	if _, err := f.admin.Exec(`UPDATE idempotency_keys SET expires_at = TIMESTAMP('2001-01-01') + INTERVAL 150 - CAST(SUBSTRING(idem_key, 2) AS UNSIGNED) SECOND
		WHERE account_id = ?`, f.key.AccountID); err != nil {
		t.Fatal(err)
	}
	var want []string // 按过期时间：e149 最早，e000 最晚
	for i := 149; i >= 0; i-- {
		want = append(want, fmt.Sprintf("%d/e%03d", f.key.AccountID, i))
	}
	first, second, err := f.s.PurgeBatches(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first, want[:100]) || len(second) < 50 || !slices.Equal(second[:50], want[100:]) {
		t.Fatalf("batches:\n%v\n%v\nwant\n%v", first, second, want)
	}
	before := f.expiredNow(t)
	if n, err := f.s.PurgeIdempotency(ctx, 2*time.Second); err != nil || int(n) != before {
		t.Fatalf("purge = %d, %v, want the %d expired records", n, err, before)
	}
	if left := f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", f.key.AccountID); left != 0 {
		t.Fatalf("%d records left", left)
	}
	dbtest.Verify(t, f.s)
}

// 清理先用不加锁的读取候选，删除时在事务里再判断一次过期：取到候选之后、删除之前，同一个键被重新使用了
// （记录改成指向新调用、过期时间往后挪 24 小时），删这一批时不能删掉它。少了这一条，会删掉一条正在用的记录。
func TestPurgeSkipsKeyReusedAfterTheScan(t *testing.T) {
	f := newFixture(t, 1<<40)
	f.expiredKeys(t, 1)
	if _, err := f.admin.Exec("UPDATE idempotency_keys SET expires_at = TIMESTAMP('2001-01-01') WHERE account_id = ?", f.key.AccountID); err != nil {
		t.Fatal(err)
	}
	keys, purge, err := f.s.PurgeFirstBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("%d/e000", f.key.AccountID); !slices.Contains(keys, want) {
		t.Fatalf("first batch %v does not have %s", keys, want)
	}
	r := f.keyed(1, "e000", "req") // 同一个键、同样的请求：记录已过期，照常执行
	if _, err := f.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := purge(); err != nil {
		t.Fatal(err)
	}
	var callID string
	var expired bool
	err = f.admin.QueryRow("SELECT call_id, expires_at <= UTC_TIMESTAMP(6) FROM idempotency_keys WHERE account_id = ? AND idem_key = 'e000'",
		f.key.AccountID).Scan(&callID, &expired)
	if err != nil || callID != r.CallID || expired {
		t.Fatalf("after the purge: call %q (want %q), expired %v, err %v: the reused record must stay", callID, r.CallID, expired, err)
	}
	f.finish(t, r, ledger.Cancelled, 0)
	dbtest.Verify(t, f.s)
}

// 一轮只取开始时（数据库时间 t0）已经过期的记录：候选集合只会变小，一轮一定结束。
// 另一个账户有 100 个 2001 年过期的键，额度行被锁住：第一批正好是它们，这个账户的事务要等满 opTimeout（1.5 秒）才失败。
// 这个测试的键在 t0 之后约 0.5 秒才过期，第二批的查询在 1.5 秒之后，那时它已经过期了，但不能进这一轮。放锁后下一轮才删掉它。
func TestPurgeRoundTakesOnlyKeysExpiredAtItsStart(t *testing.T) {
	blocked, late := newFixture(t, 1<<40), newFixture(t, 1<<40)
	blocked.expiredKeys(t, 100)
	if _, err := blocked.admin.Exec(`UPDATE idempotency_keys SET expires_at = TIMESTAMP('2001-01-01') + INTERVAL CAST(SUBSTRING(idem_key, 2) AS UNSIGNED) SECOND
		WHERE account_id = ?`, blocked.key.AccountID); err != nil {
		t.Fatal(err)
	}
	late.expiredKeys(t, 1)
	release := blocked.holdBudget(t)
	if _, err := late.admin.Exec("UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) + INTERVAL 500000 MICROSECOND WHERE account_id = ?",
		late.key.AccountID); err != nil {
		t.Fatal(err)
	}
	if _, err := late.s.PurgeIdempotency(ctx, 1500*time.Millisecond); err == nil {
		t.Fatal("the round succeeded while the blocked account's budget row was locked")
	}
	if n := late.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ? AND expires_at <= UTC_TIMESTAMP(6)", late.key.AccountID); n != 1 {
		t.Fatalf("%d expired records left, want the one that expired during the round", n)
	}
	release()
	if _, err := late.s.PurgeIdempotency(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*fixture{blocked, late} {
		if n := f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", f.key.AccountID); n != 0 {
			t.Fatalf("%d records left after the next round", n)
		}
	}
	dbtest.Verify(t, late.s)
}

// expiredKeys 给这个账户造 n 个已经过期的键（预留后释放，和网关没发出时一样）。
func (f *fixture) expiredKeys(t *testing.T, n int) {
	t.Helper()
	for i := range n {
		r := f.keyed(1, fmt.Sprintf("e%03d", i), "req")
		if _, err := f.s.Reserve(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := f.s.Finish(ctx, r, ledger.Result{Status: ledger.Cancelled, Evidence: ledger.NotSent}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.admin.Exec(`UPDATE idempotency_keys SET expires_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE account_id = ?`, f.key.AccountID); err != nil {
		t.Fatal(err)
	}
}

// expiredNow 数整库现在已过期的记录。清理返回的条数要等于它前后的差：不依赖库里本来有多少，别的账户的过期键也算得进去。
func (f *fixture) expiredNow(t *testing.T) int {
	t.Helper()
	return f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE expires_at <= UTC_TIMESTAMP(6)")
}

// 一个账户的额度行被锁住（超过 opTimeout）：它的清理事务出错，别的账户的过期键照常删掉，
// 错误带出错的个数和锁等待的原因；放开之后下一轮删掉剩下的。
func TestPurgeBlockedAccount(t *testing.T) {
	stuck, other := newFixture(t, 1<<40), newFixture(t, 1<<40)
	stuck.expiredKeys(t, 2)
	other.expiredKeys(t, 3)

	release := stuck.holdBudget(t)
	before := stuck.expiredNow(t)
	n, err := stuck.s.PurgeIdempotency(ctx, shortTimeout)
	if err == nil || !strings.HasPrefix(err.Error(), "purge idempotency: 1 transactions failed, first: ") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("purge while the budget row is locked = %d, %v, want 1 failed transaction, wrapping the lock wait", n, err)
	}
	if after := stuck.expiredNow(t); int(n) != before-after {
		t.Fatalf("purge reported %d, the expired records went from %d to %d", n, before, after)
	}
	if n < 3 { // 共用库里别的账户也可能有刚过期的键，所以只要求不少于这个账户的 3 个，具体看下面各账户剩下的
		t.Fatalf("purged %d, want at least the other account's 3", n)
	}
	if left := other.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", other.key.AccountID); left != 0 {
		t.Errorf("%d expired keys left in the other account", left)
	}
	if left := stuck.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", stuck.key.AccountID); left != 2 {
		t.Errorf("%d keys left in the blocked account, want 2 untouched", left)
	}

	release()
	if n, err := stuck.s.PurgeIdempotency(ctx, opTimeout); err != nil || n < 2 {
		t.Fatalf("next round = %d, %v, want at least the blocked account's 2", n, err)
	}
	if left := stuck.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", stuck.key.AccountID); left != 0 {
		t.Errorf("%d keys left in the formerly blocked account", left)
	}
	dbtest.Verify(t, stuck.s)
}

// 一个账户的过期记录多于一批、它的事务又一直出错：每一批都出错，也往后翻，整轮会结束，排在它后面的账户照样清理。
func TestPurgePagesPastFailingAccount(t *testing.T) {
	// 多于一批（purgeBatch = 100）：第二批里有它剩下的 50 个，还有别的账户的。账户号 stuck < other，other 的键排在后面。
	const keys = 150
	stuck, other := newFixture(t, 1<<40), newFixture(t, 1<<40)
	stuck.expiredKeys(t, keys)
	other.expiredKeys(t, 1)

	release := stuck.holdBudget(t)
	before := stuck.expiredNow(t)
	var n int64
	var err error
	within(t, 30*time.Second, func() { n, err = stuck.s.PurgeIdempotency(ctx, shortTimeout) })
	if err == nil || !strings.HasPrefix(err.Error(), "purge idempotency: 2 transactions failed, first: ") {
		t.Fatalf("purge = %d, %v, want 2 failed transactions (one per batch)", n, err)
	}
	if after := stuck.expiredNow(t); int(n) != before-after {
		t.Fatalf("purge reported %d, the expired records went from %d to %d", n, before, after)
	}
	if n < 1 {
		t.Fatalf("purged %d, want at least the account behind the failing one (1)", n)
	}
	if left := other.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", other.key.AccountID); left != 0 {
		t.Errorf("%d expired keys left in the account behind the failing one", left)
	}
	if left := stuck.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", stuck.key.AccountID); left != keys {
		t.Errorf("%d keys left in the failing account, want all %d", left, keys)
	}

	release()
	if n, err := stuck.s.PurgeIdempotency(ctx, opTimeout); err != nil || n < keys {
		t.Fatalf("next round = %d, %v, want at least %d", n, err, keys)
	}
	if left := stuck.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", stuck.key.AccountID); left != 0 {
		t.Errorf("%d keys left in the formerly failing account", left)
	}
	dbtest.Verify(t, stuck.s)
}

// keysLeft 是这个账户还剩的幂等记录数。
func (f *fixture) keysLeft(t *testing.T) int {
	t.Helper()
	return f.count(t, "SELECT COUNT(*) FROM idempotency_keys WHERE account_id = ?", f.key.AccountID)
}

// waitUntil 等一个确定的事件（库里的状态），有截止时间。
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
	}
}

// 一轮清理的期限在中间到了：先删完第一个账户；第二个账户的事务已经在等额度行的锁，期限到了也做完它（放锁以后），
// 第三个账户一个都没动；错误能分出“停下了”，不是故障；下一轮从头取，接着删掉第三个账户的。
// 不靠睡眠：期限用 cancel 在“第二个账户的事务正等着锁”这个确定的时刻到。
func TestPurgeStopsBetweenTransactions(t *testing.T) {
	first, second, third := newFixture(t, 1<<40), newFixture(t, 1<<40), newFixture(t, 1<<40) // 账户号依次变大，清理按这个顺序处理
	for _, f := range []*fixture{first, second, third} {
		f.expiredKeys(t, 2)
	}
	release := second.holdBudget(t)
	roundCtx, deadline := context.WithCancel(ctx)
	defer deadline()
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := first.s.PurgeIdempotency(roundCtx, opTimeout)
		done <- result{n, err}
	}()
	waitUntil(t, 10*time.Second, func() bool { return first.keysLeft(t) == 0 })
	waitBlocked(t, lockBudgetSQL, 1) // 第二个账户的事务在等锁
	deadline()
	release()
	var r result
	within(t, 10*time.Second, func() { r = <-done })
	if !errors.Is(r.err, ledger.ErrPurgeStopped) || !errors.Is(r.err, context.Canceled) {
		t.Fatalf("purge = %d, %v, want ErrPurgeStopped wrapping the context error", r.n, r.err)
	}
	if r.n < 4 { // 共用库里别的账户也可能有刚过期的键，所以只要求不少于前两个账户的 4 个
		t.Fatalf("purged %d, want at least the first two accounts' 4", r.n)
	}
	if left := second.keysLeft(t); left != 0 {
		t.Errorf("%d keys left in the account whose transaction was running, want it finished", left)
	}
	if left := third.keysLeft(t); left != 2 {
		t.Errorf("%d keys left in the account behind it, want 2 untouched", left)
	}

	if n, err := third.s.PurgeIdempotency(ctx, opTimeout); err != nil || n < 2 {
		t.Fatalf("next round = %d, %v, want at least the third account's 2", n, err)
	}
	if left := third.keysLeft(t); left != 0 {
		t.Errorf("%d keys left after the next round", left)
	}
	dbtest.Verify(t, third.s)
}

// 期限到之前已经有事务出错：停下之后返回的是出错的报告（个数和第一个错误），不是“停下了”。
// 第一个账户的额度行被锁住，它的事务在等；期限到的时候它还在等，随后因为等锁超时而出错，第二个账户没动。
func TestPurgeStoppedKeepsEarlierFailure(t *testing.T) {
	stuck, behind := newFixture(t, 1<<40), newFixture(t, 1<<40)
	stuck.expiredKeys(t, 2)
	behind.expiredKeys(t, 2)
	release := stuck.holdBudget(t)
	defer release()
	roundCtx, deadline := context.WithCancel(ctx)
	defer deadline()
	done := make(chan error, 1)
	go func() {
		_, err := stuck.s.PurgeIdempotency(roundCtx, opTimeout)
		done <- err
	}()
	waitBlocked(t, lockBudgetSQL, 1)
	deadline()
	var err error
	within(t, 10*time.Second, func() { err = <-done })
	if err == nil || !strings.HasPrefix(err.Error(), "purge idempotency: 1 transactions failed, first: ") || errors.Is(err, ledger.ErrPurgeStopped) {
		t.Fatalf("purge = %v, want the failed transaction reported, not ErrPurgeStopped", err)
	}
	if left := stuck.keysLeft(t); left != 2 {
		t.Errorf("%d keys left in the blocked account, want 2", left)
	}
	if left := behind.keysLeft(t); left != 2 {
		t.Errorf("%d keys left in the account behind it, want 2 untouched: the round stopped", left)
	}
}

// 迁移 0002 中途失败后可以重跑：ALTER 已经做过时跳过。之后的迁移随之重跑一遍，也要能重跑。
func TestMigration2CanRerun(t *testing.T) {
	dsn := dbtest.MigrateDSN(t)
	admin := dbtest.Admin(t)
	if _, err := admin.Exec("DELETE FROM schema_migrations WHERE version >= 2"); err != nil {
		t.Fatal(err)
	}
	from, to, err := ledger.Migrate(ctx, dsn)
	if err != nil || from != 1 || to != ledger.SchemaVersion {
		t.Fatalf("Migrate = %d, %d, %v", from, to, err)
	}
	wantAllVersions(t)
}

// 改坏幂等记录，Verify 都能报出对应的规则。
func TestVerifyFindsIdempotencyTampering(t *testing.T) {
	const record = "UPDATE idempotency_keys SET %s WHERE account_id = ? AND idem_key = '%s'"
	set := func(key, assignment string) string {
		return strings.Replace(strings.Replace(record, "%s", assignment, 1), "%s", key, 1)
	}
	cases := []struct {
		name   string
		tamper []string
		want   []string
	}{
		{"record points to another account's call", []string{
			"UPDATE idempotency_keys k JOIN calls c ON c.idempotency_key = 'other' SET k.call_id = c.id WHERE k.account_id = ? AND k.idem_key = 'unknown' AND c.account_id <> k.account_id"},
			[]string{"idem_call"}},
		{"call recorded another key", []string{
			"UPDATE calls SET idempotency_key = 'changed' WHERE account_id = ? AND idempotency_key = 'settled'"},
			[]string{"idem_call"}},
		{"snapshot on a cancelled call", []string{set("cancelled", "response_status = 200, response_bytes = 1, response_body = 'x'")},
			[]string{"idem_snapshot"}},
		{"snapshot status not 2xx", []string{set("settled", "response_status = 500")},
			[]string{"idem_snapshot"}},
		{"snapshot status differs from the call", []string{set("settled", "response_status = 201")},
			[]string{"idem_snapshot"}},
		{"snapshot length wrong", []string{set("settled", "response_bytes = response_bytes + 1")},
			[]string{"idem_snapshot"}},
		{"body dropped under the limit", []string{set("settled", "response_body = NULL")},
			[]string{"idem_snapshot"}},
		{"body kept over the limit", []string{set("big", "response_body = 'x'")},
			[]string{"idem_snapshot"}},
		{"columns without a status", []string{set("unknown", "response_bytes = 0")},
			[]string{"idem_snapshot"}},
		{"settled without a snapshot", []string{set("settled", "response_status = NULL, response_bytes = NULL, response_content_type = NULL, response_body = NULL")},
			[]string{"idem_settled_snapshot"}},
		{"settled stream without its marker", []string{set("streamed", "response_status = NULL")},
			[]string{"idem_settled_snapshot"}},
		{"stream marker with another status", []string{set("streamed", "response_status = 201")},
			[]string{"idem_snapshot"}},
		{"stream marker with a Content-Type", []string{set("streamed", "response_content_type = 'text/event-stream'")},
			[]string{"idem_snapshot"}},
		{"stream marker on an unknown call", []string{"UPDATE calls SET http_status = 200 WHERE account_id = ? AND idempotency_key = 'unknown'",
			set("unknown", "response_status = 200")}, []string{"idem_snapshot"}},
		{"stream marker on a cancelled call", []string{set("cancelled", "response_status = 200")},
			[]string{"idem_snapshot"}},
		{"non-stream snapshot reduced to a status", []string{set("settled", "response_bytes = NULL, response_content_type = NULL, response_body = NULL")},
			nil},
	}
	other := newFixture(t, 1<<40) // 另一个账户：一个带键 other 的调用
	t.Cleanup(func() { deleteAccount(t, other) })
	if _, err := other.s.Reserve(ctx, other.keyed(1, "other", "req")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 1<<40)
			t.Cleanup(func() { deleteAccount(t, f) })
			settled, cancelled, unknown, big := f.keyed(1, "settled", "r"), f.keyed(1, "cancelled", "r"), f.keyed(1, "unknown", "r"), f.keyed(1, "big", "r")
			streamed := f.keyed(1, "streamed", "r")
			for _, r := range []ledger.Reservation{settled, cancelled, unknown, big, streamed} {
				if _, err := f.s.Reserve(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			f.settleWith(t, settled, ledger.Response{Status: 200, ContentType: []string{"application/json"}, Body: []byte(`{"ok":true}`)})
			f.settleWith(t, big, ledger.Response{Status: 200, Body: bytes.Repeat([]byte("x"), ledger.MaxSnapshotBody+1)})
			f.settleWith(t, streamed, ledger.Response{Status: 200, Streamed: true})
			f.finish(t, cancelled, ledger.Cancelled, 0)
			f.sent(t, unknown)
			if err := f.s.Finish(ctx, unknown, ledger.Result{Status: ledger.UsageUnknown, HTTPStatus: 502}); err != nil {
				t.Fatal(err)
			}
			f.tamperRules(t, tc.tamper, tc.want)
		})
	}
}
