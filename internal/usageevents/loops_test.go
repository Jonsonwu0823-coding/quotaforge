package usageevents

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

func TestMain(m *testing.M) { dbtest.Main(m) }

var (
	ctx     = context.Background()
	discard = slog.New(slog.NewJSONHandler(io.Discard, nil))
)

// account 是一个只属于本测试的账户，能结清调用、能读它的汇总。
type account struct {
	s     *ledger.Store
	admin *sql.DB
	key   *ledger.Key
}

func newAccount(t *testing.T) *account {
	t.Helper()
	a := &account{s: dbtest.Store(t), admin: dbtest.Admin(t)}
	a.key = dbtest.Key(t, a.s, dbtest.Account(t, a.s, 1<<40))
	return a
}

// settle 结算一个 chat 调用（prompt 10、completion 3），返回调用号。
func (a *account) settle(t *testing.T) string {
	t.Helper()
	r := ledger.Reservation{CallID: "qf-" + rand.Text(), AccountID: a.key.AccountID, KeyID: a.key.ID, Purpose: "chat",
		Alias: "m", Upstream: "u", UpstreamModel: "um", EstInput: 10, EstOutput: 10}
	if _, err := a.s.Reserve(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := a.s.MarkSent(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := a.s.Finish(ctx, r, ledger.Result{Status: ledger.Settled, Evidence: ledger.Answered, HTTPStatus: 200,
		Usage: &ledger.Usage{Prompt: 10, Completion: 3, Total: 13, HasCompletion: true}}); err != nil {
		t.Fatal(err)
	}
	return r.CallID
}

// totals 是这个账户全部汇总里结算的调用数和 total。
func (a *account) totals(t *testing.T) (calls, total int64) {
	t.Helper()
	if err := a.admin.QueryRow(`SELECT COALESCE(SUM(settled_calls), 0), COALESCE(SUM(total_tokens), 0) FROM usage_daily
		WHERE account_id = ?`, a.key.AccountID).Scan(&calls, &total); err != nil {
		t.Fatal(err)
	}
	return calls, total
}

func (a *account) seen(t *testing.T, id string) bool {
	t.Helper()
	var n int
	if err := a.admin.QueryRow("SELECT COUNT(*) FROM usage_seen WHERE call_id = ?", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func (a *account) sent(t *testing.T, id string) bool {
	t.Helper()
	var n int
	if err := a.admin.QueryRow("SELECT COUNT(*) FROM outbox WHERE call_id = ? AND sent_at IS NOT NULL", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// records 把这个账户 outbox 里的调用变成消息（不经消息队列）：把没发出的行发给一个记下来的发送方。
func (a *account) records(t *testing.T) []Record {
	t.Helper()
	var out []Record
	if _, err := a.s.ForwardOutbox(ctx, func(_ context.Context, events []ledger.UsageEvent) error {
		for _, e := range events {
			if e.AccountID == a.key.AccountID {
				out = append(out, Record{Offset: int64(len(out)), Value: Encode(e)})
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// hold 在管理连接的事务里执行 query，不提交；返回回滚它的函数（测试结束时也会回滚）。
func (a *account) hold(t *testing.T, query string, args ...any) func() {
	t.Helper()
	return dbtest.Hold(t, a.admin, query, args...)
}

// fakeSource 是假的消息来源：按顺序交出 batches，交完就一直等到 ctx 取消。onCommit 在提交时调用，可以返回错误。
type fakeSource struct {
	mu        sync.Mutex
	batches   [][]Record
	committed []Record
	onPoll    func()
	onCommit  func(done []Record) error
}

func (f *fakeSource) Poll(ctx context.Context) ([]Record, error) {
	f.mu.Lock()
	if len(f.batches) == 0 {
		f.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	f.mu.Unlock()
	if f.onPoll != nil {
		f.onPoll()
	}
	return b, nil
}

func (f *fakeSource) Commit(_ context.Context, done []Record) error {
	if f.onCommit != nil {
		if err := f.onCommit(done); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, done...)
	return nil
}

// signal 是只关一次的通道：变异让提交多发生一次时，测试照常报断言的失败，不因为重复关闭而 panic。
type signal struct {
	once sync.Once
	ch   chan struct{}
}

func newSignal() *signal { return &signal{ch: make(chan struct{})} }
func (s *signal) fire()  { s.once.Do(func() { close(s.ch) }) }

func offsets(rs []Record) []int64 {
	var out []int64
	for _, r := range rs {
		out = append(out, r.Offset)
	}
	return out
}

// consume 跑消费者的循环，直到 stop 被调用；返回等它结束的函数。
func consume(store *ledger.Store, src Source, m *telemetry.Metrics) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Consume(ctx, store, src, 10*time.Millisecond, discard, m)
	}()
	return func() { cancel(); <-done }
}

// 先写库、再提交：提交的时候，这一批的事件都已经在汇总里了；格式不对的跳过、计数，和这一批一起提交。
func TestConsumeAppliesBeforeCommitting(t *testing.T) {
	a := newAccount(t)
	ids := []string{a.settle(t), a.settle(t)}
	batch := append(a.records(t), Record{Offset: 2, Value: []byte("not json")})
	m := telemetry.NewMetrics(a.s.Stats)
	committed := newSignal()
	src := &fakeSource{batches: [][]Record{batch}, onCommit: func([]Record) error {
		for _, id := range ids {
			if !a.seen(t, id) {
				t.Errorf("%s committed before it was applied", id)
			}
		}
		committed.fire()
		return nil
	}}
	stop := consume(a.s, src, m)
	<-committed.ch
	stop()
	if got := offsets(src.committed); !slices.Equal(got, []int64{0, 1, 2}) {
		t.Fatalf("committed offsets %v", got)
	}
	if calls, total := a.totals(t); calls != 2 || total != 26 {
		t.Fatalf("totals = %d calls, %d tokens", calls, total)
	}
	applied, malformed := testutil.ToFloat64(m.EventsConsumed.WithLabelValues("applied")), testutil.ToFloat64(m.EventsConsumed.WithLabelValues("malformed"))
	if applied != 2 || malformed != 1 {
		t.Fatalf("applied %v, malformed %v", applied, malformed)
	}
}

// 写库之后、提交之前死了（提交失败，换一个消费者）：同一批再收到一次，都按重复跳过，汇总不变。
func TestConsumeRedeliveryAfterACrash(t *testing.T) {
	a := newAccount(t)
	a.settle(t)
	a.settle(t)
	batch := a.records(t)
	m := telemetry.NewMetrics(a.s.Stats)
	failed := newSignal()
	first := &fakeSource{batches: [][]Record{batch}, onCommit: func([]Record) error {
		failed.fire()
		return errors.New("connection lost before the commit")
	}}
	stop := consume(a.s, first, m)
	<-failed.ch
	stop()
	if len(first.committed) != 0 {
		t.Fatal("the failed commit was recorded")
	}
	committed := newSignal()
	second := &fakeSource{batches: [][]Record{batch}, onCommit: func([]Record) error { committed.fire(); return nil }}
	stop = consume(a.s, second, m)
	<-committed.ch
	stop()
	if calls, total := a.totals(t); calls != 2 || total != 26 {
		t.Fatalf("totals after the redelivery = %d calls, %d tokens", calls, total)
	}
	if dup := testutil.ToFloat64(m.EventsConsumed.WithLabelValues("duplicate")); dup != 2 {
		t.Fatalf("duplicates = %v, want 2", dup)
	}
}

// 写库出错时不跳过、不提交，等一会儿再试同一条：这里管理连接先占着同一个调用号的去重行，消费者的事务等锁超时；
// 放开之后重试成功，才提交。
func TestConsumeRetriesFailedWrites(t *testing.T) {
	a := newAccount(t)
	id := a.settle(t)
	batch := a.records(t)
	release := a.hold(t, "INSERT INTO usage_seen (call_id, account_id, day, purpose, consumed_at) VALUES (?, ?, UTC_DATE(), 'chat', UTC_TIMESTAMP(6))",
		id, a.key.AccountID)
	m := telemetry.NewMetrics(a.s.Stats)
	committed := newSignal()
	src := &fakeSource{batches: [][]Record{batch}, onCommit: func(done []Record) error {
		if len(done) == 1 {
			committed.fire()
		}
		return nil
	}}
	stop := consume(a.s, src, m)
	deadline := time.Now().Add(20 * time.Second)
	for testutil.ToFloat64(m.BackgroundErrors.WithLabelValues("usage_consume")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the write never failed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(src.committed) != 0 || a.seen(t, id) {
		t.Fatal("a record that failed to apply was committed or counted")
	}
	release()
	<-committed.ch
	stop()
	if calls, total := a.totals(t); calls != 1 || total != 13 || !a.seen(t, id) {
		t.Fatalf("totals after the retry = %d calls, %d tokens", calls, total)
	}
}

// 退出：做完手上的这一条（写库在进行中时取消，它照样写完、提交），后面的不处理、不提交。写库卡住的造法：
// 管理连接插了同一个（账户、日期、用途）的汇总行、不提交，消费者累加这一行时要等它。
func TestConsumeStopsBetweenRecords(t *testing.T) {
	a := newAccount(t)
	first, second := a.settle(t), a.settle(t)
	batch := a.records(t)
	release := a.hold(t, `INSERT INTO usage_daily (account_id, day, purpose, settled_calls, prompt_tokens, completion_tokens, total_tokens,
		cached_calls, cached_tokens, cancelled_calls) VALUES (?, UTC_DATE(), 'chat', 0, 0, 0, 0, 0, 0, 0)`, a.key.AccountID)
	m := telemetry.NewMetrics(a.s.Stats)
	src := &fakeSource{batches: [][]Record{batch}}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Consume(ctx, a.s, src, 10*time.Millisecond, discard, m)
	}()
	waitApplying(t, dbtest.App(t))
	cancel()
	release()
	<-done
	if got := offsets(src.committed); !slices.Equal(got, []int64{0}) {
		t.Fatalf("committed %v, want only the record in hand", got)
	}
	if !a.seen(t, first) || a.seen(t, second) {
		t.Fatalf("seen: first %v (want true), second %v (want false)", a.seen(t, first), a.seen(t, second))
	}
}

// waitApplying 等消费者的事务卡在写汇总那一行上（另一个连接占着它）。app 是服务账号的连接：没有 PROCESS 权限，
// 只看得到自己账号的线程，消费者用的正是这个账号。
func waitApplying(t *testing.T, app *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := app.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'INSERT INTO usage_daily%'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the consumer never started writing the daily totals")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 转发循环：把没发出的行发出去、更新积压的指标；消息队列不可用时出错计数、行留着、积压看得到；恢复后补发。
func TestForwardLoop(t *testing.T) {
	a := newAccount(t)
	a.records(t) // 先把共用测试库里别的测试留下的行发掉：积压的最老一条要是本测试的
	id := a.settle(t)
	m := telemetry.NewMetrics(a.s.Stats)
	var mu sync.Mutex
	up := false
	var got []string
	send := func(_ context.Context, events []ledger.UsageEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if !up {
			return errors.New("queue unavailable")
		}
		for _, e := range events {
			got = append(got, e.CallID)
		}
		return nil
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Forward(loopCtx, a.s, send, 10*time.Millisecond, discard, m)
	}()
	waitFor(t, func() bool { return testutil.ToFloat64(m.BackgroundErrors.WithLabelValues("usage_forward")) >= 2 })
	if a.sent(t, id) {
		t.Fatal("marked sent while the queue was down")
	}
	exec(t, a.admin, "UPDATE outbox SET created_at = UTC_TIMESTAMP(6) - INTERVAL 1 HOUR WHERE call_id = ?", id)
	waitFor(t, func() bool {
		return testutil.ToFloat64(m.OutboxOldestAge) >= 3600 && testutil.ToFloat64(m.OutboxUnsent) >= 1
	})
	mu.Lock()
	up = true
	mu.Unlock()
	waitFor(t, func() bool { return a.sent(t, id) && testutil.ToFloat64(m.OutboxUnsent) == 0 })
	cancel()
	<-done
	if !slices.Contains(got, id) || testutil.ToFloat64(m.EventsForwarded) < 1 {
		t.Fatalf("forwarded %v (counter %v)", got, testutil.ToFloat64(m.EventsForwarded))
	}
}

// 退出：转发只在两批之间停。正在发的一批（发送卡住时取消）照样发完、标记，循环随后返回。
func TestForwardStopsBetweenBatches(t *testing.T) {
	a := newAccount(t)
	id := a.settle(t)
	m := telemetry.NewMetrics(a.s.Stats)
	sending, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	send := func(context.Context, []ledger.UsageEvent) error {
		once.Do(func() { close(sending) })
		<-release
		return nil
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Forward(loopCtx, a.s, send, time.Hour, discard, m)
	}()
	<-sending
	cancel()
	close(release)
	<-done
	if !a.sent(t, id) {
		t.Fatal("the batch in flight was not marked sent")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met by the deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func exec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}
