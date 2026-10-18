package usageevents

import (
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// recording 包着一个真的消息来源，记下收到的消息数和每个账户的事件落在哪些分区。
type recording struct {
	Source
	mu    sync.Mutex
	n     int
	parts map[int64]map[int32]bool
}

func (r *recording) Poll(ctx context.Context) ([]Record, error) {
	batch, err := r.Source.Poll(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.parts == nil {
		r.parts = map[int64]map[int32]bool{}
	}
	for _, rec := range batch {
		r.n++
		if e, err := Decode(rec.Value); err == nil {
			if r.parts[e.AccountID] == nil {
				r.parts[e.AccountID] = map[int32]bool{}
			}
			r.parts[e.AccountID][rec.Partition] = true
		}
	}
	return batch, err
}

func (r *recording) received() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// forwardAll 把 outbox 里没发出的行经真的生产者发到 Kafka。
func forwardAll(t *testing.T, s *ledger.Store, p *Producer) {
	t.Helper()
	if _, err := s.ForwardOutbox(ctx, p.Send); err != nil {
		t.Fatal(err)
	}
}

// kafkaSource 是消费组里的一个新成员。stopAndLeave 停下循环之后要离开消费组（Close），和真的消费者退出时一样：
// 不离开的话，同一个组的下一个成员要等它超时才分得到分区。
func kafkaSource(t *testing.T, addr, topic, group string) (*recording, func()) {
	t.Helper()
	c, err := NewConsumer([]string{addr}, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	return &recording{Source: c}, c.Close
}

// 经真的 Kafka：转发 → 主题 → 消费组 → 按天汇总。同一个账户的事件在同一个分区；消费者停掉再起来，
// 从提交过的位点接着读，不重复；新的消费组从最早的消息读起（等于把位点重置到最早），全是重复，汇总不变。
func TestKafkaRoundTrip(t *testing.T) {
	addr := dbtest.KafkaAddr(t)
	suffix := strings.ToLower(rand.Text()[:10])
	topic, group := "qf-test-"+suffix, "qf-test-"+suffix
	a, b := newAccount(t), newAccount(t)
	a.records(t) // 共用测试库里别的测试留下的行不发进这个主题
	for range 3 {
		a.settle(t)
		b.settle(t)
	}
	p, err := NewProducer([]string{addr}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	forwardAll(t, a.s, p)

	m := telemetry.NewMetrics(a.s.Stats)
	first, leave := kafkaSource(t, addr, topic, group)
	stop := consume(a.s, first, m)
	waitFor(t, func() bool { ca, _ := a.totals(t); cb, _ := b.totals(t); return ca == 3 && cb == 3 })
	stop()
	leave()
	for _, acc := range []*account{a, b} {
		if n := len(first.parts[acc.key.AccountID]); n != 1 {
			t.Fatalf("account %d's events are in %d partitions, want 1", acc.key.AccountID, n)
		}
	}

	a.settle(t)
	a.settle(t)
	forwardAll(t, a.s, p)
	second, leave2 := kafkaSource(t, addr, topic, group)
	stop = consume(a.s, second, m)
	waitFor(t, func() bool { ca, _ := a.totals(t); return ca == 5 })
	stop()
	leave2()
	if n, dup := second.received(), testutil.ToFloat64(m.EventsConsumed.WithLabelValues("duplicate")); n != 2 || dup != 0 {
		t.Fatalf("after the restart: received %d (want 2), duplicates %v (want 0)", n, dup)
	}

	again := telemetry.NewMetrics(a.s.Stats)
	third, leave3 := kafkaSource(t, addr, topic, group+"-again")
	stop = consume(a.s, third, again)
	waitFor(t, func() bool { return testutil.ToFloat64(again.EventsConsumed.WithLabelValues("duplicate")) == 8 })
	stop()
	leave3()
	ca, ta := a.totals(t)
	cb, tb := b.totals(t)
	if ca != 5 || ta != 65 || cb != 3 || tb != 39 || testutil.ToFloat64(again.EventsConsumed.WithLabelValues("applied")) != 0 {
		t.Fatalf("totals after reading everything again: a %d/%d, b %d/%d", ca, ta, cb, tb)
	}
}

// Kafka 不可用时，一批最多等 sendTimeout 就报错，不会一直挂着（转发的这一轮结束，下一轮再来）。
func TestSendGivesUpWhenKafkaIsDown(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, "qf-test-down")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	start := time.Now()
	if err := p.Send(ctx, []ledger.UsageEvent{settledChat()}); err == nil {
		t.Fatal("Send succeeded with no broker")
	}
	if d := time.Since(start); d < sendTimeout-time.Second || d > sendTimeout+2*time.Second {
		t.Fatalf("Send gave up after %v, want about %v", d, sendTimeout)
	}
}
