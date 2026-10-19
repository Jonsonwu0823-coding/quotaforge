package usageevents

import (
	"context"
	"log/slog"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// RetryDelay 是消费者写库或者取消息出错之后，再试之前等的时间。
const RetryDelay = time.Second

// commitTimeout 是提交位点的上限。退出时也用它：ctx 已经取消，提交用新的 ctx。
const commitTimeout = 5 * time.Second

// 一条消息的结果，就是 qf_usage_events_consumed_total 的 result。
const (
	resultApplied   = "applied"   // 写进了按天的汇总
	resultDuplicate = "duplicate" // 这个调用之前算过
	resultMalformed = "malformed" // 格式不对，跳过
)

// Record 是从消息队列取到的一条消息；Partition、Offset 只用于日志。
type Record struct {
	Partition int32
	Offset    int64
	Value     []byte
	raw       any // 适配器自己的记录，提交位点时用
}

// Source 是消费组里的消息来源。Poll 取一批（阻塞到有消息或者 ctx 取消）；Commit 提交已经处理完的那些的位点。
type Source interface {
	Poll(ctx context.Context) ([]Record, error)
	Commit(ctx context.Context, done []Record) error
}

// Consume 是消费者的循环：取一批，逐条写进按天的汇总（ledger.Store.ApplyUsageEvent），写库成功之后才提交这些位点。
// 写库之后、提交之前死了，重启之后会再收到这几条，按调用号去重，汇总不变。
//
//   - 写库出错：等 retry 再试同一条，不跳过（跳过就丢了）；出错计 qf_background_errors_total{task="usage_consume"}。
//   - 格式不对：记一行日志（只有分区、位点和原因，不记内容）、计数、跳过，和这一批一起提交：生产者只有 QF，
//     格式不对是程序错误，不能让它卡住同一个分区后面的事件。它的调用在对账里会一直显示“发了还没消费”。
//   - ctx 取消：做完手上的这一条就停，提交已经处理完的，剩下的不提交（下次再收到）。
//
// 一开始把 qf_usage_events_consumed_total 的三个 result 各计 0：消费者重启之后，序列从 0 开始，第一个事件让它变成 1，
// increase() 看得到；不这样，序列第一次出现时就是 1，increase() 当它没有增加，QFUsageConsumerStalled 会误报。
// 只有消费者进程调 Consume，QF 实例不导出这个指标。
func Consume(ctx context.Context, store *ledger.Store, src Source, retry time.Duration, log *slog.Logger, m *telemetry.Metrics) {
	for _, result := range []string{resultApplied, resultDuplicate, resultMalformed} {
		m.EventsConsumed.WithLabelValues(result).Add(0)
	}
	c := consumer{store: store, retry: retry, log: log, m: m}
	for ctx.Err() == nil {
		batch, err := src.Poll(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.failed("poll", err)
				c.wait(ctx)
			}
			continue
		}
		var done []Record
		for _, r := range batch {
			if ctx.Err() != nil || !c.handle(ctx, r) {
				break
			}
			done = append(done, r)
		}
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		if err := src.Commit(commitCtx, done); err != nil {
			c.failed("commit", err)
		}
		cancel()
	}
}

type consumer struct {
	store *ledger.Store
	retry time.Duration
	log   *slog.Logger
	m     *telemetry.Metrics
}

// handle 处理一条消息，返回它是不是处理完了（可以提交）：写库出错时一直重试，只有重试时 ctx 取消了才返回 false。
func (c consumer) handle(ctx context.Context, r Record) bool {
	e, err := Decode(r.Value)
	if err != nil {
		c.m.EventsConsumed.WithLabelValues(resultMalformed).Inc()
		c.log.LogAttrs(context.Background(), slog.LevelWarn, "usage_consumer", slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset), slog.String("error", err.Error()))
		return true
	}
	for {
		applied, err := c.apply(ctx, e)
		if err == nil {
			result := resultDuplicate
			if applied {
				result = resultApplied
				c.m.EventDelay.Observe(time.Since(e.ResolvedAt).Seconds())
			}
			c.m.EventsConsumed.WithLabelValues(result).Inc()
			return true
		}
		c.failed("apply", err)
		if c.wait(ctx); ctx.Err() != nil {
			return false
		}
	}
}

// apply 写库最多 config.LedgerTimeout，从 ctx 脱开：取消时不打断进行中的事务。
func (c consumer) apply(ctx context.Context, e ledger.UsageEvent) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.LedgerTimeout)
	defer cancel()
	return c.store.ApplyUsageEvent(ctx, e)
}

func (c consumer) failed(step string, err error) {
	c.m.BackgroundErrors.WithLabelValues("usage_consume").Inc()
	c.log.LogAttrs(context.Background(), slog.LevelWarn, "usage_consumer", slog.String("step", step), slog.String("error", err.Error()))
}

func (c consumer) wait(ctx context.Context) {
	t := time.NewTimer(c.retry)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
