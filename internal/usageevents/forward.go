package usageevents

import (
	"context"
	"log/slog"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// ForwardInterval 是转发两轮之间的间隔。
const ForwardInterval = time.Second

// SendFunc 把一批事件发到消息队列，都发成功才返回 nil；等待的时间由它自己限住。
type SendFunc func(ctx context.Context, events []ledger.UsageEvent) error

// Forward 是 QF 实例里的转发循环：立即跑一轮，之后每 interval 一轮，把 outbox 里没发出的行发出去
// （ledger.Store.ForwardOutbox：几个实例里同一时刻只有一个在发），再更新积压的两个指标。
// 不在请求路径上：消息队列挂了，请求照常，积压留在 outbox，恢复之后补发。
// 出错计一次 qf_background_errors_total{task="usage_forward"}、记一行 msg "usage_forward"，下一轮再来。
// ctx 取消后在两批之间停下（正在发的一批做完），循环随即返回。
func Forward(ctx context.Context, store *ledger.Store, send SendFunc, interval time.Duration, log *slog.Logger, m *telemetry.Metrics) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		n, err := store.ForwardOutbox(ctx, send)
		m.EventsForwarded.Add(float64(n))
		if berr := updateBacklog(ctx, store, m); err == nil { // 发不出去时也更新：积压就是这时候要看的
			err = berr
		}
		if err != nil && ctx.Err() == nil {
			m.BackgroundErrors.WithLabelValues("usage_forward").Inc()
			log.LogAttrs(context.Background(), slog.LevelWarn, "usage_forward", slog.Int("forwarded", n), slog.String("error", err.Error()))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// updateBacklog 更新积压的两个指标；查不到时保留上一轮的值。
func updateBacklog(ctx context.Context, store *ledger.Store, m *telemetry.Metrics) error {
	ctx, cancel := context.WithTimeout(ctx, config.LedgerTimeout)
	defer cancel()
	n, age, err := store.OutboxBacklog(ctx)
	if err != nil {
		return err
	}
	m.OutboxUnsent.Set(float64(n))
	m.OutboxOldestAge.Set(float64(age))
	return nil
}
