package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/usageevents"
)

// enableTimeout 是启动时打开用量事件开关的上限：打开要等正在结清、已经读过开关的事务提交，每个最多一次账本操作；
// 等锁超过会话的上限（1205）时在这个期限里重试，都不行才退出。
const enableTimeout = 10 * time.Second

// startForwarding 在配置里有 usage_events 时打开用量事件的开关（只开不关），并起转发循环；返回等它停下的函数。
// 没配时什么都不做：不碰开关、不连消息队列。转发不在请求路径上，消息队列不可用时请求照常，积压留在 outbox。
// ctx 取消时转发在两批之间停下（正在发的一批最多等 Kafka 5 秒、标记一次账本操作），和 HTTP 排空同时进行，
// 所以退出的上限不变。
func startForwarding(ctx context.Context, cfg *config.Config, store *ledger.Store, log *slog.Logger, m *telemetry.Metrics) (wait func(), err error) {
	ue := cfg.UsageEvents()
	if ue == nil {
		return func() {}, nil
	}
	enableCtx, cancel := context.WithTimeout(context.Background(), enableTimeout)
	defer cancel()
	if err := store.EnableUsageEvents(enableCtx); err != nil {
		return nil, err
	}
	producer, err := usageevents.NewProducer(ue.Brokers, usageevents.Topic)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		usageevents.Forward(ctx, store, producer.Send, usageevents.ForwardInterval, log, m)
	}()
	return func() {
		<-done
		producer.Close()
	}, nil
}

// usageConsumer 是 usage-consumer 子命令：用量事件的消费者，单独一个进程（和 QF 同一个镜像、同一份配置，用其中的
// database 和 usage_events 两段）。把事件按调用号去重、按账户按天汇总进库，直到收到 SIGINT 或 SIGTERM：
// 做完手上的这一条、提交处理完的位点再退出。指标照 QUOTAFORGE_METRICS_ADDR 开。返回退出码。
func usageConsumer() int {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := consume(log); err != nil {
		log.Error("usage_consumer", "err", err)
		return 1
	}
	return 0
}

func consume(log *slog.Logger) error {
	cfg, telCfg, err := loadConfig()
	if err != nil {
		return err
	}
	ue := cfg.UsageEvents()
	if ue == nil {
		return fmt.Errorf("usage_events is not in the config")
	}
	store, err := openLedger(context.Background(), cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	metrics := telemetry.NewMetrics(store.Stats)
	stopMetrics, err := listenMetrics(telCfg.MetricsAddr, metrics)
	if err != nil {
		return err
	}
	defer stopMetrics()
	source, err := usageevents.NewConsumer(ue.Brokers, usageevents.Topic, usageevents.Group)
	if err != nil {
		return err
	}
	defer source.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("usage_consumer", "topic", usageevents.Topic, "group", usageevents.Group)
	usageevents.Consume(ctx, store, source, usageevents.RetryDelay, log, metrics)
	return nil
}
