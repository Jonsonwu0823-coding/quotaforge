package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// background 是每个实例的后台循环：启动时立即跑一轮，之后每 interval 一轮。每轮先回收超过上界（after）还没结束的调用，
// 再清理过期的幂等记录，最后数一次还占着预留的未知用量调用（给指标）。多个实例同时跑也只转换一次，见 ledger.Store.Reclaim。
//
// 一轮清理最多用一个间隔：积压很多（QF 停了一阵子再起来）时，一轮删不完，到点在两个事务之间停下，下一轮从头接着取，
// 回收每轮都先跑，不会被积压饿住。到点停下不是错误，不记 purge_error；到点之前已经有事务出错的照旧报出来。
//
// ctx 取消后，回收和清理只在两个事务之间停下（每个事务有自己的 config.LedgerTimeout），循环随即返回，不留半截事务。
// 出错只记一行日志、计一次 qf_background_errors_total，下一轮再来；日志里只有条数和驱动的错误文字，不带连接串。
// 三件都没出错的一轮记下完成的时间（qf_background_last_success_timestamp_seconds），告警按它判断循环是不是卡住了。
//
// 每轮末尾顺带看一次连接池：上一轮以来有请求等过连接（池打满了），就记一行 db_pool（见 poolWaits）。
//
// 配了用量事件的实例（outbox 为 true）清理完幂等记录之后，在同一个时间上限里接着清理发出很久的 outbox 行。
func background(ctx context.Context, store *ledger.Store, after, interval time.Duration, outbox bool, log *slog.Logger, m *telemetry.Metrics) {
	t := time.NewTicker(interval)
	defer t.Stop()
	m.BackgroundInterval.Set(interval.Seconds())
	var pool sql.DBStats
	for {
		if attrs, failed, busy := round(ctx, store, after, interval, outbox, m); busy || failed { // 什么都没做、也没出错的轮次不记
			log.LogAttrs(context.Background(), slog.LevelInfo, "background", attrs...)
		}
		now := store.Stats()
		if attrs := poolWaits(pool, now); attrs != nil {
			log.LogAttrs(context.Background(), slog.LevelInfo, "db_pool", attrs...)
		}
		pool = now
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// round 是后台的一轮：回收、清理、数未知用量，记指标。返回这一轮日志的字段、有没有哪件出错、有没有回收或清理掉什么。
// 三件都没出错（又不是因为退出才停下）就记下完成的时间。
func round(ctx context.Context, store *ledger.Store, after, interval time.Duration, outbox bool, m *telemetry.Metrics) (attrs []slog.Attr, failed, busy bool) {
	stats, reclaimErr := store.Reclaim(ctx, after, config.LedgerTimeout)
	purgeCtx, stopPurge := context.WithTimeout(ctx, interval)
	purged, purgeErr := store.PurgeIdempotency(purgeCtx, config.LedgerTimeout)
	var outboxPurged int64
	var outboxErr error
	if outbox {
		outboxPurged, outboxErr = store.PurgeOutbox(purgeCtx, config.LedgerTimeout)
	}
	stopPurge()
	m.Reclaimed.WithLabelValues("released").Add(float64(stats.Released))
	m.Reclaimed.WithLabelValues("marked_unknown").Add(float64(stats.Unknown))
	m.Purged.Add(float64(purged))
	attrs = []slog.Attr{slog.Int("released", stats.Released), slog.Int("marked_unknown", stats.Unknown), slog.Int64("purged", purged)}
	fail := func(task string, err error) {
		failed = true
		m.BackgroundErrors.WithLabelValues(task).Inc()
		attrs = append(attrs, slog.String(task+"_error", err.Error()))
	}
	if reclaimErr != nil && ctx.Err() == nil {
		fail("reclaim", reclaimErr)
	}
	if purgeErr != nil && ctx.Err() == nil && !errors.Is(purgeErr, ledger.ErrPurgeStopped) {
		fail("purge", purgeErr)
	}
	if outbox {
		attrs = append(attrs, slog.Int64("outbox_purged", outboxPurged))
	}
	if outboxErr != nil && ctx.Err() == nil && !errors.Is(outboxErr, ledger.ErrPurgeStopped) {
		fail("outbox_purge", outboxErr)
	}
	if err := countUnknown(ctx, store, m); err != nil && ctx.Err() == nil {
		fail("unknown_scan", err)
	}
	if !failed && ctx.Err() == nil {
		m.BackgroundLastSuccess.SetToCurrentTime()
	}
	return attrs, failed, stats.Released+stats.Unknown > 0 || purged > 0 || outboxPurged > 0
}

// countUnknown 更新未知用量的两个指标；查不到时保留上一轮的值。
func countUnknown(ctx context.Context, store *ledger.Store, m *telemetry.Metrics) error {
	ctx, cancel := context.WithTimeout(ctx, config.LedgerTimeout)
	defer cancel()
	n, age, err := store.UnknownBacklog(ctx)
	if err != nil {
		return err
	}
	m.UnknownCalls.Set(float64(n))
	m.UnknownOldestAge.Set(float64(age))
	return nil
}

// poolWaits 比较连接池前后两次的统计：这期间有请求等过连接，返回本期新增的等待次数和等待时长，以及现在的连接数；
// 没等过返回 nil，不记日志。指标里也有这些数（qf_db_*，累计值）；这一行日志和它们并存，压测脚本按它取证。
func poolWaits(prev, now sql.DBStats) []slog.Attr {
	if now.WaitCount == prev.WaitCount {
		return nil
	}
	return []slog.Attr{
		slog.Int64("waits", now.WaitCount-prev.WaitCount),
		slog.Int64("wait_ms", (now.WaitDuration - prev.WaitDuration).Milliseconds()),
		slog.Int("in_use", now.InUse),
		slog.Int("open", now.OpenConnections),
		slog.Int("max_open", now.MaxOpenConnections),
	}
}
