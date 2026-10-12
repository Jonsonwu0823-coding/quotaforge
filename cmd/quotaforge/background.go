package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// background 是每个实例的后台循环：启动时立即跑一轮，之后每 interval 一轮。每轮先回收超过上界（after）还没结束的调用，
// 再清理过期的幂等记录。多个实例同时跑也只转换一次，见 ledger.Store.Reclaim。
//
// 一轮清理最多用一个间隔：积压很多（QF 停了一阵子再起来）时，一轮删不完，到点在两个事务之间停下，下一轮从头接着取，
// 回收每轮都先跑，不会被积压饿住。到点停下不是错误，不记 purge_error；到点之前已经有事务出错的照旧报出来。
//
// ctx 取消后，回收和清理只在两个事务之间停下（每个事务有自己的 config.LedgerTimeout），循环随即返回，不留半截事务。
// 出错只记一行日志，下一轮再来；日志里只有条数和驱动的错误文字，不带连接串。
//
// 每轮末尾顺带看一次连接池：上一轮以来有请求等过连接（池打满了），就记一行 db_pool（见 poolWaits）。
func background(ctx context.Context, store *ledger.Store, after, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var pool sql.DBStats
	for {
		stats, reclaimErr := store.Reclaim(ctx, after, config.LedgerTimeout)
		purgeCtx, stopPurge := context.WithTimeout(ctx, interval)
		purged, purgeErr := store.PurgeIdempotency(purgeCtx, config.LedgerTimeout)
		stopPurge()
		attrs := []slog.Attr{slog.Int("released", stats.Released), slog.Int("marked_unknown", stats.Unknown), slog.Int64("purged", purged)}
		if reclaimErr != nil && ctx.Err() == nil {
			attrs = append(attrs, slog.String("reclaim_error", reclaimErr.Error()))
		}
		if purgeErr != nil && ctx.Err() == nil && !errors.Is(purgeErr, ledger.ErrPurgeStopped) {
			attrs = append(attrs, slog.String("purge_error", purgeErr.Error()))
		}
		if stats.Released+stats.Unknown > 0 || purged > 0 || len(attrs) > 3 { // 什么都没做、也没出错的轮次不记
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

// poolWaits 比较连接池前后两次的统计：这期间有请求等过连接，返回本期新增的等待次数和等待时长，以及现在的连接数；
// 没等过返回 nil，不记日志。以后做指标时换成指标。
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
