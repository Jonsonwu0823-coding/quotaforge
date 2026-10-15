package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/embcache"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

const defaultAddr = "127.0.0.1:8080"

// newHandler 把健康检查和转发接口挂在一起，最外层生成调用号，所以 404、405 也带 x-request-id。
func newHandler(api http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/v1/", api)
	return gateway.WithRequestID(mux)
}

// newServer 是 API 和指标两个服务共用的设置：读请求头、读整个请求（头和体）、keep-alive 空闲连接都有上限，
// 只发一半请求体的客户端最多占住连接 ReadTimeout。不设 WriteTimeout：全局的写期限会掐断长流，
// 流式的写期限设在每次写上。ReadTimeout 只管读请求：net/http 开始后台读时自己清读期限，由 TestServerReadTimeoutSparesLongCalls
// 守着；以后升级 Go，这个测试会抓住变化。
func newServer(h http.Handler) *http.Server {
	return &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
}

// run 用 srv 在 ln 上提供服务，直到 ctx 取消。取消后等进行中的请求处理完（最多 shutdownTimeout）才返回：
// Shutdown 一调用 Serve 就会立刻返回，只有等 Shutdown 返回才算请求都结束了。
func run(ctx context.Context, ln net.Listener, srv *http.Server, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// openLedger 打开账本并确认数据库已迁移到这个程序要求的版本；不一致就返回错误，QF 拒绝启动。
// 迁移只由 qfctl 用能建表的账号做，QF 服务的账号只能增删改查。
func openLedger(ctx context.Context, cfg *config.Config) (*ledger.Store, error) {
	store, err := ledger.Open(cfg.DatabaseDSN())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := store.CheckSchema(ctx); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// openCache 在配置开了嵌入缓存时连它的 Redis（不立即拨号：它不可用只算没命中，恢复后不用重启）。没开时返回 nil：
// 是值为 nil 的接口，不是装着 nil 指针的接口，网关据此不查缓存。
func openCache(cfg *config.Config) (gateway.Cache, func(), error) {
	ec := cfg.EmbeddingCache()
	if ec == nil {
		return nil, func() {}, nil
	}
	c, err := embcache.Open(ec.URL, "qf:emb:")
	if err != nil {
		return nil, nil, err
	}
	return c, func() { c.Close() }, nil
}

// listenAddr 是服务监听的地址，健康检查也按它去连。
func listenAddr() string {
	if addr := os.Getenv("QUOTAFORGE_ADDR"); addr != "" {
		return addr
	}
	return defaultAddr
}

// command 执行子命令，返回进程的退出码。
func command(name string) int {
	switch name {
	case "healthcheck":
		if err := healthcheck(listenAddr()); err != nil {
			fmt.Fprintln(os.Stderr, "healthcheck:", err)
			return 1
		}
	case "usage-export":
		invalid, err := gateway.ExportUsage(os.Stdin, os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, "usage-export:", err)
			return 1
		}
		if invalid > 0 { // 能解析的行照常写完，再以 1 退出：和 qfctl idempotency purge 部分成功时一样
			fmt.Fprintf(os.Stderr, "usage-export: skipped %d unparseable lines\n", invalid)
			return 1
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q; commands: healthcheck, usage-export\n", name)
		return 2
	}
	return 0
}

func main() {
	if len(os.Args) > 1 {
		os.Exit(command(os.Args[1]))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	path := os.Getenv("QUOTAFORGE_CONFIG")
	if path == "" {
		slog.Error("config", "err", "QUOTAFORGE_CONFIG is not set")
		os.Exit(1)
	}
	cfg, err := config.Load(path, os.Getenv)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	telCfg, err := config.LoadTelemetry(os.Getenv)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	store, err := openLedger(context.Background(), cfg)
	if err != nil {
		slog.Error("ledger", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	// 不在启动时连 Redis：它不可用只影响要检查桶的请求（503），恢复后不用重启。
	limiter, err := ratelimit.Open(cfg.RedisURL(), "qf:rl:")
	if err != nil {
		slog.Error("rate limiter", "err", err)
		os.Exit(1)
	}
	defer limiter.Close()
	cache, closeCache, err := openCache(cfg)
	if err != nil {
		slog.Error("embedding cache", "err", err)
		os.Exit(1)
	}
	defer closeCache()

	ln, err := net.Listen("tcp", listenAddr())
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	metrics := telemetry.NewMetrics(store.Stats)
	if telCfg.MetricsAddr != "" {
		mln, err := net.Listen("tcp", telCfg.MetricsAddr)
		if err != nil {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
		slog.Info("metrics", "addr", mln.Addr().String())
		defer serveMetrics(mln, metrics)()
	}
	tel, flushSpans := telemetry.Setup(telCfg, metrics, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 退出的顺序：在途请求处理完（最多 ShutdownTimeout）、后台循环也停下之后，才把还没导出的 span 发完（最多
	// telemetry.FlushTimeout，不占在途请求的时间；到点没发完的 span 丢掉，账本在这之前已经收完尾）；
	// 然后关指标的监听（排空期间 Prometheus 仍能抓），最后关 Redis 和账本（defer）。
	done := make(chan struct{})
	go func() {
		defer close(done)
		background(ctx, store, cfg.ReclaimAfter(), cfg.ReclaimAfter()/4, logger, metrics)
	}()
	slog.Info("listening", "addr", ln.Addr().String())
	err = run(ctx, ln, newServer(newHandler(gateway.New(cfg, store, limiter, cache, logger, tel))), cfg.ShutdownTimeout())
	stop()
	<-done
	if ferr := flushSpans(); ferr != nil {
		slog.Warn("telemetry", "error", ferr.Error())
	}
	if err != nil {
		slog.Error("run", "err", err)
		os.Exit(1)
	}
}

// serveMetrics 在 ln 上另起一个只有 GET /metrics 的服务，不放在对外的监听地址上。返回的函数关掉它（立即，不等正在进行的抓取）。
func serveMetrics(ln net.Listener, m *telemetry.Metrics) (stop func()) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	srv := newServer(mux)
	go func() { _ = srv.Serve(ln) }()
	return func() { _ = srv.Close() }
}
