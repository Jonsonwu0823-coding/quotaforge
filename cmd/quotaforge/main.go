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
	"github.com/Jonsonwu0823-coding/quotaforge/internal/gateway"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
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

// run 在 ln 上提供服务，直到 ctx 取消。取消后等进行中的请求处理完（最多 shutdownTimeout）才返回：
// Shutdown 一调用 Serve 就会立刻返回，只有等 Shutdown 返回才算请求都结束了。
func run(ctx context.Context, ln net.Listener, h http.Handler, shutdownTimeout time.Duration) error {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
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

	ln, err := net.Listen("tcp", listenAddr())
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 后台循环和 HTTP 服务一起停：收到退出信号后，等两者都结束才关账本（defer store.Close）。
	done := make(chan struct{})
	go func() {
		defer close(done)
		background(ctx, store, cfg.ReclaimAfter(), cfg.ReclaimAfter()/4, logger)
	}()
	slog.Info("listening", "addr", ln.Addr().String())
	err = run(ctx, ln, newHandler(gateway.New(cfg, store, limiter, logger)), cfg.ShutdownTimeout())
	stop()
	<-done
	if err != nil {
		slog.Error("run", "err", err)
		os.Exit(1)
	}
}
