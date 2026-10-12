// fakeupstream 在本机起 QF 的假上游，行为见 internal/fakeupstream。
//
// -get URL 不起服务，只 GET 这个地址、把正文打到标准输出：假上游的镜像是 scratch，没有 curl，
// 冒烟用 docker compose exec 这样取 /_fake/bill。
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/fakeupstream"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8081", "listen address")
	get := flag.String("get", "", "GET this URL, print the body and exit")
	flag.Parse()
	if *get != "" {
		os.Exit(fetch(*get))
	}
	srv := &http.Server{Addr: *addr, Handler: fakeupstream.New(), ReadHeaderTimeout: 5 * time.Second}
	slog.Info("fake upstream listening", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

// fetch GET url，正文写到标准输出；不是 200 或连不上时退出 1。
func fetch(url string) int {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fakeupstream -get:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "fakeupstream -get: status", resp.StatusCode)
		return 1
	}
	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		fmt.Fprintln(os.Stderr, "fakeupstream -get:", err)
		return 1
	}
	return 0
}
