package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
)

// metricValue 是 /metrics 里这一行（名字加标签，原样）的值；没有这一行时返回 -1。
func metricValue(body, series string) float64 {
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` (\S+)$`).FindStringSubmatch(body)
	if m == nil {
		return -1
	}
	v, _ := strconv.ParseFloat(m[1], 64)
	return v
}

// 消息队列不可用（broker 指向没人监听的端口）：请求照常 200，事件留在 outbox 里没发出，指标看得到积压和转发的错误，
// 日志有 usage_forward；QF 照常退出（转发只在两批之间停，不拖住退出）。
func TestProcessServesWhileKafkaIsDown(t *testing.T) {
	collector, _ := spanSink(t)
	cfgPath, key, _ := processConfig(t)
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	withEvents := strings.TrimSuffix(strings.TrimSpace(string(data)), "}") + `,"usage_events":{"brokers":["127.0.0.1:1"]}}`
	cfgPath = filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(cfgPath, []byte(withEvents), 0o600); err != nil {
		t.Fatal(err)
	}
	p := startObservedQF(t, cfgPath, collector)
	api, metricsAddr := p.addr(t, "listening"), p.addr(t, "metrics")

	req, _ := http.NewRequest(http.MethodPost, "http://"+api+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	id := resp.Header.Get("x-request-id")
	if resp.StatusCode != http.StatusOK || id == "" {
		t.Fatalf("status %d, request id %q", resp.StatusCode, id)
	}
	var unsent int
	if err := dbtest.Admin(t).QueryRow("SELECT COUNT(*) FROM outbox WHERE call_id = ? AND sent_at IS NULL", id).Scan(&unsent); err != nil || unsent != 1 {
		t.Fatalf("outbox row of the call: %d unsent, %v", unsent, err)
	}
	var body string
	waitUntil(t, 30*time.Second, func() bool {
		body = get(t, "http://"+metricsAddr+"/metrics", http.StatusOK)
		return metricValue(body, `qf_background_errors_total{task="usage_forward"}`) >= 1 && metricValue(body, "qf_usage_outbox_unsent") >= 1
	})
	if v := metricValue(body, "qf_usage_events_forwarded_total"); v != 0 {
		t.Fatalf("forwarded %v events with no broker", v)
	}
	forwardLogged := false
	for _, l := range p.logLines() {
		forwardLogged = forwardLogged || l["msg"] == "usage_forward"
	}
	if !forwardLogged {
		t.Fatal("no usage_forward log line")
	}
	p.stop(t, func() bool { return true })
}

// 没有 usage_events 这一段：不碰开关、不起转发（给 QS 用的部署就是这样），返回的等待函数立即返回。
func TestNoForwardingWithoutUsageEvents(t *testing.T) {
	wait, err := startForwarding(context.Background(), ledgerConfig(t, "unused"), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	wait()
}

// usage-consumer 要配置里有 usage_events，没有就以 1 退出。
func TestUsageConsumerNeedsUsageEvents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{"upstreams":[{"name":"u","base_url":"http://127.0.0.1:1/v1","api_key_env":"K"}],
		"models":[{"alias":"m","purpose":"chat","targets":[{"upstream":"u","upstream_model":"m","weight":1}],"default_max_tokens":1}],
		"database":{"dsn_env":"K"},"redis":{"url_env":"K"}}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("QUOTAFORGE_CONFIG", path)
	t.Setenv("K", "k")
	if code := usageConsumer(); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
}
