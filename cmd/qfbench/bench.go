package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 错误响应只读这么多来取 error.code；成功的正文读完丢掉，不留。
const maxErrorBody = 64 << 10

type bench struct {
	targets, bodies, keys []string
	timeout               time.Duration
	idemKey               string // burst：每个请求都带这个键
	idemEach              bool   // 每个请求一个新键
}

// do 发第 seq 个请求，t0 是这一轮的起点（start_us 从它算）。
func (b *bench) do(client *http.Client, seq int, t0 time.Time) result {
	target := seq % len(b.targets)
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(b.targets[target], "/")+"/v1/chat/completions",
		strings.NewReader(b.bodies[seq%len(b.bodies)]))
	start := time.Now()
	r := result{Seq: seq, StartUS: start.Sub(t0).Microseconds(), Target: target}
	if err != nil {
		r.ClientError = "transport"
		return r
	}
	req.Header.Set("Authorization", "Bearer "+b.keys[seq%len(b.keys)])
	req.Header.Set("Content-Type", "application/json")
	switch {
	case b.idemKey != "":
		req.Header.Set("Idempotency-Key", b.idemKey)
	case b.idemEach:
		req.Header.Set("Idempotency-Key", "qfbench-"+rand.Text())
	}
	resp, err := client.Do(req)
	if err != nil {
		r.LatencyUS, r.ClientError = time.Since(start).Microseconds(), clientError(err)
		return r
	}
	var code string
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct{ Code string } `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&e)
		code = e.Error.Code
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	r.LatencyUS = time.Since(start).Microseconds()
	if err != nil { // 正文没读完就断了：算客户端的错，状态码不算数
		r.ClientError = clientError(err)
		return r
	}
	r.Status, r.Code = resp.StatusCode, code
	r.RequestID = resp.Header.Get("x-request-id")
	r.Replayed = resp.Header.Get("Idempotent-Replayed") == "true"
	r.RetryAfter = resp.Header.Get("Retry-After")
	return r
}

// clientError 是没拿到完整响应时记的类别：客户端超时（-timeout）或其它传输错误（连不上、被断开）。
func clientError(err error) string {
	if ne := net.Error(nil); errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "transport"
}

// sustain 是闭环压测：c 个 worker 共用一个连接池（每个目标最多留 c 个空闲连接，压测端不成瓶颈），各自发完一个再发下一个。
// 开始时间（start_us）早于 warmup 的标为预热；到 warmup+duration 不再发新的，在途的等它结束、照常记下。
func (b *bench) sustain(c int, warmup, duration time.Duration) []result {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = c
	client := &http.Client{Transport: tr, Timeout: b.timeout}
	defer tr.CloseIdleConnections()
	t0 := time.Now()
	end := t0.Add(warmup + duration)
	var seq atomic.Int64
	var mu sync.Mutex
	var rows []result
	var wg sync.WaitGroup
	for range c {
		wg.Go(func() {
			var mine []result
			for time.Now().Before(end) {
				r := b.do(client, int(seq.Add(1)-1), t0)
				r.Warmup = r.StartUS < warmup.Microseconds()
				mine = append(mine, r)
			}
			mu.Lock()
			rows = append(rows, mine...)
			mu.Unlock()
		})
	}
	wg.Wait()
	slices.SortFunc(rows, func(a, b result) int { return a.Seq - b.Seq })
	return rows
}

// burst 让 n 个请求同时出发：每个请求自己的连接池，先 GET /healthz 把连接建好（响应是什么不要紧，连接留着复用），
// 全部就绪后关掉 release 一起放开。这样放开之后每个请求只剩写请求，不用排队建连接。
func (b *bench) burst(n int) []result {
	rows := make([]result, n)
	clients := make([]*http.Client, n)
	var ready, done sync.WaitGroup
	release := make(chan struct{})
	var t0 time.Time
	for i := range n {
		tr := &http.Transport{Proxy: http.ProxyFromEnvironment}
		clients[i] = &http.Client{Transport: tr, Timeout: b.timeout}
		ready.Add(1)
		done.Go(func() {
			defer tr.CloseIdleConnections()
			if resp, err := clients[i].Get(strings.TrimRight(b.targets[i%len(b.targets)], "/") + "/healthz"); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			ready.Done()
			<-release
			rows[i] = b.do(clients[i], i, t0)
		})
	}
	ready.Wait()
	t0 = time.Now()
	close(release)
	done.Wait()
	return rows
}
