package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKeys = "key-one-SECRET\nkey-two-SECRET\n"

func runCmd(t *testing.T, stdin string, args ...string) (string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, strings.NewReader(stdin), &out, &errOut)
	if code != 0 {
		t.Logf("stderr: %s", errOut.String())
	}
	return out.String(), code
}

func summaryOf(t *testing.T, raw string) summary {
	t.Helper()
	out, code := runCmd(t, raw, "summarize")
	if code != 0 {
		t.Fatalf("summarize exit %d", code)
	}
	var s summary
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// 写出再读回，逐个字段相同；汇总从读回的原始数据算，和直接从内存里的算出来的一样。
func TestRawRoundTrip(t *testing.T) {
	rows := []result{
		{Seq: 0, StartUS: 10, Target: 0, Status: 200, LatencyUS: 51000, RequestID: "qf-A", Replayed: true},
		{Seq: 1, StartUS: 20, Target: 1, Status: 429, Code: "rate_limited", LatencyUS: 3000, RetryAfter: "2", Warmup: true},
		{Seq: 2, StartUS: 30, Target: 0, LatencyUS: 100000, ClientError: "timeout"},
		{Seq: 3, StartUS: 40, Target: 1, Status: 409, Code: "idempotency_in_progress", LatencyUS: 2500, RetryAfter: "1", RequestID: "qf-B"},
	}
	m := meta{Mode: "sustain", C: 4, WarmupMS: 1000, DurationMS: 2000}
	var buf bytes.Buffer
	if err := writeRaw(&buf, m, rows); err != nil {
		t.Fatal(err)
	}
	m2, rows2, err := readRaw(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if m2 != m || !slices.Equal(rows2, rows) {
		t.Fatalf("round trip:\n%+v %+v\n%+v %+v", m, rows, m2, rows2)
	}
	want, _ := json.Marshal(summarize(m, rows))
	got, _ := json.Marshal(summaryOf(t, buf.String()))
	if !bytes.Equal(got, want) {
		t.Fatalf("summary from the raw data:\n%s\nwant\n%s", got, want)
	}
}

func TestReadRawRejectsBadInput(t *testing.T) {
	for name, raw := range map[string]string{
		"no meta line":   strings.Join(header, ",") + "\n",
		"bad mode":       "# qfbench mode=x\n" + strings.Join(header, ",") + "\n",
		"wrong header":   "# qfbench mode=burst n=1\nseq,start_us\n",
		"bad number":     "# qfbench mode=burst n=1\n" + strings.Join(header, ",") + "\nx,0,0,200,,1,0,,0,,\n",
		"no duration":    "# qfbench mode=sustain c=1\n" + strings.Join(header, ",") + "\n",
		"missing fields": "# qfbench mode=burst n=1\n" + strings.Join(header, ",") + "\n0,0,0\n",
	} {
		if _, _, err := readRaw(strings.NewReader(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		r    result
		want string
	}{
		{result{Status: 200}, "ok"},
		{result{Status: 201}, "ok"},
		{result{Status: 429, Code: "rate_limited"}, "rate_limited"},
		{result{Status: 429, Code: "quota_exhausted"}, "quota_exhausted"},
		{result{Status: 429, Code: "something_else"}, "other_4xx"},
		{result{Status: 409, Code: "idempotency_in_progress"}, "other_4xx"},
		{result{Status: 422}, "other_4xx"},
		{result{Status: 503, Code: "ledger_unavailable"}, "5xx"},
		{result{Status: 504}, "5xx"},
		{result{ClientError: "timeout"}, "client_timeout"},
		{result{ClientError: "transport"}, "client_error"},
	} {
		if got := classify(tc.r); got != tc.want {
			t.Errorf("classify(%+v) = %s, want %s", tc.r, got, tc.want)
		}
	}
}

// 最近秩：n=1 时都是那个值；n=20 时 p50 是第 10 个、p95 第 19 个、p99 第 20 个；999 个样本 p99 不报，1000 个报第 990 个。
func TestPercentiles(t *testing.T) {
	seq := func(n int) []result {
		rows := make([]result, n)
		for i := range rows {
			rows[i] = result{Seq: i, Status: 200, LatencyUS: int64(n-i) * 1000} // 倒序给，汇总要自己排
		}
		return rows
	}
	m := meta{Mode: "burst", N: 1}
	for _, tc := range []struct {
		n             int
		p50, p95, p99 any
	}{
		{1, 1.0, 1.0, "insufficient samples"},
		{20, 10.0, 19.0, "insufficient samples"},
		{999, 500.0, 950.0, "insufficient samples"},
		{1000, 500.0, 950.0, 990.0},
	} {
		ok := summarize(m, seq(tc.n)).Classes["ok"]
		if ok.Count != tc.n || ok.P50 != tc.p50 || ok.P95 != tc.p95 || ok.P99 != tc.p99 || ok.Max != float64(tc.n) {
			t.Errorf("n=%d: %+v, want p50 %v p95 %v p99 %v max %d", tc.n, ok, tc.p50, tc.p95, tc.p99, tc.n)
		}
	}
	if p := percentile([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 99); p != 20 {
		t.Errorf("p99 of 1..20 = %d", p)
	}
}

// 预热的请求不计入任何一类；各类分开报，429、5xx、超时不进成功的百分位；吞吐 = 成功数 ÷ 测量时长。
func TestSummaryDenominators(t *testing.T) {
	rows := []result{
		{Status: 200, LatencyUS: 1000, Warmup: true},
		{Status: 200, LatencyUS: 999_000, Warmup: true},
		{Status: 200, LatencyUS: 2000},
		{Status: 200, LatencyUS: 4000},
		{Status: 429, Code: "rate_limited", LatencyUS: 500_000},
		{Status: 503, Code: "ledger_unavailable", LatencyUS: 2_000_000},
		{ClientError: "timeout", LatencyUS: 30_000_000},
	}
	s := summarize(meta{Mode: "sustain", C: 1, WarmupMS: 5000, DurationMS: 2000}, rows) // 吞吐的分母只有测量时长
	if s.Requests != 5 || s.WarmupRequests != 2 || *s.OKPerSecond != 1 {
		t.Fatalf("requests %d, warm-up %d, ok/s %v", s.Requests, s.WarmupRequests, *s.OKPerSecond)
	}
	ok := s.Classes["ok"]
	if ok.Count != 2 || ok.P50 != 2.0 || ok.Max != 4.0 {
		t.Fatalf("ok = %+v", ok)
	}
	for c, n := range map[string]int{"rate_limited": 1, "5xx": 1, "client_timeout": 1, "quota_exhausted": 0, "other_4xx": 0, "client_error": 0} {
		if s.Classes[c].Count != n {
			t.Errorf("%s: %d, want %d", c, s.Classes[c].Count, n)
		}
	}
	if s.Codes["429 rate_limited"] != 1 || s.Codes["503 ledger_unavailable"] != 1 || s.Codes["0  timeout"] != 1 {
		t.Errorf("codes = %v", s.Codes)
	}
}

// 记下服务端收到的请求：Key、请求体、幂等键。
type recorder struct {
	mu     sync.Mutex
	auth   map[string]int
	bodies map[string]int
	idem   map[string]int
}

func newRecorder() *recorder {
	return &recorder{auth: map[string]int{}, bodies: map[string]int{}, idem: map[string]int{}}
}

func (rc *recorder) note(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.auth[r.Header.Get("Authorization")]++
	rc.bodies[string(body)]++
	rc.idem[r.Header.Get("Idempotency-Key")]++
}

// 齐发：服务端把每个 POST 挂住，直到 n 个都在途才一起放行（5 秒等不齐就回 500）。一个一个发的话永远等不齐。
// 全部 200 说明 n 个请求是同时在途的；第一个 POST 到达时 n 个连接都已准备好（屏障在全部就绪之后才放开）；两个目标各一半，Key、请求体轮流用；原始数据里没有 Key。
func TestBurstReleasesTogether(t *testing.T) {
	const n = 20
	var arrived, prepared, early atomic.Int64
	all := make(chan struct{})
	rc := newRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			prepared.Add(1)
			return
		}
		if prepared.Load() != n { // 还有连接没准备好就发了：没有屏障
			early.Add(1)
		}
		rc.note(r)
		if arrived.Add(1) == n {
			close(all)
		}
		select {
		case <-all:
			w.Header().Set("x-request-id", fmt.Sprintf("qf-%d", arrived.Load()))
		case <-time.After(5 * time.Second):
			w.WriteHeader(500)
		}
	})
	a, b := httptest.NewServer(handler), httptest.NewServer(handler)
	defer a.Close()
	defer b.Close()
	raw, code := runCmd(t, testKeys, "burst", "-target", a.URL, "-target", b.URL, "-body", `{"m":1}`, "-body", `{"m":2}`, "-n", fmt.Sprint(n), "-idem-key", "same")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	s := summaryOf(t, raw)
	if s.Classes["ok"].Count != n || s.Targets["0"] != n/2 || s.Targets["1"] != n/2 {
		t.Fatalf("summary %+v", s)
	}
	if early.Load() != 0 {
		t.Fatalf("%d requests were sent before all %d connections were prepared", early.Load(), n)
	}
	if rc.auth["Bearer key-one-SECRET"] != n/2 || rc.bodies[`{"m":2}`] != n/2 || rc.idem["same"] != n {
		t.Fatalf("server saw keys %v, bodies %v, idempotency keys %v", rc.auth, rc.bodies, rc.idem)
	}
	if strings.Contains(raw, "SECRET") || strings.Contains(raw, `"m"`) {
		t.Fatal("the raw data holds a key or a body")
	}
}

// 持续：到点不再发新的，预热期间的有标记；每个请求一个新的幂等键；超时归为客户端超时。
func TestSustain(t *testing.T) {
	rc := newRecorder()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc.note(r)
		time.Sleep(5 * time.Millisecond)
	}))
	defer srv.Close()
	raw, code := runCmd(t, testKeys, "sustain", "-target", srv.URL, "-body", "{}", "-c", "3", "-warmup", "100ms", "-duration", "300ms", "-idem-each")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	m, rows, err := readRaw(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	var warm, measured int
	for i, r := range rows {
		// 到点之前决定发的才发；决定和记下开始时间之间只差建请求的那一点点
		if r.Seq != i || r.StartUS >= (m.WarmupMS+m.DurationMS+100)*1000 || r.Warmup != (r.StartUS < m.WarmupMS*1000) {
			t.Fatalf("row %d: %+v", i, r)
		}
		if r.Warmup {
			warm++
		} else {
			measured++
		}
	}
	if warm == 0 || measured == 0 {
		t.Fatalf("%d warm-up and %d measured requests", warm, measured)
	}
	if len(rc.idem) != len(rows) || rc.idem[""] != 0 {
		t.Fatalf("%d distinct idempotency keys for %d requests", len(rc.idem), len(rows))
	}
	if d := rc.auth["Bearer key-one-SECRET"] - rc.auth["Bearer key-two-SECRET"]; d < 0 || d > 1 {
		t.Fatalf("keys not used in turn: %v", rc.auth)
	}

	// 读完请求体，服务端才会察觉客户端断开（r.Context() 被取消）
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer hang.Close()
	raw, _ = runCmd(t, "k\n", "sustain", "-target", hang.URL, "-body", "{}", "-c", "1", "-warmup", "0s", "-duration", "50ms", "-timeout", "100ms")
	if s := summaryOf(t, raw); s.Classes["client_timeout"].Count != 1 || s.Classes["ok"].Count != 0 || *s.OKPerSecond != 0 {
		t.Fatalf("hanging server: %+v", s)
	}
}

// 连不上的目标归为客户端错误。
func TestConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	raw, code := runCmd(t, "k\n", "burst", "-target", url, "-body", "{}", "-n", "2")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if s := summaryOf(t, raw); s.Classes["client_error"].Count != 2 {
		t.Fatalf("summary %+v", s)
	}
}

// 正文读到一半连接断了：算客户端的错，状态码不算数（不能当成成功）。
func TestTruncatedBodyIsAClientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("0123456789"))
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer srv.Close()
	raw, code := runCmd(t, "k\n", "burst", "-target", srv.URL, "-body", "{}", "-n", "1")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	_, rows, err := readRaw(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ClientError != "transport" || rows[0].Status != 0 {
		t.Fatalf("rows = %+v, want one transport error without a status", rows)
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"nope"},
		{"sustain", "-body", "{}"},
		{"sustain", "-target", "http://x"},
		{"sustain", "-target", "http://x", "-body", "{}", "-c", "0"},
		{"burst", "-target", "http://x", "-body", "{}", "-idem-key", "k", "-idem-each"},
		{"summarize", "extra"},
	} {
		if _, code := runCmd(t, "k\n", args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
	if _, code := runCmd(t, "\n\n", "burst", "-target", "http://x", "-body", "{}"); code != 1 {
		t.Errorf("no keys: exit %d, want 1", code)
	}
}
