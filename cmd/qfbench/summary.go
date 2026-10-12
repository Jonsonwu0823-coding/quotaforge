package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// result 是一个请求的原始记录，也是 CSV 的一行。StartUS 从这一轮的起点算（sustain 是开跑，burst 是放开的那一刻）。
// 没拿到完整响应时 ClientError 是 timeout 或 transport，Status 为 0。
type result struct {
	Seq         int
	StartUS     int64
	Target      int
	Status      int
	Code        string
	LatencyUS   int64
	Warmup      bool
	RequestID   string
	Replayed    bool
	RetryAfter  string
	ClientError string
}

// meta 是这一轮的参数，写在原始数据的第一行（# qfbench mode=… c=…），summarize 靠它算吞吐。
type meta struct {
	Mode       string
	C, N       int
	WarmupMS   int64
	DurationMS int64
}

var header = []string{"seq", "start_us", "target", "status", "code", "latency_us", "warmup", "request_id", "replayed", "retry_after", "client_error"}

func (m meta) line() string {
	if m.Mode == "burst" {
		return fmt.Sprintf("# qfbench mode=burst n=%d", m.N)
	}
	return fmt.Sprintf("# qfbench mode=sustain c=%d warmup_ms=%d duration_ms=%d", m.C, m.WarmupMS, m.DurationMS)
}

func b01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func writeRaw(w io.Writer, m meta, rows []result) error {
	if _, err := fmt.Fprintln(w, m.line()); err != nil {
		return err
	}
	cw := csv.NewWriter(w)
	_ = cw.Write(header)
	for _, r := range rows {
		_ = cw.Write([]string{strconv.Itoa(r.Seq), strconv.FormatInt(r.StartUS, 10), strconv.Itoa(r.Target), strconv.Itoa(r.Status),
			r.Code, strconv.FormatInt(r.LatencyUS, 10), b01(r.Warmup), r.RequestID, b01(r.Replayed), r.RetryAfter, r.ClientError})
	}
	cw.Flush()
	return cw.Error()
}

// readRaw 读 writeRaw 写的原始数据；格式不对就报错，不猜。
func readRaw(r io.Reader) (meta, []result, error) {
	var m meta
	br := bufio.NewReader(r)
	first, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(first, "# qfbench ") {
		return m, nil, errors.New("raw data: the first line must be '# qfbench mode=...'")
	}
	for _, kv := range strings.Fields(strings.TrimPrefix(first, "# qfbench ")) {
		k, v, _ := strings.Cut(kv, "=")
		var n int64
		if k != "mode" {
			if n, err = strconv.ParseInt(v, 10, 64); err != nil {
				return m, nil, fmt.Errorf("raw data: %s in the first line", kv)
			}
		}
		switch k {
		case "mode":
			m.Mode = v
		case "c":
			m.C = int(n)
		case "n":
			m.N = int(n)
		case "warmup_ms":
			m.WarmupMS = n
		case "duration_ms":
			m.DurationMS = n
		}
	}
	if m.Mode != "sustain" && m.Mode != "burst" || m.Mode == "sustain" && m.DurationMS <= 0 {
		return m, nil, errors.New("raw data: the first line has no valid mode and duration")
	}
	cr := csv.NewReader(br)
	cr.FieldsPerRecord = len(header)
	recs, err := cr.ReadAll()
	if err != nil {
		return m, nil, fmt.Errorf("raw data: %w", err)
	}
	if len(recs) == 0 || !slices.Equal(recs[0], header) {
		return m, nil, errors.New("raw data: missing or unexpected header")
	}
	rows := make([]result, 0, len(recs)-1)
	for i, rec := range recs[1:] {
		var r result
		var e [4]error
		r.Seq, e[0] = strconv.Atoi(rec[0])
		r.StartUS, e[1] = strconv.ParseInt(rec[1], 10, 64)
		r.Target, e[2] = strconv.Atoi(rec[2])
		r.Status, e[3] = strconv.Atoi(rec[3])
		lat, err := strconv.ParseInt(rec[5], 10, 64)
		if err := errors.Join(e[0], e[1], e[2], e[3], err); err != nil {
			return m, nil, fmt.Errorf("raw data line %d: %w", i+3, err)
		}
		r.Code, r.LatencyUS, r.Warmup, r.RequestID = rec[4], lat, rec[6] == "1", rec[7]
		r.Replayed, r.RetryAfter, r.ClientError = rec[8] == "1", rec[9], rec[10]
		rows = append(rows, r)
	}
	return m, rows, nil
}

// 汇总的分类，按这个顺序报，个数为 0 的也报（分母都写出来）。
var classes = []string{"ok", "rate_limited", "quota_exhausted", "other_4xx", "5xx", "client_timeout", "client_error"}

// classify：没拿到响应的按客户端的错分；2xx 成功；429 按 error.code 分两种；其余 5xx 以下的非 2xx 都算其它 4xx（QF 不返回 3xx）。
func classify(r result) string {
	switch {
	case r.ClientError == "timeout":
		return "client_timeout"
	case r.ClientError != "":
		return "client_error"
	case r.Status >= 200 && r.Status < 300:
		return "ok"
	case r.Status == 429 && r.Code == "rate_limited":
		return "rate_limited"
	case r.Status == 429 && r.Code == "quota_exhausted":
		return "quota_exhausted"
	case r.Status >= 500:
		return "5xx"
	default:
		return "other_4xx"
	}
}

// classSummary 是一类请求的个数和耗时（毫秒）。p99 在样本少于 1000 个时是 "insufficient samples"。
type classSummary struct {
	Count int `json:"count"`
	P50   any `json:"p50_ms,omitempty"`
	P95   any `json:"p95_ms,omitempty"`
	P99   any `json:"p99_ms,omitempty"`
	Max   any `json:"max_ms,omitempty"`
}

type summary struct {
	Mode           string                  `json:"mode"`
	Concurrency    int                     `json:"concurrency,omitempty"`
	N              int                     `json:"n,omitempty"`
	WarmupMS       int64                   `json:"warmup_ms,omitempty"`
	DurationMS     int64                   `json:"duration_ms,omitempty"`
	Requests       int                     `json:"requests"`
	WarmupRequests int                     `json:"warmup_requests"`
	OKPerSecond    *float64                `json:"ok_per_second,omitempty"`
	StartSpreadMS  *float64                `json:"start_spread_ms,omitempty"`
	Classes        map[string]classSummary `json:"classes"`
	Codes          map[string]int          `json:"codes"`
	Targets        map[string]int          `json:"targets"`
}

// percentile 是最近秩：排好序的 n 个值里第 ceil(p·n/100) 个（p 是百分数）。
func percentile(sorted []int64, p int) int64 {
	rank := (p*len(sorted) + 99) / 100
	return sorted[max(rank, 1)-1]
}

func ms(us int64) float64 { return float64(us) / 1000 }

// summarize 只看非预热的请求：各类的个数和耗时分开报，成功的百分位里没有 429、5xx 和超时；
// sustain 的吞吐是成功数 ÷ 测量时长；burst 报出发时间的最大差。
func summarize(m meta, rows []result) summary {
	s := summary{Mode: m.Mode, Concurrency: m.C, N: m.N, WarmupMS: m.WarmupMS, DurationMS: m.DurationMS,
		Classes: map[string]classSummary{}, Codes: map[string]int{}, Targets: map[string]int{}}
	latencies := map[string][]int64{}
	first, last := int64(-1), int64(0)
	for _, r := range rows {
		if r.Warmup {
			s.WarmupRequests++
			continue
		}
		s.Requests++
		c := classify(r)
		latencies[c] = append(latencies[c], r.LatencyUS)
		s.Targets[strconv.Itoa(r.Target)]++
		if c != "ok" {
			s.Codes[strings.TrimSpace(fmt.Sprintf("%d %s %s", r.Status, r.Code, r.ClientError))]++
		}
		if first < 0 || r.StartUS < first {
			first = r.StartUS
		}
		last = max(last, r.StartUS)
	}
	for _, c := range classes {
		l := latencies[c]
		cs := classSummary{Count: len(l)}
		if len(l) > 0 {
			slices.Sort(l)
			cs.P50, cs.P95, cs.Max = ms(percentile(l, 50)), ms(percentile(l, 95)), ms(l[len(l)-1])
			cs.P99 = "insufficient samples"
			if len(l) >= 1000 {
				cs.P99 = ms(percentile(l, 99))
			}
		}
		s.Classes[c] = cs
	}
	switch {
	case m.Mode == "sustain":
		v := float64(len(latencies["ok"])) / (float64(m.DurationMS) / 1000)
		s.OKPerSecond = &v
	case s.Requests > 0:
		v := ms(last - first)
		s.StartSpreadMS = &v
	}
	return s
}

func writeSummary(w io.Writer, s summary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
