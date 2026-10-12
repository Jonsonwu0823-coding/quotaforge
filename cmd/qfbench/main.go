// Command qfbench 是 QF 的压测和齐发工具，只用标准库。三个子命令：
//
//	qfbench sustain   -target URL [-target URL…] -body JSON [-body JSON…] -c N -warmup 5s -duration 30s [-timeout 30s] [-idem-each]
//	qfbench burst     -target URL [-target URL…] -body JSON [-body JSON…] -n N [-idem-key K | -idem-each] [-timeout 60s]
//	qfbench summarize   stdin 是原始数据，stdout 写汇总（JSON）
//
// sustain 是闭环压测：C 个 worker 各自发完一个再发下一个，先预热再测；burst 让 N 个请求在屏障处一起放开（连接事先建好）。
// 第 i 个请求发给第 i mod T 个目标，用第 i mod B 个请求体、第 i mod K 把 Key，路径是 /v1/chat/completions。
// Key 从 stdin 读，一行一把，不进命令行，也不进任何输出；原始数据里没有 Key 和请求、响应的正文。
// sustain 和 burst 只往 stdout 写逐请求的原始数据（CSV）；汇总一律由 summarize 从原始数据算出来，所以拿原始数据复算，
// 走的就是同一段代码。
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const usage = `usage:
  qfbench sustain   -target URL [-target URL...] -body JSON [-body JSON...] -c N -warmup 5s -duration 30s [-timeout 30s] [-idem-each]
  qfbench burst     -target URL [-target URL...] -body JSON [-body JSON...] -n N [-idem-key K | -idem-each] [-timeout 60s]
  qfbench summarize  (raw data on stdin, summary JSON on stdout)
keys: one per line on stdin (sustain, burst)
`

var errUsage = errors.New("usage")

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := dispatch(args, stdin, stdout)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprint(stderr, usage)
		return 2
	default:
		fmt.Fprintln(stderr, "qfbench:", err)
		return 1
	}
}

// repeated 是可以写多次的参数。
type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, " ") }
func (r *repeated) Set(v string) error { *r = append(*r, v); return nil }

func dispatch(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	out := bufio.NewWriter(stdout)
	defer out.Flush()
	if args[0] == "summarize" {
		if len(args) != 1 {
			return errUsage
		}
		m, rows, err := readRaw(stdin)
		if err != nil {
			return err
		}
		return writeSummary(out, summarize(m, rows))
	}

	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var targets, bodies repeated
	fs.Var(&targets, "target", "base URL of QF or the upstream, e.g. http://quotaforge:8080 (repeat for round robin)")
	fs.Var(&bodies, "body", "request body (repeat for round robin)")
	idemEach := fs.Bool("idem-each", false, "a new Idempotency-Key on every request")
	var c, n *int
	var warmup, duration *time.Duration
	var idemKey *string
	timeout := 30 * time.Second
	switch args[0] {
	case "sustain":
		c = fs.Int("c", 1, "concurrent workers")
		warmup = fs.Duration("warmup", 5*time.Second, "warm-up period, not counted")
		duration = fs.Duration("duration", 30*time.Second, "measured period")
	case "burst":
		n = fs.Int("n", 10, "requests released together")
		idemKey = fs.String("idem-key", "", "the same Idempotency-Key on every request")
		timeout = 60 * time.Second
	default:
		return errUsage
	}
	fs.DurationVar(&timeout, "timeout", timeout, "client timeout per request")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 || len(targets) == 0 || len(bodies) == 0 || timeout <= 0 {
		return errUsage
	}
	keys, err := readKeys(stdin)
	if err != nil {
		return err
	}
	b := &bench{targets: targets, bodies: bodies, keys: keys, timeout: timeout, idemEach: *idemEach}
	var m meta
	var rows []result
	switch {
	case c != nil:
		if *c < 1 || *warmup < 0 || *duration <= 0 {
			return errUsage
		}
		m = meta{Mode: "sustain", C: *c, WarmupMS: warmup.Milliseconds(), DurationMS: duration.Milliseconds()}
		rows = b.sustain(*c, *warmup, *duration)
	default:
		if *n < 1 || (*idemKey != "" && *idemEach) {
			return errUsage
		}
		b.idemKey = *idemKey
		m = meta{Mode: "burst", N: *n}
		rows = b.burst(*n)
	}
	return writeRaw(out, m, rows)
}

// readKeys 读 stdin 上的 Key，一行一把，空行跳过。至少要有一把（直连上游时随便写一行）。
func readKeys(r io.Reader) ([]string, error) {
	var keys []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if k := strings.TrimSpace(sc.Text()); k != "" {
			keys = append(keys, k)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read keys: %w", err)
	}
	if len(keys) == 0 {
		return nil, errors.New("no keys on stdin (one per line; any line will do for an upstream that ignores them)")
	}
	return keys, nil
}
