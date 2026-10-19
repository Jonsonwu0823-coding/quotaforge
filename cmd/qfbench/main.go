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
	b, mode, err := parseBench(args)
	if err != nil {
		return err
	}
	if b.keys, err = readKeys(stdin); err != nil {
		return err
	}
	m, rows, err := mode.run(b)
	if err != nil {
		return err
	}
	return writeRaw(out, m, rows)
}

// parseBench 读 sustain、burst 共有的参数并校验，返回压测的设置和这种压测自己的参数。各自的参数在跑之前（读完 Key 之后）才校验。
func parseBench(args []string) (*bench, benchMode, error) {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	b := &bench{timeout: 30 * time.Second}
	fs.Var((*repeated)(&b.targets), "target", "base URL of QF or the upstream, e.g. http://quotaforge:8080 (repeat for round robin)")
	fs.Var((*repeated)(&b.bodies), "body", "request body (repeat for round robin)")
	fs.BoolVar(&b.idemEach, "idem-each", false, "a new Idempotency-Key on every request")
	var mode benchMode
	switch args[0] {
	case "sustain":
		mode = &sustainMode{}
	case "burst":
		mode = &burstMode{}
		b.timeout = 60 * time.Second
	default:
		return nil, nil, errUsage
	}
	mode.define(fs)
	fs.DurationVar(&b.timeout, "timeout", b.timeout, "client timeout per request")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 || len(b.targets) == 0 || len(b.bodies) == 0 || b.timeout <= 0 {
		return nil, nil, errUsage
	}
	return b, mode, nil
}

// benchMode 是 sustain、burst 各自的参数：define 在解析之前定义它们，run 校验它们、跑这种压测。
type benchMode interface {
	define(fs *flag.FlagSet)
	run(b *bench) (meta, []result, error)
}

// sustainMode 是 sustain 的参数：并发数、预热和计入结果的时长。
type sustainMode struct {
	c                int
	warmup, duration time.Duration
}

func (sm *sustainMode) define(fs *flag.FlagSet) {
	fs.IntVar(&sm.c, "c", 1, "concurrent workers")
	fs.DurationVar(&sm.warmup, "warmup", 5*time.Second, "warm-up period, not counted")
	fs.DurationVar(&sm.duration, "duration", 30*time.Second, "measured period")
}

func (sm *sustainMode) run(b *bench) (meta, []result, error) {
	if sm.c < 1 || sm.warmup < 0 || sm.duration <= 0 {
		return meta{}, nil, errUsage
	}
	m := meta{Mode: "sustain", C: sm.c, WarmupMS: sm.warmup.Milliseconds(), DurationMS: sm.duration.Milliseconds()}
	return m, b.sustain(sm.c, sm.warmup, sm.duration), nil
}

// burstMode 是 burst 的参数：一起放出的请求数、每个请求都带的同一个 Idempotency-Key。
type burstMode struct {
	n       int
	idemKey string
}

func (bm *burstMode) define(fs *flag.FlagSet) {
	fs.IntVar(&bm.n, "n", 10, "requests released together")
	fs.StringVar(&bm.idemKey, "idem-key", "", "the same Idempotency-Key on every request")
}

func (bm *burstMode) run(b *bench) (meta, []result, error) {
	if bm.n < 1 || (bm.idemKey != "" && b.idemEach) {
		return meta{}, nil, errUsage
	}
	b.idemKey = bm.idemKey
	return meta{Mode: "burst", N: bm.n}, b.burst(bm.n), nil
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
