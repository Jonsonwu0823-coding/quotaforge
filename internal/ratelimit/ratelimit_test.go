package ratelimit_test

import (
	"context"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

var ctx = context.Background()

// env 是一个用真 Redis、只属于当前测试的限流器（自己的 key 前缀），加一个直接读 Redis 的客户端。
type env struct {
	l   *ratelimit.Limiter
	rdb *redis.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	url := dbtest.RedisURL(t)
	return &env{l: open(t, url, dbtest.RedisPrefix()), rdb: rawClient(t, url)}
}

func open(t *testing.T, url, prefix string) *ratelimit.Limiter {
	t.Helper()
	l, err := ratelimit.Open(url, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func rawClient(t *testing.T, url string) *redis.Client {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// now 是 Redis 的时间（毫秒），和脚本用的是同一个钟。
func (e *env) now(t *testing.T) int64 {
	t.Helper()
	tm, err := e.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	return tm.UnixMilli()
}

// tokens 读桶里存着的令牌数；桶不存在时 ok 为 false。
func (e *env) tokens(t *testing.T, b ratelimit.Bucket) (v float64, ok bool) {
	t.Helper()
	s, err := e.rdb.HGet(ctx, e.l.Key(b), "tokens").Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	v, err = strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v, true
}

// ms 读桶上次写入时的 Redis 毫秒时间。
func (e *env) ms(t *testing.T, b ratelimit.Bucket) int64 {
	t.Helper()
	v, err := e.rdb.HGet(ctx, e.l.Key(b), "ms").Int64()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (e *env) admit(t *testing.T, bs ...ratelimit.Bucket) *ratelimit.Denial {
	t.Helper()
	d, err := e.l.Admit(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (e *env) adjust(t *testing.T, bs ...ratelimit.Bucket) {
	t.Helper()
	if err := e.l.Adjust(ctx, bs); err != nil {
		t.Fatal(err)
	}
}

func bucket(id string, limit, cost int64) ratelimit.Bucket {
	return ratelimit.Bucket{Layer: "key", ID: id, Metric: "rpm", Limit: limit, Cost: cost}
}

// retryRange 是 Retry-After 允许的范围：缺口 need 个令牌、按 limit 每分钟补；从扣到缺口出现到被拒之间最多过了 elapsed 毫秒，
// 期间补回的令牌让等待变短。
func retryRange(need float64, limit, elapsed int64) (lo, hi int) {
	ms := need * 60000 / float64(limit)
	hi = max(1, int(math.Ceil(ms/1000)))
	lo = max(1, int(math.Ceil((ms-float64(elapsed))/1000)))
	return lo, hi
}

// 满桶连续放行 L 次，第 L+1 次被拒；Retry-After 是补回 1 个令牌所需的秒数。
func TestAdmitUntilEmpty(t *testing.T) {
	e := newEnv(t)
	b := bucket("1", 6, 1) // 每 10 秒补 1 个
	t0 := e.now(t)
	for i := range 6 {
		if d := e.admit(t, b); d != nil {
			t.Fatalf("admission %d denied: %+v", i+1, d)
		}
	}
	d := e.admit(t, b)
	elapsed := e.now(t) - t0
	if d == nil {
		t.Fatal("admission 7 of a 6-per-minute bucket was allowed")
	}
	lo, hi := retryRange(1, 6, elapsed)
	if d.Bucket != b || d.RetryAfter < lo || d.RetryAfter > hi {
		t.Fatalf("denial = %+v, want bucket %+v and Retry-After in [%d, %d]", d, b, lo, hi)
	}
}

// 全有或全无：一个桶不够时，别的桶一个令牌都不扣，没碰过的桶也不会被建出来。
func TestAllOrNothing(t *testing.T) {
	e := newEnv(t)
	wide, narrow, fresh := bucket("wide", 100, 1), bucket("narrow", 1, 1), bucket("fresh", 100, 1)
	if d := e.admit(t, wide, narrow); d != nil {
		t.Fatalf("first admission denied: %+v", d)
	}
	before, err := e.rdb.HGetAll(ctx, e.l.Key(wide)).Result()
	if err != nil {
		t.Fatal(err)
	}
	d := e.admit(t, wide, fresh, narrow)
	if d == nil || d.Bucket != narrow {
		t.Fatalf("denial = %+v, want the narrow bucket", d)
	}
	after, err := e.rdb.HGetAll(ctx, e.l.Key(wide)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if before["tokens"] != after["tokens"] || before["ms"] != after["ms"] {
		t.Fatalf("the wide bucket changed on a denial: %v -> %v", before, after)
	}
	if _, ok := e.tokens(t, fresh); ok {
		t.Fatal("a denial created the untouched bucket")
	}
}

// 不够的桶有几个时，Retry-After 取等得最久的那个。
func TestRetryAfterIsTheLongestWait(t *testing.T) {
	e := newEnv(t)
	short, long := bucket("short", 60, 60), bucket("long", 6, 6) // 补 1 个分别要 1 秒、10 秒
	if d := e.admit(t, short, long); d != nil {
		t.Fatalf("first admission denied: %+v", d)
	}
	short.Cost, long.Cost = 1, 1
	if d := e.admit(t, short, long); d == nil || d.Bucket != long || d.RetryAfter < 9 {
		t.Fatalf("denial = %+v, want the 10-second bucket", d)
	}
}

// 花费超过整桶的请求：桶满时放行，桶变成负数；不满时不放行；之后的请求等它补回来。
func TestCostAboveCapacity(t *testing.T) {
	e := newEnv(t)
	small := bucket("big", 100, 1)
	big := bucket("big", 100, 150)
	if d := e.admit(t, small); d != nil {
		t.Fatalf("first admission denied: %+v", d)
	}
	if d := e.admit(t, big); d == nil {
		t.Fatal("a request above capacity was allowed into a bucket that is not full")
	}

	e2 := newEnv(t)
	t0 := e2.now(t)
	if d := e2.admit(t, big); d != nil {
		t.Fatalf("a request above capacity was denied on a full bucket: %+v", d)
	}
	v, _ := e2.tokens(t, big)
	if v > -50 || v < -50-1 {
		t.Fatalf("tokens after spending 150 of 100 = %v, want about -50", v)
	}
	d := e2.admit(t, small)
	elapsed := e2.now(t) - t0
	lo, hi := retryRange(51, 100, elapsed) // 从 -50 补到 1
	if d == nil || d.RetryAfter < lo || d.RetryAfter > hi {
		t.Fatalf("denial = %+v, want Retry-After in [%d, %d]", d, lo, hi)
	}
}

// 修正：加回不超过容量；多扣不检查，桶可以变成负数。
func TestAdjust(t *testing.T) {
	e := newEnv(t)
	b := bucket("adj", 100, 60)
	if d := e.admit(t, b); d != nil {
		t.Fatalf("admission denied: %+v", d)
	}
	b.Cost = -1000
	e.adjust(t, b)
	if v, _ := e.tokens(t, b); v != 100 {
		t.Fatalf("tokens after refunding more than was spent = %v, want the capacity 100", v)
	}
	b.Cost = 250
	e.adjust(t, b)
	if v, _ := e.tokens(t, b); v > -150 || v < -151 {
		t.Fatalf("tokens after charging 250 = %v, want about -150", v)
	}
	b.Cost = 1
	if d := e.admit(t, b); d == nil {
		t.Fatal("admitted after the bucket went negative")
	}
}

// 每次写都设过期时间：补满所需的时间再加 1 秒。过期太短的话桶会提前删掉、重新变满，限额就失效了，
// 所以 TTL 要落在 [补满所需时间, 补满所需时间 + 1 秒] 之内：补满所需时间 = (L − v) × 60000 / L 毫秒，
// v 是写入后 hash 里的令牌数。修正不检查：桶里不够也照扣，令牌数直接核对。
func TestEveryWriteSetsExpiry(t *testing.T) {
	e := newEnv(t)
	const limit = 60 // 补 1 个要 1 秒
	check := func(b ratelimit.Bucket, lo, hi float64) {
		t.Helper()
		v, ok := e.tokens(t, b)
		ttl, err := e.rdb.PTTL(ctx, e.l.Key(b)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !ok || v < lo || v > hi {
			t.Fatalf("%s: tokens %v, want in [%v, %v]", e.l.Key(b), v, lo, hi)
		}
		refill := time.Duration((limit - v) * 60000 / limit * float64(time.Millisecond))
		if ttl < refill || ttl > refill+time.Second+time.Millisecond {
			t.Fatalf("%s: TTL %v with %v tokens, want in [%v, %v + 1s]", e.l.Key(b), ttl, v, refill, refill)
		}
	}
	b := bucket("ttl", limit, 30)
	e.admit(t, b)
	check(b, 30, 31) // 补满要约 30 秒
	b.Cost = 90
	e.adjust(t, b) // 约 30 − 90：补满要约 120 秒
	check(b, -60, -59)
	b.Cost = -1000
	e.adjust(t, b) // 满桶：只剩 1 秒
	check(b, limit, limit)
}

// 补充速度：每毫秒补 L/60000。扣空一个桶，等 Redis 的时间过去几百毫秒，用花费 0 的修正写一次（脚本先补到现在），
// 读出 hash 里的令牌数和毫秒时间：令牌数正好等于两次写之间的毫秒数 × L / 60000。时间只看 Redis 的，不靠 sleep 判对错。
func TestRefillRate(t *testing.T) {
	e := newEnv(t)
	const limit = 6000 // 每毫秒补 0.1 个
	b := bucket("refill", limit, limit)
	if d := e.admit(t, b); d != nil {
		t.Fatalf("admission denied: %+v", d)
	}
	ms0 := e.ms(t, b)
	for e.now(t) < ms0+300 {
		time.Sleep(20 * time.Millisecond) // 只是轮询的间隔；判定用 Redis 的时间
	}
	b.Cost = 0
	e.adjust(t, b)
	ms1 := e.ms(t, b)
	v, _ := e.tokens(t, b)
	want := float64(ms1-ms0) * limit / 60000
	if ms1-ms0 < 300 || math.Abs(v-want) > 1e-6 {
		t.Fatalf("after %d ms: tokens %v, want %v (L/60000 per ms)", ms1-ms0, v, want)
	}
}

// 补充封顶到容量：用花费 0 的修正写一个满桶（L = 6000），等 Redis 的时间过去 300 毫秒
// （满桶的过期时间是 1 秒，还没过期），放行 3000：剩下的必须正好是 3000，不封顶时会多出补的约 30 个。
// 两次写之间超过 1 秒的那一次，满桶可能已经过期重建，判不了，重试。
func TestRefillCapped(t *testing.T) {
	const limit = 6000
	for try := range 5 {
		e := newEnv(t)
		b := ratelimit.Bucket{Layer: "key", ID: "cap", Metric: "tpm", Limit: limit, Cost: 0}
		e.adjust(t, b)
		if v, ok := e.tokens(t, b); !ok || v != limit {
			t.Fatalf("after a zero adjustment: tokens %v (exists %v), want %d", v, ok, limit)
		}
		ms0 := e.ms(t, b)
		for e.now(t) < ms0+300 {
			time.Sleep(20 * time.Millisecond) // 只是轮询的间隔；判定用 Redis 的时间
		}
		b.Cost = limit / 2
		if d := e.admit(t, b); d != nil {
			t.Fatalf("admission denied: %+v", d)
		}
		if elapsed := e.ms(t, b) - ms0; elapsed >= 1000 {
			t.Logf("try %d: %d ms between the writes, inconclusive", try, elapsed)
			continue
		}
		if v, _ := e.tokens(t, b); v != limit/2 {
			t.Fatalf("admitting %d from a full bucket of %d left %v tokens, want exactly %d", limit/2, limit, v, limit/2)
		}
		return
	}
	t.Fatal("no conclusive try")
}

// 两个客户端（像两个 QF 实例）各起多个 goroutine，同时抢一个桶：放行的合计在 [L, L + L·t/60] 之内，
// t 是测试前后读到的 Redis 时间之差。脚本在 Redis 里原子执行，所以不会多放。
func TestConcurrentAdmission(t *testing.T) {
	const limit, workers, tries = 50, 16, 10
	url, prefix := dbtest.RedisURL(t), dbtest.RedisPrefix()
	e := &env{l: open(t, url, prefix), rdb: rawClient(t, url)}
	clients := []*ratelimit.Limiter{e.l, open(t, url, prefix)}
	b := bucket("race", limit, 1)

	var allowed, failed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, l := range clients {
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range tries {
					d, err := l.Admit(ctx, []ratelimit.Bucket{b})
					switch {
					case err != nil:
						failed.Add(1)
					case d == nil:
						allowed.Add(1)
					}
				}
			}()
		}
	}
	t0 := e.now(t)
	close(start)
	wg.Wait()
	elapsed := e.now(t) - t0
	if failed.Load() != 0 {
		t.Fatalf("%d admissions failed", failed.Load())
	}
	hi := limit + limit*elapsed/60000
	if n := allowed.Load(); n < limit || n > hi {
		t.Fatalf("allowed %d of %d attempts in %d ms, want in [%d, %d]", n, 2*workers*tries, elapsed, limit, hi)
	}
}

// Redis 连不上：端口拒绝连接时直接返回错误。
func TestRedisRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	l := open(t, "redis://"+addr+"/0", "x:")
	c, cancel := context.WithTimeout(ctx, config.RateLimitTimeout)
	defer cancel()
	if d, err := l.Admit(c, []ratelimit.Bucket{bucket("1", 1, 1)}); err == nil || d != nil {
		t.Fatalf("Admit against a closed port = %+v, %v", d, err)
	}
}

// Redis 接受连接却不回答：调用在 context 的期限（0.5 秒）返回，不等 go-redis 自己几秒的读超时。
func TestRedisHangs(t *testing.T) {
	addr := hangingListener(t)
	l := open(t, "redis://"+addr+"/0", "x:")
	for _, admit := range []bool{true, false} {
		c, cancel := context.WithTimeout(ctx, config.RateLimitTimeout)
		start := time.Now()
		var err error
		if admit {
			_, err = l.Admit(c, []ratelimit.Bucket{bucket("1", 1, 1)})
		} else {
			err = l.Adjust(c, []ratelimit.Bucket{bucket("1", 1, 1)})
		}
		took := time.Since(start)
		cancel()
		if err == nil || took > config.RateLimitTimeout+500*time.Millisecond {
			t.Fatalf("admit=%v against a silent server: %v after %v", admit, err, took)
		}
	}
}

// hangingListener 接受连接、读走发来的字节，从不回答。
func hangingListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go func() { _, _ = c.Read(make([]byte, 1<<16)) }()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// URL 里的密码不出现在错误里：连不上时的错误，以及 URL 格式不对时的错误。
func TestErrorsHideThePassword(t *testing.T) {
	const secret = "s3cret-redis-password-XYZ"
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	l := open(t, "redis://:"+secret+"@"+addr+"/0", "x:")
	c, cancel := context.WithTimeout(ctx, config.RateLimitTimeout)
	defer cancel()
	_, admitErr := l.Admit(c, []ratelimit.Bucket{bucket("1", 1, 1)})
	if admitErr == nil || strings.Contains(admitErr.Error(), secret) {
		t.Fatalf("Admit error = %v", admitErr)
	}
	for _, bad := range []string{"redis://:" + secret + "@host:notaport/0", "http://:" + secret + "@host/0", "redis://:" + secret + "@host:6379/notadb"} {
		if _, err := ratelimit.Open(bad, "x:"); err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("Open(bad URL) error = %v", err)
		}
	}
}
