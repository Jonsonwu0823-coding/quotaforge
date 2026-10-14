// Package ratelimit 是放在 Redis 里的令牌桶。一次请求涉及的全部桶由一个 Lua 脚本原子处理（bucket.lua），
// 所以多个 QF 实例、多个连接同时准入也不会多放。哪些桶、每个桶花多少，由调用方决定。
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

//go:embed bucket.lua
var bucketLua string

var bucketScript = redis.NewScript(bucketLua)

// go-redis 自己的日志丢掉：影响请求的错误都作为返回值回到调用方，记在那次调用的日志里；
// 它自己的日志格式不归 QF 管，不能保证不带连接信息，而且默认直接写 stderr，不是 QF 的 JSON 日志。
func init() { redis.SetLogger(discard{}) }

type discard struct{}

func (discard) Printf(context.Context, string, ...any) {}

// Bucket 是一个令牌桶：容量是每分钟的限额 Limit，每毫秒补 Limit/60000，补满为止。
type Bucket struct {
	Layer  string // key、account、upstream
	ID     string // 这一层里的标识：Key 号、账户号、上游名
	Metric string // rpm、tpm
	Limit  int64  // 每分钟的限额，大于 0
	Cost   int64  // Admit 要扣的令牌；Adjust 里负数表示加回
}

// Name 是日志里的名字，例如 key/rpm。
func (b Bucket) Name() string { return b.Layer + "/" + b.Metric }

// Denial 是一次拒绝：不够的桶里补够所需时间最长的那个，以及向上取整的秒数（至少 1）。
type Denial struct {
	Bucket     Bucket
	RetryAfter int
}

// Limiter 用一个 Redis 连接池执行脚本。可以并发使用。
type Limiter struct {
	rdb    *redis.Client
	prefix string
}

// Open 按 redis://[:密码@]主机:端口/库号 连接 Redis（不立即拨号）。prefix 加在每个桶的 key 前面。
// URL 里可能有密码：解析出错时只说格式不对，不带原文（url.Parse 的错误里有整个 URL）。
func Open(url, prefix string) (*Limiter, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, errors.New("redis: invalid URL")
	}
	return newLimiter(opt, prefix), nil
}

// newLimiter 把限流要求的设置加在 opt 上，再建连接池。
func newLimiter(opt *redis.Options, prefix string) *Limiter {
	// 没有这一项，Redis 接受连接却不回答时，调用要等 go-redis 自己的读超时，而不是 context 的期限。
	opt.ContextTimeoutEnabled = true
	// 准入不是幂等的：读超时之后重试可能扣两次。出错就直接按不可用处理。
	opt.MaxRetries = -1
	// 拨号也要跟着请求的期限走。请求的期限是 RateLimitTimeout，但 go-redis 在后台拨号，拨号的 ctx 不随请求取消：
	// 默认每次 5 秒、最多 5 次，拨号活得比请求久。Redis 停着时，名字解析要好几秒才失败（Docker Desktop 上约 4 秒），
	// 同一个名字的并发查询又会合并，Redis 回来之后的请求会等进这次过时的查询，头几秒仍是 503。
	// 所以每次拨号最多 RateLimitTimeout，只试一次。
	opt.DialTimeout = config.RateLimitTimeout
	opt.DialerRetries = 1
	return &Limiter{rdb: redis.NewClient(opt), prefix: prefix}
}

// Close 关闭连接池。
func (l *Limiter) Close() error { return l.rdb.Close() }

// Admit 检查并扣除：每个桶都满足“令牌数 ≥ min(花费, 容量)”才放行，并在每个桶里扣掉花费；
// 只要有一个不满足，一个都不扣，返回 Denial。放行时返回 nil, nil。
func (l *Limiter) Admit(ctx context.Context, buckets []Bucket) (*Denial, error) {
	res, err := l.run(ctx, true, buckets)
	if err != nil || res[0] == 0 {
		return nil, err
	}
	return &Denial{Bucket: buckets[res[0]-1], RetryAfter: int(res[1])}, nil
}

// Adjust 不检查，直接在每个桶里扣掉花费（负数就是加回），结果不超过容量，可以是负数。
func (l *Limiter) Adjust(ctx context.Context, buckets []Bucket) error {
	_, err := l.run(ctx, false, buckets)
	return err
}

func (l *Limiter) run(ctx context.Context, admit bool, buckets []Bucket) ([]int64, error) {
	keys := make([]string, len(buckets))
	args := make([]any, 1, 1+2*len(buckets))
	args[0] = 0
	if admit {
		args[0] = 1
	}
	for i, b := range buckets {
		keys[i] = l.Key(b)
		args = append(args, b.Limit, b.Cost)
	}
	res, err := bucketScript.Run(ctx, l.rdb, keys, args...).Int64Slice()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 || res[0] < 0 || res[0] > int64(len(buckets)) {
		return nil, fmt.Errorf("redis: unexpected script reply %v", res)
	}
	return res, nil
}

// Key 是桶在 Redis 里的 key：<prefix><layer>:<id>:<metric>，例如 qf:rl:key:42:rpm。
// metric 是定长的后缀，layer 是固定的几个前缀，所以上游名里有冒号也不会和别的桶撞上。
func (l *Limiter) Key(b Bucket) string {
	return l.prefix + b.Layer + ":" + b.ID + ":" + b.Metric
}
