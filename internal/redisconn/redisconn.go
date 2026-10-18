// Package redisconn 是 QF 两个 Redis 客户端（限流、嵌入缓存）共同的连接设置：每次操作的上限只由调用方的 context 定，
// 不重试，拨号也跟着这个上限走。两个用途各自决定上限多长、键怎么起名。
package redisconn

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// go-redis 自己的日志丢掉：影响请求的错误都作为返回值回到调用方，记在那次调用的日志里；
// 它自己的日志格式不归 QF 管，不能保证不带连接信息，而且默认直接写 stderr，不是 QF 的 JSON 日志。
func init() { redis.SetLogger(discard{}) }

type discard struct{}

func (discard) Printf(context.Context, string, ...any) {}

// New 把共同的设置加在 opt 上，再建连接池。dialTimeout 是调用方每次操作的上限：
//   - ContextTimeoutEnabled：Redis 接受连接却不回答时，按 context 的期限放弃，而不是等 go-redis 自己几秒的读超时；
//   - 不重试（MaxRetries = -1）：限流的准入不是幂等的，读超时之后重试可能扣两次；缓存慢了不如直接回源。
//     go-redis 把拨号的错误也当成可以重试的命令错误，不关掉的话一次操作会拨好几次；
//   - 拨号最多 dialTimeout、只试一次：go-redis 在后台拨号，拨号的 ctx 不随请求取消，默认每次 5 秒、最多 5 次，
//     拨号活得比请求久。Redis 停着时名字解析要好几秒才失败（Docker Desktop 上约 4 秒），同一个名字的并发查询又会合并，
//     Redis 回来之后的请求会等进这次过时的查询，头几秒仍然失败。
func New(opt *redis.Options, dialTimeout time.Duration) *redis.Client {
	opt.ContextTimeoutEnabled = true
	opt.MaxRetries = -1
	opt.DialTimeout = dialTimeout
	opt.DialerRetries = 1
	return redis.NewClient(opt)
}
