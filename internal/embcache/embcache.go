// Package embcache 是嵌入缓存在 Redis 里的读写：按键取值、带过期时间写值。键怎么算、值里放什么、什么时候读写，
// 都由网关决定；这里只管连接和超时。缓存只省钱，不影响正确性：出错由调用方当作没命中。
package embcache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// go-redis 自己的日志丢掉：错误都作为返回值回到调用方，记在那次调用的日志里（同 ratelimit 包）。
func init() { redis.SetLogger(discard{}) }

type discard struct{}

func (discard) Printf(context.Context, string, ...any) {}

// Client 用一个 Redis 连接池读写缓存。可以并发使用。
type Client struct {
	rdb    *redis.Client
	prefix string
}

// Open 按 redis://[:密码@]主机:端口/库号 连接缓存用的 Redis（不立即拨号）。prefix 加在每个键前面。
// URL 里可能有密码：解析出错时只说格式不对，不带原文。
func Open(url, prefix string) (*Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, errors.New("embedding cache: invalid redis URL")
	}
	return newClient(opt, prefix), nil
}

// newClient 把缓存要求的设置加在 opt 上，再建连接池。每次读写的上限是调用方给的 context（config.CacheTimeout）：
//   - ContextTimeoutEnabled：Redis 接受连接却不回答时，按 context 的期限放弃，而不是等 go-redis 自己几秒的读超时；
//   - 不重试（MaxRetries = -1）：缓存慢了不如直接回源，重试只会把这次请求拖得更久；
//   - 拨号最多 CacheTimeout、只试一次：go-redis 在后台拨号，拨号不随请求取消。缓存的 Redis 停着时，名字解析要好几秒
//     才失败（Docker Desktop 上约 4 秒），不限住的话拨号活得比请求久，恢复之后的请求还要等这次过时的查询。
func newClient(opt *redis.Options, prefix string) *Client {
	opt.ContextTimeoutEnabled = true
	opt.MaxRetries = -1
	opt.DialTimeout = config.CacheTimeout
	opt.DialerRetries = 1
	return &Client{rdb: redis.NewClient(opt), prefix: prefix}
}

// Close 关闭连接池。
func (c *Client) Close() error { return c.rdb.Close() }

// Get 取 key 的值；没有这个键时返回 nil, nil。
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := c.rdb.Get(ctx, c.prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return v, err
}

// Set 写 key 的值，ttl 之后过期。
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.rdb.Set(ctx, c.prefix+key, value, ttl).Err()
}
