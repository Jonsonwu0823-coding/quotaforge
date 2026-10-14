package ratelimit_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

// stuckDialer 是一个一直挂着、直到自己的 ctx 结束才返回的拨号器，像 Redis 停着时一直等不到 DNS 应答。
type stuckDialer struct {
	ended chan time.Duration // 每次拨号从开始到 ctx 结束用了多久
}

func (d *stuckDialer) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	start := time.Now()
	<-ctx.Done()
	d.ended <- time.Since(start)
	return nil, ctx.Err()
}

// openWith 用拨号器 dial 建限流器。地址不会被用到。
func openWith(t *testing.T, dial func(context.Context, string, string) (net.Conn, error)) *ratelimit.Limiter {
	t.Helper()
	opt, err := redis.ParseURL("redis://127.0.0.1:6379/0")
	if err != nil {
		t.Fatal(err)
	}
	opt.Dialer = dial
	l := ratelimit.OpenOptions(opt, "x:")
	t.Cleanup(func() { l.Close() })
	return l
}

// 请求的期限是 0.5 秒，拨号不能比它活得久：go-redis 在后台拨号，拨号的 ctx 不随请求取消；默认每次 5 秒，
// Redis 回来之后的请求会等一次过时的 DNS 查询。拨号的 ctx 必须自己在 config.RateLimitTimeout 左右结束。
func TestDialEndsWithTheLimiterDeadline(t *testing.T) {
	d := &stuckDialer{ended: make(chan time.Duration, 8)}
	l := openWith(t, d.dial)
	c, cancel := context.WithTimeout(ctx, config.RateLimitTimeout)
	defer cancel()
	start := time.Now()
	if _, err := l.Admit(c, []ratelimit.Bucket{bucket("1", 1, 1)}); err == nil {
		t.Fatal("Admit succeeded without a connection")
	}
	if took := time.Since(start); took > config.RateLimitTimeout+500*time.Millisecond {
		t.Fatalf("Admit took %v, want about %v", took, config.RateLimitTimeout)
	}
	select {
	case took := <-d.ended: // 截止时间只用来判失败
		if took > config.RateLimitTimeout+500*time.Millisecond {
			t.Fatalf("dial ran %v, want about %v", took, config.RateLimitTimeout)
		}
	case <-time.After(config.RateLimitTimeout + 2*time.Second):
		t.Fatal("the dial is still running long after the Admit deadline")
	}
}

// 拨号只试一次：失败了不再重试。有重试的话，每次重试都是一次新的 DNS 查询，后台的拨号会一直占着。
// 这个测试同时守着两项设置：DialerRetries = 1（拨号层不重试），MaxRetries = -1（命令层不重试）。
// 后者要求拨号器返回真实拨号失败的错误类型 *net.OpError{Op: "dial"}：go-redis 的 shouldRetry 把它当作
// 可以重试的错误，没有 MaxRetries = -1 时，一次准入会再拨 3 次；换成普通的 errors.New 它就不重试，看不出来。
func TestDialTriesOnce(t *testing.T) {
	var dials atomic.Int32
	l := openWith(t, func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	})
	c, cancel := context.WithTimeout(ctx, config.RateLimitTimeout)
	defer cancel()
	// 只试一次时拨号的错误马上回来；有重试时，Admit 要等重试的间隔，回来时拨号已经不止一次。
	if _, err := l.Admit(c, []ratelimit.Bucket{bucket("1", 1, 1)}); err == nil {
		t.Fatal("Admit succeeded without a connection")
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("%d dials for one Admit, want 1", n)
	}
}
