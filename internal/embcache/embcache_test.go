package embcache_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/dbtest"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/embcache"
)

func TestMain(m *testing.M) { dbtest.Main(m) }

var ctx = context.Background()

// slack 是判失败用的余量：读写的上限是 config.CacheTimeout，加上调度的抖动。
const slack = 500 * time.Millisecond

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, config.CacheTimeout)
}

// 真 Redis：没有的键是 nil, nil；写进去的值原样读回（任意字节，包括换行和 0 字节）；键带前缀，过期时间设上了。
func TestGetSet(t *testing.T) {
	url, prefix := dbtest.RedisURL(t), dbtest.RedisPrefix()
	c, err := embcache.Open(url, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	raw := redis.NewClient(mustParse(t, url))
	t.Cleanup(func() { raw.Close() })

	octx, cancel := opCtx()
	defer cancel()
	if v, err := c.Get(octx, "k"); v != nil || err != nil {
		t.Fatalf("Get of a missing key = %q, %v; want nil, nil", v, err)
	}
	value := []byte("{\"a\":1}\n\x00body")
	if err := c.Set(octx, "k", value, 90*time.Second); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(octx, "k"); !bytes.Equal(v, value) || err != nil {
		t.Fatalf("Get = %q, %v; want %q", v, err, value)
	}
	ttl, err := raw.PTTL(ctx, prefix+"k").Result()
	if err != nil || ttl <= 85*time.Second || ttl > 90*time.Second {
		t.Fatalf("PTTL of %sk = %v, %v; want about 90s", prefix, ttl, err)
	}
}

func mustParse(t *testing.T, url string) *redis.Options {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	return opt
}

// 地址格式不对：报错不带原文（URL 里可能有密码）。
func TestOpenHidesURL(t *testing.T) {
	_, err := embcache.Open("redis://:password@[::1", "p:")
	if err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "::1") {
		t.Fatalf("Open = %v", err)
	}
}

// openWith 用拨号器 dial 建缓存的客户端。地址不会被用到。
func openWith(t *testing.T, dial func(context.Context, string, string) (net.Conn, error)) *embcache.Client {
	t.Helper()
	opt := mustParse(t, "redis://127.0.0.1:6379/0")
	opt.Dialer = dial
	c := embcache.OpenOptions(opt, "x:")
	t.Cleanup(func() { c.Close() })
	return c
}

// 拨号失败（连接被拒）：一次读只拨一次就把错误交回。go-redis 把拨号的错误当成可以重试的命令错误，
// 不关掉命令的重试（MaxRetries = -1）或者拨号的重试（DialerRetries = 1）都会拨不止一次。
func TestDialTriesOnce(t *testing.T) {
	var dials atomic.Int32
	c := openWith(t, func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	})
	octx, cancel := opCtx()
	defer cancel()
	if _, err := c.Get(octx, "k"); err == nil {
		t.Fatal("Get succeeded without a connection")
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("%d dials for one Get, want 1", n)
	}
}

// 缓存的 Redis 停着、名字解析一直没有应答：读在上限左右返回错误，后台的拨号也在上限左右自己结束，
// 不会活得比请求久（拨号不随请求取消）。
func TestDialEndsWithTheCacheDeadline(t *testing.T) {
	ended := make(chan time.Duration, 8)
	c := openWith(t, func(dctx context.Context, _, _ string) (net.Conn, error) {
		start := time.Now()
		<-dctx.Done()
		ended <- time.Since(start)
		return nil, dctx.Err()
	})
	octx, cancel := opCtx()
	defer cancel()
	start := time.Now()
	if _, err := c.Get(octx, "k"); err == nil {
		t.Fatal("Get succeeded without a connection")
	}
	if took := time.Since(start); took > config.CacheTimeout+slack {
		t.Fatalf("Get took %v, want about %v", took, config.CacheTimeout)
	}
	select {
	case took := <-ended: // 截止时间只用来判失败
		if took > config.CacheTimeout+slack {
			t.Fatalf("dial ran %v, want about %v", took, config.CacheTimeout)
		}
	case <-time.After(config.CacheTimeout + 2*time.Second):
		t.Fatal("the dial is still running long after the Get deadline")
	}
}

// 连上了却不回答（Redis 卡住）：读、写都在上限左右返回错误，不等 go-redis 自己几秒的读超时。
func TestSilentRedisTimesOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 64) // 收下连接，什么都不回；结束时一起关掉
	t.Cleanup(func() {
		ln.Close()
		for len(accepted) > 0 {
			(<-accepted).Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- conn:
			default:
				conn.Close()
			}
		}
	}()
	c, err := embcache.Open("redis://"+ln.Addr().String()+"/0", "x:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	for _, op := range []func(context.Context) error{
		func(octx context.Context) error { _, err := c.Get(octx, "k"); return err },
		func(octx context.Context) error { return c.Set(octx, "k", []byte("v"), time.Minute) },
	} {
		octx, cancel := opCtx()
		start := time.Now()
		err := op(octx)
		cancel()
		if took := time.Since(start); err == nil || took > config.CacheTimeout+slack {
			t.Fatalf("operation on a silent Redis = %v after %v, want an error after about %v", err, took, config.CacheTimeout)
		}
	}
}
