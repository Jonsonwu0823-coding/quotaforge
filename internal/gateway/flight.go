package gateway

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// flights 合并同一个实例里同时到的相同嵌入请求（防缓存击穿）：同一个缓存键同时只有一个请求（领头的）去读缓存、
// 回源，其余的（跟随的）等它的结果。只在进程内：两个实例各自合并，跨实例最多各回源一次。
type flights struct {
	mu      sync.Mutex
	m       map[string]*flight
	waiting prometheus.Gauge // 正在等领头的请求数
}

// flight 是一个缓存键上正在进行的一次读缓存、回源。done 关闭之后 entry 才能读：领头的拿到了可以共享的结果时是它，
// 没拿到（没命中又没结算成、换上游后回答的是另一个键、panic）时是 nil。
type flight struct {
	once  sync.Once
	done  chan struct{}
	entry *cacheEntry
}

func newFlights(waiting prometheus.Gauge) *flights {
	return &flights{m: map[string]*flight{}, waiting: waiting}
}

// join 加入 key 上正在进行的 flight；没有就新建一个，调用方是领头的（leader 为 true），必须在结束时 leave。
func (f *flights) join(key string) (fl *flight, leader bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if fl := f.m[key]; fl != nil {
		return fl, false
	}
	fl = &flight{done: make(chan struct{})}
	f.m[key] = fl
	return fl, true
}

// wait 等领头的结果，或者等到自己的客户端断开（返回 ctx 的错误）。不另设上限：跟随的只等领头的读缓存、回源、收尾、
// 写缓存这几步（每一步都有上限），不等领头的写回自己的客户端。
func (f *flights) wait(ctx context.Context, fl *flight) (*cacheEntry, error) {
	f.waiting.Inc()
	defer f.waiting.Dec()
	select {
	case <-fl.done:
		return fl.entry, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// share 交出领头的结果，放开所有跟随的；只有第一次算数。
func (fl *flight) share(e *cacheEntry) {
	fl.once.Do(func() {
		fl.entry = e
		close(fl.done)
	})
}

// leave 是领头的离开：还没交出结果的按“没有结果”放开跟随的，再把自己的 flight 从表里删掉（只删自己的，
// 不会删掉之后新来的领头）。领头的在写完缓存之后、写回自己的客户端之前 leave：写缓存时来的相同请求加入的是已经完成的
// flight，立即拿到结果；删掉之后再来的，缓存里已经有了；跟随的不等领头的写回。
func (f *flights) leave(key string, fl *flight) {
	fl.share(nil)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.m[key] == fl {
		delete(f.m, key)
	}
}
