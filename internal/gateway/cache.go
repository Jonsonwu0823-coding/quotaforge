package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
)

// 嵌入缓存：按账户隔离的精确缓存。键是账户加“上游名和规范化之后发给它的请求体”的哈希；值是原来那次结算过的响应。
// 命中时不预留、不调上游、不动额度，账本里记一条 CACHED。同一个实例里同时到的相同请求只回源一次（flights）。
// 缓存只省钱：读不到、出错都当作没命中，照常回源；额度和账本的正确性只靠 MySQL。

// Cache 是网关用到的嵌入缓存：生产里是 *embcache.Client。Get 没有这个键时返回 nil, nil；出错就当作没命中。
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// 一次查缓存的结果，就是日志的 cache 字段和 qf_embedding_cache_lookups_total 的 result。
const (
	cacheHit       = "hit"       // 读缓存命中
	cacheCoalesced = "coalesced" // 拿到了同时在进行的相同请求（领头的）的结果
	cacheMiss      = "miss"      // 自己回源：没命中、领头的没给出结果、等的时候客户端断开
	cacheBypass    = "bypass"    // 不走缓存：带幂等键、请求体规范化不了
	cacheError     = "error"     // 读缓存出错或者条目坏了，照常回源
)

// cacheStatus 是 RFC 9211 的 Cache-Status 响应头，按查缓存的结果给；不走缓存的不加。
var cacheStatus = map[string]string{
	cacheHit:       "quotaforge; hit",
	cacheCoalesced: "quotaforge; fwd=miss; collapsed",
	cacheMiss:      "quotaforge; fwd=miss",
	cacheError:     "quotaforge; fwd=miss",
}

// maxCachedBody 是缓存一条响应正文的上限。QS 一批 8 段、每段 1024 维约 160 KB；更大的照常转发，只是不缓存。
const maxCachedBody = 1 << 20

// cacheEntry 是缓存里的一条：还原响应要的正文和 Content-Type，写 CACHED 那一行要的原来那次的调用号、用量、
// 响应里的 id 和模型名。存成“一行 JSON + 换行 + 原样的正文”，正文不转码。
type cacheEntry struct {
	CallID        string        `json:"call_id"`
	ContentType   []string      `json:"content_type"`
	Usage         *ledger.Usage `json:"usage"`
	ResponseID    string        `json:"response_id"`
	ResponseModel string        `json:"response_model"`
	Body          []byte        `json:"-"`
}

func (e *cacheEntry) encode() []byte {
	meta, _ := json.Marshal(e) // 字段都是能编码的类型；JSON 里的换行都被转义，第一个换行就是分隔
	return append(append(meta, '\n'), e.Body...)
}

var errBadEntry = errors.New("embedding cache: malformed entry")

// decodeEntry 是从缓存读回的条目，在这里校验一次（Redis 是信任边界）：格式、调用号、用量合法。
func decodeEntry(b []byte, p config.Purpose) (*cacheEntry, error) {
	meta, body, ok := bytes.Cut(b, []byte{'\n'})
	var e cacheEntry
	if !ok || json.Unmarshal(meta, &e) != nil || !ledger.ValidCallID(e.CallID) || e.Usage == nil || !e.Usage.Valid(string(p)) {
		return nil, errBadEntry
	}
	e.Body = body
	return &e, nil
}

// cacheLookup 是一次查过缓存的请求要带到最后的东西：查的键、领头的 flight（跟随的为 nil）、读缓存有没有出错，
// 以及结算之后要写进缓存的条目和它的键（真正回答的目标算出来的）。
type cacheLookup struct {
	key       string
	fl        *flight
	getFailed bool
	entry     *cacheEntry
	storeKey  string
}

// cacheKey 是缓存的键：<账户 id>:<SHA-256(len(上游名) ":" 上游名 ‖ 规范化之后发给目标 t 的请求体)>。
// 请求体里已经是 t 的上游模型名；规范化和幂等指纹是同一条规则（canonicalJSON），键的顺序、空白不影响命中。
// 带上上游名：同一个请求体发给同一个上游才算同一份，不假定两个上游的同名模型算出的向量相同。键里没有明文。
// 请求体规范化不了时返回 false，不走缓存。
func cacheKey(accountID int64, t *config.Target, rq request) (string, bool) {
	canonical, err := canonicalJSON(rq.bodyFor(t))
	if err != nil {
		return "", false
	}
	name := t.Upstream.Name
	sum := sha256.Sum256(slices.Concat(fmt.Appendf(nil, "%d:%s", len(name), name), canonical))
	return strconv.FormatInt(accountID, 10) + ":" + hex.EncodeToString(sum[:]), true
}

// lookupCache 在准入之后、预留之前查缓存，返回命中的条目（领头的读到的，或者共享到的领头的结果）。
// 没开缓存、不是嵌入的什么都不做。带幂等键的不走缓存：重试拿到同一个结果由幂等管。其余的先加入合并的表：
// 领头的读缓存，命中就立即交给跟随的、离开合并的表；跟随的等领头的，等的时候客户端断开就返回 errClientCanceled
// （还没预留，不留记录）。
func (g *gateway) lookupCache(ctx context.Context, c *call, rq request) (*cacheEntry, *apiError) {
	if g.cache == nil || c.purpose != config.Embeddings {
		return nil, nil
	}
	key, ok := cacheKey(c.key.AccountID, c.target, rq)
	if c.fingerprint != nil || !ok {
		c.cache = cacheBypass
		return nil, nil
	}
	c.lookup = &cacheLookup{key: key}
	span := g.step(ctx, "cache_lookup")
	var spanErr *apiError
	defer func() { endStep(span, spanErr) }()
	fl, leader := g.flights.join(key)
	if !leader {
		return g.follow(ctx, c, fl)
	}
	c.lookup.fl = fl
	entry, err := g.readCache(ctx, c, key)
	switch {
	case err != nil:
		// 只有 Redis 本身出错才不写：条目坏了时 Redis 是好的，结算之后用新的条目覆盖它
		c.cache, c.lookup.getFailed, c.cacheError = cacheError, !errors.Is(err, errBadEntry), err.Error()
		spanErr = errCacheUnavailable
	case entry == nil:
		c.cache = cacheMiss
	default:
		c.cache = cacheHit
		fl.share(entry)
		g.leaveFlight(c) // 交给跟随的之后就离开，不等写 CACHED 和写回
	}
	return entry, nil
}

// follow 是跟随的：等领头的结果。领头的没给出结果就自己照常走一遍（不再读缓存：领头的刚读过）。
func (g *gateway) follow(ctx context.Context, c *call, fl *flight) (*cacheEntry, *apiError) {
	entry, err := g.flights.wait(ctx, fl)
	switch {
	case err != nil:
		c.cache = cacheMiss
		return nil, errClientCanceled
	case entry == nil:
		c.cache = cacheMiss
		return nil, nil
	}
	c.cache = cacheCoalesced
	return entry, nil
}

// readCache 读一条缓存，最多 config.CacheTimeout，不随客户端断开而取消。
func (g *gateway) readCache(ctx context.Context, c *call, key string) (*cacheEntry, error) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.CacheTimeout)
	defer cancel()
	b, err := g.cache.Get(cctx, key)
	if err != nil || b == nil {
		return nil, err
	}
	return decodeEntry(b, c.purpose)
}

// leaveFlight 是领头的离开合并的表：放开跟随的、把键删掉，只离开一次。每条路径都在写回之前调（命中时在交出条目之后，
// 没命中时在写缓存之后，查完缓存之后出错时在写错误之前）；endpoint 的 defer 里再调一次，兜住 panic 的出口。
func (g *gateway) leaveFlight(c *call) {
	if c.lookup != nil && c.lookup.fl != nil {
		g.flights.leave(c.lookup.key, c.lookup.fl)
		c.lookup.fl = nil
	}
}

// serveCached 给命中的请求写一条 CACHED（预留量 0、不动额度），再写回缓存里的正文，逐字节照原来那次。
// x-request-id 是这次 CACHED 调用自己的号。CACHED 写不进去就 503 ledger_unavailable、不给正文：QF 返回的调用号都要查得到。
func (g *gateway) serveCached(ctx context.Context, w http.ResponseWriter, c *call, e *cacheEntry) {
	c.cachedFrom = e.CallID
	span := g.step(ctx, "record_cached")
	lctx, cancel := ledgerContext(ctx)
	defer cancel()
	err := g.ledger.RecordCached(lctx, ledger.CachedCall{
		CallID: c.id, AccountID: c.key.AccountID, KeyID: c.key.ID, Purpose: string(c.purpose), Alias: c.alias,
		Upstream: c.target.Upstream.Name, UpstreamModel: c.target.UpstreamModel,
		ClientRequestID: checked(c.clientRequestID), RunID: checked(c.runID), CachedFrom: e.CallID, Usage: *e.Usage,
		ResponseID: checked(e.ResponseID), ResponseModel: checked(e.ResponseModel),
	})
	if err != nil {
		g.ledgerFailed(c, "record_cached", err)
		endStep(span, errLedgerUnavailable)
		w.Header().Del(headerCacheStatus) // 这个 503 不是来自缓存的响应
		writeError(w, c, errLedgerUnavailable)
		return
	}
	endStep(span, nil)
	c.callStatus, c.outcome = ledger.Cached, ledger.Cached
	c.upstreamResponseID, c.upstreamModel = e.ResponseID, e.ResponseModel
	writeResponse(w, c, &ledger.Response{Status: http.StatusOK, ContentType: e.ContentType, Body: e.Body})
}

// shareCache 在收尾之后、写缓存之前：查过缓存的请求结算成了一个可以缓存的响应（账本里是 SETTLED、200、正文不超过上限），
// 就记下要写进缓存的条目，键按真正回答的目标算（换过上游时是最后那个）。领头的再把它交给跟随的，前提是这个键就是大家查的
// 那个：换上游之后回答的是另一个目标，就不共享。收尾失败的（账本里不是 SETTLED）不缓存。
func (g *gateway) shareCache(c *call, rq request, res *ledger.Response) {
	l := c.lookup
	if l == nil || c.callStatus != ledger.Settled || res.Status != http.StatusOK || len(res.Body) > maxCachedBody {
		return
	}
	key, ok := cacheKey(c.key.AccountID, c.target, rq)
	if !ok {
		return
	}
	l.entry = &cacheEntry{CallID: c.id, ContentType: res.ContentType, Usage: c.usage,
		ResponseID: c.upstreamResponseID, ResponseModel: c.upstreamModel, Body: res.Body}
	l.storeKey = key
	if l.fl != nil && key == l.key {
		l.fl.share(l.entry)
	}
}

// storeCache 在收尾之后、写回之前把 shareCache 记下的条目写进缓存，最多 config.CacheTimeout，不随客户端断开而取消。
// 客户端在响应开头多等这一次：通常约 1 毫秒，最多 CacheTimeout；总时长不变。写完之后领头的才离开合并的表，
// 所以紧接着的相同请求一定命中。这次读缓存时 Redis 出错的不写：缓存坏着时每个请求只多等一次读。写失败只记日志和指标。
// 过期时长是配置的时长乘以 0.9 到 1.1 之间的随机数：同一批写进去的条目不会同时过期（防雪崩）。
func (g *gateway) storeCache(ctx context.Context, c *call) {
	l := c.lookup
	if l == nil || l.entry == nil || l.getFailed {
		return
	}
	span := g.step(ctx, "cache_store")
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.CacheTimeout)
	defer cancel()
	ttl := time.Duration(float64(g.cacheTTL) * (0.9 + 0.2*g.jitter()))
	if err := g.cache.Set(cctx, l.storeKey, l.entry.encode(), ttl); err != nil {
		c.cacheError = err.Error()
		g.tel.Metrics.CacheWrites.WithLabelValues("error").Inc()
		endStep(span, errCacheUnavailable)
		return
	}
	g.tel.Metrics.CacheWrites.WithLabelValues("ok").Inc()
	endStep(span, nil)
}
