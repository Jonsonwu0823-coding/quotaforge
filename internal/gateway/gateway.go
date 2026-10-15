// Package gateway 实现 OpenAI 兼容的 chat 和 embeddings 转发。
package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/telemetry"
)

// 请求体上限。QS 最大的请求（约 11,500 字的系统消息、上下文和 6 个工具）远小于它。
const maxRequestBody = 2 << 20

type requestIDKey struct{}

// WithRequestID 包在最外层：每个请求生成一个调用号，先设好 x-request-id 响应头再交给路由，
// 这样 404、405 和错误响应也都带它。各接口从 context 里取同一个调用号写日志。
func WithRequestID(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "qf-" + rand.Text()
		w.Header().Set("x-request-id", id)
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

// Ledger 是网关用到的账本操作：生产里是 *ledger.Store。不存在或已吊销的 Key，KeyByHash 返回 nil, nil；
// 额度不够时 Reserve 返回 ledger.ErrQuotaExhausted，带幂等键时还可能返回要重放的快照或幂等的几种拒绝。
// MarkSent 落发送意图；Reroute 是换上游时的改道（调用不再是 IN_FLIGHT 时返回 ledger.ErrNotInFlight）；
// Call 给调用状态查询用，不存在或不属于这个账户时返回 nil, nil。RecordCached 记一次嵌入缓存命中。
// 其它错误都当成账本不可用。
type Ledger interface {
	KeyByHash(ctx context.Context, hash string) (*ledger.Key, error)
	Reserve(ctx context.Context, r ledger.Reservation) (*ledger.Replay, error)
	MarkSent(ctx context.Context, r ledger.Reservation) error
	Reroute(ctx context.Context, r ledger.Reservation, upstream, upstreamModel string) error
	Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error
	Call(ctx context.Context, accountID int64, id string) (*ledger.CallInfo, error)
	RecordCached(ctx context.Context, c ledger.CachedCall) error
}

type gateway struct {
	cfg            *config.Config
	ledger         Ledger
	limiter        Limiter
	client         *http.Client
	router         *router
	log            *slog.Logger
	tel            telemetry.Telemetry
	quotaExhausted *apiError // 带上按配置算好的 Retry-After

	cache    Cache          // 嵌入缓存；没开时为 nil
	cacheTTL time.Duration  // 缓存条目的过期时长（写入时再乘以抖动）
	jitter   func() float64 // 返回 [0, 1) 里的一个数；测试换成固定的
	flights  *flights
}

// New 返回两个转发接口和调用状态查询的路由。外面要套 WithRequestID。cache 为 nil 时不查缓存（配置里没开）。
func New(cfg *config.Config, l Ledger, lim Limiter, cache Cache, log *slog.Logger, tel telemetry.Telemetry) http.Handler {
	g := newGateway(cfg, l, lim, log, tel)
	g.cache = cache
	return g.routes()
}

// newGateway 建好网关，不带缓存；测试在 routes 之前换掉熔断的时钟、选目标的随机源、上游的 Transport，或者装上缓存。
func newGateway(cfg *config.Config, l Ledger, lim Limiter, log *slog.Logger, tel telemetry.Telemetry) *gateway {
	g := &gateway{cfg: cfg, ledger: l, limiter: lim, client: newUpstreamClient(), router: newRouter(cfg, tel.Metrics),
		log: log, tel: tel, quotaExhausted: errQuotaExhausted.withRetryAfter(retryAfterSeconds(cfg)),
		jitter: mathrand.Float64, flights: newFlights(tel.Metrics.CacheWaiting)}
	if ec := cfg.EmbeddingCache(); ec != nil {
		g.cacheTTL = ec.TTL
	}
	return g
}

func (g *gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST "+chatRoute, g.endpoint(config.Chat, chatRoute))
	mux.Handle("POST "+embeddingsRoute, g.endpoint(config.Embeddings, embeddingsRoute))
	mux.HandleFunc("GET "+lookupRoute, g.lookup)
	return mux
}

// 路由：注册路由和 span 的名字、http.route 属性用同一份。
const (
	chatRoute       = "/v1/chat/completions"
	embeddingsRoute = "/v1/embeddings"
	lookupRoute     = "/v1/calls/{id}"
)

// endpoint 是一次调用的全部步骤：鉴权 → 解析、检查模型、校验幂等键、选目标、估算（prepare）→ 准入（限流）→ 查嵌入缓存
// → 预留 → 落发送意图 → 转发（拨号失败时换上游）→ 收尾 → 写嵌入缓存 → 写回（deliver）→ 修正限流的 TPM 桶
// → 记录（日志、指标、根 span）。鉴权之后才建根 span（见 startTrace），之后的每一步都挂在它下面。
// 收尾在写回之前：客户端拿到响应时，账本已经记好了。流式做不到（事件边读边写），改成流结束之后才收尾：
// 客户端收到 [DONE] 时收尾还没做，读完上游的流才做，从不在流结束之前做。带幂等键、可以重放时，预留这一步直接给出原来的响应；
// 嵌入缓存命中时不预留，记一条 CACHED、给出缓存里的响应。
// 请求发出之后不再看客户端：断开了也等上游回答或超时（流式是读完上游的流），照常收尾（有界结算），只是不写回。
// 写缓存在写回之前（客户端在响应开头多等这一次，见 storeCache）；修正在写回之后、handler 返回之前，所以响应要等它做完
// 才发完（通常约 1 毫秒，最多 config.RateLimitTimeout）。修正在 defer 里，覆盖准入之后的每个出口。
func (g *gateway) endpoint(p config.Purpose, route string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := &call{
			id:              r.Context().Value(requestIDKey{}).(string),
			start:           time.Now(),
			purpose:         p,
			clientRequestID: r.Header.Get("X-Client-Request-Id"),
			runID:           r.Header.Get("X-Run-Id"),
			idempotencyKey:  r.Header.Get("Idempotency-Key"),
		}
		defer g.record(c, string(p), g.logCall)
		e := g.authenticate(r, c)
		ctx := g.startTrace(r, c, route, e)
		defer g.adjust(ctx, c) // 先于 record 执行：日志里要有修正的错误，根 span 要包住修正
		defer g.router.release(c)
		defer g.leaveFlight(c) // 只兜住出错和 panic 的出口：正常的路径在写回之前就已经离开了合并的表
		if e != nil {
			writeError(w, c, e)
			return
		}
		g.serve(ctx, w, r, c)
	}
}

// serve 是鉴权之后的步骤：哪一步失败就写回它的错误；嵌入缓存命中、幂等重放、正常调用各自写回。
func (g *gateway) serve(ctx context.Context, w http.ResponseWriter, r *http.Request, c *call) {
	rq, e := g.prepare(w, r, c)
	if e == nil {
		e = g.admit(ctx, c)
	}
	var hit *cacheEntry
	if e == nil {
		hit, e = g.lookupCache(ctx, c, rq)
	}
	if s := cacheStatus[c.cache]; s != "" {
		w.Header().Set("Cache-Status", s)
	}
	var replay *ledger.Replay
	if e == nil && hit == nil {
		replay, e = g.reserve(ctx, c)
	}
	switch {
	case e != nil:
		writeError(w, c, e)
	case hit != nil:
		g.serveCached(ctx, w, c, hit)
	case replay != nil:
		c.replayOf = replay.CallID
		w.Header().Set("x-request-id", replay.CallID)
		w.Header().Set("Idempotent-Replayed", "true")
		writeResponse(w, c, &replay.Response)
	default:
		g.deliver(ctx, w, c, rq)
	}
}

// deliver 是预留成功之后的部分：发给上游、判定结局、收尾、写嵌入缓存、写回。客户端已经断开就不写回，状态码留 0。
// 流式已经写了响应头的，在 send 里写完了，这里只记错误码。
// 合并只包住大家共享的几步（读缓存、回源、收尾、写缓存）：领头的在写回自己的客户端之前就离开合并的表，
// 它的客户端读得慢（服务器没有全局的写期限），也不会让跟随的一直等。
func (g *gateway) deliver(ctx context.Context, w http.ResponseWriter, c *call, rq request) {
	res, e, finish := g.send(ctx, w, c, rq)
	c.outcome, c.evidence = outcome(c, res, e)
	if finish {
		g.finish(ctx, c, res, e)
		g.shareCache(c, rq, res)
	}
	g.storeCache(ctx, c)
	g.leaveFlight(c)
	switch {
	case c.streamed:
		if e != nil {
			c.errorCode = e.code
		}
	case ctx.Err() != nil:
		c.clientDisconnected = true
		if e != nil {
			c.errorCode = e.code
		}
	case e != nil:
		writeError(w, c, e)
	default:
		writeResponse(w, c, res)
	}
}

// send 是预留之后、收尾之前的步骤，返回上游的结果，以及要不要收尾。客户端已经断开就不发；先建好上游请求，
// 再落发送意图，落成了才发。发送意图没落成的不收尾：调用要么还是 RESERVED，要么库里其实已经提交成了 IN_FLIGHT，都交给回收。
// 发出之后，只有确定没发出（notSent）才换上游（failover）；所有尝试共用一个期限，从第一次尝试开始算，
// 不跟着客户端取消（有界结算）。发送意图只落一次。
func (g *gateway) send(ctx context.Context, w http.ResponseWriter, c *call, rq request) (*ledger.Response, *apiError, bool) {
	if ctx.Err() != nil {
		return nil, errClientCanceled, true
	}
	req, e := g.upstreamRequest(c, c.target, rq)
	if e != nil {
		return nil, e, true
	}
	if e := g.markSent(ctx, c); e != nil {
		return nil, e, false
	}
	upCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.Timeout(c.purpose))
	defer cancel()
	for {
		res, e := g.forward(ctx, upCtx, w, c, req)
		g.router.report(c, verdictOf(e))
		if !notSent(e) {
			return res, e, true
		}
		if req = g.failover(ctx, upCtx, c, rq); req == nil {
			return res, e, true
		}
	}
}

// writeResponse 写回上游的 2xx 或它的快照：状态码、Content-Type 和正文，不转发上游的其它响应头。
// 上游没给 Content-Type 时，这里放一个空值，防止 net/http 自己嗅探补上。
func writeResponse(w http.ResponseWriter, c *call, resp *ledger.Response) {
	w.Header()["Content-Type"] = resp.ContentType
	c.status = resp.Status
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// request 是客户端的原始请求体和它的解析结果。发给哪个目标，就按那个目标的上游模型名从原始请求体改写一次（upstreamBody）：
// 换上游时也一样，不在改写过的请求体上再改。
type request struct {
	parsed parsedRequest
	body   []byte
}

func (rq request) bodyFor(t *config.Target) []byte {
	return rq.parsed.upstreamBody(rq.body, t.UpstreamModel)
}

// prepare 读请求体、解析、检查模型，带幂等键时校验键、算指纹，选目标、估算。所有目标的上游都熔断着时快速失败
// （502 upstream_unavailable），在准入和预留之前：不进账本、不调上游。
func (g *gateway) prepare(w http.ResponseWriter, r *http.Request, c *call) (request, *apiError) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			return request{}, errRequestTooLarge
		case r.Context().Err() != nil:
			// 请求体没发完客户端就断开了：连接读出错时 net/http 会取消请求的 context。和写回之前断开一样，不写回。
			// 连接还在、只是分块编码坏了的，context 没有取消，仍是 400。
			return request{}, errClientCanceled
		}
		return request{}, c.invalid(reasonUnreadableBody)
	}
	// 服务器的 ReadTimeout 只管读请求：请求体读完、net/http 开始后台读（用来发现客户端断开）时，它自己清掉读期限，
	// 等上游久的调用和长的流不受它限制（TestServerReadTimeoutSparesLongCalls 守着；以后升级 Go，这个测试会抓住变化）。
	req, reason := parseRequest(body)
	if reason != "" {
		return request{}, c.invalid(reason)
	}
	c.alias = req.model
	if c.purpose == config.Chat && req.stream {
		if reason := req.checkStreamOptions(); reason != "" {
			return request{}, c.invalid(reason)
		}
		c.stream, c.wantsUsage = true, req.wantsUsage
	}
	if c.model = g.cfg.Model(c.purpose, req.model); c.model == nil {
		return request{}, errModelNotFound
	}
	if !c.key.Allows(req.model) {
		return request{}, errModelNotAllowed
	}
	key, e := idempotencyKey(r.Header)
	if e != nil {
		return request{}, e
	}
	if key != "" {
		// 指纹用客户端发来的原始请求体（改写 model 之前的）。
		if c.fingerprint, err = fingerprint(c.purpose, body); err != nil {
			return request{}, c.invalid(reasonFingerprintFailed)
		}
	}
	if c.target, c.permit = g.router.pick(c.model.Targets, nil); c.target == nil {
		c.circuitOpen = true
		return request{}, errUpstreamUnavailable
	}
	rq := request{parsed: req, body: body}
	// 估算按第一次选中的目标改写后的请求体算；换上游时预留量不变（差的只是上游模型名的几个字节）。
	c.planned = plan(c, rq.bodyFor(c.target), req.maxTokens)
	return rq, nil
}

// authenticate 按 Bearer 后面的 Key 的哈希查库。每个请求都查，吊销立即生效。
func (g *gateway) authenticate(r *http.Request, c *call) *apiError {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return errInvalidAPIKey
	}
	ctx, cancel := ledgerContext(r.Context())
	defer cancel()
	k, err := g.ledger.KeyByHash(ctx, apikey.Hash(token))
	switch {
	case err != nil:
		g.ledgerFailed(c, "authenticate", err)
		return errLedgerUnavailable
	case k == nil:
		return errInvalidAPIKey
	}
	c.key = k
	return nil
}
