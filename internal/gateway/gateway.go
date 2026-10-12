// Package gateway 实现 OpenAI 兼容的 chat 和 embeddings 转发。
package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/apikey"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
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
// MarkSent 落发送意图；Call 给调用状态查询用，不存在或不属于这个账户时返回 nil, nil。
// 其它错误都当成账本不可用。
type Ledger interface {
	KeyByHash(ctx context.Context, hash string) (*ledger.Key, error)
	Reserve(ctx context.Context, r ledger.Reservation) (*ledger.Replay, error)
	MarkSent(ctx context.Context, r ledger.Reservation) error
	Finish(ctx context.Context, r ledger.Reservation, res ledger.Result) error
	Call(ctx context.Context, accountID int64, id string) (*ledger.CallInfo, error)
}

type gateway struct {
	cfg            *config.Config
	ledger         Ledger
	limiter        Limiter
	client         *http.Client
	log            *slog.Logger
	quotaExhausted *apiError // 带上按配置算好的 Retry-After
}

// New 返回两个转发接口和调用状态查询的路由。外面要套 WithRequestID。
func New(cfg *config.Config, l Ledger, lim Limiter, log *slog.Logger) http.Handler {
	g := &gateway{cfg: cfg, ledger: l, limiter: lim, client: newUpstreamClient(), log: log,
		quotaExhausted: errQuotaExhausted.withRetryAfter(retryAfterSeconds(cfg))}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", g.endpoint(config.Chat))
	mux.Handle("POST /v1/embeddings", g.endpoint(config.Embeddings))
	mux.HandleFunc("GET /v1/calls/{id}", g.lookup)
	return mux
}

// endpoint 是一次调用的全部步骤：鉴权、解析、检查模型、估算、校验幂等键（prepare）→ 准入（限流）→ 预留 → 落发送意图 → 转发
// → 收尾 → 写回 → 修正限流的 TPM 桶 → 写日志。
// 收尾在写回之前：客户端拿到响应时，账本已经记好了。带幂等键、可以重放时，预留这一步直接给出原来的响应。
// 请求发出之后不再看客户端：断开了也等上游回答或超时，照常收尾（有界结算），只是不写回。
// 修正在 defer 里，覆盖准入之后的每个出口；它在 handler 返回之前执行，所以响应要等它做完才发完（通常约 1 毫秒，
// 最多 config.RateLimitTimeout）。
func (g *gateway) endpoint(p config.Purpose) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := &call{
			id:              r.Context().Value(requestIDKey{}).(string),
			start:           time.Now(),
			purpose:         p,
			clientRequestID: r.Header.Get("X-Client-Request-Id"),
			runID:           r.Header.Get("X-Run-Id"),
			idempotencyKey:  r.Header.Get("Idempotency-Key"),
		}
		defer g.logCall(c)
		defer g.adjust(r.Context(), c) // 先于 logCall 执行：日志里要有修正的错误

		body, e := g.prepare(w, r, c)
		if e == nil {
			e = g.admit(r.Context(), c)
		}
		var replay *ledger.Replay
		if e == nil {
			replay, e = g.reserve(r.Context(), c)
		}
		if e != nil {
			writeError(w, c, e)
			return
		}
		if replay != nil {
			c.replayOf = replay.CallID
			w.Header().Set("x-request-id", replay.CallID)
			w.Header().Set("Idempotent-Replayed", "true")
			writeResponse(w, c, &replay.Response)
			return
		}
		res, e, finish := g.send(r.Context(), c, body)
		c.outcome, c.evidence = outcome(c, res, e)
		if finish {
			g.finish(r.Context(), c, res, e)
		}
		switch {
		case r.Context().Err() != nil: // 客户端已经断开：不写回，状态码留 0
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
}

// send 是预留之后、收尾之前的步骤，返回上游的结果，以及要不要收尾。客户端已经断开就不发；先建好上游请求，
// 再落发送意图，落成了才发。发送意图没落成的不收尾：调用要么还是 RESERVED，要么库里其实已经提交成了 IN_FLIGHT，都交给回收。
func (g *gateway) send(ctx context.Context, c *call, body []byte) (*ledger.Response, *apiError, bool) {
	if ctx.Err() != nil {
		return nil, errClientCanceled, true
	}
	req, e := g.upstreamRequest(c, body)
	if e != nil {
		return nil, e, true
	}
	if e := g.markSent(ctx, c); e != nil {
		return nil, e, false
	}
	res, e := g.forward(ctx, c, req)
	return res, e, true
}

// writeResponse 写回上游的 2xx 或它的快照：状态码、Content-Type 和正文，不转发上游的其它响应头。
// 上游没给 Content-Type 时，这里放一个空值，防止 net/http 自己嗅探补上。
func writeResponse(w http.ResponseWriter, c *call, resp *ledger.Response) {
	w.Header()["Content-Type"] = resp.ContentType
	c.status = resp.Status
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// prepare 鉴权、读请求体、解析、检查模型、估算，带幂等键时校验键、算指纹，返回改写过 model 的请求体。
func (g *gateway) prepare(w http.ResponseWriter, r *http.Request, c *call) ([]byte, *apiError) {
	if e := g.authenticate(r, c); e != nil {
		return nil, e
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, errRequestTooLarge
		}
		return nil, errInvalidRequest
	}
	req, err := parseRequest(body)
	if err != nil {
		return nil, errInvalidRequest
	}
	c.alias = req.model
	if c.purpose == config.Chat && req.stream {
		return nil, errStreamNotSupported
	}
	if c.model = g.cfg.Model(c.purpose, req.model); c.model == nil {
		return nil, errModelNotFound
	}
	if !c.key.Allows(req.model) {
		return nil, errModelNotAllowed
	}
	key, e := idempotencyKey(r.Header)
	if e != nil {
		return nil, e
	}
	if key != "" {
		// 指纹用客户端发来的原始请求体（改写 model 之前的）。
		if c.fingerprint, err = fingerprint(c.purpose, body); err != nil {
			return nil, errInvalidRequest
		}
	}
	out := req.withModel(body, c.model.UpstreamModel)
	c.planned = plan(c, out, req.maxTokens)
	return out, nil
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
		c.ledgerError = err.Error()
		return errLedgerUnavailable
	case k == nil:
		return errInvalidAPIKey
	}
	c.key = k
	return nil
}
