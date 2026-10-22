package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ledger"
	"github.com/Jonsonwu0823-coding/quotaforge/internal/ratelimit"
)

// call 是一次调用的记录，处理过程中逐步填写，结束时写成一行日志。
type call struct {
	id      string
	start   time.Time
	purpose config.Purpose
	key     *ledger.Key
	alias   string
	model   *config.Model
	target  *config.Target // 这次发给的目标；换过上游时是最后那个
	permit  *permit        // 熔断放行了 target 的上游、结果还没交回时才有

	failoverFrom []string // 换上游之前试过、确定没收到请求（拨号失败）的上游，按顺序
	circuitOpen  bool     // 所有目标的上游都熔断着，快速失败

	planned     ledger.Reservation  // prepare 建好的预留（估算见 estimate）；准入的 TPM 花费和预留量都是它的 Amount
	reservation *ledger.Reservation // 预留成功后才有，指向 planned
	callStatus  ledger.Status       // 账本里记下的调用状态；收尾失败时还是收尾前的状态
	outcome     ledger.Status       // 网关自己判定的结局（见 outcome），和账本写没写成功无关；账本里有这次调用（预留成功、缓存命中）才有
	evidence    ledger.Evidence     // 结清时凭的证据
	ledgerError string              // 账本操作失败的原因（驱动的错误文字，不含连接串）

	admitted         []ratelimit.Bucket // 准入放行时扣过的桶；没有桶、被拒、出错时为空
	released         []ratelimit.Bucket // 换上游时换走的上游层的桶：那个上游确定没收到请求，收尾时 TPM 全部加回
	deniedBy         *ratelimit.Bucket  // 被限流时是哪个桶
	rateLimiterError string             // 准入或修正时 Redis 的错误文字（不含 URL）

	clientRequestID string
	runID           string
	idempotencyKey  string // 请求头的原值，只用来记日志；fingerprint 不为 nil 时它是校验过的键
	fingerprint     []byte // 请求带了合法的幂等键时才有
	replayOf        string // 重放时是原来的调用号
	lookupOf        string // 调用状态查询：被查的调用号，格式合格的才有

	cache      string       // 开了缓存的嵌入请求查缓存的结果（cacheHit 等）；没查时为空
	cachedFrom string       // 缓存命中时原来那次的调用号
	cacheError string       // 读、写缓存的错误文字（不含 URL）
	lookup     *cacheLookup // 查过缓存（没绕过）的请求才有

	status             int  // 写回客户端的 HTTP 状态；什么都没写回（客户端已断开）时是 0
	clientDisconnected bool // 写回之前或写回时（流式：流结束之前）客户端已经断开
	writeTimeout       bool // 客户端不读，写超过了期限（也算断开；见 writeTimeout）

	stream       bool      // chat 带 stream: true
	wantsUsage   bool      // 流式：客户端自己写了 include_usage: true，usage 那个事件照转发
	streamed     bool      // 流式：响应头已经写给客户端
	firstEventAt time.Time // 流式：收到上游第一个事件的时刻；零值是没收到（和 start 相减可能是 0，所以不记时长）

	upstreamCalled     bool
	upstreamDuration   time.Duration // 等上游的时间：发请求到读完正文（流式是整个流）
	upstreamStatus     int
	upstreamXRequestID string
	upstreamResponseID string
	upstreamModel      string
	usage              *ledger.Usage // nil 表示未知

	errorCode     string
	invalidReason invalidReason // 400 invalid_request 的原因

	span         trace.Span   // 根 span；没建时（没通过鉴权）是空实现
	parentSpanID trace.SpanID // 请求的 traceparent 里调用方的 span id；没带或不合法时为零值
}

// invalid 记下请求体不合格的原因，返回 400 invalid_request。
func (c *call) invalid(r invalidReason) *apiError {
	c.invalidReason = r
	return errInvalidRequest
}

// parseUsage 解析上游的 usage，三个数原样用上游的。缺字段、不符合 ledger.Usage.Valid（负数、chat 缺 completion_tokens）
// 都算未知（nil），不补 0。
func parseUsage(raw json.RawMessage, p config.Purpose) *ledger.Usage {
	if u := decodeUsage(raw); u != nil && u.Valid(string(p)) {
		return u
	}
	return nil
}

// decodeUsage 读出 usage 对象里的三个数；不是对象、缺 prompt_tokens 或 total_tokens 时返回 nil。
// 上游的响应和调用日志里的 usage（usageAttr 写的）是同一个形状。
func decodeUsage(raw json.RawMessage) *ledger.Usage {
	var u struct {
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
		Total      *int64 `json:"total_tokens"`
	}
	if json.Unmarshal(raw, &u) != nil || u.Prompt == nil || u.Total == nil {
		return nil
	}
	usage := ledger.Usage{Prompt: *u.Prompt, Total: *u.Total}
	if u.Completion != nil {
		usage.Completion, usage.HasCompletion = *u.Completion, true
	}
	return &usage
}

// 客户端头、模型别名和上游给的请求号进日志前的统一规则：合法 UTF-8、不超过 128 个字符、没有控制字符。
const maxLoggedLen = 128

func loggable(s string) bool {
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > maxLoggedLen {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// checked 返回符合 loggable 的值，不符合的返回空串。外部来的值进账本前都经过它，和日志同一条规则。
func checked(s string) string {
	if !loggable(s) {
		return ""
	}
	return s
}

// appendChecked 记下一个外部来的值：空的不记；合法的记原值；不合法的不记值，只标 <name>_invalid。
func appendChecked(attrs []slog.Attr, name, v string) []slog.Attr {
	switch {
	case v == "":
		return attrs
	case loggable(v):
		return append(attrs, slog.String(name, v))
	default:
		return append(attrs, slog.Bool(name+"_invalid", true))
	}
}

// logCall 每次调用写一行，d 是总耗时。不记 Key、上游 Key、请求体和响应体。重放和缓存命中的请求没调上游，所以没有 usage。
func (g *gateway) logCall(c *call, d time.Duration) {
	attrs := []slog.Attr{
		slog.String("request_id", c.id),
		slog.String("endpoint", string(c.purpose)),
		slog.Int64("duration_ms", d.Milliseconds()),
	}
	attrs = traceAttrs(attrs, c)
	if c.status != 0 {
		attrs = append(attrs, slog.Int("status", c.status))
	}
	if c.key != nil {
		attrs = append(attrs, slog.Int64("key_id", c.key.ID), slog.String("account", c.key.Account))
	}
	attrs = appendChecked(attrs, "model_alias", c.alias)
	attrs = routeLogAttrs(attrs, c)
	if c.upstreamStatus != 0 {
		attrs = append(attrs, slog.Int("upstream_status", c.upstreamStatus))
	}
	if c.errorCode != "" {
		attrs = append(attrs, slog.String("error_code", c.errorCode))
	}
	if c.invalidReason != "" {
		attrs = append(attrs, slog.String("invalid_reason", string(c.invalidReason)))
	}
	attrs = appendChecked(attrs, "client_request_id", c.clientRequestID)
	attrs = appendChecked(attrs, "run_id", c.runID)
	attrs = appendChecked(attrs, "idempotency_key", c.idempotencyKey)
	if c.replayOf != "" {
		attrs = append(attrs, slog.String("replay_of", c.replayOf))
	}
	attrs = appendChecked(attrs, "upstream_x_request_id", c.upstreamXRequestID)
	attrs = appendChecked(attrs, "upstream_response_id", c.upstreamResponseID)
	attrs = appendChecked(attrs, "upstream_model", c.upstreamModel)
	if c.upstreamCalled {
		attrs = append(attrs, usageAttr(c.usage))
	}
	if c.outcome != "" {
		attrs = append(attrs, slog.String("call_status", string(c.callStatus)), slog.String("outcome", string(c.outcome)))
	}
	if c.reservation != nil {
		attrs = append(attrs, slog.Int64("reserved_tokens", c.reservation.Amount()))
	}
	attrs = cacheLogAttrs(attrs, c)
	attrs = clientLogAttrs(attrs, c)
	if c.ledgerError != "" {
		attrs = append(attrs, slog.String("ledger_error", c.ledgerError))
	}
	if c.deniedBy != nil {
		attrs = append(attrs, slog.String("rate_limited_by", c.deniedBy.Name()))
	}
	if c.rateLimiterError != "" {
		attrs = append(attrs, slog.String("rate_limiter_error", c.rateLimiterError))
	}
	g.log.LogAttrs(context.Background(), slog.LevelInfo, "call", attrs...)
}

// routeLogAttrs 是调用行里上游的字段：upstream（最后发给的上游）、failover_from（换过上游时才有）、
// circuit_open（所有目标的上游都熔断着、快速失败时才有）。
func routeLogAttrs(attrs []slog.Attr, c *call) []slog.Attr {
	if c.target != nil {
		attrs = append(attrs, slog.String("upstream", c.target.Upstream.Name))
	}
	if len(c.failoverFrom) > 0 {
		attrs = append(attrs, slog.Any("failover_from", c.failoverFrom))
	}
	if c.circuitOpen {
		attrs = append(attrs, slog.Bool("circuit_open", true))
	}
	return attrs
}

// cacheLogAttrs 是开了缓存的嵌入请求多的字段：cache（查缓存的结果）、cached_from（命中时原来那次的调用号）、
// cache_error（读、写缓存出错时）。
func cacheLogAttrs(attrs []slog.Attr, c *call) []slog.Attr {
	if c.cache != "" {
		attrs = append(attrs, slog.String("cache", c.cache))
	}
	if c.cachedFrom != "" {
		attrs = append(attrs, slog.String("cached_from", c.cachedFrom))
	}
	if c.cacheError != "" {
		attrs = append(attrs, slog.String("cache_error", c.cacheError))
	}
	return attrs
}

// clientLogAttrs 是调用行里写回客户端的字段：client_disconnected（客户端走了才有），流式的 stream、first_event_ms
// （收到了事件才有），write_timeout（写超过期限才有）。
func clientLogAttrs(attrs []slog.Attr, c *call) []slog.Attr {
	if c.clientDisconnected {
		attrs = append(attrs, slog.Bool("client_disconnected", true))
	}
	if c.stream {
		attrs = append(attrs, slog.Bool("stream", true))
	}
	if !c.firstEventAt.IsZero() {
		attrs = append(attrs, slog.Int64("first_event_ms", c.firstEventAt.Sub(c.start).Milliseconds()))
	}
	if c.writeTimeout {
		attrs = append(attrs, slog.Bool("write_timeout", true))
	}
	return attrs
}

func usageAttr(u *ledger.Usage) slog.Attr {
	if u == nil {
		return slog.String("usage", "unknown")
	}
	fields := []any{slog.Int64("prompt_tokens", u.Prompt)}
	if u.HasCompletion {
		fields = append(fields, slog.Int64("completion_tokens", u.Completion))
	}
	return slog.Group("usage", append(fields, slog.Int64("total_tokens", u.Total))...)
}

// logProof 是调用日志里当证据用的字段。
type logProof struct {
	Msg       string          `json:"msg"`
	RequestID string          `json:"request_id"`
	Outcome   ledger.Status   `json:"outcome"`
	Usage     json.RawMessage `json:"usage"`
}

// ReadCallLog 从 QF 的调用日志（每行一个 JSON，docker compose logs --no-log-prefix 的原样输出）里取出证据。
// 只看预留过的调用（有 outcome 的 msg=call 行）：outcome 是 SETTLED 且 usage 是对象的，凭日志里的用量结算；
// outcome 是 CANCELLED 的（确定没发出、上游 4xx）释放。outcome 是 USAGE_UNKNOWN 的不算证据，只计数（unknown）。
// outcome 是 CACHED 的（缓存命中，写入时就已结清、从没发给上游）不是任何调用的证据，跳过。
// 别的行（启动日志、查询、鉴权失败、重放）跳过。不是 JSON 的行、字段不对的调用行、同一个调用号出现两次，返回 ledger.ErrBadEvidence。
func ReadCallLog(r io.Reader) (proofs []ledger.Proof, unknown int, err error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for n := 1; sc.Scan(); n++ {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var l logProof
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return nil, 0, fmt.Errorf("%w: line %d is not a JSON log line", ledger.ErrBadEvidence, n)
		}
		if l.Msg != "call" || l.Outcome == "" {
			continue
		}
		p := ledger.Proof{CallID: l.RequestID, Line: n, Status: l.Outcome, Source: ledger.CallLog}
		switch {
		case !ledger.ValidCallID(l.RequestID):
			return nil, 0, fmt.Errorf("%w: line %d: request_id is not a QF call id", ledger.ErrBadEvidence, n)
		case l.Outcome == ledger.UsageUnknown:
			unknown++
			continue
		case l.Outcome == ledger.Cached:
			continue
		case l.Outcome == ledger.Settled:
			if p.Usage = decodeUsage(l.Usage); p.Usage == nil {
				return nil, 0, fmt.Errorf("%w: line %d: a SETTLED outcome without usage", ledger.ErrBadEvidence, n)
			}
		case l.Outcome != ledger.Cancelled:
			return nil, 0, fmt.Errorf("%w: line %d: unknown outcome %q", ledger.ErrBadEvidence, n, l.Outcome)
		}
		proofs = append(proofs, p)
	}
	if err := sc.Err(); err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ledger.ErrBadEvidence, err)
	}
	return proofs, unknown, ledger.CheckDistinct(proofs)
}
