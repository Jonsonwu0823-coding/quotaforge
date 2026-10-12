package gateway

import (
	"encoding/json"
	"net/http"
)

// apiError 是返回给客户端的错误。错误码和 HTTP 状态的对照只在这里定义。
// message 是固定文字，不带上游原文、Key 或地址。
type apiError struct {
	status     int
	typ        string
	code       string
	message    string
	retryAfter string
}

var (
	errInvalidAPIKey      = &apiError{status: 401, typ: "authentication_error", code: "invalid_api_key", message: "Invalid or missing API key."}
	errRequestTooLarge    = &apiError{status: 413, typ: "invalid_request_error", code: "request_too_large", message: "Request body exceeds 2 MiB."}
	errInvalidRequest     = &apiError{status: 400, typ: "invalid_request_error", code: "invalid_request", message: "Request body must be a JSON object with a string model field."}
	errStreamNotSupported = &apiError{status: 400, typ: "invalid_request_error", code: "stream_not_supported", message: "Streaming is not supported."}
	errModelNotFound      = &apiError{status: 404, typ: "invalid_request_error", code: "model_not_found", message: "The model does not exist for this endpoint."}
	errModelNotAllowed    = &apiError{status: 403, typ: "permission_error", code: "model_not_allowed", message: "This API key is not allowed to use the model."}

	// 额度不够：立即拒绝、不排队。Retry-After 由网关按配置填上（retryAfterSeconds），这里不写死。
	errQuotaExhausted = &apiError{status: 429, typ: "insufficient_quota", code: "quota_exhausted", message: "The account's token quota is exhausted."}
	// 被限流：立即拒绝、不排队。Retry-After 是不够的桶补够所需的秒数，由网关按限流器的回答填上。
	errRateLimited = &apiError{status: 429, typ: "rate_limit_error", code: "rate_limited", message: "Rate limit exceeded; retry after the number of seconds in Retry-After."}
	// 幂等键：格式不对；同一个键配了不同的请求；同一个键的上一次还在处理、结果未知、成功了但响应太大没存下，
	// 或者已经按证据结算、没有响应。后几种都不会再执行；只有还在处理的稍后可能有结果，带 Retry-After。
	errInvalidIdempotencyKey       = &apiError{status: 400, typ: "invalid_request_error", code: "invalid_idempotency_key", message: "The Idempotency-Key header must be sent once, with 1 to 128 visible ASCII characters."}
	errIdempotencyKeyReused        = &apiError{status: 422, typ: "invalid_request_error", code: "idempotency_key_reused", message: "This Idempotency-Key was already used with a different request."}
	errIdempotencyInProgress       = &apiError{status: 409, typ: "conflict_error", code: "idempotency_in_progress", message: "A request with this Idempotency-Key is still in progress.", retryAfter: "1"}
	errIdempotencyOutcomeUnknown   = &apiError{status: 409, typ: "conflict_error", code: "idempotency_outcome_unknown", message: "A request with this Idempotency-Key was sent to the upstream and its outcome is unknown; it will not be run again."}
	errIdempotencyResponseTooLarge = &apiError{status: 409, typ: "conflict_error", code: "idempotency_response_too_large", message: "A request with this Idempotency-Key already succeeded, but its response was too large to keep; it will not be run again."}
	// 上一次已经按账单或日志结算（计过费），QF 没有它的响应：不会再执行。
	errIdempotencyResponseUnavailable = &apiError{status: 409, typ: "conflict_error", code: "idempotency_response_unavailable", message: "A request with this Idempotency-Key was completed and billed, but its response was not kept; it will not be run again."}

	// 调用状态查询：不存在、属于别的账户、调用号格式不对，都是这一个，不透露调用是否存在。
	errCallNotFound = &apiError{status: 404, typ: "invalid_request_error", code: "call_not_found", message: "No call with this id."}

	// 账本数据库不可用或超时：拒绝新请求，不放行。
	errLedgerUnavailable = &apiError{status: 503, typ: "server_error", code: "ledger_unavailable", message: "The quota ledger is unavailable."}
	// 限流用的 Redis 不可用或超时：拒绝新请求，不放行。
	errRateLimiterUnavailable = &apiError{status: 503, typ: "server_error", code: "rate_limiter_unavailable", message: "The rate limiter is unavailable."}

	errUpstreamBadRequest      = &apiError{status: 400, typ: "invalid_request_error", code: "upstream_bad_request", message: "The upstream rejected the request."}
	errUpstreamAuthFailed      = &apiError{status: 502, typ: "upstream_error", code: "upstream_auth_failed", message: "The gateway could not authenticate with the upstream."}
	errUpstreamRateLimited     = &apiError{status: 503, typ: "upstream_error", code: "upstream_rate_limited", message: "The upstream is rate limiting requests."}
	errUpstreamError           = &apiError{status: 502, typ: "upstream_error", code: "upstream_error", message: "The upstream failed to process the request."}
	errUpstreamUnavailable     = &apiError{status: 502, typ: "upstream_error", code: "upstream_unavailable", message: "Could not connect to the upstream."}
	errUpstreamTimeout         = &apiError{status: 504, typ: "upstream_error", code: "upstream_timeout", message: "The upstream did not respond in time."}
	errUpstreamInvalidResponse = &apiError{status: 502, typ: "upstream_error", code: "upstream_invalid_response", message: "The upstream returned a response the gateway cannot use."}

	// 客户端已经断开：不写响应，只在日志里记下。
	errClientCanceled = &apiError{code: "client_canceled"}
)

// fromUpstreamStatus 把上游的非 2xx 状态映射成错误；2xx 返回 nil。
func fromUpstreamStatus(status int) *apiError {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == 401 || status == 403:
		return errUpstreamAuthFailed
	case status == 429:
		return errUpstreamRateLimited
	case status >= 500:
		return errUpstreamError
	case status >= 400:
		return errUpstreamBadRequest
	default: // 3xx：不跟随重定向
		return errUpstreamInvalidResponse
	}
}

func (e *apiError) withRetryAfter(v string) *apiError {
	if v == "" {
		return e
	}
	c := *e
	c.retryAfter = v
	return &c
}

type errorBody struct {
	Error struct {
		Message string  `json:"message"`
		Type    string  `json:"type"`
		Param   *string `json:"param"`
		Code    string  `json:"code"`
	} `json:"error"`
}

// writeError 写出 OpenAI 格式的错误体，并把错误码记到本次调用上。
func writeError(w http.ResponseWriter, c *call, e *apiError) {
	c.errorCode = e.code
	if e.status == 0 {
		return
	}
	c.status = e.status
	var b errorBody
	b.Error.Message, b.Error.Type, b.Error.Code = e.message, e.typ, e.code
	if e.retryAfter != "" {
		w.Header().Set("Retry-After", e.retryAfter)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(b)
}
