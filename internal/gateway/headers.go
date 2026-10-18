package gateway

// 和客户端（QS）约定的、网关读写的请求头和响应头：网关里只在这里写头名。写成规范形式：Header 的 Get、Set 本来就按规范形式找，
// idempotencyKey 直接查 map，必须是规范形式。通用的头（Content-Type、Authorization、Accept、Cache-Control）照常写字面量。
// 测试、压测工具和假上游各写自己的字面量：被测的和测它的用同一个常量，常量写错了两边一起错，测不出来。
const (
	headerRequestID          = "X-Request-Id"        // 响应：这次调用的 QF 调用号（重放时是原来那次的）；也从上游的响应里读它的请求号
	headerClientRequestID    = "X-Client-Request-Id" // 请求：客户端的请求号；发给上游时换成 QF 的调用号
	headerRunID              = "X-Run-Id"            // 请求：客户端的 run，只记日志和账本
	headerIdempotencyKey     = "Idempotency-Key"     // 请求：幂等键
	headerIdempotentReplayed = "Idempotent-Replayed" // 响应：重放时是 true
	headerRetryAfter         = "Retry-After"         // 响应：错误带着该等几秒时（429、409 idempotency_in_progress、上游给的）；也从上游的错误响应里读
	headerCacheStatus        = "Cache-Status"        // 响应：开了缓存的嵌入请求查缓存的结果（RFC 9211）
)
