# 接口和幂等

## 接口

| 方法与路径 | 作用 |
|---|---|
| `POST /v1/chat/completions` | OpenAI 兼容的聊天转发；`stream: true` 时按 SSE 逐个转发上游的事件（见[流式](#流式)） |
| `POST /v1/embeddings` | OpenAI 兼容的嵌入转发；配置开了缓存时，相同的请求可以命中缓存（见[嵌入缓存](#嵌入缓存)） |
| `GET /v1/calls/{id}` | 调用状态查询：按 QF 的调用号查这次调用的结局（见[调用状态查询](ledger.md#调用状态回收和对账未知用量)） |
| `GET /healthz` | 存活检查，不需要鉴权，不查数据库 |

- 鉴权：`Authorization: Bearer <QF 发的 Key>`。Key 存在数据库里（只存哈希），每个请求都查库，吊销立即生效。
- 转发时只把请求体顶层的 `model` 换成上游模型名，其余字节不改；上游的 2xx 响应体原样返回，`model` 是上游给的实际模型名。别名有多个目标时，换到哪个目标就按它的上游模型名从原始请求体改写（见[多个上游](operations.md#多个上游)）。
- 发给上游的请求头只有 `Authorization`（上游的 Key）、`Content-Type`、`Accept`（`application/json`，流式时是 `text/event-stream`）和 `X-Client-Request-Id`。最后这个的值是这次调用的 QF 调用号：上游超时、拿不到上游的请求号时，凭它向上游查这次调用、要按调用的账单（OpenAI 的 API 文档里这个头就是这个用途：调用方自己设、每个请求一个值、ASCII、不超过 512 个字符）。客户端发来的 `X-Client-Request-Id`、`X-Run-Id` 只记录，不转发。
- QF 读请求体顶层的 `model`、`stream`、`max_tokens` 三个字段，流式时还有 `stream_options`（见[流式](#流式)）。它们重复出现，或者有大小写、Unicode 折叠变体（`MODEL`、`Max_Tokens`、`ſtream`），一律 400 `invalid_request`：有的上游解析键名不区分大小写，QF 和上游必须读到同一个值。`max_tokens` 必须是 0 到 2³¹−1 的整数（`1.5`、`1e3`、`"8"` 都是 400），`null` 当作没写。
- 每个响应都带 `x-request-id`（QF 的调用号，也是账本里调用的编号）。错误体是 OpenAI 格式：`{"error":{"message":…,"type":…,"param":null,"code":…}}`。
- 每次调用写一行 JSON 日志到标准输出：调用号、Key 编号（数据库里的数字 id）、账户、别名、上游、写回客户端的状态（`status`；客户端已断开、什么都没写回时没有）、上游的状态、耗时、错误码、`X-Client-Request-Id`、`X-Run-Id`、上游的请求号和模型名、用量（拿不到就是 `"unknown"`）、`call_status`（账本里实际记下的调用状态）、`outcome`（网关自己判定的结局，和账本写没写成功无关，见[上游结果怎么收尾](ledger.md#调用状态回收和对账未知用量)；只有预留成功的调用和缓存命中有，命中的是 `CACHED`）、`reserved_tokens`（预留量，预留过才有）、`cache`、`cached_from`、`cache_error`（开了缓存的嵌入请求才有，见[嵌入缓存](#嵌入缓存)）、`ledger_error`（账本操作失败的原因）、`failover_from`（换过上游时才有：之前试过、确定没收到请求的上游）、`circuit_open`（所有目标的上游都熔断着、快速失败时才有）、`client_disconnected`（写回之前客户端已经断开；流式是流结束之前）、`stream`（流式的调用才有，值是 `true`）、`first_event_ms`（流式：从调用开始到收到上游第一个事件的毫秒数）、`write_timeout`（流式：客户端不读，写一个事件超过了期限，同时有 `client_disconnected`）、`idempotency_key`（请求带的幂等键，和客户端头同一条规则记）、`replay_of`（重放时是原来的调用号）、`rate_limited_by`（被限流时是哪个桶，例如 `key/rpm`）、`rate_limiter_error`（准入或修正时 Redis 的错误原文，不含 URL）、`invalid_reason`（400 `invalid_request` 的原因，取值固定，见[日志关联](observability.md#日志关联)）、`trace_id` 和 `span_id`（开着追踪、这次调用被采样时才有：所在的 trace 和根 span）、`parent_span_id`（同样的条件下，请求带了合法的 `traceparent` 时才有：其中的 parent-id，即客户端那一次请求的 span id）。不记 Key、密码、连接串和正文。
- 调用状态查询每次写一行 `msg: "lookup"`（调用号、Key 编号、账户、被查的调用号、状态码、错误码、耗时），不是 `msg: "call"`：不算一次模型调用，用量导出和对账都不读它。

### 额度和限流的错误

| 状态 | `code` | 什么时候 | 调上游吗 |
|---|---|---|---|
| 429 | `quota_exhausted` | 账户的可用额度不够这次预留。立即返回，不排队。`type` 是 `insufficient_quota` | 不调 |
| 429 | `rate_limited` | 被限流：Key、账户或上游的某个令牌桶不够（见[限流](rate-limiting.md)）。立即返回，不排队。`type` 是 `rate_limit_error`，带 `Retry-After` | 不调 |
| 503 | `ledger_unavailable` | 账本数据库不可用或超时（查 Key、预留、发送意图、记缓存命中，每次操作 2 秒），或者发送意图没落成（调用已被回收）。拒绝，不放行 | 不调 |
| 503 | `rate_limiter_unavailable` | 限流用的 Redis 不可用或超时（每次操作 0.5 秒）。只影响要检查令牌桶的请求。拒绝，不放行；`type` 是 `server_error`，不带 `Retry-After` | 不调 |
| 502 | `upstream_unavailable` | 这个别名所有目标的上游都熔断着（见[多个上游](operations.md#多个上游)）。立即返回，在准入和预留之前，账本里没有这次调用；`type` 是 `upstream_error`，不带 `Retry-After`。和“连不上上游”是同一个错误码，日志有 `circuit_open: true` | 不调 |

- `quota_exhausted` 一定带 `Retry-After`，值是较大的上游超时（整秒，默认 25）：不经管理员改上限，额度回来的唯一途径是进行中的调用结算或释放，它们都在一个上游超时内结束。额度真的用完时，等多久都不会好，要 `qfctl account set-limit`。
- `rate_limited` 的 `Retry-After` 是不够的桶里补够所需时间最长的那个，向上取整到秒，至少 1。
- 429 只用于 QF 自己的额度和限流。上游的 429 仍是 503 `upstream_rate_limited`。
- 预留之前就被拒的请求（401、403、404、400、429、503，以及全部熔断时的 502 `upstream_unavailable`）只进调用日志，不进账本。
- 上游挂了时：只有一个目标的别名，连续几次连不上（或超时、5xx）之后熔断打开，之后的请求立即得到 502 `upstream_unavailable`，不再等上游超时（504）；有备用目标的别名直接去备用。

## 流式

`POST /v1/chat/completions` 的请求体顶层 `stream` 是 `true` 时，QF 把上游的 SSE 事件流逐个转发给客户端。嵌入接口不看 `stream`，和以前一样原样转发。

- **响应头**：上游回 2xx 之后才写：上游的状态码和 `Content-Type`（`text/event-stream`），加上 `Cache-Control: no-cache` 和 `x-request-id`；不转发上游的其它头。
- **事件原样转发**：每个事件（到空行为止的字节，含行尾）写完就刷新，不攒；`model` 是上游给的实际模型名。只有一处例外，见下面的 usage 事件。单个事件最大 1 MiB，超了按上游的响应不能用处理。
- **流开始之前失败**（上游非 2xx、连不上、超时，或者上游回了 2xx 却不是 `text/event-stream`）：和非流式一样，返回 OpenAI 格式的 JSON 错误和对应的状态码。
- **`stream_options`**：QF 按流里的 usage 记账，所以发给上游的请求一定带 `"include_usage": true`。只改这一处，其余字节不动；幂等的指纹仍按客户端发来的原始请求体算。

  | 客户端写的 | 发给上游的 | usage 那个事件转发给客户端吗 |
  |---|---|---|
  | 没写 `stream_options` | 在顶层对象开头插入 `"stream_options":{"include_usage":true},` | 不转发 |
  | `"stream_options": null` | 换成 `{"include_usage":true}` | 不转发 |
  | 对象，没有 `include_usage` | 在对象开头插入 `"include_usage":true`，其它成员不动 | 不转发 |
  | `"include_usage": false` 或 `null` | 这个值换成 `true`，其它成员不动 | 不转发 |
  | `"include_usage": true` | 原样 | 原样转发 |
  | `stream_options` 不是对象或 null，`include_usage` 不是布尔值或 null | 不调上游，400 `invalid_request`（日志 `invalid_reason: stream_options_invalid`） | — |

  - 和 `model` 等字段一样，`stream_options` 和其中的 `include_usage` 重复出现，或者有大小写、Unicode 折叠变体，都是 400 `invalid_request`（`ambiguous_field`）。`stream` 不是 `true` 时 QF 不读也不改 `stream_options`，原样转发。
  - **不转发的只有一个事件**：`choices` 是空数组、带着不为 null 的 `usage` 的那个，也就是开了 `include_usage` 才会多出来的事件（不少客户端代码直接取 `choices[0]`）。客户端没要 usage 时看到的流，除了下面这个字段，和没开这个选项时一样：开了 `include_usage` 的上游会在其它每个事件里加上 `"usage": null`，这些事件照样原样转发，所以客户端会看到这个多出来的字段。
  - 上游如果把 usage 放在带内容的事件里（`choices` 不为空），这个事件照常转发，用量照常取。流里有几个 usage 时取最后一个。
- **百炼（阿里云 Model Studio，OpenAI 兼容模式）的流**：每个事件是一行 `data: {chat.completion.chunk}` 加一个空行。开了 `include_usage` 时，带 `finish_reason` 的那个事件里 `usage` 是 null，之后一个 `choices` 为空数组、带 usage 的事件，最后是 `data: [DONE]`。依据：Model Studio 的文档（[Streaming output](https://www.alibabacloud.com/help/en/model-studio/stream)：设了 `stream_options={"include_usage": true}`，最后一个数据块带 token 用量；[OpenAI compatible - Chat](https://www.alibabacloud.com/help/zh/model-studio/compatibility-of-openai-with-dashscope)）和一份公开的原始流（[vercel/ai#4929](https://github.com/vercel/ai/issues/4929)）。流中途出错时百炼发什么，没有找到文档，QF 按下面的表处理。
- **流中途结束时客户端收到什么**（响应头和 200 已经写出，不能再改状态码）：

  | 情况 | 客户端收到的最后几个事件 | 结局 | 错误码（日志、账本） |
  |---|---|---|---|
  | 正常结束，有合法的 usage | 原样转发的事件，最后是 `data: [DONE]` | `SETTLED` | — |
  | 正常结束，没有 usage 或 usage 不合法 | 同上 | `USAGE_UNKNOWN` | — |
  | 到了总时长上限（见下面的时限） | QF 补的错误事件 `data: {"error":{…,"code":"upstream_timeout"}}`，然后关闭，没有 `[DONE]` | `USAGE_UNKNOWN` | `upstream_timeout` |
  | 上游的连接断了，或者没发 `[DONE]` 就结束 | QF 补的错误事件，`code` 是 `upstream_error`，没有 `[DONE]` | `USAGE_UNKNOWN` | `upstream_error` |
  | 上游的一个事件超过 1 MiB，或者 data 既不是 JSON 对象也不是 `[DONE]` | 这个事件不转发；QF 补的错误事件，`code` 是 `upstream_invalid_response`，没有 `[DONE]` | `USAGE_UNKNOWN` | `upstream_invalid_response` |
  | 上游自己发了错误事件（顶层带 `error` 的对象） | 上游的错误事件原样转发，之后上游发的也照转发；QF 不再补 | `USAGE_UNKNOWN` | `upstream_error` |

  - QF 补的错误事件和非流式的错误体同一个形状：`{"error":{"message":…,"type":"upstream_error","param":null,"code":…}}`。官方 SDK 遇到带 `error` 的事件会报错，客户端分得出流被截断了。
  - 用量未知的照现在的规则继续占着预留，不补 0，等对账（见[调用状态、回收和对账未知用量](ledger.md#调用状态回收和对账未知用量)）。
- **时限**：整个流受 chat 的上游超时（`timeouts.chat_ms`，默认 25 秒）管，和非流式同一个口径：从发出上游请求算起，管到读完流。等第一个事件、两个事件之间的等待都在这个上限之内，不另设。超过 25 秒的回答会被截断（记未知），和非流式超时得到 504 一样；要更长，调大 `timeouts.chat_ms`（回收上界和退出上限跟着变，见[运行](operations.md)）。
- **慢客户端**：每次写给客户端（转发的事件和 QF 补的错误事件一样）的期限是 5 秒，而且不越过“总时长上限 + 1 秒”。读在总时长上限截断；多给写的 1 秒，是让到点时已经读到的事件和补的错误事件写得出去、响应正常结束。客户端不读、写超过 5 秒的期限时，QF 不再写，当客户端走了处理（日志 `client_disconnected` 加上 `write_timeout`）；到了“上限 + 1 秒”还写不完的，按上游超时记，不算客户端的。QF 一边读上游一边写客户端，内存里最多攒一个事件。
- **客户端中途断开**：QF 不取消上游，接着读完（丢弃，不再写），拿到合法 usage 照常结算（有界结算）。代价是上游会把整个回答生成完、照常计费，QF 也照常记账：取消上游的话百炼不给 usage，只能记未知、一直占着预留。
- **收尾在流结束之后**：非流式时客户端拿到响应，账本已经记好；流式做不到（事件边读边写），QF 读完上游的流才收尾。客户端刚收到 `[DONE]` 就查 `GET /v1/calls/{id}`，可能还看到 `IN_FLIGHT`，稍后再查。收尾失败时客户端已经收完了数据，看不出区别；调用留给回收，和非流式一样。
- **幂等**：流式的调用不能重放，见[幂等](#幂等idempotency-key)的“流式”。

## 用 curl 调用

`$KEY` 是 `qfctl key issue` 发的 Key。bash：

```bash
# 在项目根目录下运行
# 聊天
curl -sS -i http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"你好"}],"temperature":0,"max_tokens":64,"stream":false}'
# 带 tools 的聊天
curl -sS -i http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"fake-tool-call","messages":[{"role":"user","content":"查一下"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{}}}}],"tool_choice":"auto","parallel_tool_calls":false}'
# 流式的聊天：-N 关掉 curl 自己的缓冲，事件一到就显示
curl -sS -N http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"你好"}],"stream":true}'
# 嵌入
curl -sS -i http://127.0.0.1:8080/v1/embeddings -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"qs-fake-upstream-v1","input":["一","二"],"dimensions":4}'
```

PowerShell（用 `curl.exe`，请求体放进文件，避免引号转义；`$key` 是发的 Key）：

```powershell
Set-Content -Encoding utf8NoBOM chat.json '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"你好"}],"temperature":0,"max_tokens":64,"stream":false}'
curl.exe -sS -i http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $key" -H 'Content-Type: application/json' --data-binary '@chat.json'

Set-Content -Encoding utf8NoBOM emb.json '{"model":"qs-fake-upstream-v1","input":["一","二"],"dimensions":4}'
curl.exe -sS -i http://127.0.0.1:8080/v1/embeddings -H "Authorization: Bearer $key" -H 'Content-Type: application/json' --data-binary '@emb.json'
```

（`utf8NoBOM` 需要 PowerShell 7；Windows PowerShell 5.1 用 `[IO.File]::WriteAllText("$PWD\chat.json", '…')`。）

## 嵌入缓存

配置里有 `embedding_cache` 这一段时（见[嵌入缓存](operations.md#嵌入缓存)），`POST /v1/embeddings` 的相同请求可以直接用之前结算过的响应，不再调上游、不扣额度。没有这一段时什么都不查，和没有这个功能时一样。聊天不缓存。

- **什么算相同**：同一个账户，发给同一个上游、同一个上游模型名、规范化之后相同的请求体（规范化和[幂等](#幂等idempotency-key)的请求指纹是同一条规则：键的顺序、空白、转义的写法不影响）。别名不算：两个别名指向同一个目标，可以互相命中。缓存的键是“账户号:SHA-256”，没有明文。
- **什么时候查**：鉴权、请求体和模型检查、选目标、准入（[限流](rate-limiting.md)）之后，预留之前。所有目标都熔断着的 502 在更前面，不拿缓存兜底。
- **不走缓存**：带 `Idempotency-Key` 的请求不读也不写缓存（重试拿到同一个结果由幂等管，重放的 `x-request-id` 是原来的调用号）；请求体规范化不了的（非法 UTF-8、孤立的代理项转义）照常转发。日志记 `cache: bypass`，响应不带 `Cache-Status`。
- **命中时**：不预留、不调上游、额度不动，额度用完的账户也照样命中（和重放一样）。账本里记一条 `CACHED` 的调用（预留量 0，用量照抄原来那次，`cached_from` 是原来那次的调用号，见[调用状态](ledger.md#调用状态回收和对账未知用量)）；这一条写不进去时返回 503 `ledger_unavailable`（不带 `Cache-Status`），不给缓存里的正文：QF 返回的 `x-request-id` 都要在账本里查得到。
  - 状态码、`Content-Type`、正文逐字节照原来那次。所以正文里的 `id`、`model` 是原来那次上游给的，和这次的 `x-request-id`（这次 `CACHED` 调用自己的号）不同。
  - 限流：照样占 1 个 RPM；TPM 先按估算扣，结束时全部退回（和重放一样）。
- **响应头 `Cache-Status`**（RFC 9211）：`quotaforge; hit` 是从缓存读到的；`quotaforge; fwd=miss; collapsed` 是和同时在进行的相同请求合并、拿到了它的结果（见下面的“并发”），也是 `CACHED`；`quotaforge; fwd=miss` 是这次去了上游（没命中、缓存出错，或者合并的那一个没拿到结果）。不走缓存的、写 `CACHED` 失败的 503 不带这个头。
- **什么会被缓存**：结算了的（账本里是 `SETTLED`）、上游回 200、正文不超过 1 MiB 的响应。错误、用量未知、收尾没写成的都不缓存。条目在配置的过期时长（默认 24 小时）乘以 0.9 到 1.1 之间的随机数之后过期：同一批写进去的不会同时过期，不会一起回源（雪崩）。
- **一致性**：上游悄悄换掉同一个模型名背后的模型时，旧的向量最多再用一个过期时长。要马上换，就换一个上游模型名，或者清掉缓存。
- **并发**（防击穿）：同一个实例里同时到的相同请求，只有第一个去读缓存、回源，其余的等它。它结算成功，其余的都当命中（`collapsed`），各记一条 `CACHED`；它没成功（上游出错、超时、收尾失败，或者换了上游、回答它的是另一个目标），其余的各自照常回源，不共享它的失败。等的时候客户端断开就不再等，不留账本记录。两个实例各自合并，第一个请求成功时，跨实例同一个请求最多各回源一次。
  - **最坏的延迟**：等的请求不另设上限：它只等第一个请求读缓存、回源、收尾、写缓存这几步，每一步都有上限；第一个请求结算之后（成功或失败）就不再让别人等，和它自己的客户端读得快慢无关。最坏是“等它超时（默认约 25 秒，加上几次账本操作），再自己回源一次、也等到超时”，大约是平时的两倍。只在上游慢到超时、又正好有并发的相同请求时出现；连续失败让这个上游的熔断打开之后，请求立即得到 502，就不会再出现。
- **多花的时间**：读缓存、写缓存各最多 0.1 秒（`config.CacheTimeout`），通常约 1 毫秒。命中：读一次缓存，加上一次账本写入。没命中：读一次缓存；结算之后先写缓存，再写回（客户端在响应开头多等这一次，总时长不变）。
- **缓存出错**（缓存的 Redis 停了、超时、条目坏了）：当作没命中，照常回源，不是 503。只有 Redis 出错（停了、超时）的，结算后也不写，所以缓存坏着时每个请求最多多等一次读（0.1 秒）；条目坏了的照常回源，结算后用新的条目覆盖它。原因记在日志的 `cache_error`。

## 幂等（Idempotency-Key）

带 `Idempotency-Key` 请求头的请求，同一个键的重试不会多扣额度：保留期内，一个键最多一次可能被计费的上游调用。chat 和 embeddings 都支持；不带这个头时，行为和没有这个功能时完全一样。

- **格式**：1 到 128 个可见 ASCII 字符（0x21–0x7E），按字节比较，区分大小写。值是不透明的字符串，QF 不解析引号。为空、超长、有空格或其它字符、请求里有不止一个这个头，都是 400 `invalid_idempotency_key`，不预留、不调上游（不合法的键不能悄悄忽略，否则客户端以为有幂等，重试却会重复扣费）。
- **范围**：账户。不同账户的同名键互不相干；同一账户的几把 Key 共用一个空间。
- **请求指纹**：SHA-256（接口 + 规范化的原始请求体），请求头（`X-Run-Id`、`X-Client-Request-Id` 等）都不算。规范化：
  - 去掉无意义的空白，字符串解码再编码（转义的写法不同算同一个），数组保持顺序；
  - 对象的成员按“折叠后的键”排序：键里每个字符换成它的大小写和 Unicode 简单折叠里最小的那个（`temperature`、`Temperature`、`TEMPERATURE` 折叠后相同，`ſtream` 和 `stream` 也相同）；
  - 折叠后相同的键（重复的、只差大小写的、Unicode 折叠相同的）都保留，保持请求体里原来的先后，不去重、也不排成固定的先后：上游可能取前一个也可能取后一个，有的上游（例如 Go 的 `encoding/json`）认键名不区分大小写、后出现的为准，`{"Temperature":0.1,"temperature":0.9}` 和换了先后的写法在它看来是两个不同的请求；
  - 数字按字面比较，不转成浮点：转成浮点会让大整数（例如 `seed`）失真，把不同的请求当成同一个而错误地重放。代价是 `1` 和 `1.0` 算两个请求，重试时得到 422，这种误报是安全的；
  - 带键时请求体必须是合法的 UTF-8，字符串里也不能有孤立的代理项转义（例如单独的 `\ud800`），否则 400 `invalid_request`：它们解码后都变成同一个替换字符。成对的代理项正常。
- **判断**：在鉴权、请求体、模型这些检查都通过之后、预留之前，按这个账户、这个键、没有过期的记录：

  | 这个键已有的记录 | QF 的行为 | 状态码和错误码 |
  |---|---|---|
  | 没有，或已过期 | 照常执行，记下这个键 | 和不带键一样 |
  | 指纹不同（不论之前的调用处于什么状态） | 不执行 | 422 `idempotency_key_reused` |
  | 之前的调用还是 `RESERVED` 或 `IN_FLIGHT`（还在处理，或者收尾失败了、等回收） | 不执行 | 409 `idempotency_in_progress`，带 `Retry-After: 1` |
  | 之前 QF 把上游的 2xx 写回了，正文不超过 8 MiB（`SETTLED`，或者 2xx 但缺 usage 的 `USAGE_UNKNOWN`） | 重放：同样的状态码和 `Content-Type`，正文逐字节相同；`x-request-id` 是原来的调用号，另加 `Idempotent-Replayed: true` | 原来的 2xx |
  | 之前 QF 把上游的 2xx 写回了，但正文超过 8 MiB，没存快照 | 不执行：上一次已经成功，可能已经计费 | 409 `idempotency_response_too_large` |
  | 之前的调用是结算了的流式调用（`SETTLED`，记录里只有状态码这个标记，没有快照） | 不执行：上一次已经计费，内容边读边写给了客户端，QF 没有留下 | 409 `idempotency_response_unavailable` |
  | 之前的调用是 `CANCELLED`（没发出、连接没建立、上游 4xx，或者被回收、对账释放） | 重新执行，记录改为指向新的调用 | 和不带键一样 |
  | 之前的调用是 `USAGE_UNKNOWN`，没有快照（发出后超时、5xx、正文不能用） | 不执行：上一次可能已经被上游执行并计费 | 409 `idempotency_outcome_unknown` |
  | 之前的调用凭证据结算成了 `SETTLED`，没有快照（超时、断开之后凭账单或日志结算） | 不执行：上一次已经计费，响应没有保存 | 409 `idempotency_response_unavailable` |

  - 重放不预留、不调上游、不写账本，额度用完了也照样重放；鉴权、模型白名单照常检查。
  - 预留之前就失败的请求（400、401、403、404、413、429 `quota_exhausted`、预留时的 503，以及全部熔断时的 502 `upstream_unavailable`）不留记录，重试时重新判断：例如额度调高之后，同一个键可以正常执行。这些检查在查记录之前，所以本来可以重放的键，这时也得到这些错误、不重放：和被限流时得到 429 `rate_limited`、不重放一样。
  - 换过上游的调用照常记键：成功了就存快照，同一个键再来是重放；两个目标都连不上的是 `CANCELLED`，同一个键再来重新执行。
  - 几个 409 的 `type` 是 `conflict_error`，422 是 `invalid_request_error`；message 都是固定文字，不带键的值。
  - 客户端中途断开时，请求已经发出的照常收尾（有界结算），拿到 2xx 的存快照，同一个键重试得到重放。迟到的结果把未知结算掉、写回了 2xx 的，也存快照。
  - **流式**：照常记键和指纹，同一个键最多执行一次；但不存快照（内容边读边写给了客户端，QF 没有留下），所以**流式的调用不能重放**。结算了的流在记录里只记状态码，当作标记（快照的其余三列都是空的），`qfctl verify` 据此分得出它和漏存了快照的非流式调用。同一个键再来时按上表：流还没结束是 409 `idempotency_in_progress`；结算了是 409 `idempotency_response_unavailable`；用量未知（截断、没有 usage）的不记标记，是 409 `idempotency_outcome_unknown`，之后对账结清成 `SETTLED` 的也是 409 `idempotency_response_unavailable`；上游在流开始之前回了 4xx 的（`CANCELLED`）重新执行。同一个键先发非流式、再发流式，请求体不同，是 422。要能重放，就用非流式。
- **保留期**：24 小时，从这个键最近一次真正执行（记录指向一个新调用）时算起，用数据库的时间。过期的记录当作没有。
- **快照**：只存响应（状态码、`Content-Type`、正文），请求体不存，只存指纹。正文不超过 8 MiB 才存（`ledger.MaxSnapshotBody`）：实测云端写 32 MiB 要 2–3 秒，超过每次账本操作 2 秒的上限；超过的照常结算或记未知，只记状态码和正文长度。快照和收尾在同一个事务里写：别的请求看到的，要么还在处理，要么已经收尾、该有快照的有快照。
- **并发**：带键的预留在一个读已提交的事务里先锁账户的额度行，再用不加锁的普通读查记录，最后写记录。同一账户里写记录的预留、收尾和清理都先拿这把锁，所以同一个键的并发请求只有一个能执行，其余的看到“还在处理”；改记录的语句都按主键定位，先锁主键、再改 `expires_at` 索引，顺序一致。
- **清理**：每个 QF 实例的后台循环自动清理（见[回收](ledger.md#调用状态回收和对账未知用量)），`qfctl idempotency purge` 也能手动清，可以重复执行、可以和服务同时运行，输出删了多少条；部分账户出错（例如额度行被长时间锁住）时，先输出已经删掉的条数，再以错误退出（退出码 1），其余账户照常清理。先用不加锁的读取出一批过期记录的主键，每个账户一个事务：锁额度行，再逐个按主键删，条件里再判断一次过期。
  - 为什么逐个按主键删：原来按 `expires_at` 范围一条 DELETE（或者 `idem_key IN (…)`），优化器会走 `expires_at` 索引，先锁索引项（其中有别的账户的）、再锁主键行，和改指向的 upsert 顺序相反，清理和“重新使用正被清理的过期键”同时进行时会死锁（1213）。实测：原来的写法 5 轮压力测试 16 次死锁，按主键删 0 次。不对 1213 重试：重试会把锁顺序的错误藏起来。
  - **怎么翻批**：候选按 `expires_at` 索引的顺序 `(expires_at, account_id, idem_key)` 一批 100 条地取（二级索引的项里带着主键，所以这就是索引本身的顺序），比较展开成 `expires_at > ? OR (expires_at = ? AND (account_id > ? OR (account_id = ? AND idem_key > ?)))`，只读过期的项、不用排序。只取这一轮开始时（数据库时间）已经过期的：之后过期的、被重新使用的都不进这一轮，一轮一定结束。取回的一批在程序里按账户分组、组内按键排序，仍是每个账户一个事务、先锁额度行、逐个按主键删。
  - 为什么这样翻批（在一次性的 MySQL 8.4 上实测）：原来按主键 `(account_id, idem_key) > (?, ?)` 翻批，MySQL 不把这种行构造器的比较当成范围：过期的多时，每批都从主键开头扫过游标之前的所有项（10 万个过期键一轮要读约 5000 万行；100 万个时 2 分钟只扫了约 28 万个），过期的少时改走 `expires_at` 索引、每批把全部过期项排一遍。同一批的读行数：

    | 数据 | 原来（主键，行构造器） | 主键，展开成 OR | `expires_at` 索引，展开成 OR（现在） |
    |---|---|---|---|
    | 10 万个全过期，1 个账户 | 5015 万行，15.6 秒 | 10 万行，0.9 秒 | 10.2 万行，0.9 秒 |
    | 100 万个全过期，1 个账户 | 2 分钟没扫完（读了 3.9 亿行） | 100 万行，10.1 秒 | 102 万行，10.1 秒 |
    | 稳态：100 万个没过期的里夹 1000 个过期的，1000 个账户 | 1.1 万行（每批排一遍全部过期项） | 同左 | 1023 行 |

  - 一整轮（含删除）的耗时，由逐行删除主导：10 万个过期键、1 个账户 77 秒 → 81 秒；稳态 1000 个 0.7 → 0.9 秒；100 万个时两种写法 2 分钟都只删了一部分（1 个账户 13.9 万 → 16.1 万，1000 个账户 15.0 万 → 11.3 万）。按过期时间翻批时，同一批里的键分散在很多账户上，每个账户一个事务，事务变多：积压很多、而且很多账户的键在时间上交错时，比原来慢（10 万个、1000 个账户：原来 77 秒删完，现在 2 分钟删了 9.3 万）。稳态（每轮只有上一轮以来过期的那些）不受影响。原来的写法在积压更多时，候选查询本身一批就会超过 2 秒，那一轮在同一个位置失败，清理就推不动了；现在每批的读行数和积压多少无关。
- **限制**：流式的调用不能重放（见上面的“流式”）。结果未知的调用不会重新执行，同一个键一直是 409，要等对账把它结清。
