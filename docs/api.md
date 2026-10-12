# 接口和幂等

## 接口

| 方法与路径 | 作用 |
|---|---|
| `POST /v1/chat/completions` | OpenAI 兼容的聊天转发，非流式（`stream=true` 返回 400 `stream_not_supported`） |
| `POST /v1/embeddings` | OpenAI 兼容的嵌入转发 |
| `GET /v1/calls/{id}` | 调用状态查询：按 QF 的调用号查这次调用的结局（见[调用状态查询](ledger.md#调用状态回收和对账未知用量)） |
| `GET /healthz` | 存活检查，不需要鉴权，不查数据库 |

- 鉴权：`Authorization: Bearer <QF 发的 Key>`。Key 存在数据库里（只存哈希），每个请求都查库，吊销立即生效。
- 转发时只把请求体顶层的 `model` 换成上游模型名，其余字节不改；上游的 2xx 响应体原样返回，`model` 是上游给的实际模型名。
- 发给上游的请求头只有 `Authorization`（上游的 Key）、`Content-Type`、`Accept` 和 `X-Client-Request-Id`。最后这个的值是这次调用的 QF 调用号：上游超时、拿不到上游的请求号时，凭它向上游查这次调用、要按调用的账单（OpenAI 的 API 文档里这个头就是这个用途：调用方自己设、每个请求一个值、ASCII、不超过 512 个字符）。客户端发来的 `X-Client-Request-Id`、`X-Run-Id` 只记录，不转发。
- QF 读请求体顶层的 `model`、`stream`、`max_tokens` 三个字段。它们重复出现，或者有大小写、Unicode 折叠变体（`MODEL`、`Max_Tokens`、`ſtream`），一律 400 `invalid_request`：有的上游解析键名不区分大小写，QF 和上游必须读到同一个值。`max_tokens` 必须是 0 到 2³¹−1 的整数（`1.5`、`1e3`、`"8"` 都是 400），`null` 当作没写。
- 每个响应都带 `x-request-id`（QF 的调用号，也是账本里调用的编号）。错误体是 OpenAI 格式：`{"error":{"message":…,"type":…,"param":null,"code":…}}`。
- 每次调用写一行 JSON 日志到标准输出：调用号、Key 编号（数据库里的数字 id）、账户、别名、上游、写回客户端的状态（`status`；客户端已断开、什么都没写回时没有）、上游的状态、耗时、错误码、`X-Client-Request-Id`、`X-Run-Id`、上游的请求号和模型名、用量（拿不到就是 `"unknown"`）、`call_status`（账本里实际记下的调用状态）、`outcome`（网关自己判定的结局，和账本写没写成功无关，见[上游结果怎么收尾](ledger.md#调用状态回收和对账未知用量)；只有预留成功的调用有）、`reserved_tokens`（预留量）、`ledger_error`（账本操作失败的原因）、`client_disconnected`（写回之前客户端已经断开）、`idempotency_key`（请求带的幂等键，和客户端头同一条规则记）、`replay_of`（重放时是原来的调用号）、`rate_limited_by`（被限流时是哪个桶，例如 `key/rpm`）、`rate_limiter_error`（准入或修正时 Redis 的错误原文，不含 URL）、`invalid_reason`（400 `invalid_request` 的原因，取值固定，见[日志关联](observability.md#日志关联)）、`trace_id` 和 `span_id`（开着追踪、这次调用被采样时才有：所在的 trace 和根 span）、`parent_span_id`（同样的条件下，请求带了合法的 `traceparent` 时才有：其中的 parent-id，即客户端那一次请求的 span id）。不记 Key、密码、连接串和正文。
- 调用状态查询每次写一行 `msg: "lookup"`（调用号、Key 编号、账户、被查的调用号、状态码、错误码、耗时），不是 `msg: "call"`：不算一次模型调用，用量导出和对账都不读它。

### 额度和限流的错误

| 状态 | `code` | 什么时候 | 调上游吗 |
|---|---|---|---|
| 429 | `quota_exhausted` | 账户的可用额度不够这次预留。立即返回，不排队。`type` 是 `insufficient_quota` | 不调 |
| 429 | `rate_limited` | 被限流：Key、账户或上游的某个令牌桶不够（见[限流](rate-limiting.md)）。立即返回，不排队。`type` 是 `rate_limit_error`，带 `Retry-After` | 不调 |
| 503 | `ledger_unavailable` | 账本数据库不可用或超时（查 Key、预留、发送意图，每次操作 2 秒），或者发送意图没落成（调用已被回收）。拒绝，不放行 | 不调 |
| 503 | `rate_limiter_unavailable` | 限流用的 Redis 不可用或超时（每次操作 0.5 秒）。只影响要检查令牌桶的请求。拒绝，不放行；`type` 是 `server_error`，不带 `Retry-After` | 不调 |

- `quota_exhausted` 一定带 `Retry-After`，值是较大的上游超时（整秒，默认 25）：不经管理员改上限，额度回来的唯一途径是进行中的调用结算或释放，它们都在一个上游超时内结束。额度真的用完时，等多久都不会好，要 `qfctl account set-limit`。
- `rate_limited` 的 `Retry-After` 是不够的桶里补够所需时间最长的那个，向上取整到秒，至少 1。
- 429 只用于 QF 自己的额度和限流。上游的 429 仍是 503 `upstream_rate_limited`。
- 预留之前就被拒的请求（401、403、404、400、429、503）只进调用日志，不进账本。

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
  | 之前的调用是 `CANCELLED`（没发出、连接没建立、上游 4xx，或者被回收、对账释放） | 重新执行，记录改为指向新的调用 | 和不带键一样 |
  | 之前的调用是 `USAGE_UNKNOWN`，没有快照（发出后超时、5xx、正文不能用） | 不执行：上一次可能已经被上游执行并计费 | 409 `idempotency_outcome_unknown` |
  | 之前的调用凭证据结算成了 `SETTLED`，没有快照（超时、断开之后凭账单或日志结算） | 不执行：上一次已经计费，响应没有保存 | 409 `idempotency_response_unavailable` |

  - 重放不预留、不调上游、不写账本，额度用完了也照样重放；鉴权、模型白名单照常检查。
  - 预留之前就失败的请求（400、401、403、404、413、429 `quota_exhausted`、预留时的 503）不留记录，重试时重新判断：例如额度调高之后，同一个键可以正常执行。
  - 几个 409 的 `type` 是 `conflict_error`，422 是 `invalid_request_error`；message 都是固定文字，不带键的值。
  - 客户端中途断开时，请求已经发出的照常收尾（有界结算），拿到 2xx 的存快照，同一个键重试得到重放。迟到的结果把未知结算掉、写回了 2xx 的，也存快照。
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
- **限制**：流式不涉及（QF 拒绝 `stream: true`）。结果未知的调用不会重新执行，同一个键一直是 409，要等对账把它结清。
