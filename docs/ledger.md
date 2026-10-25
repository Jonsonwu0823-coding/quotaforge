# 额度、账本和未知用量

## 额度和账本

账本在 MySQL 里，是额度的唯一权威。单位是 token，每个账户一个上限，不按周期重置。

一次请求：鉴权 → 解析 → 模型检查 → 选目标（[多个上游](operations.md#多个上游)）→ **估算** → 准入（[限流](rate-limiting.md)）→ 查嵌入缓存（开了[嵌入缓存](api.md#嵌入缓存)时，只对嵌入；命中就记一条 `CACHED`、不预留，到此为止）→ **预留** → **发送意图** → 转发（拨号失败时[换上游](#换上游)）→ **收尾**（结清时在同一个事务里写[用量事件](#用量事件)的 outbox）→ 写嵌入缓存 → 写回 → 修正限流的 TPM 桶（收尾在写回之前，客户端拿到响应时账本已经记好）。

- **估算**（保守的上界）：
  - 输入 = 转发给上游的请求体字节数（一个 token 一般至少对应一个字节，不用解析消息）。请求体是按第一次选中的目标的上游模型名改写过的（流式还加上 QF 替客户端要的 `include_usage`），所以别名有多个目标时，预留量会随选中的目标差几个字节；换上游时预留量不变，不重新预留，新上游的 TPM 也按这个量扣。结算按实际用量，差的这几个字节不影响已消费；
  - 输出 = chat 请求的 `max_tokens`，没写就用模型配置的 `default_max_tokens`；embeddings 为 0；
  - 预留量 = 输入 + 输出。估算值记进 `calls`，以后用来评估估算误差。
- **预留**（一个事务）：先对额度行做条件更新（`上限 − 已预留 − 已消费 ≥ 预留量` 才扣），再插调用记录（`RESERVED`）和账本的 reserve。不够就回滚、429。不先读后写，不用分布式锁。
- **发送意图**、**收尾**、回收和对账见下一节[调用状态、回收和对账](#调用状态回收和对账未知用量)。
- **超用**：实际用量超过预留时如实记账，已消费可以超过上限（余额为负），之后的预留都会被拒。
- **锁的顺序**：每个改额度的事务（预留、收尾、回收、对账、清理幂等记录）都先锁这个账户的额度行，再按主键动 `calls`、`ledger`、`idempotency_keys`；同一账户的这些事务在额度行上排队，不会互相等成死锁。不锁额度行的写只有发送意图、[换上游](#换上游)时的改道和缓存命中的 `CACHED`：前两个只锁调用这一行；`CACHED` 是一个短事务（插调用行时外键检查只对账户和 Key 加共享锁，再写用量事件），都不在持锁时等额度行和调用行，构不成等待环。
- **用量事件的开关行**：结清（进 `SETTLED`、`CANCELLED`、`CACHED`）的事务最后写用量事件，对[开关](#用量事件)那一行加共享锁，所以结清事务的顺序是：额度行 → 调用行 → 开关行（共享锁）；打开开关的事务只锁开关这一行（排他锁）。共享锁之间不冲突，只有打开的那一刻要等正在结清的事务提交。
- **等锁的上限**：每次账本操作最多 2 秒（`config.LedgerTimeout`，由 context 限住，超了是 503 `ledger_unavailable`）。数据库连接的会话级 `innodb_lock_wait_timeout` 也设成 2 秒（`ledger.Open` 解析连接串时加上，驱动在每个新连接上 `SET`，不用改 secret 文件）：
  - 没有它时，context 到期、驱动关掉连接，数据库那边的语句却还在等锁，要等到默认的 50 秒才放弃；额度行被锁住、并发又高时，等锁的线程越堆越多。有了它，等锁最多 2 秒左右就由数据库自己放弃。测试：另一个连接锁住额度行，预留在数据库时间 2 到 3.5 秒返回 1205；网关返回 503，之后 3 秒内数据库那边没有这个请求的等锁线程。
  - 等锁超时后 MySQL 默认（`innodb_rollback_on_timeout` 关）只回滚那一条语句，事务还开着；QF 的事务出错一律整个回滚，连接回到池里是干净的（context 先到期时，驱动直接关掉连接，不放回池里）。
  - 这是所有连接的入口，`qfctl` 和迁移也带上：迁移在 QF 停着的时候跑，DDL 等的是元数据锁，归 `lock_wait_timeout` 管，不受影响；`qfctl` 的写操作（例如 `reconcile -apply`）碰上正被大量请求使用的账户，可能报 1205（`Lock wait timeout exceeded`），重跑即可。

**不变量**（`qfctl verify` 检查，不成立就列出规则、账户、调用，并以 1 退出）：

| 规则 | 内容 |
|---|---|
| `budget_reserved` | 额度行的已预留 = 账本里预留变动之和 |
| `budget_consumed` | 额度行的已消费 = 账本里消费变动之和 |
| `reserved_not_negative` | 已预留不为负 |
| `reserve_entry` | 每个调用（`CACHED` 除外）恰有一条 reserve，变动 = 它的预留量 |
| `settled` | `SETTLED`：有 settle、没有 release，预留变动合计 0，消费变动 = usage 合计 |
| `cancelled` | `CANCELLED`：有 release、没有 settle，变动合计都是 0 |
| `open` | `RESERVED`、`IN_FLIGHT`、`USAGE_UNKNOWN`：只有 reserve，继续占着自己的预留 |
| `cached` | `CACHED`（嵌入缓存命中）：是嵌入，没有任何账本行，预留量 0，用量合法，`http_status` 是 200，没有幂等键；`cached_from` 指向同一账户里一个 `SETTLED` 的调用，用途、上游、上游模型名和用量都和它相同（缓存的键里有上游名和上游模型名）。别的状态的调用没有 `cached_from`。凭空写一条 `CACHED`、改它的用量、给它补账本行，都会被报出来 |
| `sent` | 发送意图的时间 `sent_at`：`RESERVED`、`CACHED` 和回收释放的（确定没发出）没有；`IN_FLIGHT`、`USAGE_UNKNOWN`、`SETTLED`，以及凭上游的回答、迟到的结果或由 `reconcile` 结清的都有。迁移前的调用（`created_at` 早于版本 3 的迁移时间）推不出发送的时间，不要求有 |
| `resolution` | `SETTLED`、`CANCELLED`、`CACHED` 的审计三列（谁、凭什么、什么时候）都有，而且是合法的组合（见下一节）；其余状态三列都是空的 |
| `idem_call` | 幂等记录指向的调用属于同一个账户，而且那次调用记着的键就是这个键 |
| `idem_snapshot` | 有快照的记录：调用是 `SETTLED` 或 `USAGE_UNKNOWN`，状态码是 2xx 且等于调用的 `http_status`，正文长度对得上（不超过 8 MiB 的存了正文，超过的只记长度）；结算了的流只记状态码、其余三列都是空的（流式的标记，见[幂等](api.md#幂等idempotency-key)），这种调用必须是 `SETTLED`；没有快照的记录，快照的几列都是空的 |
| `idem_settled_snapshot` | 指向 `SETTLED` 调用的记录有快照，或者标明正文超过 8 MiB，或者是流式的标记；只有凭证据由 `reconcile` 结算的调用可以没有（QF 没拿到或没存下响应） |
| `outbox` | 用量事件的每一行 outbox 指向一个已结清（`SETTLED`、`CANCELLED`、`CACHED`）的调用，抄过去的几列（账户、用途、别名、上游、上游模型名、状态、用量、结清时间、`cached_from`）和调用逐列相同；一个调用最多一行（唯一键）。见[用量事件](#用量事件) |
| `outbox_complete` | 开关打开过时，打开之后、最近 7 天里结清的调用都有一行 outbox。窗口按数据库的时间算：清理只删发出超过 7 天的行，而发出的时间不早于结清的时间，窗口里的行一定还在；打开之前结清的调用不要求有。开关没打开过就不查 |

校验在一个只读、可重复读的事务里做，读的是同一个快照，服务运行中也能跑。

**限制**：不按周期重置额度；没有证据的 `USAGE_UNKNOWN` 一直占着预留（见下一节的“真实上游的限制”），没有手工核销；没有 Key 的本地缓存；不按钱计费、不按模型加权。`qfctl usage` 的最后一列是 `CACHED` 的调用数（它不占额度，所以不在已消费里；加在最后，前面各列的位置不变）。

## 调用状态、回收和对账（未知用量）

**调用状态**（状态图见[架构说明](architecture.md#调用的状态)）：

| 状态 | 含义 | 额度 |
|---|---|---|
| `RESERVED` | 预留成功，还没落发送意图 | 占着预留 |
| `IN_FLIGHT` | 发送意图已落库，请求可能已经发给上游 | 占着预留 |
| `SETTLED` | 有可信的用量，按实际用量结算 | 释放预留，已消费加上 usage 的 total |
| `CANCELLED` | 确定上游没有消费 | 释放预留 |
| `USAGE_UNKNOWN` | 可能已经被上游执行并计费，用量不知道 | 继续占着预留，不补 0 |
| `CACHED` | 嵌入缓存命中：给的是原来那次（`cached_from`）结算过的响应，没调上游。写入时就已结清 | 不预留、不动额度；用量照抄原来那次，只作记录 |

**允许的转换**（其余一律不允许，`SETTLED`、`CANCELLED`、`CACHED` 是终态；转换表在 `internal/ledger/recovery.go` 里只写一份。`CACHED` 不在表里：它写入时就已结清，收尾、回收、对账都没有从它出发或者转到它的转换）：

| 从 | 到 | 谁做 | 依据 |
|---|---|---|---|
| `RESERVED` | `IN_FLIGHT` | 网关 | 发请求之前，发送意图先提交 |
| `RESERVED` | `CANCELLED` | 网关 | 发出之前失败，或者客户端已经断开 |
| `RESERVED` | `CANCELLED` | 回收 | 超过上界还没落发送意图：确定没发出 |
| `IN_FLIGHT` | `SETTLED` | 网关 | 2xx 且 usage 合法 |
| `IN_FLIGHT` | `CANCELLED` | 网关 | 确定没生成结果：连接没建立、上游 4xx |
| `IN_FLIGHT` | `USAGE_UNKNOWN` | 网关 | 其余：超时、5xx、3xx、发出后连接断开、正文不能用、2xx 缺 usage |
| `IN_FLIGHT` | `USAGE_UNKNOWN` | 回收 | 超过上界还在 `IN_FLIGHT`：进程崩溃、收尾失败 |
| `USAGE_UNKNOWN` | `SETTLED` 或 `CANCELLED` | 网关（迟到的结果）或 `qfctl reconcile` | 证据（见下） |

- 每次转换都在一个事务里：先锁账户的额度行，再按主键锁住调用这一行、重新读它现在的状态，按转换表判断，再按主键做带状态条件的更新。多个实例同时回收、回收和收尾或对账同时碰到同一个调用，后拿到锁的看到的已经是新的状态，每个调用只转换一次。账本 `(call_id, kind)` 唯一是最后一道防线。
- **审计**：结清（变成 `SETTLED` 或 `CANCELLED`）的那一次转换在 `calls` 里写 `resolved_by`（`gateway`、`reclaim`、`reconcile`）、`resolution_evidence`、`resolved_at`。合法的组合：网关结算凭 `response`、`late_response`，网关释放凭 `response`、`not_sent`、`late_response`，缓存命中由网关凭 `cache` 记下；回收只释放，凭 `not_sent`；`reconcile` 凭 `call_log`、`bill`。被标成未知是谁做的看 `error_code`：网关记自己返回的错误码，回收记 `reclaimed_in_flight`（回收释放的记 `reclaimed_before_send`）。`finished_at` 仍是网关收尾的时间。

**上游结果怎么收尾**（网关，日志的 `outcome` 就是这一列）：

| 上游的结果 | 状态 | 证据 | 账本 |
|---|---|---|---|
| 发出前客户端已断开、上游请求建不起来、发送意图没落成 | `CANCELLED` | `not_sent` | release：已预留 −预留量 |
| 连接根本没建立（`upstream_unavailable`，拨号失败），也没有能换的目标 | `CANCELLED` | `not_sent` | release |
| 拨号失败、换到了别的目标：按最后那个目标的结果收尾（本表的其它行），账本的上游是最后那个 | — | — | — |
| 上游 4xx（含 401、403、429）：上游明确拒绝，没有生成结果 | `CANCELLED` | `response` | release |
| 2xx，usage 合法 | `SETTLED` | `response` | settle：已预留 −预留量，已消费 +`usage.total_tokens`；可以超过预留量 |
| 2xx 但缺 usage 或 usage 不合法；正文不能用；上游 5xx；超时；发出后连接断开 | `USAGE_UNKNOWN` | — | 不动，继续占着预留（不补 0） |
| 上游 3xx（QF 不跟随重定向，`upstream_invalid_response`） | `USAGE_UNKNOWN` | — | 不动 |

- **流式**（`stream: true`）：上游回 2xx 之前的结局和上表一样。回了 2xx、流开始之后，QF 读完上游的流才收尾，不在流结束之前收尾；客户端收到 `[DONE]` 时账本可能还没记好（见[流式](api.md#流式)）：

  | 流怎么结束的 | 状态 | 证据 | 账本 |
  |---|---|---|---|
  | 见到 `[DONE]`、没有错误事件，流里有合法的 usage（取最后一个） | `SETTLED` | `response` | settle |
  | 见到 `[DONE]`，但没有 usage 或 usage 不合法 | `USAGE_UNKNOWN` | — | 不动（不补 0） |
  | 截断：超时、上游连接断了、没有 `[DONE]` 就结束；事件超大或不是 JSON；上游发了错误事件 | `USAGE_UNKNOWN` | — | 不动 |

  - 截断的流里即使已经出现过 usage，也记未知：没有 `[DONE]` 就不能确定流完整。
  - `http_status` 是已经写给客户端的 200，`error_code` 是流截断时补给客户端的错误事件里的那个（`upstream_timeout`、`upstream_error`、`upstream_invalid_response`）。
  - 客户端中途断开不影响结局：QF 接着读完上游的流（有界结算）。
- 3xx 记未知，不当成拒绝：3xx 不保证请求没执行（`303 See Other` 的标准用法就是“POST 已经处理完，结果去别处取”）；错判成释放会让幂等键空出来、重新执行，可能计费两次，错判成未知只是多占着预留、有账单就能结清。
- usage 合法：三个数都不为负；chat 必须有 `completion_tokens`，嵌入可以没有。上游的响应和对账的证据用同一条规则（`ledger.Usage.Valid`）。
- **收尾失败**（账本出错，或者超过 2 秒，包括 COMMIT 结果不确定）：不改已经拿到的上游结果，2xx 照样返回；调用留在 `IN_FLIGHT`，占着预留，由回收标成未知；日志里记下用量、`outcome` 和 `ledger_error`，`qfctl reconcile -log` 能凭它结清。方向是多占、不会少扣。

**发送意图**：请求发给上游之前，先把调用从 `RESERVED` 改成 `IN_FLIGHT`（一条带条件的 UPDATE，自动提交，记下 `sent_at`）。提交成功之后才发第一个字节。

- 改到 0 行（超过上界、已被回收释放）或者写失败：不发，503 `ledger_unavailable`，不收尾，调用留给回收。
- 写失败但库里其实已经提交（COMMIT 时超时）：留下的是从没发出的 `IN_FLIGHT`。回收会把它标成未知；日志的 `outcome` 是 `CANCELLED`，`qfctl reconcile -log` 能凭它释放。
- 不锁额度行（见上面[额度和账本](#额度和账本)的“锁的顺序”）。在测试库上实测 p50 约 2 毫秒。

**有界结算**：请求一旦发出，上游调用就不再跟着客户端取消，只受上游超时限制。流式时就是接着读完上游的流（只是不再写给客户端），同样受 chat 的上游超时限制。客户端中途断开了，QF 照样等到上游回答或超时，照常收尾：该结算的结算，带幂等键、拿到 2xx 的存快照，只是不写回（日志没有 `status`，有 `client_disconnected: true`）。客户端带同一个幂等键重试就能拿到重放。发出之前客户端就断开的，不发、释放，和以前一样。

**回收**：每个 QF 实例在后台定期扫描，时间一律用数据库的（`UTC_TIMESTAMP(6)`）：

- 从预留（`created_at`）算起超过上界还在 `RESERVED` 的：确定没发出，释放（`CANCELLED`）；
- 超过上界还在 `IN_FLIGHT` 的：可能已经发出，标成 `USAGE_UNKNOWN`，继续占着预留。
- **上界** = `max(chat 超时, embeddings 超时) + 3 × 2 秒 + 5 秒`，默认 25 + 6 + 5 = **36 秒**（`config.ReclaimAfter`），不另设配置项。一次活着的调用停在这两个状态的最长时间是：预留事务剩下的部分（`created_at` 在事务里写）、发送意图、上游超时（从拨号管到读完正文）、收尾，三次账本操作各最多 2 秒；5 秒余量留给 CPU、GC 停顿和解析正文。不需要实例心跳：每个实例都强制上游超时。
  - 流式也在这个上界之内：整个流受 chat 的上游超时管（从发出上游请求到读完流），读在这个超时的到期时间截断；写给客户端（转发的事件和补的错误事件）的期限不越过到期时间加 1 秒，多出的 1 秒在 5 秒余量之内。所以活着的流式调用不会被回收，优雅退出的上限（等于上界）也等得完它。
- **后台循环**：QF 启动时立即跑一轮，之后每 上界/4（默认 9 秒）一轮；每轮先回收，再清理过期的幂等记录。候选用不加锁的读取出，每个调用一个事务（锁顺序同上），不用按索引范围的批量 UPDATE。一个调用（或一个账户的清理事务）出错只算它自己：记下来，接着处理下一个，不挡住别的账户；整轮结束时有出错，就记一行 `msg: "background"` 的日志，带 `reclaim_error` 或 `purge_error`（出错的个数和第一个错误），下一轮再试。候选按 `(created_at, id)` 一批一批地取（清理按 `(expires_at, account_id, idem_key)`，只取这一轮开始时已经过期的，见[幂等](api.md#幂等idempotency-key)的“清理”），下一批接在上一批最后一个之后，所以一轮里每个候选最多处理一次，一定结束；一个账户的额度行一直被锁住时，一轮在它的每个候选（清理是每一批）上各等 2 秒。**一轮清理最多用一个间隔**（和后台循环的间隔相同，默认 9 秒）：到点就在两个账户的事务之间停下（正在做的那个事务做完），这不是错误，不记 `purge_error`，`purged` 照常记删掉的条数；没删完的下一轮从头取、接着删（取的是还没删的）。到点之前已经有事务出错的，照旧报出错的个数和第一个错误。回收不受影响，每轮都先跑：QF 停了一阵子再起来、积压了几十万条过期的键时，卡住的预留也不会被一轮很长的清理饿住。
- **连接池的等待**（`msg: "db_pool"`）：每轮末尾比较一次账本连接池的统计（`DB.Stats()`）：上一轮以来有请求等过连接（池打满了），就记一行，字段是这期间新增的等待次数 `waits`、等待时长 `wait_ms`，和现在的 `in_use`、`open`、`max_open`；没等过不记。压测用它判断连接池是不是瓶颈（见[压测](scaling.md#压测)）。开着指标时另有 `qf_db_*`（见[指标](observability.md#指标)），两者并存：没部署 Prometheus 时这一行也看得到。
- **两个实例**：每个实例都跑自己的后台循环，同时扫同一个库。回收锁住之后按读到的状态转换，已经被另一个实例转换过的不动；清理删除时再判断一次过期。所以每个调用只转换一次、每条记录只删一次，没有心跳也能接手另一个实例留下的调用（见[两个实例](scaling.md#两个实例)）。
- **优雅退出**（SIGINT、SIGTERM）的上限也是这个上界：进行中的调用都能完整收尾；后台循环在两个事务之间停下；两者都结束才关账本。真被截断的（`kill -9`、超过上限），调用留在 `IN_FLIGHT`，重启后由回收接手。正确性不依赖优雅退出。

**证据**只有三种：

1. **迟到的结果**：QF 自己拿到了结果，只是没来得及记下。
   - 进程内：回收把调用标成未知之后，原来的请求才收尾（实例卡顿超过上界）。收尾照常把自己的结果记上（结算或释放，证据 `late_response`，写回了 2xx 的照常存快照）；它自己的结果也是未知时不变。
   - QF 的调用日志：日志里这次调用的 `outcome` 是确定的（`SETTLED` 带用量，或 `CANCELLED`），账本里的收尾却没写成（`ledger_error`）。交给 `qfctl reconcile -log`。
2. **确定没发出**：回收释放超过上界的 `RESERVED`；网关在发出前失败、拨号失败时释放。
3. **导入的账单**：上游按调用给的用量，用 QF 发给上游的调用号（`X-Client-Request-Id`）对上，交给 `qfctl reconcile -bill`。账单的一行也可以明确写“没有消费”。

不算证据：账单里没有这个调用；时间过去了多久；估算值；客户端自己的记录。没有证据的调用一直是 `USAGE_UNKNOWN`、占着预留。没有手工核销。QF 返回过 2xx 的调用，遇到“没有消费”的证据判为不一致、不释放（`CANCELLED` 的意思是确定没有消费，可上游明明生成了结果；释放还会让幂等键空出来重新执行），所以会一直是 `USAGE_UNKNOWN`。

**账单的格式**（QF 自己的 CSV；QF 的假上游 `GET /_fake/bill` 直接导出这种格式）：

```
call_id,consumed,prompt_tokens,completion_tokens,total_tokens
qf-ABCDEFGHIJKLMNOPQRSTUVWXYZ,yes,120,10,130
qf-BCDEFGHIJKLMNOPQRSTUVWXYZA,yes,64,,64
qf-CDEFGHIJKLMNOPQRSTUVWXYZAB,no,,,
```

表头固定；`call_id` 是 QF 的调用号（`qf-` 加 26 个 base32 字符）；`yes` 要有非负的 `prompt_tokens`、`total_tokens`（嵌入可以没有 `completion_tokens`，chat 必须有，否则判不一致）；`no` 的三列都空。任何一行不对、同一个调用号出现两次，整份文件都不用。

**日志当证据**：直接读 QF 的日志（`docker compose logs --no-log-prefix quotaforge` 的原样输出，每行一个 JSON），不用改格式。只采用有 `outcome` 的调用行：`SETTLED` 带用量的凭日志里的用量结算，`CANCELLED` 的释放；`USAGE_UNKNOWN` 的不是证据，只计数；别的行（启动、查询、鉴权失败、重放、缓存命中的 `CACHED`）跳过。

**`qfctl reconcile`**：

```bash
# 在项目根目录下运行
# 1. 列出用量未知、还占着预留的调用，去向对应的上游要这些调用的账单
docker compose run --rm -T --no-deps qfctl unknown -account local
# 2. 先预演：逐条打印打算怎么做和汇总，库里什么都不改
docker compose run --rm -T --no-deps qfctl reconcile -bill - < bill.csv
# 3. 确认之后执行；再执行一遍应当是 nothing to change
docker compose run --rm -T --no-deps qfctl reconcile -bill - -apply < bill.csv
# 用 QF 的日志当证据
docker compose logs --no-log-prefix quotaforge > qf.log
docker compose run --rm -T --no-deps qfctl reconcile -log - < qf.log
```

- 证据从 `-bill` 或 `-log` 指定的文件读，`-` 是标准输入（`qfctl` 在容器里跑时用这个）。先把整份证据读完，格式不对就退出 2，不连库。
- 每条证据一行输出（制表符分隔）：动作、调用号、账户、处理前的状态、证据和原因。动作：预演是 `would-settle`、`would-release`，执行是 `settled`、`released`；两种都可能有 `consistent`、`INCONSISTENT`、`in-progress`、`not-found`、`FAILED`。最后一行是汇总（`dry run: …; nothing was changed`、`applied: …`，或者 `nothing to change: …`）。只有调用号、账户、状态和数字，没有正文和 Key。
- 只结清 `USAGE_UNKNOWN` 的调用，每个调用一个事务，锁住之后再判断一次；`CACHED` 的调用从没发给上游，任何证据都对不上它，记 `INCONSISTENT`、不改；已经是 `SETTLED`、`CANCELLED` 的只比对（和证据一致记 `consistent`，否则 `INCONSISTENT`），不改；还在 `RESERVED`、`IN_FLIGHT` 的跳过（`in-progress`，等回收先处理）；账本里没有的报 `not-found`。同样的证据执行第二遍时都是 `consistent`，什么都不改。
- 按证据结算只写状态、用量和审计列：当时 QF 返回的 `http_status`、`error_code` 和快照保留。
- **退出码**：0 没有要人看的东西（每一条都是结清、将要结清或一致）；1 有不一致、进行中、账本里没有的，或某个调用的事务失败（其它的照常处理）；2 用法错误、证据文件读不了或格式不对。预演和执行用同一套退出码。

**调用状态查询** `GET /v1/calls/{id}`：

- `{id}` 是 QF 的调用号（响应头 `x-request-id`；重放的响应里是原来的调用号）。拿到带 `x-request-id` 的响应时（例如 504），可以按它查这次调用的结局。还没拿到响应头连接就断开的，拿不到调用号；用同一个 `Idempotency-Key` 重试来得到结局：
  - 还在处理：409 `idempotency_in_progress`；
  - 结算过、存了响应：重放原来的响应；
  - 其余：说明结局的 409（`idempotency_outcome_unknown`、`idempotency_response_unavailable`）；
  - 上一次确定没有消费的：重新执行。
- 鉴权和转发接口一样（Bearer Key，每次查库）。调用必须属于这把 Key 的账户：属于别的账户、不存在、调用号格式不对，都是同一个 404 `call_not_found`，不透露调用是否存在。账本不可用时 503 `ledger_unavailable`。不预留额度、不调上游、不算一次模型调用。
- 200 的正文（时间是 RFC 3339、UTC、微秒；库里没有的写 null）：

  ```json
  {"id":"qf-…","object":"call","endpoint":"chat","model":"qs-fake-upstream-v1","status":"SETTLED",
   "created_at":"2026-10-08T01:02:03.123456Z","sent_at":"…","finished_at":"…",
   "usage":{"prompt_tokens":120,"completion_tokens":10,"total_tokens":130},
   "http_status":200,"error_code":null,
   "resolution":{"by":"gateway","evidence":"response","at":"…"}}
  ```

  用量未知时 `usage` 是 null；嵌入没有 `completion_tokens`；没结清的 `resolution` 是 null。`http_status`、`error_code` 是 QF 当时返回的（回收标成未知的，`error_code` 是 `reclaimed_in_flight`）。正文里没有请求体、响应体、快照、幂等键的值和 Key。

**真实上游的限制**：百炼不提供按调用的账单，也不知道会不会记下 `X-Client-Request-Id`。超时、连接断开的调用拿不到上游的请求号，就没有证据，会一直是 `USAGE_UNKNOWN`、占着预留。M1 的真实冒烟里就有这样一个样本：一次聊天在百炼那边等了 12.0 秒，QF 返回 504，预留的 17,291 tokens 没有结清。百炼大概率已经执行并计费，但 QF 拿不到任何按调用的凭据。



### 换上游

别名有多个目标时，一次调用最多试每个上游一次，但只在确定没发出时才换：连接没建立（拨号失败）。判“确定没发出”（结局 `CANCELLED`、`not_sent`）和判“能不能换上游”用同一条规则，所以换走的上游一定是没收到请求的那个，一个调用号最多到达一个可能计费的上游。超时、5xx、发出后断开、3xx、4xx、429、流已经开始，都不换，照上表收尾。

- **发送意图只落一次**：调用从第一次尝试起一直是 `IN_FLIGHT`，`sent_at` 是第一次尝试的时间。
- **改道**：换之前，新上游先过上游层的准入（见[限流](rate-limiting.md#多个上游)）；然后一条自动提交的条件更新，把 `calls.upstream`、`upstream_model` 改成新目标，条件是调用还是 `IN_FLIGHT`、上游还是原来那个。改成了才发给新上游。
  - 改道不是状态转换：状态不变，不碰额度和账本行，不在转换表里；`qfctl verify` 也不看上游两列。
  - 影响 0 行（调用已经被回收标成未知）或者出错：不发，调用照上一次尝试的结果收尾（拨号失败，释放；已经被回收标成未知的，按迟到的结果释放）。
    - 出错（例如期限到了）时，更新可能其实已经提交：调用照样按拨号失败收成 `CANCELLED`，账本里的上游是新的那个，日志的 `upstream` 是原来那个；额度和状态都对。和上面发送意图的“写失败但库里其实已经提交”是同一类。
  - 改道和回收同时碰到同一个调用：两边都要拿这一行的行锁，先到先得。改道先提交，回收把它标成未知、上游是新的；回收先提交，改道影响 0 行、不发。
- **进程在中途死掉**（每一步都是单独提交的，库里留下的就是最后一次提交的样子）：
  - 改道之前：上游是原来那个，回收把它标成未知；那个上游没收到请求，账单里没有它，保持未知（和“发送意图之后、发出之前”被杀一样）。
  - 改道之后、发给新上游之前：上游是新的那个，回收把它标成未知。偏保守：新上游其实没收到；`qfctl unknown` 列出的是新上游，向它要账单，账单里没有它，保持未知。
- **期限**：所有尝试共用一个期限，从第一次尝试算起，等于用途的超时；尝试之间的准入、改道也在这个期限之内。所以一次调用停在 `IN_FLIGHT` 的最长时间不变，回收的上界和优雅退出的上限都不变。
- 账本里只记最后那个上游；之前试过、确定没收到请求的上游在调用日志的 `failover_from` 里（见[可观测](observability.md#多个上游和熔断)）。

### 数据库和迁移

- MySQL 8.4。表：`accounts`、`budgets`（每个账户一行：上限、已预留、已消费）、`api_keys`（Key 的 SHA-256、前缀、允许的别名、状态）、`calls`（每次预留成功的调用一行，带键的调用记着它的幂等键）、`ledger`（追加式，`(call_id, kind)` 唯一）、`idempotency_keys`（幂等记录和响应快照，见[幂等](api.md#幂等idempotency-key)）、`schema_migrations`；[用量事件](#用量事件)的 `usage_events`（开关）、`outbox`，和消费者的 `usage_seen`、`usage_daily`。定义在 `internal/ledger/migrations/`，现在的版本是 6。
- 迁移只往前，SQL 文件打进程序；版本号是文件名前缀。重复运行什么都不做；两个进程同时迁移时用 MySQL 的命名锁串行（加锁、迁移、释放在同一个连接上）。
- 权限分开：迁移只由 `qfctl` 用能建表的账号做；QF 服务用只能增删改查的账号。
- QF 启动时检查数据库的版本，和程序要求的不一致（或者还没迁移、连不上）就拒绝启动，报错里说明原因，不带连接串。
- **版本 3** 给 `calls` 加了 `IN_FLIGHT` 状态（加在 ENUM 末尾，已有值的序号不变）、`sent_at`、审计三列和回收扫描的索引 `(status, created_at)`。迁移前的调用按已有列回填：
  - 旧的 `RESERVED` 可能已经发出（收尾失败的那种），改成 `IN_FLIGHT`，`sent_at` 取 `created_at`：回收会把它标成未知，不会当成没发出而释放；
  - 旧的 `SETTLED`、`CANCELLED` 记网关收尾，`resolved_at` 取 `finished_at`，证据按状态和错误码推（上游 4xx 的错误码是 `response`，其余释放是 `not_sent`）；
  - 推不出的 `sent_at` 留空，`verify` 不要求迁移前的调用有它。
- **升级步骤**：先 `docker compose stop quotaforge`，再 `docker compose up -d --build --wait`（`qfctl` 先迁移，QF 再启动）。原因：旧代码不认识 `IN_FLIGHT`，迁移之后它新建的 `RESERVED` 会直接发出，这些可能已经发出的调用会被新版本的回收当成没发出而释放。脚本每次都从空库起（`down -v`），不受影响。
- **版本 4** 给 Key 和账户加了限流限额（`rpm_limit`、`tpm_limit`），迁移前的都是 0（不限）。
- **版本 5** 给嵌入缓存加了 `CACHED` 状态、证据 `cache`（两个 ENUM 都加在末尾）和列 `cached_from`（原来那次的调用号，可为 NULL，不加外键）。只改元数据（`ALGORITHM=INSTANT`，不拷表），已有的行不变。
  - 不加外键：原来那次的调用不在了（例如数据库清空或者恢复了、缓存的 Redis 还留着），命中照常写入，由 `qfctl verify` 的 `cached` 规则报出来，而不是让命中变成 503。所以清空或恢复数据库时，要同时清空缓存的 Redis（见[嵌入缓存](operations.md#嵌入缓存)）。
  - **上线顺序**：可以不停服务。先用新版本的 `qfctl migrate`，再逐个把实例换成新版本。正在跑的旧版本照常工作：它不写 `CACHED`，回收只扫 `RESERVED`、`IN_FLIGHT`，版本只在启动时检查；但旧版本在新库上**重启会因为版本不对起不来**，所以迁移之后重启的实例都要换成新版本。新旧混跑时用新版本的 `qfctl`：旧版本的 `verify` 不认识 `CACHED`，会把它报成违规；旧版本的 `reconcile -log` 遇到 `CACHED` 的日志行会拒绝整份日志（偏安全，换新版本再跑）。
  - **元数据锁**：`ALTER TABLE` 只改元数据也要短暂拿这张表的独占元数据锁。碰上一个还没提交的事务正用着 `calls`，`ALTER` 就排队等它，排队期间后来的所有读写 `calls` 的语句也排在 `ALTER` 后面。QF 的事务都在 2 秒的账本上限内，所以最坏是几秒的停顿（这期间请求可能 503 `ledger_unavailable`）；`qfctl` 或者手工开着的长事务会让它等得更久。所以在低峰跑，或者给迁移用的连接串加上参数 `lock_wait_timeout=10`（驱动在连接上 `SET` 这个会话变量）：10 秒等不到锁就失败，过一会儿重跑（迁移可以重复执行）。

- **版本 6** 加了[用量事件](#用量事件)的四张表：开关 `usage_events`（一行，迁移后是关的）、`outbox`，消费者的 `usage_seen`、`usage_daily`。只新建表、不动已有的表，都是 `CREATE TABLE IF NOT EXISTS`，可以重跑。
  - **上线顺序**：和版本 5 一样，先用新版本的 `qfctl migrate`，再逐个把实例换成新版本；旧版本在新库上照常跑（它不写 outbox，开关关着时新版本也不写），但重启会因为版本不对起不来。要发事件时，最后给实例加上配置的 `usage_events` 段：第一个带它启动的实例打开开关。开关打开之后，**不要再用旧版本的 `qfctl` 结清调用**：旧的 `reconcile -apply` 不写事件，它结清的调用在对账里是 `LOST`，`qfctl verify` 报 `outbox_complete`（这一版之前的 `qfctl` 不查库的版本；之后的版本除了 `migrate`，库的版本不一致时都拒绝执行）。
  - 迁移只建表，没有表锁之外的等待；给 QS 用的部署迁移之后开关是关的，什么都不变（见下一节的“没打开时”）。

## 用量事件

调用结清时发一条事件，下游（计费、报表）订阅事件、按账户按天汇总，不直接读 QF 的库。投递是**至少一次**，靠消费者去重做到每个调用只算一次，不说“恰好一次”。

```
结清事务（同一个事务）              QF 的转发循环（每秒一轮）          消费者（单独的进程）
 calls 改成终态 ─┐                   GET_LOCK 拿到才发                 先写库、再提交位点
 账本行          ├─ 提交 ──> outbox ──> Kafka 主题（键 = 账户） ──> usage_seen（去重）+ usage_daily（汇总）
 outbox 一行  ───┘          发成功才标记已发出                         同一个事务
                                                                       qfctl usage-reconcile：汇总 ⇄ 账本
```

**事件**：每个调用进 `SETTLED`、`CANCELLED`、`CACHED` 时一条；`USAGE_UNKNOWN` 不是终态，不发，之后被结清（迟到的结果、`qfctl reconcile`）时再发。所以一个调用恰好一条事件，调用号就是事件的编号。消息是 JSON，键是账户 id（同一个账户的事件进同一个分区）：

```json
{"v":1,"call_id":"qf-…","account_id":12,"purpose":"chat","model_alias":"…","upstream":"…","upstream_model":"…",
 "status":"SETTLED","usage":{"prompt_tokens":120,"completion_tokens":10,"total_tokens":130},
 "resolved_at":"2026-10-17T01:02:03.123456Z","cached_from":null}
```

- 只有汇总和对账要用的字段：没有 Key、请求和响应正文、客户端给的请求号和 `run_id`。没有用量（取消的调用）时 `usage` 是 null；嵌入没有 `completion_tokens`；`cached_from` 只有缓存命中有。
- `v` 是版本：消费者只认 1，改字段的含义要换版本，主题名里也带着版本（`quotaforge.usage-events.v1`）。
- `resolved_at` 是结清的时间（UTC、微秒），按它的日期分天（见下面的“按天”）。

**outbox：和结清在同一个事务里写**。把调用改成终态的地方都走同一个函数：收尾、回收、对账（`qfctl reconcile -apply`）共用的状态转换，和缓存命中的写入（`CACHED` 因此改成了一个事务）。它在同一个事务里执行一条 `INSERT … SELECT`，内容从刚改好的调用行抄：事务回滚，这一行也没有；提交了，这一行一定在，内容和账本的那一行一致。请求路径上只多这一条语句，不碰消息队列：**消息队列挂了，请求照常**，积压留在 outbox 里。

**开关**：一行的表 `usage_events`，`enabled_at` 为空是关着。开关放在数据库里，不在配置里：结清调用的不只是 QF 实例，还有 `qfctl reconcile`，它们读同一个开关。

- 配置里写了 [`usage_events`](operations.md#配置文件) 的 QF 启动时打开它：先不加锁地读，已经开着就什么都不改、直接返回（`enabled_at` 不随重启变，也不去排开关行的锁，不会让别的实例的结清排队）；关着才锁住开关行再看一次，还是关的才改成开、记下数据库的时间。锁开关行时等锁超过 2 秒（1205：有结清的事务拿着它的共享锁不放），在启动的 10 秒里重试，都不行 QF 才以 1 退出；等锁的这几秒里，新的结清也排在它后面（每个最多等 2 秒），只在第一次打开时发生。**只开不关**，所以读到“开”就一直是开的。打开之后**至少要有一个实例保留配置里的 `usage_events` 段**：全都去掉的话，开关仍然开着，结清照样写 outbox，但没人转发、没人清理，表会静默增长；积压的两个指标照样导出，但没配的实例不查 outbox，值一直是 0，所以积压的告警（`QFUsageOutboxBacklog`）也不会响。
- **完整性**：靠写事件那条 `INSERT … SELECT` 末尾显式的 `FOR SHARE`，不靠隔离级别（结清事务用服务器默认的隔离级别，可能是可重复读，也可能是读已提交）。它读开关行时加共享锁、读最新提交的值；打开要先拿这一行的排他锁、再取时间。
  - 正在打开时结清：结清等打开提交，读到“开”，写事件。
  - 开关关着时结清：读到“关”时的共享锁也拿到提交（可重复读、读已提交的会话都是这样，MySQL 8.4 上看过），打开等这个结清提交之后才取时间，所以这个调用的 `resolved_at` 早于 `enabled_at`，不要求有事件。假设这把锁读完就放开，也不会丢：打开取时间在结清读到“关”之后，而结清的时间在这之前就写好了。
  - 所以 `enabled_at` 之后结清的调用一定有事件（打开的那一刻正在结清的，可能也有，不算错）。
- **没打开时**（例如给 QS 用的部署，配置里没有这一段）：每次结清多一条什么都不写的语句（和开关行上的一把共享锁，共享锁之间不冲突），没有转发、没有清理；`/metrics` 里多四个值一直是 0 的序列（`qf_usage_outbox_unsent`、`qf_usage_outbox_oldest_age_seconds`、`qf_usage_events_forwarded_total`、`qf_usage_event_delay_seconds`）；请求、响应、错误码、用量的口径都不变。

**转发**：配了 `usage_events` 的 QF 实例里一个后台循环，和回收、清理的循环分开，每秒一轮：

- 一轮先在一个专用连接上不等待地拿 MySQL 的命名锁 `quotaforge_outbox`，拿不到（另一个实例正在发）这一轮就不发：两个实例同一时刻只有一个在发。拿锁的连接断了，锁自动放开，下一轮另一个实例接手；某个实例连不上 Kafka 时，另一个实例照样能发。命名锁是整个 MySQL 服务器范围的、不分库：同一个服务器上的两套 QF（不同的库）会轮流转发，不出错，只是互相等一轮（迁移的锁 `quotaforge_migrate` 也是这样）。
- 不加锁地按插入的顺序读一批（100 条）没发出的行，同步发给 Kafka（确认级别 all），**发成功才**按主键标记已发出，读到空为止。读不加锁、标记按主键，不锁范围，不会挡住结清事务插新行（可重复读下按范围加锁会锁住间隙）。
- 不用 `SELECT … FOR UPDATE SKIP LOCKED` 分行抢：那要在事务里锁着这些行等 Kafka 的网络往返。
- 等 Kafka 有上限：一批发出 5 秒之后可以被取消，Kafka 的客户端在下一次元数据刷新（两次之间至少 5 秒）或连接出错时让它失败，这一轮出错、下一轮整批重发。实测 broker 停在两次发送之间、连接被拒时一轮约 6 到 7 秒返回；Kafka 在请求途中停掉、名字也解析不到（Compose 里停掉 `qf-kafka`）时，从停到这一轮报错约 9 到 10.5 秒。Kafka 的客户端默认不肯取消已经发出、还没回音的幂等请求（broker 在请求途中停掉时会一直等），这里打开了它的取消选项：代价是 Kafka 里可能多一份，由消费者去重。生产者幂等只让重复少一些，正确性不靠它。
- **至少一次**：发之前死了，下一轮发；发了、标记之前死了（或者标记失败、锁丢了），下一轮再发一遍，消费者去重；标记之后死了，不再发。
- 插入的顺序不是提交的顺序：晚提交的行下一批或下一轮照样读得到。汇总是加法，和顺序无关。
- 退出：转发只在两批之间停（手上的一批做完才停：最坏是信号正好在读这一批之前，读、标记、放锁各一次账本操作，加上按上面的实测最多约 10.5 秒的发送），和 HTTP 排空同时进行：信号一到两边一起开始，转发最多约 16.5 秒停下，远小于排空的上限，所以退出的上限（默认 36 秒）和 `stop_grace_period` 都不变；排空期间结清的调用照样写 outbox，由下次启动或另一个实例发出。
- **清理**：后台循环在清理幂等记录之后，删掉**发出**超过 7 天的 outbox 行（和 Kafka 主题的保留期一样）；没发出的永远不删。先不加锁地取候选，再按主键删。只有配了 `usage_events` 的实例做。

**消费者**：`quotaforge usage-consumer` 子命令，同一个程序、单独一个不带健康检查的镜像目标 `usage-consumer`，同一份配置（用其中的 `database` 和 `usage_events` 两段），单独一个进程；消费组 `quotaforge-usage-daily`，新的消费组从最早的消息读起。

- **先写库、再提交位点**：取一批，逐条写库，写成功的才提交。写库之后、提交之前死了，重启之后会再收到这几条，去重跳过。
- **去重和汇总在同一个事务里**（读已提交）：先插 `usage_seen`（调用号是主键），主键冲突就是处理过的重复，什么都不改；否则把这次调用加到 `usage_daily` 的（账户、日期、用途）那一行上。一个调用只会被算一次。
- `usage_seen` 不清理：每个调用一行，很小；清理了的话，比保留期还晚到的重复投递会被算两次。
- 写库出错时不跳过：等一秒再试同一条（跳过就丢了）。
- 格式不对的消息（版本不对、缺字段、不是终态、时间不是 UTC、用量和状态对不上、某一项用量超过 10^12）：用量的上限远大于任何真实的调用；超过它的事件能解码，加进汇总的同一行却可能让 BIGINT 越界，写库必然出错，消费者会一直重试它、卡住这个分区，所以也按格式不对处理。记日志（只有分区、位点和原因）、计数、跳过，和这一批一起提交。生产者只有 QF，格式不对是程序错误，不能让它卡住同一个分区后面的事件；它的调用在对账里会一直是“发了还没消费”。不做死信队列。
- 退出（SIGTERM）：做完手上的一条，提交处理完的位点，离开消费组。
- 真实部署里这两张表在下游自己的库里，不加指向账本的外键；这里放在同一个库里，好和账本对账。

**按天**：`usage_daily` 的键是（账户、UTC 日期、用途），值是结算的调用数和三项用量（就是这一天的已消费）、缓存命中的调用数和省下的 total、取消的调用数。日期按 `resolved_at`：每次结清都写它，回收、对账结清的也有（`finished_at` 只有网关收尾写）。事件里的时间从调用行抄，对账也按同一列分天，半夜前后的调用两边一定归到同一天。

**对账**：`qfctl usage-reconcile [-day YYYY-MM-DD] [-account NAME]` 拿消费者的汇总和账本比：

- 范围：不写 `-day` 时是从打开开关那天到今天（UTC）的每一天。账本那边是这些天里结清、`resolved_at` 不早于打开时间（或者已经有事件）的调用。在一个只读、可重复读的事务里查，两边是同一个快照。
- 每个（日期、账户、用途）一行，制表符分隔：`day account purpose ledger_calls consumer_calls ledger_tokens consumer_tokens unsent unconsumed result`。`tokens` 是结算的 total；`result` 是 `same`、`in-transit`（只差在路上的）或 `MISMATCH`。逐项比的是全部字段（三种状态的调用数、结算的三项用量、缓存命中省下的用量）。
- 之后每个对不上的调用一行：`unsent`（还在 outbox、没发出）、`unconsumed`（发出了、消费者还没记），这两种算**在路上**；`LOST`（结清了、没有 outbox 行、消费者也没有）；`MISFILED`（消费者记在了别的账户、日期或用途下）；`extra`（消费者记了、账本的范围里没有）。
- 最后一行是汇总：`all equal: N rows`；或者 `in transit: …`（在路上的条数和最老一条结清了多久）；或者 `MISMATCH: …`。
- 退出码：0 没有对不上的（在路上的不算）；1 有 `MISMATCH`（含 `LOST`、`MISFILED`、`extra`），或者开关没打开过；2 用法错误。都排空之后两边必须完全相同。
- 对账按天扫 `calls`，没有按 `resolved_at` 的索引（在大表上加索引要拷表，不是只改元数据），现在的数据量够用。

**积压和告警**：消息队列不可用时，请求照常 200，事件留在 outbox 里；转发每轮出错计一次 `qf_background_errors_total{task="usage_forward"}`，积压看 `qf_usage_outbox_unsent`、`qf_usage_outbox_oldest_age_seconds`，最老一条等了 5 分钟以上触发 `QFUsageOutboxBacklog`（见[可观测](observability.md#用量事件)）。恢复之后自动补发。

演示（bash，Linux、macOS 和 CI 用；约 2 分钟，构建镜像另算）：

```bash
# 在项目根目录下运行
scripts/compose-usage.sh   # Compose 项目名 qf-usage，叠加 compose.cache.yaml、compose.usage.yaml
```

- 一批请求（聊天结算、嵌入结算再命中缓存、上游 401 的取消、没有 usage 的未知、再用假上游的账单对账结清它）：汇总的各项对，按天的已消费等于账户的已消费，两边完全相同。
- 停掉 `qf-kafka`：请求照样 200，对账显示还没发（退出 0），QF 的指标看得到积压和转发的错误；起回来之后补发，完全相同。
- 停掉消费者：对账显示发了还没消费；起回来从提交过的位点接着读，没有重复。
- 用工具容器把消费组的位点重置到最早：全部重新读一遍，全是重复，汇总逐行不变。
- 最后 `qfctl verify`，日志、主题里的消息、消费者的表里没有 Key、请求正文、上游 Key 和数据库密码。

## 用量导出

`quotaforge usage-export` 从标准输入读 QF 的日志，向标准输出写 CSV，每次调用一行。账本（`qfctl usage`、`calls` 表）是额度的权威；导出是按调用核对用的。

- 列：`time, request_id, client_request_id, run_id, endpoint, model_alias, upstream_model, status, error_code, prompt_tokens, completion_tokens, total_tokens, duration_ms, replay_of, cached_from`。新列只加在最后，按列号读的脚本不受影响。
- 幂等重放的请求也有一行：`replay_of` 是原来的调用号，用量三列为空（没调上游），不算成新的用量；`request_id` 是这次重放自己的号，只出现在日志里（客户端收到的 `x-request-id` 是原来的调用号）。
- 嵌入缓存命中的请求也有一行：`cached_from` 是原来那次的调用号，用量三列为空（没调上游，不算成新的用量）；`request_id` 是这次 `CACHED` 调用自己的号，也就是客户端收到的 `x-request-id`。
- 用量三列：
  - 日志里是 `unknown` 的，导出里也是 `unknown`，不会变成 0；
  - 没调上游的留空（包括转发前客户端已断开的）；
  - 嵌入没有 `completion_tokens`，也留空。
- `status` 是写回客户端的状态；客户端已断开、什么都没写回时留空。
- 其它行：
  - 不是调用的日志行（例如启动日志）跳过；
  - 解析不了的行也跳过，但会计数，在标准错误里报出“skipped N unparseable lines”。
- **退出码**：0 每一行都解析了；1 有解析不了的行（能解析的行照常写完），或者读写出错。和 `qfctl idempotency purge` 部分成功时一样：先做完能做的，再以 1 退出。
- **防公式注入**：以 `=`、`+`、`-`、`@`、制表符、回车或单引号开头的单元格，前面加一个单引号 `'`，Excel 这类软件打开时只当文本，不当公式。别名、客户端请求号、`run_id`、上游的模型名都可能以这些字符开头；QF 自己的值（时间、调用号、接口、状态、错误码、用量、时长）不会，所以原样。还原：值以单引号开头时去掉一个就是原值（单引号开头的原值也加了一个，所以不会混淆）。

bash：

```bash
# 在项目根目录下运行
docker compose logs --no-log-prefix quotaforge | docker run --rm -i quotaforge:local usage-export > usage.csv
```

PowerShell：Windows PowerShell 5.1 的管道会重新编码原生命令的输出，交给 `cmd` 去接管道。

```powershell
cmd /c "docker compose logs --no-log-prefix quotaforge | docker run --rm -i quotaforge:local usage-export > usage.csv"
```

本机有 Go 时，也可以 `go run ./cmd/quotaforge usage-export < qf.log > usage.csv`。
