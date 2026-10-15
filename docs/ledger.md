# 额度、账本和未知用量

## 额度和账本

账本在 MySQL 里，是额度的唯一权威。单位是 token，每个账户一个上限，不按周期重置。

一次请求：鉴权 → 解析 → 模型检查 → 选目标（[多个上游](operations.md#多个上游)）→ **估算** → 准入（[限流](rate-limiting.md)）→ **预留** → **发送意图** → 转发（拨号失败时[换上游](#换上游)）→ **收尾** → 写回 → 修正限流的 TPM 桶（收尾在写回之前，客户端拿到响应时账本已经记好）。

- **估算**（保守的上界）：
  - 输入 = 转发给上游的请求体字节数（一个 token 一般至少对应一个字节，不用解析消息）。请求体是按第一次选中的目标的上游模型名改写过的（流式还加上 QF 替客户端要的 `include_usage`），所以别名有多个目标时，预留量会随选中的目标差几个字节；换上游时预留量不变，不重新预留，新上游的 TPM 也按这个量扣。结算按实际用量，差的这几个字节不影响已消费；
  - 输出 = chat 请求的 `max_tokens`，没写就用模型配置的 `default_max_tokens`；embeddings 为 0；
  - 预留量 = 输入 + 输出。估算值记进 `calls`，以后用来评估估算误差。
- **预留**（一个事务）：先对额度行做条件更新（`上限 − 已预留 − 已消费 ≥ 预留量` 才扣），再插调用记录（`RESERVED`）和账本的 reserve。不够就回滚、429。不先读后写，不用分布式锁。
- **发送意图**、**收尾**、回收和对账见下一节[调用状态、回收和对账](#调用状态回收和对账未知用量)。
- **超用**：实际用量超过预留时如实记账，已消费可以超过上限（余额为负），之后的预留都会被拒。
- **锁的顺序**：每个改额度的事务（预留、收尾、回收、对账、清理幂等记录）都先锁这个账户的额度行，再按主键动 `calls`、`ledger`、`idempotency_keys`；同一账户的这些事务在额度行上排队，不会互相等成死锁。不锁额度行的写只有发送意图和[换上游](#换上游)时的改道：它们只锁调用这一行，持锁时不等别的锁，构不成等待环。
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
| `reserve_entry` | 每个调用恰有一条 reserve，变动 = 它的预留量 |
| `settled` | `SETTLED`：有 settle、没有 release，预留变动合计 0，消费变动 = usage 合计 |
| `cancelled` | `CANCELLED`：有 release、没有 settle，变动合计都是 0 |
| `open` | `RESERVED`、`IN_FLIGHT`、`USAGE_UNKNOWN`：只有 reserve，继续占着自己的预留 |
| `sent` | 发送意图的时间 `sent_at`：`RESERVED` 和回收释放的（确定没发出）没有；`IN_FLIGHT`、`USAGE_UNKNOWN`、`SETTLED`，以及凭上游的回答、迟到的结果或由 `reconcile` 结清的都有。迁移前的调用（`created_at` 早于版本 3 的迁移时间）推不出发送的时间，不要求有 |
| `resolution` | `SETTLED`、`CANCELLED` 的审计三列（谁、凭什么、什么时候）都有，而且是合法的组合（见下一节）；其余状态三列都是空的 |
| `idem_call` | 幂等记录指向的调用属于同一个账户，而且那次调用记着的键就是这个键 |
| `idem_snapshot` | 有快照的记录：调用是 `SETTLED` 或 `USAGE_UNKNOWN`，状态码是 2xx 且等于调用的 `http_status`，正文长度对得上（不超过 8 MiB 的存了正文，超过的只记长度）；结算了的流只记状态码、其余三列都是空的（流式的标记，见[幂等](api.md#幂等idempotency-key)），这种调用必须是 `SETTLED`；没有快照的记录，快照的几列都是空的 |
| `idem_settled_snapshot` | 指向 `SETTLED` 调用的记录有快照，或者标明正文超过 8 MiB，或者是流式的标记；只有凭证据由 `reconcile` 结算的调用可以没有（QF 没拿到或没存下响应） |

校验在一个只读、可重复读的事务里做，读的是同一个快照，服务运行中也能跑。

**限制**：不按周期重置额度；没有证据的 `USAGE_UNKNOWN` 一直占着预留（见下一节的“真实上游的限制”），没有手工核销；没有 Key 的本地缓存；不按钱计费、不按模型加权。

## 调用状态、回收和对账（未知用量）

**调用状态**：

| 状态 | 含义 | 额度 |
|---|---|---|
| `RESERVED` | 预留成功，还没落发送意图 | 占着预留 |
| `IN_FLIGHT` | 发送意图已落库，请求可能已经发给上游 | 占着预留 |
| `SETTLED` | 有可信的用量，按实际用量结算 | 释放预留，已消费加上 usage 的 total |
| `CANCELLED` | 确定上游没有消费 | 释放预留 |
| `USAGE_UNKNOWN` | 可能已经被上游执行并计费，用量不知道 | 继续占着预留，不补 0 |

**允许的转换**（其余一律不允许，`SETTLED`、`CANCELLED` 是终态；转换表在 `internal/ledger/recovery.go` 里只写一份）：

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
- **审计**：结清（变成 `SETTLED` 或 `CANCELLED`）的那一次转换在 `calls` 里写 `resolved_by`（`gateway`、`reclaim`、`reconcile`）、`resolution_evidence`、`resolved_at`。合法的组合：网关结算凭 `response`、`late_response`，网关释放凭 `response`、`not_sent`、`late_response`；回收只释放，凭 `not_sent`；`reconcile` 凭 `call_log`、`bill`。被标成未知是谁做的看 `error_code`：网关记自己返回的错误码，回收记 `reclaimed_in_flight`（回收释放的记 `reclaimed_before_send`）。`finished_at` 仍是网关收尾的时间。

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
- **连接池的等待**（`msg: "db_pool"`）：每轮末尾比较一次账本连接池的统计（`DB.Stats()`）：上一轮以来有请求等过连接（池打满了），就记一行，字段是这期间新增的等待次数 `waits`、等待时长 `wait_ms`，和现在的 `in_use`、`open`、`max_open`；没等过不记。压测用它判断连接池是不是瓶颈（见[压测](scaling.md#压测)）。以后做指标时换成指标。
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

**日志当证据**：直接读 QF 的日志（`docker compose logs --no-log-prefix quotaforge` 的原样输出，每行一个 JSON），不用改格式。只采用有 `outcome` 的调用行：`SETTLED` 带用量的凭日志里的用量结算，`CANCELLED` 的释放；`USAGE_UNKNOWN` 的不是证据，只计数；别的行（启动、查询、鉴权失败、重放）跳过。

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
- 只结清 `USAGE_UNKNOWN` 的调用，每个调用一个事务，锁住之后再判断一次；已经是 `SETTLED`、`CANCELLED` 的只比对（和证据一致记 `consistent`，否则 `INCONSISTENT`），不改；还在 `RESERVED`、`IN_FLIGHT` 的跳过（`in-progress`，等回收先处理）；账本里没有的报 `not-found`。同样的证据执行第二遍时都是 `consistent`，什么都不改。
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

- MySQL 8.4。表：`accounts`、`budgets`（每个账户一行：上限、已预留、已消费）、`api_keys`（Key 的 SHA-256、前缀、允许的别名、状态）、`calls`（每次预留成功的调用一行，带键的调用记着它的幂等键）、`ledger`（追加式，`(call_id, kind)` 唯一）、`idempotency_keys`（幂等记录和响应快照，见[幂等](api.md#幂等idempotency-key)）、`schema_migrations`。定义在 `internal/ledger/migrations/`，现在的版本是 3。
- 迁移只往前，SQL 文件打进程序；版本号是文件名前缀。重复运行什么都不做；两个进程同时迁移时用 MySQL 的命名锁串行（加锁、迁移、释放在同一个连接上）。
- 权限分开：迁移只由 `qfctl` 用能建表的账号做；QF 服务用只能增删改查的账号。
- QF 启动时检查数据库的版本，和程序要求的不一致（或者还没迁移、连不上）就拒绝启动，报错里说明原因，不带连接串。
- **版本 3** 给 `calls` 加了 `IN_FLIGHT` 状态（加在 ENUM 末尾，已有值的序号不变）、`sent_at`、审计三列和回收扫描的索引 `(status, created_at)`。迁移前的调用按已有列回填：
  - 旧的 `RESERVED` 可能已经发出（收尾失败的那种），改成 `IN_FLIGHT`，`sent_at` 取 `created_at`：回收会把它标成未知，不会当成没发出而释放；
  - 旧的 `SETTLED`、`CANCELLED` 记网关收尾，`resolved_at` 取 `finished_at`，证据按状态和错误码推（上游 4xx 的错误码是 `response`，其余释放是 `not_sent`）；
  - 推不出的 `sent_at` 留空，`verify` 不要求迁移前的调用有它。
- **升级步骤**：先 `docker compose stop quotaforge`，再 `docker compose up -d --build --wait`（`qfctl` 先迁移，QF 再启动）。原因：旧代码不认识 `IN_FLIGHT`，迁移之后它新建的 `RESERVED` 会直接发出，这些可能已经发出的调用会被新版本的回收当成没发出而释放。脚本每次都从空库起（`down -v`），不受影响。

## 用量导出

`quotaforge usage-export` 从标准输入读 QF 的日志，向标准输出写 CSV，每次调用一行。账本（`qfctl usage`、`calls` 表）是额度的权威；导出是按调用核对用的。

- 列：`time, request_id, client_request_id, run_id, endpoint, model_alias, upstream_model, status, error_code, prompt_tokens, completion_tokens, total_tokens, duration_ms, replay_of`。
- 幂等重放的请求也有一行：`replay_of` 是原来的调用号，用量三列为空（没调上游），不算成新的用量；`request_id` 是这次重放自己的号，只出现在日志里（客户端收到的 `x-request-id` 是原来的调用号）。
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
