# 能力证据清单

本文把 QuotaForge 声称的每一项能力，对到实现它的代码、守住它的测试、你自己可以运行的命令，以及记录过的运行结果。代码位置都相对于仓库根目录，写的是符号名。结构见[架构说明](architecture.md)。

## 怎么读这份清单

- **Fake / Real。** Fake 是 QF 自己的假上游（`internal/fakeupstream`）或 QS 的假上游：不需要密钥，不花钱，结果每次相同，测试、演示脚本和 CI 都用它。Real 是真实上游（阿里云百炼的 `qwen-plus`、`text-embedding-v4`），会花钱，只在本机（Windows）运行过，CI 不跑。
- **测试命令**在仓库根目录下运行。带数据库的测试要先设好测试库、Redis、Kafka 的地址（见[开发和测试](development.md)）；没设时这些测试跳过，其余照跑。CI 设了 `QUOTAFORGE_TEST_REQUIRE_DB=1`，缺了就失败，所以 CI 里全部实际运行。
- **演示脚本**（`scripts/compose-*.sh`）要 Docker（带 Compose）、curl、jq，每个都从空环境起，跑完 `down -v`。“项数”是脚本里检查的条数，任何一项失败脚本就以非 0 退出。
- **记录的结果**写日期和公开仓库的提交号。“公开 CI”指本仓库在 GitHub Actions（Linux）上的两个任务：`test`（`go vet`、staticcheck、核对脚本的单元测试、带 MySQL、Redis、Kafka 的全量 `go test -race`）和 Compose 的演示（八个演示脚本，加上和 QS 的联调）。
- **独立复核。** 每次改动合并之前，由没参与实现的一方（复核方）重跑全量测试和演示脚本，自己写探针，做变异：把被测的代码改坏，对应的测试必须失败。这些探针和记录不在公开仓库里；它们抓出的问题和修法，见[问题复盘](retrospective.md)。

## 能力证据表

| 能力 | 代码 | 测试 | 自己怎么看 | 记录的结果 |
|---|---|---|---|---|
| 转发保持原样：非流式只把 `model` 换成上游模型名，tools、tool_calls、usage 和其它字段的字节不改；流式另外让上游带上 usage（例外见[流式](api.md#流式)）；上游的 2xx 响应原样返回 | `internal/gateway/body.go` 的 `parseRequest`、`upstreamBody`；`internal/gateway/upstream.go` 的 `upstreamRequest` | `TestForwardIsByteExact`、`TestToolCallsPassThrough`、`TestMissingFieldsPassThrough`、`TestUpstreamHeadersAreNotForwarded`（`internal/gateway`） | `go test -race -run 'TestForwardIsByteExact\|TestToolCallsPassThrough\|TestMissingFieldsPassThrough\|TestUpstreamHeadersAreNotForwarded' ./internal/gateway/`；`scripts/compose-smoke.sh` | 公开 CI 通过（2026-10-19，`a56cd4b`，Fake）；真实上游上经 QS 的演示都通过（见下文“真实上游”） |
| 模型白名单不能被键名的变体绕过（`MODEL`、重复的键） | `internal/gateway/body.go` 的 `parseRequest` | `TestParseRequestRejects`、`TestCaseVariantCannotBypassModelAllowList` | `go test -race -run 'TestParseRequestRejects\|TestCaseVariant' ./internal/gateway/` | 公开 CI 通过（2026-10-19，`a56cd4b`） |
| 额度账本：并发预留不超扣，账本的不变量随时可校验 | `internal/ledger/store.go` 的 `Reserve`、`reserveIn`、`Finish`；`internal/ledger/verify.go` 的 `Verify` | `TestConcurrentReservations`、`TestRandomCalls`（`internal/gateway`）；`TestReserveIsConditional`、`TestVerifyFindsTampering`（`internal/ledger`） | `go test -race -run 'TestConcurrentReservations\|TestRandomCalls' ./internal/gateway/`；`go test -race -run 'TestReserveIsConditional\|TestVerifyFindsTampering' ./internal/ledger/`；`qfctl verify` | 每个演示脚本最后都跑 `qfctl verify`；两个实例同时发 50 个、额度只够 10 个时，成功的不超过 10 个（`scripts/compose-two-instances.sh` 第 1 项） |
| 幂等：同一个 `Idempotency-Key` 的重试不多扣；结算过的原样重放，结果未知的不重新执行 | `internal/gateway/idempotency.go` 的 `fingerprint`、`canonicalJSON`；`internal/ledger/idempotency.go` 的 `reserveWithKey`、`record.decide` | `TestIdempotentReplay`、`TestIdempotentOutcomeUnknown`、`TestIdempotentConcurrentSameKey`、`TestIdempotentConcurrentAccounts` | `go test -race -run TestIdempotent ./internal/gateway/` | 两个实例同一个键同时发 20 个，假上游的账单里只有 1 条（`scripts/compose-two-instances.sh` 第 2 项）；真实上游上 `-Check` 的重放：`x-request-id` 和正文相同，已消费不变（2026-10-15、10-19） |
| 未知用量的恢复和对账：先记发送意图；超时、崩溃记成未知、不补 0；后台回收；凭证据结清 | `internal/gateway/quota.go` 的 `markSent`；`internal/ledger/recovery.go` 的 `transitions`、`Reclaim`；`internal/ledger/reconcile.go` 的 `Reconcile`；`cmd/qfctl` 的 `reconcile` | `TestWindowSendIntentFails` 等几个 `TestWindow`、`TestKillPointsSimulated`（`internal/gateway`）；`TestKillWhileUpstreamHasTheRequest`（`cmd/quotaforge`，真的杀掉 QF 的子进程）；`TestTransitionsNotAllowed`、`TestReconcile`（`internal/ledger`） | `go test -race -run 'TestWindow\|TestKillPoints' ./internal/gateway/`；`go test -race -run TestKillWhileUpstreamHasTheRequest ./cmd/quotaforge/`；`go test -race -run 'TestTransitionsNotAllowed\|TestReconcile' ./internal/ledger/`；`scripts/compose-smoke.sh` 的“真杀进程”一项 | 冒烟脚本：`kill -9` 之后回收把调用标成未知，凭假上游的账单结清，再执行一遍什么都不改；联调第 4、5 个场景（上游超时、杀掉一个实例由另一个接手） |
| 共享限流：Key、账户、上游三层，Lua 脚本全有或全无，多个实例共用 | `internal/ratelimit/ratelimit.go` 的 `Admit`、`Adjust`，`internal/ratelimit/bucket.lua`；`internal/gateway/ratelimit.go` 的 `admit`、`adjust` | `TestAllOrNothing`、`TestConcurrentAdmission`、`TestRedisHangs`（`internal/ratelimit`）；`TestAdmissionBeforeReservation`、`TestTPMRefundWithRedisAndMySQL`、`TestRedisHangsGives503`（`internal/gateway`） | `go test -race ./internal/ratelimit/`；`go test -race -run 'TestAdmissionBeforeReservation\|TestTPMRefundWithRedisAndMySQL\|TestRedisHangsGives503' ./internal/gateway/` | 两个实例共用 RPM 20 的桶，同时发 60 个，放行数在理论上界之内（`scripts/compose-two-instances.sh` 第 3 项）；冒烟脚本里 Redis 停了是 503、恢复后第一个请求就是 200 |
| 多个实例：没有进程内的共享状态，同时跑不超扣、不重复执行同一个键、不重复回收 | 每个请求都查库；限流只在 Redis 的脚本里算；回收、转发在锁住之后判断 | `TestConcurrentReclaims`、`TestForwardTwoInstances`（`internal/ledger`） | `go test -race -run 'TestConcurrentReclaims\|TestForwardTwoInstances' ./internal/ledger/`；`scripts/compose-two-instances.sh` | 53 项，公开 CI 通过（2026-10-19，`a56cd4b`）；压测见下文 |
| 嵌入缓存：命中不调上游、不扣额度，记一条 `CACHED`；同一个实例里相同请求只回源一次 | `internal/gateway/cache.go` 的 `lookupCache`、`serveCached`、`storeCache`；`internal/gateway/flight.go` 的 `flights`；`internal/embcache` | `TestEmbeddingCacheHit`、`TestEmbeddingCacheCoalesces`、`TestEmbeddingCacheStuckLeader`、`TestEmbeddingCacheCollapsedFollowersFreeUpstreamRPM`（`internal/gateway`）；`TestVerifyFindsCachedTampering`（`internal/ledger`） | `go test -race -run TestEmbeddingCache ./internal/gateway/`；`go test -race -run TestVerifyFindsCachedTampering ./internal/ledger/`；`scripts/compose-cache.sh` | 34 项，公开 CI 通过；真实上游 2026-10-19：第二遍演示建知识索引的 4 次嵌入全部命中 |
| 用量事件：结清时同一个事务写 outbox，转发到 Kafka（至少一次），消费者去重、按天汇总，和账本对账 | `internal/ledger/outbox.go` 的 `writeEvent`、`ForwardOutbox`；`internal/ledger/usage.go` 的 `ApplyUsageEvent`；`internal/usageevents` 的 `Forward`、`Consume` | `TestEventInSameTransaction`、`TestEnableFirstThenSettleUnderReadCommitted`、`TestForwardSentButNotMarked`（`internal/ledger`）；`TestConsumeRedeliveryAfterACrash`、`TestKafkaRoundTrip`（`internal/usageevents`） | `go test -race -run 'TestEventInSameTransaction\|TestEnableFirstThenSettleUnderReadCommitted\|TestForwardSentButNotMarked' ./internal/ledger/`；`go test -race ./internal/usageevents/`（要 Kafka）；`scripts/compose-usage.sh` | 30 项，公开 CI 通过：Kafka 停了请求照常、恢复后补发，消费者重读一遍汇总不变 |
| 流式：SSE 逐个转发，按流里的 usage 结算，客户端中途断开照常结算 | `internal/gateway/stream.go` 的 `relay`、`stream.pump`、`stream.send`、`writeGrace` | `TestStreamSettlesOnlyAfterTheEnd`、`TestStreamFailuresMidway`、`TestStreamDenseAtTheDeadline`、`TestStreamIdempotencyComplete` | `go test -race -run TestStream ./internal/gateway/`；`scripts/compose-stream.sh` | 21 项，公开 CI 通过；真实上游上没跑过（见“还没做的验证”） |
| 多上游和熔断：按权重分流，只在拨号失败时换上游，按上游熔断 | `internal/gateway/route.go` 的 `router.pick`、`breaker`；`internal/gateway/upstream.go` 的 `failover`；`internal/gateway/quota.go` 的 `reroute` | `TestBreakerOpensAtThreshold`、`TestBreakerStaleReport`、`TestFailoverOnDialFailure`、`TestNoFailoverAfterSent`、`TestEachUpstreamOnce` | `go test -race -run 'TestBreaker\|TestFailover\|TestNoFailoverAfterSent\|TestEachUpstreamOnce' ./internal/gateway/`；`scripts/compose-failover.sh` | 25 项，公开 CI 通过（两个假上游） |
| 可观测：一次调用一棵 span 树、接收 `traceparent`；Prometheus 指标；SLO 和告警规则 | `internal/telemetry` 的 `Setup`、`NewMetrics`、`otlp.go`；`internal/gateway/observe.go`；`deploy/prometheus/rules.yml` | `TestSpanTree`、`TestSpanAttributesAllowList`（`internal/gateway`）；`TestOTLPJSONEncoding`、`TestMetricsLint`（`internal/telemetry`）；`deploy/prometheus/rules_test.yml`（每条告警触发、不触发各有用例） | `go test -race -run 'TestSpanTree\|TestSpanAttributesAllowList' ./internal/gateway/`；`go test -race -run 'TestOTLPJSONEncoding\|TestMetricsLint' ./internal/telemetry/`；`scripts/compose-observability.sh`；promtool 的命令见[SLO 和告警](observability.md#slo-和告警) | 39 项，公开 CI 通过；联调第 2 个场景：QS 和 QF 的 span 在同一条 trace 里父子相接 |
| 和 QS 的联调：QS 的整套演示经 QF 调模型，按 run 逐个对账 | `scripts/compose-qs-e2e.sh`、`scripts/check_qs_runs.py`、`scripts/qs_query.py` | `scripts/test_check_qs_runs.py` | `scripts/compose-qs-e2e.sh <QS 的检出>`；`scripts/compose-qs-fake-check.sh` | 联调五个场景 40、4、25、27、43 项，一共 139 项，qs-fake 的连通检查 14 项；公开 CI 通过（2026-10-19，`a56cd4b`，QS 是公开的 `0ec8d76`，开着嵌入缓存） |
| 不伪造用量；Fake 和真实上游分开 | `internal/gateway/quota.go` 的 `outcome`；`internal/config/config.go` 的 `Load`（上游的 Key、地址、数据库连接串缺了就拒绝启动） | `TestFinishTable`、`TestCallLogUsage`、`TestStreamFailuresMidway`（`internal/gateway`）；`TestParseErrors`（`internal/config`） | `go test -race -run 'TestFinishTable\|TestCallLogUsage\|TestStreamFailuresMidway' ./internal/gateway/`；`go test -race -run TestParseErrors ./internal/config/` | 联调第 4 个场景：上游超时的调用记 `USAGE_UNKNOWN`、占着预留，QF 的日志结清不了它 |
| 日志、span、指标里没有 Key、密码、连接串和正文 | `internal/gateway/calllog.go` 的 `logCall`；`internal/gateway/observe.go` | `TestCallLogHasNoSecretsOrBodies`、`TestTelemetryHasNoSecrets`（`internal/gateway`）；`TestErrorsHideThePassword`（`internal/ratelimit`）；`TestOpenErrorsHideDSN`（`internal/ledger`） | `go test -race -run 'TestCallLogHasNoSecrets\|TestTelemetryHasNoSecrets' ./internal/gateway/`；`go test -race -run TestErrorsHideThePassword ./internal/ratelimit/`；`go test -race -run TestOpenErrorsHideDSN ./internal/ledger/` | 每个演示脚本最后都在日志、输出里搜 Key、密码和正文的标记，只数次数 |
| 优雅退出：等进行中的请求收完尾再退 | `cmd/quotaforge/main.go` 的 `run`、`drain` | `TestRunWaitsForInFlightRequest`、`TestDrainOrder`、`TestProcessFinishesStreamOnSIGTERM`、`TestStopGracePeriodCoversShutdown`（`cmd/quotaforge`） | `go test -race -run 'TestRun\|TestDrainOrder\|TestProcess\|TestStopGracePeriodCoversShutdown' ./cmd/quotaforge/` | 冒烟脚本：慢请求进行中 `docker compose stop`，请求拿到 200、结算记进账本，容器以 0 退出 |

## 记录的结果

### 测试和 CI

- **Go 测试**：带 MySQL、Redis、Kafka 的全量 `go test -race ./...` 一共 725 个（含子测试），2026-10-19 的公开 CI 在 `a56cd4b` 上全部通过。Windows 上有 3 个照设计跳过（`TestProcessFinishesStreamOnSIGTERM`、`TestProcessDrainsInFlightRequestBeforeFlushing` 要发 SIGTERM，Go 在 Windows 上发不了；`TestUnreadResponseTimesOut` 依赖内核的发送缓冲会被写满，Windows 的回环连接会把整个响应缓冲下来），Linux 上全部运行。
- **演示脚本**（每个从空环境起；项数是检查的条数）：

  | 脚本 | 演示什么 | 项数 |
  |---|---|---|
  | `scripts/compose-smoke.sh` | 单独运行：转发、账本、幂等、调用状态查询、有界结算、优雅退出、真杀进程和对账、限流、Redis 停了又起来 | 147 |
  | `scripts/compose-qs-fake-check.sh` | 给 QS 用的覆盖文件：网络、配置、嵌入缓存的命中 | 14 |
  | `scripts/compose-two-instances.sh` | 两个实例：超扣、同一个键、共享限流、杀掉一个由另一个接手、同时回收和清理 | 53 |
  | `scripts/compose-observability.sh` | Jaeger 和 Prometheus：span 树、日志关联、指标、告警规则 | 39 |
  | `scripts/compose-stream.sh` | 流式转发和计费 | 21 |
  | `scripts/compose-failover.sh` | 分流、换上游、熔断和恢复 | 25 |
  | `scripts/compose-cache.sh` | 嵌入缓存：命中、隔离、合并回源、缓存停了又起来、过期 | 34 |
  | `scripts/compose-usage.sh` | 用量事件：汇总、对账、Kafka 和消费者停了又起来、重读一遍 | 30 |
  | `scripts/compose-qs-e2e.sh` | 和真的 QS 联调：正常、跨服务 trace、额度耗尽、上游超时、两个实例，五个场景 40、4、25、27、43 项 | 139 |

- **公开 CI**：2026-10-19 在公开的 `a56cd4b` 上两个任务都通过：`test`，以及八个演示脚本和联调（联调检出 QS 公开的 `0ec8d76`，开着嵌入缓存）。

### 压测

- 两台机器（4 核的云端容器、24 核的笔记本），两个实例，假上游固定 50 毫秒，13 个场景各跑两遍；结果、瓶颈和原始数据见[压测](scaling.md#压测)，原始数据在 `bench/` 下。
- 一个账户的吞吐卡在约 150 次/秒（云端），瓶颈是额度行上的行锁；16 个账户、单实例、并发 64 时约 370 次/秒。这是一台机器上的数字，不能当成“QF 能支撑多少 QPS”。
- 2026-10-18 代码精简前后，在同一台机器上用同样的参数各跑一遍：13 个场景的吞吐都不低于精简前的 95%，p50、p95、p99 都不高于精简前的 110%。这次的原始输出没有放进仓库。

### 真实上游

下面几次都在本机（Windows，Windows PowerShell 5.1）运行，用当时的 PowerShell 脚本 `scripts/compose-real.ps1` 起 QF，上游是阿里云百炼的 `qwen-plus`、`text-embedding-v4`，QS 用它自己的真实演示脚本经 QF 调模型，之后用 `compose-real.ps1 -Reconcile` 逐个 run 对账。以后换成 bash 的 `scripts/compose-real.sh`（用法见[经 QS 的真实演示](queryshield.md#经-qs-的真实演示和对账本机会花钱)），它只在假上游上跑过，还没对真实上游跑过。

- **2026-10-08、10-10、10-12**（公开之前和刚公开时的版本）：各跑过一次 `-Check`、经 QS 的演示和逐个 run 的对账，都通过。
- **2026-10-15**（QF 是公开的 `a7847c0` 的代码）：`-Check` 通过；QS 的演示 14 题全部通过；13 个 run 逐个对上（聊天次数和三个用量数），`check_qs_runs: 0 failed`，`qfctl verify` 是 `ledger ok`。这次一共 56 次调用（40 次聊天，其中 1 次是 `-Check` 的重放；16 次嵌入），都是 200，没有被限流，没有熔断和换上游。
- **2026-10-19**（QF 是公开的 `a56cd4b` 的代码，开着嵌入缓存）：同一个 QF 里跑两遍 QS 的演示，都通过。第一遍 13 个 run 逐个对上，0 failed；第二遍建知识索引的 4 次嵌入全部命中缓存，这一遍的 13 个 run 也逐个对上，只多出预期的那一行 FAIL（账本里还有第一遍的 run，一次对账只对一遍演示，见[经 QS 的真实演示](queryshield.md#经-qs-的真实演示和对账本机会花钱)）。`qfctl usage`：已消费 255,808 tokens，`CACHED` 5，按 run 加起来对得上。
- **QS 的评测**：QS 经本网关跑了它的盲测（两种配置交替各跑 3 遍，72 个 run、126 次聊天、418,058 tokens，没有一次网关或上游的错误）和多轮开发评测（92 个 run、261 次聊天、885,149 tokens，没有网关的错误），网关的代码同公开的 `bbb5c01`；评测的成绩见 [QS 的能力证据清单](https://github.com/Jonsonwu0823-coding/queryshield/blob/0ec8d76d09d8431f12afdff4083ee2666e6c9515/docs/evidence.md)。

## 还没做的验证

- **百炼以外的真实上游**：只在百炼上跑过。别的 OpenAI 兼容上游的 usage、错误码、流的格式可能不同。
- **真实上游的流式**：`compose-real.sh --stream` 写好了，还没在百炼上跑过；百炼的流中途出错时发什么，也没有找到文档（见[流式](api.md#流式)）。
- **两个真实上游之间的熔断和换上游**：只用两个假上游演示过；真实运行的配置里只有一个上游。
- **真实上游上的两个实例**：两个实例只在假上游上验证过（检查脚本、压测、联调第 5 个场景）。
- **长时间的稳定性**：没有做过几小时、几天的连续运行。
- **单个 Kafka broker 以外的部署**：用量事件只在单节点的 Kafka（KRaft）上跑过，没有集群、副本和故障切换。
- **托管的 MySQL、Redis**：只在 `mysql:8.4`、`redis:8` 的容器上测过。读已提交为默认隔离级别的服务器那一条在测试里覆盖了（`TestEnableFirstThenSettleUnderReadCommitted`），别的托管服务的差异没有验证。
- **Windows 上的 3 个测试**：照设计跳过，只在 Linux 上运行（见上文“测试和 CI”）。
- **压测的规模**：只在两台机器上做过闭环压测，没有开环压测，也没有在多台机器上扩容。
