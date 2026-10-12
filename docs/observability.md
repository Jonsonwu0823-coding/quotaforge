# 可观测：追踪、指标、SLO 和告警

QF 有三个出口：每次调用一行 JSON 日志（一直有）、OpenTelemetry 的 span（以 OTLP 发给 Jaeger 这类收集端）、Prometheus 的指标。后两个默认关着，用环境变量打开（见[运行和运维的“环境变量”](operations.md#环境变量)）：

| 变量 | 作用 |
|---|---|
| `QUOTAFORGE_OTLP_ENDPOINT` | 开追踪：span 发到 `<地址>/v1/traces`，例如 `http://qf-jaeger:4318` |
| `QUOTAFORGE_TRACE_SAMPLE_RATIO` | 没有上游 `traceparent` 时的采样比例，默认 1 |
| `QUOTAFORGE_INSTANCE` | span 里的实例标识，默认主机名 |
| `QUOTAFORGE_METRICS_ADDR` | 开指标：另一个监听地址上的 `GET /metrics`，例如 `0.0.0.0:9464` |

不设时行为和没有这些功能一样：不建导出器，不读请求里的 `traceparent`，日志行里没有 trace 的字段，不开指标的端口。

三个出口都不写 Key、`Authorization` 的值、上游 Key、密码、连接串和请求、响应的正文；`internal/gateway/redaction_test.go` 走一遍成功、重放、上游出错、额度不足，把三处的输出都搜一遍。span 的属性和指标的标签还多一条规则：值只能来自固定集合（错误码、状态、结局、桶名）或配置里的名字（别名、上游名），客户端给的原始字符串一律不进，例如没配置过的模型名、幂等键、`X-Run-Id`、`X-Client-Request-Id`。这些在日志里有，按 `trace_id` 就能找到那一行。

## 追踪

### 一次调用一棵树

根是服务端 span，名字是方法加路由；每次对外部依赖的调用（账本、Redis、上游）是它的直接子 span。没执行的步骤没有 span：没有限流桶就没有 `admit`、`adjust`；额度不足就停在 `reserve`。解析、估算、指纹这些不碰外部的步骤算在根里。

```text
POST /v1/chat/completions      SERVER  整个请求，起点是请求进来的时间（和日志的 duration_ms 同一个总耗时）
├── authenticate               INTERNAL  按 Key 的哈希查账本
├── admit                      INTERNAL  Redis 准入（有限流桶时）
├── reserve                    INTERNAL  账本预留（带幂等键时可能直接给出重放）
├── mark_sent                  INTERNAL  落发送意图
├── upstream                   CLIENT    发请求到读完正文
├── finish                     INTERNAL  账本收尾
└── adjust                     INTERNAL  修正 TPM 桶（有 TPM 桶时）

GET /v1/calls/{id}             SERVER  调用状态查询
├── authenticate
└── lookup_call                INTERNAL  查账本
```

`/healthz`、404、405 不追踪。

| span | 属性 |
|---|---|
| 根 | `http.request.method`、`http.route`、`http.response.status_code`（写回了才有）、`qf.request_id`（调用号，就是响应头 `x-request-id`）、`qf.endpoint`、`qf.model_alias` 和 `qf.upstream`（查到模型之后才有）、`error.type`（错误码）、`qf.call_status`、`qf.outcome`、`qf.reserved_tokens`、`qf.replay_of`、`qf.usage.known` 和已知时的三个 token 数、`qf.client_disconnected`、`qf.rate_limited_by`（例如 `key/rpm`）、`qf.call_id`（查询时被查的调用号） |
| `upstream` | `qf.upstream`、`http.request.method`、`http.response.status_code`（上游的） |
| 出错的子 span | `error.type` |

**出错**：一步失败时它的 span 标成 Error，描述和 `error.type` 都是错误码；收尾、修正失败不改变响应，也照样标。根 span 只有 5xx 标成 Error（HTTP 服务端 span 的惯例），错误码照记。底层错误的原文（驱动、Redis、网络的错误文字，可能带地址）只进日志的 `ledger_error`、`rate_limiter_error`，不进 span，所以 QF 不用 `RecordError`，span 上没有 event。

### 接 traceparent、采样

- 客户端带 W3C 的 `traceparent`（和 `tracestate`）时，根 span 接成它的子 span，整条 trace 从客户端开始；不带就新起一条。不读 `baggage`。
- 不合法的 `traceparent`（格式不对、大写的十六进制、trace-id 或 parent-id 全 0、不认识的版本）当作没带：新起一条 trace，请求照常处理，不回 400。
- `traceparent` 不进幂等的指纹（指纹只看接口和请求体）：同一个 `Idempotency-Key`、同样的请求体，换了 `traceparent` 的重试照样重放。
- **不往上游发**：QF 不转发客户端的请求头，也不给上游加新的头，`traceparent` 和 `tracestate` 都不发（`TestUpstreamGetsNoTraceparent` 守着）。
- **采样**：有 `traceparent` 时服从它的 sampled 标志，整条 trace 由最上游决定采不采，不会出现调用方采了、QF 没采；没有时按 `QUOTAFORGE_TRACE_SAMPLE_RATIO`（默认 1，全量：QF 的量很小，全量才保证排查时 trace 一定在）。
- **鉴权之后才建 span**：没通过鉴权的（401）、鉴权这一步就出错的（账本不可用的 503）都没有 span，日志和指标照常有。所以没有 Key 的请求不能借 `traceparent` 让 QF 产生 span。根 span 的起点回填成请求进来的时间，`authenticate` 也用记下的起止时间补上。
- 通过鉴权的客户端带上 sampled 的 `traceparent`，能让自己的请求都被采样。但每个请求的 span 数是固定的几个，追踪量跟着请求量走，请求量由 Key 和账户的限流、额度管；导出队列有界（2048 个），满了就丢，不阻塞请求。所以不另外管。

### 导出

- 批量、异步：span 结束时只是入队，后台每 5 秒或攒满 512 个发一批，一批一次 HTTP POST，以 OTLP/JSON 编码。收集端挂住或不可用时，请求的结果和耗时不受影响（`TestRequestsDoNotWaitForTraceExport`）。
- 不重试、不压缩：一批发失败（连不上、超时 2 秒、非 2xx）就丢掉。错误第一次立即记一行 `msg: "telemetry"` 的日志，之后每分钟最多一行，带上这期间的次数（`problems`）；每次都计进 `qf_trace_export_errors_total`。OTel SDK 自己的错误和警告也走这里，不写 stderr（用量导出读的 `docker compose logs` 里有 stderr，一行非 JSON 会让它以 1 退出）。
- 导出器不走 `HTTP_PROXY` 这类代理变量：span 只发给同一网络里的收集端。
- **为什么自己写导出器**：官方的 `otlptracehttp` 经共用的 proto 包带进 gRPC（多 10 个模块、约 4.4 MB 的 `vendor/`），QF 不用 gRPC。自己写的在 `internal/telemetry/otlp.go`，连注释不到 200 行，实现 SDK 的 `SpanExporter` 接口，只编码 QF 用到的：没有 events、links，属性是字符串、整数、布尔、浮点；没有 gzip、重试和 protobuf 编码。编码按 OTLP 的 JSON 规则（id 是十六进制，64 位整数写成十进制字符串，kind 和 status 用 OTLP 的编号），有固定样例的单元测试，演示脚本在真的 Jaeger 上按 trace id 查回整棵树。
- **换成官方导出器**只改 `internal/telemetry/telemetry.go` 里建导出器的那一行（`newExporter(...)` 换成 `otlptracehttp.New(...)`），删掉 `otlp.go`，再 `go mod vendor`。
- **退出**：在途请求都处理完（最多关闭上限，默认 36 秒）、后台循环停下之后，才把队列里还没导出的 span 发完，最多 2 秒；到点没发完的丢掉，账本在这之前已经收完尾。两者之和 38 秒，小于 Compose 的 `stop_grace_period`（40 秒，`TestStopGracePeriodCoversShutdown` 守着）。

## 日志关联

- 被采样的调用，调用行（`msg: "call"`）和查询行（`msg: "lookup"`）多两个字段：`trace_id` 和根 span 的 `span_id`；客户端带了合法的 `traceparent` 时还有 `parent_span_id`，就是其中的 parent-id（客户端那一次请求的 span id）。没采样或没开追踪时这三个字段都没有。**日志里有 `trace_id`，Jaeger 里就有这条 trace，导出失败时除外。**
- 写不写这三个字段看 QF 自己的根 span 是不是在记录，不看 `traceparent` 里的 sampled 标志；没开追踪时 QF 根本不解析 `traceparent`。所以日志里的 `span_id` 一定是 QF 的 span，不会是客户端的。
- 从调用号找 trace：在 Jaeger 里按标签 `qf.request_id=<x-request-id>` 搜；或者先在日志里按 `request_id` 找到那一行，取它的 `trace_id`。
- 从 trace 找日志：根 span 的 `qf.request_id` 就是日志的 `request_id`；或者按 `trace_id` 搜日志。
- 400 `invalid_request` 的调用行有 `invalid_reason`，取值固定：`unreadable_body`、`not_object`、`malformed_json`、`trailing_data`、`ambiguous_field`、`model_not_string`、`model_missing`、`stream_not_boolean`、`max_tokens_invalid`、`fingerprint_failed`。不写 `encoding/json` 的错误原文：它会带上请求体里的字符。
- 请求体没发完客户端就断开时，和写回之前断开一样：日志没有 `status`，有 `client_disconnected: true`，错误码 `client_canceled`。

## 指标

在 `QUOTAFORGE_METRICS_ADDR` 上，只有 `GET /metrics`（Prometheus 的文本格式）；对外的监听地址上没有。Compose 里不发布到宿主机，只给同一网络里的 Prometheus 抓。标签都是固定集合，不用账户、Key、run、调用号这类会无限增长或可能敏感的值；按账户的用量在账本里，`qfctl usage` 可查。

| 名字 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `qf_requests_total` | counter | `endpoint`（chat、embeddings、calls）、`status`、`code` | 每个请求一次，401 也算。`status` 是写回的 HTTP 状态，客户端已断开、没写回时是 `0`；`code` 是错误码，成功时为空 |
| `qf_request_duration_seconds` | histogram | `endpoint` | 总耗时 |
| `qf_overhead_seconds` | histogram | `endpoint` | QF 自身耗时 = 总耗时 − 等上游的时间（没调上游的就是总耗时）。0.25 秒是一个桶边界，延迟 SLO 按它算 |
| `qf_upstream_requests_total` | counter | `upstream`、`endpoint`、`result` | 调过上游的调用；`result` 是 `ok` 或上游这一步的错误码（`upstream_*`） |
| `qf_upstream_duration_seconds` | histogram | `upstream`、`endpoint` | 发请求到读完正文 |
| `qf_calls_total` | counter | `endpoint`、`outcome` | 预留过的调用按网关的判定：`settled`、`cancelled`、`usage_unknown` |
| `qf_usage_tokens_total` | counter | `endpoint`、`upstream`、`type`（prompt、completion、total） | 只计结算的调用；用量未知的不计成 0，数在 `qf_calls_total{outcome="usage_unknown"}` 里 |
| `qf_idempotent_replays_total` | counter | `endpoint` | 重放。幂等的几种拒绝看 `qf_requests_total` 的 `code` |
| `qf_rate_limited_total` | counter | `layer`（key、account、upstream）、`metric`（rpm、tpm） | 限流拒绝 |
| `qf_dependency_errors_total` | counter | `dependency`（ledger、rate_limiter）、`operation` | 账本、Redis 操作失败，含不改变响应的收尾、修正失败 |
| `qf_background_reclaimed_total` | counter | `result`（released、marked_unknown） | 后台回收 |
| `qf_background_purged_total` | counter | — | 清理掉的过期幂等记录 |
| `qf_background_errors_total` | counter | `task`（reclaim、purge、unknown_scan） | 后台出错（清理到点停下不算） |
| `qf_background_last_success_timestamp_seconds` | gauge | — | 最近一次三件都没出错的一轮结束的时间 |
| `qf_background_interval_seconds` | gauge | — | 后台一轮的间隔（回收上界的四分之一，默认 9 秒） |
| `qf_unknown_usage_calls` | gauge | — | 还占着预留的未知用量调用，全部账户 |
| `qf_unknown_usage_oldest_age_seconds` | gauge | — | 其中最早一条从预留到现在的秒数，没有时为 0 |
| `qf_trace_export_errors_total` | counter | — | 追踪 SDK 报的错误，主要是导出失败 |
| `qf_db_*` | gauge、counter | — | 账本连接池：`open`、`in_use`、`idle`、`max_open` 的连接数，等连接的次数和总时长 |
| `go_*`、`process_*` | — | — | Go 运行时和进程 |

- 未知用量的两个数由后台循环每轮查一次（默认 9 秒）：一条 `COUNT` 加 `MIN(created_at)`，走索引 `calls_status_created (status, created_at)`，只扫 `USAGE_UNKNOWN` 的行（平时很少，对账后就离开这个状态）。两个实例数的是同一个库，值相同。
- 后台的 `db_pool` 日志行（这期间有请求等过连接时记一行）和 `qf_db_*` 并存：压测脚本按那一行取证，没部署 Prometheus 时也看得到。

## SLO 和告警

规则在 `deploy/prometheus/rules.yml`，单元测试在 `deploy/prometheus/rules_test.yml`。时间窗 30 天。

**可用性：99.9%。**

- 分子：QF 自己造成的失败 = `status` 是 5xx 且 `code` 不以 `upstream_` 开头。现在就是 `ledger_unavailable`（账本数据库不可用或超时）和 `rate_limiter_unavailable`（Redis 不可用或超时）；以后 QF 自己新加的 5xx 自动算进来。
- 分母：全部请求（两个转发接口和调用状态查询，含 4xx、401 和客户端断开的）。
- 上游的失败（`upstream_*`：502、503、504）不进 QF 的 SLO，单独看 `qf_upstream_requests_total`。
- 依据：账本和 Redis 都是单机；99.9% 一个月的预算约 43 分钟全停。
- 有请求、QF 还没出过 5xx 时（分子没有任何序列），记录规则的值是 0，不是空：分子写成 `sum(…) or vector(0)`，两边都没有标签，补的 0 对得上。一个请求都没有时分母是空的，比值仍然没有值，看板显示“无数据”；两种情形下两档告警都不触发。

**延迟：99% 的 chat、embeddings 请求，QF 自身耗时 ≤ 250 毫秒。**

- 分子：`qf_overhead_seconds` 里大于 0.25 秒的（按 `le="0.25"` 的桶，不插值）；分母：chat、embeddings 的全部请求。调用状态查询不算。
- 依据（[压测](scaling.md#压测)，假上游固定约 50 毫秒，扣掉直连假上游的约 53 毫秒粗算）：并发 16 时自身耗时的 p99 约 70–125 毫秒；并发 64、16 个账户时约 260–280 毫秒，已经越过 250；一个账户、并发 64 时 p50 就有约 350–370 毫秒（额度行是热点）。所以 250 毫秒在正常负载下有一倍左右的余量；并发到 64 时错误预算开始消耗，热点账户排队时快速消耗的告警会响。

**消耗速率告警**（两档，每档两个窗口：长窗口说明烧得多，短窗口说明现在还在烧，恢复后很快解除）：

| 告警 | 条件 | 持续 | 级别 |
|---|---|---|---|
| `QFAvailabilityBudgetBurnFast` | 1 小时和 5 分钟的失败比例都 > 14.4 × 0.1%（1 小时烧掉 2% 的预算） | 2 分钟 | critical |
| `QFAvailabilityBudgetBurnSlow` | 6 小时和 30 分钟的都 > 6 × 0.1%（6 小时烧掉 5%） | 15 分钟 | warning |
| `QFLatencyBudgetBurnFast` | 同上，预算 1% | 2 分钟 | critical |
| `QFLatencyBudgetBurnSlow` | 同上 | 15 分钟 | warning |

流量很小时，一两个错误就能让比例越过阈值（10 个请求错 1 个就是 100 倍）。

**运行告警**：

| 告警 | 条件 | 持续 | 级别 |
|---|---|---|---|
| `QFBackgroundStalled` | 某个实例的后台循环约 10 轮没有成功：`time() − 最近一次成功 > 10 × 间隔`（默认约 90 秒） | 1 分钟 | warning |
| `QFUnknownUsageUnreconciled` | 有未知用量的调用超过一天没对账（对账要人按上游的账单做：`qfctl unknown`、`qfctl reconcile`） | 10 分钟 | warning |
| `QFDependencyUnavailable` | 账本或 Redis 持续出错（没有请求时不会出错，也就不告警：没有用户受影响） | 5 分钟 | critical |

每条告警都有触发和不触发的测试情形，数值贴着阈值的两边。跑法（在项目根目录下运行）。两处是给 Windows 的 Git Bash 的，Linux 下没有影响：挂载用相对路径 `./deploy/prometheus`，由 docker 命令按自己的当前目录解析，不用 `$PWD`（Git Bash 里它可能是 `/tmp/...` 这种映射出来的路径，Docker Desktop 会挂上一个空目录）；`MSYS_NO_PATHCONV=1` 不让 Git Bash 把容器里的路径 `/etc/prometheus/...` 改写成 Git 的安装目录。

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v ./deploy/prometheus:/etc/prometheus:ro --entrypoint promtool prom/prometheus:v3.15.0 test rules /etc/prometheus/rules_test.yml
```

## 演示环境

覆盖文件 `compose.observability.yaml` 加上 Jaeger（`jaegertracing/jaeger:2.21.0`，all-in-one，内存存储）和 Prometheus（`prom/prometheus:v3.15.0`，每 5 秒抓一次，加载上面的规则）。两者的界面只发布到本机：

```bash
# 在项目根目录下运行
docker compose -f compose.yaml -f compose.observability.yaml up -d --build --wait
# Jaeger：http://127.0.0.1:16686   Prometheus：http://127.0.0.1:9090
# 两个实例：再叠 compose.two.yaml 和 compose.observability.two.yaml（顺序就是这样）
docker compose -f compose.yaml -f compose.observability.yaml -f compose.two.yaml -f compose.observability.two.yaml up -d --build --wait
```

- QF 的指标端口 9464 和 Jaeger 的 OTLP 端口 4318 都只在 Compose 的网络里。
- 抓哪些实例由 `deploy/prometheus/targets/` 下挂进去的文件决定：单实例只挂 `quotaforge.json`，两个实例再挂 `quotaforge-b.json`，两种叠法下都没有一直抓不到的目标。两个实例的 span 用资源属性 `service.instance.id`（`quotaforge`、`quotaforge-b`）区分，指标用 Prometheus 的 `instance` 标签区分。
- 按调用号找 trace（Jaeger 的查询 API v3，回答是 OTLP/JSON）：

```bash
curl -sS -G http://127.0.0.1:16686/api/v3/traces --data-urlencode query.service_name=quotaforge \
  --data-urlencode 'query.attributes={"qf.request_id":"<x-request-id>"}' \
  --data-urlencode "query.start_time_min=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)" \
  --data-urlencode "query.start_time_max=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
curl -sS http://127.0.0.1:16686/api/v3/traces/<trace id>
```

`scripts/compose-observability.sh`（bash，CI 也跑）起单实例的演示环境，发成功、重放、额度不足、上游不可用、查询、401 几种请求，然后核对：

- 带 `traceparent` 的调用在 Jaeger 里按 trace id 和调用号都查得到，span 的名字和父子关系是上面那棵树，根 span 的父 span 是请求里给的那个；日志行的 `trace_id`、`span_id` 和它相同；401 没有 trace；
- Prometheus 抓到的计数和发的请求对得上，规则都加载了，`promtool` 检查配置、跑规则的单元测试；
- Jaeger 里的 trace、`/metrics` 的输出、QF 的日志里没有 Key 和正文的标记（只数次数）；
- 停掉 Jaeger 之后请求照常 200，导出出错的日志不超过 2 行，QF 在宽限期内退出。

摘要写在 `evidence/compose-observability-summary.txt`。

## 开销

用[压测](scaling.md#压测)的脚本和场景（单实例，假上游固定 50 毫秒，预热 5 秒、测 30 秒，各两遍）量了两组：不设三个变量（不追踪、不开指标），和叠上两个覆盖文件（全量采样、导出到 Jaeger、Prometheus 每 5 秒抓一次）。机器是 4 核的云端容器，所有容器在同一台机器上。

| 场景 | 并发 | 吞吐（次/秒） | p50（毫秒） | p95（毫秒） | p99（毫秒） |
|---|---|---|---|---|---|
| 1 个账户，不追踪 | 1 | 15.4 / 15.0 | 64.2 / 65.8 | 68.8 / 72.3 | 样本不足 |
| 1 个账户，全量追踪 | 1 | 15.2 / 15.3 | 64.7 / 64.3 | 70.8 / 71.0 | 样本不足 |
| 16 个账户，不追踪 | 16 | 192 / 186 | 80.1 / 82.1 | 112 / 122 | 138 / 152 |
| 16 个账户，全量追踪 | 16 | 196 / 198 | 78.6 / 78.1 | 109 / 109 | 138 / 131 |
| 16 个账户，不追踪 | 64 | 286 / 309 | 212 / 197 | 357 / 316 | 428 / 378 |
| 16 个账户，全量追踪 | 64 | 315 / 356 | 192 / 171 | 315 / 272 | 381 / 325 |

（“/”左边第一遍、右边第二遍；所有请求都成功。）

- 两组的差都在同一组两遍之间的波动以内（并发 64 不追踪的两遍就差 8%），量不出追踪和指标的开销：每次调用多 8 个 span 的创建、入队和十几次计数，是微秒级，压测的瓶颈在账本（见[两个实例和压测](scaling.md#压测)）。追踪的一组反而略快，是两组先后跑、机器状态不同，不说明追踪让 QF 变快。
- 全量追踪那一组 Jaeger 收到 306,752 个 span、拒收 0 个，QF 的导出错误 0 次；这期间单实例约 3.8 万次调用、每次 8 个 span，对得上（差的一百来个是结束时还在队列里、没到 5 秒一批的那些）。
