# 接 QueryShield

## 接 QS

和 QS 公开仓库的网关模式一致：

- **网络**：外部网络 `qs-qf`，两边都按 external 引用。用前先建：`docker network create qs-qf`，已存在就跳过。`qs-qf` 上只有 `quotaforge`；`qf-mysql`、`qfctl`、`qf-redis`、`qf-redis-cache` 不接。两边的服务怎么部署，见[架构说明](architecture.md#部署)的部署图。
- **地址**：QF 在 `qs-qf` 上的服务名是 `quotaforge`，端口 8080。QS 的 `base_url` 是 `http://quotaforge:8080/v1`，chat 和 embeddings 都用它。
- **Key**：起 QF 之后用 `qfctl` 给 QS 建账户、发一把 Key，交给 QS（`QUERYSHIELD_MODEL_API_KEY`）。真实上游的脚本 `scripts/compose-real.sh` 经 `eval` 把 Key 设进当前终端的环境变量 `QUERYSHIELD_MODEL_API_KEY`（不显示，不写文件，不经剪贴板；`--stop` 清掉），同一个终端里随后运行的 QS 演示由子进程继承这个变量：QS 的 `compose-real-demo.ps1` 在网关模式下先读它，有值就不提示。自己用 bash 起 QF 时，把 Key 留在当前 shell 的变量里，不要 echo。
- **配置**：`deploy/config.qs-fake.json`（chat 和 embeddings 两个别名都是 `qs-fake-upstream-v1`，上游 `http://fake-upstream:8000/v1`）、`deploy/config.real.json`（`qwen-plus`、`text-embedding-v4`）。都是静态文件，没有要替换的占位符。
- **嵌入缓存**：两套栈（假上游、真实上游）都开着（行为见[接口](api.md#嵌入缓存)，运维见[嵌入缓存](operations.md#嵌入缓存)）。
  - 只影响**不带 `Idempotency-Key` 的嵌入请求**，也就是 QS 第一次检索时建知识索引的那批；run 里的请求都带键，不查也不写缓存，按 run 对账不受影响。
  - QS 看得见的：这些请求的响应多一个 `Cache-Status` 头；命中时正文和原来那次逐字节相同（包括上游给的 `id` 和 `usage`），`x-request-id` 是新的调用号，QF 不扣这次的额度，调用状态查询是 `CACHED`；同一个实例里同时到的相同请求合并成一次回源；缓存出错当作没命中。命中在准入时照样占着上游那一层的 1 个 RPM，收尾时退回（见[限流](rate-limiting.md#限流redis)）。
  - 缓存放在单独的 `qf-redis-cache`（不持久化，64 MB，满了按 LRU 淘汰），不接 `qs-qf`，不和限流共用 `qf-redis`。缓存的键里有账户号；真实栈每次启动都是新的账本库和新的 `qf-redis-cache`。
  - 一个 QS 进程只建一次索引，所以要 QS 重新起来、第一次检索重建索引时才会命中（例如同一个 QF 窗口里再跑一遍 QS 的演示，见下面“真实演示”的第 3 步）。
  - **怎么关**：真实栈用 `compose-real.sh --no-cache`。这时 QF 用的配置是 `deploy/config.real.json` 去掉 `embedding_cache` 那一行，和开缓存之前的配置逐字节相同，也不起 `qf-redis-cache`；容器里还留着 `QF_REDIS_CACHE_URL` 这个环境变量，但没有配置指向它，QF 不读。假上游的栈没有开关（联调靠它验证缓存），要关就删掉 `deploy/config.qs-fake.json` 里 `embedding_cache` 那一行（连同前一行末尾的逗号）。

接 QS 的假上游（QS 那边起 `app`、`fake-upstream` 并接入 `qs-qf`）：

```bash
# 在项目根目录下运行
docker network inspect qs-qf >/dev/null 2>&1 || docker network create qs-qf >/dev/null
docker compose -f compose.yaml -f compose.qs-fake.yaml up -d --build --wait quotaforge
docker compose run --rm -T --no-deps qfctl account create -name queryshield -limit 1000000
QF_KEY=$(docker compose run --rm -T --no-deps qfctl key issue -account queryshield -models qs-fake-upstream-v1 | sed -n 's/^key: //p')   # 交给 QS；不要 echo
```

### 端到端联调（真的 QS，fake-upstream 模式，五个场景）

QF 接 QS 的假上游，QS 的 `app` 经 QF 调模型。一条命令跑五个场景：正常、跨服务 trace、额度耗尽、上游超时、两个实例。每个场景都用 QF 的账本、调用日志和 Jaeger 核对。不调用任何付费模型。

- **前提**：QS 公开仓库（`https://github.com/Jonsonwu0823-coding/queryshield`）干净地检出在提交 `0ec8d76d09d8431f12afdff4083ee2666e6c9515`，目录任意；Docker、Python 3.10 起。
  `git clone https://github.com/Jonsonwu0823-coding/queryshield.git ../queryshield && git -C ../queryshield checkout 0ec8d76d09d8431f12afdff4083ee2666e6c9515`
- **命令**：`scripts/compose-qs-e2e.sh ../queryshield`（也可以用环境变量 `QS_DIR`）。
  - 用时：约 3 分钟，其中上游超时的场景要等 QF 的 chat 超时 25 秒，接手的场景要等回收约 40 秒；QS 镜像要首次构建的话，再多几分钟。
  - 结果：摘要在 `evidence/compose-qs-e2e-summary.txt`，每个场景一段，段尾报这个场景的项数、失败数和用时，最后一行是总的失败数，全部 `ok` 才以 0 退出。
  - 一个场景的检查失败只计数，后面的场景照跑；跑不下去的错误（取不到摘要、容器操作失败）直接停下。两种情况都会清理干净。
- **不碰本机已有的东西**：
  - QF、QS 各用联调专用的项目名（`qf-e2e-qf`、`qf-e2e-qs`）和镜像标签（`queryshield-app:qf-e2e-0ec8d76`）。结束时的 `down -v` 只删自己的容器、网络和卷，不会动 `queryshield_*` 的开发数据卷；`qs-qf` 是脚本建的才删。
  - QS 发布到本机的端口默认 18731，避开常用的 8000。
  - 联调镜像 `queryshield-app:qf-e2e-0ec8d76` 跑完会留着（下次直接用），不要了用 `docker rmi` 删。QS 的镜像缺了才构建；出口代理会替换证书的环境里，预先自己构建好这个标签即可。
  - 开始前报错退出的几种情况：`qs-qf` 上已有容器；同名项目在运行；端口被占（QF 两个实例的 8080、8081，QS 的 18731，Jaeger 界面的 16686；QS 的端口可以用 `QUERYSHIELD_HTTP_PORT` 换）；QS 目录不在固定提交或有改动。
- **QS 的四个 Compose 文件顺序不能换**：脚本按 `compose.yaml`、`compose.fake-upstream.yaml`、`compose.model-gateway.yaml`、`compose.fake-upstream-gateway.yaml` 的顺序，最后再加 QF 的 `deploy/compose.qs-e2e-qs.yaml`，它换镜像名，并设 QS 的 span 导出地址。顺序反了，`app` 会直连假上游、绕过 QF，而且不报错；脚本的核对（QF 账本里的聊天调用数对不上）会失败。
- **两边的 span 在同一个 Jaeger 里**：
  - QF 叠上 `compose.observability.yaml` 开着追踪（只起 `quotaforge` 和 `qf-jaeger`，不起 Prometheus）。
  - `compose.qs-e2e.yaml` 把 `qf-jaeger` 接进 `qs-qf`；`deploy/compose.qs-e2e-qs.yaml` 给 QS 的 `app` 设 `QUERYSHIELD_OTLP_TRACES_URL=http://qf-jaeger:4318/v1/traces`，QS 用 OTLP/HTTP 导出自己的 span（`service.name` 是 `queryshield`）。
  - `qs-qf` 上只有 `qf-jaeger`、`quotaforge`、QS 的 `app` 和 `fake-upstream`；第 5 个场景再加 `quotaforge-b`。脚本核对成员恰好是这些。
  - 脚本运行时，Jaeger 的界面在 `http://127.0.0.1:16686`，联调结束时随 `down -v` 一起删掉。要看一个 run 时，按摘要里的 `trace_id` 搜（Search 页的 Trace ID）。看到的是一棵树：QS 的 `queryshield.run`（每段执行一个），下面是步骤、工具和 `queryshield.model_call`，每次 HTTP 尝试一个 `queryshield.http_attempt`；QF 的根 span `POST /v1/chat/completions`（或 embeddings）就挂在这次尝试下面，再往下是 QF 自己的 `authenticate`、`admit`、`reserve`、`mark_sent`、`upstream`、`finish`、`adjust`。

**五个场景**（每个场景一个 QF 账户和一把 Key，各自对账；换账户时重建 QS 的 `app`，因为 QS 在进程启动时读 Key）：

| 场景 | 怎么造出来 | QS 那边 | QF 这边查什么 | 项数、用时（参考） |
|---|---|---|---|---|
| 1 正常（账户 `e2e`） | 跑 QS 的整套演示（14 题，12 个 run，48 次调用）；之后重建 QS 的 `app`，再提一个问题 | 摘要 `pass`，13 题 pass、1 题 not_applicable；重建之后的 run 正常结束 | 逐个 run 对账；调用日志和账本一一对应；trace 的三项；幂等键；`qfctl verify`；没有被限流的请求；重建索引的请求命中嵌入缓存（见下面） | 40 项，约 20 秒 |
| 2 跨服务 trace | 场景 1 的 12 个 run | 自己的 span 导出到 QF 的 Jaeger | 两个服务都在；每个 run 的 trace 里两边该有的 span 都在；QF 的每个根 span 挂在 QS 那次尝试的 span 下；QS 的 ERROR span 只能是失败的 run 自己的错误码 | 4 项 |
| 3 额度耗尽（`e2e-quota`） | 先提一个问题（run A，正常结束），再把账户的额度设成已消费的量（剩余 0），再提一个（run B） | run B：`FAILED`、`model_quota_exhausted`，HTTP 503，聊天 1 次（QS 发请求前就计数），`usage_total` 是 `unknown` | QF 回 429 `quota_exhausted`；run B 在账本里 0 次聊天：被拒的请求不进账本、不调上游（Jaeger 里只有失败的 `reserve`，没有 `upstream`）；已消费 + 预留 ≤ 额度，预留 0 | 25 项，约 15 秒 |
| 4 上游超时（`e2e-timeout`） | 先提一个问题（run C），再暂停 QS 的 `fake-upstream`（`docker pause`）、提一个（run D），之后恢复 | run D：`FAILED`、`upstream_http_error`（QS 把网关回的 504 记成这个码，事件里另记 504 和 `upstream_timeout`），HTTP 502，`usage_total` 是 `unknown` | QF 在 25 到 30 秒之间回 504 `upstream_timeout`，赶在 QS 自己的 30 秒之前；这次调用记 `USAGE_UNKNOWN`、占着预留；超时之后的对账见下面 | 27 项，约 40 秒 |
| 5 两个实例（`e2e-two`） | 起 `quotaforge-b`，和 `quotaforge` 共用网络别名 `quotaforge`；跑整套演示；再做一次“杀掉一个实例、另一个接手” | 演示同场景 1；run E：`FAILED`、`upstream_request_error`（连接被断开，QS 不重试）；run F 正常 | 整套演示照场景 1 逐个 run 对账，两个实例的日志合起来和账本一一对应；E 的调用由 `quotaforge-b` 回收成 `USAGE_UNKNOWN`（`reclaimed_in_flight`）；F 的 span 都来自 `quotaforge-b` | 43 项，约 65 秒 |

- **嵌入缓存**（场景 1 最后 5 项）：同一个账户里重建 QS 的 `app`（新进程，索引没了），再提一个问题，QS 重建知识索引，请求和演示时的那批逐个相同。
  - 查：这个问题的 run 正常结束；账本里不带 `X-Run-Id` 的嵌入是演示的 k 次 `SETTLED` 加重建的 k 次 `CACHED`，每次 `CACHED` 的 `cached_from` 指向演示那批里不同的一次；QF 日志里重建的 k 次是 `cache: hit`（就是 `Cache-Status: quotaforge; hit`），演示的 k 次是 `miss`，带 `X-Run-Id` 的嵌入都是 `bypass`；已消费等于已结算调用的用量合计（命中不扣额度），`qfctl usage` 的 `CACHED` 列是 k；再跑一次 `qfctl verify`。
  - 放在核对脚本之后：核对脚本要求账户里全是已结算的调用、不带 `X-Run-Id` 的嵌入都在 run 的嵌入之前，重建索引的 `CACHED` 两条都不满足。场景 3 到 5 各用新账户，缓存的键里有账户号，那几个场景里建索引的请求都不命中（两个实例共用一个 `qf-redis-cache` 也一样），核对不受影响。
  - QS 的知识快照编号不经接口返回（普通的 run 不带），联调不比它：命中时向量逐字节相同，编号也就不变，由 QS 的真实运行核对。
- **额度耗尽**：剩余设成 0，下一次预留不管多大都确定被拒，不靠计算请求的字节数。第一次提问之前要先让 QS 建好知识索引（第一个问题会触发，建索引的嵌入不属于任何 run）。剩余为 0 时建不了索引，QS 会直接拒绝提问、不建 run。
- **上游超时**：
  - 先暂停再发请求，没有时序竞争：暂停的容器里，内核照样完成 TCP 握手、收下请求，QF 已经发出，只是等不到回答。
  - QF 用默认的 25 秒 chat 超时，演示的就是“QF 先于 QS 的 30 秒回 504”这条约定。
  - 超时之后的对账：
    - 按 run 对账时，run D 列成“unknown，原因说得通”，不算对不上；
    - 账本这边，这次调用的预留留着不结清，只核对状态：已预留正好等于它占着的量，`qfctl unknown` 正好列出它，把 QF 的日志交给 `qfctl reconcile -log` 也结清不了（日志里结局未知的行不算证据），`qfctl verify` 是 `ledger ok`；
    - 能结清它的只有上游按调用给的账单（`qfctl reconcile -bill`），QS 的假上游没有账单，联调不伪造。
- **两个实例和接手**：
  - QS 每次请求都重新解析 `quotaforge` 这个名字，所以整套演示会分到两个实例上。怎么分只报不判（例如 16 和 35 次）：账本、逐个 run 的对账和 trace 都不看是哪个实例处理的。
  - 接手那一段按操作的先后确定，不靠运气：
    1. 先等两个实例在演示里的 span 都进了 Jaeger；
    2. 停掉 `quotaforge-b`，等 QS 里这个名字只解析到 `quotaforge`；
    3. 暂停假上游，提一个问题（run E），等账本里出现它在 `quotaforge` 上发出的调用（`IN_FLIGHT`）；
    4. `kill -s KILL` 掉 `quotaforge`；
    5. 再起 `quotaforge-b`，等名字只解析到它，恢复假上游，提一个问题（run F），它只能落到 `quotaforge-b`；
    6. 最多等 60 秒（回收的上界 36 秒加一轮后台循环 9 秒，再加余量），等 `quotaforge-b` 的回收把 E 的调用标成用量未知。
  - 被杀的实例来不及写 E 那次调用的日志，也来不及导出它的 span；所以 E 那次调用是唯一可以没有日志行的调用，它的 trace 里只有 QS 的 span。

**核对的规则**（都由 `scripts/check_qs_runs.py --e2e` 做，输出每条一行，`ok` 或 `FAIL`，每个 run 一行；联调和经 QS 的真实演示的对账共用这一份代码，真实演示用不带 `--e2e` 的宽松模式，见下一节）：

- **run 记录从哪来**：QS 的 `demo-summary.json`（场景 1、5），或者脚本用 `scripts/qs_query.py` 直接提问得到的记录（`--queries`，每行一个 JSON，只有编号和数字，字段和摘要的记录同名）。摘要整体的四项（`status` 为 `pass`，`model_names` 只有 `qs-fake-upstream-v1`，13 题 pass、1 题 not_applicable，已知缺口只有 `Q06:rowset_customers_differ`）只在给了摘要时查。
- **逐个 run**（QS 的 `761bc91` 起，每条记录有 `run_id` 和 `usage_total`）：
  - `usage_total` 是 `known`：QF 账本里带这个 `X-Run-Id` 的**聊天**调用，次数等于 `model_call_count`，全部已结算，prompt、completion、total 三个用量数分别等于 `usage_total` 的三个数。
  - `not_run`：账本里这个 run 的聊天是 0 次。
  - `unknown`：只有 QS 报的错误码在下表里、QF 账本的形态也和表里一致，才算说得通（联调的故障场景造的就是这几种），其余一律失败：

    | QS 的 run 错误码 | QF 账本里这个 run 的聊天 | 原因 |
    |---|---|---|
    | `model_quota_exhausted` | 比 QS 少 1 次，全部已结算 | 最后一次在预留时被拒，不进账本、不调上游 |
    | `upstream_http_error` | 和 QS 一样多；最后一次是 `USAGE_UNKNOWN`、504、`upstream_timeout`，其余已结算 | QF 等上游超时 |
    | `upstream_request_error` | 和 QS 一样多；最后一次是 `USAGE_UNKNOWN`、`reclaimed_in_flight`，其余已结算 | 处理它的 QF 实例中途被杀，另一个实例回收 |
  - 账本里带 `X-Run-Id`、记录里却没有这个 run 的聊天调用，算失败；没有 `X-Run-Id` 的聊天调用也算失败（每次聊天都属于一个 run）。
- **嵌入**不按 run 计数：带 `X-Run-Id` 的都是 200、已结算、用量已知，而且它们的 run 都在记录里；不带的（第一次检索时建知识索引的那批，4 次）必须都在第一次带 `X-Run-Id` 的嵌入之前。
- **账户整体**：
  - 每次调用都有 `X-Client-Request-Id`；
  - 除了上表说得通的那几次 `USAGE_UNKNOWN`，全部已结算；
  - 已消费等于已结算调用的用量合计，已预留等于那几次未知调用占着的预留（没有就是 0），已消费加已预留不超过额度；
  - 至少比到 1 个 `usage_total` 为 `known` 的 run（`not_run` 不算，两种模式都要求），至少有 1 次带 `X-Run-Id` 的嵌入，避免空集合上的检查“空真”。
  - 场景 1 另查幂等键：固定提交里的 QS，run 里的调用（带 `X-Run-Id` 的聊天和查询嵌入）都带 `Idempotency-Key`，run 外的（建知识索引的嵌入）不带。账本里 `calls` 的 `run_id` 和 `idempotency_key` 要么都有、要么都没有，预期 44 条带键、4 条不带。假上游不触发重试，所以带键的调用都只执行一次。
- **调用日志和账本一一对应**（`--qf-log`，每个 QF 实例一份日志）：
  - 日志里有预留的调用行（有 `call_status`）和账本里这个账户的调用，按调用号一一对应：没有少的、多的、重复的。
  - 例外只有一种：回收标成未知（`reclaimed_in_flight`）的调用可以没有日志行，处理它的实例在调用中途死了。
  - 预留之前就被拒的行（例如 429 `quota_exhausted`）不进账本，单独计数。
  - 场景 1 的预期是 48 行对 48 次。
- **`traceparent` 的三项检查**（QS 的 `b6e6485` 起，QS 每个聊天、嵌入请求都带 `traceparent`，摘要每条记录有 `trace_id`；约定见 [可观测](observability.md) 和 QS 仓库的说明）：
  - 读 QF 的调用日志（`msg` 是 `call`、`account` 是这个场景的账户的行；账本不记 trace），只输出条数，不打印日志行。
  - 一，每一行调用日志都有合法的 `parent_span_id` 和 `trace_id`（小写十六进制、不全是 0）：每个请求都带了合法的 `traceparent`。
  - 二，带 `X-Run-Id` 的调用，`trace_id` 都等于记录里那个 run 的 `trace_id`；记录里没有这个 run 或没有它的 `trace_id`，也算失败。
  - 三，不带 `X-Run-Id` 的调用（建知识索引的嵌入），`trace_id` 不和任何 run 的相同；至少要有一次这样的调用。
  - 场景 1 的预期：48 行调用日志都有 `parent_span_id`；12 个 run 的 44 次调用和摘要一致；run 外 4 次，都不在 run 的 trace 上。
- **Jaeger 的检查**（`--jaeger http://127.0.0.1:16686`，查询接口 `/api/v3/services`、`/api/v3/traces/<trace_id>`）：
  - **等 span**：两边都是每 5 秒批量导出一次，脚本不靠睡眠，等确定的事件。每个 run 的 trace 里要等到三样都在，最多等 60 秒，到点没齐就判失败：
    - QF 日志里每次调用的 `span_id`（QF 的根 span）；
    - 每个 `parent_span_id`（QS 那次尝试的 span）；
    - 一个已经结束的 `queryshield.run`。QS 按结束的先后导出，run span 最后结束。
  - 一，`quotaforge`、`queryshield` 两个服务都在。
  - 二，每个 run 的 trace 里上面这些 span 都在，QS 和 QF 的 span 都有。日志里一行都没有的 run（场景 5 的 E）不要求 QF 的 span。
  - 三，QF 的每个根 span（kind 是 SERVER）的父，都是同一条 trace 里 QS 的 `queryshield.http_attempt`（CLIENT）：两边父子相接。被杀的实例可能只导出了几个子 span、没导出根 span，这些孤儿子 span 不算根。
  - 四，QS 的 ERROR span 的 `queryshield.error_code`，只能是这个场景里 QS 报 `FAILED` 的 run 自己的错误码；没有错误码的 ERROR 也算失败。每个 `FAILED` 的 run 的 trace 里，至少要有一个带它错误码的 ERROR span。
    - run 真的暂停时（等审批 `approval_required`、等用户回答 `clarification_required`），那次工具调用的 span 不标 ERROR：状态不设，`queryshield.status` 是 `waiting_approval` 或 `waiting_user`，`queryshield.error_code` 照旧。同样遇到审批或追问、但 run 没有暂停而是以 `DENIED` 或 `FAILED` 结束的，那次工具调用的 span 仍标 ERROR。所以联调里正常的 run 没有 ERROR span，带这两个码的 ERROR span 会让这一项失败：QS 以后要是把暂停标成 ERROR，联调会报出来。
  - 有 QF 拒绝的请求时（场景 3），这次请求的 QF trace 里有失败的 `reserve`、没有 `upstream`。有 QF 等上游超时的调用时（场景 4），QF 的根 span 和 `upstream` 都以 `upstream_timeout` 失败，它挂在 QS 的一次尝试下，那次尝试记着 504 和 `upstream_timeout`。
  - `--served-by <run_id>=<实例>`（场景 5 的 run F）：这个 run 的 QF 根 span 都来自这个实例（`service.instance.id`）。
  - 场景 1 的预期：12 个 run 的 trace 里 QS 192 个、QF 352 个 span，44 个根 span 都挂在 QS 的尝试下，QS 没有 ERROR span。
- **单元测试**：`python3 -m unittest discover -s scripts -p 'test_check_qs_runs.py'`（不需要 Docker；要 Python 3.10 起）。
- **数据从哪读**：
  - QF 的账本（`calls`、`budgets` 表）：`check_qs_runs.py` 按 Compose 的标签找到项目的 `qf-mysql` 容器，在里面用容器内的 secret 文件做只读查询，密码不出容器；
  - QF 的日志（`docker compose logs --no-log-prefix`）；
  - Jaeger 的查询接口。
  - 取出的 trace 只数 span，不打印内容。QS 的 span 只带编号、枚举和数字，不带问题、SQL 和回答。

## 经 QS 的真实演示和对账（本机，会花钱）

QS 的真实演示（14 题、13 个 run）经 QF 调百炼的模型；跑完后把 QS 的 `demo-summary.json` 和 QF 的账本逐个 run 对一遍，看两边的聊天次数和用量是否一致。QS 那边的脚本和参数以 [QS 仓库](https://github.com/Jonsonwu0823-coding/queryshield) 的说明为准。

前提（装法见[本机环境](development.md#本机环境)）：

- QF 这边：bash（macOS 自带的 bash 3.2 或 zsh 都行；Windows 上是 Git Bash）、Docker Desktop、`jq`、`curl`，对账要 Python 3.10 起。
- QS 换掉它的 PowerShell 入口之前，QS 的真实演示还是 PowerShell 脚本 `compose-real-demo.ps1`：Mac 上要装 PowerShell 7（`pwsh`）；Windows 上用自带的 Windows PowerShell 5.1（`powershell.exe`），从 Git Bash 里调。QS 的新入口公开之后，这里跟着换。

1. 在**同一个终端**里，一行命令先起 QF，再跑 QS 的真实演示。QF 的脚本顺带检查聊天、嵌入和重放（花费在 0.01 元以内），起好后经 `eval` 把 QF Key 设进这个终端的环境变量 `QUERYSHIELD_MODEL_API_KEY`；QS 的脚本是这个终端的子进程，在网关模式下读这个变量，不再提示输入，所以不用粘贴任何东西。**QS 的演示调用真实模型，整套约 1 元以内**：

   macOS（bash 或 zsh）：

   ```bash
   cd '<QS 的 queryshield 目录>' && eval "$('<QF 的 quotaforge 目录>/scripts/compose-real.sh' --check)" && pwsh -NoProfile -File scripts/compose-real-demo.ps1 -GatewayBaseUrl 'http://quotaforge:8080/v1' -GatewayNetwork 'qs-qf'
   ```

   Windows（Git Bash；目录写成 `/c/...` 或 `C:/...`。QF 的脚本还没在 Windows 上跑过）：

   ```bash
   cd '<QS 的 queryshield 目录>' && eval "$('<QF 的 quotaforge 目录>/scripts/compose-real.sh' --check)" && powershell.exe -NoProfile -File scripts/compose-real-demo.ps1 -GatewayBaseUrl 'http://quotaforge:8080/v1' -GatewayNetwork 'qs-qf'
   ```

   - 两个脚本必须在同一个终端里运行：环境变量只在这个终端和它的子进程里。
   - `--check` 还把同一个嵌入发两次：第二次应当命中嵌入缓存（`Cache-Status: quotaforge; hit`、正文和第一次相同、调用状态 `CACHED`、已消费不变），不调上游。带 `--no-cache` 时不发第二次，改成核对嵌入的响应没有 `Cache-Status` 头（缓存确实关着）。
   - 账户的额度上限默认 1,000,000 tokens，`--account-limit <tokens>` 可改；长评测（例如几十段对话、近百轮的多轮评测）建议给 `--account-limit 2000000`。
   - QF 的脚本失败（例如 Docker 没开、没有输入 Key、缺了 `jq`）时整行停下，QS 的演示不会启动：脚本失败时交给 `eval` 的最后一行是 `false`，`eval` 以 1 结束，`&&` 后面的不跑。`--check` 有一项没通过（任何一次请求不是 2xx，或者重放、缓存的核对不成立：`x-request-id`、正文和第一次不同，或者已消费变了）也会整行停下：脚本在打印完额度那一行之后报错退出，QS 的演示不会启动；QF 还开着，看完日志再 `--stop`。不带 `--check` 的启动照旧。
   - 忘了 `eval`（直接运行脚本，或者把输出重定向到文件）时，脚本一开始就停下、提示写法，不起栈、不发 Key。写成 `| cat`、`| tee` 时分辨不出来，Key 会显示出来。
   - QS 的脚本结束时把这个变量恢复成开始时的值，所以变量一直留到 `--stop`。
   - 跑完 QS 会把摘要拷到 `queryshield/evidence/compose-gateway-<时间>/demo-summary.json`。

2. 回到 QF 目录对账（`--reconcile` 不碰这个变量，也不用 `eval`）：

   ```bash
   cd '<QF 的 quotaforge 目录>' && scripts/compose-real.sh --reconcile '<QS 输出的证据目录>/demo-summary.json'
   ```

3. （可选，再跑一遍演示，花费和第 1 步的演示差不多）在真实运行里看到嵌入缓存命中：QF 不停，在同一个终端里再跑一遍 QS 的演示（第 1 步那行里 `pwsh` 或 `powershell.exe` 那一段，在 QS 目录下），再回到 QF 目录对账一次。
   - QS 的脚本每次都起一个新的 `app`，第一次检索时重建知识索引；请求和第一遍逐个相同，应当都命中。
   - 看 `qfctl usage` 的 `CACHED` 列（最后一列），在 QF 目录（数据库的 secret 文件在系统临时目录下的 `quotaforge-real/db`；Git Bash 上转成 `C:/…` 写法交给 Docker）：

     ```bash
     db=${TMPDIR:-/tmp}/quotaforge-real/db; QF_DB_SECRETS_DIR=$(cygpath -m "$db" 2>/dev/null || echo "$db") docker compose run --rm -T --no-deps qfctl usage -account queryshield
     ```

     开着缓存时，Compose 会提示一行孤儿容器（`qf-redis-cache`：它只在 `compose.real.yaml` 里，这行命令只带主文件），不用管。

     第二遍 `--reconcile` 输出里 `embeddings without X-Run-Id` 那一行也数得到 `CACHED`。个数等于第一遍建索引的请求数，`--check` 跑过的话再加 1。
   - 第二遍的 `--reconcile` 按设计会多**正好一行 FAIL**（退出码 1）：`… ledger chat calls belong to … runs not in the summary …`，因为账本里还有第一遍的 run，一次对账只对一遍演示。其余每行照常核对，应当都是 `ok`，`qfctl verify` 照常是 `ledger ok`。
4. 看完结果后停掉 QF，连数据库的数据一起删掉，并清掉 `QUERYSHIELD_MODEL_API_KEY`：

   ```bash
   eval "$(scripts/compose-real.sh --stop)"
   ```

`--reconcile` 不启动也不清理任何东西（也不碰 `QUERYSHIELD_MODEL_API_KEY`），只要求第 1 步起的 QF 还在运行；不能和 `--check`、`--stream`、`--stop` 一起用。它调用 `scripts/check_qs_runs.py`（真实模式），再跑一次 `qfctl verify`。找不到 Python 3.10 或更新的版本时会提示安装（核对脚本只用标准库）。退出码：0 全部对上；1 有对不上的 run、账本校验没通过，或脚本自己报错（QF 没在运行、找不到 Python 等，错误信息在输出里）；2 核对脚本跑不了（摘要不合法、读不了账本）。

怎么读输出（每条一行，只有题号、`run_id`、次数和用量数）：

```text
ok   Q03 run-…: chat calls 2, all SETTLED, usage 96/20/116 match QS      这个 run 两边对上了
FAIL Q05 run-…: chat calls QF 3 != QS 2; total_tokens QF 640 != QS 512     对不上：把这一行的 run_id 和两边的数发给 QS
skip Q07 run-…: usage_total unknown; QS chat calls 2, QF chat calls 3 (SETTLED 2, USAGE_UNKNOWN 1)
info …                                                                      只计数，不判断
check_qs_runs: 0 failed                                                     最后一行是 FAIL 的个数
```

- **只对 `usage_total` 为 `known` 的 run 比次数和三个用量数**（只算聊天，嵌入不算）；`not_run` 的 run 要求账本里没有它的聊天。
- **`skip`**：QS 说这个 run 的用量合计不完整（`unknown`），或者摘要里有 `run_id` 却没有 `usage_total`。不算失败，把 QS 的次数、QF 的次数和这些调用的状态列出来，方便看原因。常见原因是调用超过 25 秒：QF 的 chat 上游超时是 25 秒（QS 是 30 秒），超时的调用经 QF 会得到 504，没有用量，那个 run 就成了 `skip`；这是已约定的代价，不算 QF 的回归，出现时单独记下。
- **`info`**：不带 `X-Run-Id` 的聊天（例如第 1 步 `--check` 发的那一次）、带和不带 `X-Run-Id` 的嵌入（建知识索引的那批不带），只按状态数出来，不算失败。嵌入缓存命中的是 `CACHED`（`--check` 的第二次嵌入、第二遍演示重建索引的那批）。
- **账本里有摘要以外的 run**：算失败，提示用 `compose-real.sh` 重新起一套（账本是新的）再跑。一次对账对应一次演示，不要拿一份摘要去对跑过两次演示的账本；第 3 步的第二遍对账就会多这一行 FAIL，是预期的，看其余各行。
- 对不上的 run：把 `run_id` 和两边的数发给 QS，由 QS 查它那一边。
- **一个 `known` 的 run 也没比到**（例如全是 `skip`，或者只有 `not_run`）：算失败，输出 `FAIL at least one run with known usage_total was compared (0); nothing was compared, see the skip lines above`。什么也没验证，不能显示通过；看上面的 `skip` 行找原因。
