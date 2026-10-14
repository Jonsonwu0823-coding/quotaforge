# 接 QueryShield

## 接 QS

和 QS 公开仓库的网关模式一致：

- **网络**：外部网络 `qs-qf`，两边都按 external 引用。用前先建：`docker network create qs-qf`，已存在就跳过。`qs-qf` 上只有 `quotaforge`；`qf-mysql`、`qfctl` 不接。
- **地址**：QF 在 `qs-qf` 上的服务名是 `quotaforge`，端口 8080。QS 的 `base_url` 是 `http://quotaforge:8080/v1`，chat 和 embeddings 都用它。
- **Key**：起 QF 之后用 `qfctl` 给 QS 建账户、发一把 Key，交给 QS（`QUERYSHIELD_MODEL_API_KEY`）。PowerShell 脚本把 Key 放进当前窗口的环境变量 `QUERYSHIELD_MODEL_API_KEY`（不显示，不经剪贴板；`-Stop` 清掉），QS 的 `compose-real-demo.ps1` 在网关模式下先读这个变量，有值就不提示；bash 的做法是把 Key 留在当前 shell 的变量里，不要 echo。
- **配置**：`deploy/config.qs-fake.json`（chat 和 embeddings 两个别名都是 `qs-fake-upstream-v1`，上游 `http://fake-upstream:8000/v1`）、`deploy/config.real.json`（`qwen-plus`、`text-embedding-v4`）。都是静态文件，没有要替换的占位符。

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

- **前提**：QS 公开仓库（`https://github.com/Jonsonwu0823-coding/queryshield`）干净地检出在提交 `0918d49fcea7f8ae1141ef3847092ca33c736b72`，目录任意；Docker、Python 3。
  `git clone https://github.com/Jonsonwu0823-coding/queryshield.git ../queryshield && git -C ../queryshield checkout 0918d49fcea7f8ae1141ef3847092ca33c736b72`
- **命令**：`scripts/compose-qs-e2e.sh ../queryshield`（也可以用环境变量 `QS_DIR`）。
  - 用时：约 3 分钟，其中上游超时的场景要等 QF 的 chat 超时 25 秒，接手的场景要等回收约 40 秒；QS 镜像要首次构建的话，再多几分钟。
  - 结果：摘要在 `evidence/compose-qs-e2e-summary.txt`，每个场景一段，段尾报这个场景的项数、失败数和用时，最后一行是总的失败数，全部 `ok` 才以 0 退出。
  - 一个场景的检查失败只计数，后面的场景照跑；跑不下去的错误（取不到摘要、容器操作失败）直接停下。两种情况都会清理干净。
- **不碰本机已有的东西**：
  - QF、QS 各用联调专用的项目名（`qf-e2e-qf`、`qf-e2e-qs`）和镜像标签（`queryshield-app:qf-e2e-0918d49`）。结束时的 `down -v` 只删自己的容器、网络和卷，不会动 `queryshield_*` 的开发数据卷；`qs-qf` 是脚本建的才删。
  - QS 发布到本机的端口默认 18731，避开常用的 8000。
  - 联调镜像 `queryshield-app:qf-e2e-0918d49` 跑完会留着（下次直接用），不要了用 `docker rmi` 删。QS 的镜像缺了才构建；出口代理会替换证书的环境里，预先自己构建好这个标签即可。
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
| 1 正常（账户 `e2e`） | 跑 QS 的整套演示（14 题，12 个 run，48 次调用） | 摘要 `pass`，13 题 pass、1 题 not_applicable | 逐个 run 对账；调用日志和账本一一对应；trace 的三项；幂等键；`qfctl verify`；没有被限流的请求 | 35 项，约 10 秒 |
| 2 跨服务 trace | 场景 1 的 12 个 run | 自己的 span 导出到 QF 的 Jaeger | 两个服务都在；每个 run 的 trace 里两边该有的 span 都在；QF 的每个根 span 挂在 QS 那次尝试的 span 下；QS 的 ERROR span 只有允许的几种 | 4 项 |
| 3 额度耗尽（`e2e-quota`） | 先提一个问题（run A，正常结束），再把账户的额度设成已消费的量（剩余 0），再提一个（run B） | run B：`FAILED`、`model_quota_exhausted`，HTTP 503，聊天 1 次（QS 发请求前就计数），`usage_total` 是 `unknown` | QF 回 429 `quota_exhausted`；run B 在账本里 0 次聊天：被拒的请求不进账本、不调上游（Jaeger 里只有失败的 `reserve`，没有 `upstream`）；已消费 + 预留 ≤ 额度，预留 0 | 25 项，约 15 秒 |
| 4 上游超时（`e2e-timeout`） | 先提一个问题（run C），再暂停 QS 的 `fake-upstream`（`docker pause`）、提一个（run D），之后恢复 | run D：`FAILED`、`upstream_http_error`（QS 把网关回的 504 记成这个码，事件里另记 504 和 `upstream_timeout`），HTTP 502，`usage_total` 是 `unknown` | QF 在 25 到 30 秒之间回 504 `upstream_timeout`，赶在 QS 自己的 30 秒之前；这次调用记 `USAGE_UNKNOWN`、占着预留；超时之后的对账见下面 | 27 项，约 40 秒 |
| 5 两个实例（`e2e-two`） | 起 `quotaforge-b`，和 `quotaforge` 共用网络别名 `quotaforge`；跑整套演示；再做一次“杀掉一个实例、另一个接手” | 演示同场景 1；run E：`FAILED`、`upstream_request_error`（连接被断开，QS 不重试）；run F 正常 | 整套演示照场景 1 逐个 run 对账，两个实例的日志合起来和账本一一对应；E 的调用由 `quotaforge-b` 回收成 `USAGE_UNKNOWN`（`reclaimed_in_flight`）；F 的 span 都来自 `quotaforge-b` | 43 项，约 65 秒 |

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
  - 四，QS 的 ERROR span 的 `queryshield.error_code`，只能是这个场景里 QS 报 `FAILED` 的 run 自己的错误码，或者下面这两个暂停的码；没有错误码的 ERROR 也算失败。每个 `FAILED` 的 run 的 trace 里，至少要有一个带它错误码的 ERROR span。
    - 固定提交里的 QS 把因审批、追问而暂停的那次工具调用标成 ERROR（`approval_required`、`clarification_required`），run 本身是正常暂停。给了演示摘要时，要求至少有一个这样的 span（Q07 的审批）。QS 之后的版本不再这样标，换固定提交时这条改成“没有 ERROR span”。
  - 有 QF 拒绝的请求时（场景 3），这次请求的 QF trace 里有失败的 `reserve`、没有 `upstream`。有 QF 等上游超时的调用时（场景 4），QF 的根 span 和 `upstream` 都以 `upstream_timeout` 失败，它挂在 QS 的一次尝试下，那次尝试记着 504 和 `upstream_timeout`。
  - `--served-by <run_id>=<实例>`（场景 5 的 run F）：这个 run 的 QF 根 span 都来自这个实例（`service.instance.id`）。
  - 场景 1 的预期：12 个 run 的 trace 里 QS 192 个、QF 352 个 span，44 个根 span 都挂在 QS 的尝试下，QS 的 ERROR span 只有 1 个 `approval_required`。
- **单元测试**：`python3 -m unittest discover -s scripts -p 'test_check_qs_runs.py'`（不需要 Docker）。
- **数据从哪读**：
  - QF 的账本（`calls`、`budgets` 表）：`check_qs_runs.py` 按 Compose 的标签找到项目的 `qf-mysql` 容器，在里面用容器内的 secret 文件做只读查询，密码不出容器；
  - QF 的日志（`docker compose logs --no-log-prefix`）；
  - Jaeger 的查询接口。
  - 取出的 trace 只数 span，不打印内容。QS 的 span 只带编号、枚举和数字，不带问题、SQL 和回答。

## 经 QS 的真实演示和对账（本机，会花钱）

QS 的真实演示（14 题、13 个 run）经 QF 调百炼的模型；跑完后把 QS 的 `demo-summary.json` 和 QF 的账本逐个 run 对一遍，看两边的聊天次数和用量是否一致。QS 那边的脚本和参数以 [QS 仓库](https://github.com/Jonsonwu0823-coding/queryshield) 的说明为准。

Windows PowerShell（5.1 起；需要 Docker Desktop 和 Python 3.10 起）：

1. 在**同一个 PowerShell 窗口**里，一行命令先起 QF，再跑 QS 的真实演示。QF 的脚本顺带检查聊天、嵌入和重放（花费在 0.01 元以内），起好后把 QF Key 放进这个窗口的环境变量 `QUERYSHIELD_MODEL_API_KEY`；QS 的脚本在网关模式下读这个变量，不再提示输入，所以不用粘贴任何东西。**QS 的演示调用真实模型，整套约 1 元以内**：

   ```powershell
   Set-Location '<QS 的 queryshield 目录>'; & '<QF 的 quotaforge 目录>\scripts\compose-real.ps1' -Check; .\scripts\compose-real-demo.ps1 -GatewayBaseUrl 'http://quotaforge:8080/v1' -GatewayNetwork 'qs-qf'
   ```

   - 两个脚本必须在同一个窗口里运行：环境变量只在这个窗口里。
   - QF 的脚本失败（`throw`，例如 Docker 没开、没有输入 Key）时整行停下，QS 的演示不会启动。`-Check` 有一项没通过（任何一次请求不是 2xx，或者重放的核对不成立：`x-request-id`、正文和第一次不同，或者已消费变了）也会整行停下：脚本在打印完额度那一行之后报错退出，QS 的演示不会启动；QF 还开着，看完日志再运行 `-Stop`。不带 `-Check` 的启动照旧。
   - QS 的脚本结束时把这个变量恢复成开始时的值，所以变量一直留到 `-Stop`。
   - 跑完 QS 会把摘要拷到 `queryshield/evidence/compose-gateway-<时间>/demo-summary.json`。

2. 回到 QF 目录对账（`-Reconcile` 不碰这个变量）：

   ```powershell
   .\scripts\compose-real.ps1 -Reconcile '<QS 输出的证据目录>\demo-summary.json'
   ```

3. 看完结果后停掉 QF，连数据库的数据一起删掉，并清掉 `QUERYSHIELD_MODEL_API_KEY`：

   ```powershell
   .\scripts\compose-real.ps1 -Stop
   ```

`-Reconcile` 不启动也不清理任何东西（也不碰 `QUERYSHIELD_MODEL_API_KEY`），只要求第 1 步起的 QF 还在运行；不能和 `-Check`、`-Stop` 一起用。它调用 `scripts/check_qs_runs.py`（真实模式），再跑一次 `qfctl verify`。找不到 Python 3.10 或更新的版本时会提示安装（核对脚本只用标准库）。退出码：0 全部对上；1 有对不上的 run、账本校验没通过，或脚本自己报错（QF 没在运行、找不到 Python 等，错误信息在输出里）；2 核对脚本跑不了（摘要不合法、读不了账本）。

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
- **`info`**：不带 `X-Run-Id` 的聊天（例如第 1 步 `-Check` 发的那一次）、带和不带 `X-Run-Id` 的嵌入（建知识索引的那批不带），只按状态数出来，不算失败。
- **账本里有摘要以外的 run**：算失败，提示用 `compose-real.ps1` 重新起一套（账本是新的）再跑。一次对账对应一次演示，不要拿一份摘要去对跑过两次演示的账本。
- 对不上的 run：把 `run_id` 和两边的数发给 QS，由 QS 查它那一边。
- **一个 `known` 的 run 也没比到**（例如全是 `skip`，或者只有 `not_run`）：算失败，输出 `FAIL at least one run with known usage_total was compared (0); nothing was compared, see the skip lines above`。什么也没验证，不能显示通过；看上面的 `skip` 行找原因。
