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

### 端到端联调（真的 QS，fake-upstream 模式）

QF 接 QS 的假上游，QS 的 `app` 经 QF 调模型，跑 QS 的整套演示，再用 QF 的账本核对 QF 确实收到了请求。QF 开着追踪（叠上 `compose.observability.yaml`），再核对 QS 发来的 `traceparent`。不调用任何付费模型。

- **前提**：QS 公开仓库（`https://github.com/Jonsonwu0823-coding/queryshield`）干净地检出在提交 `b6e64855b175c5abd6378be49b0027e7331c18a7`，目录任意；Docker、Python 3。
  `git clone https://github.com/Jonsonwu0823-coding/queryshield.git ../queryshield && git -C ../queryshield checkout b6e64855b175c5abd6378be49b0027e7331c18a7`
- **命令**：`scripts/compose-qs-e2e.sh ../queryshield`（也可以用环境变量 `QS_DIR`）。约 1.5 分钟，QS 镜像要首次构建的话再多几分钟。摘要在 `evidence/compose-qs-e2e-summary.txt`，全部 `ok` 才以 0 退出。
- **不碰本机已有的东西**：QF、QS 各用联调专用的项目名（`qf-e2e-qf`、`qf-e2e-qs`）和镜像标签（`queryshield-app:qf-e2e-b6e6485`），所以结束时的 `down -v` 只删自己的卷，不会动 `queryshield_*` 的开发数据卷。QS 发布到本机的端口默认 18731，避开常用的 8000。联调镜像 `queryshield-app:qf-e2e-b6e6485` 跑完会留着（下次直接用，约 370 MB），不要了用 `docker rmi` 删。开始前发现 `qs-qf` 上已有容器、同名项目在运行、端口被占（QF 的 8080、QS 的 18731，后者用 `QUERYSHIELD_HTTP_PORT` 换；Jaeger 界面的 16686）、QS 目录不在固定提交或有改动，就报错退出。QS 的镜像缺了才构建；出口代理会替换证书的环境里，预先自己构建好这个标签即可。
- **追踪也开着**：脚本只起 `quotaforge`，Jaeger（`qf-jaeger`）作为它的依赖跟着起，只在 QF 自己的网络里，不接 `qs-qf`；`qs-qf` 上的成员检查不变。Prometheus 不起。Jaeger 的界面在 `http://127.0.0.1:16686`，联调结束时随 `down -v` 一起删掉。
- **QS 的四个 Compose 文件顺序不能换**（脚本里按这个顺序，再加 `deploy/compose.qs-e2e-images.yaml` 换镜像名）：`compose.yaml`、`compose.fake-upstream.yaml`、`compose.model-gateway.yaml`、`compose.fake-upstream-gateway.yaml`。顺序反了，`app` 会直连假上游、绕过 QF，而且不报错；脚本的核对（QF 账本里的聊天调用数对不上）会失败。
- **预期结果**：QS 的摘要 `status` 为 `pass`，`model_names` 只有 `qs-fake-upstream-v1`，13 题 pass、1 题 not_applicable，已知缺口只有 `Q06:rowset_customers_differ`。QF 账本里这个账户有 33 次聊天、15 次嵌入（12 个 run，每个 run 的聊天次数和用量都和摘要一致），全部已结算，已消费约 11.1 万 tokens，已预留 0；`qfctl verify` 输出 `ledger ok`。固定提交里的 QS，run 里的调用（带 `X-Run-Id` 的聊天和查询嵌入）都带 `Idempotency-Key`，run 外的（建知识索引的嵌入）不带：账本里 `calls` 的 `run_id` 和 `idempotency_key` 要么都有、要么都没有，脚本报出两类的条数（预期 44 条带键、4 条不带，不一致就失败）。假上游不触发重试，所以带键的调用都只执行一次。
- **按 run 核对的规则**（QS 的 `761bc91` 起，摘要每条记录有 `run_id` 和 `usage_total`）：
  - 核对由 `scripts/check_qs_runs.py --e2e` 做，输出每条一行（`ok` 或 `FAIL`），每个 run 一行；联调和经 QS 的真实演示的对账共用这一份代码，真实演示用不带 `--e2e` 的宽松模式（见下一节）。
  - 摘要里每个有 `run_id` 的记录：QF 账本里带这个 `X-Run-Id` 的**聊天**调用，次数等于 `model_call_count`，全部已结算，prompt、completion、total 三个用量数分别等于 `usage_total` 的三个数。`usage_total` 是 `not_run` 的 run，账本里这个 run 的聊天是 0 次。联调是 fake-upstream，所以 `unknown` 也算失败。
  - 账本里带 `X-Run-Id`、摘要里却没有这个 run 的聊天调用，算失败；没有 `X-Run-Id` 的聊天调用也算失败（每次聊天都属于一个 run）。
  - 嵌入不按 run 计数：带 `X-Run-Id` 的都是 200、已结算、用量已知，而且它们的 run 都在摘要里；不带的（第一次检索时建知识索引的那批，4 次）必须都在第一次带 `X-Run-Id` 的嵌入之前。
  - 账户整体：每次调用都有 `X-Client-Request-Id`，全部已结算，已消费等于用量合计、已预留为 0；至少比到 1 个 `usage_total` 为 `known` 的 run（`not_run` 不算，两种模式都要求），至少有 1 次带 `X-Run-Id` 的嵌入（避免空集合上的检查“空真”：嵌入都不带 `X-Run-Id` 时，上面两条嵌入检查没有对象可查）。
  - 单元测试：`python3 -m unittest discover -s scripts -p 'test_check_qs_runs.py'`（不需要 Docker）。
- **`traceparent` 的三项检查**（QS 的 `b6e6485` 起，QS 每个聊天、嵌入请求都带 `traceparent`，摘要每条记录有 `trace_id`；约定见 [可观测](observability.md) 和 QS 仓库的说明）：
  - 规则由 `scripts/check_qs_runs.py --e2e --qf-log <QF 的日志>` 做，只在联调里查，经 QS 的真实演示不开追踪、不查。数据读 QF 的调用日志（`msg` 是 `call`、`account` 是联调账户的行；账本不记 trace），只输出条数，不打印日志行。
  - 一，每一行调用日志都有合法的 `parent_span_id` 和 `trace_id`（小写十六进制、不全是 0）：每个请求都带了合法的 `traceparent`。
  - 二，带 `X-Run-Id` 的调用，`trace_id` 都等于摘要里那个 run 的 `trace_id`；摘要里没有这个 run 或没有它的 `trace_id`，也算失败。
  - 三，不带 `X-Run-Id` 的调用（建知识索引的嵌入），`trace_id` 不和任何 run 的相同；至少要有一次这样的调用，避免空集合上的检查“空真”。
  - 一次运行的预期：48 行调用日志都有 `parent_span_id`；12 个 run 的 44 次调用和摘要一致；run 外 4 次，都不在 run 的 trace 上。
- **数据从哪读**：QF 的账本（`calls` 表）。`check_qs_runs.py` 按 Compose 的标签找到项目的 `qf-mysql` 容器，在里面用容器内的 secret 文件做只读查询，密码不出容器。

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
