# 运行和运维

## 用 Docker Compose 运行

`Dockerfile` 有四个目标：`quotaforge`（生产镜像，默认）、`qfctl`（管理工具）、`fakeupstream`（QF 的假上游，只用于测试）和 `qfbench`（压测和齐发工具，只用于测试，见[两个实例](scaling.md#两个实例)、[压测](scaling.md#压测)）。

- 运行镜像都基于 `scratch`：只有一个静态二进制，`quotaforge` 镜像另带 CA 证书（HTTPS 访问百炼要用）。服务镜像里没有 `qfctl`。
- 镜像里没有 shell，以 UID 65532 运行。
- 镜像里设了 `QUOTAFORGE_ADDR=0.0.0.0:8080`，容器里监听所有地址，`docker run -p` 和 Compose 的端口映射都连得上。监听地址只在镜像里设这一处。
- 健康检查是 `quotaforge healthcheck` 子命令：按 `QUOTAFORGE_ADDR` 请求本机的 `/healthz`。只看进程活着，不查数据库：数据库不可用时每个请求都是 503，这本身就是要的拒绝行为。
- `.dockerignore` 是白名单，只有 `go.mod`、`go.sum`、`vendor/`、`cmd/`、`internal/` 进构建上下文。

| 文件 | 用途 |
|---|---|
| `compose.yaml` | 单独运行：`quotaforge`、账本数据库 `qf-mysql`、一次性的迁移服务 `qfctl`、限流用的 `qf-redis`、QF 的假上游 `qf-fake-upstream`，配置是 `deploy/config.compose.json`，不依赖 QS |
| `compose.qs-fake.yaml` | 叠加在主文件上：`quotaforge` 接入外部网络 `qs-qf`，上游是 QS 的假上游 `http://fake-upstream:8000/v1`（配置 `deploy/config.qs-fake.json`） |
| `compose.real.yaml` | 叠加在主文件上：`quotaforge` 接入 `qs-qf`，上游是百炼（配置 `deploy/config.real.json`），地址和 Key 来自当前 shell 的环境变量 |
| `compose.two.yaml` | 叠加在主文件上：第二个实例 `quotaforge-b`（`127.0.0.1:8081`）和压测工具 `qfbench`（只用 `run`），见[两个实例](scaling.md#两个实例) |
| `compose.bench.yaml` | 再叠加在 `compose.two.yaml` 上：两个实例都换成压测配置 `deploy/config.bench.json`，见[压测](scaling.md#压测) |
| `compose.observability.yaml` | 叠加在主文件上：Jaeger、Prometheus，`quotaforge` 打开追踪和指标，见[可观测](observability.md#演示环境) |
| `compose.observability.two.yaml` | 再叠加在 `compose.two.yaml` 之后：`quotaforge-b` 也打开追踪和指标，Prometheus 也抓它 |
| `compose.failover.yaml` | 叠加在主文件上：第二个假上游 `qf-fake-upstream-b`（同一个镜像），`quotaforge` 换成两个上游的配置 `deploy/config.failover.json`、打开指标（只在 Compose 的网络里），见[多个上游](#多个上游) |
| `compose.qs-e2e.yaml` | 只给端到端联调（`scripts/compose-qs-e2e.sh`）用，叠在 `compose.qs-fake.yaml`、`compose.observability.yaml`、`compose.two.yaml`、`compose.observability.two.yaml` 之后：`qf-jaeger` 接进 `qs-qf`（QS 的 span 也发到这个 Jaeger），`quotaforge-b` 也接进 `qs-qf`、带网络别名 `quotaforge`，见[接 QueryShield](queryshield.md) |

- **启动顺序**：`qf-mysql` 健康（初始化完成）→ `qfctl` 迁移并成功退出 → `quotaforge` 启动（版本不对会拒绝启动）；`quotaforge` 也等 `qf-redis` 健康。仍是一条命令 `docker compose up -d --build --wait`。
- **`qf-redis`**：`redis:8`，不持久化（`--save '' --appendonly no`），不发布端口，不接入 `qs-qf`，健康检查是 `redis-cli ping`。不叫 `redis`，理由和 `qf-mysql` 一样。`quotaforge` 经环境变量 `QF_REDIS_URL=redis://qf-redis:6379/0` 连它（三份 `deploy/` 配置的 `redis.url_env` 都指向这个变量）；它只在 Compose 的内部网络上，不设密码。两个覆盖文件不用改，会继承这些设置。
- **`qf-mysql`**：`mysql:8.4`，不发布端口，不接入 `qs-qf`；数据在匿名卷里，`down -v` 时删掉。首次初始化时 `deploy/mysql-init.sh` 建 `qf_migrate`（库内全部权限，只给 `qfctl`）和 `qf_app`（只有增删改查，给 QF）两个账号。
- **数据库密码只有一份**，在三个 secret 文件里：`root_password`、`migrate_dsn`、`app_dsn`。初始化脚本从两个连接串里取用户名和密码；`qfctl` 挂 `migrate_dsn`，QF 挂 `app_dsn`。都不进环境变量和 `docker inspect`。
  - 默认用 `deploy/test-only-db/` 下写明“仅测试”的文件（单独运行、联调）；
  - 设了 `QF_DB_SECRETS_DIR` 就从那个目录读（真实模式由脚本每次随机生成，见下）。
- 管理命令：`docker compose run --rm -T --no-deps qfctl <命令>`。

单独运行的命令见 [README 的快速开始](../README.md#快速开始)。

- 端口只发布在 `127.0.0.1:8080`。
- `stop_grace_period` 是 40 秒，比 QF 的关闭上限（默认 36 秒）加上冲刷 span 的 2 秒长：`docker compose stop` 时，进行中的请求能处理完、收尾记进账本，之后还没导出的 span 也有时间发完。调大上游超时时，它也要跟着大于这两者之和。
- `quotaforge`、`qfctl` 的根文件系统只读。只有 `compose.real.yaml` 放开了 `quotaforge` 的，因为 Compose 要把来自环境变量的 secret 复制进容器。

### 冒烟脚本（bash，Linux 和 CI 用）

```bash
# 在项目根目录下运行
scripts/compose-smoke.sh          # 单独运行的冒烟（Compose 项目名 qf-smoke）
scripts/compose-qs-fake-check.sh  # 联调覆盖文件的连通检查（Compose 项目名 qf-qs-fake-check）
```

- 两个脚本都用自己的 Compose 项目名，开头和结尾的 `down -v` 只拆自己的项目，不会碰默认项目 `quotaforge`（例如正在跑的真实冒烟）。镜像名仍是 `quotaforge:local` 这些，会被重新构建。
- `qfctl` 的标准错误写进脚本的临时目录，脚本失败时打印出来（Key 在标准输出，不在标准错误里）。端到端联调脚本也一样。

- `compose-smoke.sh`：
  - 从空环境 `up --build --wait`，`qfctl` 迁移成功；用 `qfctl` 建账户、现发一把 Key（只在脚本变量里）；
  - 安全设置：`quotaforge` 的 9 项，`qfctl` 镜像里只有 `/qfctl`，`qf-mysql` 不发布端口、只在默认网络上，两个容器的环境变量里没有数据库密码；
  - 依次发聊天、带 `tools` 的聊天、嵌入、无效 Key、不存在的模型，查状态码、`x-request-id`、上游给的 `model` 和 `usage`；
  - 账本：三次调用都结算，已消费等于上游报的用量；把上限改成已消费后，下一次请求 429 `quota_exhausted`、`Retry-After: 25`；
  - 幂等键：同一个键、同样的请求体发两次，第二次 200、`Idempotent-Replayed: true`、`x-request-id` 和正文都和第一次相同，账本只多一次结算；请求体不同 422；慢请求进行中（账本里是 `IN_FLIGHT`）再发 409 `idempotency_in_progress`，结束后重放；不合法的键 400；`qfctl idempotency purge` 能执行；
  - 调用状态查询：成功的调用查到 `SETTLED` 和用量、由网关凭上游的回答结清；别的账户的 Key 查它和查一个不存在的调用号，都是同一个 404 `call_not_found`；
  - 有界结算：`curl --max-time 1` 调 5 秒才回答的上游，客户端先走；按客户端请求号在日志里找到这一行（有 `client_disconnected`、没有 `status`），查到 `SETTLED` 和用量；
  - 优雅退出：慢请求进行中时 `docker compose stop`，请求要拿到 200、结算记进账本，容器以 0 退出；之后没有未结清的调用（`RESERVED`、`IN_FLIGHT`、`USAGE_UNKNOWN` 和已预留都是 0），`qfctl verify` 通过；
  - 真杀进程：重启 QF，发一个 `fake-billed-hang`，等假上游记了账（账本里是 `IN_FLIGHT`），`docker compose kill -s KILL quotaforge`，再启动；等后台回收把它标成 `USAGE_UNKNOWN`（约上界之后）；只取账单里这一行，`qfctl reconcile -bill -` 预演（不改）、`-apply`（结算）、再 `-apply`（`nothing to change`）；查询接口看到由 `reconcile` 凭账单结清，`qfctl verify` 通过；
  - 调用日志：每次调用一行，客户端请求号和状态都对，重放的行有 `replay_of`、没有 `usage`，被杀的那次没有日志行，查询的行是 `lookup`、不算调用；QF、`qfctl`、`qf-mysql` 的日志里没有 Key、数据库密码和正文，`qfctl` 和查询接口的输出里没有 Key 和正文；
  - 限流（放在最后，前面按条数核对的调用日志不受影响）：
    - `qf-redis` 是 `redis:8`，不发布端口、只在默认网络上，运行中的配置是 `save ''`、`appendonly no`；
    - RPM 为 2 的 Key 连发 3 次：第 3 次 429 `rate_limited`，`Retry-After` 是 1 到 30 的整数，日志里是 `key/rpm`，查询接口查不到它，账本里只有前两次；
    - 账户的 RPM 为 2、两把 Key 本身不限：一把用了两次，另一把就被拒，日志里是 `account/rpm`；
    - TPM 的退还：Key 的 TPM 1500，请求带 `max_tokens: 1000`（估算约 1100，脚本核对估算在 1000 到 1200 之间），第一次结算后退回“估算 − 实际”，第二次才放得进；没有退还的话要等约 28 秒。对照组先发用量未知的请求（不退），第二次 429，日志里是 `key/tpm`；
    - `docker compose stop qf-redis`：请求 503 `rate_limiter_unavailable`、不带 `Retry-After`、日志有 Redis 的错误、账本里没有它；`start` 之后等到健康，不重启 QF，第一个请求就是 200；`qfctl verify` 通过，日志里没有这些 Key 和正文；
  - 最后 `down -v`。
- `compose-qs-fake-check.sh`：在 `qs-qf` 上起一个临时替身（QF 的假上游镜像，网络别名 `fake-upstream`，端口 8000），用 `compose.qs-fake.yaml` 起 QF，现发 Key，经别名 `qs-fake-upstream-v1` 各发一次聊天和嵌入；检查只有 `quotaforge` 在 `qs-qf` 上，`qf-mysql`、`qfctl`、`qf-redis` 不在。
  - 账户、Key 和上游都配了远高于用量的限流限额（账户 rpm 6000、tpm 100,000,000；Key rpm 3000、tpm 50,000,000），每个请求都经过准入和修正；最后核对日志里没有被限流、也没有限流器不可用的请求。端到端联调脚本也一样。
  - 替身只在脚本里，不在 Compose 文件里：和 QS 约定：QF 自己的假上游不能叫 `fake-upstream`（这是 QS 的假上游在 `qs-qf` 上的名字）。
  - 这只验证网络和配置是通的，不是 QS 端到端。
- 两个脚本都要 Docker、curl、jq，固定用 `127.0.0.1:8080`。摘要写到 `evidence/`（git 忽略），只有检查结果。任何一项失败都以非 0 退出。

### 代理变量

QF 调上游用 Go 默认的 HTTP 客户端，会读 `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY`。

- Compose 文件不给 `quotaforge` 传这些变量。
- Docker Desktop 如果配了代理，会把它们注入容器。这时 `NO_PROXY` 必须包含 `qf-fake-upstream` 和 `fake-upstream`，否则容器之间的请求会被送去代理。数据库连接不走 HTTP 代理。
- 冒烟脚本和 PowerShell 脚本会检查容器里这几个变量设没设，只显示“设置了”或“没设置”，不显示值，因为值里可能有代理的账号密码。
  - 单独运行的冒烟：设了代理而 `NO_PROXY` 不含 `qf-fake-upstream` 就判失败。

## 环境变量

进程和部署层面的设置走环境变量（同一份配置文件可以给两个实例用），业务配置在配置文件里。值不合法时启动就退出，说明是哪个变量，不打印它的值。

| 变量 | 说明 |
|---|---|
| `QUOTAFORGE_CONFIG` | 配置文件的路径，必填（见下一节） |
| `QUOTAFORGE_ADDR` | 监听地址，默认 `127.0.0.1:8080`；镜像里设成 `0.0.0.0:8080`。健康检查子命令也按它去连 |
| `QUOTAFORGE_METRICS_ADDR` | 指标的监听地址（`主机:端口`，例如 `0.0.0.0:9464`），只有 `GET /metrics`。不设不开 |
| `QUOTAFORGE_OTLP_ENDPOINT` | OTLP/HTTP 收集端的地址（例如 `http://qf-jaeger:4318`），span 发到它的 `/v1/traces`。规则和上游的 base URL 一样：只许 http、https，要有主机名，不能带账号密码、查询串和片段。不设不追踪 |
| `QUOTAFORGE_TRACE_SAMPLE_RATIO` | 0 到 1，没有上游 `traceparent` 时的采样比例，默认 1。要和 `QUOTAFORGE_OTLP_ENDPOINT` 一起设 |
| `QUOTAFORGE_INSTANCE` | span 的资源属性 `service.instance.id`，默认主机名。要和 `QUOTAFORGE_OTLP_ENDPOINT` 一起设 |

追踪和指标的细节见[可观测](observability.md)。

## 配置文件

路径来自环境变量 `QUOTAFORGE_CONFIG`。启动时一次性校验，有错就退出并说明哪一项错了（不打印 Key、连接串和地址）。不认识的字段也算错，防止拼错的字段悄悄走默认值。Key 不在配置里，在数据库里。

| 字段 | 说明 |
|---|---|
| `timeouts.chat_ms` | 调上游 chat 的超时，毫秒。不写或 0 用默认 25000。流式的整个流也受它管（从发出上游请求到读完流），流式没有另外的配置项 |
| `timeouts.embeddings_ms` | 调上游 embeddings 的超时，毫秒。不写或 0 用默认 25000 |
| `upstreams[].name` | 上游名，唯一，最长 255 字节 |
| `upstreams[].base_url` | 上游的 base URL，例如 `http://127.0.0.1:8081/v1`；QF 在后面拼 `/chat/completions`、`/embeddings` |
| `upstreams[].base_url_env` | 从这个环境变量读 base URL（真实上游的地址不进仓库）。和 `base_url` 正好写一个 |
| `upstreams[].api_key_env` | 上游 Key 所在的环境变量名，启动时必须有值。Key 本身不进配置 |
| `upstreams[].api_key_file` | 从这个文件读上游 Key（容器里用 Compose secrets 挂进来，Key 不出现在 `docker inspect` 里）。去掉首尾空白，内容为空就报错。和 `api_key_env` 正好写一个 |
| `upstreams[].rpm`、`upstreams[].tpm` | 这个上游每分钟的请求数、token 数限额（见[限流](rate-limiting.md)），0 到 10¹² 的整数。不写或 0 表示不限 |
| `models[].alias` | 客户端请求里的 `model`，最长 255 字节 |
| `models[].purpose` | `chat` 或 `embeddings`。同一个别名可以分别配给两种用途；同一用途里别名不能重复 |
| `models[].targets` | 这个别名的目标，至少一个，见[多个上游](#多个上游)。同一个别名里（上游，上游模型名）不能重复 |
| `models[].targets[].upstream` | 上游名，必须是 `upstreams[]` 里的 |
| `models[].targets[].upstream_model` | 转发时写进请求体的上游模型名，最长 255 字节 |
| `models[].targets[].weight` | 分流的权重，0 到 1000 的整数，至少一个目标大于 0。0 是备用：权重大于 0 的目标都用不了时才用 |
| `models[].default_max_tokens` | 请求没写 `max_tokens` 时估算输出用的上限。chat 模型必填（正整数），embeddings 模型不许写 |
| `circuit_breaker.failure_threshold` | 熔断：一个上游连续失败几次就打开。所有上游共用。不写或 0 用默认 5 |
| `circuit_breaker.cooldown_ms` | 熔断打开后多久放一个探测请求，毫秒。不写或 0 用默认 30000 |
| `database.dsn_env` | 账本数据库连接串（go-sql-driver 格式，`user:password@tcp(host:port)/db`）所在的环境变量名。用只能增删改查的账号 |
| `database.dsn_file` | 从这个文件读连接串（容器里是 secret `/run/secrets/app_dsn`）。和 `dsn_env` 正好写一个 |
| `redis.url_env` | 限流用的 Redis 地址（`redis://[:密码@]主机:端口/库号`）所在的环境变量名。必填：没有任何限流限额时请求不会用到 Redis，但这一节不能缺 |
| `redis.url_file` | 从这个文件读 Redis 地址。和 `url_env` 正好写一个。地址可能带密码，按机密处理：不进日志和报错 |
| `embedding_cache` | 可选。写了就开[嵌入缓存](#嵌入缓存)，对所有嵌入别名生效；不写就完全不查缓存 |
| `embedding_cache.url_env`、`embedding_cache.url_file` | 缓存用的 Redis 地址，规则和 `redis.url_env`、`redis.url_file` 一样，正好写一个。不要和限流用同一个 Redis（见[嵌入缓存](#嵌入缓存)） |
| `embedding_cache.ttl_ms` | 缓存条目的过期时长，毫秒；写入时再乘以 0.9 到 1.1 之间的随机数。不写或 0 用默认 86400000（24 小时） |

base URL 只接受 http 和 https，必须有主机名，不能带账号密码、查询串和片段。

一个别名只有一个上游时写一个目标，权重写 1：

```json
{"alias": "qwen-plus", "purpose": "chat", "default_max_tokens": 32768,
 "targets": [{"upstream": "bailian", "upstream_model": "qwen-plus", "weight": 1}]}
```

模型上不能再直接写 `upstream`、`upstream_model`：不认识的字段启动就报错，旧写法的配置不会被悄悄换了意思。

服务器的超时（API 和指标两个监听地址一样，不可配置）：

| 项 | 值 | 作用 |
|---|---|---|
| 读请求头 | 5 秒 | 请求头没发完的连接被关掉 |
| 读整个请求（头和体） | 30 秒 | 请求体最大 2 MiB，30 秒容得下约 0.6 Mbit/s 的慢链路；只发一半请求体的客户端最多占住连接 30 秒。这个期限只管读请求：net/http 读完请求体、开始后台读时自己清掉读期限，之后等上游、写响应不受它限制，等上游久的调用和长的流不会被当成客户端断开（`TestServerReadTimeoutSparesLongCalls` 守着；以后升级 Go，这个测试会抓住变化） |
| keep-alive 空闲 | 120 秒 | 空闲的长连接被关掉。比常见的负载均衡空闲时间（60 秒）长，免得负载均衡在 QF 刚关掉的连接上发请求 |
| 写响应 | 不设全局的 | 全局的写期限会掐断长流。流式的写期限设在每次写上：5 秒，不越过 chat 的上游超时加 1 秒（多的 1 秒让到点时已经读到的事件和补的错误事件写得出去，见[流式](api.md#流式)）|

优雅退出（SIGINT、SIGTERM）最多等回收的上界 `max(chat 超时, embeddings 超时) + 3 × 2 秒 + 5 秒`，默认 36 秒；流式的调用也等得完（整个流受 chat 的上游超时管，见[回收](ledger.md#调用状态回收和对账未知用量)）；开着追踪时，之后再用最多 2 秒把还没导出的 span 发完（到点没发完的丢掉，账本在这之前已经收完尾）。容器的停止宽限期要比两者之和长。**调大上游超时时，`stop_grace_period` 也要跟着改**，否则 `docker compose stop` 会在 QF 收尾之前把它杀掉（调用由回收接手，但会多出未知用量）。

## 多个上游

一个别名可以有多个目标（上游、上游模型名、权重），例如灰度（9:1）或主备（1:0）：

```json
"models": [
  {"alias": "split", "purpose": "chat", "default_max_tokens": 64,
   "targets": [{"upstream": "fake-a", "upstream_model": "fake-chat", "weight": 9},
               {"upstream": "fake-b", "upstream_model": "fake-chat", "weight": 1}]},
  {"alias": "primary-b", "purpose": "chat", "default_max_tokens": 64,
   "targets": [{"upstream": "fake-b", "upstream_model": "fake-chat", "weight": 1},
               {"upstream": "fake-a", "upstream_model": "fake-chat", "weight": 0}]}
],
"circuit_breaker": {"failure_threshold": 5, "cooldown_ms": 30000}
```

- **选目标**：每个请求在估算、准入、预留之前选一个目标。跳过熔断开着的上游，在剩下的权重大于 0 的目标里按权重随机选；一个都没有时，按配置顺序用第一个权重 0 的目标（备用）。估算按选中的目标改写后的请求体算（见[额度和账本](ledger.md#额度和账本)）。
- **全部熔断**：所有目标的上游都熔断着时，请求立即得到 502 `upstream_unavailable`（日志有 `circuit_open: true`），在准入和预留之前：账本里没有这次调用，上游没收到请求（见[接口](api.md#额度和限流的错误)）。
- **换上游**：只在“确定没发出”时换：连不上上游（拨号失败：连接被拒、名字查不到）。超时、5xx、发出后断开、3xx、4xx、429、流已经开始，一律不换，照常收尾：这些请求可能已经被上游执行、计费，再发给另一个上游就可能计费两次。
  - 一次调用每个上游最多试一次；所有尝试共用一个期限，从第一次尝试算起，不超过用途的超时。
  - 换之前，新上游先过上游层的限流（见[限流](rate-limiting.md#多个上游)），账本再把这次调用改到新上游（见[账本](ledger.md#换上游)）；任何一步不成就不换，返回上一次的错误。
  - 请求体按新目标的上游模型名从客户端的原始请求体重新改写。发给上游的 `X-Client-Request-Id` 还是这次调用的调用号：只有一个上游可能收到它，按调用对账单不会重复。
- **熔断**：按上游，两个别名用同一个上游时共用它的熔断；每个实例各记各的（见[两个实例](scaling.md#两个实例)）。
  - 关着：每个请求都放行；连续失败 `failure_threshold` 次就打开，中间一次成功就清零。并发时，打开之前已经放行的请求照常发出，所以多试的次数可以超过 `failure_threshold`；上游超时的话，第一个超时回来之前到达的请求都要等满超时。这些请求的结果在打开之后才回来，不再计数。
  - 开着：不发给这个上游；过了 `cooldown_ms`，下一个请求成为探测（半开），探测在飞时别的请求仍当它开着。探测成功就关上，失败就重新打开、重新计时。
  - 失败：连不上、超时、5xx、发出后断开、流中途失败、3xx 和不能用的正文。成功：2xx，以及 401、403 和其它 4xx（上游活着，只是拒绝了这个请求）。上游的 429 不算失败也不算成功（它在限流，是背压，不是故障）。
  - 只有一个目标的别名也受熔断：上游挂了时，熔断打开之后到达的请求快速失败，不再一个个等超时。请求一个接一个来时，就是第 `failure_threshold + 1` 个请求起；并发时见上面“关着”。
  - 没有请求时熔断不会去探测：上游恢复之后，要有一个请求来才会关上。
- 指标、日志和告警见[可观测](observability.md#多个上游和熔断)。

演示（bash，Linux 和 CI 用；两个假上游，约 2 分钟）：

```bash
# 在项目根目录下运行
scripts/compose-failover.sh   # Compose 项目名 qf-failover，叠加 compose.failover.yaml
```

- 权重：`split` 发 200 个，两个假上游收到的请求数合计 200，`fake-b` 在 5 到 40 之间；账本里按上游数的一致。
- 换上游：`docker compose stop` 掉 `fake-b`（名字查不到）。`primary-b` 的 8 个请求都是 200：前 5 个先试 `fake-b`、连不上、换到 `fake-a`（日志的 `failover_from`），之后 `fake-b` 的熔断开着，直接去备用；账本里都是 `fake-a`。
- 恢复：起回 `fake-b`，冷却过后的一个请求是探测，去 `fake-b`，熔断关上。
- 不换：`docker compose pause` 掉 `fake-b`（它照样完成 TCP 握手、收下请求）。5 个并发请求都在 10 秒后 504 `upstream_timeout`、用量未知、账本的上游是 `fake-b`，不换到 `fake-a`；5 次超时打开熔断，之后的请求直接去 `fake-a`。取消暂停、冷却过后探测成功，又回到按权重分流。
- 演示配置的 chat 超时是 10 秒、冷却 5 秒。Docker Desktop 上停着的容器名约 4 秒才查不到，换上游的请求也就慢这么多；脚本只看结局，不看用时。8080 被占（例如别的 QF 项目在跑）就不跑。

## 嵌入缓存

配置里写了 `embedding_cache` 就开（行为见[接口](api.md#嵌入缓存)）：

```json
"embedding_cache": {"url_env": "QF_REDIS_CACHE_URL", "ttl_ms": 86400000}
```

- **单独的 Redis**：缓存要有内存上限、满了要淘汰；限流的 `qf-redis` 不能这样用。淘汰会删掉限流的桶，而桶不在就当满桶（见[限流](rate-limiting.md)），会多放请求；改成只淘汰带过期时间的键也不行，桶也带过期时间。所以缓存放在另一个 Redis：`maxmemory`（演示里 64 MB）加 `allkeys-lru`，不持久化（丢了只是多回源几次）。两个实例共用这一个。
- **QF 启动时不连它**：它不可用只算没命中，请求照常回源，恢复后不用重启 QF（每次读写最多 0.1 秒，拨号也限在 0.1 秒、只试一次）。Docker Desktop 上停着的容器名约 4 秒才查不到，缓存起回来之后可能要等这么久才重新命中。
- **别名的几个目标应当是同一个模型**：缓存按“上游、上游模型名、请求体”精确匹配，不会把一个模型的向量给另一个模型；但同一个别名的几个目标如果是不同的模型，向量本来就不在同一个空间里，按权重分到不同的目标，结果就不能互相比较（不论开不开缓存）。
- **清空或恢复数据库时，同时清空缓存的 Redis**（例如 `docker compose restart qf-redis-cache`：它不持久化）。缓存的键按账户编号隔离，账户编号会从 1 重新开始，旧条目可能落到新的同号账户上；`CACHED` 记的原来那次的调用也不在了（`qfctl verify` 会报 `cached`）。
- 指标和日志见[可观测](observability.md#嵌入缓存)。

演示（bash，Linux 和 CI 用；约 1 分钟，构建镜像另算）：

```bash
# 在项目根目录下运行
scripts/compose-cache.sh   # Compose 项目名 qf-cache，叠加 compose.cache.yaml
```

- 同一个请求发两次：第二次是 `CACHED`、`Cache-Status: quotaforge; hit`，正文逐字节相同，假上游只收到一次，已消费只算一次；另一个账户发同样的请求不命中；带 `Idempotency-Key` 的不走缓存（第二次是重放）；额度用完的账户照样命中。
- 并发：暂停第二个假上游，8 个相同的请求一起发，等到 `qf_embedding_cache_waiting` 是 7 再取消暂停：上游只收到 1 个，其余 7 个 `collapsed`、各记一条 `CACHED`。
- 停掉 `qf-redis-cache`：请求照样 200，日志记 `cache: error`；起回来之后不重启 QF 就又能写、能命中。演示配置的条目 5 秒过期：读出的剩余时间符合 5 秒 ±10%，过期之后再发会重新回源。
- 最后核对日志一个调用一行、没有 Key 和正文，缓存的键只有账户号和哈希，`qfctl verify` 通过。

## qfctl

管理工具，直连数据库，没有 HTTP 管理接口。连接串来自环境变量 `QFCTL_MYSQL_DSN`，或者 `QFCTL_MYSQL_DSN_FILE` 指向的文件，正好设一个。任何输出都不带连接串和密码。

| 命令 | 作用 |
|---|---|
| `qfctl migrate` | 迁移到这个程序的版本（要能建表的账号） |
| `qfctl account create -name NAME -limit TOKENS [-rpm N] [-tpm N]` | 建账户，同时设额度上限和账户的限流限额（不写就是 0，不限）。名字是小写字母、数字、`.`、`_`、`-`，最长 64 |
| `qfctl account set-limit -name NAME -limit TOKENS` | 改上限。已预留、已消费不变 |
| `qfctl account set-rate -name NAME -rpm N -tpm N` | 改账户的限流限额，两个都要写，0 是不限。下一个请求就按新值 |
| `qfctl key issue -account NAME -models A,B [-rpm N] [-tpm N]` | 发一把 Key（限流限额不写就是 0，不限），只在这里输出一次：`id:`、`prefix:`、`key:` 三行。别名不对照 QF 的配置，写错的别名只会让请求返回 404 |
| `qfctl key list -account NAME` | 列出 Key 的 id、前缀、状态、别名和限流限额（不含 Key 本身），吊销时用来找 id。列的顺序：`id prefix status models rpm_limit tpm_limit` |
| `qfctl key revoke -id ID` | 吊销，立即生效。要停用一把 Key 用它，不要把限流限额设成 0（0 是不限） |
| `qfctl key set-rate -id ID -rpm N -tpm N` | 改一把可用的 Key 的限流限额，两个都要写，0 是不限。下一个请求就按新值 |
| `qfctl usage [-account NAME]` | 每个账户的上限、已预留、已消费、剩余（可以为负）、各状态的调用数和账户的限流限额，制表符分隔。列的顺序：`account limit reserved consumed remaining RESERVED SETTLED CANCELLED USAGE_UNKNOWN IN_FLIGHT rpm_limit tpm_limit`（新列都加在最后，前面各列的位置不变，按列号读的脚本不受影响） |
| `qfctl verify` | 校验账本的不变量（见[额度和账本](ledger.md#额度和账本)） |
| `qfctl idempotency purge` | 删掉过期的幂等记录和快照，输出删了多少条；可以重复执行 |
| `qfctl unknown [-account NAME]` | 列出用量未知、还占着预留的调用：调用号、账户、创建时间、接口、别名、上游、上游模型、预留量、错误码。拿这张单去向上游要账单 |
| `qfctl reconcile (-bill FILE \| -log FILE) [-apply]` | 凭证据结清未知用量，默认只预演（见[调用状态、回收和对账](ledger.md#调用状态回收和对账未知用量)） |

限流限额（`-rpm`、`-tpm`）是每分钟的请求数和 token 数，0 到 10¹² 的整数，和配置文件里上游的限额是同一条规则；不合法就是用法错误。

退出码：0 成功，1 失败（包括校验不通过、对账有要人看的调用），2 用法错误（以及对账的证据文件读不了或格式不对）。

Key 是 `qfk_` 加 32 字节 `crypto/rand` 随机数的 base64url（不带填充，43 个字符），共 47 个字符。库里只存 SHA-256 和前 12 个字符（`qfk_` 加 8 个）。前缀 `qfk_` 只用于 Key（调用号是 `qf-`），泄露的 Key 可以按“`qfk_` 加 43 个字符”扫出来。

在 Compose 里：`docker compose run --rm -T qfctl <命令>`（见[用 Docker Compose 运行](#用-docker-compose-运行)）。本机有 Go 时：`QFCTL_MYSQL_DSN=… go run ./cmd/qfctl <命令>`。

## 本机起假上游和 QF

假上游 `cmd/fakeupstream` 不调真实模型，按模型名选行为（见 `internal/fakeupstream` 的包注释，其中 `fake-overuse` 报远超预留的用量，`fake-hold` 挂住直到 `POST /_fake/release`，`fake-billed-hang` 收到就记账、然后一直挂住：上游照样计费、调用方拿不到结果），`GET /_fake/stats` 返回收到的请求数和挂住的请求数。

- **账单**：带 `X-Client-Request-Id`（QF 的调用号）的请求，每个调用号恰好记一行：回答了的（包括 `fake-no-usage`，照算用量、只是不放进响应）记 `yes` 和用量；回错误状态的、`fake-not-json`、被取消的记 `no`；`fake-billed-hang` 在收到时记 `yes`。`GET /_fake/bill` 按 QF 的账单格式导出，可以直接交给 `qfctl reconcile -bill`。
- 镜像里没有 curl：`fakeupstream -get URL` 只 GET 这个地址、把正文打到标准输出，例如 `docker compose exec -T qf-fake-upstream /fakeupstream -get http://127.0.0.1:8081/_fake/bill`。

不用 Docker 时要自己准备一个 MySQL：先用能建表的账号 `qfctl migrate`，再建账户、发 Key；QF 的配置 `config.example.json` 从环境变量 `QUOTAFORGE_MYSQL_DSN` 读只能增删改查的账号的连接串。最省事的是直接用下面的 Compose。

`config.example.json` 还要求 `redis` 一节：限流用的 Redis 地址从环境变量 `QUOTAFORGE_REDIS_URL` 读，**这个变量必须设**。示例配置里的上游、Key 和账户都没有限流限额，这时请求不会用到 Redis（QF 启动时也不连它），但地址不能缺。要试限流，起一个只绑 127.0.0.1、不持久化的 Redis：

```bash
docker run -d --name qf-local-redis -p 127.0.0.1:6379:6379 redis:8 redis-server --save '' --appendonly no
```

```bash
# 在项目根目录下运行
QFCTL_MYSQL_DSN="$MIGRATE_DSN" go run ./cmd/qfctl migrate
QFCTL_MYSQL_DSN="$MIGRATE_DSN" go run ./cmd/qfctl account create -name local -limit 100000
KEY=$(QFCTL_MYSQL_DSN="$MIGRATE_DSN" go run ./cmd/qfctl key issue -account local -models qs-fake-upstream-v1,fake-tool-call | sed -n 's/^key: //p')
go run ./cmd/fakeupstream -addr 127.0.0.1:8081 &
QUOTAFORGE_CONFIG=config.example.json QF_FAKE_UPSTREAM_KEY=test-only-upstream-key QUOTAFORGE_MYSQL_DSN="$APP_DSN" \
  QUOTAFORGE_REDIS_URL=redis://127.0.0.1:6379/0 go run ./cmd/quotaforge
```

用完 `docker rm -f qf-local-redis`。

QF 默认监听 `127.0.0.1:8080`，用环境变量 `QUOTAFORGE_ADDR` 改。镜像里设成了 `0.0.0.0:8080`（见[用 Docker Compose 运行](#用-docker-compose-运行)）。

## 真实上游冒烟（本机）

- 用百炼当上游起 QF 和它的账本数据库，接入 `qs-qf`，用 `qfctl` 给 QS 建账户（上限 1,000,000 tokens）、发一把 QF Key。
- 要真实的百炼 Key，会花一点钱（见下），CI 不跑这一步。
- 带 `-Check` 时，经 QF 发一次短聊天（`qwen-plus`）和一次短嵌入（`text-embedding-v4`），花费在 0.01 元以内；之后显示这个账户的额度（上限、已消费、剩余），以及这把 Key 和账户的限流限额。
- 限流限额远高于演示的用量（真实演示 3 分钟 54 次请求，单次聊天的估算最多约 17,300 tokens）：账户每分钟 600 个请求、2,000,000 tokens，Key 每分钟 300 个请求、1,000,000 tokens。
- `deploy/config.real.json` 里上游 `bailian` 的限额是保守的示例值：每分钟 600 个请求、2,000,000 tokens。**要以百炼控制台上这把 Key 的实际限额为准**改它；百炼的限额实际是按模型的，QF 现在只按上游限。
  - 聊天带一个新生成的 `Idempotency-Key`，再用同一个键发一遍：显示第二次是重放（`Idempotent-Replayed: true`，`x-request-id` 和正文都和第一次相同），已消费没有增加。这演示了“重试不重复计费”，不多花钱。
- `deploy/config.real.json` 里 `qwen-plus` 的 `default_max_tokens` 是 32768。依据（2026-10-08 查百炼文档）：稳定版 `qwen-plus` 最大输出 32,768 个 token、上下文 1,000,000；OpenAI 兼容接口里不写 `max_tokens` 时，默认就是模型的最大输出长度。百炼文档还说 `max_tokens` 将废弃、改用 `max_completion_tokens`；QF 只读 `max_tokens`，客户端改用新字段时 QF 按默认值预留，只会多留、不会少留。
- 百炼 Key 只在当前进程的环境变量里，经 Compose secrets 挂成容器里的文件 `/run/secrets/upstream_api_key`：
  - 不出现在 `docker inspect`、`docker compose config` 和日志里；
  - 不写文件。
- 数据库每次都是新的：每次启动随机生成三个数据库密码，写进系统临时目录下 `quotaforge-real\db` 的三个文件，经 `QF_DB_SECRETS_DIR` 交给 Compose 的 secrets。密码不进环境变量、`docker inspect` 和任何输出；`-Stop` 时连数据库的数据和这个目录一起删掉。
- 上一次没有 `-Stop` 就再次启动时，脚本先提示一句，清掉上一次的容器、数据卷和临时文件，再重新开始（否则旧库里的密码和新生成的对不上）。
- 脚本里的 `docker compose` 都带 `--progress plain --ansi never`：让 docker 的输出是逐行的纯文本，不带进度动画和颜色。
- 脚本把 `$ProgressPreference` 设成 `SilentlyContinue`（只在脚本的作用域里生效）：Windows PowerShell 5.1 的 `Invoke-WebRequest` 默认画进度条，写回窗口顶部几行时会弄乱中文，`-Check` 会用到它。

PowerShell（Windows PowerShell 5.1 起；需要 Docker Desktop）：

```powershell
# 在项目根目录下运行
.\scripts\compose-real.ps1 -Check   # 输入百炼地址和 Key（隐藏输入），起 QF，QF Key 放进这个窗口的环境变量 QUERYSHIELD_MODEL_API_KEY，再各发一次聊天和嵌入，显示额度
.\scripts\compose-real.ps1 -Stop    # 停掉 QF，删掉数据库的数据和临时目录，清掉 QUERYSHIELD_MODEL_API_KEY
```

- 不带 `-Check` 就只启动。
- QF Key 只放进运行脚本的这个 PowerShell 进程的环境变量 `QUERYSHIELD_MODEL_API_KEY`，不显示、不写文件、不经剪贴板（剪贴板谁都能读，还会进 Windows 的剪贴板历史）；脚本结束后变量留着，给同一个窗口里随后运行的 QS 脚本读，`-Stop` 时清掉（不管原来有没有值）。再次启动时新 Key 直接覆盖旧值；`-Reconcile` 不碰它。
- `-UpstreamBaseUrl <地址>` 会显式指定上游、不再询问。可以先用它指向 QF 的假上游，不花钱地走一遍流程：在 `qs-qf` 上起一个假上游容器 `docker run -d --name fake-bailian --network qs-qf quotaforge-fakeupstream:local -addr 0.0.0.0:8081`（先 `docker compose build qf-fake-upstream`），地址填 `http://fake-bailian:8081/v1`，用完 `docker rm -f fake-bailian`。
- 脚本只显示状态码、模型名、usage 和 `x-request-id`。出错时显示状态码和 QF 的错误码，例如 `状态 502，错误码 upstream_auth_failed`（百炼 Key 错了）或 `upstream_unavailable`（地址连不上）。

bash（同样的步骤）：

```bash
# 在项目根目录下运行
docker network inspect qs-qf >/dev/null 2>&1 || docker network create qs-qf >/dev/null
docker compose down -v --remove-orphans   # 清掉上一次的残留
read -rp '百炼 base URL（例如 https://dashscope.aliyuncs.com/compatible-mode/v1）：' QF_UPSTREAM_BASE_URL
read -rsp '百炼 Key（输入不显示）：' QF_UPSTREAM_API_KEY; echo
# 数据库密码：每次随机生成，只写进临时目录的三个 secret 文件
QF_DB_SECRETS_DIR=$(mktemp -d)
pw() { od -An -N16 -tx1 /dev/urandom | tr -d ' \n'; }
printf '%s\n' "$(pw)" >"$QF_DB_SECRETS_DIR/root_password"
printf 'qf_migrate:%s@tcp(qf-mysql:3306)/quotaforge\n' "$(pw)" >"$QF_DB_SECRETS_DIR/migrate_dsn"
printf 'qf_app:%s@tcp(qf-mysql:3306)/quotaforge\n' "$(pw)" >"$QF_DB_SECRETS_DIR/app_dsn"
chmod 755 "$QF_DB_SECRETS_DIR"; chmod 644 "$QF_DB_SECRETS_DIR"/*   # 容器里是 UID 65532
export QF_UPSTREAM_BASE_URL QF_UPSTREAM_API_KEY QF_DB_SECRETS_DIR
# Key 为空时不启动：Compose 会因此不注入 secret，QF 报的是“找不到 Key 文件”，看不出原因。不用 exit，免得关掉终端。
if [ -z "$QF_UPSTREAM_API_KEY" ]; then echo '百炼 Key 为空，没有启动 QF：重新执行上面的 read -rsp 那一行' >&2; else docker compose --progress plain -f compose.yaml -f compose.real.yaml up -d --build --wait quotaforge; fi
unset QF_UPSTREAM_API_KEY
docker compose run --rm -T --no-deps qfctl account create -name queryshield -limit 1000000
QF_KEY=$(docker compose run --rm -T --no-deps qfctl key issue -account queryshield -models qwen-plus,text-embedding-v4 | sed -n 's/^key: //p')   # 交给 QS；不要 echo

# 检查：各发一次聊天和嵌入，只显示状态码、x-request-id、是否重放、模型名和 usage。Key 经进程替换传给 curl，不进命令行。
# 第三个参数是 Idempotency-Key（可选）。
qf() {
  curl -sS -o /tmp/qf-check.json -D - -H @<(printf 'Authorization: Bearer %s\n' "$QF_KEY") ${3:+-H "Idempotency-Key: $3"} \
    -H 'Content-Type: application/json' -d "$2" "http://127.0.0.1:8080$1" |
    tr -d '\r' | awk -v p="$1" 'NR == 1 { s = $2 } tolower($1) == "x-request-id:" { id = $2 } tolower($1) == "idempotent-replayed:" { r = " replayed" } END { print p, s, id r }'
  jq -c '{model, usage}' /tmp/qf-check.json && rm -f /tmp/qf-check.json
}
IDEM_KEY="real-check-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
qf /v1/chat/completions '{"model":"qwen-plus","messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}' "$IDEM_KEY"
qf /v1/embeddings '{"model":"text-embedding-v4","input":["ok"]}'
docker compose run --rm -T --no-deps qfctl usage -account queryshield
# 同一个键再发一遍：应当显示 replayed、x-request-id 和第一次相同，已消费不变
qf /v1/chat/completions '{"model":"qwen-plus","messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}' "$IDEM_KEY"
docker compose run --rm -T --no-deps qfctl usage -account queryshield

# 停止：连数据库的数据和密码文件一起删掉
docker compose down -v && rm -rf "$QF_DB_SECRETS_DIR"
```

- `read -rs` 读进来的 Key 不出现在命令行和 shell 历史里。
- Key 为空（直接按了回车）时，`up` 那一行只提示原因、不启动 QF，也不会关掉当前终端。重新执行 `read -rsp` 那一行，再从 `export` 往下执行。
