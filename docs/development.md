# 开发和测试

Go 1.27.1。直接依赖：

- MySQL 驱动 `github.com/go-sql-driver/mysql`、Redis 客户端 `github.com/redis/go-redis/v9`；
- 追踪：OpenTelemetry 的 Go SDK（`go.opentelemetry.io/otel`、`otel/trace`、`otel/sdk`，v1.46.0）和它用的 `github.com/go-logr/logr`。导出器是自己写的 OTLP/JSON，不用官方的（它带进 gRPC），见[可观测](observability.md#导出)；
- 指标：`github.com/prometheus/client_golang`（v1.24.1）。
- 用量事件的消息队列：Kafka 的客户端 `github.com/twmb/franz-go`（v1.22.1，BSD-3-Clause），带进 `franz-go/pkg/kmsg`（协议的消息定义，BSD-3-Clause）、`github.com/klauspost/compress`（BSD-3-Clause 加 Apache-2.0、MIT 的部分）和 `github.com/pierrec/lz4/v4`（BSD-3-Clause），纯 Go、没有 cgo。选它：生产者默认幂等（acks=all，客户端自己的重试不在分区里写出重复）、消费组手动提交位点的接口完整（关掉自动提交、取到一批之后不让再均衡打断、按记录提交），维护活跃。没选 `github.com/segmentio/kafka-go`：它的生产者没有幂等，带进的依赖更多（go.sum 多出 x/net、xdg-go 等）、压缩库的版本更旧。正确性不靠生产者幂等：重复由消费者按调用号去重（见[用量事件](ledger.md#用量事件)）。

间接依赖是它们带进来的（`go.mod` 里标着 `// indirect`）。都 vendor 在 `vendor/` 里，许可证文件随依赖保留。构建时不联网下载模块。

本机跑测试：

- Linux（在项目根目录下运行）：`go vet ./... && go test -race ./...`
- Windows（PowerShell，在项目根目录下运行）：`go vet ./...; go test -race ./...`（`-race` 需要 cgo 和 C 编译器）
- macOS：命令和 Linux 一样；`-race` 不需要 C 编译器。

要装什么、怎么装，见下面的[本机环境](#本机环境)。

写脚本时（CI 里各有一步检查，`scripts/` 下的脚本不照做就失败）：

- 变量后面紧跟中文或全角标点要写成 `${变量}`：macOS 自带的 bash 3.2 在 UTF-8 下会把后面那个字的第一个字节算进变量名。
- 在宿主机上调用 `jq` 的脚本，开头定义 `jq() { command jq "$@" | tr -d '\r'; }`（一字不差）：Windows 上 winget 装的 `jq.exe` 输出 CRLF，不去掉的话，拿 `jq` 的输出做字符串比较就对不上。脚本都开着 `pipefail`，`jq` 的退出码照样传出来。不用 `jq -b`：Ubuntu 22.04 带的 jq 1.6 不一定认。容器里跑的 `jq` 不算。

要 MySQL 的测试从两个环境变量读连接串（go-sql-driver 的格式）：

| 变量 | 账号 |
|---|---|
| `QUOTAFORGE_TEST_MYSQL_MIGRATE_DSN` | 能建表（迁移、改坏数据做校验测试） |
| `QUOTAFORGE_TEST_MYSQL_DSN` | 只能增删改查（QF 服务用的那种） |

Redis、Kafka 的测试从另两个环境变量读地址：`QUOTAFORGE_TEST_REDIS_ADDR`（`主机:端口`，测试只用自己的键前缀，不清空库）、`QUOTAFORGE_TEST_KAFKA_ADDR`（`主机:端口`，每个测试用自己的主题和消费组，主题自动创建）。本机起一个单节点的 Kafka（native 镜像启动时偶发段错误，`--restart on-failure:3` 让它自己起来）：

```bash
docker run -d --name qf-test-kafka --restart on-failure:3 -p 127.0.0.1:19092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=3 apache/kafka-native:4.3.1
export QUOTAFORGE_TEST_KAFKA_ADDR=127.0.0.1:19092   # 用完 docker rm -f -v qf-test-kafka
```

- 没设时这些测试跳过，其余照跑；`QUOTAFORGE_TEST_REQUIRE_DB=1`（CI）时缺了就失败（MySQL、Redis、Kafka 都一样）。
- 测试库里[用量事件](ledger.md#用量事件)的开关一直开着（测试包开始时迁移之后就打开）：每个最后校验整个账本的测试都查 outbox 的完整性。测试里直接写库造出来的结清的调用，要么补 outbox 行，要么在测试结束前删掉。
- 用库的测试包在开始前拿 MySQL 的命名锁 `quotaforge_test`，`go test ./...` 里几个包依次使用同一个库；迁移测试会删表重建，结束时迁回最新。
- 本机用 Docker 起一个测试库：`scripts/create_test_db.sh` 建库 `quotaforge_test` 和 `qf_migrate`、`qf_app` 两个账号（用法见脚本开头）。
- 随机测试打印种子，`QUOTAFORGE_TEST_SEED=<种子>` 可以复现。

## 本机环境

`scripts/` 下的演示脚本（包括真实上游的 `compose-real.sh`）都是 bash，同一份不按平台分。跑过的：Linux；macOS 自带的 bash 3.2 和 BSD 命令行工具（`compose-real.sh` 只在 Linux 上用 bash 3.2 和 BSD 的 `date`、`wc` 替身跑过，还没在真的 Mac 上跑过）。Windows 走 Git Bash：脚本照顾到了路径转换，但还没在 Windows 上跑过（10 月上旬在 Git Bash 上跑过的只有冒烟和联调的旧版本）。

### 各要装什么

| | 跑测试 | 跑演示（`scripts/compose-*.sh`） | 跑真实上游的演示（`compose-real.sh`，经 QS） |
|---|---|---|---|
| Docker（Desktop，Linux 上是 Engine 加 Compose 插件） | 带库的测试要（用它起 MySQL、Redis、Kafka；没有就跳过这些测试） | 要 | 要 |
| Git（Windows 上是 Git for Windows，带 Git Bash） | 要 | 要 | 要 |
| `jq`、`curl` | | 要 | 要 |
| Python 3.10 起 | | 只有 QS 联调（`compose-qs-e2e.sh`）要 | 对账（`--reconcile`）要 |
| Go 1.27 | 要 | | |
| PowerShell 7（`pwsh`） | | | 只有 Mac 上跑 QS 的演示要（QS 换掉它的 PowerShell 入口之前，见[经 QS 的真实演示](queryshield.md#经-qs-的真实演示和对账本机会花钱)） |

### 怎么装

每样一句；“实测”是在哪台机器上看到的，“出处”是官方文档（写这一节的环境打不开这些网站，是经搜索核对的，没在这几台机器上一条条试过）。

| | macOS | Windows（Git Bash） | Linux |
|---|---|---|---|
| Docker | Docker Desktop，分 Apple 芯片版和 Intel 版（出处：[Docker Desktop for Mac](https://docs.docker.com/desktop/setup/install/mac-install/)） | Docker Desktop，WSL 2 后端（出处：[Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/)；实测：Windows 11、Docker Desktop 29.6.1） | Docker Engine（出处：[Install Docker Engine](https://docs.docker.com/engine/install/)），加 Compose 插件 `sudo apt-get install docker-compose-plugin`（出处：[Install the Compose plugin](https://docs.docker.com/compose/install/linux/)） |
| Git、bash | 自带 bash 3.2（实测：macOS 26.5.1 的 `/bin/bash` 3.2.57）；git 在 Xcode 命令行工具里，没有就 `xcode-select --install` | 装 Git for Windows（出处：[gitforwindows.org](https://gitforwindows.org/)），用它带的 Git Bash 跑脚本 | 自带 bash；git 用发行版的包，例如 `sudo apt-get install git` |
| `jq` | macOS 26 自带 `/usr/bin/jq`（实测：macOS 26.5.1 上是 jq-1.7.1-apple）；没有就 `brew install jq` | `winget install jqlang.jq`，装完重开 Git Bash（出处：[Download jq](https://jqlang.org/download/)）。它装的 `jq.exe` 输出 CRLF（实测：在 Windows 上看到过），`scripts/` 下的脚本都会去掉，不用另外处理 | `sudo apt-get install jq`（出处同左） |
| `curl` | 自带 `/usr/bin/curl` | Windows 10（1803）起自带 `curl.exe`（出处：[Tar and Curl Come to Windows](https://devblogs.microsoft.com/commandline/tar-and-curl-come-to-windows/)）；Git for Windows 也带一份 | 发行版的包，例如 `sudo apt-get install curl` |
| Python 3.10 起 | 自带的 `python3`（Xcode 命令行工具里的）是 3.9，不够：`brew install python@3.13`，或者 [python.org](https://www.python.org/downloads/macos/) 的安装包（实测：一台 M1 上 PATH 里第一个是 3.13.0） | [python.org](https://www.python.org/downloads/windows/) 的安装包，或者 `winget install -e --id Python.Python.3.13`。应用商店的 `python3` 可能只是占位程序，脚本会跳过它 | 发行版的 `python3`：Ubuntu 22.04、Debian 12 起是 3.10 以上（实测：Ubuntu 24.04 上是 3.12） |
| Go 1.27 | [go.dev/doc/install](https://go.dev/doc/install) 的 `.pkg`（实测：M1 上 go1.27.2） | 同左的 `.msi` | 同左的 `.tar.gz` |
| PowerShell 7 | `brew install --cask powershell`（出处：[Installing PowerShell on macOS](https://learn.microsoft.com/powershell/scripting/install/installing-powershell-on-macos)） | 不用装：QS 的演示用自带的 Windows PowerShell 5.1 | 不用（Linux 上不跑 QS 的真实演示） |

`compose-real.sh` 开头也查一遍：Docker 在运行、`docker compose`（v2）、`jq`、`curl`，`--reconcile` 再查 Python 3.10 起。缺哪个就停下，一句话说缺什么、三种系统上各怎么装；这时还没起栈、没问 Key。

### 换了电脑之后的自检

不花钱，在 `quotaforge` 目录下照抄。第一次要构建镜像，之后几分钟。

```bash
# 1. 工具都在（docker compose 要是 v2；Python 只有对账和 QS 联调要）
docker info --format 'Docker {{.ServerVersion}}' && docker compose version && jq --version && curl --version | head -n 1
for p in python3 python py; do command -v "$p" >/dev/null 2>&1 && "$p" -c 'import sys; print(sys.executable, sys.version.split()[0], "ok" if sys.version_info >= (3, 10) else "too old")'; done
# 2. 单独的冒烟：从空环境起 QF、账本库和假上游，发请求、核对账本，最后拆掉；最后一行是 == result: 0 failed 就是通过
scripts/compose-smoke.sh
```

要跑真实上游的演示的，再用 QF 的假上游把 `compose-real.sh` 走一遍（假上游不校验百炼 Key，这里随便给一个）：

```bash
# 3. 假上游接在 qs-qf 上，名字 fake-bailian
docker compose build qf-fake-upstream
docker network inspect qs-qf >/dev/null 2>&1 || docker network create qs-qf >/dev/null
docker run -d --rm --name fake-bailian --network qs-qf quotaforge-fakeupstream:local -addr 0.0.0.0:8081 >/dev/null
# 4. 起栈并检查，Key 经 eval 交给这个终端；通过时最后一行是 ok
eval "$(echo test-only-key | scripts/compose-real.sh --upstream-base-url http://fake-bailian:8081/v1 --check --stream)" && echo "ok: QUERYSHIELD_MODEL_API_KEY ${#QUERYSHIELD_MODEL_API_KEY} chars"
# 5. 停掉，清掉 Key，删掉假上游
eval "$(scripts/compose-real.sh --stop)"; docker rm -f fake-bailian
```

- 第 4 步通过时，最后一行是 `ok: QUERYSHIELD_MODEL_API_KEY 47 chars`；前面是 `--check`、`--stream` 的结果，都是 `true`。没通过时脚本说哪一项不成立，`ok` 那行不出现。
- 第 2 步和第 4 步都用本机的 8080 端口，别同时跑。
