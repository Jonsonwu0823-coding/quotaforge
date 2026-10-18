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

要 MySQL 的测试从两个环境变量读连接串（go-sql-driver 的格式）：

| 变量 | 账号 |
|---|---|
| `QUOTAFORGE_TEST_MYSQL_MIGRATE_DSN` | 能建表（迁移、改坏数据做校验测试） |
| `QUOTAFORGE_TEST_MYSQL_DSN` | 只能增删改查（QF 服务用的那种） |

Redis、Kafka 的测试从另两个环境变量读地址：`QUOTAFORGE_TEST_REDIS_ADDR`（`主机:端口`，测试只用自己的键前缀，不清空库）、`QUOTAFORGE_TEST_KAFKA_ADDR`（`主机:端口`，每个测试用自己的主题和消费组，主题自动创建）。本机起一个单节点的 Kafka：

```bash
docker run -d --name qf-test-kafka -p 127.0.0.1:19092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
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
