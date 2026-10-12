# 开发和测试

Go 1.27.1。直接依赖只有两个：MySQL 驱动 `github.com/go-sql-driver/mysql` 和 Redis 客户端 `github.com/redis/go-redis/v9`；间接依赖是 `filippo.io/edwards25519`（驱动的 ed25519 认证插件用）、`github.com/cespare/xxhash/v2`、`go.uber.org/atomic`、`golang.org/x/sys`。都 vendor 在 `vendor/` 里，许可证文件随依赖保留。构建时不联网下载模块。

本机跑测试：

- Linux（在项目根目录下运行）：`go vet ./... && go test -race ./...`
- Windows（PowerShell，在项目根目录下运行）：`go vet ./...; go test -race ./...`（`-race` 需要 cgo 和 C 编译器）

要 MySQL 的测试从两个环境变量读连接串（go-sql-driver 的格式）：

| 变量 | 账号 |
|---|---|
| `QUOTAFORGE_TEST_MYSQL_MIGRATE_DSN` | 能建表（迁移、改坏数据做校验测试） |
| `QUOTAFORGE_TEST_MYSQL_DSN` | 只能增删改查（QF 服务用的那种） |

- 没设时这些测试跳过，其余照跑；`QUOTAFORGE_TEST_REQUIRE_DB=1`（CI）时缺了就失败。
- 用库的测试包在开始前拿 MySQL 的命名锁 `quotaforge_test`，`go test ./...` 里几个包依次使用同一个库；迁移测试会删表重建，结束时迁回最新。
- 本机用 Docker 起一个测试库：`scripts/create_test_db.sh` 建库 `quotaforge_test` 和 `qf_migrate`、`qf_app` 两个账号（用法见脚本开头）。
- 随机测试打印种子，`QUOTAFORGE_TEST_SEED=<种子>` 可以复现。
