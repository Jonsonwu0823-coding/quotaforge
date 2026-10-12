# 压测环境

- 时间（UTC）：2026-10-11 01:23
- QF 提交：fd1ef41e2dbb0ebf8301e7d8cf59f81b5458ac61（公开前的开发版本，不在公开仓库的历史里；工作区有改动）
- 命令：`scripts/compose-bench.sh bench/cloud-2026-10-11/durability/flush-log-2-sync-binlog-0`，QF_BENCH_SCENARIOS='one-1acct-c16 one-1acct-c64' QF_BENCH_RUNS=1 QF_BENCH_WARMUP=5 QF_BENCH_DURATION=30 QF_BENCH_MYSQL_GLOBALS='innodb_flush_log_at_trx_commit = 2, sync_binlog = 0'
- CPU：Intel(R) Xeon(R) Processor @ 2.80GHz，4 个逻辑核
- 内存：15.7 GiB
- 系统：Ubuntu 24.04.5 LTS，内核 6.18.44-fc-v114
- Docker：29.8.2，Compose：5.6.0
- Go（构建镜像）：1.27.1-alpine
- MySQL：8.4.11；innodb_flush_log_at_trx_commit=2, sync_binlog=0, log_bin=1, innodb_buffer_pool_size=134217728, max_connections=151
- Redis：8.10.2
- 假上游：fake-slow-50（每个请求固定延迟 50 毫秒）；QF 的连接池每个实例 32 个连接
- 闭环：C 个并发各自发完一个再发下一个；预热 5 秒，测 30 秒；每个场景跑 1 遍（全部场景跑完一遍再跑下一遍）
- 单实例的场景只发给 quotaforge，quotaforge-b 停着；两个实例的场景轮流发给两个实例；两个实例在同一台机器上，共用 CPU 和同一个 MySQL
- 说明：对照实验，不是正式场景。“工作区有改动”是 README.md 和 scripts/compose-bench.sh（加了 QF_BENCH_MYSQL_GLOBALS，之后提交）；代码和 fd1ef41 相同。只跑了一遍。默认值不改：这是拿持久性换吞吐，QF 不这么做。
