# 压测环境

- 时间（UTC）：2026-10-11 01:03
- QF 提交：2a6a24b5752bec8fb91dfbbab91e1285085167ea（公开前的开发版本，不在公开仓库的历史里；工作区有改动）
- 命令：`scripts/compose-bench.sh bench/cloud-2026-10-11`，QF_BENCH_SCENARIOS='' QF_BENCH_RUNS=2 QF_BENCH_WARMUP=5 QF_BENCH_DURATION=30 QF_BENCH_FLUSH_LOG=''
- CPU：Intel(R) Xeon(R) Processor @ 2.80GHz，4 个逻辑核
- 内存：15.7 GiB
- 系统：Ubuntu 24.04.5 LTS，内核 6.18.44-fc-v114
- Docker：29.8.2，Compose：5.6.0
- Go（构建镜像）：1.27.1-alpine
- MySQL：8.4.11；innodb_flush_log_at_trx_commit=1, sync_binlog=1, log_bin=1, innodb_buffer_pool_size=134217728, max_connections=151
- Redis：8.10.2
- 假上游：fake-slow-50（每个请求固定延迟 50 毫秒）；QF 的连接池每个实例 32 个连接
- 闭环：C 个并发各自发完一个再发下一个；预热 5 秒，测 30 秒；每个场景跑 2 遍（全部场景跑完一遍再跑下一遍）
- 单实例的场景只发给 quotaforge，quotaforge-b 停着；两个实例的场景轮流发给两个实例；两个实例在同一台机器上，共用 CPU 和同一个 MySQL
- 说明：“工作区有改动”只是 README.md（当时正在写）。当时的脚本用的变量名是 QF_BENCH_FLUSH_LOG，之后改成 QF_BENCH_MYSQL_GLOBALS，这一轮没有设。
