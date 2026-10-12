# 压测环境（另一台机器上的重跑）

- 这是在一台笔记本上对同一组压测的原样重跑，用来和云端的 `bench/cloud-2026-10-11/` 对照。脚本原样（`scripts/compose-bench.sh`），在 `git archive` 解出的副本里跑（代码和公开前的开发版本 `bf33460` 相同，不在公开仓库的历史里），所以脚本自己读不到提交号。
- 时间（UTC）：2026-10-11 03:18 开始，约 18 分钟。跑的时候本机没有别的测试或 Compose 项目在跑。
- 命令：`scripts/compose-bench.sh <输出目录>`，QF_BENCH_SCENARIOS='' QF_BENCH_RUNS=2 QF_BENCH_WARMUP=5 QF_BENCH_DURATION=30 QF_BENCH_MYSQL_GLOBALS=''（默认值，没有改刷盘设置）
- 机器：笔记本，Intel Core Ultra 9 275HX，24 个逻辑核，31.4 GiB 内存，NVMe 固态盘；Windows 11 Home（10.0.26200）
- Docker：Docker Desktop 29.6.1（WSL2，内核 6.6.114.1-microsoft-standard-WSL2），Compose 5.2.0；Docker 虚拟机 24 个 CPU、15.3 GiB；MySQL 的数据卷在 Docker 虚拟机的虚拟磁盘里
- Go（构建镜像）：1.27.1-alpine
- MySQL：8.4.11；innodb_flush_log_at_trx_commit=1, sync_binlog=1, log_bin=1, innodb_buffer_pool_size=134217728, max_connections=151
- Redis：8.10.2
- 假上游：fake-slow-50（每个请求固定延迟 50 毫秒）；QF 的连接池每个实例 32 个连接
- 闭环：C 个并发各自发完一个再发下一个；预热 5 秒，测 30 秒；每个场景跑 2 遍（全部场景跑完一遍再跑下一遍）
- 单实例的场景只发给 quotaforge，quotaforge-b 停着；两个实例的场景轮流发给两个实例；两个实例在同一台机器上，共用 CPU 和同一个 MySQL
- 复算：`raw/` 是第二遍的逐请求原始数据，用 `qfbench summarize` 重算 13 个场景，和 `summary-run2.json` 逐个相同。原始数据、汇总、证据里没有 Key、请求正文、数据库密码。
