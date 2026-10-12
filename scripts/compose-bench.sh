#!/usr/bin/env bash
# 压测（docs/scaling.md 的“压测”）：compose.yaml + compose.two.yaml + compose.bench.yaml，假上游固定 50 毫秒（fake-slow-50），
# qfbench 在 Compose 网络里面发（不经宿主机端口转发），闭环：C 个并发，预热 5 秒，测 30 秒。全部场景跑完一遍再跑第二遍。
#   scripts/compose-bench.sh <输出目录>        例如 bench/cloud-2026-10-11
# 环境变量（可选）：
#   QF_BENCH_SCENARIOS  只跑这些场景（空格分隔，名字见下面的 scenarios）；QF_BENCH_RUNS 跑几遍（默认 2）；
#   QF_BENCH_WARMUP、QF_BENCH_DURATION  预热、测量的秒数（默认 5、30）；
#   QF_BENCH_MYSQL_GLOBALS  压测前执行 SET GLOBAL 这些设置（对照实验，例如 'innodb_flush_log_at_trx_commit = 2, sync_binlog = 0'：
#                       拿持久性换吞吐，QF 不这么做）。库是这次压测自己的，结束时连容器一起删掉。
# 输出目录里：ENVIRONMENT.md（机器、各组件版本、提交号、命令）、summary-run<N>.json（每个场景的汇总，qfbench summarize 算的）、
# table-run<N>.tsv（同一份汇总的表）、evidence-run<N>.txt（每个场景测到一半时的 docker stats、MySQL 的锁等待和连接、
# 前后的计数差、QF 的 db_pool 行、QF 到假上游的连接状态）、raw/<场景>.csv.gz（最后一遍的逐请求原始数据）。
# Key 只在临时目录里，不进输出；原始数据里没有 Key 和正文。用自己的 Compose 项目名 qf-bench，结束时 down -v。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-bench
[ $# = 1 ] || { echo "usage: $0 <output directory>" >&2; exit 2; }
out=$1
mkdir -p "$out/raw"
out=$(cd "$out" && pwd)

readonly FILES=(-f compose.yaml -f compose.two.yaml -f compose.bench.yaml)
readonly WARMUP=${QF_BENCH_WARMUP:-5} DURATION=${QF_BENCH_DURATION:-30} RUNS=${QF_BENCH_RUNS:-2}
readonly BODY='{"model":"fake-slow-50","messages":[{"role":"user","content":"x2-bench-body"}],"max_tokens":16}'
readonly A=http://quotaforge:8080 B=http://quotaforge-b:8080 FAKE=http://qf-fake-upstream:8081
readonly BIG=1000000000000 # 额度、TPM 的上限：足够大，但不为 0，每个请求都过三层 Redis 准入
readonly RPM=1000000000
# 场景：名字 目标 Key 并发 [其它 qfbench 参数]
scenarios=(
  "direct-c1 direct none 1" "direct-c16 direct none 16" "direct-c64 direct none 64"
  "one-1acct-c1 one acct1 1" "one-1acct-c16 one acct1 16" "one-1acct-c64 one acct1 64"
  "one-16acct-c16 one acct16 16" "one-16acct-c64 one acct16 64"
  "one-16acct-idem-c64 one acct16 64 -idem-each"
  "two-1acct-c16 two acct1 16" "two-1acct-c64 two acct1 64"
  "two-16acct-c16 two acct16 16" "two-16acct-c64 two acct16 64"
)
wanted=${QF_BENCH_SCENARIOS:-}

work=$(mktemp -d)
chmod 700 "$work"
dc() { docker compose "${FILES[@]}" "$@"; }
cleanup() {
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
qfctl() { dc run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
bench() { dc --profile tools run --rm -T --no-deps qfbench "$@" 2>>"$work/bench.err"; } # 镜像在开头已经构建
sql() { MSYS_NO_PATHCONV=1 dc exec -T -e MYSQL_PWD=test-only-root-password qf-mysql mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$1" 2>/dev/null; }
# 计数：Innodb 的行锁等待、刷日志的 fsync、死锁、COMMIT 的次数和总耗时（皮秒）
counters() {
  sql "SELECT CONCAT_WS(' ',
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_row_lock_waits'),
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_row_lock_time'),
    (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_os_log_fsyncs'),
    (SELECT COUNT FROM information_schema.INNODB_METRICS WHERE NAME = 'lock_deadlocks'),
    (SELECT COUNT_STAR FROM performance_schema.events_statements_summary_global_by_event_name WHERE EVENT_NAME = 'statement/sql/commit'),
    (SELECT SUM_TIMER_WAIT FROM performance_schema.events_statements_summary_global_by_event_name WHERE EVENT_NAME = 'statement/sql/commit'))"
}
# QF 实例到假上游（8081 = 0x1F91）的 TCP 连接，按状态计数：01 ESTABLISHED、06 TIME_WAIT
upstream_sockets() {
  local cid
  cid=$(dc ps -q "$1")
  [ -n "$cid" ] || return 0
  MSYS_NO_PATHCONV=1 docker run --rm --net "container:$cid" --entrypoint sh redis:8 -c 'cat /proc/net/tcp /proc/net/tcp6 2>/dev/null; true' |
    awk 'NR > 1 && $3 ~ /:1F91$/ { n[$4]++ } END { printf "established %d, time_wait %d", n["01"], n["06"] }'
}

echo "== up"
dc down -v --remove-orphans >/dev/null 2>&1
dc --progress quiet up -d --build --wait
dc --progress quiet --profile tools build qfbench
if [ -n "${QF_BENCH_MYSQL_GLOBALS:-}" ]; then
  sql "SET GLOBAL ${QF_BENCH_MYSQL_GLOBALS}"
fi

echo "== accounts and keys"
echo unused >"$work/none.keys"
: >"$work/acct16.keys"
new_key() {
  qfctl account create -name "$1" -limit "$BIG" -rpm "$RPM" -tpm "$BIG" >/dev/null
  qfctl key issue -account "$1" -models fake-slow-50 -rpm "$RPM" -tpm "$BIG" | sed -n 's/^key: //p'
}
new_key bench-1 >"$work/acct1.keys"
for i in $(seq 1 16); do new_key "bench-16-$i" >>"$work/acct16.keys"; done
[ "$(wc -l <"$work/acct16.keys")" = 16 ] && [ "$(wc -c <"$work/acct1.keys")" = 48 ]

{
  echo "# 压测环境"
  echo
  echo "- 时间（UTC）：$(date -u '+%Y-%m-%d %H:%M')"
  echo "- QF 提交：$(git rev-parse HEAD)$(git diff --quiet HEAD -- . && echo '' || echo '（工作区有改动）')"
  echo "- 命令：\`scripts/compose-bench.sh ${1}\`，QF_BENCH_SCENARIOS='${wanted}' QF_BENCH_RUNS=$RUNS QF_BENCH_WARMUP=$WARMUP QF_BENCH_DURATION=$DURATION QF_BENCH_MYSQL_GLOBALS='${QF_BENCH_MYSQL_GLOBALS:-}'"
  echo "- CPU：$(awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo)，$(nproc) 个逻辑核"
  echo "- 内存：$(awk '/^MemTotal/ { printf "%.1f GiB", $2 / 1048576 }' /proc/meminfo)"
  echo "- 系统：$(sed -n 's/^PRETTY_NAME="\(.*\)"$/\1/p' /etc/os-release)，内核 $(uname -r)"
  echo "- Docker：$(docker version --format '{{.Server.Version}}')，Compose：$(docker compose version --short)"
  echo "- Go（构建镜像）：$(sed -n 's/^FROM golang:\([^@ ]*\).*/\1/p' Dockerfile)"
  echo "- MySQL：$(sql 'SELECT VERSION()')；$(sql "SELECT CONCAT('innodb_flush_log_at_trx_commit=', @@innodb_flush_log_at_trx_commit, ', sync_binlog=', @@sync_binlog, ', log_bin=', @@log_bin, ', innodb_buffer_pool_size=', @@innodb_buffer_pool_size, ', max_connections=', @@max_connections)")"
  echo "- Redis：$(dc exec -T qf-redis redis-cli INFO server | tr -d '\r' | sed -n 's/^redis_version://p')"
  echo "- 假上游：fake-slow-50（每个请求固定延迟 50 毫秒）；QF 的连接池每个实例 32 个连接"
  echo "- 闭环：C 个并发各自发完一个再发下一个；预热 ${WARMUP} 秒，测 ${DURATION} 秒；每个场景跑 ${RUNS} 遍（全部场景跑完一遍再跑下一遍）"
  echo "- 单实例的场景只发给 quotaforge，quotaforge-b 停着；两个实例的场景轮流发给两个实例；两个实例在同一台机器上，共用 CPU 和同一个 MySQL"
} >"$out/ENVIRONMENT.md"

for run in $(seq 1 "$RUNS"); do
  echo "== run $run"
  evidence="$out/evidence-run$run.txt"
  : >"$evidence"
  : >"$work/summaries.jsonl"
  for line in "${scenarios[@]}"; do
    read -r name kind keys c extra <<<"$line"
    if [ -n "$wanted" ] && ! grep -qw -- "$name" <<<"$wanted"; then continue; fi
    case $kind in
      direct) targets=(-target "$FAKE") ;;
      one) targets=(-target "$A"); dc stop quotaforge-b >/dev/null 2>&1 ;;
      two) targets=(-target "$A" -target "$B"); dc up -d --no-recreate --wait quotaforge-b >/dev/null 2>&1 ;;
    esac
    raw="$work/$name.csv"
    calls=$(sql 'SELECT COUNT(*) FROM calls')
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    # shellcheck disable=SC2086 # extra 是可选的一个参数
    bench sustain "${targets[@]}" -body "$BODY" -c "$c" -warmup "${WARMUP}s" -duration "${DURATION}s" $extra <"$work/$keys.keys" >"$raw" &
    pid=$!
    sleep "$WARMUP"
    before=$(counters)
    sleep $((DURATION / 2))
    {
      echo "== $name (run $run): calls rows at start $calls"
      echo "-- docker stats (mid-run)"
      docker stats --no-stream --format '{{.Name}} cpu {{.CPUPerc}} mem {{.MemUsage}}' | grep "^${COMPOSE_PROJECT_NAME}-" | sort
      echo "-- MySQL (mid-run): row lock waits now $(sql 'SELECT COUNT(*) FROM performance_schema.data_lock_waits'), threads connected $(sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_connected'"), running $(sql "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_running'")"
      echo "-- MySQL connections of the QF account by client, command and state, and row lock waits (5 samples, 1 s apart)"
      for i in 1 2 3 4 5; do
        echo "  $i: $(sql "SELECT CONCAT(client, ' ', command, ' ', state, ' ', COUNT(*)) FROM (SELECT SUBSTRING_INDEX(host, ':', 1) client, command,
          COALESCE(NULLIF(state, ''), '-') state FROM information_schema.processlist WHERE user = 'qf_app') t GROUP BY client, command, state ORDER BY COUNT(*) DESC" |
          paste -sd ';' -); lock waits $(sql 'SELECT COUNT(*) FROM performance_schema.data_lock_waits')"
        sleep 1
      done
      echo "-- QF -> fake upstream sockets (mid-run): quotaforge $(upstream_sockets quotaforge); quotaforge-b $(upstream_sockets quotaforge-b)"
    } >>"$evidence" || true # 采证据出错不能打断压测（|| 让块里的 set -e 不生效）
    wait "$pid"
    after=$(counters)
    read -r w0 t0 f0 d0 c0 s0 <<<"$before"
    read -r w1 t1 f1 d1 c1 s1 <<<"$after"
    {
      echo "-- counters over the measured period (about ${DURATION}s): row lock waits $((w1 - w0)), row lock wait ms $((t1 - t0)), redo log fsyncs $((f1 - f0)), deadlocks $((d1 - d0)), commits $((c1 - c0)), mean commit $(awk -v s=$((s1 - s0)) -v n=$((c1 - c0)) 'BEGIN { if (n > 0) printf "%.3f ms", s / n / 1e9; else print "-" }')"
      echo "-- QF db_pool lines"
      dc logs --no-log-prefix --since "$since" quotaforge quotaforge-b 2>/dev/null | grep '"msg":"db_pool"' || echo "(none)"
      echo
    } >>"$evidence"
    bench summarize <"$raw" | jq -c --arg s "$name" '{scenario: $s} + .' >>"$work/summaries.jsonl"
    if [ "$run" = "$RUNS" ]; then gzip -9 -c "$raw" >"$out/raw/$name.csv.gz"; fi
    echo "$name: $(tail -n 1 "$work/summaries.jsonl" | jq -r '"ok/s \(.ok_per_second | floor), ok p50 \(.classes.ok.p50_ms) ms, p95 \(.classes.ok.p95_ms) ms, not ok \(.requests - .classes.ok.count)"')"
  done
  jq -s 'map({(.scenario): (del(.scenario))}) | add' "$work/summaries.jsonl" >"$out/summary-run$run.json"
  jq -r -s '(["scenario", "ok_per_second", "ok", "p50_ms", "p95_ms", "p99_ms", "max_ms", "rate_limited", "quota_exhausted", "other_4xx", "5xx", "client_timeout", "client_error"] | @tsv),
    (.[] | [.scenario, (.ok_per_second * 10 | round / 10), .classes.ok.count, .classes.ok.p50_ms, .classes.ok.p95_ms, .classes.ok.p99_ms, .classes.ok.max_ms,
      .classes.rate_limited.count, .classes.quota_exhausted.count, .classes.other_4xx.count, .classes["5xx"].count,
      .classes.client_timeout.count, .classes.client_error.count] | @tsv)' "$work/summaries.jsonl" >"$out/table-run$run.tsv"
done

# 原始数据、汇总、证据里不能有 Key 和正文
leaks=0
while read -r k; do
  if grep -rqF -- "$k" "$out"; then leaks=$((leaks + 1)); fi
  if zcat "$out"/raw/*.csv.gz 2>/dev/null | grep -qF -- "$k"; then leaks=$((leaks + 1)); fi
done < <(cat "$work/acct1.keys" "$work/acct16.keys")
for s in x2-bench-body '"messages"' test-only-; do
  if grep -rqF -- "$s" "$out"/*.json "$out"/*.tsv "$out"/*.txt || zcat "$out"/raw/*.csv.gz 2>/dev/null | grep -qF -- "$s"; then leaks=$((leaks + 1)); fi
done
echo "== leak check: $leaks hits (keys, request body, test-only secrets)"
du -ch "$out"/raw/*.csv.gz 2>/dev/null | tail -n 1
[ "$leaks" = 0 ]
