#!/usr/bin/env bash
# 两个 QF 实例的跨进程检查（云端和 CI 用）：compose.yaml 加 compose.two.yaml，两个实例共用 qf-mysql、qf-redis，没有负载均衡器，
# 同时发的请求用 qfbench 的齐发模式（N 个请求在屏障处一起放开，一半发给 A、一半发给 B），从 Compose 网络里面发。
#   1. 超扣：额度只够 10 次预留，同时发 50 个：200 不超过 10，其余都是 429 quota_exhausted，已预留 + 已消费不超过上限。
#   2. 同键：同一个 Idempotency-Key、同样的请求体同时发 20 个：上游账单里正好 1 条，其余是 409 idempotency_in_progress 或重放；
#      之后再发是重放。同键、不同请求体同时发 2 个：一个 200，一个 422 idempotency_key_reused（指纹不同先于一切状态）。
#   3. 共享限流：RPM 为 L=20 的 Key 同时发 60 个：放行数在 [L, L + L·t/60]，t 是前后两次 Redis TIME 之差；被拒的都是 429 rate_limited，
#      Retry-After 是 1 到 ⌈60/L⌉ 的整数；账本里只有放行的那些。
#   4. 杀掉一个实例不重启：A 上 fake-billed-hang（上游已记账、调用 IN_FLIGHT）时 kill -s KILL A；B 的后台回收在上界之后把它标成未知；
#      在 B 上带同一个键重试：回收前 409 idempotency_in_progress，回收后 409 idempotency_outcome_unknown，凭账单对账结清后
#      409 idempotency_response_unavailable；最后再启动 A，能服务。没有心跳，另一个实例也能接手。
#   5. 两个实例同时回收、同时清理：两个实例都停着时插入 300 个超过上界的遗留调用，再同时启动：启动时各跑一轮，同时扫到同一批；
#      每个调用只转换一次，两边合计正好 300。再停下、插入 3000 个过期的幂等记录（分在 50 个账户上）、同时启动，清理也一样。
#      没有出错（也就没有 1213）。两个实例不是同时起来的，每一轮最多做 3 次，“两边都动过”3 次里有一次成立即可。
# 每项后面都 qfctl verify。任何一项失败都以非 0 退出。
# 用自己的 Compose 项目名 qf-two：开头和结尾的 down -v 只拆自己的项目。8080、8081 被占就不跑。
# 时间范围只用 Redis TIME 和 Docker 记的时刻，不用宿主机的钟；等待事件用 wait_for 轮询，不靠睡眠的长短判对错。
# 摘要写到 evidence/compose-two-instances-summary.txt：只有检查结果，没有 Key、密码和正文。
# jq、awk 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-two # 优先于 compose.yaml 里的 name

readonly FILES=(-f compose.yaml -f compose.two.yaml)
# Compose 网络里的地址（qfbench 用），和宿主机上发布的地址（curl 用）
readonly A=http://quotaforge:8080 B=http://quotaforge-b:8080
readonly HOST_A=http://127.0.0.1:8080 HOST_B=http://127.0.0.1:8081
readonly UPSTREAM_KEY=test-only-upstream-key
readonly DB_PASSWORDS=(test-only-root-password test-only-migrate-password test-only-app-password)
# 回收的上界（config.ReclaimAfter，默认 36 秒），后台每 上界/4 一轮
readonly RECLAIM_AFTER_SECONDS=36
readonly SUMMARY=evidence/compose-two-instances-summary.txt

work=$(mktemp -d)
chmod 700 "$work"
failures=0
mkdir -p evidence
: >"$SUMMARY"

say() { echo "$*" | tee -a "$SUMMARY"; }
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; else say "FAIL $what"; failures=$((failures + 1)); fi
}
dc() { docker compose "${FILES[@]}" "$@"; }
cleanup() {
  local rc=$?
  if [ "$rc" != 0 ]; then
    for f in qfctl.err bench.err; do
      [ -s "$work/$f" ] && { echo "== $f" >&2; cat "$work/$f" >&2; }
    done
  fi
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

qfctl() { dc run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
# bench <模式> <参数…> <Key 文件：原始数据写到 stdout
# 镜像在开头已经单独构建：run 只在镜像缺的时候构建，构建的输出会混进 stdout
bench() { dc --profile tools run --rm -T --no-deps qfbench "$@" 2>>"$work/bench.err"; }
account_field() { qfctl usage -account "$1" | awk -F'\t' -v col="$2" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == col) c = i } NR == 2 { print $c }'; }
# new_account <名字> <额度> [qfctl key issue 的其它参数…]：建账户、发 Key，Key 写进 $work/<名字>.key（不打印）
new_account() {
  local name=$1 limit=$2
  shift 2
  qfctl account create -name "$name" -limit "$limit" >/dev/null
  qfctl key issue -account "$name" -models qs-fake-upstream-v1,fake-slow-1000,fake-billed-hang "$@" | sed -n 's/^key: //p' >"$work/$name.key"
  [ "$(wc -c <"$work/$name.key")" -eq 48 ] # 47 个字符加换行
}
verify_ok() { [ "$(qfctl verify)" = "ledger ok" ]; }
# 一次运行里不重复的标记。不用 date 的 %N：那是 GNU date 的扩展，BSD 的 date 不认。
unique() { echo "$(date +%s)-$$-$RANDOM"; }
# 原始数据的列：1 seq，2 start_us，3 target，4 status，5 code，6 latency_us，7 warmup，8 request_id，9 replayed，10 retry_after，11 client_error
rows() { tail -n +3 "$1"; }
# count <文件> [awk 的 -v 参数…] <条件>：满足条件的行数
count() {
  local f=$1
  shift
  rows "$f" | awk -F, "$@" | wc -l | tr -d ' '
}
sql() { MSYS_NO_PATHCONV=1 dc exec -T -e MYSQL_PWD=test-only-root-password qf-mysql mysql -uroot -h127.0.0.1 -N -B quotaforge "$@"; }
# Redis 的当前时间（毫秒），和限流脚本用的是同一个钟
redis_ms() { dc exec -T qf-redis redis-cli TIME | tr -d '\r' | awk 'NR == 1 { s = $1 } NR == 2 { printf "%d\n", s * 1000 + int($1 / 1000) }'; }
fake_bill() { MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get http://127.0.0.1:8081/_fake/bill; }
wait_for() {
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}
# post <名字> <地址> <Key 文件> <请求体> [Idempotency-Key]：响应体、响应头、状态码存进 $work/<名字>.*
post() {
  local name=$1 base=$2 keyfile=$3 body=$4 idem=()
  if [ $# -ge 5 ]; then idem=(-H "Idempotency-Key: $5"); fi
  curl -sS -o "$work/$name.body" -D "$work/$name.headers" -w '%{http_code}' -H "Authorization: Bearer $(cat "$keyfile")" \
    -H 'Content-Type: application/json' ${idem[@]+"${idem[@]}"} --data-binary "$body" "$base/v1/chat/completions" >"$work/$name.status" ||
    echo 000 >"$work/$name.status"
}
# answered <名字> <状态码> <error.code>
answered() { [ "$(cat "$work/$1.status") $(jq -r '.error.code // empty' "$work/$1.body" 2>/dev/null)" = "$2 $3" ]; }

say "== up"
dc down -v --remove-orphans >/dev/null 2>&1
for base in "$HOST_A" "$HOST_B"; do
  if curl -s -o /dev/null --max-time 2 "$base/"; then
    say "FAIL ${base#http://} is already in use by something else"
    exit 1
  fi
done
start=$(date +%s)
dc --progress plain up -d --build --wait
dc --progress plain --profile tools build qfbench
say "up --build --wait and the qfbench image: $(($(date +%s) - start))s"
cid_a=$(dc ps -q quotaforge)
cid_b=$(dc ps -q quotaforge-b)
inspect() { docker inspect "$1" | jq -c ".[0]$2"; }
check "settings: both instances run the same image" [ "$(inspect "$cid_a" .Image)" = "$(inspect "$cid_b" .Image)" ]
for setting in .Config.User .HostConfig.ReadonlyRootfs .HostConfig.CapDrop .HostConfig.SecurityOpt .Config.StopTimeout '.Config.Env | sort'; do
  check "settings: quotaforge-b has the same $setting as quotaforge" [ "$(inspect "$cid_a" "$setting")" = "$(inspect "$cid_b" "$setting")" ]
done
check "settings: quotaforge-b mounts the same config" \
  [ "$(inspect "$cid_a" '.Mounts | map(.Source) | sort')" = "$(inspect "$cid_b" '.Mounts | map(.Source) | sort')" ]
check "settings: quotaforge-b is published only on 127.0.0.1:8081" \
  [ "$(inspect "$cid_b" '.HostConfig.PortBindings | [.[][] | .HostIp + ":" + .HostPort]')" = '["127.0.0.1:8081"]' ]
check "settings: quotaforge-b is only on the default network, not qs-qf" \
  [ "$(inspect "$cid_b" '.NetworkSettings.Networks | keys')" = "[\"${COMPOSE_PROJECT_NAME}_default\"]" ]

say "== 1. overspend across the two instances"
over_body='{"model":"fake-slow-1000","messages":[{"role":"user","content":"overspend"}],"max_tokens":1000}'
estimate=$((${#over_body} + 1000)) # 请求体的字节数加 max_tokens（别名和上游模型同名，改写后长度不变）
over_limit=$((estimate * 10 + estimate / 2))
new_account two-over "$over_limit"
bench burst -target "$A" -target "$B" -body "$over_body" -n 50 <"$work/two-over.key" >"$work/over.csv"
over_ok=$(count "$work/over.csv" '$4 == 200')
say "overspend: $over_ok of 50 got 200 (the quota fits 10 reservations)"
check "overspend: at most 10 got 200" [ "$((over_ok >= 1 && over_ok <= 10))" = 1 ]
check "overspend: all others got 429 quota_exhausted" [ "$(count "$work/over.csv" '$4 == 429 && $5 == "quota_exhausted"')" = $((50 - over_ok)) ]
check "overspend: half went to each instance" [ "$(count "$work/over.csv" '$3 == 0')/$(count "$work/over.csv" '$3 == 1')" = 25/25 ]
check "overspend: reserved + consumed is within the limit" \
  [ $(($(account_field two-over reserved) + $(account_field two-over consumed))) -le "$over_limit" ]
check "overspend: qfctl verify passes" verify_ok

say "== 2. the same Idempotency-Key on both instances"
new_account two-idem 10000000
idem_key="two-same-$(unique)"
idem_body='{"model":"fake-slow-1000","messages":[{"role":"user","content":"same key"}]}'
bill_before=$(fake_bill | wc -l)
bench burst -target "$A" -target "$B" -body "$idem_body" -n 20 -idem-key "$idem_key" <"$work/two-idem.key" >"$work/idem.csv"
first_id=$(rows "$work/idem.csv" | awk -F, '$4 == 200 && $9 == 0 { print $8 }')
check "same key: exactly one request executed" [ "$(count "$work/idem.csv" '$4 == 200 && $9 == 0')" = 1 ]
check "same key: the others are 409 idempotency_in_progress or replays of that call" \
  [ "$(count "$work/idem.csv" -v id="$first_id" '($4 == 409 && $5 == "idempotency_in_progress") || ($4 == 200 && $9 == 1 && $8 == id)')" = 19 ]
say "same key: $(count "$work/idem.csv" '$4 == 409') in progress, $(count "$work/idem.csv" '$9 == 1') replays"
billed_once() { [ "$(fake_bill | wc -l)" -eq $((bill_before + 1)) ] && [ "$(fake_bill | awk -F, -v id="$first_id" '$1 == id' | wc -l)" -eq 1 ]; }
check "same key: the upstream billed exactly one call" billed_once
check "same key: the ledger has one call for the key" \
  [ "$(sql -e "SELECT COUNT(*) FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = 'two-idem' AND c.idempotency_key = '$idem_key'")" = 1 ]
bench burst -target "$B" -body "$idem_body" -n 1 -idem-key "$idem_key" <"$work/two-idem.key" >"$work/idem-after.csv"
check "same key: afterwards it is a replay of the same call" \
  [ "$(count "$work/idem-after.csv" -v id="$first_id" '$4 == 200 && $9 == 1 && $8 == id')" = 1 ]
diff_key="two-diff-$(unique)"
bill_before=$(fake_bill | wc -l)
bench burst -target "$A" -target "$B" -body "$idem_body" -body '{"model":"fake-slow-1000","messages":[{"role":"user","content":"other body"}]}' \
  -n 2 -idem-key "$diff_key" <"$work/two-idem.key" >"$work/diff.csv"
check "same key, different bodies: one 200 and one 422 idempotency_key_reused" \
  [ "$(count "$work/diff.csv" '$4 == 200')/$(count "$work/diff.csv" '$4 == 422 && $5 == "idempotency_key_reused"')" = 1/1 ]
check "same key, different bodies: the upstream billed one call" [ "$(fake_bill | wc -l)" -eq $((bill_before + 1)) ]
check "same key: qfctl verify passes" verify_ok

say "== 3. a shared rate limit"
readonly L=20
new_account two-rate 10000000 -rpm "$L"
t_before=$(redis_ms)
bench burst -target "$A" -target "$B" -body '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"rate"}]}' -n $((3 * L)) \
  <"$work/two-rate.key" >"$work/rate.csv"
t_after=$(redis_ms)
admitted=$(count "$work/rate.csv" '$4 == 200')
upper=$((L + L * (t_after - t_before) / 60000)) # 向下取整：放行数是整数
say "rate: $admitted of $((3 * L)) admitted; bound [$L, $upper] over $((t_after - t_before)) ms of Redis time"
check "rate: admitted within [L, L + L·t/60]" [ "$((admitted >= L && admitted <= upper))" = 1 ]
check "rate: every rejection is 429 rate_limited" [ "$(count "$work/rate.csv" '$4 == 429 && $5 == "rate_limited"')" = $((3 * L - admitted)) ]
check "rate: Retry-After is an integer from 1 to ceil(60/L)" \
  [ "$(count "$work/rate.csv" -v max=$(((60 + L - 1) / L)) '$4 == 429 && !($10 ~ /^[0-9]+$/ && $10 >= 1 && $10 <= max)')" = 0 ]
check "rate: the ledger has only the admitted calls" \
  [ "$(sql -e "SELECT COUNT(*) FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = 'two-rate'")" = "$admitted" ]
check "rate: qfctl verify passes" verify_ok

say "== 4. kill one instance, the other takes over"
new_account two-kill 10000000
kill_key="two-kill-$(unique)"
kill_body='{"model":"fake-billed-hang","messages":[{"role":"user","content":"kill A"}]}'
rows_before=$(fake_bill | wc -l)
post killed "$HOST_A" "$work/two-kill.key" "$kill_body" "$kill_key" &
killed_pid=$!
billed_more() { [ "$(fake_bill | wc -l)" -gt "$rows_before" ]; }
check "kill: the upstream billed the request" wait_for 20 billed_more
killed_id=$(fake_bill | tail -n 1 | cut -d, -f1)
check "kill: the call is IN_FLIGHT" [ "$(account_field two-kill IN_FLIGHT)" = 1 ]
dc kill -s KILL quotaforge >/dev/null 2>&1
wait "$killed_pid" || true
killed_at=$(date +%s)
check "kill: A was killed (exit 137) and stays down" [ "$(docker inspect -f '{{.State.ExitCode}} {{.State.Running}}' "$cid_a")" = "137 false" ]
post retry_before "$HOST_B" "$work/two-kill.key" "$kill_body" "$kill_key"
check "kill: before the reclaim, B answers 409 idempotency_in_progress" answered retry_before 409 idempotency_in_progress
is_unknown() { qfctl unknown -account two-kill | cut -f1 | grep -qx "$killed_id"; }
check "kill: B's reclaim marked the call USAGE_UNKNOWN" wait_for "$((RECLAIM_AFTER_SECONDS + RECLAIM_AFTER_SECONDS / 4 + 30))" is_unknown
say "reclaimed by B $(($(date +%s) - killed_at))s after the kill (bound ${RECLAIM_AFTER_SECONDS}s from the reservation)"
post retry_unknown "$HOST_B" "$work/two-kill.key" "$kill_body" "$kill_key"
check "kill: after the reclaim, B answers 409 idempotency_outcome_unknown" answered retry_unknown 409 idempotency_outcome_unknown
fake_bill | awk -F, -v id="$killed_id" 'NR == 1 || $1 == id' >"$work/killed-bill.csv"
qfctl reconcile -bill - -apply <"$work/killed-bill.csv" >"$work/reconcile.txt"
check "kill: reconcile -apply settled the call on the bill" grep -q "^settled	$killed_id	" "$work/reconcile.txt"
post retry_settled "$HOST_B" "$work/two-kill.key" "$kill_body" "$kill_key"
check "kill: after reconciling, B answers 409 idempotency_response_unavailable" answered retry_settled 409 idempotency_response_unavailable
check "kill: the ledger has one reserve and one settle for the call" \
  [ "$(sql -e "SELECT GROUP_CONCAT(kind ORDER BY kind) FROM ledger WHERE call_id = '$killed_id'")" = "reserve,settle" ]
check "kill: resolved once, by reconcile on the bill" \
  [ "$(sql -e "SELECT CONCAT(status, ' ', resolved_by, ' ', resolution_evidence) FROM calls WHERE id = '$killed_id'")" = "SETTLED reconcile bill" ]
dc logs --no-log-prefix quotaforge-b >"$work/b.log"
check "kill: B marked exactly one call unknown" \
  [ "$(jq -R -s '[split("\n")[] | fromjson? | select(.msg == "background") | .marked_unknown] | add // 0' "$work/b.log")" = 1 ]
check "kill: qfctl verify passes" verify_ok
dc up -d --no-recreate --wait quotaforge >/dev/null 2>&1
post a_again "$HOST_A" "$work/two-kill.key" '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"A is back"}]}'
check "kill: A starts again and serves" answered a_again 200 ""

say "== 5. both instances reclaim, then purge, the same batch"
# 每一轮：两个实例都停着时用 SQL 放好候选，再同时启动；启动时各跑一轮后台循环，同时扫到同一批。
# 回收和清理分两轮做：放在一轮里时，先做完回收的实例会把清理全做了，清理就没有竞争。
# 两个实例不是同时起来的：先起来的那个有时 1–3 秒就把一轮做完了，另一个什么也没做到。所以每一轮最多做 3 次，
# 每次用新的一批数据（编号带上次数）；正确性每一次都核对，“两边都动过”只要求 3 次里有一次成立。
new_account two-race 10000000
# 清理用的 50 个账户（SQL 直接建，只有账户和额度行）：清理按账户在额度行上串行，只有一个账户时，
# 后到的实例总是等前一个删完、再发现已经删掉了，计数全在一边；分在多个账户上，两个实例才会各删各的。
sql <<'SQL'
SET SESSION cte_max_recursion_depth = 100;
INSERT INTO accounts (name, created_at) WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 49)
  SELECT CONCAT('two-race-p-', n), UTC_TIMESTAMP(6) FROM s;
INSERT INTO budgets (account_id, limit_tokens, updated_at) SELECT id, 0, UTC_TIMESTAMP(6) FROM accounts WHERE name LIKE 'two-race-p-%';
SQL
background_sum() { jq -R -s --arg f "$2" '[split("\n")[] | fromjson? | select(.msg == "background") | .[$f]] | add // 0' "$1"; }
# race_start <日志名> <SQL>：停两个实例，执行 SQL，同时启动；之后每个实例的日志从它这次停下的时刻开始取：
# Docker 记的 State.FinishedAt，纳秒，和日志的时间戳同一个钟。
race_start() {
  dc stop quotaforge quotaforge-b >/dev/null 2>&1
  since_a=$(docker inspect -f '{{.State.FinishedAt}}' "$cid_a") since_b=$(docker inspect -f '{{.State.FinishedAt}}' "$cid_b")
  sql <<<"$2"
  dc up -d --no-recreate --wait quotaforge quotaforge-b >/dev/null 2>&1
}
race_logs() {
  dc logs --no-log-prefix --since "$since_a" quotaforge >"$work/quotaforge.$1.log"
  dc logs --no-log-prefix --since "$since_b" quotaforge-b >"$work/quotaforge-b.$1.log"
}
# reclaimed <调用号前缀>、purged <键前缀>：这一批都处理完了
reclaimed() { [ "$(sql -e "SELECT COUNT(*) FROM calls WHERE id LIKE '$1%' AND status IN ('RESERVED', 'IN_FLIGHT')")" = 0 ]; }
purged() { [ "$(sql -e "SELECT COUNT(*) FROM idempotency_keys WHERE idem_key LIKE '$1%'")" = 0 ]; }
# 回收、清理两轮的日志里有几行带这个错误：后台循环的失败写成 <任务>_error（reclaim_error、purge_error、unknown_scan_error；
# 这个部署不开用量事件，没有 outbox_purge）；死锁是驱动的文字 "Error 1213 (40001)"。
# 不能只搜 1213：它会匹配日志里任何一串数字（纳秒时间戳、耗时、计数）。
error_lines() { cat "$work"/*.reclaim*.log "$work"/*.purge*.log | grep -cE -- "$1" || true; }

# reclaim_try <次数>：300 个遗留调用（一半 RESERVED、一半 IN_FLIGHT）；两边都动过时返回 0
reclaim_try() {
  local n=$1 p="qf-race-r$1-" a b
  race_start "reclaim$n" "SET @a = (SELECT id FROM accounts WHERE name = 'two-race');
SET @k = (SELECT id FROM api_keys WHERE account_id = @a);
INSERT INTO calls (id, account_id, key_id, status, purpose, model_alias, upstream, upstream_model, est_input_tokens,
  est_output_tokens, reserved_tokens, idempotency_key, created_at, sent_at)
WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 299)
SELECT CONCAT('$p', LPAD(n, 3, '0')), @a, @k, IF(n % 2 = 0, 'RESERVED', 'IN_FLIGHT'), 'chat', 'qs-fake-upstream-v1',
  'qf-fake', 'fake-chat', 90, 10, 100, CONCAT('race-r$n-', n), UTC_TIMESTAMP(6) - INTERVAL 1 HOUR,
  IF(n % 2 = 0, NULL, UTC_TIMESTAMP(6) - INTERVAL 1 HOUR) FROM s;
INSERT INTO ledger (call_id, kind, reserved_delta, consumed_delta, created_at)
  SELECT id, 'reserve', 100, 0, created_at FROM calls WHERE id LIKE '$p%';
UPDATE budgets SET reserved_tokens = reserved_tokens + 30000, updated_at = UTC_TIMESTAMP(6) WHERE account_id = @a;"
  check "race reclaim (try $n): all stale calls handled" wait_for 30 reclaimed "$p"
  race_logs "reclaim$n"
  check "race reclaim (try $n): RESERVED -> CANCELLED and IN_FLIGHT -> USAGE_UNKNOWN, each by the reclaim" \
    [ "$(sql -e "SELECT GROUP_CONCAT(x ORDER BY x) FROM (SELECT CONCAT(status, ' ', COALESCE(resolved_by, '-'), ' ', COALESCE(error_code, '-'), ' ', COUNT(*)) x
      FROM calls WHERE id LIKE '$p%' GROUP BY status, resolved_by, error_code) t")" = "CANCELLED reclaim reclaimed_before_send 150,USAGE_UNKNOWN - reclaimed_in_flight 150" ]
  check "race reclaim (try $n): one release per cancelled call and nothing else" \
    [ "$(sql -e "SELECT COUNT(*) FROM ledger WHERE call_id LIKE '$p%' AND kind <> 'reserve'")" = 150 ]
  check "race reclaim (try $n): the unknown calls keep their reservations" [ "$(account_field two-race reserved)" = $((15000 * n)) ]
  a=$(($(background_sum "$work/quotaforge.reclaim$n.log" released) + $(background_sum "$work/quotaforge.reclaim$n.log" marked_unknown)))
  b=$(($(background_sum "$work/quotaforge-b.reclaim$n.log" released) + $(background_sum "$work/quotaforge-b.reclaim$n.log" marked_unknown)))
  say "race reclaim (try $n): A moved $a calls, B moved $b"
  check "race reclaim (try $n): the two instances moved 300 calls in total" [ $((a + b)) = 300 ]
  [ "$((a > 0 && b > 0))" = 1 ]
}
# purge_try <次数>：3000 条过期的幂等记录，分在 50 个账户上；记录只要指向存在的调用（外键），借用回收第 1 次的调用
purge_try() {
  local n=$1 a b
  race_start "purge$n" "SET SESSION cte_max_recursion_depth = 3000;
INSERT INTO idempotency_keys (account_id, idem_key, fingerprint, call_id, expires_at)
WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 2999)
SELECT a.id, CONCAT('race-p$n-', LPAD(n, 4, '0')), UNHEX(SHA2(n, 256)), CONCAT('qf-race-r1-', LPAD(n % 300, 3, '0')),
  UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE FROM s JOIN accounts a ON a.name = CONCAT('two-race-p-', n % 50);"
  check "race purge (try $n): all expired records purged" wait_for 30 purged "race-p$n-"
  race_logs "purge$n"
  a=$(background_sum "$work/quotaforge.purge$n.log" purged) b=$(background_sum "$work/quotaforge-b.purge$n.log" purged)
  say "race purge (try $n): A purged $a records, B purged $b"
  check "race purge (try $n): the two instances purged 3000 records in total" [ $((a + b)) = 3000 ]
  [ "$((a > 0 && b > 0))" = 1 ]
}
for round in reclaim purge; do
  both=0
  for n in 1 2 3; do
    if "${round}_try" "$n"; then both=1; break; fi
  done
  check "race $round: both instances took part (in one of up to 3 tries)" [ "$both" = 1 ]
done
reclaim_errors=$(error_lines reclaim_error) purge_errors=$(error_lines purge_error) scan_errors=$(error_lines unknown_scan_error)
deadlocks=$(error_lines 'Error 1213')
check "race: no background errors in any try (lines with reclaim_error $reclaim_errors, purge_error $purge_errors, unknown_scan_error $scan_errors, Error 1213 $deadlocks)" \
  [ $((reclaim_errors + purge_errors + scan_errors + deadlocks)) = 0 ]
check "race: qfctl verify passes" verify_ok

say "== logs"
dc logs --no-log-prefix quotaforge quotaforge-b qfctl qf-mysql >"$work/all.log" 2>&1
no_secrets() {
  local f secret
  for f in "$work"/*.key; do
    secret=$(cat "$f")
    ! grep -qF -- "$secret" "$work/all.log" "$work"/*.csv "$SUMMARY" || return 1
  done
  for secret in "${DB_PASSWORDS[@]}" "$UPSTREAM_KEY" Bearer overspend 'same key' 'kill A'; do
    ! grep -qF -- "$secret" "$work/all.log" "$work"/*.csv || return 1
  done
}
check "logs and raw data: no key, password, Authorization header or request body" no_secrets

say "== $failures failed"
[ "$failures" = 0 ]
