#!/usr/bin/env bash
# 多个上游的演示和核对（云端和 CI 用）：compose.yaml 加 compose.failover.yaml，两个假上游（同一个镜像）
# qf-fake-upstream（配置里叫 fake-a）和 qf-fake-upstream-b（fake-b）。别名 split 按 9:1 分给 a、b；primary-b 主用 b、
# a 是权重 0 的备用。熔断：连续 5 次失败打开，冷却 5 秒；chat 的上游超时 10 秒。核对：
#   1. 权重：split 发 200 个，两个假上游收到的请求数合计 200，b 在 [5, 40] 之内（二项分布 n=200、p=0.1，
#      两边超出的概率各约 1e-5）；账本里按上游数的和两个假上游的计数一致。
#   2. 换上游：停掉 b（名字查不到，拨号失败）。primary-b 发 8 个都是 200：前 5 个先试 b、连不上、换到 a
#      （日志 failover_from ["fake-b"]），之后 b 的熔断开着，后 3 个直接去备用 a；账本里 8 个的上游都是 fake-a；
#      指标：b 的拨号失败 5 次、换上游 5 次、熔断状态 2（开）。b 起回来之后计数是 0：它一个请求都没收到。
#   3. 恢复：冷却过后，下一个 primary-b 的请求是探测，去 b、200，熔断关上（状态 0）。
#   4. 不换上游：暂停 b（它照样完成 TCP 握手、收下请求，QF 只能当成可能已经发出）。并发发 5 个 primary-b：
#      都是 10 秒后 504 upstream_timeout、用量未知，账本的上游是 fake-b，不换到 a（a 的计数不变）；
#      5 次超时打开熔断，之后冷却之内的 3 个直接去 a、200。
#   5. 恢复：取消暂停，冷却过后探测去 b、200，熔断关上；split 再发 100 个，两个假上游都收到。
#   6. 日志一个调用一行，没有 Key、上游 Key 和请求正文（只数次数）；qfctl verify。
# 任何一项失败都以非 0 退出。用自己的 Compose 项目名 qf-failover：开头和结尾的 down -v 只拆自己的项目。8080 被占
# （例如 quotaforge 项目在跑）就不跑。等待用 wait_for 轮询，截止时间只用来判失败；拨号失败在 Docker Desktop 上
# 要等名字查不到（约 4 秒），所以只看结局，不看用时。摘要写到 evidence/compose-failover-summary.txt：只有检查结果，
# 没有 Key 和正文。jq 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-failover # 优先于 compose.yaml 里的 name

readonly QF=http://127.0.0.1:8080
readonly REQ_MARKER=failover-request-marker-3b9c # 请求正文里的标记
readonly SUMMARY=evidence/compose-failover-summary.txt

work=$(mktemp -d)
chmod 700 "$work"
failures=0
sent=0 # 发给 QF 的聊天请求数：日志里应当正好这么多行 call
mkdir -p evidence
: >"$SUMMARY"

say() { echo "$*" | tee -a "$SUMMARY"; }
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; else say "FAIL $what"; failures=$((failures + 1)); fi
}
dc() { docker compose -f compose.yaml -f compose.failover.yaml "$@"; }
cleanup() {
  local rc=$?
  if [ "$rc" != 0 ] && [ -s "$work/qfctl.err" ]; then
    echo "== qfctl.err" >&2
    cat "$work/qfctl.err" >&2
  fi
  dc unpause qf-fake-upstream-b >/dev/null 2>&1 || true
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

qfctl() { dc run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
wait_for() {
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}
# chat <名字> <别名>：发一个聊天请求，状态码、响应头、正文存进 $work/<名字>.*。
chat() {
  curl -sS -o "$work/$1.body" -D "$work/$1.headers" -w '%{http_code}' -H "Authorization: Bearer $(cat "$work/demo.key")" \
    -H 'Content-Type: application/json' --max-time 30 \
    --data-binary "{\"model\":\"$2\",\"messages\":[{\"role\":\"user\",\"content\":\"$REQ_MARKER\"}]}" \
    "$QF/v1/chat/completions" >"$work/$1.status" || echo 000 >"$work/$1.status"
}
status_of() { cat "$work/$1.status"; }
request_id() { tr -d '\r' <"$work/$1.headers" | awk -F': ' 'tolower($1) == "x-request-id" { print $2 }'; }
code_of() { jq -r '.error.code // "none"' "$work/$1.body" 2>/dev/null || echo none; }
# burst <前缀> <别名> <个数> [并发]：发一批请求，最多 <并发> 个同时在飞；名字是 <前缀>-1 … <前缀>-N。
burst() {
  local prefix=$1 alias=$2 n=$3 parallel=${4:-1} i
  for i in $(seq 1 "$n"); do
    chat "$prefix-$i" "$alias" &
    [ $((i % parallel)) != 0 ] || wait
  done
  wait
  sent=$((sent + n))
}
# statuses <前缀> <个数>：这一批的“状态码/错误码”，去重计数，例如 200/none=8。
statuses() {
  local i
  for i in $(seq 1 "$2"); do echo "$(status_of "$1-$i")/$(code_of "$1-$i")"; done | sort | uniq -c | awk '{ printf "%s%s=%s", sep, $2, $1; sep = "," }'
}
ids() {
  local i
  for i in $(seq 1 "$2"); do request_id "$1-$i"; done
}
# 假上游的镜像没有 curl，用它自己的 -get；QF 的指标端口只在 Compose 的网络里，也经假上游 a 去读。
fake_get() { MSYS_NO_PATHCONV=1 dc exec -T "$1" /fakeupstream -get "http://127.0.0.1:8081$2"; }
chats() { fake_get "$1" /_fake/stats | jq .chat_completions; }
metric() {
  MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get http://quotaforge:9464/metrics |
    awk -v m="$1" '$1 == m { v = $2 } END { print v + 0 }'
}
sql() { MSYS_NO_PATHCONV=1 dc exec -T -e MYSQL_PWD=test-only-root-password qf-mysql mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$1"; }
# upstreams_of <前缀> <个数>：这一批调用在账本里的“上游/状态”，去重计数。
upstreams_of() {
  local list
  list=$(ids "$1" "$2" | awk '{ printf "%s'\''%s'\''", sep, $1; sep = "," }')
  sql "SELECT CONCAT(upstream, '/', status) FROM calls WHERE id IN ($list)" | sort | uniq -c | awk '{ printf "%s%s=%s", sep, $2, $1; sep = "," }'
}
# log_lines <前缀> <个数>：这一批调用的日志行。响应写回之后日志才写，用 logged 等齐。
log_lines() {
  ids "$1" "$2" >"$work/ids"
  dc logs --no-log-prefix quotaforge 2>/dev/null |
    jq -c --rawfile ids "$work/ids" 'select(.msg == "call" and (.request_id as $id | $ids | split("\n") | any(. == $id)))'
}
logged() { [ "$(log_lines "$1" "$2" | wc -l | tr -d ' ')" = "$2" ]; }
# log_of <前缀> <个数> <jq 过滤式>：这一批每个调用的日志行套上过滤式，去重计数。
log_of() {
  wait_for 10 logged "$1" "$2" || true
  log_lines "$1" "$2" | jq -r "$3" | sort | uniq -c | awk '{ printf "%s%s=%s", sep, $2, $1; sep = "," }'
}
# probe：冷却过后 primary-b 的下一个请求是探测、去 b。每秒发一个，直到有一个是 b 回答的（冷却之内的都去 a）。
probe_once() {
  burst probe primary-b 1
  [ "$(log_of probe 1 '.upstream')" = "fake-b=1" ]
}

say "== up"
dc unpause qf-fake-upstream-b >/dev/null 2>&1 || true
dc down -v --remove-orphans >/dev/null 2>&1
if curl -s -o /dev/null --max-time 2 "$QF/"; then
  say "FAIL 127.0.0.1:8080 is already in use by something else (another Compose project of QuotaForge?)"
  exit 1
fi
start=$(date +%s)
dc --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
qfctl account create -name failover -limit 100000000 >/dev/null
qfctl key issue -account failover -models split,primary-b | sed -n 's/^key: //p' >"$work/demo.key"
check "setup: issued a key" [ "$(wc -c <"$work/demo.key")" = 48 ]
check "setup: both circuits start closed" [ "$(metric 'qf_upstream_circuit_state{upstream="fake-a"}')/$(metric 'qf_upstream_circuit_state{upstream="fake-b"}')" = 0/0 ]

say "== 1. weights: split is 9:1 over fake-a and fake-b"
a0=$(chats qf-fake-upstream)
b0=$(chats qf-fake-upstream-b)
burst w split 200 8
a=$(($(chats qf-fake-upstream) - a0))
b=$(($(chats qf-fake-upstream-b) - b0))
say "fake-a got $a, fake-b got $b"
check "weights: all 200 are 200" [ "$(statuses w 200)" = "200/none=200" ]
check "weights: the two fake upstreams got 200 together" [ $((a + b)) = 200 ]
check "weights: fake-b got 5-40 of them (expected 20)" [ "$((b >= 5 && b <= 40))" = 1 ]
check "weights: the ledger has the same split, all settled" [ "$(upstreams_of w 200)" = "fake-a/SETTLED=$a,fake-b/SETTLED=$b" ]

say "== 2. failover: fake-b is stopped (its name does not resolve, the dial fails)"
dc stop qf-fake-upstream-b >/dev/null 2>&1
a0=$(chats qf-fake-upstream)
start=$(date +%s)
burst f primary-b 8
say "8 requests took $(($(date +%s) - start))s"
check "failover: all 8 are 200" [ "$(statuses f 8)" = "200/none=8" ]
check "failover: fake-a answered all 8" [ $(($(chats qf-fake-upstream) - a0)) = 8 ]
check "failover: the ledger has fake-a as the upstream of all 8, settled" [ "$(upstreams_of f 8)" = "fake-a/SETTLED=8" ]
check "failover: the first 5 tried fake-b first (failover_from), the circuit then skipped it" \
  [ "$(log_of f 8 '[.upstream, (.failover_from // [] | join("+"))] | join("<")')" = "fake-a<=3,fake-a<fake-b=5" ]
check "failover: metrics: fake-b failed to connect 5 times, 5 failovers, its circuit is open (2)" \
  [ "$(metric 'qf_upstream_requests_total{endpoint="chat",result="upstream_unavailable",upstream="fake-b"}')/$(metric 'qf_upstream_failovers_total{endpoint="chat",upstream="fake-b"}')/$(metric 'qf_upstream_circuit_state{upstream="fake-b"}')" = 5/5/2 ]
dc start qf-fake-upstream-b >/dev/null 2>&1
wait_for 30 fake_get qf-fake-upstream-b /_fake/stats >/dev/null
check "failover: fake-b got none of them (0 since it came back)" [ "$(chats qf-fake-upstream-b)" = 0 ]

say "== 3. recovery: after the cooldown one probe goes to fake-b"
start=$(date +%s)
check "recovery: a probe went to fake-b and was answered" wait_for 30 probe_once
say "the probe was answered $(($(date +%s) - start))s after fake-b came back"
check "recovery: the probe is 200 and the circuit is closed again (0)" \
  [ "$(status_of probe-1)/$(metric 'qf_upstream_circuit_state{upstream="fake-b"}')" = 200/0 ]

say "== 4. no failover after a request may have been sent: fake-b is paused"
a0=$(chats qf-fake-upstream)
dc pause qf-fake-upstream-b >/dev/null 2>&1
start=$(date +%s)
burst p primary-b 5 5
took=$(($(date +%s) - start))
# 5 次超时刚打开 b 的熔断，冷却只有 5 秒：“冷却之内的 3 个”要紧跟着发，中间不做任何 docker compose exec
# （Windows 上每次约 1 秒）。晚了的话第一个会成为发给还暂停着的 b 的探测，10 秒后 504。两批的检查都放在后面。
burst o primary-b 3
say "5 concurrent requests took ${took}s (chat timeout 10s)"
check "pause: all 5 are 504 upstream_timeout" [ "$(statuses p 5)" = "504/upstream_timeout=5" ]
check "pause: the ledger has them on fake-b with unknown usage" [ "$(upstreams_of p 5)" = "fake-b/USAGE_UNKNOWN=5" ]
check "pause: none of the 5 was moved to fake-a (no failover_from)" [ "$(log_of p 5 '(.failover_from // []) | length')" = "0=5" ]
check "pause: 5 timeouts opened fake-b's circuit (2)" [ "$(metric 'qf_upstream_circuit_state{upstream="fake-b"}')" = 2 ]
check "pause: within the cooldown the next 3 went straight to fake-a and are 200; fake-a's count went up by exactly 3 (the 3, none of the 5)" \
  [ "$(statuses o 3)/$(log_of o 3 '.upstream')/$(($(chats qf-fake-upstream) - a0))" = "200/none=3/fake-a=3/3" ]

say "== 5. recovery after the pause"
dc unpause qf-fake-upstream-b >/dev/null 2>&1
start=$(date +%s)
check "recovery: a probe went to fake-b and was answered" wait_for 30 probe_once
say "the probe was answered $(($(date +%s) - start))s after fake-b was unpaused"
check "recovery: the probe is 200 and the circuit is closed again (0)" \
  [ "$(status_of probe-1)/$(metric 'qf_upstream_circuit_state{upstream="fake-b"}')" = 200/0 ]
# 暂停期间发给 b 的 5 个请求在它的接收缓冲里，取消暂停后 b 可能还会读到、计进 /_fake/stats（0 到 5 个，看 QF 关连接
# 和 b 读的先后）。它们在探测之前就处理完了，所以下面的判法只用探测成功之后的增量。
# b 从第 2 步起回来以来收到的：第 3 步的探测、暂停期间读到的、刚才的探测。
say "fake-b counted $(($(chats qf-fake-upstream-b) - 2)) of the 5 paused requests after it was unpaused (only reported)"
a0=$(chats qf-fake-upstream)
b0=$(chats qf-fake-upstream-b)
burst r split 100 8
a=$(($(chats qf-fake-upstream) - a0))
b=$(($(chats qf-fake-upstream-b) - b0))
say "split again: fake-a got $a, fake-b got $b"
check "recovery: split goes to both again (100 together, each > 0)" [ "$(statuses r 100)/$((a + b))/$((a > 0 && b > 0))" = "200/none=100/100/1" ]

say "== 6. logs and ledger"
dc logs --no-log-prefix quotaforge >"$work/qf.log" 2>&1
check "logs: one call line per request ($sent)" [ "$(jq -c 'select(.msg == "call")' "$work/qf.log" | wc -l | tr -d ' ')" = "$sent" ]
leaks=0
for s in "$(cat "$work/demo.key")" Bearer "$REQ_MARKER" test-only-upstream-key; do
  leaks=$((leaks + $(grep -cF -- "$s" "$work/qf.log" || true)))
done
check "logs: no key, upstream key or request body ($leaks matches)" [ "$leaks" = 0 ]
check "ledger: qfctl verify" [ "$(qfctl verify)" = "ledger ok" ]

say "== result: $failures failed"
[ "$failures" = 0 ]
