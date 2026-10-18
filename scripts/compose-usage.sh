#!/usr/bin/env bash
# 用量事件的演示和核对（云端和 CI 用）：compose.yaml 加 compose.cache.yaml、compose.usage.yaml。结清的调用在同一个事务里
# 写 outbox；QF 的转发循环发到 qf-kafka；qf-usage-consumer 按调用号去重、按账户按天汇总；qfctl usage-reconcile 拿汇总和
# 账本对账（“完全相同”= 退出 0 而且最后一行是 all equal）。核对：
#   1. 一批请求：聊天结算、嵌入结算再命中缓存（CACHED）、上游 401（取消）、没有 usage 的（未知，不发事件），再用假上游的
#      账单对账结清它（发事件）；汇总的各项对（按天的已消费 = 账户的已消费），两边完全相同。
#   2. 停掉 qf-kafka：请求照样 200；事件留在 outbox（对账显示还没发、退出 0），QF 的指标看得到积压和转发的错误；
#      起回来之后补发，完全相同。
#   3. 停掉消费者：事件发出去了、还没消费（对账显示 unconsumed、退出 0）；起回来，从提交过的位点接着读：只有新的，没有重复。
#   4. 重复投递：消费组的位点重置到最早，全部重新读一遍：全是重复，汇总逐行不变，完全相同。
#   5. qfctl verify；日志、主题里的消息、消费者的两张表里没有 Key、请求正文、上游 Key 和数据库密码。
# 任何一项失败都以非 0 退出。用自己的 Compose 项目名 qf-usage：开头和结尾的 down -v 只拆自己的项目。8080 被占就不跑。
# 等待用 wait_for 轮询，截止时间只用来判失败。摘要写到 evidence/compose-usage-summary.txt：只有检查结果，没有 Key 和正文。
# jq 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-usage # 优先于 compose.yaml 里的 name

readonly QF=http://127.0.0.1:8080
readonly REQ_MARKER=usage-request-marker-5c1a # 请求正文里的标记
readonly TOPIC=quotaforge.usage-events.v1
readonly GROUP=quotaforge-usage-daily
# 仅测试的上游 Key 和数据库密码，和 compose.yaml、deploy/test-only-db/ 一致。QF 的 Key 由 qfctl 现发。
readonly SECRETS=(test-only-upstream-key test-only-root-password test-only-migrate-password test-only-app-password)
readonly SUMMARY=evidence/compose-usage-summary.txt

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
dc() { docker compose -f compose.yaml -f compose.cache.yaml -f compose.usage.yaml "$@"; }
cleanup() {
  local rc=$?
  if [ "$rc" != 0 ] && [ -s "$work/qfctl.err" ]; then
    echo "== qfctl.err" >&2
    cat "$work/qfctl.err" >&2
  fi
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
# account <名字>：建一个账户、发一把能用全部别名的 Key，存进 $work/<名字>.key。
account() {
  qfctl account create -name "$1" -limit 100000000 >/dev/null
  qfctl key issue -account "$1" -models usage-chat,usage-rejected,usage-unknown,usage-emb | sed -n 's/^key: //p' >"$work/$1.key"
}
# call <名字> <账户> <路径> <正文>：发一个请求，状态码、响应头存进 $work/<名字>.*。
call() {
  curl -sS -o "$work/$1.body" -D "$work/$1.headers" -w '%{http_code}' -H "Authorization: Bearer $(cat "$work/$2.key")" \
    -H 'Content-Type: application/json' --max-time 40 --data-binary "$4" "$QF$3" >"$work/$1.status" || echo 000 >"$work/$1.status"
}
chat() { call "$1" "$2" /v1/chat/completions "{\"model\":\"$3\",\"messages\":[{\"role\":\"user\",\"content\":\"$REQ_MARKER $1\"}]}"; }
emb() { call "$1" "$2" /v1/embeddings "{\"model\":\"usage-emb\",\"input\":[\"$REQ_MARKER $3\"]}"; }
status_of() { cat "$work/$1.status"; echo; } # curl 的 -w 不带换行
request_id() { tr -d '\r' <"$work/$1.headers" | awk -F': ' 'tolower($1) == "x-request-id" { print $2 }'; }
sql() { MSYS_NO_PATHCONV=1 dc exec -T -e MYSQL_PWD=test-only-root-password qf-mysql mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$1"; }
status_in_ledger() { sql "SELECT status FROM calls WHERE id = '$(request_id "$1")'"; }
# metric <服务:端口> <名字加标签>：读 QF（quotaforge:9464）或消费者（qf-usage-consumer:9465）的一个指标，经假上游去读
# （端口只在 Compose 的网络里，假上游的镜像没有 curl，用它自己的 -get）。
metric() {
  MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get "http://$1/metrics" |
    awk -v m="$2" '$1 == m { v = $2 } END { print v + 0 }'
}
consumer_metric() { metric qf-usage-consumer:9465 "qf_usage_events_consumed_total{result=\"$1\"}"; }
# reconcile：跑 qfctl usage-reconcile，输出存进 $work/reconcile.out，退出码存进 $work/reconcile.rc。
reconcile() {
  local rc=0
  qfctl usage-reconcile >"$work/reconcile.out" || rc=$?
  echo "$rc" >"$work/reconcile.rc"
}
equal() { reconcile && [ "$(cat "$work/reconcile.rc")" = 0 ] && tail -n 1 "$work/reconcile.out" | grep -q '^all equal: '; }
# in_transit <没发的> <没消费的>：对账退出 0，最后一行报的在路上的条数正好是这么多。
in_transit() {
  reconcile && [ "$(cat "$work/reconcile.rc")" = 0 ] &&
    tail -n 1 "$work/reconcile.out" | grep -q "^in transit: $1 unsent, $2 not consumed yet"
}
tools() { MSYS_NO_PATHCONV=1 dc run --rm -T qf-kafka-tools "$@" 2>>"$work/tools.err"; }
daily() { sql 'SELECT account_id, day, purpose, settled_calls, prompt_tokens, completion_tokens, total_tokens, cached_calls, cached_tokens, cancelled_calls FROM usage_daily ORDER BY 1, 2, 3'; }
events_seen() { sql 'SELECT COUNT(*) FROM usage_seen'; }

say "== up"
dc down -v --remove-orphans >/dev/null 2>&1
if curl -s -o /dev/null --max-time 2 "$QF/"; then
  say "FAIL 127.0.0.1:8080 is already in use by something else (another Compose project of QuotaForge?)"
  exit 1
fi
start_all=$(date +%s)
start=$(date +%s)
dc --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
account usage-a
account usage-b
check "setup: issued two keys" [ "$(cat "$work"/usage-*.key | wc -c)" = 96 ]
check "setup: QF turned usage events on at startup" [ "$(sql 'SELECT enabled_at IS NOT NULL FROM usage_events')" = 1 ]
check "setup: qf-kafka is the native image, pinned" [ "$(docker inspect -f '{{.Config.Image}}' "$(dc ps -q qf-kafka)")" = apache/kafka-native:4.3.1 ]

say "== 1. a batch: settled, cached, rejected, unknown then reconciled"
for i in 1 2 3; do chat "a-chat-$i" usage-a usage-chat; done
chat b-chat-1 usage-b usage-chat
chat b-chat-2 usage-b usage-chat
emb a-emb-1 usage-a one
emb a-emb-2 usage-a one
chat a-rejected usage-a usage-rejected
chat a-unknown usage-a usage-unknown
statuses=$(for n in a-chat-1 a-chat-2 a-chat-3 b-chat-1 b-chat-2 a-emb-1 a-emb-2 a-unknown a-rejected; do status_of "$n"; done | paste -sd, -)
check "batch: the chats and embeddings are 200, the rejected one 502 ($statuses)" [ "$statuses" = "200,200,200,200,200,200,200,200,502" ]
check "batch: ledger: SETTLED, CACHED, CANCELLED, USAGE_UNKNOWN" \
  [ "$(status_in_ledger a-emb-1)/$(status_in_ledger a-emb-2)/$(status_in_ledger a-rejected)/$(status_in_ledger a-unknown)" = SETTLED/CACHED/CANCELLED/USAGE_UNKNOWN ]
check "batch: the unknown call has no event" [ "$(sql "SELECT COUNT(*) FROM outbox WHERE call_id = '$(request_id a-unknown)'")" = 0 ]
check "batch: consumer and ledger equal" wait_for 60 equal
MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get http://127.0.0.1:8081/_fake/bill >"$work/bill.csv"
{ head -n 1 "$work/bill.csv"; grep "^$(request_id a-unknown)," "$work/bill.csv"; } >"$work/unknown-bill.csv"
check "batch: qfctl reconcile -bill settles the unknown call" \
  grep -q '^applied: 1 settled' <(qfctl reconcile -bill - -apply <"$work/unknown-bill.csv")
check "batch: the reconciled call has its event" [ "$(sql "SELECT COUNT(*) FROM outbox WHERE call_id = '$(request_id a-unknown)'")" = 1 ]
check "batch: consumer and ledger equal after reconciling" wait_for 60 equal
a_id=$(sql "SELECT id FROM accounts WHERE name = 'usage-a'")
row() { sql "SELECT CONCAT_WS(' ', settled_calls, cached_calls, cancelled_calls) FROM usage_daily WHERE account_id = $a_id AND purpose = '$1'"; }
check "batch: usage-a's chat row: 4 settled (with the reconciled one), 0 cached, 1 cancelled" [ "$(row chat)" = "4 0 1" ]
check "batch: usage-a's embeddings row: 1 settled, 1 cached, 0 cancelled" [ "$(row embeddings)" = "1 1 0" ]
check "batch: the day's settled total of usage-a is its consumed tokens" \
  [ "$(sql "SELECT SUM(total_tokens) FROM usage_daily WHERE account_id = $a_id")" = "$(sql "SELECT consumed_tokens FROM budgets WHERE account_id = $a_id")" ]
check "batch: the cache hit saved the first call's tokens" \
  [ "$(sql "SELECT cached_tokens FROM usage_daily WHERE account_id = $a_id AND purpose = 'embeddings'")" = "$(jq .usage.total_tokens "$work/a-emb-1.body")" ]

say "== 2. qf-kafka stopped: requests still 200, the events wait in the outbox"
dc stop qf-kafka >/dev/null 2>&1
for i in 1 2 3; do chat "down-$i" usage-a usage-chat; done
check "kafka down: requests are 200" [ "$(status_of down-1)/$(status_of down-2)/$(status_of down-3)" = 200/200/200 ]
check "kafka down: usage-reconcile shows 3 unsent and exits 0" wait_for 30 in_transit 3 0
forward_failing() { [ "$(metric quotaforge:9464 'qf_background_errors_total{task="usage_forward"}')" -ge 1 ] && [ "$(metric quotaforge:9464 qf_usage_outbox_unsent)" = 3 ]; }
check "kafka down: QF's metrics show the backlog (3) and forwarding errors" wait_for 60 forward_failing
dc start qf-kafka >/dev/null 2>&1
check "kafka back: the backlog is sent and consumed; equal" wait_for 120 equal
check "kafka back: QF's backlog metric is 0" wait_for 30 [ "$(metric quotaforge:9464 qf_usage_outbox_unsent)" = 0 ]

say "== 3. consumer stopped: the events are sent, not consumed; it resumes from its committed offsets"
dc stop qf-usage-consumer >/dev/null 2>&1
chat paused-1 usage-b usage-chat
chat paused-2 usage-b usage-chat
check "consumer down: usage-reconcile shows 2 not consumed yet and exits 0" wait_for 30 in_transit 0 2
dc start qf-usage-consumer >/dev/null 2>&1
check "consumer back: equal" wait_for 60 equal
check "consumer back: it read only the 2 new events, no duplicates" \
  [ "$(consumer_metric applied)/$(consumer_metric duplicate)" = 2/0 ]

say "== 4. redelivery: the group's offsets reset to the earliest"
daily >"$work/daily-before"
total=$(events_seen)
dc stop qf-usage-consumer >/dev/null 2>&1
tools /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server qf-kafka:9092 --group "$GROUP" --topic "$TOPIC" \
  --reset-offsets --to-earliest --execute >"$work/reset.out"
check "redelivery: the offsets are reset to 0" [ "$(awk -v g="$GROUP" '$1 == g { print $4 }' "$work/reset.out" | sort -u)" = 0 ]
dc start qf-usage-consumer >/dev/null 2>&1
all_again() { [ "$(consumer_metric duplicate)" -ge "$total" ]; }
check "redelivery: all $total events read again, each a duplicate" wait_for 60 all_again
check "redelivery: nothing applied twice" [ "$(consumer_metric applied)" = 0 ]
daily >"$work/daily-after"
check "redelivery: the daily totals are unchanged, row by row" cmp -s "$work/daily-before" "$work/daily-after"
check "redelivery: equal" equal
say "usage-reconcile: $(tail -n 1 "$work/reconcile.out")"

say "== 5. ledger and leaks"
check "ledger: qfctl verify" [ "$(qfctl verify)" = "ledger ok" ]
tools /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server qf-kafka:9092 --topic "$TOPIC" --from-beginning \
  --timeout-ms 10000 >"$work/topic.out" || true
check "topic: holds one message per event ($total)" [ "$(grep -c '"call_id":"qf-' "$work/topic.out")" -ge "$total" ]
dc logs --no-log-prefix quotaforge qf-usage-consumer >"$work/logs" 2>&1
sql 'SELECT * FROM usage_seen; SELECT * FROM usage_daily' >"$work/tables"
leaks=0
for s in $(cat "$work"/usage-*.key) Bearer "$REQ_MARKER" "${SECRETS[@]}"; do
  for f in logs topic.out tables reconcile.out; do leaks=$((leaks + $(grep -cF -- "$s" "$work/$f" || true))); done
done
check "leaks: no key, request body, upstream key or database password in the logs, the topic, the tables or the report ($leaks matches)" [ "$leaks" = 0 ]

say "== result: $failures failed ($(($(date +%s) - start_all))s)"
[ "$failures" = 0 ]
