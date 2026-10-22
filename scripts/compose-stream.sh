#!/usr/bin/env bash
# 流式的演示和核对（云端和 CI 用）：用 compose.yaml 起 QF、账本数据库、Redis 和 QF 的假上游，发几种流式请求，核对：
#   1. 正常的流：响应头是 SSE；事件逐个到达（第一个和最后一个事件到达的时间差得开，不是最后一起到）；最后是 [DONE]；
#      QF 替客户端要的 usage 事件不转发，其它事件里的 "usage":null 照转发；调用结算，用量和假上游的账单一致。
#   2. 客户端自己要了 usage：usage 事件照转发，结算的用量就是它的数。
#   3. 客户端在流的中途断开：QF 接着读完上游，调用照常结算，日志有 client_disconnected；上游没有被取消，账单是完整的回答。
#   4. 上游在流的中途断开：客户端收到 QF 补的错误事件（upstream_error），没有 [DONE]；用量未知，预留继续占着。
#   5. 超时：事件之间慢的流在 chat 的上游超时（25 秒）截断，客户端收到 upstream_timeout 的错误事件；用量未知。
#   6. 幂等：带键的流式结束后，同一个键再来是 409 idempotency_response_unavailable，上游只收到一次。
#   7. 非流式照旧；脱敏：QF 的日志里没有 Key、请求正文和流里的内容（只数次数）；账本的预留只剩未知的两个；qfctl verify。
# 任何一项失败都以非 0 退出。用自己的 Compose 项目名 qf-stream：开头和结尾的 down -v 只拆自己的项目。8080 被占就不跑。
# 等待用 wait_for 轮询，不靠睡眠的长短判对错。摘要写到 evidence/compose-stream-summary.txt：只有检查结果，没有 Key 和正文；
# 流的原文只在临时目录里，结束时删掉。jq 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-stream # 优先于 compose.yaml 里的 name

readonly QF=http://127.0.0.1:8080
readonly REQ_MARKER=stream-request-marker-7d2e STREAM_MARKER='fake reply' # 请求正文里的标记；假上游流里的内容
readonly SUMMARY=evidence/compose-stream-summary.txt
# 假上游正常流的事件：role、10 个内容块、finish、usage、[DONE]；没要 usage 的客户端少 usage 这一个。
readonly CLIENT_EVENTS=13

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
dc() { docker compose "$@"; }
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
key() { cat "$work/stream.key"; }
chat_body() { echo "{\"model\":\"$1\",\"stream\":${2:-true}${3:+,$3},\"messages\":[{\"role\":\"user\",\"content\":\"$REQ_MARKER\"}]}"; }
# stream <名字> <请求体> [curl 参数…]：读一个流，每行前面加上到达的时刻（秒），存进 $work/<名字>.events；响应头存进 .headers。
stream() {
  local name=$1 body=$2
  shift 2
  { curl -sS -N -D "$work/$name.headers" -H "Authorization: Bearer $(key)" -H 'Content-Type: application/json' \
    --data-binary "$body" "$@" "$QF/v1/chat/completions" 2>/dev/null || true; } |
    while IFS= read -r line; do
      line=${line%$'\r'}
      [ -z "$line" ] || printf '%s %s\n' "$(date +%s)" "$line"
    done >"$work/$name.events"
}
data_lines() { grep -c ' data: ' "$work/$1.events" || true; }
last_data() { grep ' data: ' "$work/$1.events" | tail -n 1 | cut -d' ' -f2-; }
request_id() { tr -d '\r' <"$work/$1.headers" | awk -F': ' 'tolower($1) == "x-request-id" { print $2 }'; }
header() { tr -d '\r' <"$work/$1.headers" | awk -F': ' -v h="$2" 'tolower($1) == h { print $2 }'; }
# call <调用号> <jq 过滤式>：调用状态查询
call() { curl -sS -H "Authorization: Bearer $(key)" "$QF/v1/calls/$1" | jq -r "$2"; }
call_is() { [ "$(call "$1" .status)" = "$2" ]; }
log_line() { dc logs --no-log-prefix quotaforge | jq -c --arg id "$1" 'select(.msg == "call" and .request_id == $id)'; }
# 假上游的镜像没有 curl，用它自己的 -get。
fake_get() { MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get "http://127.0.0.1:8081$1"; }
bill_total() { fake_get /_fake/bill | awk -F, -v id="$1" '$1 == id { print $5 }'; }

say "== up"
dc down -v --remove-orphans >/dev/null 2>&1
if curl -s -o /dev/null --max-time 2 "$QF/"; then
  say "FAIL 127.0.0.1:8080 is already in use by something else"
  exit 1
fi
start=$(date +%s)
dc --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
qfctl account create -name stream -limit 100000000 >/dev/null
qfctl key issue -account stream -models qs-fake-upstream-v1,fake-no-usage,fake-stream-slow-300,fake-stream-cut,fake-stream-slow-2500 |
  sed -n 's/^key: //p' >"$work/stream.key"
check "setup: issued a key" [ "$(wc -c <"$work/stream.key")" -eq 48 ]

say "== 1. a normal stream (the client did not ask for usage)"
stream normal "$(chat_body fake-stream-slow-300)"
id=$(request_id normal)
first=$(grep ' data: ' "$work/normal.events" | head -n 1 | cut -d' ' -f1)
last=$(grep ' data: ' "$work/normal.events" | tail -n 1 | cut -d' ' -f1)
say "events: $(data_lines normal), first to last: $((last - first)) s"
check "normal: 200 text/event-stream with x-request-id" \
  [ "$(head -n 1 "$work/normal.headers" | tr -d '\r' | cut -d' ' -f2)/$(header normal content-type)/${id:0:3}" = "200/text/event-stream; charset=utf-8/qf-" ]
check "normal: $CLIENT_EVENTS events, the last is [DONE]" [ "$(data_lines normal)/$(last_data normal)" = "$CLIENT_EVENTS/data: [DONE]" ]
check "normal: events arrive one by one (first to last >= 2 s; the upstream sends one every 300 ms)" [ $((last - first)) -ge 2 ]
check "normal: the usage event QF asked for is not forwarded; the other events keep \"usage\":null" \
  [ "$(grep -c '"choices":\[\]' "$work/normal.events" || true)/$(grep -c '"usage":null' "$work/normal.events")" = "0/12" ]
wait_for 10 call_is "$id" SETTLED
check "normal: the call is SETTLED, its usage is the fake upstream's bill" \
  [ "$(call "$id" .status)/$(call "$id" .usage.total_tokens)" = "SETTLED/$(bill_total "$id")" ]

say "== 2. the client asks for usage itself"
stream wants "$(chat_body qs-fake-upstream-v1 true '"stream_options":{"include_usage":true}')"
wid=$(request_id wants)
usage_event=$(grep '"choices":\[\]' "$work/wants.events" | cut -d' ' -f3- | jq -r '.usage.total_tokens' || true)
wait_for 10 call_is "$wid" SETTLED
check "usage requested: the usage event is forwarded, the call is settled with its numbers" \
  [ "$(data_lines wants)/$(call "$wid" .usage.total_tokens)" = "$((CLIENT_EVENTS + 1))/$usage_event" ]

say "== 3. the client goes away in the middle of the stream"
canceled_before=$(fake_get /_fake/stats | jq .canceled)
stream gone "$(chat_body fake-stream-slow-300)" --max-time 1.5
gid=$(request_id gone)
say "events before the client left: $(data_lines gone)"
check "disconnect: the client got some events but not [DONE]" \
  [ "$(($(data_lines gone) >= 1 && $(data_lines gone) < CLIENT_EVENTS))/$(grep -c 'data: \[DONE\]' "$work/gone.events" || true)" = "1/0" ]
wait_for 15 call_is "$gid" SETTLED
check "disconnect: QF read the rest of the stream: SETTLED, the log has client_disconnected and status 200" \
  [ "$(log_line "$gid" | jq -r '[.outcome, .client_disconnected, .status] | join("/")')" = "SETTLED/true/200" ]
check "disconnect: the upstream was not canceled, the bill has the whole answer (completion 10)" \
  [ "$(fake_get /_fake/stats | jq .canceled)/$(fake_get /_fake/bill | awk -F, -v id="$gid" '$1 == id { print $4 }')" = "$canceled_before/10" ]

say "== 4. the upstream drops the connection in the middle"
stream cut "$(chat_body fake-stream-cut)"
cid=$(request_id cut)
check "cut: the last event is QF's error event (upstream_error), no [DONE]" \
  [ "$(last_data cut | cut -c7- | jq -r .error.code)/$(grep -c 'data: \[DONE\]' "$work/cut.events" || true)" = "upstream_error/0" ]
wait_for 10 call_is "$cid" USAGE_UNKNOWN
check "cut: the call's usage is unknown (null, not 0) and it keeps its reservation" \
  [ "$(call "$cid" '[.status, .usage, .error_code] | map(tostring) | join("/")')" = "USAGE_UNKNOWN/null/upstream_error" ]

say "== 5. a stream slower than the chat timeout (25 s)"
start=$(date +%s)
stream slow "$(chat_body fake-stream-slow-2500)"
took=$(($(date +%s) - start))
sid=$(request_id slow)
say "the stream ended after $took s with $(data_lines slow) events"
check "timeout: cut at the chat timeout with QF's error event (upstream_timeout), no [DONE]" \
  [ "$(last_data slow | cut -c7- | jq -r .error.code)/$(grep -c 'data: \[DONE\]' "$work/slow.events" || true)" = "upstream_timeout/0" ]
check "timeout: it took about the chat timeout (24-30 s)" [ "$((took >= 24 && took <= 30))" = 1 ]
wait_for 10 call_is "$sid" USAGE_UNKNOWN
check "timeout: the call's usage is unknown" [ "$(call "$sid" '[.status, .error_code] | join("/")')" = "USAGE_UNKNOWN/upstream_timeout" ]

say "== 6. a streaming call with an Idempotency-Key"
chats_before=$(fake_get /_fake/stats | jq .chat_completions)
stream idem "$(chat_body qs-fake-upstream-v1)" -H 'Idempotency-Key: stream-demo-1'
wait_for 10 call_is "$(request_id idem)" SETTLED
retry=$(curl -sS -H "Authorization: Bearer $(key)" -H 'Content-Type: application/json' -H 'Idempotency-Key: stream-demo-1' \
  --data-binary "$(chat_body qs-fake-upstream-v1)" "$QF/v1/chat/completions" | jq -r .error.code)
check "idempotency: the retry is 409 idempotency_response_unavailable, not run again (the upstream got one request)" \
  [ "$retry/$(($(fake_get /_fake/stats | jq .chat_completions) - chats_before))" = "idempotency_response_unavailable/1" ]

say "== 7. non-streaming, logs, ledger"
plain=$(curl -sS -H "Authorization: Bearer $(key)" -H 'Content-Type: application/json' --data-binary "$(chat_body qs-fake-upstream-v1 false)" \
  "$QF/v1/chat/completions" | jq -r '[.object, .usage.completion_tokens] | map(tostring) | join("/")')
check "non-streaming: a JSON answer as before" [ "$plain" = "chat.completion/10" ]
dc logs --no-log-prefix quotaforge >"$work/qf.log" 2>&1
check "logs: one call line per stream, all with stream: true" [ "$(jq -c 'select(.msg == "call" and .stream == true)' "$work/qf.log" | wc -l)" -eq 7 ]
leaks=0
for s in "$(key)" Bearer "$REQ_MARKER" "$STREAM_MARKER" test-only-upstream-key; do
  leaks=$((leaks + $(grep -cF -- "$s" "$work/qf.log" || true)))
done
check "logs: no key, request body or stream content ($leaks matches)" [ "$leaks" = 0 ]
reserved=$(qfctl usage -account stream | awk -F'\t' 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "reserved") c = i } NR == 2 { print $c }')
held=$(($(log_line "$cid" | jq .reserved_tokens) + $(log_line "$sid" | jq .reserved_tokens)))
check "ledger: only the two unknown calls still hold reservations ($reserved tokens)" [ "$reserved" = "$held" ]
check "ledger: qfctl verify" [ "$(qfctl verify)" = "ledger ok" ]

say "== result: $failures failed"
[ "$failures" = 0 ]
