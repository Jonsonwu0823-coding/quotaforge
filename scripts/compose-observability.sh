#!/usr/bin/env bash
# 可观测的演示和核对（云端和 CI 用）：compose.yaml 加 compose.observability.yaml，起 QF、假上游、Jaeger、Prometheus，
# 发几种请求，然后核对：
#   1. 设置：QF 的指标端口 9464 不发布；Jaeger、Prometheus 的界面只发布到 127.0.0.1；对外的 8080 上没有 /metrics。
#   2. 追踪：带指定 traceparent 的调用在 Jaeger 里按 trace id 查得到，span 的名字和父子关系是 docs/observability.md 的那棵树，
#      根 span 的父 span 是请求里给的那个，资源属性里有实例标识；按调用号（qf.request_id）也搜得到同一条 trace；
#      日志行的 trace_id、span_id 和它相同，parent_span_id 是请求里给的 parent-id。没通过鉴权的请求没有 trace，日志行没有 trace_id。
#   3. 指标：Prometheus 抓到了，计数和发的请求对得上；规则都加载了；promtool 检查配置、跑规则的单元测试。
#   4. 脱敏：Jaeger 里的 trace、/metrics 的输出、QF 的日志里没有 Key、上游 Key、Bearer 和请求、响应正文的标记（只数次数）。
#   5. Jaeger 停掉之后：请求照常 200；导出出错的日志有节制（不超过 2 行），计数在涨；退出在宽限期之内。
# 每项后面都 qfctl verify。任何一项失败都以非 0 退出。
# 用自己的 Compose 项目名 qf-obs：开头和结尾的 down -v 只拆自己的项目。8080、9090、16686 被占就不跑。
# 等待用 wait_for 轮询，不靠睡眠的长短判对错。摘要写到 evidence/compose-observability-summary.txt：只有检查结果，没有 Key 和正文。
# jq 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-obs # 优先于 compose.yaml 里的 name

readonly FILES=(-f compose.yaml -f compose.observability.yaml)
readonly QF=http://127.0.0.1:8080 JAEGER=http://127.0.0.1:16686 PROM=http://127.0.0.1:9090
readonly UPSTREAM_KEY=test-only-upstream-key
readonly PROM_IMAGE=prom/prometheus:v3.15.0 # 和 compose.observability.yaml 里的相同
readonly REQ_MARKER=obs-request-marker-c4f1 RESP_MARKER='fake reply' # 请求正文里的标记；假上游回答的正文
readonly SUMMARY=evidence/compose-observability-summary.txt
# 请求里给的 trace：调用（会采样）、401（不该有 trace）、最后一个调用（等它出现，说明之前的都导出过了）
readonly TRACE=4bf92f3577b34da6a3ce929d0e0e4736 PARENT=00f067aa0ba902b7
readonly TRACE_401=5bf92f3577b34da6a3ce929d0e0e4737 TRACE_LAST=6bf92f3577b34da6a3ce929d0e0e4738

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
  if [ "$rc" != 0 ] && [ -s "$work/qfctl.err" ]; then
    echo "== qfctl.err" >&2
    cat "$work/qfctl.err" >&2
  fi
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

qfctl() { dc run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
verify_ok() { [ "$(qfctl verify)" = "ledger ok" ]; }
wait_for() {
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}
# new_key <账户> <额度>：建账户、发 Key，Key 写进 $work/<账户>.key（不打印）
new_key() {
  qfctl account create -name "$1" -limit "$2" >/dev/null
  qfctl key issue -account "$1" -models qs-fake-upstream-v1 | sed -n 's/^key: //p' >"$work/$1.key"
  [ "$(wc -c <"$work/$1.key")" = 48 ] # 47 个字符加换行
}
# send <名字> <Key 文件> <路径> <请求体> [请求头…]：响应体、响应头、状态码存进 $work/<名字>.*
send() {
  local name=$1 keyfile=$2 path=$3 body=$4
  shift 4
  local headers=()
  for h in "$@"; do headers+=(-H "$h"); done
  curl -sS -o "$work/$name.body" -D "$work/$name.headers" -w '%{http_code}' -H "Authorization: Bearer $(cat "$keyfile")" \
    -H 'Content-Type: application/json' "${headers[@]}" ${body:+--data-binary "$body"} "$QF$path" >"$work/$name.status" ||
    echo 000 >"$work/$name.status"
}
status_of() { cat "$work/$1.status"; }
request_id() { tr -d '\r' <"$work/$1.headers" | awk -F': ' 'tolower($1) == "x-request-id" { print $2 }'; }
qf_logs() { dc logs --no-log-prefix quotaforge; }
log_line() { qf_logs | jq -c --arg id "$1" 'select(.request_id == $id)'; }
# Jaeger 的查询 API v3，回答是 OTLP/JSON：span 在 .result.resourceSpans[].scopeSpans[].spans[]。
# jaeger_trace <trace id>：原样输出写进 $work/trace-<id>.json；查到了返回 0
jaeger_trace() {
  curl -sS -o "$work/trace-$1.json" "$JAEGER/api/v3/traces/$1" &&
    [ "$(jq '[.result.resourceSpans[]?.scopeSpans[].spans[]] | length' "$work/trace-$1.json" 2>/dev/null)" -gt 0 ]
}
# jaeger_search <属性 JSON>：最近一小时里 quotaforge 的、带这些属性的 trace
jaeger_search() {
  curl -sS -G "$JAEGER/api/v3/traces" --data-urlencode query.service_name=quotaforge --data-urlencode "query.attributes=$1" \
    --data-urlencode "query.start_time_min=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)" \
    --data-urlencode "query.start_time_max=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)" \
    --data-urlencode query.num_traces=1000
}
promql() { curl -sS -G "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "none"'; }
qf_metrics() { dc exec -T qf-prometheus wget -q -O - http://quotaforge:9464/metrics; }

say "== up"
dc down -v --remove-orphans >/dev/null 2>&1
for base in "$QF" "$PROM" "$JAEGER"; do
  if curl -s -o /dev/null --max-time 2 "$base/"; then
    say "FAIL ${base#http://} is already in use by something else"
    exit 1
  fi
done
start=$(date +%s)
dc --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
wait_for 60 curl -sf -o /dev/null "$JAEGER/api/v3/services"

say "== 1. settings"
published() { docker inspect "$(dc ps -q "$1")" | jq -c '[.[0].HostConfig.PortBindings // {} | to_entries[] | .key + "=" + (.value[] | .HostIp + ":" + .HostPort)]'; }
check "settings: quotaforge publishes only 127.0.0.1:8080 (not the metrics port 9464)" [ "$(published quotaforge)" = '["8080/tcp=127.0.0.1:8080"]' ]
check "settings: qf-jaeger publishes only its UI on 127.0.0.1:16686" [ "$(published qf-jaeger)" = '["16686/tcp=127.0.0.1:16686"]' ]
check "settings: qf-prometheus publishes only its UI on 127.0.0.1:9090" [ "$(published qf-prometheus)" = '["9090/tcp=127.0.0.1:9090"]' ]
check "settings: GET /metrics on 8080 is 404" [ "$(curl -s -o /dev/null -w '%{http_code}' "$QF/metrics")" = 404 ]

say "== requests"
new_key obs 100000000
new_key obs-poor 1 # 每个请求都超额
chat_body="{\"model\":\"qs-fake-upstream-v1\",\"messages\":[{\"role\":\"user\",\"content\":\"$REQ_MARKER\"}]}"
emb_body="{\"model\":\"qs-fake-upstream-v1\",\"input\":[\"$REQ_MARKER\"]}"
send traced "$work/obs.key" /v1/chat/completions "$chat_body" "traceparent: 00-$TRACE-$PARENT-01" "tracestate: qs=obs"
send emb "$work/obs.key" /v1/embeddings "$emb_body" "Idempotency-Key: obs-idem-1"
send emb-replay "$work/obs.key" /v1/embeddings "$emb_body" "Idempotency-Key: obs-idem-1"
send poor "$work/obs-poor.key" /v1/chat/completions "$chat_body"
dc stop qf-fake-upstream >/dev/null 2>&1
send upstream-down "$work/obs.key" /v1/chat/completions "$chat_body"
dc start qf-fake-upstream >/dev/null 2>&1
send lookup "$work/obs.key" "/v1/calls/$(request_id traced)" ""
echo qfk_not-a-real-key >"$work/wrong.key"
send unauthenticated "$work/wrong.key" /v1/chat/completions "$chat_body" "traceparent: 00-$TRACE_401-$PARENT-01"
send last "$work/obs.key" /v1/chat/completions "$chat_body" "traceparent: 00-$TRACE_LAST-$PARENT-01"
got=""
for r in traced emb emb-replay poor upstream-down lookup unauthenticated last; do got="$got $r=$(status_of "$r")"; done
say "statuses:$got"
check "requests: 200, 200, replay 200, 429, 502, 200, 401, 200" \
  [ "$got" = " traced=200 emb=200 emb-replay=200 poor=429 upstream-down=502 lookup=200 unauthenticated=401 last=200" ]
check "requests: the second embeddings call is a replay" grep -qi '^idempotent-replayed: true' "$work/emb-replay.headers"

say "== 2. traces"
check "jaeger: the trace of the call with a traceparent is found by its trace id" wait_for 30 jaeger_trace "$TRACE"
check "jaeger: the trace of the last call is there too (everything before it was exported)" wait_for 30 jaeger_trace "$TRACE_LAST"
t="$work/trace-$TRACE.json"
spans() { jq -c '[.result.resourceSpans[].scopeSpans[].spans[]]' "$t"; }
root_id=$(spans | jq -r '.[] | select(.name == "POST /v1/chat/completions") | .spanId')
say "spans: $(spans | jq -c 'map(.name) | sort')"
check "jaeger: the spans are the root and authenticate, admit, reserve, mark_sent, upstream, finish, adjust" \
  [ "$(spans | jq -c 'map(.name) | sort')" = \
  '["POST /v1/chat/completions","adjust","admit","authenticate","finish","mark_sent","reserve","upstream"]' ]
check "jaeger: the root's parent is the span in the request's traceparent" \
  [ "$(spans | jq -r --arg id "$root_id" '.[] | select(.spanId == $id) | .parentSpanId')" = "$PARENT" ]
check "jaeger: every other span is a direct child of the root" \
  [ "$(spans | jq --arg id "$root_id" '[.[] | select(.spanId != $id and .parentSpanId != $id)] | length')" = 0 ]
# kind：OTLP 的编号，SERVER 是 2，CLIENT 是 3
kind() { spans | jq -r --arg n "$1" '.[] | select(.name == $n) | .kind'; }
check "jaeger: the root is a server span, upstream a client span" [ "$(kind 'POST /v1/chat/completions') $(kind upstream)" = "2 3" ]
root_attr() { spans | jq -r --arg id "$root_id" --arg k "$1" '.[] | select(.spanId == $id) | .attributes[] | select(.key == $k) | .value.stringValue'; }
check "jaeger: the root carries the call id and the outcome" \
  [ "$(root_attr qf.request_id) $(root_attr qf.outcome)" = "$(request_id traced) SETTLED" ]
check "jaeger: the resource names the instance" \
  [ "$(jq -r '.result.resourceSpans[].resource.attributes[] | select(.key == "service.instance.id") | .value.stringValue' "$t" | sort -u)" = quotaforge ]
found=$(jaeger_search "{\"qf.request_id\":\"$(request_id traced)\"}" | jq -r '[.result.resourceSpans[]?.scopeSpans[].spans[].traceId] | unique | join(",")')
check "jaeger: searching by the call id (qf.request_id) finds the same trace" [ "$found" = "$TRACE" ]
traced_line=$(log_line "$(request_id traced)")
check "logs: the call's trace_id and span_id are the trace and its root span" \
  [ "$(jq -r '.trace_id + " " + .span_id' <<<"$traced_line")" = "$TRACE $root_id" ]
check "logs: the call's parent_span_id is the parent-id in its traceparent" [ "$(jq -r '.parent_span_id' <<<"$traced_line")" = "$PARENT" ]
check "jaeger: no trace for the request that failed authentication" [ "$(curl -s -o /dev/null -w '%{http_code}' "$JAEGER/api/v3/traces/$TRACE_401")" = 404 ]
check "logs: the 401 line has no trace_id" [ "$(log_line "$(request_id unauthenticated)" | jq 'has("trace_id")')" = false ]

say "== 3. metrics"
check "prometheus: the quotaforge target is up" \
  [ "$(curl -sS "$PROM/api/v1/targets" | jq -c '[.data.activeTargets[] | .labels.instance + "=" + .health]')" = '["quotaforge:9464=up"]' ]
counted() { [ "$(promql 'sum(qf_requests_total)')" = 8 ]; }
check "prometheus: scraped all 8 requests" wait_for 30 counted
by_status=$(curl -sS -G "$PROM/api/v1/query" --data-urlencode 'query=sum by (status, code) (qf_requests_total)' |
  jq -c '[.data.result[] | .metric.status + "/" + (.metric.code // "") + "=" + .value[1]] | sort')
say "requests by status/code: $by_status"
check "prometheus: the counts by status and code match the requests" \
  [ "$by_status" = '["200/=5","401/invalid_api_key=1","429/quota_exhausted=1","502/upstream_unavailable=1"]' ]
check "prometheus: one replay, one settled embeddings call, one cancelled chat call (upstream down)" \
  [ "$(promql 'sum(qf_idempotent_replays_total)') $(promql 'sum(qf_calls_total{endpoint="embeddings",outcome="settled"})') $(promql 'sum(qf_calls_total{endpoint="chat",outcome="cancelled"})')" = "1 1 1" ]
check "prometheus: the background loop reports a recent success" \
  [ "$(promql 'time() - max(qf_background_last_success_timestamp_seconds) < 60')" != none ]
rules=$(curl -sS "$PROM/api/v1/rules" | jq -c '[.data.groups[].rules[] | .type + ":" + .health] | group_by(.) | map(.[0] + "=" + (length | tostring))')
say "rules: $rules"
check "prometheus: 8 recording rules and 11 alerts are loaded and healthy" [ "$rules" = '["alerting:ok=11","recording:ok=8"]' ]
# 挂载用相对路径：由 docker 命令按自己的当前目录解析。Windows 的 Git Bash 里 $PWD 可能是 /tmp/...（%TEMP% 的映射），
# Docker Desktop 会把它当成虚拟机里的路径，挂上空目录。
# MSYS_NO_PATHCONV：Git Bash 会把容器里的路径 /etc/prometheus/... 改写成 Git 的安装目录。两者在 Linux 下都没有影响
promtool() { MSYS_NO_PATHCONV=1 docker run --rm -v ./deploy/prometheus:/etc/prometheus:ro --entrypoint promtool "$PROM_IMAGE" "$@"; }
check "promtool: check config" promtool check config /etc/prometheus/prometheus.yml
check "promtool: test rules" promtool test rules /etc/prometheus/rules_test.yml

say "== 4. no keys or bodies"
jaeger_search '{}' >"$work/all-traces.json"
qf_metrics >"$work/metrics.txt"
qf_logs >"$work/qf.log"
nonempty() { [ "$(jq '[.result.resourceSpans[].scopeSpans[].spans[]] | length' "$work/all-traces.json")" -ge 20 ] && grep -q '^qf_requests_total' "$work/metrics.txt"; }
check "evidence: the traces and metrics are not empty" nonempty
for out in all-traces.json metrics.txt qf.log; do
  hits=0
  for secret in "$(cat "$work/obs.key")" "$(cat "$work/obs-poor.key")" qfk_not-a-real-key "$UPSTREAM_KEY" Bearer "$REQ_MARKER" "$RESP_MARKER"; do
    hits=$((hits + $(grep -cF -- "$secret" "$work/$out" || true)))
  done
  check "redaction: keys, Bearer and body markers in $out: $hits" [ "$hits" = 0 ]
done
check "ledger: verify" verify_ok

say "== 5. jaeger down"
dc stop qf-jaeger >/dev/null 2>&1
ok=0
for i in $(seq 1 20); do
  send "down-$i" "$work/obs.key" /v1/chat/completions "$chat_body"
  [ "$(status_of "down-$i")" = 200 ] && ok=$((ok + 1))
done
check "jaeger down: 20 of 20 requests got 200 ($ok)" [ "$ok" = 20 ]
export_errors() { qf_metrics | awk '$1 == "qf_trace_export_errors_total" { print $2 }'; }
export_failed() { [ "$(export_errors)" != 0 ]; }
check "jaeger down: failed exports are counted" wait_for 30 export_failed
telemetry_lines=$(qf_logs | jq -c 'select(.msg == "telemetry")' | wc -l)
say "jaeger down: $(export_errors) failed exports, $telemetry_lines telemetry log lines"
check "jaeger down: at most 2 telemetry log lines" [ "$telemetry_lines" -le 2 ]
check "jaeger down: every QF log line is JSON" bash -c "docker compose ${FILES[*]} logs --no-log-prefix quotaforge | jq -e . >/dev/null"
grace=$(docker inspect "$(dc ps -q quotaforge)" | jq -r '.[0].Config.StopTimeout')
stop_start=$(date +%s)
dc stop quotaforge >/dev/null 2>&1
stop_seconds=$(($(date +%s) - stop_start))
say "jaeger down: stop took ${stop_seconds}s (stop_grace_period ${grace}s)"
check "jaeger down: QF stopped within the grace period" [ "$stop_seconds" -lt "$grace" ]
check "jaeger down: QF exited cleanly (exit code 0)" [ "$(docker inspect "$(dc ps -a -q quotaforge)" | jq '.[0].State.ExitCode')" = 0 ]
check "ledger: verify after everything" verify_ok

say "== $failures failed"
[ "$failures" = 0 ]
