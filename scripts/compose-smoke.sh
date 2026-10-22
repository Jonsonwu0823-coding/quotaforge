#!/usr/bin/env bash
# 单独运行的 Compose 冒烟（云端和 CI 用）：从空环境起 QF、账本数据库和 QF 的假上游，用 qfctl 建账户、发 Key，
# 经 127.0.0.1:8080 发几种请求，检查镜像和容器的安全设置、响应、账本、额度用完时的 429、调用日志、代理变量和
# 优雅退出、幂等键（重放、请求不同、处理中、格式不对、清理）、调用状态查询、有界结算（客户端先走，调用照常结算）、
# 真杀进程之后的恢复（docker kill -s KILL → 重启 → 回收标成未知 → 用假上游的账单 qfctl reconcile 预演、执行、再执行），
# 限流（Key 和账户的 RPM、TPM 的退还、停掉 Redis 时 503、Redis 回来后不重启 QF 就恢复），最后校验账本并 down -v。
# 任何一项失败都以非 0 退出。
# 用自己的 Compose 项目名 qf-smoke：开头和结尾的 down -v 只拆自己的项目，不碰默认项目 quotaforge（例如正在跑的真实冒烟）。
# qfctl 的 stderr 写进临时目录，脚本失败时打印出来（Key 在 stdout，不在 stderr）。
# 摘要写到 evidence/compose-smoke-summary.txt：只有检查结果，没有 Key、密码、正文和代理变量的值。
# jq 过滤式里的 $p、$id 是 jq 的变量，写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-smoke # 优先于 compose.yaml 里的 name

# 仅测试的上游 Key 和数据库密码，和 compose.yaml、deploy/test-only-db/ 一致。QF 的 Key 由 qfctl 现发。
readonly UPSTREAM_KEY=test-only-upstream-key
readonly DB_PASSWORDS=(test-only-root-password test-only-migrate-password test-only-app-password)
readonly ACCOUNT=smoke
readonly BASE=http://127.0.0.1:8080
# QF 的关闭上限，和回收的上界相同：默认 embeddings 超时 25 秒 + 3 次账本操作各 2 秒 + 余量 5 秒
# （config.ShutdownTimeout、config.ReclaimAfter）。stop_grace_period 必须比它长。
readonly SHUTDOWN_CAP_SECONDS=36
readonly RECLAIM_AFTER_SECONDS=36
# quota_exhausted 的 Retry-After：较大的上游超时（默认 25 秒）。
readonly RETRY_AFTER=25
readonly SUMMARY=evidence/compose-smoke-summary.txt

work=$(mktemp -d)
# 一次运行里不重复的标记。不用 date 的 %N：那是 GNU date 的扩展，BSD 的 date 不认。
unique() { echo "$(date +%s)-$$-$RANDOM"; }
marker="smoke-marker-$(unique)"
failures=0
mkdir -p evidence
: >"$SUMMARY"

say() { echo "$*" | tee -a "$SUMMARY"; }
# check <说明> <命令...>：命令成功记 ok，失败记 FAIL 并继续，最后统一按失败数退出。
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; else say "FAIL $what"; failures=$((failures + 1)); fi
}
cleanup() {
  local rc=$?
  if [ "$rc" != 0 ] && [ -s "$work/qfctl.err" ]; then
    echo "== qfctl stderr" >&2
    cat "$work/qfctl.err" >&2
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

# post <名字> <路径> <Key> <请求体> [Idempotency-Key]：响应体、响应头、状态码、发出的 X-Client-Request-Id
# 都存进 $work/<名字>.*
post() {
  local name=$1 path=$2 key=$3 body=$4 crid idem=()
  if [ $# -ge 5 ]; then idem=(-H "Idempotency-Key: $5"); fi
  crid="smoke-$name-$(unique)"
  echo "$crid" >"$work/$name.crid"
  curl -sS -o "$work/$name.body" -D "$work/$name.headers" -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H 'Content-Type: application/json' -H "X-Client-Request-Id: $crid" ${idem[@]+"${idem[@]}"} \
    --data-binary "$body" "$BASE$path" >"$work/$name.status" || echo 000 >"$work/$name.status"
}
status_is() { [ "$(cat "$work/$1.status")" = "$2" ]; }
header_is() { [ "$(tr -d '\r' <"$work/$1.headers" | awk -v h="$(tr '[:upper:]' '[:lower:]' <<<"$2"):" 'tolower($1) == h { print $2 }')" = "$3" ]; }
# qfctl 经一次性容器执行管理命令；数据库已经起来了，不再等依赖。
qfctl() { docker compose run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
# account_field <账户> <列名>：qfctl usage 里这个账户某一列的值。usage_field <列名>：smoke 账户的。
account_field() { qfctl usage -account "$1" | awk -F'\t' -v col="$2" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == col) c = i } NR == 2 { print $c }'; }
usage_field() { account_field "$ACCOUNT" "$1"; }
header() { tr -d '\r' <"$work/$1.headers" | awk -v h="$(tr '[:upper:]' '[:lower:]' <<<"$2"):" 'tolower($1) == h { print $2 }'; }
request_id() { header "$1" x-request-id; }
has_request_id() { [[ "$(request_id "$1")" == qf-* ]]; }
body_ok() { jq -e "${@:2}" "$work/$1.body" >/dev/null; }
# lookup <名字> <调用号> <Key>：GET /v1/calls/<调用号>，正文和状态码存进 $work/<名字>.*
lookup() {
  curl -sS -o "$work/$1.body" -w '%{http_code}' -H "Authorization: Bearer $3" "$BASE/v1/calls/$2" >"$work/$1.status" || echo 000 >"$work/$1.status"
}
# fake_bill：QF 的假上游到现在为止的账单（QF 的账单格式）。假上游的镜像没有 curl，用它自己的 -get。
# MSYS_NO_PATHCONV=1 只加在这条命令上：Git Bash 会把容器里的路径 /fakeupstream 改写成 Windows 路径（容器里就找不到命令）。
# 不全局设，因为脚本交给 curl、jq 的 /tmp 下的路径要靠 Git Bash 的转换。Linux 上这个变量没有作用。
fake_bill() { MSYS_NO_PATHCONV=1 docker compose exec -T qf-fake-upstream /fakeupstream -get http://127.0.0.1:8081/_fake/bill; }
# wait_for <秒> <命令...>：每 0.5 秒试一次，直到命令成功（确定的事件），超时返回失败。
wait_for() {
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}

say "== up"
docker compose down -v --remove-orphans >/dev/null 2>&1
if curl -s -o /dev/null --max-time 2 "$BASE/"; then
  say "FAIL 127.0.0.1:8080 is already in use by something else"
  exit 1
fi
start=$(date +%s)
docker compose --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
for image in quotaforge:local quotaforge-qfctl:local quotaforge-fakeupstream:local; do
  say "image $image: $(docker image inspect -f '{{.Size}}' "$image") bytes"
done
cid=$(docker compose ps -q quotaforge)
mysql_cid=$(docker compose ps -q qf-mysql)
check "migration: qfctl exited 0" [ "$(docker inspect -f '{{.State.ExitCode}}' "$(docker compose ps -a -q qfctl)")" = 0 ]

say "== account and key (qfctl)"
qfctl account create -name "$ACCOUNT" -limit 1000000 >/dev/null
# Key 只进这个变量和临时目录，不打印。
KEY=$(qfctl key issue -account "$ACCOUNT" -models qs-fake-upstream-v1,fake-tool-call,fake-no-usage,fake-slow-5000,fake-billed-hang | sed -n 's/^key: //p')
check "qfctl issued a key" [ "${#KEY}" = 47 ]
# 另一个账户的 Key：查不到 smoke 账户的调用
qfctl account create -name "$ACCOUNT-other" -limit 1000 >/dev/null
OTHER_KEY=$(qfctl key issue -account "$ACCOUNT-other" -models qs-fake-upstream-v1 | sed -n 's/^key: //p')

say "== container and image settings (values not shown)"
# container <jq 路径>、image <jq 路径>：读 docker inspect 的一项，缺的项是 null，不会让 inspect 报错。
container() { docker inspect "$cid" | jq -c ".[0]$1"; }
image() { docker image inspect quotaforge:local | jq -c ".[0]$1"; }
grace=$(container .Config.StopTimeout)
check "settings: runs as 65532:65532" [ "$(container .Config.User)" = '"65532:65532"' ]
check "settings: read-only root filesystem" [ "$(container .HostConfig.ReadonlyRootfs)" = true ]
check "settings: all capabilities dropped" [ "$(container .HostConfig.CapDrop)" = '["ALL"]' ]
check "settings: no-new-privileges" [ "$(container '.HostConfig.SecurityOpt | index("no-new-privileges:true") != null')" = true ]
check "settings: published ports only on 127.0.0.1" \
  [ "$(container '.HostConfig.PortBindings | [.[][].HostIp] | length > 0 and all(. == "127.0.0.1")')" = true ]
check "settings: image has the healthcheck" [ "$(image .Config.Healthcheck.Test)" = '["CMD","/quotaforge","healthcheck"]' ]
check "settings: stop_grace_period longer than the ${SHUTDOWN_CAP_SECONDS}s shutdown cap" [ "$grace" -gt "$SHUTDOWN_CAP_SECONDS" ]
# 只看镜像自己的非空普通文件；.dockerenv、etc/hosts 这类是 docker create 生成的空文件。
# 解开再用 find 列：tar -tv 的列序 GNU tar 和 macOS 的 bsdtar 不一样。
image_files() {
  local c dir
  c=$(docker create "$1")
  dir=$(mktemp -d "$work/image.XXXXXX")
  docker export "$c" | tar -x -C "$dir"
  (cd "$dir" && find . -type f -size +0 | sed 's|^\./||' | sort | tr '\n' ' ')
  docker rm "$c" >/dev/null
}
check "settings: image holds only /quotaforge and the CA bundle" [ "$(image_files quotaforge:local)" = "etc/ssl/certs/ca-certificates.crt quotaforge " ]
check "settings: qfctl image holds only /qfctl" [ "$(image_files quotaforge-qfctl:local)" = "qfctl " ]
mysql_networks() { docker inspect "$mysql_cid" | jq -c '[.[0].NetworkSettings.Networks | keys[]]'; }
check "settings: qf-mysql publishes no ports" [ "$(docker inspect "$mysql_cid" | jq -c '[.[0].HostConfig.PortBindings // {} | .[][]?] | length')" = 0 ]
check "settings: qf-mysql is only on the default network, not qs-qf" [ "$(mysql_networks)" = "[\"${COMPOSE_PROJECT_NAME}_default\"]" ]
# 数据库密码只在 secret 文件里：两个容器的环境变量里没有。
env_has_no_db_password() {
  local env pw
  env=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$cid" "$mysql_cid")
  for pw in "${DB_PASSWORDS[@]}"; do ! grep -qF -- "$pw" <<<"$env" || return 1; done
}
check "settings: no database password in the environment of quotaforge and qf-mysql" env_has_no_db_password
# compose.real.yaml 渲染出来的配置里不能有上游 Key：Key 只能经 secret 进容器。用现生成的假 Key，不起容器。
fake_upstream_key="fake-secret-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
real_config_hides_key() {
  QF_UPSTREAM_API_KEY=$fake_upstream_key QF_UPSTREAM_BASE_URL=http://upstream.invalid/v1 \
    docker compose -f compose.yaml -f compose.real.yaml config >"$work/real-compose.txt" 2>&1 &&
    grep -q upstream_api_key "$work/real-compose.txt" && ! grep -qF -- "$fake_upstream_key" "$work/real-compose.txt"
}
check "settings: compose.real.yaml config does not contain the upstream key" real_config_hides_key

say "== requests"
plen=${#marker}
post chat /v1/chat/completions "$KEY" \
  '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"'"$marker"'"}],"temperature":0,"stream":false}'
check "chat: 200" status_is chat 200
check "chat: x-request-id" has_request_id chat
check "chat: model is the upstream's" body_ok chat '.model == "fake-chat-20260101"'
check "chat: usage is the upstream's, unchanged" body_ok chat --argjson p "$plen" \
  '.usage == {"prompt_tokens": $p, "completion_tokens": 10, "total_tokens": ($p + 10), "fake": true}'

post tools /v1/chat/completions "$KEY" \
  '{"model":"fake-tool-call","messages":[{"role":"user","content":"'"$marker"'"}],"tools":[{"type":"function","function":{"name":"smoke_lookup","parameters":{"type":"object","properties":{}}}}],"tool_choice":"auto"}'
check "tools: 200" status_is tools 200
check "tools: x-request-id" has_request_id tools
check "tools: tool_calls passed through" body_ok tools \
  '.choices[0].finish_reason == "tool_calls" and .choices[0].message.tool_calls[0].function.name == "smoke_lookup"'
check "tools: model and usage" body_ok tools --argjson p "$plen" \
  '.model == "fake-tool-call-20260101" and .usage == {"prompt_tokens": $p, "completion_tokens": 10, "total_tokens": ($p + 10), "fake": true}'

post embeddings /v1/embeddings "$KEY" '{"model":"qs-fake-upstream-v1","input":["'"$marker"'","ab"],"dimensions":4}'
check "embeddings: 200" status_is embeddings 200
check "embeddings: x-request-id" has_request_id embeddings
check "embeddings: two vectors, model is the upstream's" body_ok embeddings \
  '(.data | length) == 2 and (.data[0].embedding | length) == 4 and .model == "fake-embed-20260101"'
check "embeddings: usage is the upstream's, unchanged" body_ok embeddings --argjson p "$((plen + 2))" \
  '.usage == {"prompt_tokens": $p, "total_tokens": $p, "fake": true}'

post bad_key /v1/chat/completions "qfk_not-a-key" '{"model":"qs-fake-upstream-v1"}'
check "invalid key: 401 invalid_api_key" body_ok bad_key '.error.code == "invalid_api_key"'
check "invalid key: status" status_is bad_key 401
check "invalid key: x-request-id" has_request_id bad_key

post no_model /v1/chat/completions "$KEY" '{"model":"no-such-model"}'
check "unknown model: 404 model_not_found" body_ok no_model '.error.code == "model_not_found"'
check "unknown model: status" status_is no_model 404
check "unknown model: x-request-id" has_request_id no_model

say "== ledger"
# 至此 chat、tools、embeddings 三次结算；Key 错、模型不存在的请求没进账本。
settled_total=$(( (plen + 10) * 2 + plen + 2 ))
check "ledger: three calls settled" [ "$(usage_field SETTLED)" = 3 ]
check "ledger: consumed equals the usage the upstream reported" [ "$(usage_field consumed)" = "$settled_total" ]
check "ledger: nothing left reserved" [ "$(usage_field reserved)" = 0 ]
# 额度用完：上限改成已消费，下一次请求立即 429 quota_exhausted，带 Retry-After。
qfctl account set-limit -name "$ACCOUNT" -limit "$settled_total" >/dev/null
post quota /v1/chat/completions "$KEY" '{"model":"qs-fake-upstream-v1","max_tokens":1}'
check "quota: 429 quota_exhausted" body_ok quota '.error.code == "quota_exhausted"'
check "quota: status" status_is quota 429
check "quota: Retry-After ${RETRY_AFTER}" header_is quota Retry-After "$RETRY_AFTER"
check "quota: x-request-id" has_request_id quota
qfctl account set-limit -name "$ACCOUNT" -limit 1000000 >/dev/null

say "== idempotency key"
idem_body='{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"'"$marker"'-idem"}]}'
idem_key="smoke-idem-$(unique)"
settled_before=$(usage_field SETTLED)
consumed_before=$(usage_field consumed)
post idem_first /v1/chat/completions "$KEY" "$idem_body" "$idem_key"
post idem_replay /v1/chat/completions "$KEY" "$idem_body" "$idem_key"
check "idempotency: first 200" status_is idem_first 200
check "idempotency: same key and body again: 200" status_is idem_replay 200
check "idempotency: Idempotent-Replayed: true" header_is idem_replay Idempotent-Replayed true
check "idempotency: x-request-id is the first call's" [ "$(request_id idem_replay)" = "$(request_id idem_first)" ]
check "idempotency: body is byte-for-byte the same" cmp -s "$work/idem_first.body" "$work/idem_replay.body"
check "idempotency: one more settled call, not two" [ "$(usage_field SETTLED)" = "$((settled_before + 1))" ]
check "idempotency: consumed grew once, by the first call's usage" \
  [ "$(usage_field consumed)" = "$((consumed_before + $(jq .usage.total_tokens "$work/idem_first.body")))" ]
post idem_reused /v1/chat/completions "$KEY" '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"other"}]}' "$idem_key"
check "idempotency: same key, different body: 422 idempotency_key_reused" body_ok idem_reused '.error.code == "idempotency_key_reused"'
check "idempotency: 422 status" status_is idem_reused 422
# 处理中：先在后台发一个慢请求，等账本里出现这次已经落了发送意图的调用（IN_FLIGHT，确定的事件，不靠睡眠），再用同一个键发。
slow_key="smoke-idem-slow-$(unique)"
slow_body='{"model":"fake-slow-5000","messages":[{"role":"user","content":"slow idem"}]}'
post idem_slow /v1/chat/completions "$KEY" "$slow_body" "$slow_key" &
idem_slow_pid=$!
for _ in $(seq 60); do [ "$(usage_field IN_FLIGHT)" = 1 ] && break; sleep 0.2; done
post idem_busy /v1/chat/completions "$KEY" "$slow_body" "$slow_key"
check "idempotency: while in progress: 409 idempotency_in_progress" body_ok idem_busy '.error.code == "idempotency_in_progress"'
check "idempotency: 409 has Retry-After: 1" header_is idem_busy Retry-After 1
wait "$idem_slow_pid"
post idem_after /v1/chat/completions "$KEY" "$slow_body" "$slow_key"
check "idempotency: after it finished: replayed" header_is idem_after Idempotent-Replayed true
check "idempotency: replay of the slow call has its x-request-id" [ "$(request_id idem_after)" = "$(request_id idem_slow)" ]
post idem_bad_key /v1/chat/completions "$KEY" "$idem_body" "has space"
check "idempotency: invalid key: 400 invalid_idempotency_key" body_ok idem_bad_key '.error.code == "invalid_idempotency_key"'
qfctl idempotency purge >"$work/purge.txt"
check "idempotency: qfctl idempotency purge runs (nothing expired yet)" [ "$(cat "$work/purge.txt")" = "purged 0 expired idempotency records" ]

say "== call status lookup"
lookup lookup_chat "$(request_id chat)" "$KEY"
check "lookup: 200" status_is lookup_chat 200
check "lookup: SETTLED with the usage, resolved by the gateway on the response" body_ok lookup_chat --argjson p "$plen" --arg id "$(request_id chat)" \
  '.id == $id and .status == "SETTLED" and .usage == {"prompt_tokens": $p, "completion_tokens": 10, "total_tokens": ($p + 10)} and .http_status == 200 and .resolution.by == "gateway" and .resolution.evidence == "response"'
check "lookup: no request or response body, no key" bash -c '! grep -qF -e "$1" -e "fake reply" -e "$2" "$3"' _ "$marker" "$KEY" "$work/lookup_chat.body"
lookup lookup_other "$(request_id chat)" "$OTHER_KEY"
lookup lookup_missing "qf-AAAAAAAAAAAAAAAAAAAAAAAAAA" "$KEY"
check "lookup: another account's call: 404 call_not_found" body_ok lookup_other '.error.code == "call_not_found"'
check "lookup: 404 status" status_is lookup_other 404
check "lookup: another account's call and a missing one look the same" cmp -s "$work/lookup_other.body" "$work/lookup_missing.body"

say "== bounded settlement (the client leaves first)"
# 客户端 1 秒就走，上游 5 秒才回答：请求已经发出，QF 照常等到回答、结算，只是不写回。客户端拿不到 x-request-id，
# 按它发的 X-Client-Request-Id 在日志里找到这一行（写完日志才算处理完，确定的事件）。
bounded_crid="smoke-bounded-$(unique)"
client_left() {
  ! curl -sS -o /dev/null --max-time 1 -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -H "X-Client-Request-Id: $bounded_crid" \
    --data-binary '{"model":"fake-slow-5000","messages":[{"role":"user","content":"bounded"}]}' "$BASE/v1/chat/completions" 2>/dev/null
}
check "bounded: the client gave up after 1s" client_left
bounded_line() { docker compose logs --no-log-prefix quotaforge | jq -R -c --arg c "$bounded_crid" 'fromjson? | select(.msg == "call" and .client_request_id == $c)'; }
bounded_seen() { [ -n "$(bounded_line)" ]; }
check "bounded: the call finished after the client left" wait_for 20 bounded_seen
bounded_id=$(bounded_line | jq -r .request_id)
bounded_logged() { [ "$(bounded_line | jq -c '[.client_disconnected, has("status"), .outcome]')" = '[true,false,"SETTLED"]' ]; }
check "bounded: log has client_disconnected and no status" bounded_logged
lookup lookup_bounded "$bounded_id" "$KEY"
check "bounded: lookup shows SETTLED with usage" body_ok lookup_bounded '.status == "SETTLED" and .usage.total_tokens == 17'

say "== proxy variables in the quotaforge container (values not shown)"
container_env=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$cid")
proxied=0
no_proxy=""
for name in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
  if grep -q "^$name=" <<<"$container_env"; then
    say "$name: set"
    case $name in [Nn][Oo]_*) no_proxy+=",$(grep "^$name=" <<<"$container_env" | cut -d= -f2-)" ;; *) proxied=1 ;; esac
  else
    say "$name: unset"
  fi
done
proxy_safe() { [ "$proxied" = 0 ] || [[ ",$no_proxy," == *,qf-fake-upstream,* ]]; }
check "proxy: unset, or NO_PROXY contains qf-fake-upstream" proxy_safe

say "== graceful shutdown"
post slow /v1/chat/completions "$KEY" '{"model":"fake-slow-5000","messages":[{"role":"user","content":"slow"}]}' &
slow_pid=$!
sleep 1
start=$(date +%s)
docker compose stop quotaforge
stop_seconds=$(($(date +%s) - start))
wait "$slow_pid"
say "stop took ${stop_seconds}s (stop_grace_period ${grace}s)"
check "shutdown: in-flight request got 200" status_is slow 200
check "shutdown: container exited 0" [ "$(docker inspect -f '{{.State.ExitCode}}' "$cid")" = 0 ]
check "shutdown: stopped before the grace period ran out" [ "$stop_seconds" -lt "$grace" ]

say "== ledger after shutdown"
check "ledger: the in-flight request settled before exit" [ "$(usage_field SETTLED)" = 7 ]
check "ledger: no call left open" [ "$(usage_field RESERVED)/$(usage_field IN_FLIGHT)/$(usage_field USAGE_UNKNOWN)/$(usage_field reserved)" = "0/0/0/0" ]
qfctl verify >"$work/verify.txt" || true
check "ledger: qfctl verify passes" [ "$(cat "$work/verify.txt")" = "ledger ok" ]

say "== kill -9 and recovery"
# 上游照样计费、QF 却拿不到结果：fake-billed-hang 收到请求就记账，然后挂住。请求发出后直接杀掉 QF（SIGKILL），
# 重启；新进程的后台回收在上界之后把它标成未知；用假上游账单里这一行对账：预演不改，执行结算，再执行一遍不改。
docker compose up -d --no-recreate --wait quotaforge >/dev/null 2>&1
bill_rows() { fake_bill | wc -l; }
rows_before=$(bill_rows)
post killed /v1/chat/completions "$KEY" '{"model":"fake-billed-hang","messages":[{"role":"user","content":"kill me"}]}' &
killed_pid=$!
billed_more() { [ "$(bill_rows)" -gt "$rows_before" ]; }
check "kill: the upstream billed the request" wait_for 20 billed_more
killed_id=$(fake_bill | tail -n 1 | cut -d, -f1)
check "kill: the call is IN_FLIGHT before the kill" [ "$(usage_field IN_FLIGHT)" = 1 ]
docker compose kill -s KILL quotaforge >/dev/null 2>&1
wait "$killed_pid" || true
check "kill: the client got no answer" status_is killed 000
check "kill: the container was killed (exit 137)" [ "$(docker inspect -f '{{.State.ExitCode}}' "$cid")" = 137 ]
killed_at=$(date +%s)
docker compose up -d --no-recreate --wait quotaforge >/dev/null 2>&1
check "kill: still IN_FLIGHT after the restart" [ "$(usage_field IN_FLIGHT)" = 1 ]
is_unknown() { qfctl unknown -account "$ACCOUNT" | cut -f1 | grep -qx "$killed_id"; }
# 上界从预留算起；后台每 上界/4 回收一次。
check "kill: the reclaim marked it USAGE_UNKNOWN" wait_for "$((RECLAIM_AFTER_SECONDS + RECLAIM_AFTER_SECONDS / 4 + 30))" is_unknown
say "reclaimed $(($(date +%s) - killed_at))s after the kill (bound ${RECLAIM_AFTER_SECONDS}s from the reservation)"
fake_bill | awk -F, -v id="$killed_id" 'NR == 1 || $1 == id' >"$work/killed-bill.csv"
check "kill: the bill has the call as consumed" grep -q "^$killed_id,yes," "$work/killed-bill.csv"
reconcile() { qfctl reconcile -bill - "$@" <"$work/killed-bill.csv"; }
consumed_before=$(usage_field consumed)
reconcile >"$work/reconcile-dry.txt" && dry_rc=0 || dry_rc=$?
check "reconcile: dry run exits 0 and would settle the call" bash -c '[ "$1" = 0 ] && grep -q "^would-settle	$2	" "$3"' _ "$dry_rc" "$killed_id" "$work/reconcile-dry.txt"
check "reconcile: the dry run changed nothing" [ "$(usage_field USAGE_UNKNOWN)/$(usage_field consumed)" = "1/$consumed_before" ]
reconcile -apply >"$work/reconcile-apply.txt" && apply_rc=0 || apply_rc=$?
check "reconcile: -apply exits 0 and settles the call" bash -c '[ "$1" = 0 ] && grep -q "^settled	$2	" "$3"' _ "$apply_rc" "$killed_id" "$work/reconcile-apply.txt"
reconcile -apply >"$work/reconcile-again.txt" && again_rc=0 || again_rc=$?
check "reconcile: a second -apply changes nothing" bash -c '[ "$1" = 0 ] && grep -q "^nothing to change: 1 consistent" "$2"' _ "$again_rc" "$work/reconcile-again.txt"
say "qfctl reconcile output (dry run, -apply, -apply again):"
cat "$work/reconcile-dry.txt" "$work/reconcile-apply.txt" "$work/reconcile-again.txt" | tee -a "$SUMMARY" # 摘要之外也进日志（调用号、状态、用量数）
lookup lookup_killed "$killed_id" "$KEY"
check "kill: lookup shows SETTLED by reconcile on the bill" body_ok lookup_killed '.status == "SETTLED" and .resolution.by == "reconcile" and .resolution.evidence == "bill"'
check "kill: consumed grew by the billed usage" [ "$(usage_field consumed)" = "$((consumed_before + $(cut -d, -f5 <"$work/killed-bill.csv" | tail -n 1)))" ]
check "ledger: nothing left open after the recovery" [ "$(usage_field RESERVED)/$(usage_field IN_FLIGHT)/$(usage_field USAGE_UNKNOWN)/$(usage_field reserved)" = "0/0/0/0" ]
qfctl verify >"$work/verify.txt" || true
check "ledger: qfctl verify passes after the recovery" [ "$(cat "$work/verify.txt")" = "ledger ok" ]

say "== call log"
docker compose logs --no-log-prefix quotaforge >"$work/qf.log"
# 重放的两次另外核对：它们的 x-request-id 是原来的调用号，日志行自己的 request_id 是新的。
calls=(chat tools embeddings bad_key no_model quota idem_first idem_reused idem_slow idem_busy idem_bad_key slow)
replays=(idem_replay idem_after)
call_lines() { jq -R -c 'fromjson? | select(.msg == "call")' "$work/qf.log"; }
one_line_per_call() {
  local name=$1 id
  id=$(request_id "$name")
  [ "$(call_lines | jq -c --arg id "$id" --arg crid "$(cat "$work/$name.crid")" --argjson st "$(cat "$work/$name.status")" \
    'select(.request_id == $id and .client_request_id == $crid and .status == $st)' | wc -l)" -eq 1 ]
}
for name in "${calls[@]}"; do
  check "log: one line for $name with its client request id and status" one_line_per_call "$name"
done
# 重放的行：replay_of 是原来的调用号，没有 usage
one_replay_line() {
  [ "$(call_lines | jq -c --arg id "$(request_id "$1")" --arg crid "$(cat "$work/$1.crid")" \
    'select(.replay_of == $id and .client_request_id == $crid and .status == 200 and (has("usage") | not))' | wc -l)" -eq 1 ]
}
for name in "${replays[@]}"; do
  check "log: one replay line for $name with replay_of and no usage" one_replay_line "$name"
done
# 有界结算的那一次另外核对过（没有 status）；被杀的那一次没有日志行。查询的行是 lookup，不算调用。
check "log: $((${#calls[@]} + ${#replays[@]} + 1)) call lines in total" [ "$(call_lines | wc -l)" -eq "$((${#calls[@]} + ${#replays[@]} + 1))" ]
check "log: no line for the killed call" [ "$(call_lines | jq -c --arg id "$killed_id" 'select(.request_id == $id)' | wc -l)" -eq 0 ]
check "log: lookups are not call lines" [ "$(jq -R -c 'fromjson? | select(.msg == "lookup")' "$work/qf.log" | wc -l)" -eq 5 ]
not_in_log() { ! grep -qF -- "$1" "$work/qf.log"; }
check "log: no QF key" not_in_log "$KEY"
docker compose logs --no-log-prefix qfctl qf-mysql >"$work/other.log" 2>&1
no_db_password() {
  local pw
  for pw in "${DB_PASSWORDS[@]}"; do ! grep -qF -- "$pw" "$work/qf.log" "$work/other.log" "$work/qfctl.err" || return 1; done
}
check "log: no database password in the logs of quotaforge, qfctl and qf-mysql" no_db_password
check "log: no upstream key" not_in_log "$UPSTREAM_KEY"
check "log: no Authorization header" not_in_log "Bearer"
check "log: no request body" not_in_log "$marker"
check "log: no response body" not_in_log "fake reply"
qfctl_output_clean() {
  ! grep -sqF -e "fake reply" -e "$marker" -e "$KEY" "$work/verify.txt" "$work/purge.txt" "$work/qfctl.err" "$work"/reconcile-*.txt "$work"/lookup_*.body
}
check "qfctl and lookups: no request or response body and no key in their output" qfctl_output_clean
check "log: no tool definitions" not_in_log "smoke_lookup"

say "== usage export"
# 有解析不了的行时以 1 退出：先接住退出码再检查，不让 set -e 直接中止脚本
export_code=0
docker run --rm -i quotaforge:local usage-export <"$work/qf.log" >"$work/usage.csv" 2>"$work/export.err" || export_code=$?
check "usage-export: exits 0" [ "$export_code" = 0 ]
check "usage-export: no unparseable lines" [ ! -s "$work/export.err" ]
check "usage-export: one row per call line" [ "$(($(wc -l <"$work/usage.csv") - 1))" = "$((${#calls[@]} + ${#replays[@]} + 1))" ]
# 重放的行：最后一列 replay_of 是原来的调用号，usage 三列为空
check "usage-export: the replay row has replay_of and no usage" \
  grep -q ",200,,,,,[0-9]*,$(request_id idem_first),\$" "$work/usage.csv"
cat "$work/usage.csv" >>"$SUMMARY"

say "== rate limits (Redis)"
# 限流的检查放在最后：前面按条数核对的调用日志和用量导出不受这里的请求影响。
redis_cid=$(docker compose ps -q qf-redis)
check "settings: qf-redis runs redis:8" [ "$(docker inspect -f '{{.Config.Image}}' "$redis_cid")" = redis:8 ]
check "settings: qf-redis publishes no ports" [ "$(docker inspect "$redis_cid" | jq -c '[.[0].HostConfig.PortBindings // {} | .[][]?] | length')" = 0 ]
check "settings: qf-redis is only on the default network, not qs-qf" \
  [ "$(docker inspect "$redis_cid" | jq -c '[.[0].NetworkSettings.Networks | keys[]]')" = "[\"${COMPOSE_PROJECT_NAME}_default\"]" ]
redis_config() { docker compose exec -T qf-redis redis-cli config get "$1" | tail -n 1 | tr -d '\r'; }
check "settings: qf-redis does not persist (save '', appendonly no)" [ "$(redis_config save)/$(redis_config appendonly)" = "/no" ]
# call_line <名字>：这次请求的调用日志行（按响应的 x-request-id 找）。
call_line() { docker compose logs --no-log-prefix quotaforge | jq -R -c --arg id "$(request_id "$1")" 'fromjson? | select(.msg == "call" and .request_id == $id)'; }
line_ok() { call_line "$1" | jq -e "${@:2}" >/dev/null; }
retry_after_within() { [[ "$(header "$1" Retry-After)" =~ ^[0-9]+$ ]] && [ "$(header "$1" Retry-After)" -ge 1 ] && [ "$(header "$1" Retry-After)" -le "$2" ]; }
rl_body='{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"rate"}]}'
rejected='.status == 429 and (has("reserved_tokens") | not) and (has("call_status") | not)'

# RPM 为 2 的 Key：连发 3 次。补回 1 个令牌要 30 秒，3 次请求之间只隔几十毫秒，第 3 次一定被拒。
qfctl account create -name "$ACCOUNT-rpm" -limit 1000000 >/dev/null
RPM_KEY=$(qfctl key issue -account "$ACCOUNT-rpm" -models qs-fake-upstream-v1 -rpm 2 | sed -n 's/^key: //p')
for n in 1 2 3; do post "rpm$n" /v1/chat/completions "$RPM_KEY" "$rl_body"; done
check "rpm: the first two requests 200" bash -c '[ "$(cat "$1/rpm1.status")/$(cat "$1/rpm2.status")" = 200/200 ]' _ "$work"
check "rpm: the third 429 rate_limited (rate_limit_error)" body_ok rpm3 '.error.code == "rate_limited" and .error.type == "rate_limit_error"'
check "rpm: 429 status" status_is rpm3 429
check "rpm: Retry-After is an integer from 1 to 30 (60/2)" retry_after_within rpm3 30
check "rpm: logged as key/rpm, nothing reserved" line_ok rpm3 ".rate_limited_by == \"key/rpm\" and $rejected"
lookup lookup_rpm3 "$(request_id rpm3)" "$RPM_KEY"
check "rpm: the limited request is not in the ledger (404)" status_is lookup_rpm3 404
rpm_used=$(($(jq .usage.total_tokens "$work/rpm1.body") + $(jq .usage.total_tokens "$work/rpm2.body")))
check "rpm: the ledger has the two calls and only their usage" \
  [ "$(account_field "$ACCOUNT-rpm" SETTLED)/$(account_field "$ACCOUNT-rpm" consumed)/$(account_field "$ACCOUNT-rpm" reserved)" = "2/$rpm_used/0" ]

# 账户这一层：账户 RPM 为 2，两把 Key 本身不限，共用账户的桶。
qfctl account create -name "$ACCOUNT-acct" -limit 1000000 -rpm 2 >/dev/null
ACCT_KEY_A=$(qfctl key issue -account "$ACCOUNT-acct" -models qs-fake-upstream-v1 | sed -n 's/^key: //p')
ACCT_KEY_B=$(qfctl key issue -account "$ACCOUNT-acct" -models qs-fake-upstream-v1 | sed -n 's/^key: //p')
post acct_a1 /v1/chat/completions "$ACCT_KEY_A" "$rl_body"
post acct_a2 /v1/chat/completions "$ACCT_KEY_A" "$rl_body"
post acct_b /v1/chat/completions "$ACCT_KEY_B" "$rl_body"
check "account: key A twice 200" bash -c '[ "$(cat "$1/acct_a1.status")/$(cat "$1/acct_a2.status")" = 200/200 ]' _ "$work"
check "account: then key B 429 rate_limited" body_ok acct_b '.error.code == "rate_limited"'
check "account: logged as account/rpm" line_ok acct_b ".rate_limited_by == \"account/rpm\" and $rejected"

# TPM 的退还：Key 的 TPM 1500，请求带 max_tokens 1000，估算约 1100（请求体字节数 + 1000），实际约 16。
# 第一次结算后退回“估算 − 实际”，桶里约 1484，第二次放得进；没有退还的话桶里约 400，按 25/秒补要约 28 秒。
# 对照：另一把同样的 Key 先发用量未知的请求（不退），第二次就被拒。
qfctl account create -name "$ACCOUNT-tpm" -limit 1000000 >/dev/null
TPM_KEY=$(qfctl key issue -account "$ACCOUNT-tpm" -models qs-fake-upstream-v1,fake-no-usage -tpm 1500 | sed -n 's/^key: //p')
KEPT_KEY=$(qfctl key issue -account "$ACCOUNT-tpm" -models qs-fake-upstream-v1,fake-no-usage -tpm 1500 | sed -n 's/^key: //p')
tpm_body='{"model":"qs-fake-upstream-v1","max_tokens":1000,"messages":[{"role":"user","content":"refund"}]}'
post tpm1 /v1/chat/completions "$TPM_KEY" "$tpm_body"
post tpm2 /v1/chat/completions "$TPM_KEY" "$tpm_body"
check "tpm: the estimate is 1000-1200 tokens (the numbers above rely on it)" line_ok tpm1 '.reserved_tokens >= 1000 and .reserved_tokens <= 1200'
check "tpm: first 200 and settled" line_ok tpm1 '.status == 200 and .outcome == "SETTLED"'
check "tpm: second 200: it fits only with the refund" status_is tpm2 200
post kept1 /v1/chat/completions "$KEPT_KEY" '{"model":"fake-no-usage","max_tokens":1000,"messages":[{"role":"user","content":"refund"}]}'
post kept2 /v1/chat/completions "$KEPT_KEY" "$tpm_body"
check "tpm control: usage unknown: 200, nothing refunded" line_ok kept1 '.status == 200 and .outcome == "USAGE_UNKNOWN"'
check "tpm control: the next request 429 rate_limited" body_ok kept2 '.error.code == "rate_limited"'
check "tpm control: logged as key/tpm" line_ok kept2 ".rate_limited_by == \"key/tpm\" and $rejected"

# Redis 停了：smoke 账户本身不限，但上游 qf-fake 配了限额，所以每个请求都要经过 Redis。
qf_started=$(docker inspect -f '{{.State.StartedAt}}' "$cid")
settled_before=$(usage_field SETTLED)
docker compose stop qf-redis >/dev/null 2>&1
post redis_down /v1/chat/completions "$KEY" "$rl_body"
check "redis down: 503 rate_limiter_unavailable" body_ok redis_down '.error.code == "rate_limiter_unavailable" and .error.type == "server_error"'
check "redis down: 503 status, no Retry-After" bash -c '[ "$(cat "$1/redis_down.status")" = 503 ] && ! grep -qi "^retry-after:" "$1/redis_down.headers"' _ "$work"
check "redis down: logged with the Redis error, nothing reserved" \
  line_ok redis_down '.status == 503 and (.rate_limiter_error | length > 0) and (has("reserved_tokens") | not)'
lookup lookup_redis_down "$(request_id redis_down)" "$KEY"
check "redis down: not in the ledger (404)" status_is lookup_redis_down 404
check "redis down: no new call in the ledger" [ "$(usage_field SETTLED)/$(usage_field reserved)" = "$settled_before/0" ]
docker compose start qf-redis >/dev/null 2>&1
redis_healthy() { [ "$(docker inspect -f '{{.State.Health.Status}}' "$redis_cid")" = healthy ]; }
check "redis back: healthy again" wait_for 60 redis_healthy
post redis_back /v1/chat/completions "$KEY" "$rl_body"
check "redis back: the first request after it is 200" status_is redis_back 200
check "redis back: QF was not restarted" [ "$(docker inspect -f '{{.State.StartedAt}}' "$cid")" = "$qf_started" ]
qfctl verify >"$work/verify.txt" || true
check "ledger: qfctl verify passes after the rate limit checks" [ "$(cat "$work/verify.txt")" = "ledger ok" ]
docker compose logs --no-log-prefix quotaforge >"$work/qf.log"
rl_keys_not_logged() {
  local k
  for k in "$RPM_KEY" "$ACCT_KEY_A" "$ACCT_KEY_B" "$TPM_KEY" "$KEPT_KEY"; do ! grep -qF -- "$k" "$work/qf.log" || return 1; done
}
check "log: no key of the rate limit checks" rl_keys_not_logged
check "log: no request body of the rate limit checks" not_in_log '"refund"'

say "== result: $failures failed"
[ "$failures" = 0 ]
