#!/usr/bin/env bash
# 联调覆盖文件 compose.qs-fake.yaml 的连通检查（云端和 CI 用）。只验证 QF 这一侧，不用 QS 的检出：
# 在外部网络 qs-qf 上起一个临时替身代替 QS 的假上游：QF 自己的假上游镜像，网络别名 fake-upstream，端口 8000。
# 接 QS 的真假上游跑整套联调的是 compose-qs-e2e.sh。
# 替身只在本脚本里，不进仓库的 Compose 文件（和 QS 约定：QF 自己的假上游服务不能叫 fake-upstream）。
# 这不是 QS 端到端，只验证网络、联调配置和覆盖文件是通的，以及账本数据库不在 qs-qf 上；联调配置开着嵌入缓存，
# 同一个嵌入发两次，第二次命中。
# QF Key 由 qfctl 现发，只存在本脚本的临时目录里，不打印。摘要写到 evidence/compose-qs-fake-check-summary.txt。
# 用自己的 Compose 项目名 qf-qs-fake-check：down -v 只拆自己的项目，不碰默认项目 quotaforge。
# qfctl 的 stderr 写进临时目录，脚本失败时打印出来（Key 在 stdout，不在 stderr）。
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-qs-fake-check # 优先于 compose.yaml 里的 name

readonly STANDIN=qf-qs-fake-standin
readonly BASE=http://127.0.0.1:8080
readonly SUMMARY=evidence/compose-qs-fake-check-summary.txt

work=$(mktemp -d)
created_network=0
failures=0
mkdir -p evidence
: >"$SUMMARY"

say() { echo "$*" | tee -a "$SUMMARY"; }
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
  docker rm -f "$STANDIN" >/dev/null 2>&1 || true
  if [ "$created_network" = 1 ]; then docker network rm qs-qf >/dev/null 2>&1 || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose --progress plain build
if ! docker network inspect qs-qf >/dev/null 2>&1; then
  docker network create qs-qf >/dev/null
  created_network=1
fi
docker rm -f "$STANDIN" >/dev/null 2>&1 || true
docker run -d --name "$STANDIN" --network qs-qf --network-alias fake-upstream \
  --read-only --cap-drop ALL quotaforge-fakeupstream:local -addr 0.0.0.0:8000 >/dev/null

docker compose -f compose.yaml -f compose.qs-fake.yaml --progress plain up -d --wait quotaforge
# 带上覆盖文件：qf-redis-cache 只在 compose.qs-fake.yaml 里。
networks() { docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}' "$(docker compose -f compose.yaml -f compose.qs-fake.yaml ps -a -q "$1")"; }
say "quotaforge networks: $(networks quotaforge)"
check "quotaforge is on qs-qf" grep -qw qs-qf <<<"$(networks quotaforge)"
not_on_qs_qf() { ! grep -qw qs-qf <<<"$(networks "$1")"; }
check "qf-mysql is not on qs-qf" not_on_qs_qf qf-mysql
check "qfctl is not on qs-qf" not_on_qs_qf qfctl
check "qf-redis-cache is not on qs-qf" not_on_qs_qf qf-redis-cache
check "qf-redis is not on qs-qf" not_on_qs_qf qf-redis
check "qf-fake-upstream is not started" [ -z "$(docker compose ps -q qf-fake-upstream)" ]

# 现发 QF Key：Key 只写进只有本用户能读的请求头文件。
qfctl() { docker compose run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
# 限流的限额远高于联调的用量：每个请求都经过准入和修正，但不会被拒。
qfctl account create -name queryshield -limit 1000000 -rpm 6000 -tpm 100000000 >/dev/null
key=$(qfctl key issue -account queryshield -models qs-fake-upstream-v1 -rpm 3000 -tpm 50000000 | sed -n 's/^key: //p')
printf 'Authorization: Bearer %s\n' "$key" >"$work/auth-header"

# post <名字> <路径> <请求体>：正文、响应头、状态码各存一个文件
post() {
  curl -sS -o "$work/$1.body" -D "$work/$1.headers" -w '%{http_code}' -H @"$work/auth-header" -H 'Content-Type: application/json' \
    --data-binary "$3" "$BASE$2" >"$work/$1.status" || echo 000 >"$work/$1.status"
}
status_is() { [ "$(cat "$work/$1.status")" = "$2" ]; }
model_is() { jq -e --arg m "$2" '.model == $m' "$work/$1.body" >/dev/null; }

post chat /v1/chat/completions '{"model":"qs-fake-upstream-v1","messages":[{"role":"user","content":"hi"}]}'
check "chat via qs-fake-upstream-v1: 200" status_is chat 200
check "chat: model is the stand-in's" model_is chat qs-fake-upstream-v1-20260101
post embeddings /v1/embeddings '{"model":"qs-fake-upstream-v1","input":["hi"]}'
check "embeddings via qs-fake-upstream-v1: 200" status_is embeddings 200
check "embeddings: model is the stand-in's" model_is embeddings qs-fake-upstream-v1-20260101
# 不带 Idempotency-Key 的嵌入查缓存：第一次没命中、回源，同样的请求再发一次命中，正文逐字节相同（上游的 id、usage 也一样）。
post embeddings-again /v1/embeddings '{"model":"qs-fake-upstream-v1","input":["hi"]}'
cache_status() { tr -d '\r' <"$work/$1.headers" | awk 'tolower($1) == "cache-status:" { sub(/^[^:]*: */, ""); print }'; }
hit_again() {
  status_is embeddings-again 200 && [ "$(cache_status embeddings)" = 'quotaforge; fwd=miss' ] &&
    [ "$(cache_status embeddings-again)" = 'quotaforge; hit' ] && cmp -s "$work/embeddings.body" "$work/embeddings-again.body"
}
check "embeddings cache: the first one is a miss, the same request again a hit ($(cache_status embeddings-again)), same body" hit_again
check "ledger: both calls settled" [ "$(qfctl usage -account queryshield | awk -F'\t' 'NR == 2 { print $7 }')" = 2 ]
usage_column() { qfctl usage -account queryshield | awk -F'\t' -v col="$1" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == col) c = i } NR == 2 { print $c }'; }
check "ledger: the hit is one CACHED call" [ "$(usage_column CACHED)" = 1 ]
not_limited() { ! docker compose logs --no-log-prefix quotaforge | grep -qE 'rate_limited|rate_limiter_unavailable'; }
check "log: no request was rate limited or hit an unavailable rate limiter" not_limited

say "== result: $failures failed"
[ "$failures" = 0 ]
