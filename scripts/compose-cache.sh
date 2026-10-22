#!/usr/bin/env bash
# 嵌入缓存的演示和核对（云端和 CI 用）：compose.yaml 加 compose.cache.yaml，缓存放在单独的 qf-redis-cache 里，
# 条目 5 秒过期；两个假上游（同一个镜像）qf-fake-upstream（配置里叫 fake-a）和 qf-fake-upstream-b（fake-b）。核对：
#   1. 命中：同一个请求发两次，第二次是 CACHED、Cache-Status 是 hit，正文逐字节相同；假上游只收到 1 个；
#      已消费只算一次；调用状态查询查得到 CACHED。
#   2. 按账户隔离：另一个账户发同一个请求，没命中、到上游。
#   3. 带 Idempotency-Key 的不走缓存：没有 Cache-Status，第一次到上游，第二次是重放。
#   4. 额度用完的账户照样命中（没缓存过的请求是 429）。
#   5. 并发合并：暂停 fake-b，8 个相同的请求一起发；等到 QF 的 qf_embedding_cache_waiting 是 7（7 个在等领头的）再取消暂停；
#      fake-b 只收到 1 个，其余 7 个 collapsed、各记一条 CACHED。
#   6. 停掉缓存的 Redis：请求照样 200，日志记 cache: error；起回来之后又能写、能命中（不用重启 QF）。
#   7. 过期：写进去的条目剩余 4.5 到 5.5 秒（5 秒 ±10%）；过期之后再发，没命中、重新到上游。
#   8. 日志一个调用一行，没有 Key、请求正文和缓存的 URL；缓存的键只有账户号和哈希；qfctl verify；用量导出分得出命中的行。
# 任何一项失败都以非 0 退出。用自己的 Compose 项目名 qf-cache：开头和结尾的 down -v 只拆自己的项目。8080 被占就不跑。
# 等待用 wait_for 轮询，截止时间只用来判失败；停掉的缓存在 Docker Desktop 上要等名字查不到（约 4 秒），起回来之后也可能
# 要等一会儿才重新命中，所以只看结局。摘要写到 evidence/compose-cache-summary.txt：只有检查结果，没有 Key 和正文。
# jq 的过滤式写在单引号里正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=qf-cache # 优先于 compose.yaml 里的 name

readonly QF=http://127.0.0.1:8080
readonly REQ_MARKER=cache-request-marker-7d2e # 请求正文里的标记
readonly CACHE_URL=redis://qf-redis-cache:6379/0
readonly SUMMARY=evidence/compose-cache-summary.txt

work=$(mktemp -d)
chmod 700 "$work"
failures=0
sent=0 # 发给 QF 的嵌入请求数：日志里应当正好这么多行 call
mkdir -p evidence
: >"$SUMMARY"

say() { echo "$*" | tee -a "$SUMMARY"; }
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; else say "FAIL $what"; failures=$((failures + 1)); fi
}
dc() { docker compose -f compose.yaml -f compose.cache.yaml "$@"; }
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
# account <名字> <额度>：建一个账户、发一把能用两个别名的 Key，存进 $work/<名字>.key。
account() {
  qfctl account create -name "$1" -limit "$2" >/dev/null
  qfctl key issue -account "$1" -models cache-emb,cache-emb-b | sed -n 's/^key: //p' >"$work/$1.key"
}
# emb <名字> <账户> <别名> <输入> [请求头]：发一个嵌入请求，状态码、响应头、正文存进 $work/<名字>.*。
emb() {
  local extra=()
  if [ $# -ge 5 ]; then extra=(-H "$5"); fi
  curl -sS -o "$work/$1.body" -D "$work/$1.headers" -w '%{http_code}' -H "Authorization: Bearer $(cat "$work/$2.key")" \
    -H 'Content-Type: application/json' ${extra[@]+"${extra[@]}"} --max-time 40 \
    --data-binary "{\"model\":\"$3\",\"input\":[\"$REQ_MARKER $4\"],\"dimensions\":4}" \
    "$QF/v1/embeddings" >"$work/$1.status" || echo 000 >"$work/$1.status"
  sent=$((sent + 1))
}
status_of() { cat "$work/$1.status"; }
header_of() { tr -d '\r' <"$work/$1.headers" | awk -F': ' -v h="$2" 'tolower($1) == tolower(h) { print $2 }'; }
request_id() { header_of "$1" x-request-id; }
code_of() { jq -r '.error.code // "none"' "$work/$1.body" 2>/dev/null || echo none; }
# 假上游的镜像没有 curl，用它自己的 -get；QF 的指标端口只在 Compose 的网络里，经假上游 a 去读。
fake_get() { MSYS_NO_PATHCONV=1 dc exec -T "$1" /fakeupstream -get "http://127.0.0.1:8081$2"; }
embeddings() { fake_get "$1" /_fake/stats | jq .embeddings; }
metric() {
  MSYS_NO_PATHCONV=1 dc exec -T qf-fake-upstream /fakeupstream -get http://quotaforge:9464/metrics |
    awk -v m="$1" '$1 == m { v = $2 } END { print v + 0 }'
}
sql() { MSYS_NO_PATHCONV=1 dc exec -T -e MYSQL_PWD=test-only-root-password qf-mysql mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$1"; }
consumed() { sql "SELECT b.consumed_tokens FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = '$1'"; }
account_id() { sql "SELECT id FROM accounts WHERE name = '$1'"; }
status_in_ledger() { sql "SELECT status FROM calls WHERE id = '$(request_id "$1")'"; }
# log_of <名字> <jq 过滤式>：这次调用的日志行套上过滤式。响应写回之后日志才写，所以等它出现。
log_line() { dc logs --no-log-prefix quotaforge 2>/dev/null | jq -c --arg id "$1" 'select(.msg == "call" and .request_id == $id)'; }
has_log() { [ -n "$(log_line "$1")" ]; }
log_of() {
  local id
  id=$(request_id "$1")
  wait_for 10 has_log "$id" || true
  log_line "$id" | jq -r "$2"
}
rcache() { MSYS_NO_PATHCONV=1 dc exec -T qf-redis-cache redis-cli "$@"; }
keys_of() { rcache --scan --pattern "qf:emb:$(account_id "$1"):*"; }
no_keys() { [ -z "$(keys_of "$1")" ]; }

say "== up"
dc unpause qf-fake-upstream-b >/dev/null 2>&1 || true
dc down -v --remove-orphans >/dev/null 2>&1
if curl -s -o /dev/null --max-time 2 "$QF/"; then
  say "FAIL 127.0.0.1:8080 is already in use by something else (another Compose project of QuotaForge?)"
  exit 1
fi
start_all=$(date +%s)
start=$(date +%s)
dc --progress plain up -d --build --wait
say "up --build --wait: $(($(date +%s) - start))s"
for a in cache-a cache-b cache-c cache-e; do account "$a" 100000000; done
check "setup: issued four keys" [ "$(cat "$work"/cache-*.key | wc -c)" -eq 192 ]
check "setup: qf-redis-cache has a memory limit and evicts by LRU" \
  [ "$(rcache config get maxmemory | tail -1)/$(rcache config get maxmemory-policy | tail -1)" = "67108864/allkeys-lru" ]
check "setup: qf-redis (rate limits) has no memory limit and evicts nothing" \
  [ "$(MSYS_NO_PATHCONV=1 dc exec -T qf-redis redis-cli config get maxmemory-policy | tail -1)" = noeviction ]

say "== 1. a hit: the same request twice"
e0=$(embeddings qf-fake-upstream)
emb first cache-a cache-emb one
emb second cache-a cache-emb one
check "hit: both 200; Cache-Status miss, then hit" \
  [ "$(status_of first)/$(header_of first Cache-Status)/$(status_of second)/$(header_of second Cache-Status)" = "200/quotaforge; fwd=miss/200/quotaforge; hit" ]
check "hit: the body is byte for byte the first one (with its id)" cmp -s "$work/first.body" "$work/second.body"
check "hit: a new x-request-id" [ "$(request_id first)" != "$(request_id second)" ]
check "hit: the upstream got only the first" [ $(($(embeddings qf-fake-upstream) - e0)) = 1 ]
check "hit: ledger: SETTLED, then CACHED" [ "$(status_in_ledger first)/$(status_in_ledger second)" = SETTLED/CACHED ]
check "hit: consumed counts the first call only" [ "$(consumed cache-a)" = "$(jq .usage.total_tokens "$work/first.body")" ]
check "hit: logged as a hit of the first call, nothing reserved" \
  [ "$(log_of second '[.cache, .cached_from, .call_status, has("reserved_tokens"), has("usage")] | join("/")')" = "hit/$(request_id first)/CACHED/false/false" ]
curl -sS -H "Authorization: Bearer $(cat "$work/cache-a.key")" "$QF/v1/calls/$(request_id second)" >"$work/lookup.json"
check "hit: the call lookup says CACHED, resolved by the gateway on the cache" \
  [ "$(jq -r '[.status, .resolution.by, .resolution.evidence, .sent_at] | join("/")' "$work/lookup.json")" = "CACHED/gateway/cache/" ]

say "== 2. per account: another account sends the same request"
e0=$(embeddings qf-fake-upstream)
emb other cache-b cache-emb one
check "account: a miss, the upstream got it" \
  [ "$(status_of other)/$(header_of other Cache-Status)/$(($(embeddings qf-fake-upstream) - e0))" = "200/quotaforge; fwd=miss/1" ]

say "== 3. an Idempotency-Key bypasses the cache"
e0=$(embeddings qf-fake-upstream)
emb idem1 cache-a cache-emb one "Idempotency-Key: cache-idem-1"
emb idem2 cache-a cache-emb one "Idempotency-Key: cache-idem-1"
check "idempotency: no Cache-Status; the first went upstream, the second is a replay" \
  [ "$(header_of idem1 Cache-Status)/$(header_of idem2 Cache-Status)/$(header_of idem2 Idempotent-Replayed)/$(($(embeddings qf-fake-upstream) - e0))" = "//true/1" ]
check "idempotency: logged as bypass" [ "$(log_of idem1 .cache)" = bypass ]

say "== 4. an account with no quota left still hits"
emb quota1 cache-c cache-emb four
qfctl account set-limit -name cache-c -limit "$(consumed cache-c)" >/dev/null
emb quota2 cache-c cache-emb four-new
emb quota3 cache-c cache-emb four
check "quota: an uncached request is 429 quota_exhausted" [ "$(status_of quota2)/$(code_of quota2)" = "429/quota_exhausted" ]
check "quota: the cached one is 200 and CACHED" [ "$(status_of quota3)/$(header_of quota3 Cache-Status)/$(status_in_ledger quota3)" = "200/quotaforge; hit/CACHED" ]

say "== 5. coalescing: 8 identical requests while fake-b is paused"
b0=$(embeddings qf-fake-upstream-b)
dc pause qf-fake-upstream-b >/dev/null 2>&1
for i in $(seq 1 8); do emb "c-$i" cache-a cache-emb-b five & done
waiting7() { [ "$(metric qf_embedding_cache_waiting)" = 7 ]; }
check "coalescing: 7 requests wait for the one at the paused upstream" wait_for 30 waiting7
dc unpause qf-fake-upstream-b >/dev/null 2>&1
wait
sent=$((sent + 8)) # emb 在子进程里跑，计数没有加回来
statuses=$(for i in $(seq 1 8); do echo "$(status_of "c-$i")/$(header_of "c-$i" Cache-Status)"; done | sort | uniq -c | awk '{ $1 = $1 "x"; print }' | paste -sd, -)
say "coalescing: $statuses"
check "coalescing: all 200; one went upstream, 7 collapsed onto it" \
  [ "$statuses" = "1x 200/quotaforge; fwd=miss,7x 200/quotaforge; fwd=miss; collapsed" ]
check "coalescing: fake-b got 1" [ $(($(embeddings qf-fake-upstream-b) - b0)) = 1 ]
ledger5=$(for i in $(seq 1 8); do status_in_ledger "c-$i"; done | sort | uniq -c | awk '{ print $2 "=" $1 }' | paste -sd, -)
check "coalescing: ledger: 7 CACHED, 1 SETTLED ($ledger5)" [ "$ledger5" = "CACHED=7,SETTLED=1" ]
check "coalescing: nobody waits any more" [ "$(metric qf_embedding_cache_waiting)" = 0 ]

say "== 6. the cache Redis is stopped"
dc stop qf-redis-cache >/dev/null 2>&1
e0=$(embeddings qf-fake-upstream)
start=$(date +%s)
emb down cache-a cache-emb six
say "a request with the cache down took $(($(date +%s) - start))s"
check "cache down: still 200, went upstream" [ "$(status_of down)/$(header_of down Cache-Status)/$(($(embeddings qf-fake-upstream) - e0))" = "200/quotaforge; fwd=miss/1" ]
check "cache down: logged as cache error with the reason, settled" \
  [ "$(log_of down '[.cache, (.cache_error | length > 0), .call_status] | join("/")')" = "error/true/SETTLED" ]
check "cache down: counted as a lookup error, not a dependency error" \
  [ "$(metric 'qf_embedding_cache_lookups_total{result="error"}')/$(metric 'qf_dependency_errors_total{dependency="cache",operation="get"}')" = 1/0 ]
dc start qf-redis-cache >/dev/null 2>&1
dc --progress plain up -d --wait qf-redis-cache >/dev/null 2>&1
n=0
hit_again() {
  n=$((n + 1))
  emb "back-$n" cache-a cache-emb seven
  [ "$(header_of "back-$n" Cache-Status)" = "quotaforge; hit" ]
}
start=$(date +%s)
check "cache back: writes and hits again without restarting QF" wait_for 30 hit_again
say "cache back: the first hit came $(($(date +%s) - start))s after the cache was healthy ($n requests)"

say "== 7. expiry"
e0=$(embeddings qf-fake-upstream)
# 毫秒的钟用缓存 Redis 的 TIME（秒和微秒两行）：不靠 GNU date 的 %N，也和条目的过期时间是同一个钟。
ms() { rcache time | tr -d '\r' | awk 'NR == 1 { s = $1 } NR == 2 { printf "%d\n", s * 1000 + int($1 / 1000) }'; }
t0=$(ms)
emb exp1 cache-e cache-emb eight
keys_of cache-e >"$work/exp.keys"
ttl=$(rcache pttl "$(head -1 "$work/exp.keys")")
elapsed=$(($(ms) - t0)) # 从发请求到读出剩余时间：写入时的过期时长在 [剩余, 剩余 + 这段时间] 之间
say "expiry: the entry expires in ${ttl}ms, read ${elapsed}ms after the request was sent (written with 5000 ms ±10%)"
ttl_ok() { [ "$(wc -l <"$work/exp.keys")" -eq 1 ] && [ "$ttl" -le 5500 ] && [ $((ttl + elapsed)) -ge 4500 ]; }
check "expiry: one key for the account, written to expire in 4500-5500 ms" ttl_ok
check "expiry: the entry is gone after its TTL" wait_for 15 no_keys cache-e
emb exp2 cache-e cache-emb eight
emb exp3 cache-e cache-emb eight
check "expiry: then a miss that goes upstream again, then a hit" \
  [ "$(header_of exp2 Cache-Status)/$(header_of exp3 Cache-Status)/$(($(embeddings qf-fake-upstream) - e0))" = "quotaforge; fwd=miss/quotaforge; hit/2" ]

say "== 8. logs, cache keys and ledger"
dc logs --no-log-prefix quotaforge >"$work/qf.log" 2>&1
check "logs: one call line per request ($sent)" [ "$(jq -c 'select(.msg == "call")' "$work/qf.log" | wc -l)" -eq "$sent" ]
leaks=0
for s in $(cat "$work"/cache-*.key) Bearer "$REQ_MARKER" test-only-upstream-key "$CACHE_URL" redis://; do
  leaks=$((leaks + $(grep -cF -- "$s" "$work/qf.log" || true)))
done
check "logs: no key, upstream key, request body or cache URL ($leaks matches)" [ "$leaks" = 0 ]
rcache --scan --pattern '*' >"$work/keys"
check "cache keys: only qf:emb:<account id>:<sha-256> ($(wc -l <"$work/keys" | tr -d ' ') keys)" \
  [ "$(grep -cvE '^qf:emb:[0-9]+:[0-9a-f]{64}$' "$work/keys" || true)" = 0 ]
check "cache keys: no request text" [ "$(grep -c "$REQ_MARKER" "$work/keys" || true)" = 0 ]
docker run --rm -i quotaforge:local usage-export <"$work/qf.log" >"$work/usage.csv"
check "usage-export: the hit row has cached_from and no usage" \
  grep -q ",200,,,,,[0-9]*,,$(request_id first)\$" <(grep -F "$(request_id second)" "$work/usage.csv")
check "ledger: qfctl verify" [ "$(qfctl verify)" = "ledger ok" ]

say "== result: $failures failed ($(($(date +%s) - start_all))s)"
[ "$failures" = 0 ]
