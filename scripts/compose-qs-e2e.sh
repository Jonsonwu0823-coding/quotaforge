#!/usr/bin/env bash
# QS 端到端联调（fake-upstream 模式）：QF 接 QS 的假上游，QS 的 app 经 QF 调模型，一条命令跑五个场景（用法见 docs/queryshield.md 的“端到端联调”）：
#   1. 正常：QS 的整套演示，QF 的账本逐个 run 对上，调用日志和账本一一对应；
#   2. 跨服务 trace：QS 自己的 span 也导出到 QF 的 Jaeger，每个 run 的 trace 里两边的 span 父子相接；
#   3. 额度耗尽：账户的剩余设成 0，QF 在预留时拒绝，QS 的 run 以 model_quota_exhausted 结束，被拒的请求不进账本、不调上游；
#   4. 上游超时：暂停 QS 的假上游，QF 的 chat 超时（25 秒）先于 QS 的 30 秒到点，回 504；这次调用记用量未知、占着预留；
#   5. 两个实例：quotaforge-b 和 quotaforge 共用网络别名，QS 的调用分到两边；再杀掉一个实例，另一个回收它中途的调用。
#   用法：scripts/compose-qs-e2e.sh <QS 公开仓库的本地目录>      （也可以用环境变量 QS_DIR）
# QS 目录必须干净地检出在固定提交上；脚本不往里写任何文件。QS 的镜像缺了才构建，已有同名镜像时直接用。
# 每个场景一个 QF 账户和一把 Key（QS 在进程启动时读 Key，换账户就重建 QS 的 app），各自对账。逐个 run 的核对、调用日志
# 和 Jaeger 的检查由 scripts/check_qs_runs.py 做，和经 QS 的真实演示的对账共用同一份代码；QS 的单个问题由 scripts/qs_query.py 提。
# 一个场景的检查失败只计数，后面的场景照跑；每个场景报自己的项数，最后一行是总的失败数。
# 等待都等确定的事件（span 进了 Jaeger、调用已经发出、容器已经暂停、名字只解析到预期的实例），截止时间只用来判失败。
# 联调用自己的项目名、镜像标签和端口，不碰本机已有的 QF、QS 环境；开始前发现冲突就报错退出，只删自己建的东西。
# QF 的 Key 由 qfctl 现发，只经环境变量交给 QS，不写文件、不打印。摘要写到 evidence/compose-qs-e2e-summary.txt。
# qfctl 的 stderr 写进临时目录，脚本失败时打印出来（Key 在 stdout，不在 stderr）。
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash：不要把传给 docker 的容器内路径改写成 Windows 路径
# 用第一个真能运行的：Windows 上的 python3 可能只是应用商店的占位程序（打印 Python was not found，退出 49）。
PY=
for candidate in python3 python; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys' >/dev/null 2>&1; then PY=$candidate; break; fi
done
[ -n "$PY" ] || { echo "FAIL 找不到能运行的 Python（python3、python 都不行；Windows 上的 python3 可能只是应用商店的占位程序）" >&2; exit 1; }
# Windows 的 docker、git、python 看不懂 Git Bash 的 /tmp、/c/... 路径：有 cygpath 就转成 C:/... 写法。
native() { cygpath -m "$1" 2>/dev/null || printf '%s' "$1"; }
QS_DIR=$(native "$(cd "${1:-${QS_DIR:?用法：$0 <QS 公开仓库的本地目录>}}" && pwd)")
cd "$(dirname "$0")/.."

readonly QS_COMMIT=0918d49fcea7f8ae1141ef3847092ca33c736b72
readonly QF_PROJECT=qf-e2e-qf QS_PROJECT=qf-e2e-qs
# 额度 100 万 tokens：整套演示实测消费约 11.1 万（33 次聊天、15 次嵌入），留足余量；且远大于一次预留（请求字节数加 default_max_tokens 1024）。
readonly LIMIT=1000000
readonly SUMMARY=evidence/compose-qs-e2e-summary.txt
readonly JAEGER=http://127.0.0.1:16686
# 等另一个实例回收：上界 36 秒（较大的上游超时 25 秒 + 3 次账本操作各 2 秒 + 余量 5 秒）+ 一轮后台循环 9 秒 + 余量。
readonly RECLAIM_WAIT=60
export QF_E2E_QS_IMAGE=queryshield-app:qf-e2e-${QS_COMMIT:0:7}
export QUERYSHIELD_GATEWAY_NETWORK=qs-qf
export QUERYSHIELD_GATEWAY_BASE_URL=http://quotaforge:8080/v1
export QUERYSHIELD_HTTP_PORT=${QUERYSHIELD_HTTP_PORT:-18731}

work=$(native "$(mktemp -d)")
qs_file=$(native "$PWD/deploy/compose.qs-e2e-qs.yaml")
mkdir -p evidence
: >"$SUMMARY"
failures=0 created_network=0 qf_started=0 qs_started=0 paused=0 key='' keys='' account=''
scenario='' scenario_checks=0 scenario_failed=0 scenario_start=0

say() { echo "$*" | tee -a "$SUMMARY"; }
count() {
  scenario_checks=$((scenario_checks + 1))
  if [ "$1" != ok ]; then scenario_failed=$((scenario_failed + 1)) failures=$((failures + 1)); fi
}
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; count ok; else say "FAIL $what"; count FAIL; fi
}
# tally <文件>：把 check_qs_runs.py 的输出抄进摘要，ok、FAIL 行算进这个场景的项数
tally() {
  local line
  while IFS= read -r line; do
    say "$line"
    case $line in "ok "*) count ok ;; FAIL*) count FAIL ;; esac
  done <"$1"
}
begin() { scenario=$1 scenario_checks=0 scenario_failed=0 scenario_start=$SECONDS; say "== scenario $scenario"; }
finish() { say "== scenario $scenario: $scenario_checks checks, $scenario_failed failed, $((SECONDS - scenario_start)) s"; }
die() { echo "FAIL $*" >&2; exit 1; }
redact() { KEYS=$keys "$PY" -c 'import os, sys
text = sys.stdin.read()
for k in os.environ["KEYS"].split():
    text = text.replace(k, "***")
sys.stdout.write(text)'; }
wait_for() {
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}

# 文件顺序不能换：compose.model-gateway.yaml 必须在 compose.fake-upstream.yaml 后面，否则 app 绕过 QF 直连假上游。
qs() {
  (
    export QUERYSHIELD_MODEL_API_KEY=${key:-unset}
    cd "$QS_DIR"
    docker compose -p "$QS_PROJECT" --env-file "$work/qs.env" --progress plain \
      -f compose.yaml -f compose.fake-upstream.yaml -f compose.model-gateway.yaml -f compose.fake-upstream-gateway.yaml \
      -f "$qs_file" "$@"
  )
}
qf() {
  docker compose -p "$QF_PROJECT" -f compose.yaml -f compose.qs-fake.yaml -f compose.observability.yaml \
    -f compose.two.yaml -f compose.observability.two.yaml -f compose.qs-e2e.yaml --progress plain "$@"
}
qfctl() { qf run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
# 只读 SQL：root 密码在容器里的 secret 文件中，经进程环境变量交给 mysql，不出现在命令行、输出和宿主机上。
# shellcheck disable=SC2016 # $(...) 要在容器里展开，所以用单引号
sql() { qf exec -T qf-mysql sh -c 'MYSQL_PWD=$(cat /run/secrets/mysql_root_password) exec mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$0"' "$1"; }
# 在 QS 的 app 里提一个问题，输出这个 run 的一行记录（只有编号和数字），见 scripts/qs_query.py
qs_query() { qs exec -T app python - <scripts/qs_query.py; }
# outcome <记录>：HTTP 状态、run 的状态、错误码、聊天次数、usage_total 的状态
outcome() { "$PY" -c 'import json, sys; r = json.loads(sys.argv[1]); print(r["http_status"], r["status"], r["error_code"], r["model_call_count"], (r["usage_total"] or {}).get("status"))' "$1"; }
# ask <名字>：提一个问题，记录追加到 $work/<名字>.jsonl，再原样输出
ask() {
  local record
  record=$(qs_query) || die "在 QS 里提问失败（qs_query）"
  echo "$record" >>"$work/$1.jsonl"
  printf '%s' "$record"
}
run_of() { "$PY" -c 'import json, sys; print(json.loads(sys.argv[1])["run_id"])' "$1"; }
# log_sum <日志> <表达式> [参数]：QF 日志里每个 JSON 行 e 上的表达式求和（真算 1）；表达式里 arg 是第三个参数
log_sum() {
  "$PY" - "$@" <<'EOF'
import json, sys
path, expr, arg = sys.argv[1], sys.argv[2], (sys.argv[3:] or [None])[0]
total = 0
for line in open(path, encoding="utf-8", errors="replace"):
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if isinstance(e, dict):
        total += eval(expr, {}, {"e": e, "arg": arg})
print(total)
EOF
}
# in_jaeger <日志> <账户>：日志里这个账户每次调用的 QF span 都已经进了 Jaeger（取数、摊平用 check_qs_runs.py 的函数）
in_jaeger() {
  "$PY" - "$@" "$JAEGER" <<'EOF'
import sys
sys.path.insert(0, "scripts")
import check_qs_runs as cq
path, account, jaeger = sys.argv[1:]
want = {}
for row in cq.read_call_log(path, account):
    want.setdefault(row["trace_id"], set()).add(row["span_id"])
missing = [t for t, spans in want.items() if not spans <= {s["id"] for s in cq.flatten(cq.jaeger_get(f"{jaeger}/api/v3/traces/{t}"))}]
sys.exit(1 if missing else 0)  # 这个实例在演示里一次也没收到时，没有要等的
EOF
}
account_sql() { sql "SELECT $1 FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = '$account'${2:+ AND $2}"; }

cleanup() {
  local rc=$?
  if [ "$paused" = 1 ]; then docker unpause "$(qs ps -q fake-upstream)" >/dev/null 2>&1 || true; fi
  if [ "$rc" != 0 ]; then
    if [ "$qf_started" = 1 ]; then echo "== quotaforge 最后 40 行日志（已隐去 Key）" >&2; qf logs --no-log-prefix --tail 40 quotaforge quotaforge-b 2>&1 | redact >&2 || true; fi
    if [ "$qs_started" = 1 ]; then echo "== QS app 最后 40 行日志（已隐去 Key）" >&2; qs logs --no-log-prefix --tail 40 app 2>&1 | redact >&2 || true; fi
    if [ -s "$work/qfctl.err" ]; then echo "== qfctl stderr" >&2; cat "$work/qfctl.err" >&2; fi
  fi
  if [ "$qs_started" = 1 ]; then qs down -v --remove-orphans >/dev/null 2>&1 || true; fi
  if [ "$qf_started" = 1 ]; then qf down -v --remove-orphans >/dev/null 2>&1 || true; fi
  if [ "$created_network" = 1 ]; then docker network rm qs-qf >/dev/null 2>&1 || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

# use_account <账户>：建账户、发 Key；QS 已经在跑时带新 Key 重建 app（QS 在进程启动时读 Key）。
# 限流的限额远高于联调的用量：每个请求都经过准入和修正，但不会被拒。
use_account() {
  account=$1
  qfctl account create -name "$account" -limit "$LIMIT" -rpm 6000 -tpm 100000000 >/dev/null
  key=$(qfctl key issue -account "$account" -models qs-fake-upstream-v1 -rpm 3000 -tpm 50000000 | sed -n 's/^key: //p')
  [ -n "$key" ] || die "qfctl 没有发出 Key"
  keys="$keys $key"
  if [ "$qs_started" = 1 ]; then qs up -d --no-deps --wait app; fi
}
members_are() {
  local members
  members=$(docker ps -a --filter network=qs-qf --format '{{.Label "com.docker.compose.project"}}/{{.Label "com.docker.compose.service"}}' | sort | tr '\n' ' ')
  check "qs-qf members are exactly $* ($members)" [ "$members" = "$* " ]
}
# run_demo <名字>：跑 QS 的整套演示，摘要取到 $work/<名字>.json
run_demo() {
  local start=$SECONDS
  qs exec -T app python scripts/demo_run.py --mode fake-upstream --base-url http://127.0.0.1:8000 --evidence-dir /tmp/evidence-gateway >"$work/demo.out" 2>&1 || true
  say "demo took $((SECONDS - start)) s"
  qs exec -T app cat /tmp/evidence-gateway/demo-summary.json >"$work/$1.json" 2>/dev/null || die "取不到 demo-summary.json，演示输出最后一行：$(tail -n 1 "$work/demo.out" | redact)"
}
# checker <输出文件> <参数...>：check_qs_runs.py 联调模式加 Jaeger；跑不了（退出码 2）就停下
checker() {
  local out=$1 rc=0
  shift
  "$PY" scripts/check_qs_runs.py --e2e --project "$QF_PROJECT" --account "$account" --jaeger "$JAEGER" "$@" >"$out" || rc=$?
  [ "$rc" != 2 ] || die "check_qs_runs.py 跑不了（原因见上面的 error）"
  # 没跑完（例如抛了没接住的异常，退出码 1、什么都没打印）也要停下，不能让整段核对悄悄消失
  tail -n 1 "$out" | grep -qE '^check_qs_runs: [0-9]+ failed$' || die "check_qs_runs.py 没有跑完（退出码 $rc）"
}
verify() {
  qfctl verify >"$work/verify.txt" || true
  check "qfctl verify prints ledger ok" [ "$(cat "$work/verify.txt")" = "ledger ok" ]
}
fake_upstream() { qs ps -q fake-upstream; }
pause_upstream() {
  docker pause "$(fake_upstream)" >/dev/null
  paused=1
  [ "$(docker inspect -f '{{.State.Paused}}' "$(fake_upstream)")" = true ] || die "QS 的假上游没有暂停"
}
unpause_upstream() { docker unpause "$(fake_upstream)" >/dev/null; paused=0; }
# QS 里 quotaforge 这个名字只解析到这个实例：每次请求都重新解析，切换实例之后要等它稳定，否则请求可能落到刚停掉的实例上
resolves_only_to() {
  local want got
  want=$(docker inspect -f '{{(index .NetworkSettings.Networks "qs-qf").IPAddress}}' "$(qf ps -q "$1")")
  got=$(qs exec -T app python -c 'import socket; print(" ".join(sorted({a[4][0] for a in socket.getaddrinfo("quotaforge", 8080)})))' 2>/dev/null) || return 1
  [ -n "$want" ] && [ "$got" = "$want" ]
}

scenario_normal() {
  begin "1 normal: the whole QS demo through QF (account e2e)"
  members_are "$QF_PROJECT/qf-jaeger $QF_PROJECT/quotaforge $QS_PROJECT/app $QS_PROJECT/fake-upstream"
  check "ledger: the account has no calls yet" [ "$(account_sql 'COUNT(*)')" = 0 ]
  run_demo normal
  # 日志只交给核对脚本，不打印；脚本只输出题号、run_id 和条数。
  qf logs --no-log-prefix quotaforge >"$work/normal.log" 2>/dev/null || die "取不到 QF 的日志（docker compose logs 失败）"
  checker "$work/normal.check" --qf-log "$work/normal.log" "$work/normal.json"
  grep -v '^[a-zA-Z]* *jaeger:' "$work/normal.check" >"$work/normal.ledger" || true
  tally "$work/normal.ledger"
  # 固定提交里的 QS，run 里的调用（带 X-Run-Id）都带 Idempotency-Key，run 外的（建索引的嵌入）都不带。QF 把两者存在 calls 里。
  local keyed_in_run plain_outside_run keyless_in_run keyed_outside_run
  read -r keyed_in_run plain_outside_run keyless_in_run keyed_outside_run < <(account_sql "
    COALESCE(SUM(c.run_id IS NOT NULL AND c.idempotency_key IS NOT NULL), 0), COALESCE(SUM(c.run_id IS NULL AND c.idempotency_key IS NULL), 0),
    COALESCE(SUM(c.run_id IS NOT NULL AND c.idempotency_key IS NULL), 0), COALESCE(SUM(c.run_id IS NULL AND c.idempotency_key IS NOT NULL), 0)")
  keys_match() { [ "$keyed_in_run" -gt 0 ] && [ "$keyless_in_run" = 0 ] && [ "$keyed_outside_run" = 0 ]; }
  check "idempotency keys: $keyed_in_run calls with X-Run-Id carry one, $plain_outside_run without X-Run-Id do not ($keyless_in_run in-run lack a key, $keyed_outside_run out-of-run have one)" \
    keys_match
  verify
  check "log: no request was rate limited or hit an unavailable rate limiter" \
    [ "$(log_sum "$work/normal.log" "e.get('error_code') in ('rate_limited', 'rate_limiter_unavailable')")" = 0 ]
  finish
}

scenario_trace() {
  begin "2 cross-service trace: QS's and QF's spans of the demo's runs in one Jaeger"
  grep '^[a-zA-Z]* *jaeger:' "$work/normal.check" >"$work/normal.jaeger" || true
  tally "$work/normal.jaeger"
  finish
}

scenario_quota() {
  begin "3 quota exhausted: QF refuses the reservation, QS ends the run with model_quota_exhausted (account e2e-quota)"
  use_account e2e-quota
  local a b
  a=$(ask quota)
  check "run A, before the limit: $(outcome "$a")" [ "$(outcome "$a")" = "200 SUCCEEDED None 2 known" ]
  # 剩余设成 0：之后的预留不管多大都确定被拒，不靠算请求的字节数
  qfctl account set-limit -name "$account" -limit "$(sql "SELECT b.consumed_tokens FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = '$account'")" >/dev/null
  b=$(ask quota)
  check "run B, nothing left: $(outcome "$b")" [ "$(outcome "$b")" = "503 FAILED model_quota_exhausted 1 unknown" ]
  qf logs --no-log-prefix quotaforge >"$work/quota.log" 2>/dev/null || die "取不到 QF 的日志"
  refused_once() {
    [ "$(log_sum "$work/quota.log" "e.get('msg') == 'call' and e.get('run_id') == arg" "$(run_of "$b")")" = 1 ] &&
      [ "$(log_sum "$work/quota.log" "e.get('msg') == 'call' and e.get('run_id') == arg and e.get('status') == 429 and e.get('error_code') == 'quota_exhausted' and 'call_status' not in e and 'upstream_status' not in e" "$(run_of "$b")")" = 1 ]
  }
  check "log: run B has one call line, 429 quota_exhausted, refused before reservation and never sent upstream" refused_once
  checker "$work/quota.check" --qf-log "$work/quota.log" --queries "$work/quota.jsonl"
  tally "$work/quota.check"
  verify
  finish
}

scenario_timeout() {
  begin "4 upstream timeout: QF gives up on a paused upstream after its 25 s chat timeout, the call stays USAGE_UNKNOWN (account e2e-timeout)"
  use_account e2e-timeout
  local c d start call
  c=$(ask timeout)
  check "run C, upstream running: $(outcome "$c")" [ "$(outcome "$c")" = "200 SUCCEEDED None 2 known" ]
  # 先暂停再发请求，没有时序竞争：内核替暂停的进程完成握手、收下请求，QF 已经发出，等不到回答
  pause_upstream
  start=$SECONDS
  d=$(ask timeout)
  say "run D took $((SECONDS - start)) s"
  unpause_upstream
  check "run D, upstream paused: $(outcome "$d")" [ "$(outcome "$d")" = "502 FAILED upstream_http_error 1 unknown" ]
  qf logs --no-log-prefix quotaforge >"$work/timeout.log" 2>/dev/null || die "取不到 QF 的日志"
  check "log: run D's call is 504 upstream_timeout, USAGE_UNKNOWN, after 25 to 30 s (QF answers before QS's own 30 s)" \
    [ "$(log_sum "$work/timeout.log" "e.get('msg') == 'call' and e.get('run_id') == arg and e.get('status') == 504 and e.get('error_code') == 'upstream_timeout' and e.get('call_status') == 'USAGE_UNKNOWN' and 25000 <= e.get('duration_ms', 0) < 30000" "$(run_of "$d")")" = 1 ]
  # 超时之后的对账：未知用量的预留留着，只核对状态。QS 的假上游没有按调用的账单，QF 的日志里结局未知的行也不算证据，所以结清不了。
  call=$(account_sql c.id "c.status = 'USAGE_UNKNOWN'")
  qfctl unknown -account "$account" >"$work/unknown.txt" || true
  listed_alone() { [ -n "$call" ] && [ "$(tail -n +2 "$work/unknown.txt" | cut -f1)" = "$call" ]; }
  check "qfctl unknown lists exactly run D's call" listed_alone
  qfctl reconcile -log - <"$work/timeout.log" >"$work/reconcile.txt" || true
  no_evidence() {
    grep -qx '1 log lines with an unknown outcome are not evidence; ignored' "$work/reconcile.txt" &&
      grep -q '^nothing to change: ' "$work/reconcile.txt" && [ "$(account_sql c.status "c.id = '$call'")" = USAGE_UNKNOWN ]
  }
  check "qfctl reconcile -log: QF's log is no evidence for it (1 line ignored, nothing to change), it stays USAGE_UNKNOWN" no_evidence
  checker "$work/timeout.check" --qf-log "$work/timeout.log" --queries "$work/timeout.jsonl"
  tally "$work/timeout.check"
  verify
  finish
}

scenario_two() {
  begin "5 two instances: QS's calls spread over quotaforge and quotaforge-b; one is killed mid-call and the other takes over (account e2e-two)"
  qf up -d --no-deps --wait quotaforge-b
  members_are "$QF_PROJECT/qf-jaeger $QF_PROJECT/quotaforge $QF_PROJECT/quotaforge-b $QS_PROJECT/app $QS_PROJECT/fake-upstream"
  use_account e2e-two
  run_demo two
  # 接手：只留 A 在别名上，暂停假上游，E 的第一次聊天在 A 上发出之后杀掉 A；E 断开，B 起来之后 F 只能落到 B，B 回收 A 留下的调用。
  # 先等两个实例在演示里的 span 都进了 Jaeger：QF 每 5 秒批量导出一次，被 KILL 的实例还没导出的那一批就丢了。
  local e f pid call instance
  for instance in quotaforge quotaforge-b; do
    qf logs --no-log-prefix "$instance" >"$work/demo-$instance.log" 2>/dev/null || die "取不到 $instance 的日志"
    wait_for 60 in_jaeger "$work/demo-$instance.log" "$account" || die "$instance 在演示里的 span 没有都进 Jaeger"
  done
  qf stop quotaforge-b >/dev/null
  wait_for 30 resolves_only_to quotaforge || die "QS 里 quotaforge 没有只解析到实例 A"
  pause_upstream
  qs_query >"$work/e.json" &
  pid=$!
  in_flight() { [ "$(account_sql 'COUNT(*)' "c.status = 'IN_FLIGHT'")" = 1 ]; }
  wait_for 30 in_flight || die "等不到 E 的调用在 A 上发出"
  call=$(account_sql c.id "c.status = 'IN_FLIGHT'")
  qf kill -s KILL quotaforge >/dev/null
  wait "$pid" || die "E 的 qs_query 失败"
  e=$(cat "$work/e.json")
  echo "$e" >>"$work/two.jsonl"
  check "run E, its instance killed mid-call: $(outcome "$e")" [ "$(outcome "$e")" = "502 FAILED upstream_request_error 1 unknown" ]
  qf up -d --no-deps --wait quotaforge-b
  wait_for 30 resolves_only_to quotaforge-b || die "QS 里 quotaforge 没有只解析到实例 B"
  unpause_upstream
  f=$(ask two)
  check "run F, only quotaforge-b left: $(outcome "$f")" [ "$(outcome "$f")" = "200 SUCCEEDED None 2 known" ]
  reclaimed() { [ "$(account_sql 'c.status, c.error_code' "c.id = '$call'")" = "$(printf 'USAGE_UNKNOWN\treclaimed_in_flight')" ]; }
  check "quotaforge-b reclaimed E's call within ${RECLAIM_WAIT} s: USAGE_UNKNOWN, reclaimed_in_flight" wait_for "$RECLAIM_WAIT" reclaimed
  qf logs --no-log-prefix quotaforge >"$work/a.log" 2>/dev/null || die "取不到实例 A 的日志"
  qf logs --no-log-prefix quotaforge-b >"$work/b.log" 2>/dev/null || die "取不到实例 B 的日志"
  took_over() {
    [ "$(log_sum "$work/b.log" "e.get('marked_unknown', 0) if e.get('msg') == 'background' else 0")" = 1 ] &&
      [ "$(log_sum "$work/a.log" "e.get('request_id') == arg" "$call")" = 0 ]
  }
  check "log: quotaforge-b's background loop marked exactly 1 call unknown, and quotaforge (killed) never logged E's call" took_over
  checker "$work/two.check" --qf-log "$work/a.log" --qf-log "$work/b.log" --queries "$work/two.jsonl" --served-by "$(run_of "$f")=quotaforge-b" "$work/two.json"
  tally "$work/two.check"
  verify
  finish
}

echo "== 开始前：检查固定提交，以及有没有别人的东西挡路"
qs_head=$(git -C "$QS_DIR" rev-parse HEAD 2>/dev/null) || die "读不到 QS 目录 $QS_DIR 的提交号（不是 git 检出？）"
[ "$qs_head" = "$QS_COMMIT" ] || die "QS 目录在提交 $qs_head，不是固定提交 $QS_COMMIT"
[ -z "$(git -C "$QS_DIR" status --porcelain)" ] || die "QS 目录有改动或多余的文件，请用干净的检出"
for project in "$QF_PROJECT" "$QS_PROJECT"; do
  [ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$project")" ] || die "同名项目 $project 已有容器"
  [ -z "$(docker volume ls -q --filter "label=com.docker.compose.project=$project")" ] || die "同名项目 $project 已有数据卷"
done
for port in 8080 8081 "$QUERYSHIELD_HTTP_PORT" 16686; do # 8081：第二个实例；16686：Jaeger 的界面
  # 连得上 127.0.0.1 的这个端口就是被占（监听在 0.0.0.0 的也算）。不用 bind 试探：Windows 上它分不出 0.0.0.0 的占用。
  if "$PY" -c 'import socket, sys; socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=3).close()' "$port" 2>/dev/null; then
    die "本机端口 $port 被占用（QS 的端口可用 QUERYSHIELD_HTTP_PORT 换；8080、8081 是 QF 的两个实例，16686 是 Jaeger 的界面）"
  fi
done
if docker network inspect qs-qf >/dev/null 2>&1; then
  [ -z "$(docker ps -aq --filter network=qs-qf)" ] || die "qs-qf 上已经有容器"
else
  docker network create qs-qf >/dev/null
  created_network=1
fi

echo "== 起 QF（quotaforge 和 qf-jaeger；quotaforge-b 第 5 个场景才起），建第一个账户、发 Key"
qf_started=1
qf up -d --build --wait quotaforge
use_account e2e

echo "== 起 QS（四个文件加联调的覆盖文件；镜像缺了才构建）"
"$PY" "$QS_DIR/scripts/new_env.py" --path "$work/qs.env" >/dev/null
qs_started=1
qs up -d --wait

scenario_normal
scenario_trace
scenario_quota
scenario_timeout
scenario_two

say "== result: $failures failed"
[ "$failures" = 0 ]
