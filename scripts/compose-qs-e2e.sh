#!/usr/bin/env bash
# QS 端到端联调（fake-upstream 模式）：QF 接 QS 的假上游，QS 的 app 经 QF 调模型，跑 QS 的整套演示，
# 再用 QF 的账本核对 QF 确实收到了请求（用法见 docs/queryshield.md 的“端到端联调”）。
#   用法：scripts/compose-qs-e2e.sh <QS 公开仓库的本地目录>      （也可以用环境变量 QS_DIR）
# QS 目录必须干净地检出在固定提交上；脚本不往里写任何文件。QS 的镜像缺了才构建，已有同名镜像时直接用。
# 核对（QS 摘要对 QF 账本，逐个 run）由 scripts/check_qs_runs.py 做，和经 QS 的真实演示的对账共用同一份代码。
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

readonly QS_COMMIT=979ddbd9caca730c42a44d83df12d02d30e72484
readonly QF_PROJECT=qf-e2e-qf QS_PROJECT=qf-e2e-qs
readonly ACCOUNT=e2e
# 额度 100 万 tokens：整套演示实测消费约 11.1 万（33 次聊天、15 次嵌入），留足余量；且远大于一次预留（请求字节数加 default_max_tokens 1024）。
readonly LIMIT=1000000
readonly SUMMARY=evidence/compose-qs-e2e-summary.txt
export QF_E2E_QS_IMAGE=queryshield-app:qf-e2e-${QS_COMMIT:0:7}
export QUERYSHIELD_GATEWAY_NETWORK=qs-qf
export QUERYSHIELD_GATEWAY_BASE_URL=http://quotaforge:8080/v1
export QUERYSHIELD_HTTP_PORT=${QUERYSHIELD_HTTP_PORT:-18731}

work=$(native "$(mktemp -d)")
images_file=$(native "$PWD/deploy/compose.qs-e2e-images.yaml")
mkdir -p evidence
: >"$SUMMARY"
failures=0 created_network=0 qf_started=0 qs_started=0 key=

say() { echo "$*" | tee -a "$SUMMARY"; }
check() {
  local what=$1
  shift
  if "$@"; then say "ok   $what"; else say "FAIL $what"; failures=$((failures + 1)); fi
}
die() { echo "FAIL $*" >&2; exit 1; }
redact() { KEY=$key "$PY" -c 'import os, sys; sys.stdout.write(sys.stdin.read().replace(os.environ["KEY"], "***") if os.environ["KEY"] else sys.stdin.read())'; }

# 文件顺序不能换：compose.model-gateway.yaml 必须在 compose.fake-upstream.yaml 后面，否则 app 绕过 QF 直连假上游。
qs() {
  (
    export QUERYSHIELD_MODEL_API_KEY=${key:-unset}
    cd "$QS_DIR"
    docker compose -p "$QS_PROJECT" --env-file "$work/qs.env" --progress plain \
      -f compose.yaml -f compose.fake-upstream.yaml -f compose.model-gateway.yaml -f compose.fake-upstream-gateway.yaml \
      -f "$images_file" "$@"
  )
}
qf() { docker compose -p "$QF_PROJECT" -f compose.yaml -f compose.qs-fake.yaml --progress plain "$@"; }
qfctl() { qf run --rm -T --no-deps qfctl "$@" 2>>"$work/qfctl.err"; }
# 只读 SQL：root 密码在容器里的 secret 文件中，经进程环境变量交给 mysql，不出现在命令行、输出和宿主机上。
# shellcheck disable=SC2016 # $(...) 要在容器里展开，所以用单引号
sql() { qf exec -T qf-mysql sh -c 'MYSQL_PWD=$(cat /run/secrets/mysql_root_password) exec mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$0"' "$1"; }

cleanup() {
  local rc=$?
  if [ "$rc" != 0 ]; then
    if [ "$qf_started" = 1 ]; then echo "== quotaforge 最后 40 行日志（已隐去 Key）" >&2; qf logs --no-log-prefix --tail 40 quotaforge 2>&1 | redact >&2 || true; fi
    if [ "$qs_started" = 1 ]; then echo "== QS app 最后 40 行日志（已隐去 Key）" >&2; qs logs --no-log-prefix --tail 40 app 2>&1 | redact >&2 || true; fi
    if [ -s "$work/qfctl.err" ]; then echo "== qfctl stderr" >&2; cat "$work/qfctl.err" >&2; fi
  fi
  if [ "$qs_started" = 1 ]; then qs down -v --remove-orphans >/dev/null 2>&1 || true; fi
  if [ "$qf_started" = 1 ]; then qf down -v --remove-orphans >/dev/null 2>&1 || true; fi
  if [ "$created_network" = 1 ]; then docker network rm qs-qf >/dev/null 2>&1 || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

echo "== 开始前：检查固定提交，以及有没有别人的东西挡路"
qs_head=$(git -C "$QS_DIR" rev-parse HEAD 2>/dev/null) || die "读不到 QS 目录 $QS_DIR 的提交号（不是 git 检出？）"
[ "$qs_head" = "$QS_COMMIT" ] || die "QS 目录在提交 $qs_head，不是固定提交 $QS_COMMIT"
[ -z "$(git -C "$QS_DIR" status --porcelain)" ] || die "QS 目录有改动或多余的文件，请用干净的检出"
for project in "$QF_PROJECT" "$QS_PROJECT"; do
  [ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$project")" ] || die "同名项目 $project 已有容器"
  [ -z "$(docker volume ls -q --filter "label=com.docker.compose.project=$project")" ] || die "同名项目 $project 已有数据卷"
done
for port in 8080 "$QUERYSHIELD_HTTP_PORT"; do
  # 连得上 127.0.0.1 的这个端口就是被占（监听在 0.0.0.0 的也算）。不用 bind 试探：Windows 上它分不出 0.0.0.0 的占用。
  if "$PY" -c 'import socket, sys; socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=3).close()' "$port" 2>/dev/null; then
    die "本机端口 $port 被占用（QS 的端口可用 QUERYSHIELD_HTTP_PORT 换）"
  fi
done
if docker network inspect qs-qf >/dev/null 2>&1; then
  [ -z "$(docker ps -aq --filter network=qs-qf)" ] || die "qs-qf 上已经有容器"
else
  docker network create qs-qf >/dev/null
  created_network=1
fi

echo "== 起 QF，建账户、发 Key"
qf_started=1
qf up -d --build --wait quotaforge
# 限流的限额远高于联调的用量：每个请求都经过准入和修正，但不会被拒。
qfctl account create -name "$ACCOUNT" -limit "$LIMIT" -rpm 6000 -tpm 100000000 >/dev/null
key=$(qfctl key issue -account "$ACCOUNT" -models qs-fake-upstream-v1 -rpm 3000 -tpm 50000000 | sed -n 's/^key: //p')
[ -n "$key" ] || die "qfctl 没有发出 Key"

echo "== 起 QS（四个文件加联调镜像覆盖文件；镜像缺了才构建）"
"$PY" "$QS_DIR/scripts/new_env.py" --path "$work/qs.env" >/dev/null
qs_started=1
qs up -d --wait

echo "== 演示前核对"
members=$(docker ps -a --filter network=qs-qf --format '{{.Label "com.docker.compose.project"}}/{{.Label "com.docker.compose.service"}}' | sort | tr '\n' ' ')
check "qs-qf members are exactly quotaforge, app, fake-upstream ($members)" \
  [ "$members" = "$QF_PROJECT/quotaforge $QS_PROJECT/app $QS_PROJECT/fake-upstream " ]
check "ledger: the account has no calls yet" [ "$(sql "SELECT COUNT(*) FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = '$ACCOUNT'")" = 0 ]

echo "== 跑 QS 的演示（fake-upstream 模式）"
start=$SECONDS
qs exec -T app python scripts/demo_run.py --mode fake-upstream --base-url http://127.0.0.1:8000 --evidence-dir /tmp/evidence-gateway >"$work/demo.out" 2>&1 || true
say "demo took $((SECONDS - start)) s"
qs exec -T app cat /tmp/evidence-gateway/demo-summary.json >"$work/summary.json" 2>/dev/null || die "取不到 demo-summary.json，演示输出最后一行：$(tail -n 1 "$work/demo.out" | redact)"

echo "== 核对（逐个 run，规则在 check_qs_runs.py 里）"
"$PY" scripts/check_qs_runs.py --e2e --project "$QF_PROJECT" --account "$ACCOUNT" "$work/summary.json" | tee -a "$SUMMARY" || failures=$((failures + 1))

# 固定提交里的 QS，run 里的调用（带 X-Run-Id）都带 Idempotency-Key，run 外的（建索引的嵌入）都不带。QF 把两者存在 calls 里。
idempotency_keys_match() { [ "$keyed_in_run" -gt 0 ] && [ "$keyless_in_run" = 0 ] && [ "$keyed_outside_run" = 0 ]; }
read -r keyed_in_run plain_outside_run keyless_in_run keyed_outside_run < <(sql "SELECT
  COALESCE(SUM(c.run_id IS NOT NULL AND c.idempotency_key IS NOT NULL), 0), COALESCE(SUM(c.run_id IS NULL AND c.idempotency_key IS NULL), 0),
  COALESCE(SUM(c.run_id IS NOT NULL AND c.idempotency_key IS NULL), 0), COALESCE(SUM(c.run_id IS NULL AND c.idempotency_key IS NOT NULL), 0)
  FROM calls c JOIN accounts a ON a.id = c.account_id WHERE a.name = '$ACCOUNT'")
check "idempotency keys: $keyed_in_run calls with X-Run-Id carry one, $plain_outside_run without X-Run-Id do not ($keyless_in_run in-run lack a key, $keyed_outside_run out-of-run have one)" \
  idempotency_keys_match

qfctl verify >"$work/verify.txt" || true
check "qfctl verify prints ledger ok" [ "$(cat "$work/verify.txt")" = "ledger ok" ]
not_limited() { ! qf logs --no-log-prefix quotaforge | grep -qE 'rate_limited|rate_limiter_unavailable'; }
check "log: no request was rate limited or hit an unavailable rate limiter" not_limited

say "== result: $failures failed"
[ "$failures" = 0 ]
