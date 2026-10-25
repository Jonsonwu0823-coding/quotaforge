#!/usr/bin/env bash
# 本机用（Linux、macOS 自带的 bash 3.2、Windows 的 Git Bash）：以真实上游（百炼）起 QF 和它的账本数据库，接入外部网络 qs-qf，
# 用 qfctl 给 QS 建账户、发一把 QF Key，交给当前终端。嵌入缓存默认开着（见下面的 --no-cache）。在项目根目录下：
#
#   eval "$(scripts/compose-real.sh)"            输入百炼地址和 Key，起 QF，QF Key 设进当前终端的环境变量 QUERYSHIELD_MODEL_API_KEY
#   eval "$(scripts/compose-real.sh --check)"    同上，再经 QF 发一次聊天、一次嵌入（花费很少），只显示状态码、模型名、usage、x-request-id，
#                                                出错时显示状态码和错误码；聊天带一个新生成的 Idempotency-Key，再用同一个键发一遍，
#                                                显示第二次是重放、已消费没有增加（不多花钱）；同一个嵌入再发一遍，显示第二次命中嵌入缓存
#                                                （Cache-Status: quotaforge; hit、正文和第一次相同、调用状态 CACHED、已消费没有增加，不调上游）；
#                                                带 --no-cache 时不发第二次，改成核对嵌入的响应没有 Cache-Status 头（缓存确实关着）。
#                                                最后显示这个账户的额度（上限、已消费、剩余）和缓存命中的次数，以及这把 Key 和账户的限流限额
#                                                （每分钟的请求数和 token 数）。任何一次请求不是 2xx，或者重放、缓存的核对不成立，
#                                                就在额度那一行之后报错退出；QF 还开着，QUERYSHIELD_MODEL_API_KEY 照样设进终端
#   eval "$(scripts/compose-real.sh --stream)"   同上，再经 QF 发三次流式聊天（花费很少），只显示流的结构，不显示内容：
#                                                1. 不带 stream_options：data 数、最后一个是不是 [DONE]、choices 为空的 data 数（QF 去掉了
#                                                   usage 事件，应为 0）、行尾是不是 CRLF（\r\n）；调用状态是 SETTLED、有用量；
#                                                2. 带 include_usage: true：usage 事件在倒数第几个、它的 choices 是不是空数组；调用的用量等于它的数；
#                                                3. 要一个长一点的回答，读 2 秒就断开：等一会儿查调用状态，应是 SETTLED（QF 读完了上游的流）。
#                                                流的原文写进临时目录（--stop 时删掉），不显示。可以和 --check 一起用；任何一项不成立就报错退出
#   eval "$(scripts/compose-real.sh --stop)"     停掉 QF，连数据库的数据一起删掉，删掉临时目录，清掉 QUERYSHIELD_MODEL_API_KEY
#   scripts/compose-real.sh --reconcile <QS 的 demo-summary.json>
#                                                经 QS 跑完真实演示之后：把 QS 的摘要和正在运行的 QF 的账本逐个 run 对账
#                                                （check_qs_runs.py，真实模式），再跑一次 qfctl verify。不启动、不清理任何东西，不碰 Key，不用 eval；
#                                                退出码：0 全部对上；1 有对不上的 run、账本校验失败，或本脚本报错（QF 没在运行、找不到
#                                                Python 等）；2 核对脚本跑不了（摘要不合法、读不了账本）。不能和 --check、--stream、--stop 一起用
#
# --upstream-base-url <地址> 显式指定上游地址，不再询问。用来把“真实”上游指向别的 OpenAI 兼容服务
# （例如 QF 自己的假上游），在不花钱的情况下检查脚本流程。
#
# --account-limit <tokens> 是给 QS 建的账户的额度上限，默认 1,000,000；长评测（例如几十段对话的多轮评测）建议给 2000000。
#
# 嵌入缓存默认开着：只影响不带 Idempotency-Key 的嵌入请求（QS 建知识索引的那批），带键的请求（run 里的）不查缓存。
# 缓存放在单独的 qf-redis-cache 里（不持久化），和账本的库一样每次启动都是新的，停止时一起删掉。
# --no-cache 关掉它：QF 用的配置是 deploy/config.real.json 去掉 embedding_cache 那一行（写进临时目录），和开缓存之前的配置
# 逐字节相同，也不起 qf-redis-cache。容器里仍有 QF_REDIS_CACHE_URL 这个环境变量，但没有配置指向它，QF 不读。
#
# Key 怎么交给终端：脚本是子进程，它设的变量带不回调用它的终端，所以起 QF 时要经 eval（和 eval "$(ssh-agent)" 一样）。
# 提示和结果都写到 stderr；stdout 只有一行 export QUERYSHIELD_MODEL_API_KEY='qfk_…'，由 eval 在当前终端执行，屏幕上不出现 Key，
# 也不写任何文件（库里只有它的哈希）。同一个终端里随后运行的 QS 真实演示读这个变量（网关模式下先读它，有值就不提示）。
# stdout 不是管道时（忘了 eval，输出到了终端或文件里），脚本一开始就停下，不起栈、不发 Key。脚本失败时 stdout 最后多一行 false，
# eval 就以 1 结束，eval "$(…)" && <QS 的演示> 这样的一行命令会停在这里。--stop 输出 unset QUERYSHIELD_MODEL_API_KEY；
# 没经 eval 时照样停栈，只是终端里的旧变量留着（库已经删了，那把 Key 也就没用了）。
#
# 百炼 Key 隐藏输入，只放在本进程的环境变量 QF_UPSTREAM_API_KEY 里，经 Compose secrets 挂成容器里的文件；启动之后就从本进程里删掉。
# 不打印、不写文件。
#
# 数据库每次都是新的：每次启动随机生成数据库密码，写进系统临时目录下 quotaforge-real/db 的三个文件，
# 经 Compose secrets 挂进容器（不进环境变量、docker inspect 和任何输出）。--stop 时连数据和这个目录一起删掉。
# 上一次没有 --stop 就再次启动时，先自动清掉上一次的容器、数据卷和临时文件，否则旧库里的密码和新生成的对不上。
#
# docker compose 都带 --progress plain --ansi never：让 docker 的输出是逐行的纯文本，不带进度动画和颜色。
# 开头先查要用的东西（Docker 在运行、docker compose v2、jq、curl；--reconcile 再查 Python 3.10 起），缺了就停下、说怎么装。
# 提示里的 eval "$(…)"、jq 过滤式里的 $n、$u 写在单引号里，正是为了不让 shell 展开。
# shellcheck disable=SC2016
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash：不要把传给 docker 的容器内路径改写成 Windows 路径
# Windows 的 docker、curl、jq、python 看不懂 Git Bash 的 /tmp、/c/... 路径：有 cygpath 就转成 C:/... 写法。
native() { cygpath -m "$1" 2>/dev/null || printf '%s' "$1"; }

readonly USAGE='用法：eval "$(scripts/compose-real.sh [--check] [--stream] [--no-cache] [--account-limit <tokens>] [--upstream-base-url <地址>])"；eval "$(scripts/compose-real.sh --stop)"；scripts/compose-real.sh --reconcile <QS 的 demo-summary.json>'
readonly KEY_PATTERN='^qfk_[A-Za-z0-9_-]{43}$' # qfctl 发的 Key：qfk_ 加 32 字节的 base64url（不补 =）
readonly BASE=http://127.0.0.1:8080
readonly ACCOUNT=queryshield
readonly PROJECT=quotaforge # compose.yaml 里的 name:，所有 docker compose 命令靠它找到这套栈

handoff=0 # 1：stdout 已经留给 eval（fd 3），别的输出都到 stderr
reconcile=0
die() { echo "compose-real.sh：$*" >&2; exit 1; }
# 失败时给 eval 一行 false，eval 就以 1 结束（参数不对时也是）。成功退出时不写；--reconcile 不经 eval，也不写。
finish() {
  if [ $? = 0 ] || [ "$reconcile" = 1 ]; then return; fi
  if [ "$handoff" = 1 ]; then echo false >&3; elif [ -p /dev/stdout ]; then echo false; fi
}
trap finish EXIT

check=0 stream=0 stop=0 no_cache=0 summary='' upstream='' limit=1000000
while [ $# -gt 0 ]; do
  case $1 in
    --check) check=1 ;;
    --stream) stream=1 ;;
    --stop) stop=1 ;;
    --no-cache) no_cache=1 ;;
    --upstream-base-url | --reconcile | --account-limit)
      [ $# -ge 2 ] || die "$1 后面要跟一个值。$USAGE"
      case $1 in
        --upstream-base-url) upstream=$2 ;;
        --reconcile) summary=$2 ;;
        --account-limit) limit=$2 ;;
      esac
      shift
      ;;
    *) die "不认识的参数 ${1}。$USAGE" ;;
  esac
  shift
done
[[ $limit =~ ^[1-9][0-9]*$ ]] || die "--account-limit 要是正整数（tokens），现在是 ${limit}。"
if [ -n "$summary" ]; then
  [ "$check$stream$stop" = 000 ] || die '--reconcile 不能和 --check、--stream、--stop 一起用。'
  [ -f "$summary" ] || die "找不到 QS 的摘要文件：${summary}"
  # 下面会切到 QF 目录，所以先把相对路径变成完整路径。
  summary=$(native "$(cd "$(dirname "$summary")" && pwd)/$(basename "$summary")")
  reconcile=1
elif [ "$stop" = 0 ] && ! [ -p /dev/stdout ]; then
  die '要经 eval 运行，QF Key 才能交给当前终端：eval "$(scripts/compose-real.sh …)"（stdout 只有一行 export，由 eval 执行，屏幕上不显示 Key）。现在 stdout 不是管道（输出到了终端或文件），没有起 QF，也没有发 Key。'
fi
piped=0
[ ! -p /dev/stdout ] || piped=1
if [ "$reconcile" = 0 ]; then
  exec 3>&1 1>&2 # stdout 只留给最后那一行 export 或 unset；别的输出（包括 docker 的）都到 stderr
  handoff=1
fi

# 缺了什么就停下：一句话说缺什么，再说三种系统上怎么装（出处见 docs/development.md 的“本机环境”）。这时还没起栈、没问 Key。
missing() {
  die "缺 ${1}。
  macOS：$2
  Windows（Git Bash）：$3
  Linux：$4
装好之后重新运行（Windows 上装完要重开 Git Bash）。这时什么都还没做：没有起 QF，也没有问 Key。本机环境见 docs/development.md 的本机环境一节。"
}
readonly DOCKER_MAC='装 Docker Desktop：https://docs.docker.com/desktop/setup/install/mac-install/'
readonly DOCKER_WIN='装 Docker Desktop：https://docs.docker.com/desktop/setup/install/windows-install/'
command -v docker >/dev/null 2>&1 ||
  missing 'docker 命令' "$DOCKER_MAC" "$DOCKER_WIN" '装 Docker Engine：https://docs.docker.com/engine/install/'
docker compose version >/dev/null 2>&1 ||
  missing 'docker compose（v2，docker 的子命令）' "Docker Desktop 自带；$DOCKER_MAC" "Docker Desktop 自带；$DOCKER_WIN" \
    '装 Compose 插件：https://docs.docker.com/compose/install/linux/（Ubuntu、Debian 上 sudo apt-get install docker-compose-plugin）'
docker info --format '{{.ServerVersion}}' >/dev/null 2>&1 ||
  die 'Docker 没有运行：请先启动 Docker Desktop（Linux 上启动 docker 服务：sudo systemctl start docker）。还没有起 QF，也没有问 Key。'
command -v jq >/dev/null 2>&1 ||
  missing jq 'macOS 26 自带 /usr/bin/jq；更早的系统 brew install jq' 'winget install jqlang.jq' 'sudo apt-get install jq（Fedora：sudo dnf install jq）'
command -v curl >/dev/null 2>&1 ||
  missing curl 'macOS 自带 /usr/bin/curl' 'Git for Windows 自带' 'sudo apt-get install curl（Fedora：sudo dnf install curl）'
# Windows 上 winget 装的 jq.exe 输出 CRLF：去掉 \r 再用。开着 pipefail，jq 的退出码照样传出来。
# 放在缺工具的检查之后：command -v 对函数也返回 0。
jq() { command jq "$@" | tr -d '\r'; }
py=
if [ "$reconcile" = 1 ]; then
  # 用第一个真能运行、3.10 以上的（核对脚本要 3.10）：Windows 上的 python3 可能只是应用商店的占位程序；macOS 自带的 python3 是 3.9。
  for candidate in python3 python py; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 10))' >/dev/null 2>&1; then py=$candidate; break; fi
  done
  [ -n "$py" ] || missing 'Python 3.10 或更新的版本（python3、python、py 都不行；核对脚本只用标准库，不用装包）' \
    'brew install python@3.13，或者 https://www.python.org/downloads/macos/ 的安装包（自带的 python3 是 3.9，不够）' \
    'winget install Python.Python.3.13，或者 https://www.python.org/downloads/windows/ 的安装包' \
    'sudo apt-get install python3（Ubuntu 22.04、Debian 12 起是 3.10 以上）'
fi

cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=$PROJECT # 优先于 compose.yaml 里的 name；终端里设过别的项目名也不影响
# 只带主文件的命令（qfctl、ps、down）会把 compose.real.yaml 里的 qf-redis-cache 当成孤儿，每次都警告一行；关掉这个警告。
# 不影响清理：down 带着 --remove-orphans，照样删掉它。
export COMPOSE_IGNORE_ORPHANS=true
tmp=${TMPDIR:-/tmp}
temp_dir=$(native "${tmp%/}/quotaforge-real")
db_dir=$temp_dir/db

# 所有 docker compose 调用都经这里，带同一组全局参数。不带 -f 时只用主文件：qfctl、ps、down 不需要真实覆盖文件。
compose() { docker compose --progress plain --ansi never "$@"; }
qfctl() { compose run --rm -T --no-deps qfctl "$@" || die "qfctl $1 $2 失败（退出码 $?）"; }
# field <qfctl 的输出> <列名>：第二行里这一列的值（制表符分隔，第一行是列名）
field() { awk -F'\t' -v col="$2" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == col) c = i } NR == 2 && c { print $c }' <<<"$1"; }
consumed() { field "$(qfctl usage -account "$ACCOUNT")" consumed; }
# 32 个十六进制字符的随机数。
hex() { od -An -N16 -tx1 /dev/urandom | tr -d ' \n'; }

# 写三个数据库 secret 文件。容器里的进程不是本机用户（QF 是 UID 65532），所以目录和文件要让别人能读。
new_db_secrets() {
  mkdir -p "$db_dir"
  printf '%s\n' "$(hex)" >"$db_dir/root_password"
  printf 'qf_migrate:%s@tcp(qf-mysql:3306)/quotaforge\n' "$(hex)" >"$db_dir/migrate_dsn"
  printf 'qf_app:%s@tcp(qf-mysql:3306)/quotaforge\n' "$(hex)" >"$db_dir/app_dsn"
  chmod 755 "$temp_dir" "$db_dir"
  chmod 644 "$db_dir/root_password" "$db_dir/migrate_dsn" "$db_dir/app_dsn"
}

# --no-cache 用的配置：deploy/config.real.json 去掉 embedding_cache 那一行（连同上一行末尾的逗号），其余逐字节不变，
# 也就是开缓存之前的配置。核对正好删掉一处、一行，否则报错：配置被改过时，不悄悄生成一份半开半关的。
new_no_cache_config() {
  if ! awk '{ line[NR] = $0 }
    /^[ \t]*"embedding_cache":[ \t]*[{][^}]*[}]/ { hit = NR; found++ }
    END {
      if (found != 1 || hit < 2 || line[hit - 1] !~ /,[ \t]*$/) exit 1
      sub(/,[ \t]*$/, "", line[hit - 1])
      for (i = 1; i <= NR; i++) if (i != hit) print line[i]
    }' deploy/config.real.json >"$temp_dir/config.real.json" || grep -q embedding_cache "$temp_dir/config.real.json"; then
    die 'deploy/config.real.json 里 embedding_cache 应当正好是一行（前一行以逗号结尾），没有生成关掉缓存的配置。'
  fi
}

# 删掉容器、数据卷（数据库的数据）和临时目录。--stop 和发现上一次的残留时都用它。
# 只带主文件：qf-redis-cache 只在 compose.real.yaml 里，这里算孤儿，由 --remove-orphans 删掉（它的 /data 是 tmpfs，没有卷）。
remove_previous() {
  compose down -v --remove-orphans || die "docker compose down 失败（退出码 $?）"
  rm -rf "$temp_dir"
}

# 只显示代理变量设没设：值里可能有代理的账号密码（所以也不经 <<< 的临时文件）。
show_proxy_variables() {
  local vars name state
  vars=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$(compose ps -q quotaforge)")
  for name in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
    state=未设置
    if printf '%s\n' "$vars" | grep -q "^${name}="; then state=已设置; fi
    echo "  ${name}：$state"
  done
}

# QF Key 经 curl 的配置从 stdin 交给 curl，不进命令行（别的进程看得到命令行）。
auth() { printf 'header = "Authorization: Bearer %s"\n' "$qf_key"; }
# header <名字> <小写的头名>：$temp_dir/<名字>.headers 里这个头的值（有几段响应头时取最后一段的）
header() { tr -d '\r' <"$temp_dir/$1.headers" | awk -v h="$2:" 'tolower($1) == h { sub(/^[^:]*:[ \t]*/, ""); v = $0 } END { print v }'; }
# call <调用号>：GET /v1/calls/<调用号> 的正文（调用的状态和用量）
call() { auth | curl -sS -K - "$BASE/v1/calls/$1" || true; }

# post <名字> <路径> <请求体> [Idempotency-Key]：发一次请求，显示一行结果；正文和响应头写进 $temp_dir/<名字>.body、.headers。
# 不是 2xx 时显示状态码和 QF 的错误码（例如 upstream_auth_failed 是 Key 错，upstream_unavailable 是地址错），返回 1。
post() {
  local name=$1 path=$2 body=$3 idem=() status code line value
  if [ $# -ge 4 ]; then idem=(-H "Idempotency-Key: $4"); fi
  status=$(auth | curl -sS -K - -o "$temp_dir/$name.body" -D "$temp_dir/$name.headers" -w '%{http_code}' \
    -H 'Content-Type: application/json' ${idem[@]+"${idem[@]}"} --data-binary "$body" "$BASE$path") || status=000
  if [ "${status#2}" = "$status" ]; then
    code=$(jq -r '.error.code // empty' "$temp_dir/$name.body" 2>/dev/null || true)
    echo "  ${path}：状态 ${status}${code:+，错误码 $code}"
    return 1
  fi
  line="  ${path}：状态 ${status}，模型 $(jq -r .model "$temp_dir/$name.body")，usage $(jq -c .usage "$temp_dir/$name.body")，x-request-id $(header "$name" x-request-id)"
  value=$(header "$name" idempotent-replayed)
  if [ -n "$value" ]; then line="${line}，重放（Idempotent-Replayed: ${value}）"; fi
  value=$(header "$name" cache-status)
  if [ -n "$value" ]; then line="${line}，Cache-Status: $value"; fi
  echo "$line"
}

# --check，见文件开头。返回是否全部成立。
check_requests() {
  local chat='{"model":"qwen-plus","messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
  local embedding='{"model":"text-embedding-v4","input":["ok"]}'
  local idem passed=1 first=1 embedded=1 before after same_id same_body hit status usage key_rate
  idem=real-check-$(hex)
  echo '经 QF 各发一次聊天和嵌入（聊天带一个新的 Idempotency-Key）：'
  post chat /v1/chat/completions "$chat" "$idem" || first=0
  post embedding /v1/embeddings "$embedding" || embedded=0
  [ "$first$embedded" = 11 ] || passed=0 # 任何一次请求不是 2xx 就没通过
  if [ "$first" = 1 ]; then
    # 同一个键、同样的请求体再发一遍：QF 重放第一次的响应，不调上游、不扣额度。
    before=$(consumed)
    echo '用同一个 Idempotency-Key 再发一遍聊天：'
    if post replay /v1/chat/completions "$chat" "$idem"; then
      after=$(consumed)
      same_id=false same_body=false
      if [ "$(header replay x-request-id)" = "$(header chat x-request-id)" ]; then same_id=true; fi
      if cmp -s "$temp_dir/replay.body" "$temp_dir/chat.body"; then same_body=true; fi
      echo "  重放：Idempotent-Replayed $(header replay idempotent-replayed)，x-request-id 和第一次相同 ${same_id}，正文相同 ${same_body}；已消费 $before → $after"
      [ "$same_id$same_body" = truetrue ] && [ "$after" = "$before" ] || passed=0
    else
      passed=0
    fi
  fi
  if [ "$embedded" = 1 ] && [ "$no_cache" = 1 ]; then
    # Cache-Status 只在开着缓存时才加：没有它，说明这次运行确实没查缓存。
    if [ -z "$(header embedding cache-status)" ]; then
      echo '嵌入缓存关着（--no-cache）：嵌入的响应没有 Cache-Status 头 true'
    else
      echo '嵌入缓存关着（--no-cache）：嵌入的响应没有 Cache-Status 头 false'
      passed=0
    fi
  elif [ "$embedded" = 1 ]; then
    # 同样的嵌入（不带 Idempotency-Key）再发一遍：命中嵌入缓存，不调上游、不扣额度，记一条 CACHED。
    before=$(consumed)
    echo '同一个嵌入再发一遍（应当命中嵌入缓存）：'
    if post again /v1/embeddings "$embedding"; then
      after=$(consumed)
      hit=false same_body=false
      if [ "$(header again cache-status)" = 'quotaforge; hit' ]; then hit=true; fi
      if cmp -s "$temp_dir/again.body" "$temp_dir/embedding.body"; then same_body=true; fi
      status=$(call "$(header again x-request-id)" | jq -r .status)
      echo "  命中：Cache-Status 是 quotaforge; hit ${hit}，正文和第一次相同 ${same_body}，调用状态 ${status}；已消费 $before → $after"
      [ "$hit$same_body$status" = truetrueCACHED ] && [ "$after" = "$before" ] || passed=0
    else
      passed=0
    fi
  fi
  usage=$(qfctl usage -account "$ACCOUNT")
  echo "账户 $ACCOUNT 的额度：上限 $(field "$usage" limit)，已消费 $(field "$usage" consumed)，剩余 $(field "$usage" remaining) tokens；嵌入缓存命中（CACHED）$(field "$usage" CACHED) 次"
  # 限流限额（0 表示不限）：qfctl key list 和 usage 的 rpm_limit、tpm_limit 两列
  key_rate=$(qfctl key list -account "$ACCOUNT")
  echo "限流（每分钟）：这把 Key 最多 $(field "$key_rate" rpm_limit) 个请求、$(field "$key_rate" tpm_limit) tokens；账户最多 $(field "$usage" rpm_limit) 个请求、$(field "$usage" tpm_limit) tokens"
  [ "$passed" = 1 ]
}

# stream <名字> <请求体> [curl 的参数…]：发一次流式聊天，curl 边收边写进 $temp_dir/<名字>.sse；每个事件的 data 写成 .data 里的一行
# （多个 data 行用换行连起来；QF 和百炼的事件都只有一个 data 行）。只用 -s（不带 -S）：读 2 秒就断开那一次 curl 以 28 退出，不算错。
stream() {
  local name=$1 body=$2
  shift 2
  : >"$temp_dir/$name.sse"
  auth | curl -s -N -K - -o "$temp_dir/$name.sse" -D "$temp_dir/$name.headers" -H 'Content-Type: application/json' \
    --data-binary "$body" "$@" "$BASE/v1/chat/completions" || true
  awk '{ sub(/\r$/, "") }
    $0 == "" { if (d != "") print d; d = ""; next }
    /^data:/ { v = substr($0, 6); sub(/^ +/, "", v); d = (d == "" ? v : d "\n" v) }
    END { if (d != "") print d }' "$temp_dir/$name.sse" >"$temp_dir/$name.data"
}
# data <名字> <jq 表达式>：表达式作用在 .data 的 data 数组（字符串）上，输出制表符分隔的一行
data() { jq -R -s -r "split(\"\n\") | map(select(. != \"\")) | $2 | @tsv" "$temp_dir/$1.data"; }

# --stream 的三次检查，见文件开头。返回是否全部成立。
check_stream() {
  local passed=1 id count last_done empty crlf from_end choices total status usage same i
  stream plain '{"model":"qwen-plus","stream":true,"messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
  read -r count last_done empty <<<"$(data plain '[length, (last == "[DONE]"), (map(select(. != "[DONE]") | fromjson | select((.choices | length) == 0)) | length)]')"
  crlf=false
  if grep -q $'\r$' "$temp_dir/plain.sse"; then crlf=true; fi
  sleep 1 # 收尾在流结束之后
  id=$(header plain x-request-id)
  read -r status usage <<<"$(call "$id" | jq -r '[.status, (.usage | tojson)] | @tsv')"
  echo "  不带 stream_options：$count 个 data，最后一个是 [DONE] ${last_done}，choices 为空的 $empty 个，行尾是 CRLF ${crlf}；调用 ${status}，用量 $usage"
  [ "$last_done$empty$status" = true0SETTLED ] || passed=0

  stream usage '{"model":"qwen-plus","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
  # usage 事件：最后一个 usage 不为 null 的 data；from_end 是它在倒数第几个（没有时是 0）
  read -r count from_end choices total <<<"$(data usage 'length as $n | (to_entries | map(select(.value != "[DONE]") | .value |= fromjson | select(.value.usage != null)) | last) as $u
    | if $u then [$n, $n - $u.key, ($u.value.choices == []), $u.value.usage.total_tokens] else [$n, 0, false, -1] end')"
  sleep 1
  read -r status usage <<<"$(call "$(header usage x-request-id)" | jq -r '[.status, (.usage.total_tokens // -2)] | @tsv')"
  same=false
  if [ "$usage" = "$total" ]; then same=true; fi
  echo "  带 include_usage：$count 个 data，usage 在倒数第 $from_end 个，它的 choices 是空数组 ${choices}；调用 ${status}，用量和 usage 事件相同 $same"
  [ "$from_end" != 0 ] && [ "$status$same" = SETTLEDtrue ] || passed=0 # choices 是不是空数组只显示，不判（和旧脚本一样）

  stream gone '{"model":"qwen-plus","stream":true,"messages":[{"role":"user","content":"Count from 1 to 300, one number per line."}],"max_tokens":1200}' --max-time 2
  id=$(header gone x-request-id)
  status=''
  for ((i = 0; i < 30; i++)); do
    case $status in SETTLED | USAGE_UNKNOWN) break ;; esac
    sleep 1
    status=$(call "$id" | jq -r .status)
  done
  read -r count last_done <<<"$(data gone '[length, (length > 0 and last == "[DONE]")]')"
  read -r status usage <<<"$(call "$id" | jq -r '[.status, (.usage | tojson)] | @tsv')"
  echo "  读 2 秒就断开：断开前收到 $count 个 data，最后一个是 [DONE] ${last_done}；调用 ${status}，用量 $usage"
  [ "$status" = SETTLED ] && [ "$passed" = 1 ]
}

# 逐个 run 对账，再校验账本。退出码：1 表示对不上或校验失败，2 表示核对跑不了。
reconcile_summary() {
  local rc=0 verify_rc=0
  if [ -z "$(compose ps --status running -q quotaforge)" ] || [ ! -d "$db_dir" ]; then
    die '真实栈没有在运行：请先用 eval "$(scripts/compose-real.sh)" 起 QF，再跑 QS 的真实演示。'
  fi
  echo "== QS 摘要对 QF 账本，逐个 run（项目 ${PROJECT}，账户 ${ACCOUNT}）"
  "$py" scripts/check_qs_runs.py --project "$PROJECT" --account "$ACCOUNT" "$summary" || rc=$?
  QF_DB_SECRETS_DIR=$db_dir
  export QF_DB_SECRETS_DIR
  echo '== qfctl verify'
  compose run --rm -T --no-deps qfctl verify | sed 's/^/  /' || verify_rc=$?
  if [ "$rc" = 0 ] && [ "$verify_rc" != 0 ]; then rc=1; fi
  exit "$rc"
}

if [ "$reconcile" = 1 ]; then
  reconcile_summary
fi
if [ "$stop" = 1 ]; then
  remove_previous
  if [ "$piped" = 1 ]; then
    echo 'unset QUERYSHIELD_MODEL_API_KEY' >&3
    echo '已停止 QF，数据库的数据和临时目录已删除；eval 会清掉当前终端的 QUERYSHIELD_MODEL_API_KEY。'
  else
    echo '已停止 QF，数据库的数据和临时目录已删除。没有经 eval 运行，当前终端的 QUERYSHIELD_MODEL_API_KEY 还在（那把 Key 已经没用了）；要清掉就运行 unset QUERYSHIELD_MODEL_API_KEY。'
  fi
  exit 0
fi
if [ -n "$(compose ps -a -q)" ] || [ -e "$temp_dir" ]; then
  echo '发现上一次没有 --stop 的残留，先清理（容器、数据库的数据、临时目录），再重新开始。'
  remove_previous
fi
if [ -z "$(docker network ls --filter 'name=^qs-qf$' --format '{{.Name}}')" ]; then
  docker network create qs-qf >/dev/null || die "创建网络 qs-qf 失败（退出码 $?）"
fi

if [ -z "$upstream" ]; then
  read -rp '百炼的 base URL（例如 https://dashscope.aliyuncs.com/compatible-mode/v1）：' upstream || true
fi
[ -n "$upstream" ] || die '上游地址为空。'
upstream_key=''
read -rsp '百炼 Key（输入不显示）：' upstream_key || true
echo
[ -n "$upstream_key" ] || die '百炼 Key 为空。'
export QF_UPSTREAM_BASE_URL=$upstream QF_UPSTREAM_API_KEY=$upstream_key

new_db_secrets
QF_DB_SECRETS_DIR=$db_dir
export QF_DB_SECRETS_DIR
services=(quotaforge qf-redis-cache)
if [ "$no_cache" = 1 ]; then
  new_no_cache_config
  QF_REAL_CONFIG=$temp_dir/config.real.json
  export QF_REAL_CONFIG
  services=(quotaforge)
fi

compose build quotaforge qfctl || die "构建 QF 镜像失败（退出码 $?）"
compose -f compose.yaml -f compose.real.yaml up -d --wait "${services[@]}" || die "启动 QF 失败（退出码 $?）"
unset QF_UPSTREAM_API_KEY upstream_key # 只有 up 要它（Compose 把它复制成容器里的 secret 文件）
qfctl account create -name "$ACCOUNT" -limit "$limit" -rpm 600 -tpm 2000000 >/dev/null
# 限流限额（每分钟）：远高于演示的用量（2026-10-15 那次真实演示一共 56 次调用，单次聊天的估算最多约 17,300 tokens）。
issued=$(qfctl key issue -account "$ACCOUNT" -models qwen-plus,text-embedding-v4 -rpm 300 -tpm 1000000)
# 用管道不用 <<<：bash 5.1 之前的 <<< 经临时文件，Key 会落盘。
qf_key=$(printf '%s\n' "$issued" | sed -n 's/^key: //p')
unset issued
[[ $qf_key =~ $KEY_PATTERN ]] || die 'qfctl key issue 的输出格式不对（应当有一行 key: qfk_ 加 43 个 base64url 字符），没有交出 Key。'
printf "export QUERYSHIELD_MODEL_API_KEY='%s'\n" "$qf_key" >&3
echo 'QF 已启动：本机 http://127.0.0.1:8080/v1，qs-qf 网络上 http://quotaforge:8080/v1。'
echo "账户 $ACCOUNT 的额度上限 $limit tokens（--account-limit 可改）。"
if [ "$no_cache" = 1 ]; then
  echo '嵌入缓存关着（--no-cache）：QF 用的配置去掉了 embedding_cache，和开缓存之前的配置逐字节相同；没有起 qf-redis-cache。'
else
  echo '嵌入缓存开着（qf-redis-cache，每次启动都是新的）：只影响不带 Idempotency-Key 的嵌入请求；--no-cache 关掉。'
fi
echo 'QF Key 已交给 eval（不显示）：脚本结束后，eval 把它设进当前终端的环境变量 QUERYSHIELD_MODEL_API_KEY，只在这个终端里有效。在同一个终端里运行 QS 的真实演示，它会直接读取，不用粘贴；eval "$(scripts/compose-real.sh --stop)" 时清掉。'
echo 'quotaforge 容器里的代理变量（只显示设没设）：'
show_proxy_variables

if [ "$check" = 1 ]; then
  check_requests || die '--check 没有全部通过，见上面的输出。QF 还开着，看完日志再运行 eval "$(scripts/compose-real.sh --stop)"。'
fi
if [ "$stream" = 1 ]; then
  echo '经 QF 发三次流式聊天（只显示流的结构）：'
  check_stream || die '--stream 没有全部通过，见上面的输出。QF 还开着，看完日志再运行 eval "$(scripts/compose-real.sh --stop)"。'
fi
