#!/usr/bin/env python3
"""逐个 run 核对：QS 的 run 记录对 QF 账本（联调和经 QS 的真实演示共用，用法见 docs/queryshield.md）。

  check_qs_runs.py [--e2e [--qf-log <QF 的日志>]... [--queries <记录>] [--jaeger <URL> [--served-by <run_id>=<实例>]]]
                   --project <QF 的 Compose 项目名> --account <账户名> [<demo-summary.json>]

默认是真实模式：只对 usage_total 为 known 的 run 比次数和用量，unknown 的列出来（skip），不算失败。
--e2e 是联调模式（fake-upstream，不花钱），规则更严：账户里的每次调用都得对得上；usage_total 不是 known 的 run，
只有 QS 报的错误码在 UNKNOWN_RUNS 里、QF 账本的形态也和它一致才说得通（联调的故障场景造的就是这几种）。
防“空真”：两种模式都要求至少比到 1 个 usage_total 为 known 的 run（not_run 不算）；联调另要求至少 1 次带 X-Run-Id 的嵌入，
而且带 X-Run-Id 的嵌入属于这些 run。
run 记录来自 QS 的 demo-summary.json，或者 --queries（联调脚本直接调 QS 的 POST /queries 得到的记录，每行一个 JSON，
字段和摘要的记录同名）；摘要整体的几项（status、题目的结论）只在给了摘要时查。--queries 只能和 --e2e 一起用。
--qf-log（只能和 --e2e 一起用，可以给几次，每个 QF 实例一份）是联调里 QF 开着追踪时的日志
（docker compose logs --no-log-prefix）：再核对调用日志里的 trace_id、parent_span_id 和每个 run 的 trace_id，
以及日志里有预留的调用和账本一一对应（账本不记 trace）。经 QS 的真实演示不开追踪，不用它。
--jaeger（要有 --qf-log）是 Jaeger 查询接口的地址：等两边的 span 都导出到 Jaeger，再核对每个 run 的 trace 里两边的 span
接在一起。--served-by 再要求这个 run 的 QF span 都来自这个实例。
退出码：0 全部 ok；1 有 FAIL；2 跑不了（找不到容器、摘要不合法、Jaeger 连不上等）。

只用标准库，Python 3.10 起可用。账本在 qf-mysql 容器里用 root 只读查询，root 密码只在容器的 secret 文件里。
输出只有题号、run_id、次数、用量数和 span 的个数，纯 ASCII。
"""
import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict

USAGE_KEYS = (("prompt", "prompt_tokens"), ("completion", "completion_tokens"), ("total", "total_tokens"))
USAGE_STATUSES = ("known", "unknown", "not_run")
NAME = re.compile(r"[A-Za-z0-9_.-]+")
TRACE_ID = re.compile(r"[0-9a-f]{32}")  # W3C Trace Context：小写十六进制，不能全是 0
SPAN_ID = re.compile(r"[0-9a-f]{16}")
COLUMNS = "id purpose status http prompt completion total client run created error reserved".split()
INT_COLUMNS = ("http", "prompt", "completion", "total", "reserved")
CALLS_SQL = (
    "SELECT c.id, c.purpose, c.status, c.http_status, c.usage_prompt_tokens, c.usage_completion_tokens, c.usage_total_tokens,"
    " c.client_request_id, c.run_id, c.created_at, c.error_code, c.reserved_tokens FROM calls c JOIN accounts a ON a.id = c.account_id"
    " WHERE a.name = '{account}' ORDER BY c.created_at, c.id"
)
BUDGET_SQL = (
    "SELECT b.reserved_tokens, b.consumed_tokens, b.limit_tokens FROM budgets b JOIN accounts a ON a.id = b.account_id"
    " WHERE a.name = '{account}'"
)
LOG_FIELDS = ("request_id", "run_id", "trace_id", "span_id", "parent_span_id", "status", "error_code", "call_status")
# 联调里 QS 报 FAILED（usage_total 是 unknown）的 run，QF 账本里只有这几种形态说得通：
# QS 的错误码 -> (QF 比 QS 少记几次聊天, QF 最后一次聊天的 (状态, http_status, error_code)；None 表示那一次不在账本里)。
UNKNOWN_RUNS = {
    "model_quota_exhausted": (1, None),  # 预留时被拒（429 quota_exhausted）：不进账本，也不调上游
    "upstream_http_error": (0, ("USAGE_UNKNOWN", 504, "upstream_timeout")),  # QF 等上游超时，回 504
    "upstream_request_error": (0, ("USAGE_UNKNOWN", None, "reclaimed_in_flight")),  # 处理它的 QF 实例中途被杀，另一个实例回收
}
RUN_NOT_FINISHED = frozenset({"RUNNING", "CANCEL_REQUESTED", "WAITING_APPROVAL", "WAITING_USER"})
SPAN_SERVER, SPAN_CLIENT, STATUS_ERROR = 2, 3, 2  # OTLP 的枚举值
JAEGER_WAIT_SECONDS = 60


class CannotRun(Exception):
    """输入或环境有问题，核对跑不了（退出码 2）。"""


def verdict(ok, text):
    return ("ok" if ok else "FAIL", text)


# ---- 读账本 ----------------------------------------------------------------------------------------------------


def docker(*args):
    try:
        done = subprocess.run(["docker", *args], capture_output=True, text=True, encoding="utf-8", timeout=60)
    except (OSError, subprocess.TimeoutExpired) as err:
        raise CannotRun(f"cannot run docker: {type(err).__name__}")
    if done.returncode != 0:
        raise CannotRun(f"docker {args[0]} failed: {done.stderr.strip()[:200]}")
    return done.stdout


def sql(project, query):
    """在项目的 qf-mysql 容器里执行只读 SQL。密码经容器内的进程环境变量交给 mysql，不出现在命令行和输出里。"""
    ids = docker(
        "ps", "-q",
        "--filter", f"label=com.docker.compose.project={project}",
        "--filter", "label=com.docker.compose.service=qf-mysql",
    ).split()
    if len(ids) != 1:
        raise CannotRun(f"expected one running qf-mysql container of project {project}, found {len(ids)} (is QF running?)")
    script = 'MYSQL_PWD=$(cat /run/secrets/mysql_root_password) exec mysql -uroot -h127.0.0.1 -N -B quotaforge -e "$0"'
    return docker("exec", ids[0], "sh", "-c", script, query)


def parse_calls(text):
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        cells = line.split("\t")
        if len(cells) != len(COLUMNS):
            raise CannotRun(f"unexpected ledger row with {len(cells)} columns")
        row = dict(zip(COLUMNS, (None if cell == "NULL" else cell for cell in cells)))
        for name in INT_COLUMNS:
            if row[name] is not None:
                row[name] = int(row[name])
        rows.append(row)
    return rows


def read_ledger(project, account):
    """返回 (账户的全部调用, (已预留, 已消费, 额度))。"""
    budget = sql(project, BUDGET_SQL.format(account=account)).split()
    if len(budget) != 3:
        raise CannotRun(f"account {account} not found in the ledger")
    return parse_calls(sql(project, CALLS_SQL.format(account=account))), tuple(int(n) for n in budget)


def valid_id(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None and value.strip("0") != ""


def read_call_log(path, account):
    """账户的调用日志行（msg 是 call），只留 LOG_FIELDS。不是 JSON 的行（启动之外的杂项输出）跳过。"""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError as err:
        raise CannotRun(f"cannot read the QF log: {type(err).__name__}")
    rows = []
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict) and entry.get("msg") == "call" and entry.get("account") == account:
            rows.append({key: entry.get(key) for key in LOG_FIELDS})
    return rows


# ---- 读 run 记录 -------------------------------------------------------------------------------------------------


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def check_records(records, seen):
    """run 记录是信任边界：这里检查一次，之后的核对直接用。seen 是已经出现过的 run_id（摘要和 --queries 合起来不能重复）。"""
    for record in records:
        if not isinstance(record, dict):
            raise CannotRun("summary record is not an object")
        if record.get("trace_id") is not None and not valid_id(TRACE_ID, record["trace_id"]):
            raise CannotRun("summary has a trace_id that is not 32 lowercase hex digits")
        if record.get("error_code") is not None and not isinstance(record["error_code"], str):
            raise CannotRun("summary has an error_code that is not a string")
        run_id = record.get("run_id")
        if run_id is None:
            continue
        if not isinstance(run_id, str) or not run_id:
            raise CannotRun("summary has a run_id that is not a non-empty string")
        if run_id in seen:
            raise CannotRun(f"summary has run_id {run_id} twice")
        seen.add(run_id)
        if not is_int(record.get("model_call_count")):
            raise CannotRun(f"run {run_id} has no integer model_call_count")
        total = record.get("usage_total")
        if total is None:
            continue
        if not isinstance(total, dict) or total.get("status") not in USAGE_STATUSES:
            raise CannotRun(f"run {run_id} has an unknown usage_total status")
        if total["status"] == "known" and not all(is_int(total.get(key)) for _, key in USAGE_KEYS):
            raise CannotRun(f"run {run_id} is known but its usage numbers are not integers")


def load_summary(path):
    try:
        with open(path, encoding="utf-8-sig") as f:
            summary = json.load(f)
    except (OSError, ValueError) as err:
        raise CannotRun(f"cannot read the summary as JSON: {type(err).__name__}")
    if not isinstance(summary, dict) or not isinstance(summary.get("records"), list):
        raise CannotRun("summary has no records list")
    return summary


def load_queries(path):
    """--queries：每行一个 run 记录（scripts/qs_query.py 的输出）。"""
    try:
        with open(path, encoding="utf-8-sig") as f:
            return [json.loads(line) for line in f if line.strip()]
    except (OSError, ValueError) as err:
        raise CannotRun(f"cannot read the query records as JSON lines: {type(err).__name__}")


# ---- 核对（纯函数：run 记录和账本行进，结果行出）------------------------------------------------------------------


def by_status(rows):
    counts = Counter(row["status"] for row in rows)
    return ", ".join(f"{status} {n}" for status, n in sorted(counts.items())) or "none"


def run_problems(record, rows):
    total = record["usage_total"]
    problems = []
    if len(rows) != record["model_call_count"]:
        problems.append(f"chat calls QF {len(rows)} != QS {record['model_call_count']}")
    unsettled = [row for row in rows if row["status"] != "SETTLED"]
    if unsettled:
        problems.append(f"not SETTLED ({by_status(unsettled)})")
    for column, key in USAGE_KEYS:
        values = [row[column] for row in rows]
        if None in values:
            problems.append(f"{key} unknown in QF for some calls")
        elif sum(values) != total[key]:
            problems.append(f"{key} QF {sum(values)} != QS {total[key]}")
    return problems


def unknown_problems(record, rows):
    """联调里 usage_total 不是 known 的 run：QS 报的错误码在 UNKNOWN_RUNS 里、QF 账本的形态也和它一致，才说得通。"""
    code = record.get("error_code")
    if (record.get("usage_total") or {}).get("status") != "unknown" or code not in UNKNOWN_RUNS:
        return [f"QS error_code {code} is not one the end-to-end scenarios explain"]
    missing, last = UNKNOWN_RUNS[code]
    problems = []
    if len(rows) != record["model_call_count"] - missing:
        problems.append(f"chat calls QF {len(rows)} != QS {record['model_call_count']} - {missing} for {code}")
    head = rows[:-1] if last else rows
    if any(row["status"] != "SETTLED" for row in head):
        problems.append(f"the calls that should be SETTLED are not all SETTLED ({by_status(head)})")
    if last and rows and (rows[-1]["status"], rows[-1]["http"], rows[-1]["error"]) != last:
        got = " ".join(str(rows[-1][key]) for key in ("status", "http", "error"))
        problems.append(f"the last chat call is {got}, not {' '.join(map(str, last))}")
    return problems


def run_line(record, rows, e2e):
    """一个 run 的结果行和它算哪一类：known、not_run、unknown（联调里说得通的那几种）、skip（真实模式）。"""
    label = f"{record.get('id', '?')} {record['run_id']}"
    total = record.get("usage_total")
    status = total["status"] if total else "missing"
    if status == "known":
        problems = run_problems(record, rows)
        usage = "/".join(str(total[key]) for _, key in USAGE_KEYS)
        return verdict(not problems, f"{label}: " + ("; ".join(problems) or f"chat calls {len(rows)}, all SETTLED, usage {usage} match QS")), status
    if status == "not_run":
        return verdict(not rows, f"{label}: not_run, QF chat calls {len(rows)} == 0"), status
    if not e2e:
        return ("skip", f"{label}: usage_total {status}; QS chat calls {record['model_call_count']}, QF chat calls {len(rows)} ({by_status(rows)})"), "skip"
    problems = unknown_problems(record, rows)
    explained = f"QS chat calls {record['model_call_count']}, QF chat calls {len(rows)} ({by_status(rows)})"
    return verdict(not problems, f"{label}: usage_total is {status}, " + ("; ".join(problems) or f"as expected for {record['error_code']}: {explained}")), "unknown"


def run_lines(records, chat_by_run, e2e):
    """逐个 run 一行，再加计数和防空真的一项。返回 (结果行, 说得通的 USAGE_UNKNOWN 调用号)。"""
    out, unknown_calls, kinds = [], set(), Counter()
    for record in records:
        if not record.get("run_id"):
            continue
        rows = chat_by_run.get(record["run_id"], [])
        line, kind = run_line(record, rows, e2e)
        out.append(line)
        kinds[kind] += 1
        if kind == "unknown" and line[0] == "ok" and UNKNOWN_RUNS[record["error_code"]][1]:
            unknown_calls.add(rows[-1]["id"])
    without_run = sum(1 for record in records if not record.get("run_id"))
    compared = kinds["known"] + kinds["not_run"] + kinds["unknown"]
    out.append(("info", f"runs: {compared} compared, {kinds['skip']} skipped; {without_run} records without run_id"))
    # not_run 不算：它只证明账本里没有这个 run 的聊天，没有比到任何用量。全是 unknown 时什么也没验证，不能显示通过。
    nothing = "" if kinds["known"] or e2e else "; nothing was compared, see the skip lines above"
    out.append(verdict(kinds["known"] >= 1, f"at least one run with known usage_total was compared ({kinds['known']}){nothing}"))
    return out, unknown_calls


def summary_lines(summary, e2e):
    """QS 演示摘要整体的几项：联调要求固定的结论，真实模式只列出来。"""
    verdicts = dict(Counter(str(record.get("verdict")) for record in summary["records"]))
    if not e2e:
        return [("info", f"QS summary: status {summary.get('status')}, verdicts {verdicts}, known_gaps {summary.get('known_gaps')}")]
    return [
        verdict(summary.get("status") == "pass", f"QS summary: status is pass (got {summary.get('status')})"),
        verdict(summary.get("model_names") == ["qs-fake-upstream-v1"], f"QS summary: model_names is [qs-fake-upstream-v1] (got {summary.get('model_names')})"),
        verdict(verdicts == {"pass": 13, "not_applicable": 1}, f"QS summary: 13 pass, 1 not_applicable, 0 fail (got {verdicts})"),
        verdict(summary.get("known_gaps") == ["Q06:rowset_customers_differ"], f"QS summary: known_gaps is exactly Q06:rowset_customers_differ (got {summary.get('known_gaps')})"),
    ]


def outside_lines(calls, chat_by_run, runs, e2e):
    """账本里不属于这些 run 的调用：联调要求严，真实模式只数出来。"""
    extra = sorted(run for run in chat_by_run if run not in runs)
    extra_calls = sum(len(chat_by_run[run]) for run in extra)
    hint = "" if e2e else "; the ledger holds another demo's runs, restart with compose-real.sh (fresh ledger) and rerun"
    if extra:
        out = [verdict(False, f"{extra_calls} ledger chat calls belong to {len(extra)} runs not in the summary ({', '.join(extra[:10])}){hint}")]
    else:
        out = [verdict(True, "every ledger chat call with X-Run-Id belongs to a run in the summary")]
    chat_without_run = [row for row in calls if row["purpose"] == "chat" and not row["run"]]
    run_embeddings = [row for row in calls if row["purpose"] == "embeddings" and row["run"]]
    plain_embeddings = [row for row in calls if row["purpose"] == "embeddings" and not row["run"]]
    if not e2e:
        return out + [
            ("info", f"chat calls without X-Run-Id: {len(chat_without_run)} ({by_status(chat_without_run)})"),
            ("info", f"embeddings with X-Run-Id: {len(run_embeddings)} ({by_status(run_embeddings)})"),
            ("info", f"embeddings without X-Run-Id: {len(plain_embeddings)} ({by_status(plain_embeddings)})"),
        ]
    out.append(verdict(not chat_without_run, f"every chat call has X-Run-Id ({len(chat_without_run)} without)"))
    good = all(row["http"] == 200 and row["status"] == "SETTLED" and row["total"] is not None for row in run_embeddings)
    out.append(verdict(good, f"embeddings with X-Run-Id ({len(run_embeddings)}) are all 200, SETTLED, usage known"))
    stray_runs = sorted({row["run"] for row in run_embeddings if row["run"] not in runs})
    strays = sum(1 for row in run_embeddings if row["run"] not in runs)
    if strays:
        out.append(verdict(False, f"{strays} embeddings with X-Run-Id belong to {len(stray_runs)} runs not in the summary ({', '.join(stray_runs[:10])})"))
    else:
        out.append(verdict(True, "every embedding with X-Run-Id belongs to a run in the summary"))
    first = min((row["created"] for row in run_embeddings), default=None)
    early = all(first is None or row["created"] < first for row in plain_embeddings)
    out.append(verdict(early, f"embeddings without X-Run-Id ({len(plain_embeddings)}) all come before the first one with X-Run-Id (index build)"))
    out.append(verdict(len(run_embeddings) >= 1, f"the ledger has at least one embedding with X-Run-Id ({len(run_embeddings)})"))
    return out


def account_lines(calls, budget, unknown_calls):
    """账户整体（只有联调）：除了说得通的那几次 USAGE_UNKNOWN 全部结算；已消费、预留都对得上，没有超过额度。"""
    reserved, consumed, limit = budget
    settled = [row for row in calls if row["status"] == "SETTLED"]
    used = sum(row["total"] or 0 for row in settled)
    held = sum(row["reserved"] for row in calls if row["id"] in unknown_calls)
    others = [row for row in calls if row["id"] not in unknown_calls]
    return [
        verdict(all(row["client"] for row in calls), f"every one of the {len(calls)} calls has X-Client-Request-Id"),
        verdict(all(row["status"] == "SETTLED" for row in others),
                f"all {len(calls)} calls are SETTLED except {len(unknown_calls)} USAGE_UNKNOWN of the runs above ({by_status(calls)})"),
        verdict(consumed == used and reserved == held and consumed + reserved <= limit and all(row["total"] is not None for row in settled),
                f"account consumed {consumed} == usage total of the SETTLED calls {used}, reserved {reserved} == held by the unknown calls {held},"
                f" consumed + reserved <= limit {limit}"),
    ]


def reconcile(summary, calls, budget, e2e, queries=()):
    """返回 [(级别, 文字)]，级别是 ok、FAIL、skip、info。summary 是 QS 的演示摘要（可以是 None），queries 是脚本直接调出的 run 记录。"""
    records = (summary["records"] if summary else []) + list(queries)
    chat_by_run = defaultdict(list)
    for row in calls:
        if row["purpose"] == "chat" and row["run"]:
            chat_by_run[row["run"]].append(row)
    runs = {record["run_id"] for record in records if record.get("run_id")}
    out = summary_lines(summary, e2e) if summary else []
    lines, unknown_calls = run_lines(records, chat_by_run, e2e)
    out += lines + outside_lines(calls, chat_by_run, runs, e2e)
    if e2e:
        out += account_lines(calls, budget, unknown_calls)
    return out


def trace_checks(records, log_rows):
    """联调的三项 trace 检查（纯函数：run 记录和日志行进，结果行出）。每个 run 的 trace_id 以 QS 的记录为准，QF 不从 run 反推。"""
    run_trace = {record["run_id"]: record.get("trace_id") for record in records if record.get("run_id")}
    in_run = [row for row in log_rows if row["run_id"]]
    outside = [row for row in log_rows if not row["run_id"]]

    bad = [row for row in log_rows if not (valid_id(SPAN_ID, row["parent_span_id"]) and valid_id(TRACE_ID, row["trace_id"]))]
    wrong = [row for row in in_run if not run_trace.get(row["run_id"]) or row["trace_id"] != run_trace[row["run_id"]]]
    runs = len({row["run_id"] for row in in_run})
    run_traces = {trace for trace in run_trace.values() if trace}
    shared = [row for row in outside if row["trace_id"] in run_traces]
    traces = len({row["trace_id"] for row in outside})
    return [
        verdict(log_rows and not bad, f"trace: every call log line has a valid parent_span_id and trace_id ({len(log_rows)} lines, {len(bad)} without)"),
        verdict(in_run and not wrong, f"trace: {len(in_run)} calls of {runs} runs carry the trace_id of their run in the summary ({len(wrong)} do not)"),
        verdict(outside and not shared, f"trace: {len(outside)} calls without X-Run-Id are on {traces} traces, none of them a run's ({len(shared)} are)"),
    ]


def call_log_check(calls, log_rows):
    """日志里有预留的调用行和账本的调用一一对应（按调用号）。只有回收标成未知的调用可以没有日志行：处理它的实例中途死了，
    日志没写成。预留之前就被拒的行（例如 429 quota_exhausted）不进账本，单独计数。"""
    logged = [row["request_id"] for row in log_rows if row["call_status"]]
    ledger = {row["id"] for row in calls}
    unlogged_ok = {row["id"] for row in calls if row["error"] == "reclaimed_in_flight"} - set(logged)
    missing = ledger - set(logged) - unlogged_ok
    extra = set(logged) - ledger
    twice = len(logged) - len(set(logged))
    refused = sum(1 for row in log_rows if not row["call_status"])
    return verdict(
        calls and not missing and not extra and not twice,
        f"call log: {len(logged)} lines with a reservation == {len(ledger)} ledger calls ({len(unlogged_ok)} reclaimed after their"
        f" instance died, without a line; {len(missing)} ledger calls without a line, {len(extra)} lines not in the ledger,"
        f" {twice} repeated); {refused} lines refused before reservation",
    )


# ---- Jaeger：两边的 span 接在一起 -----------------------------------------------------------------------------------


def jaeger_get(url):
    """取 Jaeger 查询接口的 JSON。还没有这个 trace 时 Jaeger 回 404，当作空的。直连，不走代理。"""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=10) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as err:
        err.close()  # HTTPError 也是一个响应，不关就留着连接
        if err.code == 404:
            return {}
        raise CannotRun(f"Jaeger answered {err.code}")
    except (OSError, ValueError) as err:
        raise CannotRun(f"cannot query Jaeger: {type(err).__name__}")


def attributes(holder):
    return {item.get("key"): next(iter(item.get("value", {}).values()), None) for item in holder.get("attributes", [])}


def flatten(trace):
    """Jaeger 的 OTLP JSON（信任边界）摊平成 span 的列表，每个带服务名和实例名。只取编号、名字、枚举和属性，不取别的。"""
    spans = []
    for resource in trace.get("result", {}).get("resourceSpans", []):
        service = attributes(resource.get("resource", {}))
        for scope in resource.get("scopeSpans", []):
            for span in scope.get("spans", []):
                status = span.get("status", {})
                spans.append({
                    "id": span.get("spanId"), "parent": span.get("parentSpanId") or None, "name": span.get("name"), "kind": span.get("kind"),
                    "error": status.get("code") == STATUS_ERROR, "message": status.get("message"), "attrs": attributes(span),
                    "service": service.get("service.name"), "instance": service.get("service.instance.id"),
                })
    return spans


def expected_spans(records, log_rows):
    """每个 run 的 trace 里一定要有的 span：QF 日志里这个 trace 上每次调用的根 span（span_id）和 QS 那次尝试的 span（parent_span_id）。"""
    want = {record["trace_id"]: set() for record in records if record.get("run_id") and record.get("trace_id")}
    for row in log_rows:
        if row["trace_id"] in want:
            want[row["trace_id"]].update(span for span in (row["span_id"], row["parent_span_id"]) if span)
    return want


def complete(spans, want):
    """该有的 span 都到了，而且 QS 的 run 已经结束（终态的 queryshield.run span 最后结束；QS 按结束的先后批量导出）。"""
    finished = any(s["name"] == "queryshield.run" and s["attrs"].get("queryshield.status") not in RUN_NOT_FINISHED | {None} for s in spans)
    return finished and want <= {s["id"] for s in spans}


def fetch_traces(base, want, get=jaeger_get, clock=time.monotonic, pause=time.sleep, wait=JAEGER_WAIT_SECONDS):
    """等到每个 trace 里该有的 span 都进了 Jaeger（两边都是批量导出），最多等 wait 秒；到点还没齐的照原样返回，检查会失败。"""
    deadline = clock() + wait
    traces, pending = {}, set(want)
    while True:
        for trace_id in sorted(pending):
            traces[trace_id] = flatten(get(f"{base}/api/v3/traces/{trace_id}"))
            if complete(traces[trace_id], want[trace_id]):
                pending.discard(trace_id)
        if not pending or clock() >= deadline:
            return traces
        pause(0.5)


def server_spans(traces):
    return [(trace_id, s) for trace_id, spans in traces.items() for s in spans if s["service"] == "quotaforge" and s["kind"] == SPAN_SERVER]


def parent_of(traces, trace_id, span):
    return next((s for s in traces.get(trace_id, []) if s["id"] == span["parent"]), None)


def is_attempt(span):
    return bool(span) and span["service"] == "queryshield" and span["name"] == "queryshield.http_attempt" and span["kind"] == SPAN_CLIENT


def children(traces, trace_id, span):
    return [s for s in traces.get(trace_id, []) if s["parent"] == span["id"]]


def jaeger_basics(records, log_rows, services, traces):
    """前三项：两个服务都在；每个 run 的 trace 里两边该有的 span 都在；QF 的每个根 span（SERVER）挂在 QS 那次尝试的 span 下。"""
    runs = [record for record in records if record.get("run_id")]
    want = expected_spans(records, log_rows)
    logged = Counter(row["run_id"] for row in log_rows)  # 按 run 数：QF 没接上 QS 的 trace 时，它的日志行在别的 trace 上
    incomplete, qs_total, qf_total = 0, 0, 0
    for record in runs:
        spans = traces.get(record.get("trace_id"), [])
        qs = sum(1 for s in spans if s["service"] == "queryshield")
        qf = sum(1 for s in spans if s["service"] == "quotaforge")
        qs_total, qf_total = qs_total + qs, qf_total + qf
        # 日志里一行都没有的 run（处理它的实例在调用中途被杀）不要求 QF 的 span
        if not qs or (logged[record["run_id"]] and not qf) or not want.get(record.get("trace_id"), {None}) <= {s["id"] for s in spans}:
            incomplete += 1
    roots = server_spans(traces)
    orphans = sum(1 for trace_id, s in roots if not is_attempt(parent_of(traces, trace_id, s)))
    return [
        verdict({"quotaforge", "queryshield"} <= set(services), f"jaeger: services quotaforge and queryshield are both there (got {sorted(services)})"),
        verdict(runs and not incomplete, f"jaeger: {len(runs)} run traces have all their spans, {qs_total} from queryshield and {qf_total} from quotaforge ({incomplete} incomplete)"),
        verdict(roots and not orphans, f"jaeger: {len(roots)} quotaforge SERVER spans each hang under a queryshield.http_attempt span of the same trace ({orphans} do not)"),
    ]


def jaeger_errors(records, traces):
    """第四项：QS 的 ERROR span 只能是这个场景里 QS 报 FAILED 的 run 自己的错误码；每个这样的 run 至少有一个。"""
    failed = {record["trace_id"]: record["error_code"] for record in records if record.get("run_id") and record.get("error_code") in UNKNOWN_RUNS}
    errors = [(trace_id, s["attrs"].get("queryshield.error_code")) for trace_id, spans in traces.items() for s in spans
              if s["service"] == "queryshield" and s["error"]]
    # 按 span 所在的 trace 比：不是失败的 run 的 trace，或者码不是那个 run 自己的，都不对（不能只看码在不在所有失败的码里）
    bad = sum(1 for trace_id, code in errors if trace_id not in failed or failed[trace_id] != code)
    unmarked = sum(1 for trace_id, code in failed.items() if (trace_id, code) not in errors)
    codes = dict(sorted(Counter(str(code) for _, code in errors).items()))
    return [verdict(not bad and not unmarked,
                    f"jaeger: queryshield ERROR spans {codes} are all the failed runs' own codes ({bad} are not);"
                    f" {len(failed)} failed runs each have one with their code ({unmarked} do not)")]


def jaeger_failures(log_rows, traces):
    """QF 拒绝或超时的那几次调用，在 Jaeger 里两边各自怎么记（只在有这样的调用时出现）。"""
    by_id = {s["id"]: (trace_id, s) for trace_id, spans in traces.items() for s in spans if s["service"] == "quotaforge"}
    out = []
    refused = [by_id.get(row["span_id"]) for row in log_rows if not row["call_status"] and row["error_code"] == "quota_exhausted"]
    if refused:
        bad = sum(1 for hit in refused if not hit or not refused_cleanly(traces, *hit))
        out.append(verdict(not bad, f"jaeger: {len(refused)} requests QF refused for quota have a failed reserve span and no upstream span ({bad} do not)"))
    timed_out = [by_id.get(row["span_id"]) for row in log_rows if row["status"] == 504 and row["error_code"] == "upstream_timeout"]
    if timed_out:
        bad = sum(1 for hit in timed_out if not hit or not timed_out_on_both_sides(traces, *hit))
        out.append(verdict(not bad, f"jaeger: {len(timed_out)} calls QF timed out on: QF root and upstream spans failed with upstream_timeout,"
                                    f" under a queryshield attempt that recorded 504 upstream_timeout ({bad} do not)"))
    return out


def refused_cleanly(traces, trace_id, root):
    kids = children(traces, trace_id, root)
    return any(s["name"] == "reserve" and s["error"] for s in kids) and not any(s["name"] == "upstream" for s in kids)


def timed_out_on_both_sides(traces, trace_id, root):
    attempt = parent_of(traces, trace_id, root)
    upstream = [s for s in children(traces, trace_id, root) if s["name"] == "upstream"]
    return (root["error"] and root["message"] == "upstream_timeout" and len(upstream) == 1 and upstream[0]["error"] and is_attempt(attempt)
            and str(attempt["attrs"].get("http.response.status_code")) == "504"
            and attempt["attrs"].get("queryshield.provider_error_code") == "upstream_timeout")


def served_by_check(records, traces, run_id, instance):
    """--served-by：这个 run 的 QF SERVER span 都来自这个实例（两个实例的场景里按构造确定）。"""
    trace_id = next((record.get("trace_id") for record in records if record.get("run_id") == run_id), None)
    roots = [s for t, s in server_spans(traces) if t == trace_id]
    others = sum(1 for s in roots if s["instance"] != instance)
    return verdict(roots and not others, f"jaeger: run {run_id} was served by {instance} only ({len(roots)} SERVER spans, {others} from another instance)")


def jaeger_checks(records, log_rows, services, traces, served_by=None):
    """跨服务 trace 的检查（纯函数：run 记录、日志行和 Jaeger 里的 trace 进，结果行出）。只报个数，不打印 span 的内容。"""
    out = jaeger_basics(records, log_rows, services, traces) + jaeger_errors(records, traces) + jaeger_failures(log_rows, traces)
    if served_by:
        out.append(served_by_check(records, traces, *served_by))
    instances = dict(sorted(Counter(str(s["instance"]) for _, s in server_spans(traces)).items()))
    out.append(("info", f"jaeger: quotaforge SERVER spans by instance {instances}"))
    return out


# ---- 入口 ------------------------------------------------------------------------------------------------------


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Reconcile QS runs with the QF ledger, run by run.")
    parser.add_argument("--e2e", action="store_true", help="strict mode for the fake-upstream end-to-end run")
    parser.add_argument("--project", required=True, help="Compose project name of QF")
    parser.add_argument("--account", required=True, help="QF account name")
    parser.add_argument("--qf-log", action="append", help="a QF instance's log (docker compose logs --no-log-prefix), adds the log checks; needs --e2e")
    parser.add_argument("--queries", help="run records from scripts/qs_query.py, one JSON per line; needs --e2e")
    parser.add_argument("--jaeger", help="Jaeger query URL, adds the cross-service trace checks; needs --qf-log")
    parser.add_argument("--served-by", metavar="RUN_ID=INSTANCE", help="the run's QF spans must all come from this instance; needs --jaeger")
    parser.add_argument("summary", nargs="?", help="path of QS's demo-summary.json")
    args = parser.parse_args(argv)
    if (args.qf_log or args.queries) and not args.e2e:
        parser.error("--qf-log and --queries need --e2e: only the end-to-end run has tracing on and drives QS directly")
    if args.jaeger and not args.qf_log:
        parser.error("--jaeger needs --qf-log: the spans to wait for come from the QF log")
    if args.served_by and (not args.jaeger or "=" not in args.served_by):
        parser.error("--served-by needs --jaeger and the form RUN_ID=INSTANCE")
    if not args.summary and not args.queries:
        parser.error("give a demo summary, --queries, or both")
    return args


def gather(args):
    """读全部输入，跑不了就抛 CannotRun。返回 (摘要, --queries 的记录, 账本, 日志行, 每份日志的行数)。"""
    for value in (args.project, args.account):
        if not NAME.fullmatch(value):
            raise CannotRun("project and account names may only contain letters, digits, '_', '.', '-'")
    summary = load_summary(args.summary) if args.summary else None
    queries = load_queries(args.queries) if args.queries else []
    check_records((summary["records"] if summary else []) + queries, set())
    calls, budget = read_ledger(args.project, args.account)
    logs = [read_call_log(path, args.account) for path in args.qf_log or []]
    return summary, queries, calls, budget, logs


def main(argv):
    args = parse_args(argv)
    try:
        summary, queries, calls, budget, logs = gather(args)
        results = reconcile(summary, calls, budget, args.e2e, queries)
        if logs:
            records = (summary["records"] if summary else []) + queries
            log_rows = [row for rows in logs for row in rows]
            if len(logs) > 1:
                results.append(("info", "call log lines of the account per QF instance: " + ", ".join(str(len(rows)) for rows in logs)))
            results += trace_checks(records, log_rows) + [call_log_check(calls, log_rows)]
        if args.jaeger:
            traces = fetch_traces(args.jaeger, expected_spans(records, log_rows))
            # 等完 span 再取服务列表：QS 的 span 进了 Jaeger，queryshield 这个服务才一定在
            services = jaeger_get(f"{args.jaeger}/api/v3/services").get("services", [])
            results += jaeger_checks(records, log_rows, services, traces, args.served_by and args.served_by.split("=", 1))
    except CannotRun as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    for level, text in results:
        print(f"{level:<4} {text}")
    failed = sum(1 for level, _ in results if level == "FAIL")
    print(f"check_qs_runs: {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
