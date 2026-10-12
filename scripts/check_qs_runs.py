#!/usr/bin/env python3
"""逐个 run 核对：QS 的 demo-summary.json 对 QF 账本（联调和经 QS 的真实演示共用，用法见 docs/queryshield.md）。

  check_qs_runs.py [--e2e] --project <QF 的 Compose 项目名> --account <账户名> <demo-summary.json>

默认是真实模式：只对 usage_total 为 known 的 run 比次数和用量，unknown 的列出来（skip），不算失败。
--e2e 是联调模式（fake-upstream，不花钱），规则更严：每个 run 都得是 known，账户里的每次调用都得对得上。
防“空真”：两种模式都要求至少比到 1 个 usage_total 为 known 的 run（not_run 不算）；联调另要求至少 1 次带 X-Run-Id 的嵌入，
而且带 X-Run-Id 的嵌入属于摘要里的 run。
退出码：0 全部 ok；1 有 FAIL；2 跑不了（找不到容器、摘要不合法等）。

只用标准库，Python 3.10 起可用。账本在 qf-mysql 容器里用 root 只读查询，root 密码只在容器的 secret 文件里。
输出只有题号、run_id、次数和用量数，纯 ASCII。
"""
import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict

USAGE_KEYS = (("prompt", "prompt_tokens"), ("completion", "completion_tokens"), ("total", "total_tokens"))
USAGE_STATUSES = ("known", "unknown", "not_run")
NAME = re.compile(r"[A-Za-z0-9_.-]+")
COLUMNS = "id purpose status http prompt completion total client run created".split()
INT_COLUMNS = ("http", "prompt", "completion", "total")
CALLS_SQL = (
    "SELECT c.id, c.purpose, c.status, c.http_status, c.usage_prompt_tokens, c.usage_completion_tokens, c.usage_total_tokens,"
    " c.client_request_id, c.run_id, c.created_at FROM calls c JOIN accounts a ON a.id = c.account_id"
    " WHERE a.name = '{account}' ORDER BY c.created_at, c.id"
)
BUDGET_SQL = (
    "SELECT b.reserved_tokens, b.consumed_tokens FROM budgets b JOIN accounts a ON a.id = b.account_id WHERE a.name = '{account}'"
)


class CannotRun(Exception):
    """输入或环境有问题，核对跑不了（退出码 2）。"""


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
    """返回 (账户的全部调用, (已预留, 已消费))。"""
    budget = sql(project, BUDGET_SQL.format(account=account)).split()
    if len(budget) != 2:
        raise CannotRun(f"account {account} not found in the ledger")
    return parse_calls(sql(project, CALLS_SQL.format(account=account))), (int(budget[0]), int(budget[1]))


# ---- 读摘要 ----------------------------------------------------------------------------------------------------


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def load_summary(path):
    """摘要是信任边界：这里检查一次，之后的核对直接用。"""
    try:
        with open(path, encoding="utf-8-sig") as f:
            summary = json.load(f)
    except (OSError, ValueError) as err:
        raise CannotRun(f"cannot read the summary as JSON: {type(err).__name__}")
    if not isinstance(summary, dict) or not isinstance(summary.get("records"), list):
        raise CannotRun("summary has no records list")
    seen = set()
    for record in summary["records"]:
        if not isinstance(record, dict):
            raise CannotRun("summary record is not an object")
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
    return summary


# ---- 核对（纯函数：摘要和账本行进，结果行出）----------------------------------------------------------------------


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


def reconcile(summary, calls, budget, e2e):
    """返回 [(级别, 文字)]，级别是 ok、FAIL、skip、info。"""
    out = []

    def check(ok, text):
        out.append(("ok" if ok else "FAIL", text))

    records = summary["records"]
    chat = [row for row in calls if row["purpose"] == "chat"]
    embeddings = [row for row in calls if row["purpose"] == "embeddings"]
    chat_by_run = defaultdict(list)
    for row in chat:
        if row["run"]:
            chat_by_run[row["run"]].append(row)
    runs = {record["run_id"] for record in records if record.get("run_id")}

    # QS 摘要本身
    verdicts = dict(Counter(str(record.get("verdict")) for record in records))
    if e2e:
        check(summary.get("status") == "pass", f"QS summary: status is pass (got {summary.get('status')})")
        check(summary.get("model_names") == ["qs-fake-upstream-v1"], f"QS summary: model_names is [qs-fake-upstream-v1] (got {summary.get('model_names')})")
        check(verdicts == {"pass": 13, "not_applicable": 1}, f"QS summary: 13 pass, 1 not_applicable, 0 fail (got {verdicts})")
        check(summary.get("known_gaps") == ["Q06:rowset_customers_differ"], f"QS summary: known_gaps is exactly Q06:rowset_customers_differ (got {summary.get('known_gaps')})")
    else:
        out.append(("info", f"QS summary: status {summary.get('status')}, verdicts {verdicts}, known_gaps {summary.get('known_gaps')}"))

    # 逐个 run
    compared = skipped = known = 0
    for record in records:
        run_id = record.get("run_id")
        if not run_id:
            continue
        label = f"{record.get('id', '?')} {run_id}"
        rows = chat_by_run.get(run_id, [])
        total = record.get("usage_total")
        status = total["status"] if total else "missing"
        if status == "known":
            compared += 1
            known += 1
            problems = run_problems(record, rows)
            usage = "/".join(str(total[key]) for _, key in USAGE_KEYS)
            check(not problems, f"{label}: " + (f"chat calls {len(rows)}, all SETTLED, usage {usage} match QS" if not problems else "; ".join(problems)))
        elif status == "not_run":
            compared += 1
            check(not rows, f"{label}: not_run, QF chat calls {len(rows)} == 0")
        elif e2e:
            check(False, f"{label}: usage_total is {status}, the fake upstream always reports usage")
        else:
            skipped += 1
            out.append(("skip", f"{label}: usage_total {status}; QS chat calls {record['model_call_count']}, QF chat calls {len(rows)} ({by_status(rows)})"))
    without_run = sum(1 for record in records if not record.get("run_id"))
    out.append(("info", f"runs: {compared} compared, {skipped} skipped; {without_run} records without run_id"))
    # not_run 不算：它只证明账本里没有这个 run 的聊天，没有比到任何用量。全是 unknown 时什么也没验证，不能显示通过。
    nothing = "" if known or e2e else "; nothing was compared, see the skip lines above"
    check(known >= 1, f"at least one run with known usage_total was compared ({known}){nothing}")

    # 账本里摘要以外的 run
    extra = sorted(run for run in chat_by_run if run not in runs)
    extra_calls = sum(len(chat_by_run[run]) for run in extra)
    hint = "" if e2e else "; the ledger holds another demo's runs, restart with compose-real.ps1 (fresh ledger) and rerun"
    if extra:
        check(False, f"{extra_calls} ledger chat calls belong to {len(extra)} runs not in the summary ({', '.join(extra[:10])}){hint}")
    else:
        check(True, "every ledger chat call with X-Run-Id belongs to a run in the summary")

    # 不属于任何摘要 run 的调用：联调要求严，真实模式只数出来
    chat_without_run = [row for row in chat if not row["run"]]
    run_embeddings = [row for row in embeddings if row["run"]]
    plain_embeddings = [row for row in embeddings if not row["run"]]
    if e2e:
        check(not chat_without_run, f"every chat call has X-Run-Id ({len(chat_without_run)} without)")
        good = all(row["http"] == 200 and row["status"] == "SETTLED" and row["total"] is not None for row in run_embeddings)
        check(good, f"embeddings with X-Run-Id ({len(run_embeddings)}) are all 200, SETTLED, usage known")
        strays = [row for row in run_embeddings if row["run"] not in runs]
        stray_runs = sorted({row["run"] for row in strays})
        if strays:
            check(False, f"{len(strays)} embeddings with X-Run-Id belong to {len(stray_runs)} runs not in the summary ({', '.join(stray_runs[:10])})")
        else:
            check(True, "every embedding with X-Run-Id belongs to a run in the summary")
        first = min((row["created"] for row in run_embeddings), default=None)
        early = all(first is None or row["created"] < first for row in plain_embeddings)
        check(early, f"embeddings without X-Run-Id ({len(plain_embeddings)}) all come before the first one with X-Run-Id (index build)")
    else:
        out.append(("info", f"chat calls without X-Run-Id: {len(chat_without_run)} ({by_status(chat_without_run)})"))
        out.append(("info", f"embeddings with X-Run-Id: {len(run_embeddings)} ({by_status(run_embeddings)})"))
        out.append(("info", f"embeddings without X-Run-Id: {len(plain_embeddings)} ({by_status(plain_embeddings)})"))

    # 账户整体（只有联调检查）
    if e2e:
        reserved, consumed = budget
        used = sum(row["total"] or 0 for row in calls)
        check(all(row["client"] for row in calls), f"every one of the {len(calls)} calls has X-Client-Request-Id")
        check(all(row["status"] == "SETTLED" for row in calls), f"all {len(calls)} calls are SETTLED ({by_status(calls)})")
        check(consumed == used and reserved == 0 and all(row["total"] is not None for row in calls),
              f"account consumed {consumed} == usage total of the calls {used}, reserved {reserved} == 0")
        check(len(run_embeddings) >= 1, f"the ledger has at least one embedding with X-Run-Id ({len(run_embeddings)})")
    return out


def main(argv):
    parser = argparse.ArgumentParser(description="Reconcile a QS demo-summary.json with the QF ledger, run by run.")
    parser.add_argument("--e2e", action="store_true", help="strict mode for the fake-upstream end-to-end run")
    parser.add_argument("--project", required=True, help="Compose project name of QF")
    parser.add_argument("--account", required=True, help="QF account name")
    parser.add_argument("summary", help="path of QS's demo-summary.json")
    args = parser.parse_args(argv)
    try:
        for value in (args.project, args.account):
            if not NAME.fullmatch(value):
                raise CannotRun("project and account names may only contain letters, digits, '_', '.', '-'")
        summary = load_summary(args.summary)
        calls, budget = read_ledger(args.project, args.account)
    except CannotRun as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    results = reconcile(summary, calls, budget, args.e2e)
    for level, text in results:
        print(f"{level:<4} {text}")
    failed = sum(1 for level, _ in results if level == "FAIL")
    print(f"check_qs_runs: {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
