"""check_qs_runs.py 的单元测试：不需要 Docker，用构造的摘要和账本行。

  python3 -m unittest discover -s scripts -p 'test_check_qs_runs.py'     （在项目根目录下）

表格每一格（两种模式、通过和失败）各有用例；输入错误走退出码 2。
"""
import contextlib
import copy
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_qs_runs as cq  # noqa: E402

RUN_IDS = [f"run-{i:04d}" for i in range(1, 13)]
NOT_RUN = RUN_IDS[10]  # 第 11 个 run 没有聊天调用
LIMIT = 1000000


def stamp(n):
    return f"2026-10-08 10:{n // 60:02d}:{n % 60:02d}.000000"


def world():
    """一次干净的联调：12 个 run（11 个 known，1 个 not_run）、Q12 没有 run、Q04b 不适用、2 次建索引的嵌入。"""
    records, calls, clock = [], [], [0]

    def add(purpose, run=None, prompt=0, completion=0):
        clock[0] += 1
        total = prompt + completion
        calls.append({
            "id": f"call-{clock[0]:04d}", "purpose": purpose, "status": "SETTLED", "http": 200,
            "prompt": prompt, "completion": completion if purpose == "chat" else 0, "total": total,
            "client": f"client-{clock[0]:04d}", "run": run, "created": stamp(clock[0]), "error": None, "reserved": 1000 + clock[0],
        })

    add("embeddings", prompt=7)
    add("embeddings", prompt=7)
    for i, run in enumerate(RUN_IDS, start=1):
        record = {"id": f"Q{i:02d}", "verdict": "pass", "run_id": run}
        if run == NOT_RUN:
            record.update(model_call_count=0, usage_total={"status": "not_run", "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        else:
            record.update(model_call_count=2, usage_total={"status": "known", "prompt_tokens": 2 * (10 + i), "completion_tokens": 10, "total_tokens": 2 * (15 + i)})
            add("chat", run, prompt=10 + i, completion=5)
            add("chat", run, prompt=10 + i, completion=5)
        add("embeddings", run, prompt=3)
        records.append(record)
    records.append({"id": "Q12", "verdict": "pass", "run_id": None, "model_call_count": None, "usage_total": None})
    records.append({"id": "Q04b", "verdict": "not_applicable"})
    summary = {"status": "pass", "model_names": ["qs-fake-upstream-v1"], "known_gaps": ["Q06:rowset_customers_differ"], "records": records}
    return summary, calls, [0, sum(c["total"] for c in calls), LIMIT]


def budget_of(calls):
    return [0, sum(c["total"] or 0 for c in calls), LIMIT]


def record_of(summary, run):
    return next(r for r in summary["records"] if r.get("run_id") == run)


def chat_of(calls, run):
    return [c for c in calls if c["purpose"] == "chat" and c["run"] == run]


class Cases(unittest.TestCase):
    def results(self, summary, calls, budget, e2e):
        return cq.reconcile(summary, calls, budget, e2e)

    def fails(self, summary, calls, budget, e2e):
        return [text for level, text in self.results(summary, calls, budget, e2e) if level == "FAIL"]

    def assertOnlyFail(self, fails, *needles):
        """恰好这些检查失败：每个 needle 对应且只对应一行。"""
        self.assertEqual(len(fails), len(needles), fails)
        for needle in needles:
            self.assertEqual(sum(needle in text for text in fails), 1, (needle, fails))

    def both(self):
        return (True, False)


class CleanWorld(Cases):
    def test_clean_world_has_no_failures_in_both_modes(self):
        for e2e in self.both():
            summary, calls, budget = world()
            self.assertEqual(self.fails(summary, calls, budget, e2e), [])

    def test_output_is_ascii_and_has_one_line_per_run(self):
        summary, calls, budget = world()
        lines = self.results(summary, calls, budget, True)
        for _, text in lines:
            text.encode("ascii")
        self.assertEqual(sum(1 for _, text in lines if text.startswith("Q") and "run-" in text), 12)


class KnownRuns(Cases):
    def test_chat_count_differs_only_in_count(self):
        for e2e in self.both():
            summary, calls, budget = world()
            record_of(summary, RUN_IDS[0])["model_call_count"] = 3
            self.assertOnlyFail(self.fails(summary, calls, budget, e2e), "chat calls QF 2 != QS 3")

    def test_extra_zero_usage_chat_call_in_qf_fails_on_count_alone(self):
        for e2e in self.both():
            summary, calls, budget = world()
            calls.append(dict(chat_of(calls, RUN_IDS[0])[0], id="extra", prompt=0, completion=0, total=0))
            self.assertOnlyFail(self.fails(summary, calls, budget_of(calls), e2e), "chat calls QF 3 != QS 2")

    def test_each_usage_number_is_compared_separately(self):
        for e2e in self.both():
            for key, needle in (("prompt_tokens", "prompt_tokens QF"), ("completion_tokens", "completion_tokens QF"), ("total_tokens", "total_tokens QF")):
                summary, calls, budget = world()
                record_of(summary, RUN_IDS[0])["usage_total"][key] += 1
                self.assertOnlyFail(self.fails(summary, calls, budget, e2e), needle)

    def test_prompt_and_completion_moved_in_opposite_directions_keep_the_total(self):
        for e2e in self.both():
            summary, calls, budget = world()
            first, second = chat_of(calls, RUN_IDS[0])
            first["prompt"] += 1
            first["completion"] -= 1
            fails = self.fails(summary, calls, budget, e2e)
            self.assertEqual(len(fails), 1, fails)
            self.assertTrue("prompt_tokens QF 23 != QS 22" in fails[0] and "completion_tokens QF 9 != QS 10" in fails[0] and "total_tokens" not in fails[0], fails)

    def test_unsettled_chat_call_fails(self):
        for e2e in self.both():
            summary, calls, budget = world()
            chat_of(calls, RUN_IDS[0])[0]["status"] = "USAGE_UNKNOWN"
            fails = self.fails(summary, calls, budget, e2e)
            self.assertTrue(any("not SETTLED (USAGE_UNKNOWN 1)" in text for text in fails), fails)

    def test_chat_call_with_unknown_usage_fails(self):
        summary, calls, budget = world()
        chat_of(calls, RUN_IDS[0])[0].update(prompt=None, completion=None, total=None)
        self.assertTrue(any("unknown in QF" in text for text in self.fails(summary, calls, budget, False)))

    def test_call_moved_to_another_run_is_caught(self):
        for e2e in self.both():
            summary, calls, budget = world()
            chat_of(calls, RUN_IDS[0])[0]["run"] = RUN_IDS[1]
            self.assertEqual(sum(RUN_IDS[0] in t or RUN_IDS[1] in t for t in self.fails(summary, calls, budget, e2e)), 2)


class NotRun(Cases):
    def test_not_run_with_no_chat_calls_is_ok(self):
        for e2e in self.both():
            summary, calls, budget = world()
            lines = self.results(summary, calls, budget, e2e)
            self.assertIn(("ok", f"Q11 {NOT_RUN}: not_run, QF chat calls 0 == 0"), lines)

    def test_not_run_with_chat_calls_fails(self):
        for e2e in self.both():
            summary, calls, budget = world()
            extra = dict(chat_of(calls, RUN_IDS[0])[0], id="extra", run=NOT_RUN)
            calls.append(extra)
            self.assertOnlyFail(self.fails(summary, calls, budget_of(calls), e2e), "not_run, QF chat calls 1 == 0")


class UnknownRuns(Cases):
    def unknown(self, how):
        summary, calls, budget = world()
        record = record_of(summary, RUN_IDS[0])
        if how == "missing":
            record["usage_total"] = None
        else:
            record["usage_total"] = {"status": "unknown", "prompt_tokens": None, "completion_tokens": None, "total_tokens": None}
        record["model_call_count"] = 1  # 两边的次数不同：真实模式也不能拿它去比
        return summary, calls, budget

    def test_e2e_fails_on_unknown_and_on_missing_usage_total(self):
        for how in ("unknown", "missing"):
            self.assertOnlyFail(self.fails(*self.unknown(how), True), f"usage_total is {how}")

    def test_real_mode_skips_with_both_counts_and_the_call_states(self):
        for how in ("unknown", "missing"):
            summary, calls, budget = self.unknown(how)
            chat_of(calls, RUN_IDS[0])[1]["status"] = "USAGE_UNKNOWN"
            lines = self.results(summary, calls, budget, False)
            self.assertEqual([t for level, t in lines if level == "FAIL"], [])
            skips = [t for level, t in lines if level == "skip"]
            self.assertEqual(skips, [f"Q01 {RUN_IDS[0]}: usage_total {how}; QS chat calls 1, QF chat calls 2 (SETTLED 1, USAGE_UNKNOWN 1)"])

    def test_real_mode_counts_compared_and_skipped_runs(self):
        lines = self.results(*self.unknown("unknown"), False)
        self.assertIn(("info", "runs: 11 compared, 1 skipped; 2 records without run_id"), lines)


class RunsOutsideTheSummary(Cases):
    def test_ledger_run_missing_from_the_summary_fails_in_both_modes(self):
        for e2e in self.both():
            summary, calls, budget = world()
            calls.append(dict(chat_of(calls, RUN_IDS[0])[0], id="old-1", run="run-old"))
            fails = self.fails(summary, calls, budget, e2e)
            self.assertTrue(any("1 ledger chat calls belong to 1 runs not in the summary (run-old)" in t for t in fails), fails)
            self.assertEqual(any("compose-real.sh" in t for t in fails), not e2e)
            if not e2e:
                # 真实模式下只有这一行 FAIL，其余照常核对：同一个 QF 里跑第二遍演示，-Reconcile 的输出就是这样读的
                self.assertOnlyFail(fails, "1 ledger chat calls belong to 1 runs not in the summary (run-old)")

    def test_embeddings_of_other_runs_do_not_count_as_chat(self):
        summary, calls, budget = world()
        calls.append(dict(calls[-1], id="old-emb", run="run-old"))
        self.assertEqual(self.fails(summary, calls, budget, False), [])


class CallsWithoutRun(Cases):
    def test_chat_without_run_fails_in_e2e_and_is_counted_in_real_mode(self):
        summary, calls, budget = world()
        calls.append(dict(chat_of(calls, RUN_IDS[0])[0], id="check-chat", run=None, created=stamp(500)))
        self.assertOnlyFail(self.fails(summary, calls, budget_of(calls), True), "every chat call has X-Run-Id (1 without)")
        lines = self.results(summary, calls, budget, False)
        self.assertEqual([t for level, t in lines if level == "FAIL"], [])
        self.assertIn(("info", "chat calls without X-Run-Id: 1 (SETTLED 1)"), lines)

    def test_embeddings_with_run_must_be_200_settled_and_known_in_e2e(self):
        for change in ({"http": 500}, {"status": "USAGE_UNKNOWN"}, {"total": None}):
            summary, calls, budget = world()
            next(c for c in calls if c["purpose"] == "embeddings" and c["run"]).update(change)
            fails = self.fails(summary, calls, budget, True)
            self.assertTrue(any("embeddings with X-Run-Id (12) are all 200" in t for t in fails), (change, fails))
            real = self.results(summary, calls, budget, False)
            self.assertEqual([t for level, t in real if level == "FAIL"], [])

    def test_embeddings_without_run_must_come_before_the_first_one_with_run(self):
        summary, calls, budget = world()
        late = dict(calls[0], id="late-index", created=stamp(900))
        calls.append(late)
        fails = self.fails(summary, calls, [0, budget[1] + 7, LIMIT], True)
        self.assertTrue(any("embeddings without X-Run-Id (3) all come before" in t for t in fails), fails)
        real = self.results(summary, calls, budget, False)
        self.assertIn(("info", "embeddings without X-Run-Id: 3 (SETTLED 3)"), real)
        self.assertEqual([t for level, t in real if level == "FAIL"], [])

    def test_cached_embeddings_without_run_are_counted_in_real_mode(self):
        # 嵌入缓存命中（CACHED）的不带 X-Run-Id 的嵌入：QS 重建索引的那批、compose-real.sh --check 的第二次嵌入。
        # 真实模式只数出来，不判 FAIL；info 行里按状态数得到 CACHED。
        summary, calls, budget = world()
        for i, index in enumerate([c for c in calls if c["purpose"] == "embeddings" and not c["run"]]):
            calls.append(dict(index, id=f"hit-{i}", status="CACHED", created=stamp(900 + i), reserved=0))
        real = self.results(summary, calls, budget, False)
        self.assertEqual([t for level, t in real if level == "FAIL"], [])
        self.assertIn(("info", "embeddings without X-Run-Id: 4 (CACHED 2, SETTLED 2)"), real)

    def test_the_embedding_order_uses_the_first_embedding_with_run(self):
        summary, calls, budget = world()
        first = min((c for c in calls if c["purpose"] == "embeddings" and c["run"]), key=lambda c: c["created"])
        calls.append(dict(calls[0], id="same-time", created=first["created"]))
        self.assertTrue(any("embeddings without X-Run-Id" in t for t in self.fails(summary, calls, [0, budget[1] + 7, LIMIT], True)))


class SummaryItself(Cases):
    def test_e2e_checks_the_four_summary_facts(self):
        for change, needle in (
            ({"status": "fail"}, "status is pass"),
            ({"model_names": ["other-model"]}, "model_names is"),
            ({"known_gaps": []}, "known_gaps is exactly"),
            ({"known_gaps": ["Q06:rowset_customers_differ", "Q07:x"]}, "known_gaps is exactly"),
        ):
            summary, calls, budget = world()
            summary.update(change)
            self.assertOnlyFail(self.fails(summary, calls, budget, True), needle)

    def test_e2e_checks_the_verdict_counts(self):
        summary, calls, budget = world()
        summary["records"][0]["verdict"] = "fail"
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "13 pass, 1 not_applicable")

    def test_real_mode_only_shows_the_summary(self):
        summary, calls, budget = world()
        summary.update(status="fail", model_names=["qwen-plus"], known_gaps=["Q03:x"])
        summary["records"][0]["verdict"] = "fail"
        lines = self.results(summary, calls, budget, False)
        self.assertEqual([t for level, t in lines if level == "FAIL"], [])
        self.assertTrue(any(level == "info" and "status fail" in t and "'fail': 1" in t and "Q03:x" in t for level, t in lines))


class WholeAccount(Cases):
    def test_every_call_needs_a_client_request_id_in_e2e_only(self):
        summary, calls, budget = world()
        calls[0]["client"] = None
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "has X-Client-Request-Id")
        self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_every_call_must_be_settled_in_e2e_only(self):
        summary, calls, budget = world()
        calls[0]["status"] = "RESERVED"
        fails = self.fails(summary, calls, budget, True)
        self.assertTrue(any("are SETTLED except 0 USAGE_UNKNOWN of the runs above (RESERVED 1, SETTLED" in t for t in fails), fails)
        self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_consumed_and_reserved_must_match_the_ledger_in_e2e_only(self):
        for budget_change in ([0, 1], [5, None]):
            summary, calls, budget = world()
            budget = [budget_change[0], budget[1] if budget_change[1] is None else budget[1] + budget_change[1], LIMIT]
            self.assertOnlyFail(self.fails(summary, calls, budget, True), "account consumed")
            self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_unknown_usage_in_a_call_breaks_the_account_total_in_e2e(self):
        summary, calls, budget = world()
        calls[0]["total"] = None
        self.assertTrue(any("account consumed" in t for t in self.fails(summary, calls, budget, True)))

    def test_e2e_without_any_run_fails_the_known_run_guard_without_the_real_mode_hint(self):
        summary, calls, budget = world()
        summary["records"] = [r for r in summary["records"] if not r.get("run_id")]
        calls = [c for c in calls if not c["run"]]
        fails = self.fails(summary, calls, budget_of(calls), True)
        guard = [t for t in fails if "at least one run with known usage_total" in t]
        self.assertEqual(guard, ["at least one run with known usage_total was compared (0)"], fails)

    def test_at_least_one_embedding_with_run_is_required_in_e2e(self):
        summary, calls, budget = world()
        for call in calls:
            call["run"] = call["run"] if call["purpose"] == "chat" else None
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "at least one embedding with X-Run-Id (0)")
        self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_embeddings_of_runs_missing_from_the_summary_fail_in_e2e_only(self):
        summary, calls, budget = world()
        with_run = [c for c in calls if c["purpose"] == "embeddings" and c["run"]]
        with_run[0]["run"] = "run-ghost-1"
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "1 embeddings with X-Run-Id belong to 1 runs not in the summary (run-ghost-1)")
        with_run[1]["run"] = "run-ghost-2"
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "2 embeddings with X-Run-Id belong to 2 runs not in the summary (run-ghost-1, run-ghost-2)")
        self.assertEqual(self.fails(summary, calls, budget, False), [])


class KnownRunGuard(Cases):
    GUARD = "at least one run with known usage_total was compared (0); nothing was compared, see the skip lines above"

    def unknown_runs(self, keep_known=()):
        summary, calls, budget = world()
        for record in summary["records"]:
            if record.get("run_id") and record["usage_total"]["status"] == "known" and record["run_id"] not in keep_known:
                record["usage_total"] = {"status": "unknown", "prompt_tokens": None, "completion_tokens": None, "total_tokens": None}
        return summary, calls, budget

    def test_real_mode_with_only_unknown_runs_fails_and_says_why(self):
        summary, calls, budget = self.unknown_runs()
        self.assertOnlyFail(self.fails(summary, calls, budget, False), self.GUARD)
        self.assertTrue(any(level == "skip" for level, _ in self.results(summary, calls, budget, False)))

    def test_not_run_does_not_count_as_a_compared_run(self):
        summary, calls, budget = self.unknown_runs()
        self.assertEqual(record_of(summary, NOT_RUN)["usage_total"]["status"], "not_run")
        self.assertIn(("info", "runs: 1 compared, 11 skipped; 2 records without run_id"), self.results(summary, calls, budget, False))
        self.assertOnlyFail(self.fails(summary, calls, budget, False), self.GUARD)

    def test_a_single_known_run_is_enough(self):
        summary, calls, budget = self.unknown_runs(keep_known=(RUN_IDS[0],))
        self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_empty_summary_and_ledger_in_real_mode_fails(self):
        summary = {"status": "pass", "records": []}
        lines = self.results(summary, [], [0, 0, LIMIT], False)
        self.assertEqual([t for level, t in lines if level == "FAIL"], [self.GUARD])
        self.assertIn(("info", "runs: 0 compared, 0 skipped; 0 records without run_id"), lines)

    def test_the_guard_is_the_same_rule_in_both_modes(self):
        summary, calls, budget = world()
        for e2e in self.both():
            self.assertIn(("ok", "at least one run with known usage_total was compared (11)"), self.results(summary, calls, budget, e2e))


def trace_world():
    """world() 加上 trace：摘要里每个 run 一个 trace_id（建 run 之外的记录是 null，Q04b 没有这个键）；日志行一条对一次调用，
    带 run 的用它的 run 的 trace，不带的各自一条新 trace，每次尝试一个新的 parent_span_id。"""
    summary, calls, budget = world()
    traces = {}
    for n, record in enumerate(summary["records"], start=1):
        if record.get("run_id"):
            traces[record["run_id"]] = f"{n:032x}"
            record["trace_id"] = traces[record["run_id"]]
        elif record.get("id") == "Q12":
            record["trace_id"] = None
    rows = []
    for n, call in enumerate(calls, start=1):
        trace = traces[call["run"]] if call["run"] else f"{1000 + n:032x}"
        rows.append({"run_id": call["run"], "trace_id": trace, "parent_span_id": f"{n:016x}", "span_id": f"{5000 + n:016x}",
                     "request_id": call["id"], "status": 200, "error_code": None, "call_status": "SETTLED"})
    return summary, rows


def fresh_trace():
    return f"{0xabc:032x}"


class TraceChecks(unittest.TestCase):
    def fails(self, summary, rows):
        return [text for level, text in cq.trace_checks(summary["records"], rows) if level == "FAIL"]

    def test_clean_world_passes_all_three_checks_and_reports_the_counts(self):
        summary, rows = trace_world()
        results = cq.trace_checks(summary["records"], rows)
        self.assertEqual([level for level, _ in results], ["ok", "ok", "ok"])
        self.assertEqual(
            [text for _, text in results],
            [
                "trace: every call log line has a valid parent_span_id and trace_id (" + str(len(rows)) + " lines, 0 without)",
                "trace: 34 calls of 12 runs carry the trace_id of their run in the summary (0 do not)",
                "trace: 2 calls without X-Run-Id are on 2 traces, none of them a run's (0 are)",
            ],
        )

    def test_a_line_without_parent_span_id_fails_only_the_first_check(self):
        for bad in (None, "", "A" * 16, "0" * 16, "ab" * 7, "ab" * 9, 7):
            summary, rows = trace_world()
            rows[5]["parent_span_id"] = bad
            (fail,) = self.fails(summary, rows)
            self.assertIn("1 without", fail, bad)

    def test_a_line_without_a_valid_trace_id_fails_the_first_check(self):
        for bad in (None, "0" * 32, "F" * 32, "ab" * 15):
            summary, rows = trace_world()
            rows[5]["trace_id"] = bad
            self.assertIn("1 without", self.fails(summary, rows)[0], bad)

    def test_a_run_call_on_another_trace_fails_only_the_second_check(self):
        summary, rows = trace_world()
        next(row for row in rows if row["run_id"] == RUN_IDS[2])["trace_id"] = fresh_trace()
        (fail,) = self.fails(summary, rows)
        self.assertIn("(1 do not)", fail)

    def test_a_run_call_on_another_runs_trace_fails_only_the_second_check(self):
        summary, rows = trace_world()
        other = record_of(summary, RUN_IDS[1])["trace_id"]
        next(row for row in rows if row["run_id"] == RUN_IDS[2])["trace_id"] = other
        (fail,) = self.fails(summary, rows)
        self.assertIn("(1 do not)", fail)

    def test_a_run_whose_summary_has_no_trace_id_fails_the_second_check(self):
        for edit in (lambda r: r.update(trace_id=None), lambda r: r.pop("trace_id")):
            summary, rows = trace_world()
            edit(record_of(summary, RUN_IDS[2]))
            self.assertIn("do not", self.fails(summary, rows)[0])
            self.assertEqual(len(self.fails(summary, rows)), 1)

    def test_a_call_of_a_run_that_is_not_in_the_summary_fails_the_second_check(self):
        summary, rows = trace_world()
        rows.append(dict(rows[-1], run_id="run-9999", trace_id=fresh_trace(), parent_span_id=f"{99:016x}"))
        (fail,) = self.fails(summary, rows)
        self.assertIn("(1 do not)", fail)

    def test_a_call_outside_runs_on_a_runs_trace_fails_only_the_third_check(self):
        summary, rows = trace_world()
        outside = next(row for row in rows if not row["run_id"])
        outside["trace_id"] = record_of(summary, RUN_IDS[0])["trace_id"]
        (fail,) = self.fails(summary, rows)
        self.assertIn("(1 are)", fail)

    def test_calls_outside_runs_may_share_a_trace_with_each_other(self):
        summary, rows = trace_world()
        outside = [row for row in rows if not row["run_id"]]
        outside[1]["trace_id"] = outside[0]["trace_id"]
        self.assertEqual(self.fails(summary, rows), [])

    def test_empty_groups_do_not_pass(self):
        summary, rows = trace_world()
        self.assertEqual(len(self.fails(summary, [])), 3)
        self.assertEqual(len(self.fails(summary, [row for row in rows if row["run_id"]])), 1)  # 没有 run 外的调用
        self.assertEqual(len(self.fails(summary, [row for row in rows if not row["run_id"]])), 1)  # 没有 run 内的调用

    def test_output_is_ascii(self):
        summary, rows = trace_world()
        for _, text in cq.trace_checks(summary["records"], rows):
            text.encode("ascii")


class CallLog(unittest.TestCase):
    def read(self, text, account="e2e"):
        handle = tempfile.NamedTemporaryFile("w", suffix=".log", delete=False, encoding="utf-8", newline="")
        handle.write(text)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return cq.read_call_log(handle.name, account)

    def line(self, **fields):
        return json.dumps({"time": "t", "level": "INFO", "msg": "call", "account": "e2e", **fields}) + "\r\n"

    def test_only_call_lines_of_the_account_are_kept_with_the_log_fields(self):
        text = (
            self.line(run_id="r1", trace_id="t1", parent_span_id="p1", request_id="x", key_id=1)
            + self.line(trace_id="t2")
            + self.line(account="other", run_id="r9", trace_id="t9", parent_span_id="p9")
            + self.line(msg="lookup", trace_id="t3")
            + "not json at all\r\n[1, 2]\r\n\r\n"
            + json.dumps({"msg": "listening", "addr": "x"}) + "\n"
        )
        self.assertEqual(
            self.read(text),
            [dict(dict.fromkeys(cq.LOG_FIELDS), run_id="r1", trace_id="t1", parent_span_id="p1", request_id="x"), dict(dict.fromkeys(cq.LOG_FIELDS), trace_id="t2")],
        )

    def test_an_unreadable_log_cannot_run(self):
        with self.assertRaises(cq.CannotRun):
            cq.read_call_log(os.path.join(tempfile.gettempdir(), "no-such-qf.log"), "e2e")


class Inputs(unittest.TestCase):
    def write(self, content):
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
        handle.write(content if isinstance(content, str) else json.dumps(content))
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def run_main(self, summary_path, *extra, ledger=None):
        argv = ["--project", "p", "--account", "a", *extra, summary_path]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch.object(cq, "read_ledger", return_value=ledger) as read:
            code = cq.main(argv)
        return code, out.getvalue(), err.getvalue(), read

    def bad_summary(self, mutate):
        summary, _, _ = world()
        mutate(summary)
        return self.write(summary)

    def test_inputs_that_cannot_be_reconciled_exit_2_before_reading_the_ledger(self):
        cases = {
            "not json": self.write("{nope"),
            "missing file": os.path.join(tempfile.gettempdir(), "no-such-summary.json"),
            "no records": self.write({"status": "pass"}),
            "records not a list": self.write({"records": {}}),
            "duplicate run_id": self.bad_summary(lambda s: s["records"][1].update(run_id=RUN_IDS[0])),
            "run_id not a string": self.bad_summary(lambda s: s["records"][0].update(run_id=7)),
            "no call count": self.bad_summary(lambda s: s["records"][0].pop("model_call_count")),
            "bad status": self.bad_summary(lambda s: s["records"][0]["usage_total"].update(status="done")),
            "known without numbers": self.bad_summary(lambda s: s["records"][0]["usage_total"].update(total_tokens=None)),
            "record not an object": self.bad_summary(lambda s: s["records"].append(3)),
            "trace_id upper case": self.bad_summary(lambda s: s["records"][0].update(trace_id="A" * 32)),
            "trace_id all zero": self.bad_summary(lambda s: s["records"][0].update(trace_id="0" * 32)),
            "trace_id not a string": self.bad_summary(lambda s: s["records"][0].update(trace_id=5)),
        }
        for name, path in cases.items():
            code, out, err, read = self.run_main(path, ledger=([], [0, 0, LIMIT]))
            self.assertEqual((code, out, name, read.called), (2, "", name, False), err)
            self.assertTrue(err.startswith("error: "), err)

    def test_bad_names_exit_2(self):
        path = self.write(world()[0])
        for argv in (["--project", "p; rm", "--account", "a"], ["--project", "p", "--account", "a'--"]):
            with contextlib.redirect_stderr(io.StringIO()), mock.patch.object(cq, "read_ledger") as read:
                self.assertEqual(cq.main([*argv, path]), 2)
            self.assertFalse(read.called)

    def test_exit_codes_for_ok_and_fail(self):
        summary, calls, budget = world()
        path = self.write(summary)
        code, out, _, _ = self.run_main(path, "--e2e", ledger=(calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (0, "check_qs_runs: 0 failed"))
        calls[0]["client"] = None
        code, out, _, _ = self.run_main(path, "--e2e", ledger=(calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (1, "check_qs_runs: 1 failed"))
        code, out, _, _ = self.run_main(path, ledger=(calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (0, "check_qs_runs: 0 failed"))
        self.assertTrue(all(line.startswith(("ok", "FAIL", "skip", "info", "check_qs_runs")) for line in out.splitlines()))

    def test_summaries_without_trace_ids_still_load(self):
        cq.load_summary(self.write(world()[0]))  # QS 带 traceparent 之前的摘要
        cq.load_summary(self.write(trace_world()[0]))

    def test_qf_log_needs_e2e(self):
        summary, rows = trace_world()
        path = self.write(summary)
        with contextlib.redirect_stderr(io.StringIO()) as err, mock.patch.object(cq, "read_ledger") as read:
            with self.assertRaises(SystemExit) as ctx:
                cq.main(["--project", "p", "--account", "a", "--qf-log", "x.log", path])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--qf-log and --queries need --e2e", err.getvalue())
        self.assertFalse(read.called)

    def test_the_trace_checks_are_appended_and_count_in_the_exit_code(self):
        summary, rows = trace_world()
        _, calls, budget = world()
        path = self.write(summary)
        with mock.patch.object(cq, "read_call_log", return_value=rows) as read:
            code, out, _, _ = self.run_main(path, "--e2e", "--qf-log", "qf.log", ledger=(calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (0, "check_qs_runs: 0 failed"))
        self.assertEqual(read.call_args.args, ("qf.log", "a"))
        self.assertEqual(sum(line.startswith("ok   trace:") for line in out.splitlines()), 3)
        rows[0]["parent_span_id"] = None
        with mock.patch.object(cq, "read_call_log", return_value=rows):
            code, out, _, _ = self.run_main(path, "--e2e", "--qf-log", "qf.log", ledger=(calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (1, "check_qs_runs: 1 failed"))
        code, out, _, _ = self.run_main(path, "--e2e", ledger=(calls, tuple(budget)))  # 不给 --qf-log：不查 trace
        self.assertEqual(sum("trace:" in line for line in out.splitlines()), 0)

    def test_an_unreadable_qf_log_exits_2(self):
        path = self.write(trace_world()[0])
        code, out, err, _ = self.run_main(path, "--e2e", "--qf-log", os.path.join(tempfile.gettempdir(), "no-such-qf.log"), ledger=([], (0, 0, LIMIT)))
        self.assertEqual((code, out), (2, ""))
        self.assertIn("error: cannot read the QF log", err)

    def test_a_ledger_that_cannot_be_read_exits_2(self):
        path = self.write(world()[0])
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err, \
                mock.patch.object(cq, "read_ledger", side_effect=cq.CannotRun("no container")):
            self.assertEqual(cq.main(["--project", "p", "--account", "a", path]), 2)
        self.assertIn("error: no container", err.getvalue())

    def test_utf8_bom_in_the_summary_is_accepted(self):
        path = self.write("﻿" + json.dumps(world()[0]))
        cq.load_summary(path)


class Ledger(unittest.TestCase):
    def test_parse_calls_reads_nulls_and_numbers(self):
        text = "c1\tchat\tSETTLED\t200\t5\t7\t12\tcl-1\trun-1\t2026-10-08 10:00:01.000000\tNULL\t1100\nc2\tembeddings\tUSAGE_UNKNOWN\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\t2026-10-08 10:00:02.000000\treclaimed_in_flight\t64\n\n"
        first, second = cq.parse_calls(text)
        self.assertEqual((first["prompt"], first["completion"], first["total"], first["http"], first["run"]), (5, 7, 12, 200, "run-1"))
        self.assertEqual((second["http"], second["total"], second["client"], second["run"], second["status"]), (None, None, None, None, "USAGE_UNKNOWN"))
        self.assertEqual((first["error"], first["reserved"], second["error"], second["reserved"]), (None, 1100, "reclaimed_in_flight", 64))

    def test_parse_calls_rejects_a_row_with_the_wrong_shape(self):
        with self.assertRaises(cq.CannotRun):
            cq.parse_calls("c1\tchat\n")

    def test_sql_needs_exactly_one_running_qf_mysql_container(self):
        for ids in ("", "a\nb\n"):
            with mock.patch.object(cq, "docker", return_value=ids), self.assertRaises(cq.CannotRun) as ctx:
                cq.sql("quotaforge", "SELECT 1")
            self.assertIn("is QF running", str(ctx.exception))

    def test_sql_filters_by_project_and_service_and_keeps_the_password_off_the_command_line(self):
        with mock.patch.object(cq, "docker", side_effect=["abc123\n", "result\n"]) as docker:
            self.assertEqual(cq.sql("quotaforge", "SELECT 1"), "result\n")
        listing, execute = (call.args for call in docker.call_args_list)
        self.assertIn("label=com.docker.compose.project=quotaforge", listing)
        self.assertIn("label=com.docker.compose.service=qf-mysql", listing)
        self.assertEqual(execute[:2], ("exec", "abc123"))
        self.assertEqual(execute[-1], "SELECT 1")
        self.assertIn("MYSQL_PWD=$(cat /run/secrets/mysql_root_password)", execute[-2])
        self.assertNotIn("-p", " ".join(execute))

    def test_a_missing_account_cannot_be_reconciled(self):
        with mock.patch.object(cq, "sql", return_value=""), self.assertRaises(cq.CannotRun) as ctx:
            cq.read_ledger("quotaforge", "ghost")
        self.assertIn("account ghost not found", str(ctx.exception))

    def test_read_ledger_returns_calls_and_budget(self):
        calls_text = "c1\tchat\tSETTLED\t200\t1\t2\t3\tcl\trun-1\t2026-10-08 10:00:01.000000\tNULL\t1100\n"
        with mock.patch.object(cq, "sql", side_effect=["0\t3\t100\n", calls_text]):
            calls, budget = cq.read_ledger("quotaforge", "queryshield")
        self.assertEqual((len(calls), budget), (1, (0, 3, 100)))

    def test_docker_failure_is_reported_without_a_traceback(self):
        failed = mock.Mock(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon\n")
        with mock.patch.object(cq.subprocess, "run", return_value=failed), self.assertRaises(cq.CannotRun):
            cq.docker("ps")
        with mock.patch.object(cq.subprocess, "run", side_effect=FileNotFoundError), self.assertRaises(cq.CannotRun):
            cq.docker("ps")


UNKNOWN = {"status": "unknown", "prompt_tokens": None, "completion_tokens": None, "total_tokens": None}


def unknown_world(code, first=RUN_IDS[0]):
    """world() 里第一个 run 按 code 失败：QS 报 FAILED、unknown，QF 账本是 UNKNOWN_RUNS 里写的形态，账户的数跟着对上。"""
    summary, calls, _ = world()
    record = record_of(summary, first)
    record.update(error_code=code, usage_total=dict(UNKNOWN))
    chats = chat_of(calls, first)
    if code == "model_quota_exhausted":
        record["model_call_count"] = len(chats) + 1
    else:
        record["model_call_count"] = len(chats)
        http, error = (504, "upstream_timeout") if code == "upstream_http_error" else (None, "reclaimed_in_flight")
        chats[-1].update(status="USAGE_UNKNOWN", http=http, error=error, prompt=None, completion=None, total=None)
    held = sum(c["reserved"] for c in calls if c["status"] == "USAGE_UNKNOWN")
    return summary, calls, [held, sum(c["total"] or 0 for c in calls), LIMIT]


class ExplainedUnknownRuns(Cases):
    CODES = ("model_quota_exhausted", "upstream_http_error", "upstream_request_error")

    def test_each_explained_failure_passes_in_e2e_with_its_line(self):
        for code in self.CODES:
            summary, calls, budget = unknown_world(code)
            lines = self.results(summary, calls, budget, True)
            self.assertEqual([t for level, t in lines if level == "FAIL"], [], code)
            self.assertTrue(any(level == "ok" and f"usage_total is unknown, as expected for {code}" in t for level, t in lines), code)

    def test_real_mode_still_skips_them(self):
        for code in self.CODES:
            lines = self.results(*unknown_world(code), False)
            self.assertEqual(sum(level == "skip" for level, _ in lines), 1, code)

    def test_a_refused_call_that_reached_the_ledger_fails(self):
        summary, calls, budget = unknown_world("model_quota_exhausted")
        record_of(summary, RUN_IDS[0])["model_call_count"] = 2
        self.assertOnlyFail(self.fails(summary, calls, budget, True), "chat calls QF 2 != QS 2 - 1 for model_quota_exhausted")

    def test_the_last_call_must_have_the_shape_of_the_code(self):
        for code, change in (("upstream_http_error", {"http": 502}), ("upstream_http_error", {"error": "upstream_error"}),
                             ("upstream_request_error", {"error": "upstream_timeout", "http": 504})):
            summary, calls, budget = unknown_world(code)
            chat_of(calls, RUN_IDS[0])[-1].update(change)
            fails = self.fails(summary, calls, budget, True)
            self.assertTrue(any("the last chat call is USAGE_UNKNOWN" in t for t in fails), (code, change, fails))

    def test_a_timed_out_call_recorded_as_settled_fails(self):
        summary, calls, budget = unknown_world("upstream_http_error")
        chat_of(calls, RUN_IDS[0])[-1].update(status="SETTLED", prompt=0, completion=0, total=0)
        fails = self.fails(summary, calls, [0, budget[1], LIMIT], True)
        self.assertOnlyFail(fails, "the last chat call is SETTLED 504 upstream_timeout, not USAGE_UNKNOWN 504 upstream_timeout")

    def test_earlier_calls_of_the_run_must_be_settled(self):
        summary, calls, budget = unknown_world("upstream_http_error")
        chat_of(calls, RUN_IDS[0])[0]["status"] = "CANCELLED"
        self.assertTrue(any("the calls that should be SETTLED are not all SETTLED (CANCELLED 1)" in t for t in self.fails(summary, calls, budget, True)))

    def test_every_call_of_a_quota_run_must_be_settled_including_the_last(self):
        summary, calls, budget = world()
        record = record_of(summary, RUN_IDS[0])
        record.update(error_code="model_quota_exhausted", usage_total=dict(UNKNOWN), model_call_count=3)
        chat_of(calls, RUN_IDS[0])[-1].update(status="USAGE_UNKNOWN", prompt=None, completion=None, total=None)
        fails = self.fails(summary, calls, budget_of(calls), True)
        self.assertTrue(any(RUN_IDS[0] in t and "should be SETTLED are not all SETTLED (SETTLED 1, USAGE_UNKNOWN 1)" in t for t in fails), fails)

    def test_a_code_outside_the_table_or_a_known_status_is_not_explained(self):
        for edit in (lambda r: r.update(error_code="upstream_timeout"), lambda r: r.update(error_code=None), lambda r: r.update(usage_total=None)):
            summary, calls, budget = unknown_world("upstream_http_error")
            edit(record_of(summary, RUN_IDS[0]))
            fails = self.fails(summary, calls, budget, True)
            self.assertTrue(any("is not one the end-to-end scenarios explain" in t for t in fails), fails)


class AccountWithUnknownCalls(Cases):
    def test_reserved_must_equal_what_the_unknown_calls_hold(self):
        for delta in (-1, 1):
            summary, calls, budget = unknown_world("upstream_http_error")
            budget[0] += delta
            self.assertOnlyFail(self.fails(summary, calls, budget, True), "account consumed")

    def test_the_old_rule_reserved_zero_fails_when_a_call_is_unknown(self):
        summary, calls, budget = unknown_world("upstream_http_error")
        self.assertGreater(budget[0], 0)
        self.assertOnlyFail(self.fails(summary, calls, [0, budget[1], LIMIT], True), "account consumed")

    def test_an_unexplained_unknown_call_fails(self):
        summary, calls, budget = world()
        calls[0].update(status="USAGE_UNKNOWN", total=None)
        fails = self.fails(summary, calls, [calls[0]["reserved"], budget[1] - 7, LIMIT], True)
        self.assertTrue(any("are SETTLED except 0 USAGE_UNKNOWN" in t for t in fails), fails)

    def test_consumed_plus_reserved_may_not_exceed_the_limit(self):
        summary, calls, budget = unknown_world("upstream_http_error")
        self.assertOnlyFail(self.fails(summary, calls, [budget[0], budget[1], budget[0] + budget[1] - 1], True), "consumed + reserved <= limit")
        self.assertEqual(self.fails(summary, calls, [budget[0], budget[1], budget[0] + budget[1]], True), [])


class CallLogMatchesLedger(unittest.TestCase):
    def world(self):
        summary, rows = trace_world()
        _, calls, _ = world()
        return calls, rows

    def check(self, calls, rows):
        return cq.call_log_check(calls, rows)

    def test_one_line_per_ledger_call_passes(self):
        calls, rows = self.world()
        self.assertEqual(self.check(calls, rows), ("ok", f"call log: {len(rows)} lines with a reservation == {len(calls)} ledger calls (0 reclaimed"
                                                         " after their instance died, without a line; 0 ledger calls without a line, 0 lines not"
                                                         " in the ledger, 0 repeated); 0 lines refused before reservation"))

    def test_a_missing_line_an_extra_ledger_call_a_wrong_id_or_a_repeated_line_fail(self):
        for name, edit in (
            ("missing line", lambda calls, rows: rows.pop(5)),
            ("extra ledger call", lambda calls, rows: calls.append(dict(calls[0], id="extra"))),
            ("wrong id", lambda calls, rows: rows[3].update(request_id="ghost")),
            ("repeated line", lambda calls, rows: rows.append(dict(rows[3]))),
            ("line not in the ledger", lambda calls, rows: rows.append(dict(rows[0], request_id="ghost"))),
        ):
            calls, rows = self.world()
            edit(calls, rows)
            self.assertEqual(self.check(calls, rows)[0], "FAIL", name)

    def test_only_a_reclaimed_call_may_have_no_line(self):
        calls, rows = self.world()
        calls[7]["error"] = "reclaimed_in_flight"
        rows = [row for row in rows if row["request_id"] != calls[7]["id"]]
        level, text = self.check(calls, rows)
        self.assertEqual(level, "ok", text)
        self.assertIn("(1 reclaimed after their instance died", text)
        calls[7]["error"] = "upstream_timeout"
        self.assertEqual(self.check(calls, rows)[0], "FAIL")

    def test_lines_refused_before_reservation_are_counted_apart(self):
        calls, rows = self.world()
        rows.append(dict(rows[0], request_id="refused", call_status=None, status=429, error_code="quota_exhausted"))
        level, text = self.check(calls, rows)
        self.assertEqual(level, "ok", text)
        self.assertTrue(text.endswith("1 lines refused before reservation"), text)

    def test_an_empty_ledger_does_not_pass(self):
        self.assertEqual(self.check([], [])[0], "FAIL")


def attr(key, value):
    return {"key": key, "value": {"intValue" if isinstance(value, int) else "stringValue": str(value) if isinstance(value, int) else value}}


def span(span_id, parent, name, kind=1, error=False, message=None, **attrs):
    status = {"code": 2, **({"message": message} if message else {})} if error else {}
    return {"spanId": span_id, "parentSpanId": parent or "", "name": name, "kind": kind, "status": status,
            "attributes": [attr(k.replace("__", "."), v) for k, v in attrs.items()]}


def otlp(groups):
    """{(服务, 实例): [span]} -> Jaeger 的 /api/v3/traces 返回的 OTLP JSON。"""
    return {"result": {"resourceSpans": [
        {"resource": {"attributes": [attr("service.name", service)] + ([attr("service.instance.id", instance)] if instance else [])},
         "scopeSpans": [{"scope": {"name": "x"}, "spans": spans}]}
        for (service, instance), spans in groups.items()]}}


def jaeger_world():
    """trace_world() 在 Jaeger 里的样子：每个 run 的 trace 有 QS 的 run span 和每次尝试的 http_attempt，QF 的 SERVER span 挂在尝试下面，
    带 reserve、upstream 两个子 span；第一个 run 有一个审批暂停的 tool span（QS 不把暂停标成 ERROR）。返回 (摘要, 日志行, 服务, {trace_id: 摊平的 span})。"""
    summary, rows = trace_world()
    traces = {}
    for n, record in enumerate(r for r in summary["records"] if r.get("run_id")):
        run_span = f"{0xa000 + n:016x}"
        qs = [span(run_span, None, "queryshield.run", queryshield__status="SUCCEEDED")]
        qf = []
        if n == 0:
            qs.append(span(f"{0xb000:016x}", run_span, "queryshield.tool", queryshield__status="waiting_approval", queryshield__error_code="approval_required"))
        for row in (r for r in rows if r["run_id"] == record["run_id"]):
            qs.append(span(row["parent_span_id"], run_span, "queryshield.http_attempt", kind=3, queryshield__status="succeeded"))
            qf.append(span(row["span_id"], row["parent_span_id"], "POST /v1/chat/completions", kind=2))
            qf.append(span("c" + row["span_id"][1:], row["span_id"], "reserve"))
            qf.append(span("d" + row["span_id"][1:], row["span_id"], "upstream", kind=3))
        traces[record["trace_id"]] = cq.flatten(otlp({("queryshield", None): qs, ("quotaforge", "quotaforge"): qf}))
    return summary, rows, ["jaeger-all-in-one", "queryshield", "quotaforge"], traces


def drop(traces, trace_id, keep):
    traces[trace_id] = [s for s in traces[trace_id] if keep(s)]


class JaegerChecks(unittest.TestCase):
    def results(self, summary, rows, services, traces, served_by=None):
        return cq.jaeger_checks(summary["records"], rows, services, traces, served_by)

    def fails(self, *args, **kwargs):
        return [t for level, t in self.results(*args, **kwargs) if level == "FAIL"]

    def first_trace(self, summary):
        return record_of(summary, RUN_IDS[0])["trace_id"]

    def test_clean_world_passes_with_the_counts(self):
        summary, rows, services, traces = jaeger_world()
        results = self.results(summary, rows, services, traces)
        self.assertEqual([level for level, _ in results], ["ok"] * 4 + ["info"], results)
        self.assertIn("12 run traces have all their spans, 47 from queryshield and 102 from quotaforge (0 incomplete)", results[1][1])
        self.assertIn("34 quotaforge SERVER spans each hang under", results[2][1])
        self.assertEqual(results[3], ("ok", "jaeger: queryshield ERROR spans {} are all the failed runs' own codes (0 are not);"
                                            " 0 failed runs each have one with their code (0 do not)"))
        self.assertEqual(results[4], ("info", "jaeger: quotaforge SERVER spans by instance {'quotaforge': 34}"))
        for _, text in results:
            text.encode("ascii")

    def test_a_missing_service_fails_j1(self):
        summary, rows, _, traces = jaeger_world()
        self.assertEqual(len(self.fails(summary, rows, ["quotaforge"], traces)), 1)

    def test_a_trace_without_queryshield_spans_or_without_a_logged_qf_span_fails_j2(self):
        for keep in (lambda s: s["service"] != "queryshield", lambda s: not (s["service"] == "quotaforge" and s["kind"] == 2)):
            summary, rows, services, traces = jaeger_world()
            drop(traces, self.first_trace(summary), keep)
            self.assertTrue(any("(1 incomplete)" in t for t in self.fails(summary, rows, services, traces)))

    def test_a_trace_that_never_reached_jaeger_fails_j2(self):
        summary, rows, services, traces = jaeger_world()
        del traces[self.first_trace(summary)]
        self.assertTrue(any("(1 incomplete)" in t for t in self.fails(summary, rows, services, traces)))

    def test_a_run_without_log_lines_needs_no_qf_span(self):
        summary, rows, services, traces = jaeger_world()
        trace = self.first_trace(summary)
        rows = [row for row in rows if row["trace_id"] != trace]
        drop(traces, trace, lambda s: s["service"] == "queryshield" and s["name"] != "queryshield.http_attempt")
        self.assertEqual(self.fails(summary, rows, services, traces), [])

    def test_qf_spans_on_another_trace_fail_j2_for_a_run_with_log_lines(self):
        summary, rows, services, traces = jaeger_world()
        trace = self.first_trace(summary)
        for row in rows:
            if row["trace_id"] == trace:
                row["trace_id"] = fresh_trace()  # QF 没读 traceparent：日志行和 span 都在它自己起的 trace 上
        drop(traces, trace, lambda s: s["service"] == "queryshield")
        self.assertTrue(any("(1 incomplete)" in t for t in self.fails(summary, rows, services, traces)))

    def test_expected_spans_hold_both_the_qf_span_and_the_qs_attempt_of_each_logged_call(self):
        summary, rows, _, _ = jaeger_world()
        want = cq.expected_spans(summary["records"], rows)
        self.assertEqual(set(want), {r["trace_id"] for r in summary["records"] if r.get("run_id")})
        for trace, spans in want.items():
            logged = [row for row in rows if row["trace_id"] == trace]
            self.assertEqual(spans, {row["span_id"] for row in logged} | {row["parent_span_id"] for row in logged}, trace)

    def test_orphan_qf_child_spans_of_a_killed_instance_do_not_count_as_roots(self):
        summary, rows, services, traces = jaeger_world()
        trace = self.first_trace(summary)
        traces[trace] += cq.flatten(otlp({("quotaforge", "quotaforge"): [span(f"{0xe1:016x}", f"{0xe0:016x}", "authenticate"),
                                                                         span(f"{0xe2:016x}", f"{0xe0:016x}", "reserve")]}))
        self.assertEqual(self.fails(summary, rows, services, traces), [])

    def test_a_server_span_without_a_queryshield_attempt_parent_fails_j3(self):
        for parent in (None, f"{0xa000:016x}", "f" * 16):  # 没有父、父是 run span、父不在这条 trace 里
            summary, rows, services, traces = jaeger_world()
            root = next(s for s in traces[self.first_trace(summary)] if s["kind"] == 2)
            root["parent"] = parent
            fails = self.fails(summary, rows, services, traces)
            self.assertTrue(any("(1 do not)" in t and "SERVER" in t for t in fails), (parent, fails))

    def test_a_parent_named_http_attempt_that_is_not_a_client_span_fails_j3(self):
        summary, rows, services, traces = jaeger_world()
        trace = self.first_trace(summary)
        root = next(s for s in traces[trace] if s["kind"] == 2)
        next(s for s in traces[trace] if s["id"] == root["parent"])["kind"] = 1  # 名字对，kind 不是 CLIENT
        self.assertTrue(any("(1 do not)" in t and "SERVER" in t for t in self.fails(summary, rows, services, traces)))

    def error_span(self, traces, code, trace):
        """trace 这个 run 里一个 ERROR 的 QS span，挂在这个 run 的 run span 下面。"""
        run_span = next(s["id"] for s in traces[trace] if s["name"] == "queryshield.run")
        return cq.flatten(otlp({("queryshield", None): [span(f"{0xb1:016x}", run_span, "queryshield.step.model_decision", error=True,
                                                             **({"queryshield__error_code": code} if code else {}))]}))

    def test_an_error_outside_the_allowed_codes_or_without_a_code_fails_j4(self):
        for code in ("upstream_http_error", "model_quota_exhausted", None):
            summary, rows, services, traces = jaeger_world()
            traces[self.first_trace(summary)] += self.error_span(traces, code, self.first_trace(summary))
            fails = self.fails(summary, rows, services, traces)
            self.assertTrue(len(fails) == 1 and "(1 are not)" in fails[0], (code, fails))

    def test_a_failed_runs_own_code_is_allowed_and_required(self):
        summary, rows, services, traces = jaeger_world()
        record_of(summary, RUN_IDS[1]).update(error_code="upstream_http_error")
        fails = self.fails(summary, rows, services, traces)
        self.assertTrue(len(fails) == 1 and "1 failed runs each have one with their code (1 do not)" in fails[0], fails)
        trace = record_of(summary, RUN_IDS[1])["trace_id"]
        traces[trace] += self.error_span(traces, "upstream_http_error", trace)
        self.assertEqual(self.fails(summary, rows, services, traces), [])

    def test_each_failed_run_needs_its_own_code_on_its_own_trace(self):
        """两个失败的 run 的码对调：每个码都是某个失败的 run 的，但都不在那个 run 自己的 trace 上。"""
        summary, rows, services, traces = jaeger_world()
        one, two = record_of(summary, RUN_IDS[1]), record_of(summary, RUN_IDS[2])
        one.update(error_code="upstream_http_error")
        two.update(error_code="model_quota_exhausted")
        traces[one["trace_id"]] += self.error_span(traces, "model_quota_exhausted", one["trace_id"])
        traces[two["trace_id"]] += self.error_span(traces, "upstream_http_error", two["trace_id"])
        fails = self.fails(summary, rows, services, traces)
        self.assertTrue(len(fails) == 1 and "(2 are not)" in fails[0] and "2 failed runs each have one with their code (2 do not)" in fails[0], fails)

    def test_a_failed_runs_code_on_another_runs_trace_fails_j4(self):
        """失败的 run 自己的 trace 上有它的码；同一个码又出现在一个成功的 run 的 trace 上，也不对。"""
        summary, rows, services, traces = jaeger_world()
        failed = record_of(summary, RUN_IDS[1])
        failed.update(error_code="upstream_http_error")
        traces[failed["trace_id"]] += self.error_span(traces, "upstream_http_error", failed["trace_id"])
        traces[self.first_trace(summary)] += self.error_span(traces, "upstream_http_error", self.first_trace(summary))
        fails = self.fails(summary, rows, services, traces)
        self.assertTrue(len(fails) == 1 and "(1 are not)" in fails[0] and "(0 do not)" in fails[0], fails)

    def test_a_paused_tool_span_marked_error_fails_j4_when_no_run_failed(self):
        """QS 不把暂停的工具调用标成 ERROR（夹具里那个不标）；它以后要是又标了，联调要报出来。"""
        for code in ("approval_required", "clarification_required"):
            summary, rows, services, traces = jaeger_world()
            paused = next(s for s in traces[self.first_trace(summary)] if s["name"] == "queryshield.tool")
            paused.update(error=True)
            paused["attrs"]["queryshield.error_code"] = code
            fails = self.fails(summary, rows, services, traces)
            self.assertTrue(len(fails) == 1 and "(1 are not)" in fails[0], (code, fails))

    def failure_world(self, kind):
        """第一个 run 的第一次调用改成 QF 拒绝（refused）或超时（timeout）在 Jaeger 和日志里的样子。"""
        summary, rows, services, traces = jaeger_world()
        trace = self.first_trace(summary)
        row = next(r for r in rows if r["trace_id"] == trace)
        root = next(s for s in traces[trace] if s["id"] == row["span_id"])
        kids = [s for s in traces[trace] if s["parent"] == root["id"]]
        attempt = next(s for s in traces[trace] if s["id"] == root["parent"])
        if kind == "refused":
            row.update(call_status=None, status=429, error_code="quota_exhausted")
            kids[0]["error"] = True
            drop(traces, trace, lambda s: s is not kids[1])
        else:
            row.update(call_status="USAGE_UNKNOWN", status=504, error_code="upstream_timeout")
            root.update(error=True, message="upstream_timeout")
            kids[1]["error"] = True
            attempt["attrs"].update({"http.response.status_code": "504", "queryshield.provider_error_code": "upstream_timeout"})
        return summary, rows, services, traces, root, kids, attempt

    def test_a_refused_request_has_a_failed_reserve_and_no_upstream_span(self):
        summary, rows, services, traces, *_ = self.failure_world("refused")
        lines = self.results(summary, rows, services, traces)
        self.assertIn(("ok", "jaeger: 1 requests QF refused for quota have a failed reserve span and no upstream span (0 do not)"), lines)
        summary, rows, services, traces, root, kids, _ = self.failure_world("refused")
        traces[self.first_trace(summary)].append(dict(kids[0], id="u" * 16, name="upstream", error=False))
        self.assertTrue(any("refused for quota" in t for t in self.fails(summary, rows, services, traces)))

    def test_a_timed_out_call_is_failed_on_both_sides(self):
        summary, rows, services, traces, *_ = self.failure_world("timeout")
        self.assertEqual(self.fails(summary, rows, services, traces), [])
        self.assertTrue(any("1 calls QF timed out on" in t for _, t in self.results(summary, rows, services, traces)))
        for edit in (lambda root, kids, attempt: root.update(error=False), lambda root, kids, attempt: kids[1].update(error=False),
                     lambda root, kids, attempt: attempt["attrs"].pop("http.response.status_code"),
                     lambda root, kids, attempt: attempt["attrs"].update({"queryshield.provider_error_code": "upstream_error"})):
            summary, rows, services, traces, root, kids, attempt = self.failure_world("timeout")
            edit(root, kids, attempt)
            self.assertTrue(any("timed out on" in t for t in self.fails(summary, rows, services, traces)))

    def test_served_by_needs_every_server_span_of_the_run_from_that_instance(self):
        summary, rows, services, traces = jaeger_world()
        self.assertEqual(self.fails(summary, rows, services, traces, served_by=(RUN_IDS[0], "quotaforge")), [])
        fails = self.fails(summary, rows, services, traces, served_by=(RUN_IDS[0], "quotaforge-b"))
        self.assertTrue(len(fails) == 1 and "(3 SERVER spans, 3 from another instance)" in fails[0], fails)
        self.assertEqual(len(self.fails(summary, rows, services, traces, served_by=("run-ghost", "quotaforge"))), 1)


class FetchTraces(unittest.TestCase):
    def setUp(self):
        self.summary, self.rows, _, _ = jaeger_world()
        self.want = cq.expected_spans(self.summary["records"], self.rows)
        self.trace = next(iter(self.want))
        self.want = {self.trace: self.want[self.trace]}
        qs = [span(i, None, "queryshield.http_attempt", kind=3) for i in self.want[self.trace]]
        self.partial = otlp({("queryshield", None): qs})
        self.whole = otlp({("queryshield", None): qs + [span("r" * 16, None, "queryshield.run", queryshield__status="SUCCEEDED")]})

    def fetch(self, answers, ticks):
        clock = iter(ticks)
        pauses = []
        got = cq.fetch_traces("http://j", self.want, get=lambda url: next(answers), clock=lambda: next(clock), pause=pauses.append, wait=60)
        return got, pauses

    def test_it_waits_until_the_expected_spans_and_a_finished_run_span_are_there(self):
        waiting = otlp({("queryshield", None): [span("r" * 16, None, "queryshield.run", queryshield__status="WAITING_APPROVAL")]})
        got, pauses = self.fetch(iter([{}, self.partial, waiting, self.whole]), [0, 1, 2, 3, 4])
        self.assertEqual(len(pauses), 3)
        self.assertTrue(cq.complete(got[self.trace], self.want[self.trace]))

    def test_it_gives_up_at_the_deadline_and_returns_what_it_has(self):
        got, pauses = self.fetch(iter([self.partial] * 10), [0, 30, 61])
        self.assertEqual(len(pauses), 1)
        self.assertFalse(cq.complete(got[self.trace], self.want[self.trace]))

    def test_jaeger_404_is_an_empty_trace_and_other_failures_cannot_run(self):
        def opener_raising(err):
            return mock.Mock(open=mock.Mock(side_effect=err))
        not_found = cq.urllib.error.HTTPError("http://j", 404, "nf", None, None)
        with mock.patch.object(cq.urllib.request, "build_opener", return_value=opener_raising(not_found)):
            self.assertEqual(cq.jaeger_get("http://j/api/v3/traces/x"), {})
        for err in (cq.urllib.error.HTTPError("http://j", 500, "e", None, None), cq.urllib.error.URLError("refused")):
            with mock.patch.object(cq.urllib.request, "build_opener", return_value=opener_raising(err)), self.assertRaises(cq.CannotRun):
                cq.jaeger_get("http://j/api/v3/services")


class Queries(unittest.TestCase):
    write = Inputs.write

    def queries_file(self, records):
        return self.write("".join(json.dumps(record) + "\n" for record in records))

    def main(self, argv, ledger):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch.object(cq, "read_ledger", return_value=ledger) as read:
            code = cq.main(["--project", "p", "--account", "a", *argv])
        return code, out.getvalue(), err.getvalue(), read

    def test_query_records_alone_are_reconciled_without_the_summary_lines(self):
        summary, calls, budget = world()
        code, out, _, _ = self.main(["--e2e", "--queries", self.queries_file(summary["records"])], (calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (0, "check_qs_runs: 0 failed"))
        self.assertNotIn("QS summary", out)
        self.assertEqual(sum(f" {run}: " in out for run in RUN_IDS), 12)

    def test_summary_and_query_records_are_reconciled_together(self):
        summary, calls, budget = world()
        queries = [r for r in summary["records"] if r.get("run_id") in RUN_IDS[6:]]
        summary["records"] = [r for r in summary["records"] if r not in queries]
        summary["records"][0]["verdict"] = "pass"
        code, out, _, _ = self.main(["--e2e", "--queries", self.queries_file(queries), self.write(summary)], (calls, tuple(budget)))
        self.assertIn("13 pass, 1 not_applicable", out)
        self.assertNotIn("runs not in the summary", out)
        self.assertEqual(sum(f" {run}: " in out for run in RUN_IDS), 12)

    def test_bad_query_records_exit_2_before_reading_the_ledger(self):
        summary, _, _ = world()
        for path in (self.write("{not json\n"), self.queries_file([dict(summary["records"][0], model_call_count="2")]),
                     self.queries_file([dict(summary["records"][0], error_code=5)])):
            code, out, err, read = self.main(["--e2e", "--queries", path], ([], (0, 0, LIMIT)))
            self.assertEqual((code, out, read.called), (2, "", False), err)
        both = self.write(summary)
        code, _, err, read = self.main(["--e2e", "--queries", self.queries_file(summary["records"][:1]), both], ([], (0, 0, LIMIT)))
        self.assertEqual((code, read.called), (2, False))
        self.assertIn("twice", err)

    def test_argument_rules(self):
        for argv, needle in ((["--queries", "q.jsonl"], "need --e2e"), (["--e2e"], "give a demo summary"),
                             (["--e2e", "--jaeger", "http://j", "s.json"], "--jaeger needs --qf-log"),
                             (["--e2e", "--qf-log", "l", "--served-by", "run-1=b", "s.json"], "--served-by needs --jaeger"),
                             (["--e2e", "--qf-log", "l", "--jaeger", "http://j", "--served-by", "run-1", "s.json"], "RUN_ID=INSTANCE")):
            with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit) as ctx:
                cq.main(["--project", "p", "--account", "a", *argv])
            self.assertEqual(ctx.exception.code, 2)
            self.assertIn(needle, err.getvalue())

    def test_two_logs_are_read_and_counted_and_jaeger_is_queried(self):
        summary, rows, services, traces = jaeger_world()
        _, calls, budget = world()
        with mock.patch.object(cq, "read_call_log", side_effect=[rows[:10], rows[10:]]), \
                mock.patch.object(cq, "jaeger_get", return_value={"services": services}), \
                mock.patch.object(cq, "fetch_traces", return_value=traces) as fetch:
            code, out, _, _ = self.main(["--e2e", "--qf-log", "a.log", "--qf-log", "b.log", "--jaeger", "http://j",
                                         "--served-by", f"{RUN_IDS[0]}=quotaforge", self.write(summary)], (calls, tuple(budget)))
        self.assertEqual((code, out.splitlines()[-1]), (0, "check_qs_runs: 0 failed"), out)
        self.assertIn(f"info call log lines of the account per QF instance: 10, {len(rows) - 10}", out)
        self.assertEqual(sum(line.startswith("ok   jaeger:") for line in out.splitlines()), 5)
        self.assertEqual(fetch.call_args.args[0], "http://j")

if __name__ == "__main__":
    unittest.main()
