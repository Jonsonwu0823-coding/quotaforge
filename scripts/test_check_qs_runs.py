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
            "client": f"client-{clock[0]:04d}", "run": run, "created": stamp(clock[0]),
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
    return summary, calls, [0, sum(c["total"] for c in calls)]


def budget_of(calls):
    return [0, sum(c["total"] or 0 for c in calls)]


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
            self.assertEqual(any("compose-real.ps1" in t for t in fails), not e2e)

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
        fails = self.fails(summary, calls, [0, budget[1] + 7], True)
        self.assertTrue(any("embeddings without X-Run-Id (3) all come before" in t for t in fails), fails)
        real = self.results(summary, calls, budget, False)
        self.assertIn(("info", "embeddings without X-Run-Id: 3 (SETTLED 3)"), real)
        self.assertEqual([t for level, t in real if level == "FAIL"], [])

    def test_the_embedding_order_uses_the_first_embedding_with_run(self):
        summary, calls, budget = world()
        first = min((c for c in calls if c["purpose"] == "embeddings" and c["run"]), key=lambda c: c["created"])
        calls.append(dict(calls[0], id="same-time", created=first["created"]))
        self.assertTrue(any("embeddings without X-Run-Id" in t for t in self.fails(summary, calls, [0, budget[1] + 7], True)))


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
        self.assertTrue(any("are SETTLED (RESERVED 1, SETTLED" in t for t in fails), fails)
        self.assertEqual(self.fails(summary, calls, budget, False), [])

    def test_consumed_and_reserved_must_match_the_ledger_in_e2e_only(self):
        for budget_change in ([0, 1], [5, None]):
            summary, calls, budget = world()
            budget = [budget_change[0], budget[1] if budget_change[1] is None else budget[1] + budget_change[1]]
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
        lines = self.results(summary, [], [0, 0], False)
        self.assertEqual([t for level, t in lines if level == "FAIL"], [self.GUARD])
        self.assertIn(("info", "runs: 0 compared, 0 skipped; 0 records without run_id"), lines)

    def test_the_guard_is_the_same_rule_in_both_modes(self):
        summary, calls, budget = world()
        for e2e in self.both():
            self.assertIn(("ok", "at least one run with known usage_total was compared (11)"), self.results(summary, calls, budget, e2e))


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
        }
        for name, path in cases.items():
            code, out, err, read = self.run_main(path, ledger=([], [0, 0]))
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
        text = "c1\tchat\tSETTLED\t200\t5\t7\t12\tcl-1\trun-1\t2026-10-08 10:00:01.000000\nc2\tembeddings\tUSAGE_UNKNOWN\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\t2026-10-08 10:00:02.000000\n\n"
        first, second = cq.parse_calls(text)
        self.assertEqual((first["prompt"], first["completion"], first["total"], first["http"], first["run"]), (5, 7, 12, 200, "run-1"))
        self.assertEqual((second["http"], second["total"], second["client"], second["run"], second["status"]), (None, None, None, None, "USAGE_UNKNOWN"))

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
        calls_text = "c1\tchat\tSETTLED\t200\t1\t2\t3\tcl\trun-1\t2026-10-08 10:00:01.000000\n"
        with mock.patch.object(cq, "sql", side_effect=["0\t3\n", calls_text]):
            calls, budget = cq.read_ledger("quotaforge", "queryshield")
        self.assertEqual((len(calls), budget), (1, (0, 3)))

    def test_docker_failure_is_reported_without_a_traceback(self):
        failed = mock.Mock(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon\n")
        with mock.patch.object(cq.subprocess, "run", return_value=failed), self.assertRaises(cq.CannotRun):
            cq.docker("ps")
        with mock.patch.object(cq.subprocess, "run", side_effect=FileNotFoundError), self.assertRaises(cq.CannotRun):
            cq.docker("ps")


if __name__ == "__main__":
    unittest.main()
