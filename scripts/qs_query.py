"""在 QS 的 app 容器里提一个问题（同步的 POST /queries），输出一行 JSON：这个 run 的编号和数字。

  docker compose ... exec -T app python - < scripts/qs_query.py

联调脚本（scripts/compose-qs-e2e.sh）用它造额度耗尽、上游超时和实例接手这几个场景：QS 的演示脚本遇到
5xx 不停，也不能只跑其中几题。问题固定是“退款后净额是怎么算的？”：fake-upstream 模式下这个 run
有 2 次聊天、1 次带 X-Run-Id 的查询嵌入，出错时一定出在第 1 次聊天。经 stdin 交给容器里的 python，
避开命令行里中文参数的编码问题。

Token 从容器的环境变量读，不经宿主机。输出只有编号、状态和数字，没有回答正文和 Token；
字段和 QS 演示摘要的记录同名，scripts/check_qs_runs.py 用 --queries 读。
"""
import json
import os
import urllib.error
import urllib.request

QUESTION = "退款后净额是怎么算的？"
FIELDS = ("run_id", "trace_id", "status", "error_code", "model_call_count", "usage_total")


def main():
    request = urllib.request.Request(
        "http://127.0.0.1:8000/queries",
        data=json.dumps({"question": QUESTION}).encode("utf-8"),
        headers={"Authorization": "Bearer " + os.environ["QUERYSHIELD_TOKEN_A_REQUESTER"], "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as err:
        status, body = err.code, err.read()
    run = json.loads(body)
    record = {"id": "Q09", "http_status": status, **{key: run.get(key) for key in FIELDS}}
    if record["error_code"] is None and isinstance(run.get("error"), dict):
        record["error_code"] = run["error"].get("code")
    print(json.dumps(record))


main()
