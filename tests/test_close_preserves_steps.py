"""实例终结不得抹掉步骤状态 —— 父链回流通道依赖它。

现场（R110，2026-10-21）：
  workflow/client.py 的 fail()/cancel() 用 json.dumps({"reason": reason}) 整体覆写
  step_results，全部步骤状态连同 subflow_id 一起消失。生产库 49 个实例如此。
  task_generator._scan_tasks 的父链回流扫描要求 step_results[step] 是 dict 且有
  subflow_id，顶层 `reason` 是 str 被 continue 跳过 → 该父链的回流通道静默关闭
  （实测 wf_579f636f62 名下 7 条 completed 子链，无一条回流）。
  manager.cancel_step（:600-614）早已是合并语义并写明了同样的后果。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_close_preserves_steps.py -q
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from workflow.client import WorkflowClient  # noqa: E402


def _client_with_running_step():
    """一个在跑且挂着子链的实例，模拟生产 wf_579f636f62 的形态。"""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    c = WorkflowClient("engineer", db_path=tmp.name)
    # 表由 create_connection 的 schema 建好，这里只塞一行
    sr = {"s1": {"status": "completed", "subflow_id": "wf_sub_aaa", "completed_at": 1.0},
          "s2": {"status": "running", "subflow_id": "wf_sub_bbb"}}
    c._conn.execute(
        "INSERT INTO workflow_instances(instance_id, task_id, status, step_results, "
        "template_id, current_step_id, assigner, assignee, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        ("wf_x", "task_x", "running", json.dumps(sr), "WL-full_dev_pipeline",
         "s2", "engineer", "engineer", 1.0))
    c._conn.commit()
    return c, tmp.name


def test_cancel_preserves_step_results():
    """cancel 后步骤与 subflow_id 必须还在 —— 否则父链永远等不到子链回流。"""
    c, path = _client_with_running_step()
    try:
        c.cancel("wf_x", reason="人工终止")
        sr = json.loads(c.get("wf_x")["step_results"])
        assert "s1" in sr and sr["s1"].get("subflow_id") == "wf_sub_aaa", \
            f"cancel 抹掉了历史步骤: {list(sr)}"
        assert sr["s2"].get("subflow_id") == "wf_sub_bbb", "cancel 抹掉了当前步 subflow_id"
        assert sr["_close_reason"]["reason"] == "人工终止", "终结原因未记录"
    finally:
        c.close(); Path(path).unlink(missing_ok=True)


def test_fail_preserves_step_results():
    """fail 同样必须合并，不能整体覆写。"""
    c, path = _client_with_running_step()
    try:
        c.fail("wf_x", "引擎判定失败")
        sr = json.loads(c.get("wf_x")["step_results"])
        assert sr.get("s1", {}).get("status") == "completed", f"fail 抹掉了步骤: {list(sr)}"
        row = c.get("wf_x")
        assert row["status"] == "failed"
        assert row["completed_at"], "终结时间未写"
        assert sr["_close_reason"]["status"] == "failed"
    finally:
        c.close(); Path(path).unlink(missing_ok=True)


def test_close_survives_corrupt_step_results():
    """旧值已损坏（非 JSON / 非 dict）时不得抛穿 —— 终结比保留坏数据重要。"""
    for bad in ("not json", "[1,2,3]", '{"reason": "旧格式"}'):
        c, path = _client_with_running_step()
        try:
            c._conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (bad, "wf_x"))
            c._conn.commit()
            c.cancel("wf_x", reason="清理")
            sr = json.loads(c.get("wf_x")["step_results"])
            assert isinstance(sr, dict) and "_close_reason" in sr, \
                f"坏值 {bad!r} 下终结失败: {sr}"
            if bad.startswith('{"reason"'):
                assert "reason" in sr, "旧格式的 reason 不该被丢弃"
        finally:
            c.close(); Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    for name, fn in [("cancel 保留步骤", test_cancel_preserves_step_results),
                     ("fail 保留步骤", test_fail_preserves_step_results),
                     ("坏值不抛穿", test_close_survives_corrupt_step_results)]:
        try:
            fn(); print(f"  OK  {name}")
        except AssertionError as ex:
            print(f"  X   {name}: {ex}"); raise
    print("全部通过")
