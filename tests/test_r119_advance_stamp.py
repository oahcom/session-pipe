#!/usr/bin/env python3
"""推进分支必须给旧步骤盖戳 — 否则证据链在推进瞬间丢失。

根因（2026-10-21 生产实测）：wf_lifecycle._advance_production_wf 的推进分支
用 **tick 快照** results 封旧步骤，只写 status="completed"，不写
completed_at/completed_by；而末步分支（:553）早已改成实时读 DB。
角色走 launcher `wf complete` 时行级状态先变 step_done_ready、戳落在顶层，
快照里没有 → 整份覆写后 124 步只剩 status=completed（10-17 cron 巡检链 71 步一批）。
后果：task_generator._subflow_verdict_rejected 只读 exit_messages、liveness 把
无盖戳的 completed 当空闲、回收器看不见非当前步 → 永不补戳。

运行: cd /home/administrator/session-pipeline && python3 -m pytest tests/test_r119_advance_stamp.py -q
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def _wf():
    from pipeflow.engine import WorkflowDef, Step
    return WorkflowDef(
        name="two_step", title="t", description="d",
        steps=[Step(id="s1", title="一", target_role="pm", prompt_template="x", exit_condition={}),
               Step(id="s2", title="二", target_role="product_architect", prompt_template="y", exit_condition={})])


def _drive_advance(db_step_results: dict, snapshot: dict) -> dict:
    """真正调用 _advance_production_wf 的推进分支，返回写库时的 step_results。"""
    from pipeflow.engine import WorkflowEngine, WorkflowDef, Step

    tmp = Path(tempfile.mkdtemp())
    eng = WorkflowEngine(workflows_dir=tmp)
    eng._workflows = {"two_step": _wf()}
    eng._send_to_role = MagicMock(return_value=True)

    written = {}

    class _Lm:
        _conn = MagicMock()
        def close_wf(self, *a, **k): pass
        def _conn_tx(self):
            lm = self
            class _Ctx:
                def __enter__(self):
                    c = MagicMock()
                    # 同一假行被三种 SELECT 复用（行状态 / step_results / tasks），
                    # 缺键会让 _advance_production_wf 在 task["title"] 处炸掉
                    row = {"status": "running", "step_results": json.dumps(db_step_results),
                           "title": "夹具任务", "description": "夹具描述"}
                    c.execute.return_value.fetchone.return_value = row
                    c.execute.return_value.rowcount = 1
                    lm._c = c
                    return c
                def __exit__(self, *a): return False
            return _Ctx()

    eng._lm = _Lm()
    eng._advance_production_wf("wf_stamp", eng._lifecycle, "two_step", "s1", snapshot)

    for call in eng._lm._c.execute.call_args_list:
        sql, args = call[0][0], call[0][1] if len(call[0]) > 1 else ()
        # 参数以元组形式传入：(json_text, wf_id)
        if "SET step_results=?" in sql and isinstance(args, tuple) and isinstance(args[0], str):
            written = json.loads(args[0])
    return written


def test_advance_stamps_completed_step():
    """DB 里没有盖戳的旧步骤，推进时必须补 completed_at / completed_by。"""
    got = _drive_advance({"s1": {"status": "step_done_ready"}},
                         {"s1": {"status": "step_done_ready"}})
    assert got, "推进分支没有写 step_results（测试夹具失效？）"
    assert got["s1"]["status"] == "completed", got["s1"]
    assert got["s1"].get("completed_at"), f"缺 completed_at: {got['s1']}"
    assert got["s1"].get("completed_by") == "workflow_engine", got["s1"]
    assert got["s2"]["status"] == "notified", got["s2"]
    print("✅ 推进时补齐 completed_at/completed_by ✓")


def test_existing_stamp_preserved():
    """已有盖戳不许被覆盖（保留真实完成时刻与完成者）。"""
    got = _drive_advance(
        {"s1": {"status": "step_done_ready", "completed_at": 111.0, "completed_by": "pm"}},
        {"s1": {"status": "notified"}})
    assert got["s1"]["completed_at"] == 111.0, got["s1"]
    assert got["s1"]["completed_by"] == "pm", got["s1"]
    print("✅ 已有盖戳保持原值 ✓")



def test_merge_keeps_snapshot_only_fields():
    """快照里仅存内存的字段必须留住（不被 DB 整步替换掉）。"""
    got = _drive_advance({"s1": {"status": "step_done_ready"}},
                         {"s1": {"status": "notified", "exit_messages": [{"text": "声明"}]}})
    assert got["s1"].get("exit_messages"), f"快照字段被整步替换丢了: {got['s1']}"
    assert got["s1"]["status"] == "completed", got["s1"]
    print("✅ 快照独有字段在逐步合并下保留 ✓")


def test_non_object_db_json_falls_back_without_raising():
    """DB 里 step_results 不是 object（生产实测 640 例非 dict）时不得抛错半推进。"""
    from pipeflow.engine import WorkflowEngine
    tmp = Path(tempfile.mkdtemp())
    eng = WorkflowEngine(workflows_dir=tmp)
    eng._workflows = {"two_step": _wf()}
    eng._send_to_role = MagicMock(return_value=True)

    class _Lm:
        _conn = MagicMock()
        def close_wf(self, *a, **k): pass
        def _conn_tx(self):
            lm = self
            class _Ctx:
                def __enter__(self):
                    c = MagicMock()
                    c.execute.return_value.fetchone.return_value = {
                        "status": "running", "step_results": '"summary"',
                        "title": "夹具任务", "description": "夹具描述"}
                    c.execute.return_value.rowcount = 1
                    lm._c = c
                    return c
                def __exit__(self, *a): return False
            return _Ctx()

    eng._lm = _Lm()
    eng._advance_production_wf("wf_badjson", eng._lifecycle, "two_step", "s1",
                               {"s1": {"status": "step_done_ready"}})
    written = {}
    for call in eng._lm._c.execute.call_args_list:
        sql, args = call[0][0], call[0][1] if len(call[0]) > 1 else ()
        if "SET step_results=?" in sql and isinstance(args, tuple) and isinstance(args[0], str):
            written = json.loads(args[0])
    assert written.get("s1", {}).get("status") == "completed",         f"非 object JSON 应回退快照而不是丢弃: {written}"
    print("✅ 非 object JSON 回退快照不抛错 ✓")


if __name__ == "__main__":
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
