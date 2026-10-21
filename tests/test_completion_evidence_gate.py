#!/usr/bin/env python3
"""T1–T7 回归：完成证据与投递状态解耦（DESIGN-wf_sub_d468dfaec353）。

根因（bus #428132 复核，2026-10-21 实测）：running 17 中 16 步的 status 不是
notified/running → run_once 走投递重试分支、_tick 门（engine.py:1311）永不进入
→ **角色早已写完的完成消息不被消费**，只能等排队预算耗尽被 close_wf 回收。
A2 的 5 例实证中，完成消息都写在回收时刻之前。

本用例集锁死四件事：
  1. status='' 但 bus 已有匹配完成消息 → 一次 run_once 即进入 _tick 并推进
  2. status='' 且无匹配消息 → 行为与现状一致（照常投递，不伪造推进）
  3. 队列回收前也能被证据拦下（不再"已完成却被回收"）
  4. 同一份证据重复 tick 不重复推进（exit_messages 不重复、步骤只前进一次）

运行: cd ~/session-pipeline && python3 -m pytest tests/test_completion_evidence_gate.py -q
"""
import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine

def _tag() -> str:
    """每用例唯一：blackboard 对同 (cat,title) UPSERT 且 _birth 取 MIN(log.ts)，
    跨用例复用同一标题会让新实例的 created_after 屏障挡住「本用例刚写的」消息。"""
    import uuid
    return f"ev{uuid.uuid4().hex[:12]}"


def _env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _write_wf(d, name, steps):
    (d / f"{name}.json").write_text(json.dumps(
        {"name": name, "title": name, "description": "", "steps": steps},
        ensure_ascii=False))


def _step(sid, text, tag=None, **kw):
    base = {
        "id": sid, "title": sid, "target_role": "test_role",
        "prompt_template": f"do {sid}",
        "exit_condition": {"bus_category": "notice",
                           "text_contains": f"{tag or _tag()}_{text}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(kw)
    return base


def _clear_status(eng, rid, sid):
    """模拟"投递没成功/角色忙"：status 留空但保留其余账本（真实生产形态）。"""
    lm = eng._lifecycle
    row = lm._get_wf_unsafe(rid)
    results = json.loads(row["step_results"] or "{}")
    results[sid] = {**results.get(sid, {}), "status": ""}
    lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
               (json.dumps(results, ensure_ascii=False), rid))


def _sd(eng, rid, sid):
    return json.loads(eng._lifecycle._get_wf_unsafe(rid)["step_results"])[sid]


_PATCH = patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))


@_PATCH
def test_empty_status_with_evidence_advances(mock_run):
    """正常路径：status 空 + 完成消息已到 → 推进到下一步，不重投。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "ev2", [_step("s1", "S1", tag), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("ev2", {})
    eng.run_once()                                    # 首投 → notified
    _clear_status(eng, rid, "s1")                     # 投递状态丢失/角色忙
    eng._bb.write("notice", f"{tag}_S1 done", src="test_role")
    for _ in range(6):
        eng.run_once()
        if eng.status(rid)["current_step"] != "s1":
            break
    st = eng.status(rid)
    assert st["current_step"] == "s2", f"完成证据未被消费: {st}"
    assert _sd(eng, rid, "s1")["status"] in ("step_done_ready", "completed"), st


@_PATCH
def test_empty_status_without_evidence_delivers(mock_run):
    """边界：无匹配消息 → 走原投递路径（盖 notified），不得伪造推进。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "ev1", [_step("s1", "S1", tag), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("ev1", {})
    _clear_status(eng, rid, "s1") if eng.status(rid)["results"].get("s1") else None
    eng.run_once()
    st = eng.status(rid)
    assert st["current_step"] == "s1", f"无证据却推进了: {st}"
    assert _sd(eng, rid, "s1")["status"] == "notified", st
    assert mock_run.called, "投递未被调用"


@_PATCH
def test_require_wf_rejects_foreign_id(mock_run):
    """异常路径：require_wf 步骤不吃别的实例的完成消息（防跨链串味）。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "evwf", [_step("s1", "S1", tag, exit_condition={
        "bus_category": "notice", "text_contains": f"{tag}_S1",
        "timeout_minutes": 30, "require_wf": True}), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("evwf", {})
    eng.run_once()
    _clear_status(eng, rid, "s1")
    eng._bb.write("notice", f"{tag}_S1 wf_other_instance done", src="test_role")
    eng.run_once()
    assert eng.status(rid)["current_step"] == "s1", "外来实例的消息闭合了本步"
    eng._bb.write("notice", f"{tag}_S1 {rid} done", src="test_role")
    for _ in range(6):
        eng.run_once()
        if eng.status(rid)["current_step"] != "s1":
            break
    assert eng.status(rid)["current_step"] == "s2", "本实例证据未被消费"


@_PATCH
def test_dual_node_not_preempted(mock_run):
    """边界：dual_node 步骤不在投递分支抢跑（node1/node2 两段式由 _tick 管）。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "evdn", [_step("s1", "S1", tag, dual_node=True), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("evdn", {})
    eng.run_once()
    _clear_status(eng, rid, "s1")
    eng._bb.write("notice", f"{tag}_S1 done", src="test_role")
    eng.run_once()
    assert eng.status(rid)["current_step"] == "s1", "dual_node 被投递分支抢跑"
    assert not _sd(eng, rid, "s1").get("exit_messages"), "dual_node 不应被预置 exit_messages"


@_PATCH
def test_evidence_idempotent_across_ticks(mock_run):
    """幂等（AC3）：同一份证据多轮 tick，步骤只前进一次、exit_messages 不重复。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "evidem", [_step("s1", "S1", tag), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("evidem", {})
    eng.run_once()
    _clear_status(eng, rid, "s1")
    eng._bb.write("notice", f"{tag}_S1 done", src="test_role")
    for _ in range(5):
        eng.run_once()
    s1 = _sd(eng, rid, "s1")
    assert eng.status(rid)["current_step"] == "s2", eng.status(rid)
    ids = [m["id"] for m in s1.get("exit_messages") or []]
    assert ids, f"exit_messages 丢失（task_evidence 依赖它）: {s1}"
    assert len(ids) == len(set(ids)), f"exit_messages 重复: {ids}"


@_PATCH
def test_evidence_blocks_queue_reclaim(mock_run):
    """AC2：排队预算已耗尽但完成消息已到 → 不得 close_wf(cancelled)。"""
    tag = _tag()
    d = _env()
    _write_wf(d, "evcap", [_step("s1", "S1", tag), _step("s2", "S2", tag)])
    eng = WorkflowEngine(d)
    rid = eng.start("evcap", {})
    # 构造"角色长期忙"的现场：status 空 + 排队起点远早于预算
    eng._bb.write("notice", f"{tag}_S1 done", src="test_role")
    lm = eng._lifecycle
    row = lm._get_wf_unsafe(rid)
    results = json.loads(row["step_results"] or "{}")
    results["s1"] = {"status": "", "queued": 30,
                     "queued_since": time.time() - 30 * 3600}
    # created_at 必须回拨：回收器对 <5min 实例整段放过（wf_lifecycle.py:208），
    # 新建实例的 AC2 用例永远是绿——它测不到回收路径，正好掩盖了证据门排在
    # 回收之后这个 blocker（三方审查 2026-10-21 实测）。
    lm.execute("UPDATE workflow_instances SET step_results=?, created_at=? WHERE instance_id=?",
               (json.dumps(results, ensure_ascii=False), time.time() - 2 * 3600, rid))
    with patch("pipeflow.wf_lifecycle._role_producing_recently", return_value=False):
        eng.run_once()
    st = eng.status(rid)
    assert st["status"] != "cancelled", f"已完成却被回收: {st}"
    assert st["current_step"] == "s2", f"证据未消费: {st}"


if __name__ == "__main__":
    raise SystemExit(__import__("pytest").main([__file__, "-q"]))
