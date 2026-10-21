#!/usr/bin/env python3
"""链路投递契约：N→N+1 必须真的把任务派给下一棒角色，且推进后状态不被回写抹掉。

为什么需要这份契约：推进的两件事发生在不同时刻 —— `current_step_id` 与下一步的
`notified` 在 _advance_production_wf 锁内 commit（wf_lifecycle.py:670/722），
而 _send_to_role 在锁外（:808）。**只断言 current_step 在走**的既有 e2e 用例
（test_workcycle_e2e.py::test_handoff_chain_auto_flows）因此在派单完全丢失时照样绿。
实测缺口：7 天 1001 条实例只有 67 条（6.7%）走完 >=4 步的链。

第二条断言针对同一处的回写：带 subflow_template 的步骤在建子链时把整份 step_results
写回（wf_lifecycle.py:1004），而那份数据来自本 tick 起始快照 —— 刚 commit 的
`notified` 与子链 id 一起被抹掉。生产形态（wf_413e05355b/s4：notified 但无
subflow_id）就是这么来的。
"""
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow.engine import WorkflowEngine

def _wf(dirpath, name, steps):
    (dirpath / f"{name}.json").write_text(json.dumps(
        {"name": name, "title": name, "description": "", "steps": steps},
        ensure_ascii=False))


def _step(step_id, role, **overrides):
    base = {
        "id": step_id, "title": step_id, "target_role": role,
        "prompt_template": f"do {step_id}",
        "exit_condition": {"bus_category": "notice",
                           "text_contains": f"TAG_{step_id}_DONE",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(overrides)
    return base


def _until(eng, pred, tries=200):
    """推进 tick 直到 pred 成立；200x20ms ≈ 4s（pytest alarm 20s）。"""
    for _ in range(tries):
        time.sleep(0.02)
        eng.run_once()
        if pred():
            return True
    return False


def _wait_step(eng, rid, sid, tries=30):
    """轮询到指定步骤接手。"""
    for _ in range(tries):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] == sid:
            return True
    return False


def _tick(eng, times=6):
    for _ in range(times):
        time.sleep(0.05)
        eng.run_once()


def _drive(eng, rid, step_id, tries=20):
    """投递本步 → 写完成消息 → 轮询到推进。"""
    for _ in range(tries):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] != step_id:
            return


@patch("pipeflow.engine._DEV_ROLE_POOL", ())  # 本用例锁“下一棒是谁”，不测 dev 池分流
@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_every_hop_dispatches_to_the_next_role(mock_run):
    """pm → 架构 → 开发：每一跳都必须给下一棒角色真的发一次投递。"""
    d = Path(tempfile.mkdtemp())
    roles = ["pm", "product_architect", "engineer"]
    _wf(d, "chain", [_step(f"s{i+1}", r, type="handoff")
                     for i, r in enumerate(roles)])
    eng = WorkflowEngine(d)

    sent = []

    def _record(role, prompt, wf_id="", step_id="", **kw):
        # 催办/完整性提醒也走 _send_to_role，同一步会多次 send；只把真派单计入契约。
        if "完整性提醒" not in prompt:
            sent.append((role, step_id, wf_id, prompt[:60]))
        return True

    eng._send_to_role = _record
    rid = eng.start("chain", {})

    for i, role in enumerate(roles):
        sid = f"s{i+1}"
        assert _until(eng, lambda: eng.status(rid)["current_step"] == sid), \
            f"{sid} 没被接手: {eng.status(rid)}"
        # 带实例 id：bus 按标题 UNIQUE 去重并保留首次 ts（blackboard_legacy.py:248），
        # 同文本重写复用旧 fid → 出生屏障（step_handler.py:1604）把本用例刚写的
        # 消息挡在 created_after 之前，步骤无法闭环。唯一标题才有新出生时间。
        eng._bb.write("notice", f"TAG_{sid}_DONE {rid}", src=role)
        nxt = f"s{i+2}"
        assert _until(eng, lambda: eng.status(rid)["current_step"] == nxt
                      or eng.status(rid)["status"] == "completed"), \
            f"{sid} 完成后没有进入下一步: {eng.status(rid)}"

    assert eng.status(rid)["status"] == "completed", eng.status(rid)
    # s1 由 start 投递一次；s2/s3 由推进投递一次。少一次 = 那一跳没人干活。
    for i, role in enumerate(roles[1:], start=2):
        hits = [x for x in sent if (x[0], x[1]) == (role, f"s{i}")]
        assert len(hits) == 1, f"s{i} 应恰好派给 {role} 一次，实投={hits}"
        # 首跳也必须带 wf_id：不带则投递前 CLI 忙预检整段跳过（tick 仍可能被拖停）。
        assert hits[0][2] == rid, f"s{i} 投递缺 wf_id: {hits[0]}"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_subflow_takeover_keeps_notified_and_subflow_id(mock_run):
    """建子链后父步必须同时保有 notified（已派）与 subflow_id（子链指针）。"""
    d = Path(tempfile.mkdtemp())
    _wf(d, "child", [_step("s1", "product_architect")])
    # s1 用 handoff：它的完成走引擎推进分支（wf_lifecycle），那条分支本 tick 里
    # 先把 s2 落成 notified 再建子链 —— 正是被整份回写抹掉的那一处。
    _wf(d, "parent", [
        _step("s1", "pm", type="handoff"),
        _step("s2", "product_architect", subflow_template="child"),
    ])
    eng = WorkflowEngine(d)

    def _ok(role, prompt, wf_id="", step_id="", **kw):
        return True

    eng._send_to_role = _ok
    rid = eng.start("parent", {})
    lm = eng._lifecycle
    parent = eng._workflows["parent"]

    # 复刻生产时序：推进分支本 tick 已把 s2 落成 notified（含 poll_since），
    # 随后才建子链，而调用方手里攥着的是**本 tick 起始快照**（只有 s1）。
    notified = {"status": "notified", "notified_at": 1.0, "poll_since": 1.0,
                "bus_anchor": 1.0}
    lm.execute(
        "UPDATE workflow_instances SET current_step_id='s2', step_results=? "
        "WHERE instance_id=?",
        (json.dumps({"s1": {"status": "completed", "completed_at": 2.0,
                            "completed_by": "workflow_engine"},
                     "s2": notified}, ensure_ascii=False), rid))
    stale_snapshot = {"s1": {"status": "completed"}}   # 旧代码会整份写回这份

    assert eng._subflow_takeover(lm, rid, "parent", parent.steps[1], 0,
                                 stale_snapshot, "s1") is True, "子链没建出来"

    row = lm.query("SELECT step_results FROM workflow_instances WHERE instance_id=?",
                   (rid,))[0]
    sr = json.loads(row["step_results"])
    assert sr["s2"]["status"] == "notified", f"父步 notified 被回写抹掉: {sr['s2']}"
    assert sr["s2"].get("subflow_id", "").startswith("wf_sub_"), \
        f"父步丢了子链指针: {sr['s2']}"
    assert sr["s1"].get("completed_at") == 2.0, f"上一步完成戳被抹掉: {sr['s1']}"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
