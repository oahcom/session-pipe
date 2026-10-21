#!/usr/bin/env python3
"""推进不能被 tick 前置阶段堵住 —— "完成一步 → 通知下一步"必须最先发生。

生产证据（2026-10-22 隔离探针，DB 副本 + faulthandler 打栈）：
- 单 tick 399s，绝大部分是 subprocess.run 等 tmux/ccs 返回（_scan_tasks 逐角色
  liveness._tmux_has_session 5s 超时 × N 角色，_send_to_role 60s 超时）；
- 推进排在这些阶段后面，服务 TimeoutStopSec=90 的重启每次把整轮打断
  → 实测 4 个实例卡在 step_done_ready 1.5~2.4h，一步都推不动。
本用例锁住两条：① 预推进发生在 _scan_tasks 之前；② 预算为 0 时确实不推进
（防止把"预算"写成恒真/恒假）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_tick_budget.py -q
"""
import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow import engine as engine_mod  # noqa: E402
from pipeflow.engine import WorkflowEngine  # noqa: E402


def _env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _wf(d, name, ids):
    steps = [{"id": i, "title": i, "target_role": "test_role",
              "prompt_template": f"do {i}",
              "exit_condition": {"bus_category": "notice",
                                 "text_contains": f"TICKBUDGET_{i.upper()}_DONE",
                                 "timeout_minutes": 30},
              "max_retries": 1, "type": "single"} for i in ids]
    (d / f"{name}.json").write_text(json.dumps(
        {"name": name, "title": name, "description": "", "steps": steps},
        ensure_ascii=False))


def _fast(eng):
    """打桩与本用例无关的慢阶段（真实实现里是 tmux/ccs 子进程扫描）。"""
    for name in ("_ensure_guard_roles", "_kick_stalled_roles", "_recover_delivery_blocked",
                 "_cleanup_stale_workflows", "_cleanup_orphan_tasks", "_scan_tasks"):
        setattr(eng, name, lambda *a, **k: None)
    return eng


def _ready(eng, rid, step="s1"):
    """直接把 s1 置为 step_done_ready —— 角色已交付、引擎还没消费的现场。"""
    now = time.time()
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
        (json.dumps({step: {"status": "step_done_ready", "ts": now,
                            "notified_at": now}}, ensure_ascii=False), rid))


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_advance_happens_before_scan_tasks(_sp):
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rid = eng.start("tbflow", {})
    _ready(eng, rid)

    seen = []
    def _probe_scan(*a, **k):
        row = eng._lifecycle.query(
            "SELECT current_step_id FROM workflow_instances WHERE instance_id=?", (rid,))[0]
        seen.append(row["current_step_id"])
    eng._scan_tasks = _probe_scan          # 覆盖 _fast 的桩

    eng.run_once()
    assert seen, "_scan_tasks 没被调用"
    assert seen[0] == "s2", f"_scan_tasks 开始时应已推进到 s2，实际 {seen[0]}"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_zero_budget_stops_advance(_sp):
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rid = eng.start("tbflow", {})
    _ready(eng, rid)

    _old = engine_mod._TICK_BUDGET_SEC
    engine_mod._TICK_BUDGET_SEC = 0       # 预算恒超 → 不推进
    try:
        eng.run_once()
    finally:
        engine_mod._TICK_BUDGET_SEC = _old
    assert eng.status(rid)["current_step"] == "s1", "预算为 0 时不得推进"


if __name__ == "__main__":
    test_advance_happens_before_scan_tasks(None)
    test_zero_budget_stops_advance(None)
    print("ok")


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_prepass_sends_step_done_reminder(_sp):
    """预推进也必须发"每步完整性提醒"（codex 复审 2026-10-22 指出的丢失点）。

    预推进先跑完的实例，主循环再也看不到 step_done_ready → 该分支的
    send_bypass_reminder 若不搬进预推进，提醒整段消失（用户要求每个步骤都提醒）。
    """
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rid = eng.start("tbflow", {})
    _ready(eng, rid)

    import pipeflow.integrity as integ
    seen = []
    real = integ.send_bypass_reminder

    def _spy(run, step, status, **kw):
        seen.append((getattr(run, "id", ""), getattr(step, "id", ""), status))
        return real(run, step, status, **kw)

    with patch.object(integ, "send_bypass_reminder", _spy):
        eng.run_once()

    assert (rid, "s1", "step_done_ready") in seen, seen
    assert eng.status(rid)["current_step"] == "s2", "提醒之外仍要推进"
