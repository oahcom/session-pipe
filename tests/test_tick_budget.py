#!/usr/bin/env python3
"""推进不能被 tick 前置阶段堵住 —— "完成一步 → 通知下一步"必须最先发生。

生产证据（2026-10-22 隔离探针，DB 副本 + faulthandler 打栈）：
- 单 tick 399s，绝大部分是 subprocess.run 等 tmux/ccs 返回（_scan_tasks 逐角色
  liveness._tmux_has_session 5s 超时 × N 角色，_send_to_role 60s 超时）；
- 推进排在这些阶段后面，服务 TimeoutStopSec=15 的重启每次把整轮打断
  → 实测 4 个实例卡在 step_done_ready 1.5~2.4h，一步都推不动。
本用例锁住两条：① 预推进发生在 _scan_tasks 之前；② 预算恒超时恰好推进一条
（防止把"预算"写成恒真/恒假，也防退回到"一条都不推进"的整体饥饿）。

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

import pytest  # noqa: E402

from pipeflow import engine as engine_mod  # noqa: E402
from pipeflow.engine import WorkflowEngine  # noqa: E402


class _Alive(dict):
    """tmux 探针已改为一次 list-panes -a 快照；本用例只关心推进顺序，不碰真 tmux。"""

    def __contains__(self, k):
        return True

    def get(self, k, d=None):
        return ("1", "0")

    def __getitem__(self, k):
        return ("1", "0")


@pytest.fixture(autouse=True)
def _no_tmux():
    with patch("liveness.tmux_snapshot", lambda max_age=2.0: _Alive()), \
            patch("pipeflow.role_lifecycle._tmux_snapshot", lambda max_age=2.0: _Alive()):
        yield


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
def test_zero_budget_advances_exactly_one(_sp):
    """预算恒超 → 首行保底推进，且不得一次推完。

    旧契约是"预算为 0 → 一条都不推进"，而那正是整体饥饿的成因：预算自 run_once
    起点算，前置阶段（_load_workflows / 清理 / _scan_tasks）吃满 60s 后主循环连
    第一条都进不去，断点原地不动，轮转结构上救不了（三方审查 2026-10-22 A-1）。
    新契约：首行保底（一定推进），其余留到下轮（靠轮转保证不饿死）。
    """
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rids = [eng.start("tbflow", {}) for _ in range(2)]
    for rid in rids:
        _ready(eng, rid)
        # 置回 pending：预推进只查 running/step_done_ready，pending 是主循环独有集合，
        # 这样推进只可能来自主循环，守卫被拆掉时这里必然 0 条。
        eng._lifecycle.execute(
            "UPDATE workflow_instances SET status='pending' WHERE instance_id=?", (rid,))

    _old = engine_mod._TICK_BUDGET_SEC
    engine_mod._TICK_BUDGET_SEC = -1      # 负预算 = 恒超（不依赖时钟分辨率）
    try:
        eng.run_once()
    finally:
        engine_mod._TICK_BUDGET_SEC = _old

    steps = [eng.status(r)["current_step"] for r in rids]
    advanced = sum(1 for s in steps if s == "s2")
    assert advanced == 1, f"预算恒超时主循环必须保底推进恰好一条: {steps}"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_zero_budget_also_advances_prepass(_sp):
    """预推进层同样要首行保底（否则预算一紧它就整层空转，白拿 60s 预算）。

    观测点：主循环取行那一刻（SQL 含 effective_roles）该实例是否已在 s2。
    """
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rid = eng.start("tbflow", {})
    _ready(eng, rid)

    at_main = []
    real_query = eng._lifecycle.query
    def _spy(sql, *a, **k):
        if "effective_roles" in sql:      # 主循环的取行语句
            at_main.append(eng.status(rid)["current_step"])
        return real_query(sql, *a, **k)
    eng._lifecycle.query = _spy

    _old = engine_mod._TICK_BUDGET_SEC
    engine_mod._TICK_BUDGET_SEC = -1
    try:
        eng.run_once()
    finally:
        engine_mod._TICK_BUDGET_SEC = _old

    assert at_main and set(at_main) == {"s2"}, f"预算恒超时预推进必须先保底推进一条: {at_main}"


if __name__ == "__main__":
    # @patch 装饰的用例由装饰器注入 mock，调用时不要再传参
    test_advance_happens_before_scan_tasks()
    test_zero_budget_advances_exactly_one()
    test_zero_budget_also_advances_prepass()
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


# ── 巡检预算与忙时重投退避（2026-10-22：单 tick 563s 的两个来源）─────────────

def _queued(eng, rid, step="s1"):
    row = eng._lifecycle.query(
        "SELECT step_results FROM workflow_instances WHERE instance_id=?", (rid,))[0]
    return (json.loads(row["step_results"] or "{}").get(step) or {}).get("queued", 0)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_guard_sweep_is_bounded_and_rotates(_sp):
    """巡检必须有时间上限，且超预算时停在角色之间、下轮从游标续（不饿死固定前缀）。"""
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    seen = []

    def _slow(role, deadline=0.0):  # 巡检把预算透传给每个角色（engine.py 的巡检循环）
        # 断言透传真的发生：改回 _ensure_role_alive(_role) 时这条会红（缺 deadline）。
        assert deadline > 0, "巡检未把 deadline 透传给 _ensure_role_alive"
        seen.append(role)
        # 单个角色的耗时必须**远大于**预算：原先 sleep 0.02 配 5ms 预算，在负载高的机器上
        # 连第一次进入循环前的那几毫秒判断都能超时 → seen 恒为 []（实测 PSI full 28% 时必红）。
        time.sleep(0.5)
        return True

    eng._ensure_role_alive = _slow
    eng._collect_guard_roles = lambda: {"r1", "r2", "r3"}
    del eng._ensure_guard_roles      # 撤掉 _fast 的桩，测真实实现

    eng._ensure_guard_roles(deadline=time.time() + 0.4)   # 时间只够 1 个角色
    assert len(seen) == 1, f"预算内应只巡视 1 个角色: {seen}"
    eng._ensure_guard_roles(deadline=time.time() + 0.4)
    assert seen[1] != seen[0], f"第二轮必须换角色，否则固定前缀永久饿死: {seen}"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_busy_delivery_backs_off(_sp):
    """角色忙时不得每个 tick 都重投：实测 s2 重投 208 次、67 分钟才送达。"""
    d = _env()
    _wf(d, "tbflow", ["s1", "s2"])
    eng = _fast(WorkflowEngine(d))
    rid = eng.start("tbflow", {})
    eng._send_to_role = lambda *a, **k: "busy"

    eng.run_once()
    assert _queued(eng, rid) == 1, "首投忙应记 1 次排队"
    eng.run_once()          # 紧接着一轮：退避期内
    assert _queued(eng, rid) == 1, "退避期内不得再投（否则就是每 tick 一次 ccs send）"
