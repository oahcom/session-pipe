"""主循环轮转：预算中断后从断点继续，任何实例都不会被永久跳过（R132）。

生产证据（2026-10-22 05:40）：tick 预算 60s 超限即 break 本轮剩余实例，而查询
无 ORDER BY（SQLite 按插入序）→ 尾部实例每轮都排不到：22 条在飞实例 150s 内
零变化，wf_rw_2664ec3aada3 等 3 条返工链 queued 卡在 235 从未投出，父链被回收。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_loop_rotation.py -q
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest  # noqa: E402

from pipeflow import engine as engine_mod  # noqa: E402
from pipeflow.engine import WorkflowEngine, _rotate_after  # noqa: E402

# tmux 探针已改为一次 list-panes -a 快照；"所有角色都活着"的桩让用例不碰真 tmux
# （真实调用会把不存在的 test_role 判死 → _wait_for_role_ready 每次 sleep 5s）。
_ALIVE = type("_A", (dict,), {
    "__contains__": lambda s, k: True,
    "get": lambda s, k, d=None: ("1", "0"),
    "__getitem__": lambda s, k: ("1", "0"),
})()


@pytest.fixture(autouse=True)
def _no_tmux():
    with patch("pipeflow.role_lifecycle._tmux_snapshot", lambda max_age=2.0: _ALIVE):
        yield


def _rows(*ids):
    return [{"instance_id": i} for i in ids]


def test_resume_after_last_processed():
    rows = _rows("a", "b", "c", "d")
    assert [r["instance_id"] for r in _rotate_after(rows, "b")] == ["c", "d", "a", "b"]


def test_wraps_when_breakpoint_is_last():
    rows = _rows("a", "b", "c")
    assert [r["instance_id"] for r in _rotate_after(rows, "c")] == ["a", "b", "c"]


def test_no_resume_id_keeps_order():
    rows = _rows("a", "b")
    assert [r["instance_id"] for r in _rotate_after(rows, "")] == ["a", "b"]


def test_unknown_resume_id_keeps_order():
    """断点实例已终态/被删：保持原序，不丢任何实例。"""
    rows = _rows("a", "b")
    assert [r["instance_id"] for r in _rotate_after(rows, "gone")] == ["a", "b"]


def test_rotation_gives_every_instance_a_turn():
    """模拟预算只够 2 条：连续 3 轮必须覆盖全部 5 条（按插入序则永远只有前 2 条）。"""
    ids = ["a", "b", "c", "d", "e"]
    rows = _rows(*ids)
    seen, resume = set(), ""
    for _ in range(3):
        rotated = _rotate_after(rows, resume)
        for x in rotated[:2]:
            seen.add(x["instance_id"])
        resume = rotated[1]["instance_id"]
    assert seen == set(ids)


# ── 接线层：纯函数测试挡不住"指针根本没赋值/赋值反了"（三方审查 2026-10-22 B-M1）──

def _env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _wf(d, name):
    steps = [{"id": "s1", "title": "s1", "target_role": "test_role",
              "prompt_template": "do s1",
              "exit_condition": {"bus_category": "notice",
                                 "text_contains": "NEVER_MATCH_ROTATION",
                                 "timeout_minutes": 30},
              "max_retries": 1, "type": "single"}]
    (d / f"{name}.json").write_text(json.dumps(
        {"name": name, "title": name, "description": "", "steps": steps},
        ensure_ascii=False))


def _fast(eng):
    """打桩与本用例无关的慢阶段（真实实现里是 tmux/ccs 子进程扫描）。"""
    for name in ("_ensure_guard_roles", "_kick_stalled_roles", "_recover_delivery_blocked",
                 "_cleanup_stale_workflows", "_cleanup_orphan_tasks", "_scan_tasks"):
        setattr(eng, name, lambda *a, **k: None)
    return eng


@patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=""))
def test_budget_break_walks_every_instance(_sp):
    """预算只够一条时，连续 tick 必须轮流走到每一条实例并回绕。

    断点未赋值 → seen 全空串；未轮转 → 恒同一条；回绕缺失 → 第 4 轮对不上。
    """
    d = _env()
    _wf(d, "rot")
    eng = _fast(WorkflowEngine(d))
    ids = [eng.start("rot", {}) for _ in range(3)]

    _old = engine_mod._TICK_BUDGET_SEC
    engine_mod._TICK_BUDGET_SEC = -1      # 负预算 = 恒超 → 每 tick 只走一条
    try:
        seen = []
        for _ in range(4):
            eng.run_once()
            seen.append(eng._loop_resume_id)
    finally:
        engine_mod._TICK_BUDGET_SEC = _old

    assert set(seen[:3]) == set(ids), f"三轮未覆盖全部实例: {seen} vs {ids}"
    assert seen[3] == seen[0], f"第四轮未回绕到起点: {seen}"
