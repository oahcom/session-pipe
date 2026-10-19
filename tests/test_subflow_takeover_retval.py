#!/usr/bin/env python3
"""_subflow_takeover 返回值语义 — 防「子链接管后父步重复投递」。

根因（2026-10-19 生产实测）：子链创建后父步 status 留空，engine 每 tick 重入
投递分支 → 命中「已有 subflow_id」的防重分支 → 裸 `return`（falsy）被调用方
当作「未接管」→ 照常投递父步 → 与子链抢同一角色。
实测 18 实例累计 queued=4240（单步最大 293，wf_5d6bf3bf79/s1）；活体复现
wf_e50bbe5664：父链 6552.68 建 → 子链 6554.79 接管 → 父步 6556.50 仍被投递。

返回值契约（2 个调用点均以 `if ... and self._subflow_takeover(...)` 判定）：
  True  = 子链仍活，调用方**不得**再投父步
  False = 未接管/子链已死，调用方投父步

状态判据为**活跃白名单**（fail-closed）：生产步骤级 status 实测 7 种取值，
其中 delivery_blocked（233 条）既非 failed 也非 cancelled——若按「非终态即
活着」放行，父步永不自投，而子链要等 _recover_delivery_blocked 恢复（角色
持续离线时永不恢复）→ 父步永久悬空。

运行: cd /home/administrator/session-pipeline && python3 tests/test_subflow_takeover_retval.py
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pipeflow.wf_lifecycle as wfl  # noqa: E402


class _FakeCur:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    """最小 sqlite 连接替身：只回答本用例需要的两条 SELECT。"""

    def __init__(self, sr, sub_status):
        self._sr = json.dumps(sr) if sr is not None else None
        self._sub_status = sub_status

    def execute(self, sql, params=()):
        if "step_results FROM workflow_instances" in sql:
            return _FakeCur(None if self._sr is None else {"step_results": self._sr})
        if "SELECT status FROM workflow_instances" in sql:
            return _FakeCur(None if self._sub_status is None else {"status": self._sub_status})
        return _FakeCur(None)


class _FakeTx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *a):
        return False


def _taker(sr, sub_status):
    m = wfl.WorkflowLifecycleMixin()
    m._workflows = {}
    m._is_role_busy = lambda *a, **k: False
    lm = MagicMock()
    lm._conn_tx.return_value = _FakeTx(_FakeConn(sr, sub_status))
    return m, lm


def _step():
    s = MagicMock()
    s.id = "s1"
    s.target_role = "pm"
    s.subflow_template = "pm_requirements"
    return s


def _run(sr, sub_status):
    m, lm = _taker(sr, sub_status)
    return m._subflow_takeover(lm, "wf_parent", "wfname", _step(), -1,
                               {"s1": sr["s1"]}, "s1")


# ── 活跃状态 → 接管 ───────────────────────────────────────────────

def test_live_subflow_returns_true():
    """子链 running → True（接管）。裸 return 会让父步重复投递。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "running")
    assert r is True, f"期望 True（已接管），实际 {r!r} —— 父步会与子链抢角色"


def test_pending_subflow_returns_true():
    """子链 pending（已建未跑）→ True，父步不得抢跑。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "pending")
    assert r is True, f"期望 True，实际 {r!r}"


def test_step_done_ready_subflow_returns_true():
    """子链 step_done_ready（等审批，仍活）→ 接管。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "step_done_ready")
    assert r is True, f"期望 True，实际 {r!r}"


# ── 终态/停滞态 → 交还父步 ────────────────────────────────────────

def test_failed_subflow_returns_false():
    """子链 failed → 交还父步自投，否则该步永久悬空。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "failed")
    assert r is False, f"期望 False（交还父步），实际 {r!r} —— 该步将永久悬空"


def test_cancelled_subflow_returns_false():
    """子链 cancelled → 同上。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "cancelled")
    assert r is False, f"期望 False（交还父步），实际 {r!r}"


def test_delivery_blocked_subflow_returns_false():
    """子链 delivery_blocked → 交还父步。生产实测该状态 233 条。

    它既非 failed 也非 cancelled，若当"活着"则父步永不自投，而子链要等
    _recover_delivery_blocked 恢复（角色持续离线时永不恢复）→ 永久悬空。
    """
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "delivery_blocked")
    assert r is False, f"期望 False（交还父步），实际 {r!r} —— 父步将永久悬空"


# ── 未知/缺失状态 → fail-closed ───────────────────────────────────

def test_unknown_status_returns_false():
    """未知状态 → fail-closed 交还父步，不依赖「非终态即活着」的默认放行。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "weird_unknown")
    assert r is False, f"期望 False（fail-closed），实际 {r!r}"


def test_missing_subflow_row_returns_false():
    """子链行查不到 → 保守交还父步，不静默卡死。"""
    r = _run({"s1": {"subflow_id": "wf_sub_gone"}}, None)
    assert r is False, f"期望 False（保守自投），实际 {r!r}"


def test_empty_string_status_returns_false():
    """空 status（生产中 23 条）→ 交还父步，不当活跃处理。"""
    r = _run({"s1": {"subflow_id": "wf_sub_child"}}, "")
    assert r is False, f"期望 False，实际 {r!r}"


def test_no_subflow_returns_falsy():
    """无 subflow_id → 未接管，且不得抛异常。"""
    r = _run({"s1": {}}, "running")
    assert not r, f"期望 falsy（未接管），实际 {r!r}"


if __name__ == "__main__":
    tests = [
        ("子链 running → True（接管）", test_live_subflow_returns_true),
        ("子链 pending → True（接管）", test_pending_subflow_returns_true),
        ("子链 step_done_ready → True", test_step_done_ready_subflow_returns_true),
        ("子链 failed → False（交还）", test_failed_subflow_returns_false),
        ("子链 cancelled → False（交还）", test_cancelled_subflow_returns_false),
        ("子链 delivery_blocked → False", test_delivery_blocked_subflow_returns_false),
        ("未知状态 → False（fail-closed）", test_unknown_status_returns_false),
        ("子链行缺失 → False（保守）", test_missing_subflow_row_returns_false),
        ("空 status → False", test_empty_string_status_returns_false),
        ("无 subflow_id → falsy", test_no_subflow_returns_falsy),
    ]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  OK  {name}")
        except AssertionError as e:
            failed += 1
            print(f"  X   {name}: {e}")
        except Exception as e:
            failed += 1
            print(f"  E   {name}: {type(e).__name__}: {e}")
    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
