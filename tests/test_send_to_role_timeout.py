#!/usr/bin/env python3
"""回归守卫：ccs send 超时必须被 _send_to_role 收口，不得冒泡中断 tick。

现场（2026-10-19，8 次复发：19:53 / 19:57 / 20:54 / 21:19）：
  engine.py:933  run_once → self._send_to_role(_eff, prompt, ...)
  role_lifecycle.py:780  subprocess.run(..., timeout=30)  ← 抛出
  subprocess.py:1269     _check_timeout → TimeoutExpired

TimeoutExpired 不是 ValueError/KeyError/TypeError 的子类，原先不被捕获：
  1. 冒泡出 _send_to_role → 冒泡出 run_once
  2. 被 engine.py:1332 的 sqlite_block 兜住 → 打 "生产工作流扫描异常"
  3. **整个 tick 的生产工作流扫描被中断**（同 tick 其他工作流全部停一步）
  4. 本步拿不到返回值 → 违反红线「投递失败必须返回 False，下轮重试」

触发条件：prompt 长（实测 2299 字符的 reviewer task_spec 就超 30s）。review 类
模板 prompt 本就长（六维审查 + 合规性检查项），故 s4/review 步骤最易命中。

行为契约：超时 → 返回 "busy"（排队等下轮），不抛异常、不灌 delivery_blocked。
"""
import sys
import subprocess
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def _mk_engine():
    """构造一个最小 WorkflowEngine，绕过 __init__ 的重依赖。"""
    from pipeflow.engine import WorkflowEngine

    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng._bb = MagicMock()
    eng._last_bus_write = {}
    # 投递前的守卫全部放行，直达 ccs send 那一行
    eng._wait_for_role_ready = MagicMock(return_value=True)
    eng._is_role_busy = MagicMock(return_value=False)
    return eng


def _call_send_to_role(eng, prompt="x" * 2299):
    return eng._send_to_role("reviewer", prompt, wf_id="wf_sub_timeout", step_id="s1")


def test_timeout_returns_busy_not_raises():
    """ccs send 超时 → 返回 "busy"（不抛异常）。"""
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ccs send", timeout=30)
        result = _call_send_to_role(eng)
    assert result == "busy", f"超时应返回 busy（排队重试），实际 {result!r}"


def test_timeout_does_not_propagate():
    """超时不得冒泡出 _send_to_role（否则 engine.run_once 的 tick 被中断）。"""
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ccs send", timeout=30)
        try:
            _call_send_to_role(eng)
        except subprocess.TimeoutExpired:
            raise AssertionError("TimeoutExpired 冒泡了 —— 会中断整个 tick 的扫描")


def test_timeout_writes_blocker_for_visibility():
    """超时应写 blocker，使资源类阻断可被外部观测。"""
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ccs send", timeout=30)
        _call_send_to_role(eng)
    cats = [c.args[0] for c in eng._bb.write.call_args_list if c.args]
    assert "blocker" in cats, f"超时应写 blocker，实际写入 {cats}"


def test_other_exceptions_still_handled():
    """既有的 ValueError/KeyError/TypeError 兜底分支不得被新分支吞掉。"""
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run") as mock_run:
        # 主路径抛 TypeError → 应走 fallback 分支（第二次调用也抛 → 返回 False）
        mock_run.side_effect = TypeError("bad arg")
        result = _call_send_to_role(eng)
    assert result is False, f"兜底耗尽应返回 False（投递链路异常），实际 {result!r}"


def test_rc1_with_busy_reason_returns_busy():
    """ccs.py 对忙态以 rc=1 退出（`{"success": false, "reason": "busy"}`）→ 必须返回 busy。

    非零退码一律判 False 会让角色长跑期间的重推累计 delivery_fail_count，
    最终灌成 delivery_blocked —— 正好抵消忙时拒投的意义。
    """
    eng = _mk_engine()
    _busy_out = json.dumps({"success": False, "reason": "busy",
                            "error": "CCS reviewer[1] 思考中，未投递"})
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=1, stdout=_busy_out, stderr=b"")):
        result = _call_send_to_role(eng)
    assert result == "busy", f"忙态 rc=1 应返回 busy（排队重试），实际 {result!r}"


def test_rc1_busy_reason_as_bytes_returns_busy():
    """生产路径 capture_output 无 text=True → stdout 是 bytes，同样要识出 busy。"""
    eng = _mk_engine()
    _busy_out = json.dumps({"success": False, "reason": "busy"}).encode()
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=1, stdout=_busy_out, stderr=b"")):
        result = _call_send_to_role(eng)
    assert result == "busy", f"bytes stdout 的忙态应返回 busy，实际 {result!r}"


def test_rc1_without_busy_reason_returns_false():
    """防修过头：非 busy 的 rc=1 是真故障，仍必须返回 False。

    若把整个非零码分支都收口为 busy，投递失败会被静默吞掉、永不计数。
    """
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=1, stdout="", stderr=b"boom")):
        result = _call_send_to_role(eng)
    assert result is False, f"真失败 rc=1 应返回 False，实际 {result!r}"


if __name__ == "__main__":
    import traceback

    failed = 0
    for fn in (test_timeout_returns_busy_not_raises,
               test_timeout_does_not_propagate,
               test_timeout_writes_blocker_for_visibility,
               test_other_exceptions_still_handled,
               test_rc1_with_busy_reason_returns_busy,
               test_rc1_busy_reason_as_bytes_returns_busy,
               test_rc1_without_busy_reason_returns_false):
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
        except Exception:
            failed += 1
            print(f"  FAIL  {fn.__name__}")
            traceback.print_exc()
    print("FAILED" if failed else "ALL PASS")
    sys.exit(1 if failed else 0)
