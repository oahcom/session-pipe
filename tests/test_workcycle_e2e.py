#!/usr/bin/env python3
"""
E2E workflow lifecycle tests --- complete workcycle from start to finish.
All subprocess calls (tmux, ccs) are mocked for isolation.

Run: cd ~/session-pipeline && python3 -m pytest tests/test_workcycle_e2e.py -x -q
"""
import json
import signal
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow.engine import WorkflowEngine

# Global test timeout — prevents any single test from hanging
_TEST_TIMEOUT = 20


def _make_env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _write_wf(wf_dir, name, steps):
    data = {"name": name, "title": name, "description": "", "steps": steps}
    (wf_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False))


def _step(step_id, **overrides):
    base = {
        "id": step_id, "title": step_id, "target_role": "test_role",
        "prompt_template": f"do {step_id}",
        "exit_condition": {"bus_category": "notice",
                           "text_contains": f"DONE_{step_id.upper()}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════
# 1. Single-step lifecycle
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_single_step_lifecycle(mock_run):
    """start -> notify -> bus exit -> run_once -> completed."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "single", [_step("s1")])
    eng = WorkflowEngine(d)
    rid = eng.start("single", {})

    # Initial state: running, step not yet in results
    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["current_step"] == "s1"

    # run_once -> sends prompt, marks notified
    eng.run_once()
    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "notified", s

    # Bus exit -> run_once -> complete
    eng._bb.write("notice", "DONE_S1 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()
    s = eng.status(rid)
    assert s["status"] == "completed", f"expected completed, got {s}"

    eng.cancel(rid)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 2. Two-step sequential
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_two_step_sequential(mock_run):
    """s1 -> advance -> s2 -> complete."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "two", [_step("s1"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("two", {})

    # Notify + exit s1
    eng.run_once()
    eng._bb.write("notice", "DONE_S1 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["current_step"] == "s2", f"expected s2, got {s}"

    # 先 run_once 让 s2 标记 notified（bus_anchor=notified_at），再写 DONE_S2
    # 否则消息被 s2 的 created_after 过滤，永远无法推进
    eng.run_once()
    # Exit s2
    eng._bb.write("notice", "DONE_S2 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["status"] == "completed", f"expected completed, got {s}"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 3. Handoff approval
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_handoff_approval(mock_run):
    """handoff -> step_done_ready -> confirm -> advance."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "ho", [_step("s1", type="handoff"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("ho", {})

    eng.run_once()
    eng._bb.write("notice", "DONE_S1 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "step_done_ready", s

    # Token-based confirm
    token = eng._lifecycle.get_approval_token(rid, "s1")
    assert token, "handoff must produce approval token"
    ok = eng._lifecycle.confirm_step(rid, "s1", token=token, approved=True)
    assert ok

    s = eng.status(rid)
    assert s["current_step"] == "s2", f"expected s2, got {s}"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 4. Handoff rejection
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_handoff_rejection(mock_run):
    """handoff -> step_done_ready -> reject -> still running."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "hr", [_step("s1", type="handoff")])
    eng = WorkflowEngine(d)
    rid = eng.start("hr", {})

    eng.run_once()
    eng._bb.write("notice", "DONE_S1 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()

    token = eng._lifecycle.get_approval_token(rid, "s1")
    ok = eng._lifecycle.confirm_step(rid, "s1", token=token, approved=False,
                                      reason="no good")
    assert not ok

    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["results"]["s1"]["status"] == "rejected"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 5. Cancel mid-workflow
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_cancel_mid_workflow(mock_run):
    """Cancel after s1 complete -> cancelled."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "cw", [_step("s1"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("cw", {})

    eng.run_once()
    eng._bb.write("notice", "DONE_S1 done", src="test_role")
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["current_step"] == "s2"

    # cancel 有"创建不足 5 分钟拒绝取消"保护 → 把 created_at 改老再 cancel
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET created_at=? WHERE instance_id=?",
        (time.time() - 600, rid))
    ok = eng.cancel(rid)
    assert ok
    s = eng.status(rid)
    assert s["status"] == "cancelled"
    assert not eng.cancel(rid)  # second cancel returns False
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 6. Invalid workflow name
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_start_invalid_workflow(mock_run):
    """Start nonexistent -> ValueError."""
    d = _make_env()
    eng = WorkflowEngine(d)
    try:
        eng.start("nonexistent", {})
        assert False, "Should raise"
    except ValueError as e:
        assert "未知工作流" in str(e)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 7. Empty steps
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_start_empty_steps(mock_run):
    """Workflow with empty steps -> ValueError."""
    d = _make_env()
    _write_wf(d, "empty", [])
    eng = WorkflowEngine(d)
    try:
        eng.start("empty", {})
        assert False, "Should raise"
    except ValueError as e:
        assert "无步骤" in str(e)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 8. Status on nonexistent
# ═══════════════════════════════════════════════════════════════════

def test_status_nonexistent():
    """Status of unknown ID -> error dict."""
    d = _make_env()
    eng = WorkflowEngine(d)
    s = eng.status("wf_nonexistent_999")
    assert s.get("error") == "不存在"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════════

def _cleanup(d):
    import shutil
    shutil.rmtree(d, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, lambda s, f: (
            print(f"\nTIMEOUT after {_TEST_TIMEOUT}s"), sys.exit(1)))
    import traceback as tb
    tests = [
        ("单步生命周期", test_single_step_lifecycle),
        ("两步顺序推进", test_two_step_sequential),
        ("Handoff 审批通过", test_handoff_approval),
        ("Handoff 拒绝", test_handoff_rejection),
        ("中途取消", test_cancel_mid_workflow),
        ("无效工作流名", test_start_invalid_workflow),
        ("空步骤", test_start_empty_steps),
        ("不存在状态", test_status_nonexistent),
    ]
    passed, failed = 0, 0
    for name, fn in tests:
        signal.alarm(_TEST_TIMEOUT)
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            tb.print_exc()
            failed += 1
    signal.alarm(0)
    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
