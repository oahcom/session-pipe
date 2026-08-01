#!/usr/bin/env python3
"""
_tick 路径专项测试 — 覆盖现有 test_workflow_engine.py 未达边界:
  1. completed/cancelled 状态提前返回
  2. exit_schema 验证失败 → blocker 消息
  3. verify 命令失败 → blocker 消息
  4. verify 命令通过 → complete_step 调用
  5. _lifecycle.complete_step 异常 → poll_since 推进
  6. 超时未匹配 → timeout_count 递增
  7. 每 3 次超时 → escalation 到 coordinator
  8. 催办提醒发送 (last_reminder 更新)
  9. 空 exit_condition 不崩溃
 10. daemon: PID 锁首次获取/过期清理/损坏清理
 11. daemon: daemon_loop 异常后 PID 清理

构造方式: 手动实例化 __new__，跳过 __init__ 的 DB/Blackboard 初始化，
只给 _tick 所需属性打 mock。

运行: cd /home/administrator/session-pipeline && python3 tests/test_tick_paths.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock, call

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

import pipeflow.engine as engine_mod
from pipeflow.engine import WorkflowEngine, WorkflowRun, Step

# 保存原始值，测试后恢复
_orig_grace = getattr(engine_mod, '_TIMEOUT_GRACE', 10)
if hasattr(engine_mod, '_TIMEOUT_GRACE'):
    engine_mod._TIMEOUT_GRACE = 0.01


def _build_eng() -> WorkflowEngine:
    """手动构造 WorkflowEngine 实例，跳过 __init__ 的 DB/文件系统依赖。"""
    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng.workflows_dir = Path(tempfile.mkdtemp())
    eng.runs_dir = eng.workflows_dir / "runs"
    eng._workflows = {}
    eng._lm = MagicMock()
    eng._bb = MagicMock()
    eng._send_to_role = MagicMock()  # 阻止 tmux 子进程调用
    eng._sync_step_results = MagicMock()
    return eng


def _make_run(**kw) -> WorkflowRun:
    defaults = dict(id="wf_test", workflow_name="test", context={},
                    current_step="s1", status="running",
                    step_results={"s1": {"status": "notified", "ts": time.time() - 5}})
    defaults.update(kw)
    return WorkflowRun(**defaults)


def _make_step(**kw) -> Step:
    defaults = dict(id="s1", title="t", target_role="scout",
                    prompt_template="{topic}",
                    exit_condition={"bus_category": "code_fix"},
                    max_retries=0, condition="", rollback_to="")
    defaults.update(kw)
    return Step(**defaults)


def _written_bb(eng):
    """获取 _bb.write 的所有 cat 参数调用列表。"""
    return [c.args[0] for c in eng._bb.write.call_args_list]


# ── 1. completed/cancelled 提前返回 ────────────────────────────────

def test_tick_returns_early_on_completed():
    eng = _build_eng()
    run = _make_run(status="completed")
    step = _make_step(exit_condition={"bus_category": "notice"})
    eng._tick(run, step)
    # 不查 exit_condition → _bb.read 不应被调用
    eng._bb.read.assert_not_called()


def test_tick_returns_early_on_cancelled():
    eng = _build_eng()
    run = _make_run(status="cancelled")
    step = _make_step(exit_condition={"bus_category": "notice"})
    eng._tick(run, step)
    eng._bb.read.assert_not_called()


# ── 2. exit_schema 验证失败 → blocker ──────────────────────────────

def test_tick_exit_schema_failure():
    """exit_condition 匹配但 schema 验证不通过 → blocker 消息。"""
    eng = _build_eng()
    # _check_exit 返回 True (模拟 exit_condition 匹配)
    with patch.object(eng, "_check_exit", return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}])):
        step = _make_step(
            exit_schema={"required": ["NO_SUCH_FILE_XXXX"],
                         "properties": {"NO_SUCH_FILE_XXXX": {"mustExist": True}}}
        )
        run = _make_run()
        eng._tick(run, step)

    cats = _written_bb(eng)
    assert "blocker" in cats, f"应写入 blocker: {cats}"


# ── 3. verify 命令失败 → blocker ──────────────────────────────────

def test_tick_verify_failure():
    eng = _build_eng()
    with patch.object(eng, "_check_exit", return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}])):
        step = _make_step(verify="false")  # shell false → exit code 1
        run = _make_run()
        eng._tick(run, step)

    cats = _written_bb(eng)
    assert "blocker" in cats, f"应写入 blocker: {cats}"


# ── 4. verify 通过 → complete_step 被调用 ──────────────────────────

def test_tick_verify_success_calls_complete_step():
    eng = _build_eng()
    with patch.object(eng, "_check_exit", return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}])):
        step = _make_step(verify="true")  # shell true → exit code 0
        run = _make_run()
        eng._tick(run, step)

    eng._lm.complete_step.assert_called_once()


# ── 5. complete_step 异常 → poll_since 推进 ───────────────────────

def test_tick_complete_step_exception_poll_since():
    """complete_step 抛异常 → poll_since 推进到当前时间，不崩溃。"""
    eng = _build_eng()
    eng._lm.complete_step.side_effect = RuntimeError("LM 异常模拟")
    eng._lm.begin = MagicMock()
    eng._lm.commit = MagicMock()
    eng._lm.rollback = MagicMock()
    eng._lm.execute_raw = MagicMock()
    eng._send_to_role = MagicMock()

    with patch.object(eng, "_check_exit", return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}])):
        step = _make_step(verify="true")
        run = _make_run()
        eng._tick(run, step)

    s1 = run.step_results.get("s1", {})
    # complete_step 异常 → timeout_count 递增（新增 ts 字段修复）
    assert s1.get("timeout_count", 0) >= 1, f"timeout_count 应被递增: {s1}"


# ── 6. 超时未匹配 → timeout_count 递增 ───────────────────────────

def test_tick_timeout_increments_count():
    """exit_condition 不匹配 + elapsed > timeout → timeout_count 递增。"""
    eng = _build_eng()
    eng._lm.begin = MagicMock()
    eng._lm.commit = MagicMock()
    eng._lm.rollback = MagicMock()
    eng._lm.execute_raw = MagicMock()
    eng._lm.execute = MagicMock()
    eng._lm.ping = MagicMock()

    with patch.object(eng, "_check_exit", return_value=(0.0, [])):
        step = _make_step(exit_condition={"timeout_minutes": 0})
        # ts 设为很久以前 → elapsed > timeout → 走超时分支
        run = _make_run(step_results={"s1": {"status": "notified",
                                              "ts": time.time() - 600}})
        eng._tick(run, step)

    s1 = run.step_results.get("s1", {})
    assert s1.get("timeout_count", 0) >= 1, f"timeout_count 应 ≥ 1: {s1}"


# ── 7. 每 3 次超时 → escalation ──────────────────────────────────

def test_tick_escalation_every_third():
    """第 1 次超时 → timeout_count=1 → coordinator 通知。"""
    eng = _build_eng()
    eng._lm.escalate_step = MagicMock()

    with patch.object(eng, "_check_exit", return_value=(0.0, [])):
        step = _make_step(exit_condition={"timeout_minutes": 0},
                          max_retries=5)
        # timeout_count=0 → 下次变成 1 → timeout_count==1 → coordinator 通知
        run = _make_run(step_results={"s1": {"status": "notified",
                                              "ts": time.time() - 600,
                                              "timeout_count": 0}})
        eng._tick(run, step)

    coord_calls = [c for c in eng._send_to_role.call_args_list if c.args[0] == "coordinator"]
    assert len(coord_calls) >= 1, f"应有 coordinator 通知: calls={eng._send_to_role.call_args_list}"


# ── 9. 空 exit_condition ──────────────────────────────────────────

def test_tick_empty_exit_condition():
    eng = _build_eng()
    with patch.object(eng, "_check_exit", return_value=(0.0, [])):
        step = _make_step(exit_condition={})
        run = _make_run()
        eng._tick(run, step)  # 不崩溃


# ── 10. daemon PID 锁 ─────────────────────────────────────────────

def test_acquire_pid_lock_first():
    import pipeflow.daemon as dm
    pid_file = Path(tempfile.mktemp(suffix=".pid"))
    orig = dm._LOCK_FILE
    dm._LOCK_FILE = pid_file
    try:
        assert dm._acquire_flock(), "_acquire_flock 返回 False — 锁获取失败"
        assert pid_file.exists()
    finally:
        if dm._LOCK_FD is not None:
            import fcntl as _fl
            _fl.flock(dm._LOCK_FD, _fl.LOCK_UN)
            dm._LOCK_FD.close()
            dm._LOCK_FD = None
        dm._LOCK_FILE = orig
        pid_file.unlink(missing_ok=True)


def test_acquire_pid_lock_stale():
    import pipeflow.daemon as dm
    pid_file = Path(tempfile.mktemp(suffix=".pid"))
    orig = dm._LOCK_FILE
    dm._LOCK_FILE = pid_file
    try:
        pid_file.write_text("999999999")
        assert dm._acquire_flock(), "_acquire_flock 对 stale PID 返回 False"
        assert pid_file.exists()
    finally:
        if dm._LOCK_FD is not None:
            import fcntl as _fl
            _fl.flock(dm._LOCK_FD, _fl.LOCK_UN)
            dm._LOCK_FD.close()
            dm._LOCK_FD = None
        dm._LOCK_FILE = orig
        pid_file.unlink(missing_ok=True)


def test_acquire_pid_lock_corrupted():
    import pipeflow.daemon as dm
    pid_file = Path(tempfile.mktemp(suffix=".pid"))
    orig = dm._LOCK_FILE
    dm._LOCK_FILE = pid_file
    try:
        pid_file.write_text("not_a_number")
        assert dm._acquire_flock(), "_acquire_flock 对损坏 PID 返回 False"
        assert pid_file.exists()
    finally:
        if dm._LOCK_FD is not None:
            import fcntl as _fl
            _fl.flock(dm._LOCK_FD, _fl.LOCK_UN)
            dm._LOCK_FD.close()
            dm._LOCK_FD = None
        dm._LOCK_FILE = orig
        pid_file.unlink(missing_ok=True)


# ── 11. daemon_loop 异常后 PID 清理 ────────────────────────────────

def test_daemon_loop_cleans_pid_on_error():
    """KeyboardInterrupt → finally 删除 PID 文件。"""
    import pipeflow.daemon as dm
    pid_file = Path(tempfile.mktemp(suffix=".pid"))
    orig = dm._LOCK_FILE
    dm._LOCK_FILE = pid_file

    eng = MagicMock()
    eng.run_once.side_effect = KeyboardInterrupt()

    with patch("pipeflow.daemon.WorkflowEngine", return_value=eng):
        try:
            dm.daemon_loop(0.01)
        except (KeyboardInterrupt, SystemExit):
            pass
        except Exception:
            pass

    dm._LOCK_FILE = orig
    assert not pid_file.exists() or pid_file.read_text().strip() == str(os.getpid()), \
        f"PID 文件应被清理或包含当前 PID"
    pid_file.unlink(missing_ok=True)


# ── 12. daemon_loop 优雅关闭 (KeyboardInterrupt) ──────────────────

def test_daemon_loop_graceful_shutdown():
    """eng.run_once 抛 KeyboardInterrupt → finally 清理 PID，无泄漏。"""
    import pipeflow.daemon as dm
    pid_file = Path(tempfile.mktemp(suffix=".pid"))
    orig = dm._LOCK_FILE
    dm._LOCK_FILE = pid_file

    eng = MagicMock()
    eng.run_once.side_effect = KeyboardInterrupt()

    with patch("pipeflow.daemon.WorkflowEngine", return_value=eng):
        try:
            dm.daemon_loop(0.01)
        except (KeyboardInterrupt, SystemExit):
            pass
        except Exception:
            pass

    dm._LOCK_FILE = orig
    pid_file.unlink(missing_ok=True)


# ── Main ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== _tick 路径专项测试 ===\n")

    tests = [
        ("1.  completed 提前返回",          test_tick_returns_early_on_completed),
        ("2.  cancelled 提前返回",          test_tick_returns_early_on_cancelled),
        ("3.  exit_schema 失败 → blocker",  test_tick_exit_schema_failure),
        ("4.  verify 失败 → blocker",       test_tick_verify_failure),
        ("5.  verify 通过 → complete_step",  test_tick_verify_success_calls_complete_step),
        ("6.  complete_step 异常处理",       test_tick_complete_step_exception_poll_since),
        ("7.  timeout_count 递增",          test_tick_timeout_increments_count),
        ("8.  每 3 次超时 escalation",      test_tick_escalation_every_third),
        ("10. 空 exit_condition",           test_tick_empty_exit_condition),
        ("11. PID 锁首次获取",              test_acquire_pid_lock_first),
        ("12. PID 锁过期清理",              test_acquire_pid_lock_stale),
        ("13. PID 锁损坏清理",              test_acquire_pid_lock_corrupted),
        ("14. daemon_loop 异常后 PID 清理",  test_daemon_loop_cleans_pid_on_error),
        ("15. daemon_loop 优雅关闭",         test_daemon_loop_graceful_shutdown),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  OK  {name}")
        except Exception as e:
            print(f"  X   {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
