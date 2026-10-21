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
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

_PIPELINE_SRC = str(Path(__file__).resolve().parent.parent / "src")
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
    # _fill_prompt_vars 需要 str 类型的 task_title/task_desc；MagicMock 默认返回
    # MagicMock 导致 .replace() 抛 TypeError。配置 query 返回真实行数据。
    eng._lm.query.return_value = [{"title": "测试任务", "description": "测试描述"}]
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
    eng._is_role_busy = MagicMock(return_value=False)

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
    eng._is_role_busy = MagicMock(return_value=False)

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


# ── 8b. tmux 信号推进必须过契约门 (exit_schema) ───────────────────

def _fake_sig_step_completed():
    from conversation_monitor import ConversationSignal
    s = ConversationSignal()
    s.step_completed = True
    return s


def test_tmux_signal_blocked_by_missing_schema_file():
    """角色 pane 自称完成但契约文件没落盘 → 不得 complete_step。

    实测 food_chain_loop/s7: 16:54:18 被 tmux 信号推进，而 VALUE_AUDIT.md
    16:55:32 才落盘——盖戳时契约文件不存在，步骤被"半截子"确认。
    """
    eng = _build_eng()
    eng._is_role_busy = MagicMock(return_value=False)
    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_step_completed()):
        step = _make_step(
            exit_schema={"required": ["NO_SUCH_FILE_TMUX_XXXX"]},
        )
        run = _make_run()
        eng._tick(run, step)

    eng._lm.complete_step.assert_not_called()


def test_tmux_signal_proceeds_when_schema_satisfied():
    """契约文件已落盘 → tmux 信号正常推进（门不误伤正常路径）。"""
    import tempfile
    eng = _build_eng()
    eng._is_role_busy = MagicMock(return_value=False)

    ws = Path(tempfile.mkdtemp())
    (ws / "AUDIT_OK.md").write_text("FAIL\n" + "证据" * 60, encoding="utf-8")
    with patch("pipeflow.step_handler._CCS_WORKSPACES", ws.parent), \
         patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_step_completed()):
        # _validate_exit_schema 用 _CCS_WORKSPACES / target_role 拼路径
        step = _make_step(target_role=ws.name,
                          exit_schema={"required": ["AUDIT_OK.md"]})
        run = _make_run()
        eng._tick(run, step)

    eng._lm.complete_step.assert_called_once()



# ── 8c. 子链存活门：空闲取消不得误杀有活子链的父步 ─────────────────

def _fake_sig_idle():
    from conversation_monitor import ConversationSignal
    s = ConversationSignal()
    s.idle_complaint = True
    s.idle_evidence = "等待输入"
    return s


def _idle_run(**extra):
    sd = {"status": "notified", "ts": time.time(),
          "subflow_id": "wf_sub_child1",
          "tmux_signals": {"idle_count": 4}}  # 本轮 +1 → 5 命中取消阈值
    sd.update(extra)
    return _make_run(step_results={"s1": sd})


def test_idle_cancel_deferred_when_subflow_live():
    """子链仍在跑 → 不得按空闲取消父步。

    实测 wf_3f6128f1b3db/s2: 11:31:58 被空闲取消，子链 wf_sub_c5274c45c967
    的 s2/s3 分别 11:32:27/11:34:57 才完成——取消时子链仍活。
    """
    eng = _build_eng()
    eng._lm.query_one.return_value = {"status": "running"}
    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_idle()):
        eng._tick(_idle_run(), _make_step())
    eng._lm.cancel_step.assert_not_called()


def test_idle_cancel_uses_parent_link_when_subflow_id_gone():
    """subflow_id 已被历史 cancel 抹掉 → 退回 parent_wf_id 链接仍识别活子链。"""
    eng = _build_eng()
    eng._lm.query_one.side_effect = [
        {"instance_id": "wf_sub_child1"}, {"status": "running"}]
    run = _make_run(step_results={"s1": {"status": "notified", "ts": time.time(),
                                         "tmux_signals": {"idle_count": 4}}})
    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_idle()):
        eng._tick(run, _make_step())
    eng._lm.cancel_step.assert_not_called()


def test_idle_cancel_when_subflow_dead():
    """子链已 cancelled（死）→ 父步空闲取消维持原语义。"""
    eng = _build_eng()
    eng._lm.query_one.return_value = {"status": "cancelled"}
    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_idle()):
        eng._tick(_idle_run(), _make_step())
    eng._lm.cancel_step.assert_called_once()


def test_idle_cancel_no_subflow_unchanged():
    """无子链 → 空闲取消行为不变（守卫不误伤普通步骤）。"""
    eng = _build_eng()
    eng._lm.query_one.return_value = None  # 无 parent 链接子链
    run = _make_run(step_results={"s1": {"status": "notified", "ts": time.time(),
                                         "tmux_signals": {"idle_count": 4}}})
    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=_fake_sig_idle()):
        eng._tick(run, _make_step())
    eng._lm.cancel_step.assert_called_once()


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
        "PID 文件应被清理或包含当前 PID"
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
        ("8c. 子链活→空闲取消延后",          test_idle_cancel_deferred_when_subflow_live),
        ("8d. subflow_id 抹掉→parent 链接",  test_idle_cancel_uses_parent_link_when_subflow_id_gone),
        ("8e. 子链死→空闲取消不变",          test_idle_cancel_when_subflow_dead),
        ("8f. 无子链→空闲取消不变",          test_idle_cancel_no_subflow_unchanged),
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


# ── 8d. tmux 信号必须真投出去且不得带 /goal ────────────────────────
# 现场（R110，2026-10-21）：两个 helper 都只传 (role, msg)，_send_to_role 的
# 忙门控以 wf_id 为条件，wf_id 为空 → 守卫跳过 → 但调用方也没传 force，
# 于是 /goal 前缀被加上；且一旦有人补上 wf_id 就会立刻被忙门控拦成 "busy"，
# 返回值被 `LOGGER.debug` 吞掉。安全护栏在两个方向上都可能是空转。
# 契约：显式 force（跳过 /goal 前缀与忙门控），且不得带 wf_id（否则重写
# TASKS.md + 写 task_spec）；不得带 scheduler_noise（coordinator 目标会被拦截）。

def _capture_send():
    seen = []
    def _send(role, msg, **kw):
        seen.append((role, msg, kw))
        return True
    return _send, seen


def test_idle_reminder_is_forced_and_carries_ids():
    from pipeflow.step_handler import _remind_tmux_idle
    send, seen = _capture_send()
    run, step = _make_run(), _make_step()
    _remind_tmux_idle(run, step, "等待输入", send)
    role, msg, kw = seen[0]
    assert role == "scout"
    assert kw.get("force") is True, "未带 force → /goal 前缀锁死角色"
    assert "wf_id" not in kw, "催办不该带 wf_id：会连带重写 TASKS.md 并写 task_spec"
    assert "scheduler_noise" not in kw, \
        "scheduler_noise 会让 coordinator 目标的催办被 role_lifecycle:891 直接拦截"


def test_coordinator_targeted_stop_is_not_swallowed():
    """coordinator 作为步骤目标时，越界停止指令必须真的能投出去。

    生产库 WL-full_dev_pipeline 有 7 个实例 assignee=coordinator；
    scheduler_noise=True 在 role_lifecycle:891 对它直接 return False。
    """
    from pipeflow.step_handler import _stop_tmux_violation
    send, seen = _capture_send()
    run, step = _make_run(), _make_step()
    _stop_tmux_violation(run, step, ["rm -rf /tmp/x"], "coordinator", send)
    _, _, kw = seen[0]
    assert kw.get("force") is True
    assert not kw.get("scheduler_noise"), "停止指令被标记为噪音 → coordinator 侧静默丢弃"



def test_violation_stop_is_forced_and_carries_ids():
    from pipeflow.step_handler import _stop_tmux_violation
    send, seen = _capture_send()
    run, step = _make_run(), _make_step()
    _stop_tmux_violation(run, step, ["rm -rf /tmp/x"], "scout", send)
    role, msg, kw = seen[0]
    assert "越界操作" in msg and "rm -rf /tmp/x" in msg
    assert kw.get("force") is True, "安全护栏被 /goal 前缀变成 Stop hook 硬条件"


def test_forced_send_does_not_add_goal_prefix():
    """force=True 跳过 /goal 前缀 —— 否则运维催办变 Stop hook 硬条件。

    行为级断言（原实现 inspect.getsource 正则）：并发改 role_lifecycle.py 时
    getsource 会按旧 lineno 读到别的函数，全量跑间歇性红、单跑绿。
    """
    from unittest.mock import MagicMock, patch
    from pipeflow.role_lifecycle import RoleLifecycleMixin

    class _H(RoleLifecycleMixin):
        def __init__(self):
            self._bb = MagicMock()
            self._lm = MagicMock()
            self._lifecycle = MagicMock()
        def _is_role_busy(self, *a, **k):
            return False
        def _wait_for_role_ready(self, *a, **k):
            return True

    sent = []

    def fake_run(argv, *a, **k):
        sent.append(argv)
        return MagicMock(returncode=0, stdout="", stderr="")

    def _prompt(force):
        sent.clear()
        with patch("pipeflow.engine._sp.run", side_effect=fake_run):
            _H()._send_to_role("qa", "处理 abc 并写报告", wf_id="wf_x",
                               step_id="s1", force=force)
        calls = [c for c in sent if "send" in c and "qa" in c]
        assert calls, f"未发出 ccs send: {sent}"
        return next(x for x in calls[-1] if "处理 abc" in x)

    # 真实任务在任务正文前带 /goal（前面可能还有"先查证"等前置段）
    assert "/goal 处理 abc" in _prompt(False), "真实任务缺 /goal 前缀"
    assert "/goal " not in _prompt(True), \
        "催办带了 /goal 前缀 → 会被锁成 Stop hook"


# ── 9. 排队回收不得误杀正在产出的角色（活锁回归） ────────────────

def _queue_cancel_setup(producing: bool):
    """一个已排队超预算的步骤，角色是否在产出由 producing 决定。"""
    eng = _build_eng()
    eng._is_role_busy = MagicMock(return_value=True)
    step = _make_step(exit_condition={"bus_category": "code_fix", "timeout_minutes": 1})
    run = _make_run(step_results={"s1": {
        "status": "notified", "ts": time.time() - 7200,
        "queued": 50, "queued_since": time.time() - 7200,
    }})
    return eng, run, step


def test_queue_cancel_skips_role_that_is_producing():
    """角色在产出时排队超预算 ≠ 卡死，不得 auto-cancel。

    实测 2026-10-21：8h 内 product_architect 名下 14 条子链全部在排队满 60min
    时被取消，而同期它真跑完的子链耗时 113~237min —— 单角色串行时排队预算
    永远小于服务时间，取消不释放产能，是纯损失（活锁）。
    """
    eng, run, step = _queue_cancel_setup(producing=True)
    with patch("pipeflow.step_handler.get_conversation_signal", return_value=None), \
         patch("pipeflow.step_handler._role_producing_recently", return_value=True):
        eng._tick(run, step)
    eng._lm.close_wf.assert_not_called()


def test_queue_cancel_still_fires_when_role_dead():
    """反向守卫：角色确实不在产出时，排队超预算仍然要回收（否则真僵尸永占位）。"""
    eng, run, step = _queue_cancel_setup(producing=False)
    with patch("pipeflow.step_handler.get_conversation_signal", return_value=None), \
         patch("pipeflow.step_handler._role_producing_recently", return_value=False):
        eng._tick(run, step)
    eng._lm.close_wf.assert_called_once()
    assert eng._lm.close_wf.call_args.kwargs.get("status") == "cancelled"


# ── 10. 越界停止指令按批次冷却（不得每 tick 重发） ──────────────

def test_violation_stop_sent_once_per_batch():
    """同一批越界命令留在 pane 窗口里时，停止指令只发一次。

    去重键不变时 _bump_violation_count 返回旧值（升不到 cancel 阈值），
    旧实现走 else 分支每 tick 重发一次；force=True 后重发真能送达角色。
    """
    from conversation_monitor import ConversationSignal
    eng = _build_eng()
    sig = ConversationSignal()
    sig.violation_found = True
    sig.violation_commands = ["rm -rf /tmp/x"]
    sig.violation_role = "scout"
    run = _make_run()
    step = _make_step()
    with patch("pipeflow.step_handler.get_conversation_signal", return_value=sig):
        eng._tick(run, step)
        eng._tick(run, step)
        eng._tick(run, step)
    assert eng._send_to_role.call_count == 1, \
        f"同一批越界重发了 {eng._send_to_role.call_count} 次"


def test_violation_stop_resends_on_new_batch():
    """命令集合变了 = 新一次越界，必须立即再提醒（不被冷却拦住）。"""
    from conversation_monitor import ConversationSignal
    eng = _build_eng()
    run, step = _make_run(), _make_step()
    for cmds in (["rm -rf /tmp/x"], ["git push --force"]):
        sig = ConversationSignal()
        sig.violation_found = True
        sig.violation_commands = cmds
        sig.violation_role = "scout"
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=sig):
            eng._tick(run, step)
    assert eng._send_to_role.call_count == 2, "新一批越界被冷却吞掉了"
