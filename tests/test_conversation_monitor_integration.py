#!/usr/bin/env python3
"""conversation_monitor × step_handler 集成测试 — tmux 信号补充 exit_condition 触发。"""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.step_handler import StepHandlerMixin
from conversation_monitor import get_conversation_signal, ConversationSignal


class FakeEngine(StepHandlerMixin):
    """最小化引擎：只测 _tick 的 tmux 信号补充路径。"""

    def __init__(self):
        self._lifecycle = MagicMock()
        self._send_to_role = MagicMock()
        self._sync_step_results = MagicMock()
        self._is_role_busy = MagicMock(return_value=False)
        self._consumed_exit_ids = set()
        self._bb = MagicMock()  # Blackboard mock（timeout 路径依赖）
        self._workflows = MagicMock()  # step_handler 末位值门禁访问 self._workflows
        # 显式声明无活跃子流：_tick 的 idle/violation cancel 分支都带
        # `and not self._has_live_subflow(...)` 守卫（bus #407499 父步误杀子流修复）。
        # 不覆写则 MagicMock 返回 truthy → 永远走 else，cancel_step 永不调用，
        # 测试结论与生产逻辑相反（2 例假失败）。本测试组语义即"无子流"路径。
        self._has_live_subflow = MagicMock(return_value=False)

    def _fill_prompt_vars(self, prompt, step, wf_name, title, desc, results,
                          prev_step_id="", wf_id=""):
        """桩：真实实现在 WorkflowLifecycleMixin（本类只继承 StepHandlerMixin）。

        超时重推路径会调它做 {title}/{description} 等变量替换；此处只需原样返回，
        让本测试聚焦 tmux 信号分支。
        """
        return prompt


class _FakeStep:
    """真实类替代 MagicMock：exit_condition 必须是真 dict 才能被 .get() 正确访问。"""

    def __init__(self, exit_condition: dict):
        self.exit_condition = exit_condition
        self.exit_schema = None
        self.type = "single"
        self.max_retries = 1
        self.id = "s1"
        self.target_role = "scout"
        self.dual_node = False
        self.wait_roles = None
        self.prompt_template = ""
        self.verify = {}
        self.title = "test step"
        self.track = {}


def _step(exit_condition: dict) -> _FakeStep:
    return _FakeStep(exit_condition)
    step.max_retries = 1
    step.id = "s1"
    step.target_role = "scout"
    step.dual_node = False
    step.wait_roles = None
    return step


class _FakeRun:
    """真实类替代 MagicMock：step_results 必须是真 dict 才能被 .get() 正确访问。"""

    def __init__(self, status: str = "running"):
        self.id = "wf_test"
        self.workflow_name = "test_wf"
        self.status = status
        self.created_at = 0.0
        self.current_step = "s1"
        self.context = {}
        self.step_results = {"s1": {"ts": 0.0, "poll_since": 0.0, "bus_anchor": None}}


def _run(status: str = "running") -> _FakeRun:
    return _FakeRun(status)


def test_tmux_step_completed_blocks_when_bus_contract_without_schema():
    """声明 bus 契约但无 exit_schema → pane 文本不得推进（防假完成）。

    实测 wf_7045df494d/s1：pane 里「s1 闭环（已完成）」在投递 98 秒后触发
    complete_step，exit_messages 为空——pane 是叙述不是产出证据。
    """
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(step_completed=True,
                                               completion_evidence="s1 完成")) as mock_sig:
        eng._tick(run, step)
        mock_sig.assert_called_once()
        assert eng._lifecycle.complete_step.call_count == 0, \
            "声明 bus 契约且无 exit_schema 时 tmux 信号不得推进"


def test_tmux_step_completed_triggers_exit_without_contract():
    """无 bus 契约的步骤 → tmux 信号仍可推进（否则只能等超时）。"""
    eng = FakeEngine()
    step = _step({"timeout_minutes": 30})
    run = _run()

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(step_completed=True,
                                               completion_evidence="s1 完成")) as mock_sig:
        eng._tick(run, step)
        mock_sig.assert_called_once()
        assert eng._lifecycle.complete_step.call_count == 1, \
            "无契约步骤的 tmux 完成信号应触发 complete_step"
        assert eng._lifecycle.complete_step.call_args[0][:2] == ("wf_test", "s1")


def test_tmux_idle_triggers_reminder_not_cancel():
    """tmux pane 空闲 → idle_complaint → 发提醒 prompt，不取消工作流。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(idle_complaint=True,
                                               idle_evidence="无任务待办")):
        eng._tick(run, step)
        # 空闲不触发 complete，但发提醒（timeout 催办也会发，因此 count >= 1）
        assert eng._lifecycle.complete_step.call_count == 0
        assert eng._send_to_role.call_count >= 1, "空闲应发至少一次提醒"
        # 第一条消息应是空闲提醒
        remind = eng._send_to_role.call_args_list[0]
        assert "空闲" in str(remind) or "提醒" in str(remind), \
            f"空闲应发提醒，实际: {remind}"


def test_tmux_idle_throttled_after_first():
    """第二次 idle 信号在冷却期内 → 不发提醒（节流）。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()
    # 预置：第一次 idle 已计过数且在冷却期内
    run.step_results["s1"]["tmux_signals"] = {"idle_count": 1,
                                              "idle_cool_until": time.time() + 300}

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(idle_complaint=True,
                                               idle_evidence="无任务待办")) as mock_sig:
        eng._tick(run, step)
        mock_sig.assert_called_once()
        # 冷却期内不应有新的 idle 提醒（原 idle 只发过一次）
        idle_msgs = [c for c in eng._send_to_role.call_args_list
                     if "空闲" in str(c) or "提醒" in str(c)]
        assert len(idle_msgs) == 0, f"冷却期不应发空闲提醒，实际: {idle_msgs}"


def test_tmux_idle_escalates_to_cancel_after_5():
    """连续 5 次 idle 信号 → cancel_step（不再等待）。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()
    run.step_results["s1"]["tmux_signals"] = {"idle_count": 4}  # 已 4 次

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(idle_complaint=True,
                                               idle_evidence="无任务待办")):
        eng._tick(run, step)
        assert eng._lifecycle.cancel_step.call_count == 1, \
            "第 5 次 idle 应升级 cancel_step"
        assert eng._lifecycle.cancel_step.call_args[0][:2] == ("wf_test", "s1")


def test_tmux_violation_escalates_to_cancel_after_5():
    """连续 5 次 violation → cancel_step。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()
    run.step_results["s1"]["tmux_signals"] = {"violation_count": 4}

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(violation_found=True,
                                               violation_commands=["deploy: kubectl apply"],
                                               violation_role="scout")):
        eng._tick(run, step)
        assert eng._lifecycle.cancel_step.call_count == 1, \
            "第 5 次 violation 应升级 cancel_step"
        assert eng._lifecycle.cancel_step.call_args[0][:2] == ("wf_test", "s1")


def test_tmux_violation_injects_stop():
    """tmux pane 越界操作 → violation → 注入 stop 指令。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal(violation_found=True,
                                               violation_commands=["deploy: kubectl apply"],
                                               violation_role="scout")):
        eng._tick(run, step)
        assert eng._send_to_role.call_count >= 1, "越界应发停止指令"
        stop = eng._send_to_role.call_args_list[0]
        assert "停止" in str(stop) or "stop" in str(stop).lower(), \
            f"越界应注入停止指令，实际: {stop}"


def test_no_tmux_signal_no_change():
    """无 tmux 信号 → 不影响原 bus exit 检查路径。"""
    eng = FakeEngine()
    step = _step({"bus_category": "workflow", "timeout_minutes": 30})
    run = _run()

    with patch("pipeflow.step_handler.get_conversation_signal",
               return_value=ConversationSignal()):
        eng._tick(run, step)
        # 无信号 → 只走原逻辑（此 mock 下无 bus 匹配，仅可能超时催办）
        assert eng._lifecycle.complete_step.call_count == 0


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {len(tests) - failed}/{len(tests)}")
    sys.exit(1 if failed else 0)
