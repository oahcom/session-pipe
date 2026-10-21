#!/usr/bin/env python3
"""闸门长期不过必须升级给模板所有者 —— 判据恒假时角色再怎么改也没用。

生产证据（2026-10-22）：WL-full_dev_pipeline/s4 的 verify 是
`'P0' not in REVIEW.md`，而任何合格审查报告都要写 P0/P1 计数
（实测 REVIEW.md 28130B、grep -c P0 = 4）→ 闸门恒真拦下，实例空转 18.3h
直到 24h 全局回收，期间角色被反复催一条做不到的指令。
抢救分支原本只 `return`：不计次、不升级、不落 workflow_logs（观测盲区）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_gate_escalation.py -q
"""
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow import step_handler as sh  # noqa: E402


def _harness(blocked=True):
    fake = SimpleNamespace()
    fake._bb = MagicMock()
    fake._notify_role = MagicMock()
    fake._send_to_role = MagicMock(return_value=True)
    fake._eff_role = MagicMock(return_value="reviewer")
    # _step_sd 是 StepHandlerMixin 的 staticmethod：鸭子类型的 self 也要绑上
    fake._step_sd = sh.StepHandlerMixin._step_sd
    fake._sync_step_results = MagicMock()
    fake._quality_gate_blocks = MagicMock(return_value=blocked)
    fake._lifecycle = MagicMock()
    run = SimpleNamespace(
        id="wf_test", workflow_name="WL-full_dev_pipeline", context={},
        current_step="s4", status="running", created_at=1.0, updated_at=1.0,
        step_results={"s4": {"status": "notified", "notified_at": 1.0,
                             "exit_messages": [{"id": 1, "text": "s4-complete"}]}},
    )
    step = SimpleNamespace(id="s4", target_role="reviewer", title="审查",
                           type="review", max_retries=1, dual_node=False,
                           exit_condition={"text_contains": "full_dev_pipeline/s4-complete"},
                           exit_schema=None, verify="false", wait_roles=None, guard=None)
    return fake, run, step


def test_gate_block_escalates_once():
    fake, run, step = _harness()
    for _ in range(sh._GATE_ESCALATE_AT + 1):
        sh.StepHandlerMixin._tick(fake, run, step)
    sd = run.step_results["s4"]
    assert sd["gate_block_count"] == sh._GATE_ESCALATE_AT + 1, sd.get("gate_block_count")
    assert sd.get("gate_escalated_at"), "达阈值必须落升级锚"
    assert fake._bb.write.call_count == 1, f"只升级一次，实际 {fake._bb.write.call_count}"
    cat, title = fake._bb.write.call_args[0][:2]
    assert cat == "blocker" and "判据可能不可满足" in title, title
    assert fake._lifecycle.complete_step.call_count == 0, "判据没过不得 complete"


def test_gate_not_blocked_does_not_count():
    fake, run, step = _harness(blocked=False)
    sh.StepHandlerMixin._tick(fake, run, step)
    assert "gate_block_count" not in run.step_results["s4"]
    assert fake._lifecycle.complete_step.call_count == 1


if __name__ == "__main__":
    test_gate_block_escalates_once()
    test_gate_not_blocked_does_not_count()
    print("ok")
