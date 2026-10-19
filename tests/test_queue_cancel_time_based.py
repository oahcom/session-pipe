#!/usr/bin/env python3
"""排队 auto-cancel 必须按**时长**而非 tick 次数。

现场（2026-10-19 17:37-17:43）：三条返工链 wf_rw_7b8aef / 14f17bd / c7fd5f 在
queued_since→cancel 仅 97~121s 后全被回收。daemon 每 10s 一 tick，
`queued>=10` 实际只有 ~100s，而角色正常执行一步就要几分钟。

契约：queued 达 10 且排队时长 >= 30min 才 cancel；不足 30min 只延长窗口。
"""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import Step, WorkflowRun
from pipeflow.step_handler import StepHandlerMixin


class _Eng(StepHandlerMixin):
    def __init__(self):
        self._lifecycle = MagicMock()
        self._lifecycle.query_one.return_value = None
        self._bb = MagicMock()
        self._send_to_role = MagicMock(return_value=True)
        self._notify_role = MagicMock()
        self._sync_step_results = MagicMock()
        self._validate_exit_schema = MagicMock(return_value=(True, []))
        self._has_live_subflow = MagicMock(return_value=False)
        self._workflows = {}

    def _eff_role(self, run, step):
        return step.target_role

    def _is_role_busy(self, role, exclude_wf_id=None):
        return True

    def _check_exit(self, *a, **kw):
        return (None, [])


def _no_signal():
    # 真实 conversation_monitor 会读 tmux pane；测试机上有活 pane 时返回
    # step_completed，直接推进而绕过排队分支（实测本用例因此假通过）。
    return patch("pipeflow.step_handler.get_conversation_signal", return_value=None)


def _run(queued, since, timeout_count=0):
    run = WorkflowRun(id="wf_q", workflow_name="tpl", context={},
                      current_step="s1", status="running", created_at=time.time())
    run.step_results = {"s1": {"status": "notified", "queued": queued,
                               "queued_since": since, "ts": since,
                               "timeout_count": timeout_count,
                               "notified_at": since}}
    return run


def _step():
    # timeout 极短，强制进入超时/排队分支
    return Step(id="s1", title="t", target_role="engineer", prompt_template="t",
                exit_condition={"bus_category": "notice", "timeout_minutes": 0.001})


def test_short_queue_does_not_cancel():
    eng = _Eng()
    run = _run(queued=30, since=time.time() - 120)  # 2 分钟、30 次
    with _no_signal():
        eng._tick(run, _step())
    assert not eng._lifecycle.close_wf.called, "排队 2 分钟（哪怕 30 次）不得取消"


def test_long_queue_cancels():
    eng = _Eng()
    run = _run(queued=30, since=time.time() - 1900)  # 31 分钟
    run.step_results["s1"]["ts"] = time.time() - 1900
    with _no_signal():
        eng._tick(run, _step())
    eng._lifecycle.close_wf.assert_called_once()
    args, kwargs = eng._lifecycle.close_wf.call_args
    assert kwargs.get("status") == "cancelled", kwargs
