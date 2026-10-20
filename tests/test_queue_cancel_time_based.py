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

def test_budget_follows_declared_timeout():
    """上限 = 声明 × 次数，不是全局 1800（bus #425410 判据 2）。"""
    from pipeflow.step_handler import _queue_budget_sec
    assert _queue_budget_sec({"timeout_minutes": 120}, 4) == 120 * 60 * 4
    assert _queue_budget_sec({"timeout_minutes": 60}, 1) == 3600

def test_budget_floor_and_parent_override():
    """短步不得低于 1800s；子链预算不低于父步声明。"""
    from pipeflow.step_handler import _queue_budget_sec
    assert _queue_budget_sec({"timeout_minutes": 0.001}, 4) == 1800
    assert _queue_budget_sec({}, 4) == 30 * 60 * 4   # 未声明按默认 30min
    assert _queue_budget_sec({"timeout_minutes": 15}, 4, parent_minutes=60) == 60 * 60 * 4

def test_requeue_clears_queue_ledger():
    """父步重置后必须清 queued/queued_since，否则重投立刻撞旧账本（判据 4）。"""
    from pipeflow.engine import _REQUEUE_DROP
    sd = {"status": "rejected", "queued": 26, "queued_since": time.time() - 4000,
          "subflow_retry": 2}
    cleaned = {k: v for k, v in sd.items() if k not in _REQUEUE_DROP}
    assert "queued" not in cleaned and "queued_since" not in cleaned
    assert cleaned.get("subflow_retry") == 2, "重建预算跨重投保留"

def test_parent_minutes_accepts_db_row():
    """engine.run_once 传的是 DB 行 dict，不是 WorkflowRun（曾误调不存在的方法）。"""
    from pipeflow.engine import WorkflowDef, Step as EStep
    eng = _Eng()
    eng._lifecycle.query_one.return_value = {
        "parent_wf_id": "wf_p", "subflow_source_step_id": "s2", "template_id": "tpl_p"}
    eng._workflows = {"tpl_p": WorkflowDef(name="tpl_p", title="t", description="d", steps=[
        EStep(id="s2", title="t", target_role="engineer", prompt_template="t",
              exit_condition={"timeout_minutes": 60})])}
    assert eng._parent_step_minutes({"instance_id": "wf_c"}) == 60
    assert eng._parent_step_minutes(object()) == 0.0, "无 id 的入参必须安全返回 0"


def test_degrade_path_respects_30min_window():
    """降级为超时计数也必须等满 30 分钟，否则 4 个 tick 就绕过门槛。

    codex P1：_queued>=3 后走降级分支，timeout_count 每 tick +1，
    _auto_cancel_at=4 → 排队约 50s 即 cancel，与修复前的 97~121s 误杀同型。
    """
    eng = _Eng()
    # 排队 2 分钟、timeout_count 已经到 3（降级后正常值）
    run = _run(queued=5, since=time.time() - 120, timeout_count=3)
    for _ in range(3):
        with _no_signal():
            eng._tick(run, _step())
    assert not eng._lifecycle.close_wf.called, \
        f"排队 2 分钟且 tc=3 时不得 cancel（实际 {eng._lifecycle.close_wf.call_args_list}）"
