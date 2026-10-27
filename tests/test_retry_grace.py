"""传输层退避宽限（R168）。

背景：2026-10-27 11:28 `food_chain_loop/wf_59463da67abf/s0` 被 4 次超时回收。
R167 的现场快照给出真相：engineer 的 CLI 底栏停在
`Connection refused — a firewall or proxy m… · Retrying in 0s · attempt 10/15`，
pane 活着、CLI「忙」（在重试）、转录零工具调用。即角色没死也没偷懒，是在等上游回来。

这里给这种情况一个**有上限**的窗口延长：不记超时、不推任务，只把 ts 刷新，
`retry_since` 记住真实起点，超过 2 个窗口就按原路径回收。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_retry_grace.py -q
"""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, Step, WorkflowRun

STEP = Step(id="s0", title="初始化", target_role="engineer", prompt_template="x",
            exit_condition={"timeout_minutes": 30}, type="handoff", max_retries=0)
TIMEOUT = 30 * 60


def _eng():
    with patch.object(WorkflowEngine, "__init__", lambda s: None):
        eng = WorkflowEngine()
    eng._sync_step_results = MagicMock()
    return eng


def _run(retry_since=None, ts=None):
    sr = {"status": "notified", "ts": ts if ts is not None else time.time()}
    if retry_since is not None:
        sr["retry_since"] = retry_since
    run = WorkflowRun(id="wf_deadbeef1234", workflow_name="food_chain_loop",
                      context={}, current_step="s0", status="running", created_at=0.0)
    run.step_results = {"s0": sr}
    return run


def test_retrying_pane_extends_window_without_counting():
    eng = _eng()
    run = _run(ts=time.time() - TIMEOUT - 5)
    before = dict(run.step_results["s0"])
    with patch("liveness.pane_cli_retrying", return_value=True):
        assert eng._retry_grace(run, STEP, "engineer", TIMEOUT) is True
    after = run.step_results["s0"]
    assert "timeout_count" not in after, "延窗口不能顺带记一次超时"
    assert after["ts"] > before["ts"], "ts 必须刷新（否则下一 tick 立刻又超时）"
    assert after["retry_since"] <= time.time()
    eng._sync_step_results.assert_called_once()


def test_grace_is_capped_at_two_windows():
    eng = _eng()
    run = _run(retry_since=time.time() - 2 * TIMEOUT - 1, ts=time.time() - TIMEOUT - 5)
    with patch("liveness.pane_cli_retrying", return_value=True):
        assert eng._retry_grace(run, STEP, "engineer", TIMEOUT) is False, "宽限用完应放手回收"
    assert "retry_since" in run.step_results["s0"]  # 不动状态，交给原超时路径


def test_not_retrying_returns_false_and_touches_nothing():
    eng = _eng()
    run = _run(ts=time.time() - TIMEOUT - 5)
    before = dict(run.step_results["s0"])
    with patch("liveness.pane_cli_retrying", return_value=False):
        assert eng._retry_grace(run, STEP, "engineer", TIMEOUT) is False
    assert run.step_results["s0"] == before
    eng._sync_step_results.assert_not_called()


def test_probe_failure_is_not_treated_as_retrying():
    """探测失败(None)按「不延期」处理：不能因为抓不到 pane 就让步骤永不超时。"""
    eng = _eng()
    run = _run(ts=time.time() - TIMEOUT - 5)
    with patch("liveness.pane_cli_retrying", return_value=None):
        assert eng._retry_grace(run, STEP, "engineer", TIMEOUT) is False


# ── 接线验证：`_tick` 真的走到了宽限分支（不是只有 helper 自己能跑） ──
def test_tick_does_not_count_timeout_while_cli_retrying():
    """timeout_minutes=0 → elapsed 恒 >= timeout；pane 在退避时必须延窗口而非计数。"""
    eng = _eng()
    eng._bb = MagicMock()
    eng._lm = MagicMock()
    eng.__dict__["_lifecycle"] = eng._lm
    eng._lm.query = MagicMock(return_value=[])
    eng._workflows = {}
    eng._send_to_role = MagicMock()
    eng._notify_role = MagicMock()
    eng._is_role_busy = MagicMock(return_value=False)
    eng._check_exit = MagicMock(return_value=(0.0, []))
    eng._eff_role = MagicMock(return_value="engineer")

    step = Step(id="s0", title="初始化", target_role="engineer", prompt_template="x",
                exit_condition={"timeout_minutes": 30}, type="handoff", max_retries=0)
    run = WorkflowRun(id="wf_wire", workflow_name="food_chain_loop", context={},
                      current_step="s0", status="running", created_at=0.0)
    # 刚过 30 分钟窗口（elapsed > timeout），且本轮是第一次退避（无 retry_since）
    run.step_results = {"s0": {"status": "notified", "ts": time.time() - 1830}}

    with patch("pipeflow.step_handler.get_conversation_signal", return_value=None), \
         patch("liveness.pane_cli_retrying", return_value=True):
        eng._tick(run, step)

    sd = run.step_results["s0"]
    assert "timeout_count" not in sd, f"退避中不该记超时: {sd}"
    assert sd["ts"] > time.time() - 60, "ts 必须被刷新（窗口延长）"
    assert sd.get("retry_since"), "退避起点要落库，否则宽限永远算不出上限"
