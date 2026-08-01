"""稳定性加固回归测试 — timeout_count 防误回收 + _is_role_busy pane_fallback。

覆盖故障模式：
1. pane_fallback: survival 失效 + tmux pane 存活 → busy (防误回收)
2. 排队路径递增 queued 不递增 timeout_count (防忙时误回收)
3. timeout_count 达阈值 auto-cancel
"""
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import sys
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src/pipeflow"))

from pipeflow.engine import WorkflowEngine, Step, WorkflowRun

_HEALTH = Path(tempfile.mkdtemp())


def _eng(**overrides) -> WorkflowEngine:
    """构造最小可用 engine mock。"""
    with patch.object(WorkflowEngine, "__init__", lambda self: None):
        eng = WorkflowEngine()
    eng._HEALTH_DIR = _HEALTH
    eng._lm = MagicMock()
    # _lifecycle 是懒加载 property（内部 new LifecycleManager），直接 mock 掉
    eng._lm.close_wf = MagicMock()
    eng._lm.query = MagicMock(return_value=[])
    eng._lm.complete_step = MagicMock()
    eng._lm.start_wf = MagicMock()
    eng._lm.upsert_template = MagicMock()
    type(eng)._lifecycle = PropertyMock(return_value=eng._lm)
    eng._bb = MagicMock()
    eng._workflows = {}
    eng._tmux_pane_alive = MagicMock(return_value=False)
    eng._send_to_role = MagicMock()
    eng._notify_role = MagicMock()
    for k, v in overrides.items():
        setattr(eng, k, v)
    return eng


def _step(**kw) -> Step:
    defaults = dict(
        id="s1", title="test", target_role="qa", prompt_template="do",
        exit_condition={}, type="single", completion_check=None,
        max_retries=0, condition="", rollback_to="", verify="",
        failure_patterns=None, subflow_template="", exit_schema=None,
    )
    defaults.update(kw)
    return Step(**defaults)


def _run() -> WorkflowRun:
    now = time.time()
    return WorkflowRun(id="wf_t", workflow_name="test", context={},
                       current_step="s1", status="running", step_results={},
                       created_at=now, updated_at=now)


class TestPaneFallbackBusy(unittest.TestCase):
    """survival L2 失效时 pane_fallback 兜底。"""

    def test_idle_health_pane_alive_is_busy(self):
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text(json.dumps(
            {"survival_overall": "idle"}))
        eng._tmux_pane_alive.return_value = True
        self.assertTrue(eng._is_role_busy("qa"))

    def test_idle_health_no_pane_is_free(self):
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text(json.dumps(
            {"survival_overall": "idle"}))
        eng._tmux_pane_alive.return_value = False
        self.assertFalse(eng._is_role_busy("qa"))

    def test_l2_false_pane_alive_is_busy(self):
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text(json.dumps(
            {"survival_overall": "healthy", "survival_l2_thinking": False}))
        eng._tmux_pane_alive.return_value = True
        self.assertTrue(eng._is_role_busy("qa"))

    def test_no_health_file_degrade_db_check(self):
        eng = _eng()
        eng._lifecycle.query.return_value = []
        self.assertFalse(eng._is_role_busy("qa"))

    def test_pane_fallback_disabled_pane_alive_returns_free(self):
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text(json.dumps(
            {"survival_overall": "idle"}))
        eng._tmux_pane_alive.return_value = True
        self.assertFalse(eng._is_role_busy("qa", pane_fallback=False))


class TestTimeoutQueuedNotIncremented(unittest.TestCase):
    """排队路径：角色忙时递增 queued 而非 timeout_count（防误回收）。"""

    def test_busy_role_increments_queued_only(self):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=True)
        eng._sync_step_results = MagicMock()
        run = _run()
        step = _step(target_role="qa", exit_condition={"timeout_minutes": 1})
        # 手动设置超时场景
        run.step_results["s1"] = {"ts": time.time() - 100}
        with patch.object(eng, "_sync_step_results"):
            eng._tick(run, step)
        sdata = run.step_results["s1"]
        self.assertNotIn("timeout_count", sdata,
            "排队路径不应递增 timeout_count")
        self.assertEqual(sdata.get("queued"), 1,
            "排队路径应递增 queued")


class TestAutoCancelThreshold(unittest.TestCase):
    """超限自动 cancel 阈值 = max(max_retries+3, 4)。"""

    def test_threshold_reached_cancels(self):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        run = _run()
        step = _step(target_role="qa", max_retries=0,
                      exit_condition={"timeout_minutes": 1})
        # timeout_count=3, 阈值=4 → 超限
        run.step_results["s1"] = {"ts": time.time() - 200, "timeout_count": 3}
        eng._tick(run, step)
        eng._lm.close_wf.assert_called_once()
        self.assertEqual(run.step_results["s1"]["timeout_count"], 4)


if __name__ == "__main__":
    unittest.main()
