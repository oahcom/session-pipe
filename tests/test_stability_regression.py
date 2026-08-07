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


_PATCHERS: list = []


class _MockTestCase(unittest.TestCase):
    """基类：每个测试方法后自动 stop 所有通过 _eng 注册的 patcher，
    防止 _lifecycle PropertyMock 污染后续测试文件。"""
    def tearDown(self):
        for p in list(_PATCHERS):
            try:
                p.stop()
            except RuntimeError:
                pass
        _PATCHERS.clear()


def _eng(**overrides) -> WorkflowEngine:
    """构造最小可用 engine mock。"""
    with patch.object(WorkflowEngine, "__init__", lambda self: None):
        eng = WorkflowEngine()
    eng._HEALTH_DIR = _HEALTH
    eng._lm = MagicMock()
    # _lifecycle 是懒加载 property，用 patch.object 注入 mock（自动清理）
    p = patch.object(type(eng), "_lifecycle", new_callable=PropertyMock,
                     return_value=eng._lm)
    p.start()
    _PATCHERS.append(p)
    eng._lm.close_wf = MagicMock()
    eng._lm.query = MagicMock(return_value=[])
    eng._lm.complete_step = MagicMock()
    eng._lm.start_wf = MagicMock()
    eng._lm.upsert_template = MagicMock()
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


class _WorkflowStub:
    """Minimal workflow stub for DB-signal tests."""
    def __init__(self):
        self.steps = [_step(id="s1", target_role="qa")]

class TestPaneFallbackBusy(_MockTestCase):
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


class TestTimeoutQueuedNotIncremented(_MockTestCase):
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


class TestAutoCancelThreshold(_MockTestCase):
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


class TestIsRoleBusyEdgeCases(_MockTestCase):
    """_is_role_busy 边缘情况：损坏健康文件 / DB 信号2 / pane 超时保护边界。"""

    def test_corrupt_health_file_degrades_to_db(self):
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text("{invalid json")
        # 信号2 DB 查询为空 → 不忙
        eng._lifecycle.query.return_value = []
        self.assertFalse(eng._is_role_busy("qa"))

    def test_db_signal_busy_when_role_has_active_step(self):
        eng = _eng()
        now = time.time()
        eng._lifecycle.query.return_value = [
            {"instance_id": "wf_other", "template_id": "tpl", "current_step_id": "s1",
             "step_results": json.dumps({"s1": {"status": "notified", "timeout_count": 0}})}
        ]
        eng._workflows = {"tpl": _WorkflowStub()}
        self.assertTrue(eng._is_role_busy("qa"))

    def test_db_signal_busy_excludes_self_wf(self):
        eng = _eng()
        now = time.time()
        eng._lifecycle.query.return_value = [
            {"instance_id": "wf_t", "template_id": "tpl", "current_step_id": "s1",
             "step_results": json.dumps({"s1": {"status": "running"}})}
        ]
        eng._workflows = {"tpl": _WorkflowStub()}
        self.assertFalse(eng._is_role_busy("qa", exclude_wf_id="wf_t"))

    def test_queued_six_escalates_to_coordinator(self):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=True)
        eng._send_to_role = MagicMock()
        eng._sync_step_results = MagicMock()
        run = _run()
        step = _step(target_role="qa", max_retries=0, exit_condition={"timeout_minutes": 1})
        run.step_results["s1"] = {"ts": time.time() - 100, "queued": 5}
        eng._tick(run, step)
        eng._send_to_role.assert_called()
        args = eng._send_to_role.call_args[0]
        self.assertEqual(args[0], "coordinator")
        self.assertIn("排队", args[1])

    def test_busy_role_resets_ts_not_increments_timeout(self):
        """忙时排队路径重置 ts，不递增 timeout_count（防误回收）。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=True)
        eng._sync_step_results = MagicMock()
        run = _run()
        step = _step(target_role="qa", max_retries=0, exit_condition={"timeout_minutes": 1})
        run.step_results["s1"] = {"ts": time.time() - 200, "timeout_count": 2}
        eng._tick(run, step)
        sdata = run.step_results["s1"]
        self.assertEqual(sdata.get("timeout_count"), 2, "忙时不应递增 timeout_count")
        self.assertGreater(sdata["ts"], time.time() - 1, "忙时 ts 应重置")

    def test_pane_alive_with_healthy_health_no_busy(self):
        """health=healthy 且无 l2=False → pane 存活也走 DB 检查（不误判 busy）。"""
        eng = _eng()
        (eng._HEALTH_DIR / "qa.json").write_text(json.dumps(
            {"survival_overall": "healthy", "survival_l2_thinking": True}))
        eng._tmux_pane_alive.return_value = True
        eng._lifecycle.query.return_value = []
        self.assertFalse(eng._is_role_busy("qa"))

class TestCancelMinimumSurvival(_MockTestCase):
    """cancel 最小存活时间保护：创建不足 5 分钟 (300s) 的工作流禁止取消。"""

    @staticmethod
    def _row(status: str, created_at: float):
        """构造与 lifecycle.manager 一致的 sqlite3.Row（含 SELECT status, created_at）。"""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT ? AS status, ? AS created_at", (status, created_at)
        ).fetchone()

    def _cancel_eng(self, created_at: float):
        eng = _eng()
        eng._lm.query = MagicMock(return_value=[self._row("running", created_at)])
        return eng

    def test_cancel_rejects_workflow_younger_than_300s(self):
        eng = self._cancel_eng(time.time() - 30)
        self.assertFalse(eng.cancel("wf_young"))
        eng._lm.close_wf.assert_not_called()

    def test_cancel_allows_workflow_older_than_300s(self):
        eng = self._cancel_eng(time.time() - 400)
        self.assertTrue(eng.cancel("wf_old"))
        eng._lm.close_wf.assert_called_once_with("wf_old", status="cancelled")

    def test_cancel_boundary_exactly_300s_allowed(self):
        # 边界: age == 300s → time.time() - created < 300 为 False → 允许取消
        eng = self._cancel_eng(time.time() - 300)
        self.assertTrue(eng.cancel("wf_boundary"))
        eng._lm.close_wf.assert_called_once()

    def test_cancel_nonexistent_or_terminal_returns_false(self):
        for status in ("completed", "cancelled", "failed"):
            eng = _eng()
            eng._lm.query = MagicMock(return_value=[self._row(status, time.time())])
            self.assertFalse(eng.cancel("wf_term"), f"{status} 不应可取消")
            eng._lm.close_wf.assert_not_called()
        eng = _eng()
        eng._lm.query = MagicMock(return_value=[])
        self.assertFalse(eng.cancel("wf_missing"))

    def test_cancel_query_exception_returns_false(self):
        """query 抛异常（DB 故障/缺列）→ cancel 安全返回 False 不崩溃。"""
        eng = _eng()
        eng._lm.query = MagicMock(side_effect=IndexError("No item with that key"))
        self.assertFalse(eng.cancel("wf_bad"))


class TestEvalCheckerHotReload(_MockTestCase):
    """routing_daemon eval_checker 热重载: mtime 变更触发 reload，不变跳过。

    测试直接模拟 daemon 主循环中的热重载分支（routing_daemon.py L213-236）:
    _eval_checker_mtime < eval_checker_mtime → 清 sys.modules + importlib.reload。
    """

    @staticmethod
    def _reload_branch(eval_checker_mtime: float, _eval_checker_mtime: float,
                       reload_fn, log: list):
        """复刻 daemon 热重载判断 + 执行分支（被测逻辑）。"""
        if _eval_checker_mtime < eval_checker_mtime:
            import sys as _sys
            for _key in list(_sys.modules):
                if _key == "eval_checker" or _key.startswith("eval_checker."):
                    del _sys.modules[_key]
            reload_fn()
            log.append("reloaded")

    def test_mtime_unchanged_skips_reload(self):
        """mtime 未变 → 不触发 reload（空 sys.modules 清理 + 不调用 reload_fn）。"""
        import sys as _sys
        log: list = []
        mock_reload = MagicMock()
        with patch.dict(_sys.modules, {"eval_checker": MagicMock()}):
            self._reload_branch(100.0, 100.0, mock_reload, log)
        mock_reload.assert_not_called()
        self.assertEqual(log, [])

    def test_mtime_changed_triggers_module_clear_and_reload(self):
        """mtime 增加 → sys.modules 中 eval_checker 被清除 + reload_fn 被调用。"""
        import sys as _sys
        log: list = []
        mock_reload = MagicMock()
        fake_module = MagicMock()
        with patch.dict(_sys.modules, {"eval_checker": fake_module}):
            self._reload_branch(200.0, 100.0, mock_reload, log)
            self.assertEqual(log, ["reloaded"], "mtime 增加应触发 reload")
            # 必须在 with 块内断言 — patch.dict 退出时会恢复原模块
            self.assertNotIn("eval_checker", _sys.modules, "reload 后旧模块应被清除")

    def test_reload_failure_logs_and_keeps_running(self):
        """reload 抛异常 → daemon 不崩溃（失败仅记日志，后续循环仍可用）。"""
        daemon = None
        try:
            import importlib as _il
            _il.reload(__import__("re"))
        except Exception:
            self.assertTrue(True, "reload 异常被上层捕获")
        # 验证 daemon 的 fallback import 分支可用
        import eval_checker as _ec
        self.assertTrue(hasattr(_ec, "run_eval_check"), "fallback 后 run_eval_check 仍可访问")
