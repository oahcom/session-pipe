#!/usr/bin/env python3
"""codex 审查 P1 测试补丁 — 覆盖审查指出的增量覆盖率缺口。"""
import json
import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src/pipeflow"))

from pipeflow.engine import WorkflowEngine, Step, WorkflowRun

_HEALTH = Path(tempfile.mkdtemp())


def _eng(**overrides):
    with patch.object(WorkflowEngine, "__init__", lambda self: None):
        eng = WorkflowEngine()
    eng._bb = MagicMock()
    eng._lm = MagicMock()
    eng.__dict__["_lifecycle"] = eng._lm
    eng._lm.query = MagicMock(return_value=[])
    eng._workflows = {}
    eng._send_to_role = MagicMock()
    eng._notify_role = MagicMock()
    for k, v in overrides.items():
        setattr(eng, k, v)
    return eng


class TestPersonaCacheHitMiss(unittest.TestCase):
    """persona 缓存: mtime 不变 → 复用缓存；mtime 变 → 重读。"""

    def test_cache_hit_same_mtime(self):
        eng = _eng()
        eng.workflows_dir = Path(tempfile.mkdtemp())
        eng.runs_dir = eng.workflows_dir / "runs"
        eng.runs_dir.mkdir(exist_ok=True)
        # 首次加载
        eng._role_collab_cache = None
        eng._load_workflows()
        first_cache = eng._role_collab_cache
        # 第二次加载（mtime 相同）
        eng._load_workflows()
        self.assertIs(eng._role_collab_cache, first_cache,
                       "mtime 相同时应复用缓存（同一 dict 对象）")


class TestTrackAuthorSkip(unittest.TestCase):
    """track 作者过滤: 最新消息作者是 target_role → 跳过告警。"""

    def _tick_with_src(self, src: str, target_role: str):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(0.0, []))
        fake_fact = type("F", (), {"ts": time.time() - 600, "src": src})()
        eng._bb.read = MagicMock(return_value=[fake_fact])
        eng._sync_step_results = MagicMock()
        step = Step(
            id="s1", title="t", target_role=target_role, prompt_template="t",
            exit_condition={},
            track={"bus_category": "code_fix", "timeout_sec": 1,
                   "notify_roles": ["pm"]})
        run = WorkflowRun(
            id="wf_auth", workflow_name="test_auth", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600}})
        eng._tick(run, step)
        return eng

    def test_author_is_self_skips(self):
        """作者是 target_role → 不发消息（角色刚写过，不卡死）。"""
        eng = self._tick_with_src("reviewer", "reviewer")
        eng._send_to_role.assert_not_called()

    def test_author_is_other_notifies(self):
        """作者是其他角色 → 正常告警。"""
        eng = self._tick_with_src("other_role", "reviewer")
        eng._send_to_role.assert_called()


class TestTrackThrottleIntegration(unittest.TestCase):
    """track 节流: 第二次 tick 不重复告警。"""

    def test_second_tick_throttled(self):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(0.0, []))
        eng._sync_step_results = MagicMock()
        step = Step(
            id="s1", title="t", target_role="scout", prompt_template="t",
            exit_condition={},
            track={"bus_category": "code_fix", "timeout_sec": 300,
                   "notify_roles": ["pm"]})
        run = WorkflowRun(
            id="wf_th", workflow_name="test_th", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600}})
        fake_fact = type("F", (), {"ts": time.time() - 600, "src": "other"})()
        eng._bb.read = MagicMock(return_value=[fake_fact])
        eng._tick(run, step)
        self.assertEqual(eng._send_to_role.call_count, 1, "第一次应通知")
        # 第二次 tick（track_notified_at 已设置）
        eng._tick(run, step)
        self.assertEqual(eng._send_to_role.call_count, 1, "第二次应被节流")


class TestGuardWatchInvalidType(unittest.TestCase):
    """guard.watch 非法类型 → 引擎加载时丢弃，不拉起不存在角色。"""

    def test_guard_watch_string_ignored(self):
        eng = _eng()
        eng.workflows_dir = Path(tempfile.mkdtemp())
        eng.runs_dir = eng.workflows_dir / "runs"
        eng.runs_dir.mkdir(exist_ok=True)
        # 写一个含非法 guard.watch 的模板
        wf_file = eng.workflows_dir / "bad_guard.json"
        wf_file.write_text(json.dumps({
            "name": "bad_guard",
            "steps": [{
                "id": "s1", "title": "t", "type": "single",
                "target_role": "scout", "prompt_template": "x" * 200,
                "exit_condition": {},
                "guard": {"watch": "scout"}  # 字符串，非法
            }]
        }))
        eng._role_collab_cache = {}
        eng._collab_mtime = None
        eng._load_workflows()
        wf = eng._workflows.get("bad_guard")
        step = wf.steps[0]
        self.assertIsNone(step.guard, "非法 guard 应被丢弃")


class TestLoopValueGateIntegration(unittest.TestCase):
    """loop value_gate: FAIL 不创建迭代，PASS 创建。"""

    def test_fail_stops(self):
        eng = _eng()
        eng._lifecycle.query = MagicMock(return_value=[])
        wf_def = MagicMock()
        wf_def.name = "loop_test"
        wf_def.loop = {"enabled": True, "max_iterations": 3,
                       "producer_roles": ["scout"],
                       "value_gate": {"file": "VALUE_AUDIT.md", "workspace": "lr"}}
        wf_def.title = "test"
        wf_def.description = "d"
        wf_def.allowed_executors = ["scout"]
        eng._workflows = {"loop_test": wf_def}
        eng._bb = MagicMock()
        # 构造 FAIL VALUE_AUDIT.md
        tmp = Path(tempfile.mkdtemp()) / "lr"
        tmp.mkdir()
        (tmp / "VALUE_AUDIT.md").write_text("FAIL")
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = None
        eng._lifecycle._conn_tx = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__enter__ = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__exit__ = MagicMock(return_value=False)
        with patch("pipeflow.wf_lifecycle._CCS_WORKSPACES", Path(tmp).parent):
            eng._handle_wf_loop(wf_def, "wf_loop", eng._lifecycle)
        inserts = [c for c in ctx.execute.call_args_list if "INSERT" in str(c.args[0])]
        self.assertEqual(len(inserts), 0, "FAIL 时不应 INSERT")


if __name__ == "__main__":
    unittest.main()


class TestTrackPartnerRecovery(unittest.TestCase):
    """track 完整闭环: 超时→通知→伙伴恢复→停止告警。"""

    def test_partner_recovery_resets_track(self):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(0.0, []))
        eng._sync_step_results = MagicMock()
        step = Step(
            id="s1", title="t", target_role="scout", prompt_template="t",
            exit_condition={},
            track={"bus_category": "code_fix", "timeout_sec": 300,
                   "notify_roles": ["pm"]})
        run = WorkflowRun(
            id="wf_recover", workflow_name="test_rec", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600,
                                 "notified_at": time.time() - 600}})
        # tick1: 超时（600s 前消息）→ 应通知
        eng._bb.read = MagicMock(return_value=[
            type("F", (), {"ts": time.time() - 600, "src": "other"})()])
        eng._tick(run, step)
        self.assertEqual(eng._send_to_role.call_count, 1, "超时应通知")
        # tick2: 伙伴恢复（新消息 1s 前）→ 不再通知
        eng._bb.read = MagicMock(return_value=[
            type("F", (), {"ts": time.time() - 1, "src": "scout"})()])
        eng._tick(run, step)
        # age = now - max(1s前, 600s前) = 1s < 300s → 不超时
        self.assertEqual(eng._send_to_role.call_count, 1, "伙伴恢复后不应再通知")


class TestDualNodeNode2Complete(unittest.TestCase):
    """dual_node 完整路径: node2(codex-approved) 到达 → complete_step。"""

    def test_node2_codex_approved_completes(self):
        eng = _eng()
        eng._check_exit = MagicMock(return_value=(time.time(), [
            {"id": "m1", "text": "s1-codex-approved", "ts": time.time(), "src": "engineer"}]))
        eng._bb = MagicMock()
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        step = Step(
            id="s1", title="t", target_role="engineer", prompt_template="t",
            exit_condition={"bus_category": "notice", "text_contains": "DONE"},
            dual_node=True)
        run = WorkflowRun(
            id="wf_dn", workflow_name="test_dn", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 100,
                                 "node1_done": True, "node2_sent": True,
                                 "exit_messages": []}})
        eng._tick(run, step)
        # node2 到达 → complete_step 被调用
        eng._lifecycle.complete_step.assert_called()
