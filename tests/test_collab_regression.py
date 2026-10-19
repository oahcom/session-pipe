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

# 模块级单例 patch（_eng() 每次重用时先 stop 旧的，防栈式累积泄漏）
_GS_PATCH: "unittest.mock._patch" = None


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
    # 测试不依赖真实 tmux 信号（conversation_monitor 读取真实 pane 会干扰 _tick 流程）
    # 模块级单例 patch: 每次 _eng() 前先 stop 旧 patch，防止栈式累积泄漏
    # （全量测试 flaky 根因: 多次 start 后 get_conversation_signal 意外失效 → 真实 tmux 调用）
    global _GS_PATCH
    if _GS_PATCH is not None:
        _GS_PATCH.stop()
    _GS_PATCH = patch("pipeflow.step_handler.get_conversation_signal", return_value=None)
    _GS_PATCH.start()
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
            "trigger_scene": ["test"],
            "allowed_initiators": ["pm"],
            "allowed_executors": ["scout"],
            "max_duration_hours": 1,
            "quality_standards": "模板质量校验通过的测试数据",
            "steps": [{
                "id": "s1", "title": "t", "type": "single",
                "target_role": "scout",
                "prompt_template": ("## 目标\n执行代码扫描并记录发现，"
                    "输出格式符合团队规范要求。详细列出每个扫描维度，"
                    "包括边界情况和异常路径的处理验证。\n\n## 完成\n"
                    "写 bus cat=code_fix，标题含 s1-complete。"),
                "exit_condition": {"bus_category": "code_fix"},
                "guard": {"watch": "scout"}  # 字符串，非法
            }]
        }))
        eng._role_collab_cache = {}
        eng._collab_mtime = None
        eng._load_workflows()
        wf = eng._workflows.get("bad_guard")
        self.assertIsNotNone(wf, "非法 guard 不应导致整份模板被拒")
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


class TestDualNodeWaitRolesGateRescue(unittest.TestCase):
    """dual_node 超时抢救不绕过 wait_roles 门（codex P0：协作门被跳过）。"""

    def test_rescue_with_wait_roles_missing_role_blocks(self):
        """wait_roles 中有角色未产出 → dual_node 超时抢救也不 complete。"""
        eng = _eng()
        # 只有 engineer 写消息，qa/reviewer 都没写 → 门不过
        eng._check_exit = MagicMock(return_value=(0.0, []))
        eng._bb = MagicMock()
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        eng._lifecycle.complete_step = MagicMock()
        step = Step(
            id="s1", title="t", target_role="engineer", prompt_template="t",
            exit_condition={"bus_category": "notice", "text_contains": "DONE",
                            "timeout_minutes": 1},
            dual_node=True, wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_dnw", workflow_name="test_dnw", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600,
                                 "node1_done": True, "node2_sent": True,
                                 "timeout_count": 1, "exit_messages": []}})
        eng._tick(run, step)
        eng._lifecycle.complete_step.assert_not_called()

    def test_rescue_with_wait_roles_all_roles_advances(self):
        """wait_roles 全部产出 → dual_node 超时抢救正常 complete。"""
        eng = _eng()
        _ts = time.time() - 100
        eng._check_exit = MagicMock(return_value=(_ts, [
            {"id": "m1", "text": "s1-codex-approved", "ts": _ts, "src": "engineer"},
            {"id": "m2", "text": "s1-codex-approved", "ts": _ts, "src": "qa"},
            {"id": "m3", "text": "s1-codex-approved", "ts": _ts, "src": "reviewer"}]))
        eng._bb = MagicMock()
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        step = Step(
            id="s1", title="t", target_role="engineer", prompt_template="t",
            exit_condition={"bus_category": "notice", "text_contains": "DONE",
                            "timeout_minutes": 1},
            dual_node=True, wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_dnw2", workflow_name="test_dnw2", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600,
                                 "node1_done": True, "node2_sent": True,
                                 "timeout_count": 1, "exit_messages": []}})
        eng._tick(run, step)
        eng._lifecycle.complete_step.assert_called()


class TestCollectGuardRolesIncludesWaitRoles(unittest.TestCase):
    """_collect_guard_roles 收集 wait_roles 角色（防止协作门角色不存活）。"""

    def test_wait_roles_collected(self):
        eng = _eng()
        wf_def = MagicMock()
        wf_def.name = "gw"
        s1 = Step(id="s1", title="t", target_role="engineer", prompt_template="t",
                  exit_condition={}, wait_roles=["qa", "reviewer"])
        wf_def.steps = [s1]
        eng._workflows = {"gw": wf_def}
        eng._lifecycle.query = MagicMock(side_effect=[
            [],  # assignee 查询
            [{"instance_id": "wf1", "template_id": "gw", "current_step_id": "s1"}],
        ])
        roles = eng._collect_guard_roles()
        self.assertIn("engineer", roles, "target_role 应被收集")
        self.assertIn("qa", roles, "wait_roles 角色应被收集")
        self.assertIn("reviewer", roles, "wait_roles 角色应被收集")


class TestWaitRolesCollaborationGate(unittest.TestCase):
    """协作门 wait_roles: 多角色协作 — 等所有角色产出才推进。"""

    def setUp(self):
        # mock conversation_monitor：避免真实 tmux 信号干扰 wait_roles 测试
        # 复用模块级 _GS_PATCH（_eng() 已 start），此处不重复 start
        pass

    def tearDown(self):
        # 模块级 patch 由下次 _eng() 统一 stop，这里不单独 stop（防双重 stop）
        pass

    def _tick_with_wait_roles(self, bus_msgs):
        """构造步骤含 wait_roles，bus 返回指定消息列表，测试 _tick 行为。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        lm = MagicMock()
        eng._lm = lm
        eng.__dict__["_lifecycle"] = lm  # 覆盖 _eng() 里缓存的旧 _lifecycle 引用
        fake_facts = [type("F", (), {"id": f"id_{i}", "t": f["text"],
                                      "ts": time.time(), "src": f["src"]})()
                      for i, f in enumerate(bus_msgs)]
        eng._bb.read = MagicMock(return_value=fake_facts)

        step = Step(
            id="s3", title="集成", target_role="closer", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60},
            wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_wait", workflow_name="wait_test", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 5,
                                 "notified_at": time.time() - 1000}})
        # patch 覆盖 _tick 执行期间，防止真实 tmux capture-pane 干扰
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        return eng, run, lm

    def test_all_roles_matched_advances(self):
        """qa 和 reviewer 都写了 test_report → complete_step 被调（协作门通过）。"""
        eng, run, lm = self._tick_with_wait_roles([
            {"text": "s5-complete", "ts": time.time() - 10, "src": "qa"},
            {"text": "s6-complete", "ts": time.time() - 5, "src": "reviewer"},
        ])
        # wait_roles 门通过 → exit 匹配 → complete_step
        self.assertTrue(run.step_results["s3"].get("exit_messages"),
                        "wait_roles 门通过后应记录 exit_messages")
        lm.complete_step.assert_called()

    def test_one_role_missing_blocks(self):
        """只有 qa 写了 test_report，reviewer 没写 → 不推进。"""
        eng, run, lm = self._tick_with_wait_roles([
            {"text": "s5-complete", "ts": time.time() - 10, "src": "qa"},
        ])
        self.assertFalse(run.step_results["s3"].get("exit_messages"),
                         "reviewer 未产出，不应记录 exit_messages")
        lm.complete_step.assert_not_called()

    def test_empty_bus_blocks(self):
        """bus 完全空 → 不推进。"""
        eng, run, lm = self._tick_with_wait_roles([])
        self.assertFalse(run.step_results["s3"].get("exit_messages"),
                         "bus 空，不应记录 exit_messages")
        lm.complete_step.assert_not_called()

    def test_no_wait_roles_advances_normally(self):
        """无 wait_roles → 单角色正常推进（回归保护）。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        eng._lifecycle.complete_step = MagicMock()
        eng._bb.read = MagicMock(return_value=[
            type("F", (), {"id": "m1", "t": "s3-complete",
                           "ts": time.time() - 1, "src": "qa"})()])
        step = Step(
            id="s3", title="t", target_role="qa", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60})
        run = WorkflowRun(
            id="wf_norm", workflow_name="norm_test", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 600,
                                 "notified_at": time.time() - 1000}})
        eng._tick(run, step)
        eng._lifecycle.complete_step.assert_called()

    def test_wait_roles_supplement_after_window_overflow(self):
        """主查询 50 条窗口滑掉协作消息 → per-role 补查命中 → 门通过（窗口滚动）。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        eng._lifecycle.complete_step = MagicMock()
        _old = time.time() - 600  # 早于 created_after 窗口的消息（被主查询过滤？）
        # 模拟：主查询(limit=50)返回 qa 消息（在窗口内），reviewer 消息在更早的 200 条窗口
        # read 第一次调用 → 主查询结果（仅 qa）；第二次调用(limit=200) → 补查命中 reviewer
        eng._bb.read = MagicMock(side_effect=[
            # 主查询（limit=50）：qa 消息在窗口内
            [type("F", (), {"id": "m1", "t": "s3-complete",
                            "ts": time.time() - 10, "src": "qa"})()],
            # 补查 reviewer（limit=200）：reviewer 消息滑出主窗口但补查可见
            [type("F", (), {"id": "m2", "t": "s3-complete",
                            "ts": _old, "src": "reviewer"})()],
        ])
        # target 是 closer，wait_roles=[qa, reviewer]
        step = Step(
            id="s3", title="t", target_role="closer", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60},
            wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_sup", workflow_name="sup_test", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 600,
                                 "notified_at": time.time() - 1000}})
        eng._tick(run, step)
        # 补查命中 reviewer → 门通过 → complete_step
        eng._lifecycle.complete_step.assert_called()

    def test_wait_roles_supplement_missing_role_blocks(self):
        """补查仍找不到角色消息 → 门不过。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        eng._lifecycle.complete_step = MagicMock()
        eng._bb.read = MagicMock(side_effect=[
            # 主查询：qa 消息在窗口内
            [type("F", (), {"id": "m1", "t": "s3-complete",
                            "ts": time.time() - 10, "src": "qa"})()],
            # 补查 reviewer：空（reviewer 没写）
            [],
        ])
        step = Step(
            id="s3", title="t", target_role="closer", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60},
            wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_sup2", workflow_name="sup_test2", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 600,
                                 "notified_at": time.time() - 1000}})
        eng._tick(run, step)
        eng._lifecycle.complete_step.assert_not_called()

    def test_source_contains_with_wait_roles_passes(self):
        """source_contains + wait_roles 同设：协作角色消息不被 src 过滤（死锁修复）。"""
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        eng._lifecycle.complete_step = MagicMock()
        # target 是 engineer(source_contains=engineer)，但 wait_roles=qa/reviewer
        # qa 和 reviewer 的消息 src 不是 engineer → 修复前被 src 过滤 → 死锁
        eng._bb.read = MagicMock(return_value=[
            type("F", (), {"id": "m1", "t": "s3-complete",
                           "ts": time.time() - 10, "src": "engineer"})(),
            type("F", (), {"id": "m2", "t": "s3-complete",
                           "ts": time.time() - 5, "src": "qa"})(),
            type("F", (), {"id": "m3", "t": "s3-complete",
                           "ts": time.time() - 1, "src": "reviewer"})()])
        step = Step(
            id="s3", title="t", target_role="engineer", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60,
                            "source_contains": "engineer"},
            wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_src", workflow_name="src_test", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 600,
                                 "notified_at": time.time() - 1000}})
        eng._tick(run, step)
        eng._lifecycle.complete_step.assert_called()


class TestWaitRolesRequeryExactSrc(unittest.TestCase):
    """re-query 补查：只接受目标角色精确 src，claude-src 消息不得满足缺失角色
    （codex 9 问题审查 P2：单条 claude 消息曾可满足所有 wait_roles，削弱协作门）。"""

    def _tick(self, bus_msgs):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._sync_step_results = MagicMock()
        eng._consumed_exit_ids = set()
        lm = MagicMock()
        eng._lm = lm
        eng.__dict__["_lifecycle"] = lm
        # 主循环 50 条窗口内只有 qa 的产出 → reviewer 需走 re-query 补查
        eng._bb.read = MagicMock(return_value=[
            type("F", (), {"id": f"id_{i}", "t": m["text"],
                           "ts": time.time() - 5, "src": m["src"]})()
            for i, m in enumerate(bus_msgs)])
        step = Step(
            id="s3", title="集成", target_role="closer", prompt_template="t",
            exit_condition={"bus_category": "test_report", "timeout_minutes": 60},
            wait_roles=["qa", "reviewer"])
        run = WorkflowRun(
            id="wf_requery", workflow_name="requery_test", context={},
            current_step="s3", status="running", created_at=time.time() - 1000,
            step_results={"s3": {"status": "notified", "ts": time.time() - 5,
                                 "notified_at": time.time() - 1000}})
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        return eng, lm

    def test_claude_src_does_not_satisfy_missing_role(self):
        """reviewer 未产出，只有一条 claude-src 消息 → 不推进（re-query 精确匹配）。"""
        eng, lm = self._tick([
            {"text": "s5-complete", "src": "qa"},
            {"text": "s6-complete", "src": "claude"},
        ])
        lm.complete_step.assert_not_called()

    def test_exact_role_src_satisfies_requery(self):
        """reviewer 写了 test_report（主窗口外）→ re-query 补查到 → 推进。"""
        eng, lm = self._tick([
            {"text": "s5-complete", "src": "qa"},
            {"text": "s6-complete", "src": "reviewer"},
        ])
        lm.complete_step.assert_called()


if __name__ == "__main__":
    unittest.main()
