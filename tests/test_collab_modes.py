#!/usr/bin/env python3
"""四种协作模式端到端验证测试 — guard/track 机制实证。

1. 主从模式: run_once 通过 _lifecycle.query 收集 guard.watch → _ensure_role_alive 被调用
2. 对等模式: track 超时 → 写 monitor_audit + 提醒 notify_roles；节流验证；同分类跳过验证
3. 组合模式: guard + track 同时生效（通过 run_once 真实收集路径）
4. 仲裁模式: 多 watch 角色全部被守护 + loop value_gate

关键: guard 测试通过 mock lifecycle.query 驱动真实引擎收集逻辑，
不复制引擎代码。track 测试调真实 _tick。
"""
import json
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src/pipeflow"))

from pipeflow.engine import WorkflowEngine, Step, WorkflowRun

_HEALTH = Path(__import__("tempfile").mkdtemp())


def _eng(**overrides) -> WorkflowEngine:
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


# ── 主从模式: guard 通过真实引擎收集路径 ──────────────────

class TestMasterSlaveGuard(unittest.TestCase):
    """主从模式: _lifecycle.query 返回含 guard 的 workflow → 引擎收集角色 → ensure_role_alive"""

    def test_guard_watch_roles_collected_via_engine(self):
        """调用真实引擎 run_once 的角色收集逻辑（不复制代码），验证 guard.watch 被正确收集。"""
        eng = _eng()
        eng._ensure_role_alive = MagicMock(return_value=True)
        eng._check_context_overflow = MagicMock()
        eng._kick_stalled_roles = MagicMock()
        eng._cleanup_stale_workflows = MagicMock()

        # 注入一个含 guard 的工作流到引擎
        step = Step(
            id="s2", title="审查", target_role="reviewer", prompt_template="t",
            exit_condition={}, guard={"watch": ["engineer", "qa"],
                                      "auto_restart": True})
        wf_def = MagicMock()
        wf_def.steps = [step]
        wf_def.is_subflow = False
        eng._workflows = {"dev_implement": wf_def}

        # mock lifecycle.query 返回 1 个 running workflow
        eng._lm.query = MagicMock(side_effect=[
            [{"assignee": "pm"}],
            [{"instance_id": "wf_g", "template_id": "dev_implement",
              "current_step_id": "s2"}],
        ])

        # 调用真实引擎的 guard 收集方法（不再复制循环）
        _alive_roles = eng._collect_guard_roles()
        for _role in _alive_roles:
            eng._ensure_role_alive(_role)

        called = {c.args[0] for c in eng._ensure_role_alive.call_args_list}
        self.assertEqual(called, {"reviewer", "engineer", "qa", "pm"})


class TestEnsureRoleAliveFalseBranch(unittest.TestCase):
    """_ensure_role_alive 的死亡分支: tmux 死 → 真拉起。"""

    def test_tmux_dead_then_restarted(self):
        eng = _eng()
        # mock tmux has-session → returncode=1（死亡）
        # mock tmux new-session → returncode=0（拉起成功）
        # mock _is_agent_alive → True（拉起后 claude 存活）
        from unittest.mock import call
        import pipeflow.role_lifecycle as rl
        eng._is_agent_alive = MagicMock(return_value=True)
        eng._detect_code_change = MagicMock(return_value=False)
        with patch.object(rl, "_engine_mod") as mock_em:
            mock_em._sp.run = MagicMock(side_effect=[
                MagicMock(returncode=1),   # has-session: 不存在
                MagicMock(returncode=0),   # start 成功
                MagicMock(returncode=0),   # has-session: 已拉起
            ])
            result = eng._ensure_role_alive("writer")
        self.assertTrue(result)


# ── 对等模式: track 真实触发 ──────────────────

class TestPeerTrack(unittest.TestCase):
    """对等模式: track 超时 → 写审计 + 提醒伙伴。"""

    def _eng_with_track(self, latest_ts: float):
        eng = _eng()
        eng._is_role_busy = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(0.0, []))
        fake_fact = type("F", (), {"ts": latest_ts, "src": "other_role"})()
        eng._bb.read = MagicMock(return_value=[fake_fact])
        eng._lifecycle.complete_step = MagicMock(return_value=None)
        eng._sync_step_results = MagicMock()
        return eng

    def test_track_timeout_writes_audit_and_notifies(self):
        eng = self._eng_with_track(latest_ts=time.time() - 600)
        step = Step(
            id="s1", title="生产", target_role="scout", prompt_template="t",
            exit_condition={},
            track={"bus_category": "evolution_report", "timeout_sec": 300,
                   "notify_roles": ["pm"]})
        run = WorkflowRun(
            id="wf_x", workflow_name="food_chain", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600}})
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        audit_writes = [c.args for c in eng._bb.write.call_args_list
                        if c.args[0] == "monitor_audit"]
        self.assertTrue(audit_writes, "超时应写 monitor_audit")
        self.assertIn("轮次", audit_writes[0][1])
        sent = [c.args for c in eng._send_to_role.call_args_list]
        self.assertTrue(any("pm" in str(c) for c in sent), "应提醒 pm 伙伴")

    def test_track_throttle_no_spam(self):
        """节流: 第二次 tick 在 timeout 窗口内不应再发消息。"""
        eng = self._eng_with_track(latest_ts=time.time() - 600)
        step = Step(
            id="s1", title="生产", target_role="scout", prompt_template="t",
            exit_condition={},
            track={"bus_category": "evolution_report", "timeout_sec": 300,
                   "notify_roles": ["pm"]})
        # 第一次 tick
        run = WorkflowRun(
            id="wf_throttle", workflow_name="test", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600}})
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        first_call_count = eng._send_to_role.call_count
        self.assertEqual(first_call_count, 1, "第一次应提醒")
        # 第二次 tick（节流窗口内，track_notified_at 已设置）
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        second_call_count = eng._send_to_role.call_count
        self.assertEqual(second_call_count, 1, "第二次应被节流，不重复提醒")

    def test_track_same_cat_as_exit_skipped(self):
        """track_cat == exit cat 时跳过 track 消费。"""
        eng = self._eng_with_track(latest_ts=time.time() - 600)
        step = Step(
            id="s1", title="生产", target_role="scout", prompt_template="t",
            exit_condition={"bus_category": "evolution_report"},
            track={"bus_category": "evolution_report", "timeout_sec": 1,
                   "notify_roles": ["pm"]})
        run = WorkflowRun(
            id="wf_same", workflow_name="test", context={},
            current_step="s1", status="running",
            step_results={"s1": {"status": "notified", "ts": time.time() - 600}})
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)
        audit_writes = [c.args for c in eng._bb.write.call_args_list
                        if c.args[0] == "monitor_audit"]
        self.assertFalse(audit_writes, "相同分类时 track 应跳过，不写审计")


# ── 组合模式: guard + track 同时 ──────────────────

class TestCombinedMode(unittest.TestCase):
    """组合模式: guard + track 同一步骤同时生效。"""

    def test_guard_and_track_both_active(self):
        """通过 run_once 收集路径验证 guard + 通过 _tick 验证 track。"""
        eng = _eng()
        eng._ensure_role_alive = MagicMock(return_value=True)
        eng._is_role_busy = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(0.0, []))
        fake_fact = type("F", (), {"ts": time.time() - 600, "src": "other_role"})()
        eng._bb.read = MagicMock(return_value=[fake_fact])
        eng._sync_step_results = MagicMock()
        eng._lifecycle.complete_step = MagicMock()

        step = Step(
            id="s2", title="审查", target_role="reviewer", prompt_template="t",
            exit_condition={},
            guard={"watch": ["engineer"], "auto_restart": True},
            track={"bus_category": "code_review", "timeout_sec": 300,
                   "notify_roles": ["engineer"]})

        # guard: 真实引擎收集路径
        wf_def = MagicMock()
        wf_def.steps = [step]
        eng._workflows = {"dev": wf_def}
        eng._lm.query = MagicMock(side_effect=[
            [{"assignee": "pm"}],
            [{"instance_id": "wf_c", "template_id": "dev", "current_step_id": "s2"}],
        ])
        _alive = eng._collect_guard_roles()
        for r in _alive:
            eng._ensure_role_alive(r)

        # track: 调真实 _tick
        run = WorkflowRun(
            id="wf_c", workflow_name="test", context={},
            current_step="s2", status="running",
            step_results={"s2": {"status": "notified", "ts": time.time() - 600}})
        with patch("pipeflow.step_handler.get_conversation_signal", return_value=None):
            eng._tick(run, step)

        called = {c.args[0] for c in eng._ensure_role_alive.call_args_list}
        self.assertIn("engineer", called, "guard: engineer 被守护")
        audit = [c.args for c in eng._bb.write.call_args_list
                 if c.args[0] == "monitor_audit"]
        self.assertTrue(audit, "track: 超时写审计")


# ── 仲裁模式: 多 watch + loop value_gate ──────────────────

class TestArbiterMultiGuard(unittest.TestCase):
    """仲裁模式: 监控者同时守护多个角色。"""

    def test_multi_watch_all_ensured(self):
        eng = _eng()
        eng._ensure_role_alive = MagicMock(return_value=True)
        step = Step(
            id="s1", title="监控", target_role="coordinator", prompt_template="t",
            exit_condition={},
            guard={"watch": ["pm", "engineer", "qa"], "auto_restart": True})
        wf_def = MagicMock()
        wf_def.steps = [step]
        eng._workflows = {"monitor_wf": wf_def}
        eng._lm.query = MagicMock(side_effect=[
            [{"assignee": "coordinator"}],
            [{"instance_id": "wf_a", "template_id": "monitor_wf", "current_step_id": "s1"}],
        ])
        _alive = eng._collect_guard_roles()
        for r in _alive:
            eng._ensure_role_alive(r)
        called = {c.args[0] for c in eng._ensure_role_alive.call_args_list}
        self.assertEqual(called, {"coordinator", "pm", "engineer", "qa"})


class TestLoopValueGate(unittest.TestCase):
    """仲裁 loop + value_gate: FAIL→停循环 / PASS→新迭代。"""

    def test_value_gate_fail_stops_loop(self):
        """VALUE_AUDIT.md 含 FAIL → 写循环停止 bus，不 INSERT 新迭代。"""
        eng = _eng()
        eng._lifecycle.query = MagicMock(return_value=[])
        eng._bb = MagicMock()
        wf_def = MagicMock()
        wf_def.name = "food_chain_loop"
        wf_def.loop = {"enabled": True, "max_iterations": 3,
                       "producer_roles": ["scout"],
                       "value_gate": {"file": "VALUE_AUDIT.md", "workspace": "lr"}}
        wf_def.title = "食物链"
        wf_def.description = "test"
        wf_def.allowed_executors = ["scout"]
        eng._workflows["food_chain_loop"] = wf_def

        # mock VALUE_AUDIT.md 含 FAIL（临时目录模拟 lr workspace）
        import tempfile, os
        tmp = tempfile.mkdtemp()
        lr_ws = os.path.join(tmp, "lr")
        os.makedirs(lr_ws, exist_ok=True)
        with open(os.path.join(lr_ws, "VALUE_AUDIT.md"), "w") as f:
            f.write("FAIL\n无收益证据")

        # mock _conn_tx：context 查询返回空（首轮），execute 记录调用
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = None  # 无 context → 首轮
        eng._lifecycle._conn_tx = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__enter__ = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__exit__ = MagicMock(return_value=False)

        import pipeflow.wf_lifecycle as wfl
        with patch.object(wfl, "_CCS_WORKSPACES", Path(tmp)):
            eng._handle_wf_loop(wf_def, "wf_loop", eng._lifecycle)

        # FAIL → 不执行 INSERT（conn.execute 只被 SELECT context 调用一次）
        insert_calls = [c for c in ctx.execute.call_args_list
                        if "INSERT" in str(c.args[0])]
        self.assertEqual(insert_calls, [], "FAIL 时不应创建新迭代")
        # 写了循环停止 bus
        wf_writes = [c.args for c in eng._bb.write.call_args_list
                     if c.args[0] == "workflow"]
        self.assertTrue(any("停止" in str(w) or "循环" in str(w) for w in wf_writes),
                        "FAIL 应写循环停止 bus")

    def test_value_gate_pass_creates_iteration(self):
        """VALUE_AUDIT.md 含 PASS → INSERT 新迭代（task + workflow_instance）。"""
        eng = _eng()
        eng._lifecycle.query = MagicMock(return_value=[])
        eng._bb = MagicMock()
        eng._sp = MagicMock()  # mock subprocess（生产者重启，避免真实 ccs stop/start）
        wf_def = MagicMock()
        wf_def.name = "food_chain_loop"
        wf_def.loop = {"enabled": True, "max_iterations": 3,
                       "producer_roles": ["scout"],
                       "value_gate": {"file": "VALUE_AUDIT.md", "workspace": "lr"}}
        wf_def.title = "食物链"
        wf_def.description = "test"
        wf_def.allowed_executors = ["scout"]
        eng._workflows["food_chain_loop"] = wf_def

        import tempfile, os
        tmp = tempfile.mkdtemp()
        lr_ws = os.path.join(tmp, "lr")
        os.makedirs(lr_ws, exist_ok=True)
        with open(os.path.join(lr_ws, "VALUE_AUDIT.md"), "w") as f:
            f.write("PASS\n有实际收益")

        # mock _conn_tx：context 查询返回空（首轮），execute 记录调用
        ctx = MagicMock()
        ctx.execute = MagicMock()
        ctx.execute.return_value.fetchone.return_value = None
        eng._lifecycle._conn_tx = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__enter__ = MagicMock(return_value=ctx)
        eng._lifecycle._conn_tx.return_value.__exit__ = MagicMock(return_value=False)

        import pipeflow.wf_lifecycle as wfl
        with patch.object(wfl, "_CCS_WORKSPACES", Path(tmp)):
            eng._handle_wf_loop(wf_def, "wf_loop", eng._lifecycle)

        # PASS → 应执行 INSERT（新迭代创建）
        insert_calls = [c for c in ctx.execute.call_args_list
                        if "INSERT" in str(c.args[0])]
        self.assertEqual(len(insert_calls), 2,
                         "PASS 时应 INSERT task + workflow_instance（2 次）")


class TestWaitRolesValidation(unittest.TestCase):
    """wait_roles schema 校验：非法类型丢弃，合法 list 保留。"""

    _GOOD_STEP = {
        "id": "s1", "title": "t", "type": "single",
        "target_role": "qa",
        "prompt_template": ("## 目标\n执行测试验证并记录结果，确保所有关键场景覆盖，"
                             "输出格式符合团队规范要求。详细列出每个测试用例的执行步骤和预期结果，"
                             "包括边界情况和异常路径的处理验证，并在报告中注明覆盖范围与遗漏风险，"
                             "完成后将结果归档。\n\n## 完成\n"
                             "写 bus cat=test_report，标题含 s1-complete。"),
        "exit_condition": {"bus_category": "test_report"},
    }

    @staticmethod
    def _load_tmpl(eng, name, wait_roles):
        eng.workflows_dir = Path(__import__("tempfile").mkdtemp())
        eng.runs_dir = eng.workflows_dir / "runs"
        eng.runs_dir.mkdir(exist_ok=True)
        step = dict(TestWaitRolesValidation._GOOD_STEP)
        step["wait_roles"] = wait_roles
        eng.workflows_dir.joinpath(f"{name}.json").write_text(json.dumps({
            "name": name,
            "trigger_scene": ["test"],
            "allowed_initiators": ["pm"],
            "allowed_executors": ["qa"],
            "max_duration_hours": 1,
            "quality_standards": "模板质量校验通过的测试数据",
            "steps": [step]
        }))
        eng._role_collab_cache = {}
        eng._collab_mtime = None
        eng._load_workflows()
        return eng._workflows[name].steps[0].wait_roles

    def test_string_type_dropped(self):
        eng = _eng()
        self.assertIsNone(self._load_tmpl(eng, "bad1", "qa"))

    def test_int_type_dropped(self):
        eng = _eng()
        self.assertIsNone(self._load_tmpl(eng, "bad2", 42))

    def test_dict_type_dropped(self):
        eng = _eng()
        self.assertIsNone(self._load_tmpl(eng, "bad3", {"qa": True}))

    def test_list_of_str_kept(self):
        eng = _eng()
        self.assertEqual(self._load_tmpl(eng, "ok1", ["qa", "reviewer"]),
                         ["qa", "reviewer"])

    def test_empty_list_dropped_with_warning(self):
        """空 wait_roles 列表（语义=无门控）→ 丢弃并置 None（防误写空列表）。"""
        eng = _eng()
        self.assertIsNone(self._load_tmpl(eng, "ok2", []))


if __name__ == "__main__":
    unittest.main()