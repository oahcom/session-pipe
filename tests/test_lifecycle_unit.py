"""Unit tests for lifecycle/manager.py — core state machine logic.
Uses in-memory SQLite, no production DB interference."""
import sys, os, tempfile
from pathlib import Path
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path: sys.path.insert(0, str(_src))

import unittest
from lifecycle.manager import LifecycleManager

class TestLifecycleStateMachine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()
        self.lm = LifecycleManager("test_role", db_path=self.db_path)

    def tearDown(self):
        self.lm.close()
        os.unlink(self.db_path)

    def test_upsert_and_get_wf(self):
        steps = [{"step_id": "s1", "title": "step1", "target_role": "tester",
                  "prompt_template": "do {topic}", "exit_condition": {"bus_category": "notice"}}]
        self.lm.upsert_template("test_tmpl", "测试模板", "desc", steps,
                                trigger_scene=["test"], allowed_initiators=["tester"],
                                allowed_executors=["tester"])
        ok = self.lm.start_wf("wf_test", "s1", "test_tmpl")
        self.assertTrue(ok)
        wf = self.lm.get_wf("wf_test")
        self.assertIsNotNone(wf)
        self.assertEqual(wf["status"], "running")
        self.assertEqual(wf["current_step_id"], "s1")

    def test_step_lifecycle(self):
        self.lm.upsert_template("test_tmpl2", "T2", "desc",
            [{"step_id": "s1", "title": "step1", "target_role": "tester",
              "prompt_template": "do {topic}", "exit_condition": {"bus_category": "notice"}},
             {"step_id": "s2", "title": "step2", "target_role": "verifier",
              "prompt_template": "verify {topic}", "exit_condition": {"bus_category": "code_fix"}}])
        self.lm.start_wf("wf_test_2", "s1", "test_tmpl2")
        r = self.lm.complete_step("wf_test_2", "s1")
        self.assertIn(r, ("completed_and_advanced", "step_done_ready"))
        wf = self.lm.get_wf("wf_test_2")
        self.assertEqual(wf["status"], "running")

    def test_cancel_wf(self):
        self.lm.upsert_template("test_tmpl3", "T3", "desc",
            [{"step_id": "s1", "target_role": "tester",
              "prompt_template": "do", "exit_condition": {"bus_category": "notice"}}])
        self.lm.start_wf("wf_test_3", "s1", "test_tmpl3")
        ok = self.lm.close_wf("wf_test_3", status="cancelled")
        self.assertTrue(ok)
        wf = self.lm.get_wf("wf_test_3")
        self.assertEqual(wf["status"], "cancelled")

    def test_close_wf_records_reason_in_log(self):
        """回收原因必须落库，否则事后 6 条同秒 cancelled 无法归因（#429188）。"""
        self.lm.upsert_template("test_tmpl_r", "TR", "desc",
            [{"step_id": "s1", "target_role": "tester",
              "prompt_template": "do", "exit_condition": {"bus_category": "notice"}}])
        self.lm.start_wf("wf_test_r", "s1", "test_tmpl_r")
        self.lm.close_wf("wf_test_r", status="cancelled", reason="queue 45min >= 30min")
        row = self.lm._conn.execute(
            "SELECT detail FROM workflow_logs WHERE workflow_instance_id=? "
            "AND action='wf_closed'", ("wf_test_r",)).fetchone()
        self.assertEqual(row["detail"], "workflow cancelled (queue 45min >= 30min)")

    def test_review_without_verdict_blocks_advance(self):
        """评审步没写判定词 → 不推进（三态 fail-closed 的角色完成路径）。

        unknown 只装在引擎 tick 路径时，角色走 `wf complete`（manager._advance_unsafe）
        交上来的「已完成，无新动作。」仍被当通过 —— 同一条还原在两条推进路径上
        判定相反，正是 review_gate 模块 docstring 要消灭的那类分歧。
        """
        self.lm.upsert_template("gate_unknown", "GU", "d",
            [{"step_id": "s1", "title": "review", "target_role": "tester",
              "type": "review", "prompt_template": "review {topic}",
              "exit_condition": {"bus_category": "notice"}},
             {"step_id": "s2", "title": "impl", "target_role": "tester",
              "prompt_template": "do {topic}",
              "exit_condition": {"bus_category": "notice"}}])
        self.assertTrue(self.lm.start_wf("wf_unknown_1", "s1", "gate_unknown"))
        self.lm._set_step_result_unsafe(
            "wf_unknown_1", "s1", "completed",
            {"exit_messages": [{"text": "已完成，无新动作。", "src": "tester"}]})
        self.assertFalse(self.lm._advance_unsafe("wf_unknown_1", None),
                         "评审无结论不得推进")
        wf = self.lm.get_wf("wf_unknown_1")
        self.assertEqual(wf["current_step_id"], "s1")
        self.assertEqual(self.lm._parse_results(wf)["s1"]["status"], "step_done_ready")

if __name__ == "__main__":
    unittest.main()
