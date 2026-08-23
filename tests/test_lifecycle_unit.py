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

    def test_get_assigned_workflows(self):
        self.lm.upsert_template("test_tmpl4", "T4", "desc",
            [{"step_id": "s1", "target_role": "tester",
              "prompt_template": "do", "exit_condition": {"bus_category": "notice"}}])
        self.lm.start_wf("wf_test_4", "s1", "test_tmpl4")
        assigned = self.lm.get_assigned_workflows("tester")
        self.assertIsInstance(assigned, list)

if __name__ == "__main__":
    unittest.main()
