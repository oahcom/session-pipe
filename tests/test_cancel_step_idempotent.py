"""cancel_step 的幂等与校验顺序（codex 三轮审查 P2#5/#14/#20）。

生产证据：wf_ae31fd1c4c11 一条链 31.3 分钟内落 49 行 step_cancelled，中位间隔
10.7s —— 正好一 tick 一行，因为调用方每 tick 重算 idle_count>=5 就会再调一次。
"""
import sys, os, sqlite3, tempfile
from pathlib import Path
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path: sys.path.insert(0, str(_src))

import unittest
from lifecycle.manager import LifecycleManager


class TestCancelStepIdempotent(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()
        self.lm = LifecycleManager("test_role", db_path=self.db_path)
        self.lm.upsert_template("t_idem", "T", "desc",
            [{"step_id": "s1", "title": "step1", "target_role": "tester",
              "prompt_template": "do {topic}", "exit_condition": {"bus_category": "notice"}}])
        self.lm.start_wf("wf_idem", "s1", "t_idem")

    def tearDown(self):
        self.lm.close()
        os.unlink(self.db_path)

    def _cancel_rows(self):
        c = sqlite3.connect(self.db_path)
        try:
            return c.execute("SELECT COUNT(*) FROM workflow_logs WHERE action='step_cancelled'"
                             " AND workflow_instance_id='wf_idem'").fetchone()[0]
        finally:
            c.close()

    def test_second_cancel_is_noop_and_logs_once(self):
        self.assertTrue(self.lm.cancel_step("wf_idem", "s1"), "首次取消必须生效")
        self.assertFalse(self.lm.cancel_step("wf_idem", "s1"), "重复取消必须返回 False")
        self.assertFalse(self.lm.cancel_step("wf_idem", "s1"), "第三次仍须 False")
        self.assertEqual(self._cancel_rows(), 1, "重复取消不得再落 step_cancelled 日志")

    def test_wrong_step_still_raises_after_cancel(self):
        self.lm.cancel_step("wf_idem", "s1")
        with self.assertRaises(ValueError):
            self.lm.cancel_step("wf_idem", "s9")

    def test_other_terminal_states_are_not_reopened(self):
        """completed/failed 的实例再调 cancel_step 不得被改回 cancelled。"""
        for status in ("completed", "failed"):
            with self.subTest(status=status):
                self.lm.start_wf(f"wf_term_{status}", "s1", "t_idem")
                self.lm.close_wf(f"wf_term_{status}", status=status)
                self.assertFalse(self.lm.cancel_step(f"wf_term_{status}", "s1"),
                                 f"{status} 实例不应被重新取消")
                wf = self.lm.get_wf(f"wf_term_{status}")
                self.assertEqual(wf["status"], status, f"{status} 状态不得被覆写")
