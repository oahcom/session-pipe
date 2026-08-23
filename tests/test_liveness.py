"""liveness.py — 统合存活/忙检测模块测试。"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from liveness import (
    _db_fetch, _health_gate, _load_steps_json, _row_get,
    _s0_alive, _s1_thinking, _s2_producing, _s3_assigned, _s4_stepping,
    _tmux_has_session, role_liveness, tmux_pane_active,
)


class TestRowGet(unittest.TestCase):
    def test_dict_row(self):
        row = {"instance_id": "wf_1", "template_id": "tpl"}
        self.assertEqual(_row_get(row, "instance_id", 0), "wf_1")
        self.assertIsNone(_row_get(row, "missing", 5))

    def test_tuple_row(self):
        row = ("wf_1", "tpl")
        self.assertEqual(_row_get(row, "instance_id", 0), "wf_1")

    def test_index_out_of_range(self):
        self.assertIsNone(_row_get(("only_one",), "missing", 5))


class TestDbFetch(unittest.TestCase):
    def test_raw_connection(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE t(id TEXT)")
        conn.execute("INSERT INTO t VALUES('a')")
        rows = _db_fetch(conn, "SELECT id FROM t")
        self.assertEqual(rows[0][0], "a")
        conn.close()

    def test_lifecycle_manager_mock(self):
        class FakeLM:
            def query(self, sql, params=()):
                return [{"id": "x"}]
        rows = _db_fetch(FakeLM(), "SELECT id FROM t")
        self.assertEqual(rows[0]["id"], "x")


class TestHealthGate(unittest.TestCase):
    def setUp(self):
        self._hd = Path(tempfile.mkdtemp())

    def _write(self, role, data):
        (self._hd / f"{role}.json").write_text(json.dumps(data))

    @patch("liveness.tmux_pane_active", return_value=True)
    def test_orphan_returns_busy_false(self, mock_tpa):
        self._write("qa", {"survival_overall": "orphan"})
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertFalse(gate)

    @patch("liveness.tmux_pane_active", return_value=True)
    def test_stale_pane_active_returns_busy_true(self, mock_tpa):
        self._write("qa", {"survival_overall": "stale"})
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertTrue(gate)

    @patch("liveness.tmux_pane_active", return_value=False)
    def test_stale_pane_inactive_returns_busy_false(self, mock_tpa):
        self._write("qa", {"survival_overall": "stale"})
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertFalse(gate)

    @patch("liveness.tmux_pane_active", return_value=True)
    def test_l2_false_pane_active_returns_busy_true(self, mock_tpa):
        self._write("qa", {"survival_overall": "healthy", "survival_l2_thinking": False})
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertTrue(gate)

    def test_idle_continues_to_db(self):
        self._write("qa", {"survival_overall": "idle"})
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertIsNone(gate)

    def test_no_health_file_continues(self):
        gate, h = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertIsNone(gate)


class TestL3Assigned(unittest.TestCase):
    def _conn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE workflow_instances(instance_id TEXT, assignee TEXT, status TEXT, template_id TEXT)")
        return conn

    def test_assigned_when_pending(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','qa','pending','tpl')")
        s = _s3_assigned("qa", conn)
        self.assertTrue(s["assigned"])
        conn.close()

    def test_not_assigned_when_completed(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','qa','completed','tpl')")
        s = _s3_assigned("qa", conn)
        self.assertFalse(s["assigned"])
        conn.close()

    def test_no_db_returns_unavailable(self):
        s = _s3_assigned("qa", None)
        self.assertFalse(s["available"])


class TestL4Stepping(unittest.TestCase):
    def _conn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE workflow_instances(instance_id TEXT, template_id TEXT, step_results TEXT, assigner TEXT, parent_wf_id TEXT, status TEXT)")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        conn.execute("INSERT INTO workflow_templates('template_id','steps_json') VALUES('tpl',?)",
                      (json.dumps({"steps": [{"id": "s1", "target_role": "qa"}]}),))
        return conn

    def test_busy_when_step_running(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn)
        self.assertTrue(s["busy"])
        conn.close()

    def test_not_busy_when_excluded(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running"}}),))
        s = _s4_stepping("qa", conn, exclude_wf_id="wf_1")
        self.assertFalse(s["busy"])
        conn.close()

    def test_not_busy_when_timeout_ge_3(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 3}}),))
        s = _s4_stepping("qa", conn)
        self.assertFalse(s["busy"])
        conn.close()

    def test_queued_6_exempt_same_wf(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "queued": 6, "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn, exclude_wf_id="wf_1")
        self.assertFalse(s["busy"])
        conn.close()

    def test_subflow_exempt(self):
        """wf_sub_ 子链（parent 是当前检测者）→ 豁免不忙。"""
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_sub_x','tpl',?,'workflow_engine','wf_main','running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn, exclude_wf_id="wf_main")
        self.assertFalse(s["busy"])
        conn.close()

    def test_subflow_not_exempt_for_other_parent(self):
        """wf_sub_ 子链（parent 是其他工作流）→ 不豁免，判忙。"""
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_sub_x','tpl',?,'workflow_engine','wf_other_parent','running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn, exclude_wf_id="wf_main")
        self.assertTrue(s["busy"])
        conn.close()

    def test_no_db_returns_unavailable(self):
        s = _s4_stepping("qa", None)
        self.assertFalse(s["busy"])

    def test_truncated_scan_returns_not_busy(self):
        """>40 个 running 实例 → 截断，不判忙（安全方向，review P2 补测）。"""
        conn = self._conn()
        for i in range(41):
            conn.execute("INSERT INTO workflow_instances VALUES(?, 'tpl', ?, 'engineer', NULL, 'running')",
                          (f"wf_{i}", json.dumps({"s1": {"status": "running", "timeout_count": 0}})))
        s = _s4_stepping("qa", conn)
        self.assertFalse(s["busy"])
        self.assertTrue(s.get("truncated"))
        conn.close()

    def test_list_format_steps_json(self):
        """steps_json 为 list 格式（真实 DB 生产格式）→ 正确读取步骤定义。"""
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE workflow_instances(instance_id TEXT, template_id TEXT, step_results TEXT, assigner TEXT, parent_wf_id TEXT, status TEXT)")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        # list 格式：直接是步骤数组（非 {"steps": [...]} 包装）
        conn.execute("INSERT INTO workflow_templates VALUES('tpl',?)",
                      (json.dumps([{"id": "s1", "target_role": "qa"}]),))
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn)
        self.assertTrue(s["busy"], "list 格式 steps_json 应被正确解析并判忙")
        conn.close()

    def test_load_steps_json_direct(self):
        """直接测试 _load_steps_json 的 list/dict/损坏格式兼容。"""
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        # list 格式
        conn.execute("INSERT INTO workflow_templates VALUES('list_tpl',?)",
                      (json.dumps([{"id": "s1", "target_role": "qa"}]),))
        self.assertEqual(len(_load_steps_json("list_tpl", conn)), 1)
        # dict 格式
        conn.execute("INSERT INTO workflow_templates VALUES('dict_tpl',?)",
                      (json.dumps({"steps": [{"id": "s1", "target_role": "qa"}]}),))
        self.assertEqual(len(_load_steps_json("dict_tpl", conn)), 1)
        # 不存在
        self.assertIsNone(_load_steps_json("missing", conn))
        # 损坏
        conn.execute("INSERT INTO workflow_templates VALUES('bad','{json!')")
        self.assertIsNone(_load_steps_json("bad", conn))
        conn.close()

    def test_corrupt_steps_json_returns_not_busy(self):
        """DB steps_json 损坏 → 无法解析 → 跳过（review P2 补测）。"""
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE workflow_instances(instance_id TEXT, template_id TEXT, step_results TEXT, assigner TEXT, parent_wf_id TEXT, status TEXT)")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        conn.execute("INSERT INTO workflow_templates VALUES('tpl','{invalid json')")
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running"}}),))
        s = _s4_stepping("qa", conn)
        self.assertFalse(s["busy"])
        conn.close()

    def test_workflows_param_bypasses_db(self):
        """workflows 参数：直接用内存 WorkflowDef，不查 DB steps_json。"""
        class Step:
            def __init__(self, id, target_role):
                self.id = id
                self.target_role = target_role
        class WFDef:
            def __init__(self): self.steps = [Step("s1", "qa")]
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn, workflows={"tpl": WFDef()})
        self.assertTrue(s["busy"])
        conn.close()

    def test_cache_miss_falls_back_to_db(self):
        conn = self._conn()
        conn.execute("INSERT INTO workflow_templates VALUES('tpl2',"
                     "?)", (json.dumps([{"id": "s1", "target_role": "qa"}]),))
        conn.execute("INSERT INTO workflow_instances VALUES('wf_2','tpl2',?,'engineer',NULL,'running')",
                     (json.dumps({"s1": {"status": "running", "timeout_count": 0}}),))
        # workflows 已提供但 tpl2 不在其中 → 必须回退 DB 加载 steps，否则漏判 busy
        s = _s4_stepping("qa", conn, workflows={"other": object()})
        self.assertTrue(s["busy"])
        conn.close()


class TestRoleLiveness(unittest.TestCase):
    def setUp(self):
        self._hd = Path(tempfile.mkdtemp())

    @patch("liveness._tmux_has_session", return_value=True)
    def test_no_health_no_db_returns_not_busy(self, mock_tmux):
        res = role_liveness("qa", level=4, db_conn=sqlite3.connect(":memory:"),
                            health_dir=self._hd)
        self.assertFalse(res["busy"])

    def test_degradation_no_db(self):
        res = role_liveness("qa", level=4, db_conn=None, health_dir=self._hd)
        self.assertFalse(res["busy"])

    def test_level1_thinking_busy(self):
        (self._hd / "qa.json").write_text(json.dumps(
            {"survival_overall": "healthy", "survival_l2_thinking": True}))
        res = role_liveness("qa", level=1, health_dir=self._hd)
        self.assertTrue(res["busy"])
        self.assertIn(1, res["signals"])


if __name__ == "__main__":
    unittest.main()
