"""liveness.py — 统合存活/忙检测模块测试。"""
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from liveness import (
    PANE_ACTIVE_WINDOW, _STALE_NOTIFIED_SEC,
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

    def test_stale_notified_not_busy(self):
        """陈旧 notified（投递后长期零响应）不计占用 —— 防两条链互为"忙"饿死。

        实测 wf_sub_4e63/wf_sub_b548 各卡一个 pg/s1 notified 110 分钟，
        双方互判 busy，kick-stalled 每 10 分钟触发却直接 continue。
        """
        conn = self._conn()
        stale = time.time() - _STALE_NOTIFIED_SEC - 60
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "notified", "notified_at": stale,
                                          "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn)
        self.assertFalse(s["busy"])
        conn.close()

    def test_fresh_notified_still_busy(self):
        """新鲜 notified 仍算占用（角色可能正在思考，不该被抢）。"""
        conn = self._conn()
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "notified",
                                          "notified_at": time.time() - 5,
                                          "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn)
        self.assertTrue(s["busy"])
        conn.close()

    def test_stale_running_still_busy(self):
        """running 不受陈旧豁免：它是角色主动写的执行中状态。"""
        conn = self._conn()
        stale = time.time() - _STALE_NOTIFIED_SEC - 60
        conn.execute("INSERT INTO workflow_instances VALUES('wf_1','tpl',?,'engineer',NULL,'running')",
                      (json.dumps({"s1": {"status": "running", "notified_at": stale,
                                          "timeout_count": 0}}),))
        s = _s4_stepping("qa", conn)
        self.assertTrue(s["busy"])
        conn.close()

    def test_no_db_returns_unavailable(self):
        s = _s4_stepping("qa", None)
        self.assertFalse(s["busy"])

    def test_high_instance_count_still_detects_busy(self):
        """>40 个 running 实例不再截断放行：生产实测 82~173 个实例常驻，
        旧截断会把所有角色判为不忙 → 引擎继续投递 → 积压自我放大。"""
        conn = self._conn()
        for i in range(60):
            conn.execute("INSERT INTO workflow_instances VALUES(?, 'tpl', ?, 'engineer', NULL, 'running')",
                          (f"wf_{i}", json.dumps({"s1": {"status": "running", "timeout_count": 0}})))
        s_busy = _s4_stepping("qa", conn)
        self.assertTrue(s_busy["busy"], "高实例数下仍应按步骤 target_role 判忙")
        self.assertNotIn("truncated", s_busy, "截断键已删除，不再返回")
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


class TestTmuxPaneActiveRegression(unittest.TestCase):
    """回归守卫：tmux_pane_active 必须读活动时间戳，不得读焦点标志。

    2026-10-17 P0：原实现读 #{pane_active}（焦点 0/1），单 pane 会话下恒为 1 →
    恒 True → stale/dead 兜底永远成立 → 空闲角色被判忙 → 任务排队 → 饿死 → 回收，
    全链停摆（bus #395128/#395235/#395251）。
    """

    def _run_stdout(self, stdout):
        fake = MagicMock(returncode=0, stdout=stdout)
        with patch("liveness.subprocess.run", return_value=fake) as m:
            result = tmux_pane_active("qa", window=300)
        return result, m

    def test_reads_window_activity_not_pane_active(self):
        """必须查 #{window_activity}；查 #{pane_active} 则此断言失败。"""
        _, mock = self._run_stdout(str(int(time.time())) + "\n")
        cmd = mock.call_args[0][0]
        self.assertIn("#{window_activity}", cmd[-1],
                      "必须用活动时间戳，焦点标志 pane_active 恒 1 会永远判忙")
        self.assertNotIn("#{pane_active}", cmd[-1])

    def test_recent_activity_is_true(self):
        result, _ = self._run_stdout(str(int(time.time()) - 10) + "\n")
        self.assertIs(result, True, "10s 前有输出应判活跃")

    def test_silent_beyond_window_is_false(self):
        result, _ = self._run_stdout(str(int(time.time()) - 5000) + "\n")
        self.assertIs(result, False, "静默超窗应判不活跃")

    def test_silent_but_cli_busy_is_true(self):
        """长工具调用（codex review）静默 8~12min，但 CLI 仍在跑 → 不得判死亡。

        2026-10-19 P0：只按 window_activity 判活，reviewer 跑 codex review 时
        被判 False → 条件4 auto-cancel 杀掉 wf_sub_8de33/wf_sub_fb975。
        """
        ts = MagicMock(returncode=0, stdout=str(int(time.time()) - 5000) + "\n")
        busy = MagicMock(returncode=0, stdout="⏵⏵ bypass permissions on · esc to interrupt")
        with patch("liveness.subprocess.run", side_effect=[ts, busy]):
            result = tmux_pane_active("qa", window=300)
        self.assertIs(result, True, "CLI 忙态被当成死亡：长工具调用期间会误杀工作流")

    def test_silent_and_cli_idle_is_false(self):
        """静默超窗且底栏无忙态标记 → 真空闲/已死，保留回收路径。"""
        ts = MagicMock(returncode=0, stdout=str(int(time.time()) - 5000) + "\n")
        idle = MagicMock(returncode=0, stdout="❯ \n⏵⏵ bypass permissions on")
        with patch("liveness.subprocess.run", side_effect=[ts, idle]):
            result = tmux_pane_active("qa", window=300)
        self.assertIs(result, False, "真空闲被误判为忙：回收路径失效，卡死实例永驻")

    def test_silent_but_429_retry_is_true(self):
        """429 客户端退避中：pane 无 esc 提示、转录也不写，但角色没死。

        2026-10-21 15:16:47 实测：maintainer pane 停在 `Request rejected (429)`，
        随后 410s 转录零写入（> 300s 窗）→ 条件4 判僵尸回收 wf_rw_3df85d1ed26f，
        而该角色 15:58 写出了 s1-complete。退避行 `Retrying in ... · attempt N/M`
        必须与 esc 提示同等视为存活。
        """
        ts = MagicMock(returncode=0, stdout=str(int(time.time()) - 5000) + "\n")
        retry = MagicMock(returncode=0,
                          stdout="✻ Retrying in 4m 27s · attempt 19/30\n❯ ")
        with patch("liveness.subprocess.run", side_effect=[ts, retry]):
            result = tmux_pane_active("qa", window=300)
        self.assertIs(result, True, "退避中被判死：角色活着、任务白丢")

    def test_idle_pane_without_retry_stays_false(self):
        """只看得到 `attempt` 字样（如历史输出残留）而没 `Retrying` → 不算存活。"""
        ts = MagicMock(returncode=0, stdout=str(int(time.time()) - 5000) + "\n")
        idle = MagicMock(returncode=0, stdout="done after attempt 3\n❯ ")
        with patch("liveness.subprocess.run", side_effect=[ts, idle]):
            result = tmux_pane_active("qa", window=300)
        self.assertIs(result, False, "单凭 attempt 放行会让回收路径失效")

    def test_busy_probe_failure_is_unknown(self):
        """忙态二次探测失败 → None（保守不杀），不把探测故障翻译成死亡。"""
        ts = MagicMock(returncode=0, stdout=str(int(time.time()) - 5000) + "\n")
        with patch("liveness.subprocess.run", side_effect=[ts, FileNotFoundError]):
            result = tmux_pane_active("qa", window=300)
        self.assertIsNone(result)

    def test_empty_output_is_unknown_not_busy(self):
        """恒空输出（旧代码失败的形态）必须返回 None，不能返回 True。"""
        result, _ = self._run_stdout("\n")
        self.assertIsNone(result, "无数据 ≠ 活跃，须交调用方按未知处理")

    def test_zero_timestamp_is_unknown(self):
        result, _ = self._run_stdout("0\n")
        self.assertIsNone(result)

    def test_unparsable_output_is_unknown(self):
        result, _ = self._run_stdout("not-a-number\n")
        self.assertIsNone(result)

    def test_tmux_missing_is_unknown(self):
        with patch("liveness.subprocess.run", side_effect=FileNotFoundError):
            self.assertIsNone(tmux_pane_active("qa"))


class TestStaleRoleNotBusyWhenSilent(unittest.TestCase):
    """回归守卫：stale 判定本身不得被 pane 存活推翻（P0 的直接症状）。

    stale 的语义就是「无输出且无产出」；若此时因 pane 存活改判 busy，
    引擎会一直排队等待，而角色永远不会产出 → 饿死。
    """

    def setUp(self):
        self._hd = Path(tempfile.mkdtemp())

    def _write(self, data):
        (self._hd / "qa.json").write_text(json.dumps(data))

    @patch("liveness.tmux_pane_active", return_value=False)
    def test_stale_and_silent_is_not_busy(self, _m):
        self._write({"survival_overall": "stale", "survival_l3_producing": False})
        gate, _ = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertIs(gate, False)

    @patch("liveness.tmux_pane_active", return_value=True)
    def test_stale_but_recent_output_is_busy(self, _m):
        """兜底仍应存在：health 快照 stale 但 pane 刚有输出（推理中）→ 不打断。"""
        self._write({"survival_overall": "stale"})
        gate, _ = _health_gate("qa", self._hd, pane_fallback=True)
        self.assertIs(gate, True)


class TestL4EffectiveRoleOverride(unittest.TestCase):
    """开发角色池分流后，忙检测必须按实际执行者判定（不是模板 target_role）。

    生产事故（2026-10-20 02:07~03:2x）：wf_sub_4e63ad633d39 / wf_sub_b548d0e2495a
    分流到 pg、effective_roles={"s1":"pg"}，模板 target_role 仍是 engineer。
    只比对模板值 → engineer 被两个自己不执行的子链永久判忙 →
    父链 WL-food_chain_loop/s4 与 WF-product_delivery/s4 停滞 75min+
    （journal 每 tick: liveness: skip engineer busy with wf_sub_b548/s1）。
    """

    def _conn(self, effective_roles: str):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE workflow_instances(instance_id TEXT, template_id TEXT, "
            "step_results TEXT, assigner TEXT, parent_wf_id TEXT, effective_roles TEXT, "
            "status TEXT)")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        conn.execute("INSERT INTO workflow_templates('template_id','steps_json') VALUES('tpl',?)",
                      (json.dumps({"steps": [{"id": "s1", "target_role": "engineer"}]}),))
        conn.execute(
            "INSERT INTO workflow_instances VALUES('wf_sub_x','tpl',?,'workflow_engine',"
            "'wf_main',?,'running')",
            (json.dumps({"s1": {"status": "notified", "timeout_count": 0}}), effective_roles))
        return conn

    def test_engineer_not_busy_when_step_reassigned_to_pg(self):
        conn = self._conn(json.dumps({"s1": "pg"}))
        s = _s4_stepping("engineer", conn, exclude_wf_id="wf_other")
        self.assertFalse(s["busy"], "步骤已改投 pg，engineer 不应被判忙")
        conn.close()

    def test_pg_busy_when_step_reassigned_to_pg(self):
        conn = self._conn(json.dumps({"s1": "pg"}))
        s = _s4_stepping("pg", conn, exclude_wf_id="wf_other")
        self.assertTrue(s["busy"], "实际执行者 pg 应被判忙")
        conn.close()

    def test_engineer_busy_without_override(self):
        conn = self._conn("{}")
        s = _s4_stepping("engineer", conn, exclude_wf_id="wf_other")
        self.assertTrue(s["busy"], "无覆盖时仍按模板 target_role 判忙")
        conn.close()

    def test_bare_name_template_resolves_with_wf_prefix(self):
        """实例写裸名 engineer_feature，注册表存 WF-engineer_feature → 需回退查到。

        不回退时 _load_steps_json 返回 None → 该实例在忙检测中被静默跳过
        （实测 wf_sub_4e63/wf_sub_b548 在 running 中却从未被 pg 判忙）。
        """
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE workflow_instances(instance_id TEXT, template_id TEXT, "
            "step_results TEXT, assigner TEXT, parent_wf_id TEXT, effective_roles TEXT, "
            "status TEXT)")
        conn.execute("CREATE TABLE workflow_templates(template_id TEXT, steps_json TEXT)")
        conn.execute("INSERT INTO workflow_templates VALUES('WF-engineer_feature',?)",
                      (json.dumps([{"step_id": "s1", "target_role": "engineer"}]),))
        conn.execute(
            "INSERT INTO workflow_instances VALUES('wf_sub_y','engineer_feature',?,"
            "'workflow_engine','wf_main','{}','running')",
            (json.dumps({"s1": {"status": "notified", "timeout_count": 0}}),))
        s = _s4_stepping("engineer", conn, exclude_wf_id="wf_other")
        self.assertTrue(s["busy"], "裸名模板应回退到 WF- 前缀后判忙")
        conn.close()


if __name__ == "__main__":
    unittest.main()
