#!/usr/bin/env python3
"""
p0_exemption.py 单元测试 — P0 豁免通道 + 审计轨迹。

覆盖:
  create_p0_task: 角色权限(coordinator/lr/pm/非法角色)、reason长度、title/assignee必填
  update_task_template_id: 补录成功、超时拒绝、task不存在
  check_timeouts: 超时标记violation、无超时不触发
  can_mark_p0 / can_mark_draft: 角色检查
  mark_p0_draft: 正常标记、reason过短、task不存在、task已completed
  confirm_p0: 权限检查、状态流转(draft→confirmed/escalated)
  downgrade_p0: 权限检查、状态流转
  extend_deadline: 单次上限、累计上限、budget缩减
  p0_audit_scan: 1h升级、50min预警、4h自动降级
  _check_escalation_chain: 6h→supervisor、12h→@everyone

运行: cd ~/session-pipeline && python3 -m pytest tests/test_p0_exemption.py -v
"""

import json
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch, MagicMock

_PIPELINE_SRC = str(os.path.expanduser("~/session-pipeline/src"))
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

from p0_exemption import (
    P0Exemption, ALLOWED_P0_ROLES, ALLOWED_DRAFT_ROLES,
    P0_TIMEOUT_HOURS, P0_DRAFT_CONFIRM_WINDOW,
    PM_WORK_START, PM_WORK_END,
)


def _make_pex(role: str = "coordinator") -> P0Exemption:
    """创建隔离的 P0Exemption，使用内存数据库，跳过 bus 通知。"""
    pex = P0Exemption.__new__(P0Exemption)
    pex.role = role
    pex.db_path = ":memory:"
    import sqlite3
    pex._conn = sqlite3.connect(":memory:", timeout=10)
    pex._conn.row_factory = sqlite3.Row
    pex._conn.execute("PRAGMA journal_mode=WAL")
    pex._conn.executescript("""
        CREATE TABLE IF NOT EXISTS workflow_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_instance_id TEXT,
            task_id TEXT,
            action TEXT NOT NULL,
            actor TEXT NOT NULL,
            detail TEXT,
            ts REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT DEFAULT '',
            assigner TEXT DEFAULT '',
            assignee TEXT DEFAULT '',
            template_id TEXT DEFAULT NULL,
            priority INTEGER DEFAULT 0,
            status TEXT DEFAULT 'created',
            created_at REAL,
            updated_at REAL,
            p0_state TEXT DEFAULT NULL,
            p0_reason TEXT DEFAULT '',
            p0_marked_at REAL DEFAULT NULL,
            p0_marked_by TEXT DEFAULT ''
        );
    """)
    pex._conn.commit()
    return pex


def _seed_task(pex: P0Exemption, task_id: str = "task_abc123",
               title: str = "测试任务", status: str = "created",
               created_at: float = None, template_id: str = None,
               p0_state: str = None, p0_marked_at: float = None,
               p0_marked_by: str = "", assignee: str = "engineer",
               priority: int = 0) -> str:
    """向内存 DB 写入一条 task 记录。"""
    now = time.time()
    created = created_at if created_at is not None else now
    pex._conn.execute(
        "INSERT INTO tasks (task_id, title, status, created_at, updated_at, "
        "template_id, p0_state, p0_marked_at, p0_marked_by, assignee, "
        "assigner, priority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (task_id, title, status, created, now, template_id,
         p0_state, p0_marked_at, p0_marked_by, assignee, "coordinator",
         priority)
    )
    pex._conn.commit()
    return task_id


class TestCreateP0Task(unittest.TestCase):

    def test_coordinator_can_create(self):
        pex = _make_pex("coordinator")
        tid = pex.create_p0_task("紧急修复", "生产环境崩溃需要立即处理", "engineer1", "coordinator", "生产环境服务宕机严重影响全部用户正常使用")
        self.assertTrue(tid.startswith("task_"))

    def test_lr_can_create(self):
        pex = _make_pex("lr")
        tid = pex.create_p0_task("安全漏洞", "发现高危SQL注入漏洞需立即修复", "security", "lr", "远程代码执行风险影响所有用户数据安全")
        self.assertTrue(tid.startswith("task_"))

    def test_unauthorized_role_rejected(self):
        pex = _make_pex("engineer")
        with self.assertRaises(PermissionError) as ctx:
            pex.create_p0_task("紧急", "描述需要超过十五个字符来测试权限", "e1", "engineer", "这是权限测试的足够长的reason字符串")
        self.assertIn("coordinator/lr", str(ctx.exception))

    def test_pm_outside_work_hours_rejected(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = 23
            mock_time.time.return_value = time.time()
            with self.assertRaises(PermissionError) as ctx:
                pex.create_p0_task("紧急", "描述需要超过十五个字符来测试", "e1", "pm", "PM在非工作时间标记P0测试场景")
            self.assertIn("仅能在", str(ctx.exception))

    def test_pm_inside_work_hours_allowed(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = 10
            real_time = time.time()
            mock_time.time.return_value = real_time
            tid = pex.create_p0_task("PM紧急", "PM在工作时间标记P0任务的测试场景", "e1", "pm", "PM在工作时间内可以标记P0豁免任务")
            self.assertTrue(tid.startswith("task_"))

    def test_short_reason_rejected(self):
        pex = _make_pex("coordinator")
        with self.assertRaises(ValueError) as ctx:
            pex.create_p0_task("紧急", "short", "e1", "coordinator", "太短了")
        self.assertIn("15", str(ctx.exception))

    def test_empty_title_rejected(self):
        pex = _make_pex("coordinator")
        with self.assertRaises(ValueError):
            pex.create_p0_task("", "描述需要超过十五个字符", "e1", "coordinator", "title为空测试理由长度够十五个字")

    def test_empty_assignee_rejected(self):
        pex = _make_pex("coordinator")
        with self.assertRaises(ValueError):
            pex.create_p0_task("紧急", "描述需要超过十五个字符来测试", "", "coordinator", "assignee为空测试理由长度够十五个字")


class TestUpdateTaskTemplateId(unittest.TestCase):

    def test_success_within_window(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, created_at=time.time())
        ok = pex.update_task_template_id(tid, "tpl_001")
        self.assertTrue(ok)

    def test_returns_false_when_task_not_exists(self):
        pex = _make_pex("coordinator")
        ok = pex.update_task_template_id("nonexistent", "tpl_001")
        self.assertFalse(ok)

    def test_returns_false_when_timed_out(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        tid = _seed_task(pex, created_at=old)
        ok = pex.update_task_template_id(tid, "tpl_late")
        self.assertFalse(ok)
        logs = pex._conn.execute(
            "SELECT detail FROM workflow_logs WHERE task_id=?", (tid,)
        ).fetchall()
        self.assertTrue(any("超时" in json.loads(l["detail"])["detail"] for l in logs))


class TestCheckTimeouts(unittest.TestCase):

    def test_timeout_detected(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        tid = _seed_task(pex, created_at=old, title="超时任务")
        violations = pex.check_timeouts()
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0]["task_id"], tid)

    def test_no_timeout_when_recent(self):
        pex = _make_pex("coordinator")
        _seed_task(pex, created_at=time.time(), title="新任务")
        violations = pex.check_timeouts()
        self.assertEqual(len(violations), 0)

    def test_completed_task_skipped(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        _seed_task(pex, created_at=old, status="completed")
        violations = pex.check_timeouts()
        self.assertEqual(len(violations), 0)

    def test_task_with_template_id_skipped(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        _seed_task(pex, created_at=old, template_id="tpl_ok")
        violations = pex.check_timeouts()
        self.assertEqual(len(violations), 0)


class TestCanMarkP0(unittest.TestCase):

    def test_coordinator_always_true(self):
        pex = _make_pex("coordinator")
        self.assertTrue(pex.can_mark_p0())

    def test_lr_always_true(self):
        pex = _make_pex("lr")
        self.assertTrue(pex.can_mark_p0())

    def test_pm_true_during_work_hours(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = 14
            self.assertTrue(pex.can_mark_p0())

    def test_pm_false_outside_work_hours(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = 23
            self.assertFalse(pex.can_mark_p0())

    def test_engineer_false(self):
        pex = _make_pex("engineer")
        self.assertFalse(pex.can_mark_p0())


class TestCanMarkDraft(unittest.TestCase):

    def test_any_role_can_draft(self):
        for role in ("engineer", "qa", "devops", "pm", "coordinator", "scout"):
            pex = _make_pex(role)
            self.assertTrue(pex.can_mark_draft(), f"{role} should be able to draft")


class TestMarkP0Draft(unittest.TestCase):

    def test_mark_draft_success(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex)
        result = pex.mark_p0_draft(tid, "紧急需要标记为P0草稿等待确认", "engineer")
        self.assertEqual(result["p0_state"], "draft")
        row = pex._conn.execute("SELECT p0_state FROM tasks WHERE task_id=?", (tid,)).fetchone()
        self.assertEqual(dict(row)["p0_state"], "draft")

    def test_short_reason_rejected(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex)
        with self.assertRaises(ValueError):
            pex.mark_p0_draft(tid, "太短", "engineer")

    def test_nonexistent_task_rejected(self):
        pex = _make_pex("engineer")
        # reason 长度检查先于 task 存在性检查
        with self.assertRaises(ValueError):
            pex.mark_p0_draft("task_nope", "这个不存在的任务需要标记P0草稿测试原因够长", "engineer")

    def test_completed_task_rejected(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex, status="completed")
        with self.assertRaises(ValueError) as ctx:
            pex.mark_p0_draft(tid, "已完成任务不可标记P0草稿测试", "engineer")
        self.assertIn("completed", str(ctx.exception))


class TestConfirmP0(unittest.TestCase):

    def test_coordinator_can_confirm_draft(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft")
        result = pex.confirm_p0(tid, "coordinator")
        self.assertEqual(result["p0_state"], "confirmed")
        row = pex._conn.execute("SELECT p0_state FROM tasks WHERE task_id=?", (tid,)).fetchone()
        self.assertEqual(dict(row)["p0_state"], "confirmed")

    def test_lr_can_confirm_escalated(self):
        pex = _make_pex("lr")
        tid = _seed_task(pex, p0_state="escalated")
        result = pex.confirm_p0(tid, "lr")
        self.assertEqual(result["p0_state"], "confirmed")

    def test_engineer_cannot_confirm(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex, p0_state="draft")
        with self.assertRaises(PermissionError) as ctx:
            pex.confirm_p0(tid, "engineer")
        self.assertIn("coordinator/lr", str(ctx.exception))

    def test_invalid_state_rejected(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state=None)
        with self.assertRaises(ValueError):
            pex.confirm_p0(tid, "coordinator")

    def test_downgraded_state_rejected(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="downgraded")
        with self.assertRaises(ValueError):
            pex.confirm_p0(tid, "coordinator")


class TestDowngradeP0(unittest.TestCase):

    def test_coordinator_can_downgrade(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft")
        result = pex.downgrade_p0(tid, "降级理由需要超过十五个字符", "coordinator")
        self.assertEqual(result["p0_state"], "downgraded")
        row = pex._conn.execute("SELECT p0_state, priority FROM tasks WHERE task_id=?", (tid,)).fetchone()
        self.assertEqual(dict(row)["p0_state"], "downgraded")
        self.assertEqual(dict(row)["priority"], 1)

    def test_system_can_downgrade(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="escalated")
        result = pex.downgrade_p0(tid, "系统自动降级因为超时超过四小时了", "system")
        self.assertEqual(result["p0_state"], "downgraded")

    def test_engineer_cannot_downgrade(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex, p0_state="draft")
        with self.assertRaises(PermissionError):
            pex.downgrade_p0(tid, "engineer不可降级P0任务测试", "engineer")


class TestExtendDeadline(unittest.TestCase):

    def test_within_limits(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=time.time())
        result = pex.extend_deadline(tid, 20, "coordinator")
        self.assertEqual(result["extended_minutes"], 20)
        self.assertAlmostEqual(result["remaining_budget"], 100, delta=1)

    def test_single_max_30min(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=time.time())
        with self.assertRaises(ValueError) as ctx:
            pex.extend_deadline(tid, 31, "coordinator")
        self.assertIn("≤30", str(ctx.exception))

    def test_cumulative_max_120min(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=time.time())
        for _ in range(3):
            pex.extend_deadline(tid, 30, "coordinator")
        result = pex.extend_deadline(tid, 30, "coordinator")
        self.assertEqual(result["extended_minutes"], 30)

    def test_cumulative_exhausted(self):
        pex = _make_pex("coordinator")
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=time.time())
        for _ in range(4):
            pex.extend_deadline(tid, 30, "coordinator")
        with self.assertRaises(ValueError) as ctx:
            pex.extend_deadline(tid, 10, "coordinator")
        self.assertIn("无延长额度", str(ctx.exception))

    def test_unauthorized_role_rejected(self):
        pex = _make_pex("engineer")
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=time.time())
        with self.assertRaises(PermissionError):
            pex.extend_deadline(tid, 10, "engineer")


class TestP0AuditScan(unittest.TestCase):

    def test_escalate_draft_after_1h(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - (P0_DRAFT_CONFIRM_WINDOW * 3600 + 600)
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        actions = [r["action"] for r in results if r["task_id"] == tid]
        self.assertIn("escalated", actions)

    def test_warning_before_1h(self):
        pex = _make_pex("coordinator")
        warning_at = P0_DRAFT_CONFIRM_WINDOW * 3600 - 600
        marked_at = time.time() - (warning_at + 100)
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        actions = [r["action"] for r in results if r["task_id"] == tid]
        self.assertIn("warning_sent", actions)

    def test_auto_downgrade_after_4h(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - (P0_TIMEOUT_HOURS * 3600 + 600)
        tid = _seed_task(pex, p0_state="escalated", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        actions = [r["action"] for r in results if r["task_id"] == tid]
        self.assertIn("auto_downgraded", actions)

    def test_draft_within_window_no_action(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 600
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        task_actions = [r for r in results if r["task_id"] == tid]
        self.assertEqual(len(task_actions), 0)

    def test_confirmed_tasks_skipped(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        tid = _seed_task(pex, p0_state="confirmed", p0_marked_at=old,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        task_actions = [r for r in results if r["task_id"] == tid]
        self.assertEqual(len(task_actions), 0)

    def test_downgraded_tasks_skipped(self):
        pex = _make_pex("coordinator")
        old = time.time() - (P0_TIMEOUT_HOURS + 1) * 3600
        tid = _seed_task(pex, p0_state="downgraded", p0_marked_at=old,
                        p0_marked_by="engineer")
        results = pex.p0_audit_scan()
        task_actions = [r for r in results if r["task_id"] == tid]
        self.assertEqual(len(task_actions), 0)


class TestEscalationChain(unittest.TestCase):

    def test_6h_escalation(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 7 * 3600
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex._check_escalation_chain()
        actions = [r["action"] for r in results if r["task_id"] == tid]
        self.assertIn("escalated_6h", actions)

    def test_12h_escalation(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 13 * 3600
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex._check_escalation_chain()
        actions = [r["action"] for r in results if r["task_id"] == tid]
        self.assertIn("escalated_12h", actions)

    def test_no_escalation_under_6h(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 3 * 3600
        tid = _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                        p0_marked_by="engineer")
        results = pex._check_escalation_chain()
        task_actions = [r for r in results if r["task_id"] == tid]
        self.assertEqual(len(task_actions), 0)

    def test_confirmed_tasks_skipped(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 20 * 3600
        _seed_task(pex, p0_state="confirmed", p0_marked_at=marked_at,
                   p0_marked_by="engineer")
        results = pex._check_escalation_chain()
        self.assertEqual(len(results), 0)

    def test_completed_tasks_skipped(self):
        pex = _make_pex("coordinator")
        marked_at = time.time() - 20 * 3600
        _seed_task(pex, p0_state="draft", p0_marked_at=marked_at,
                   p0_marked_by="engineer", status="completed")
        results = pex._check_escalation_chain()
        self.assertEqual(len(results), 0)


class TestAuditLog(unittest.TestCase):

    def test_log_audit_records(self):
        pex = _make_pex("coordinator")
        pex._log_audit("task_1", "coordinator", "test detail")
        logs = pex._conn.execute("SELECT * FROM workflow_logs").fetchall()
        self.assertEqual(len(logs), 1)
        log = dict(logs[0])
        self.assertEqual(log["task_id"], "task_1")
        self.assertEqual(log["actor"], "coordinator")
        self.assertEqual(log["action"], "p0_exemption")

    def test_audit_trace_on_create(self):
        pex = _make_pex("coordinator")
        tid = pex.create_p0_task("紧急修复任务", "生产环境数据库连接池耗尽影响全部用户", "engineer1", "coordinator", "紧急故障P0任务审计日志记录确保痕迹完整可追溯")
        logs = pex._conn.execute(
            "SELECT * FROM workflow_logs WHERE task_id=?", (tid,)
        ).fetchall()
        self.assertGreater(len(logs), 0)


class TestPmWorkHoursBoundary(unittest.TestCase):

    def test_pm_allowed_at_start(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = PM_WORK_START
            self.assertTrue(pex._check_pm_work_hours())

    def test_pm_blocked_at_end(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = PM_WORK_END
            self.assertFalse(pex._check_pm_work_hours())

    def test_pm_allowed_just_before_end(self):
        pex = _make_pex("pm")
        with patch("p0_exemption.time") as mock_time:
            mock_time.localtime.return_value.tm_hour = PM_WORK_END - 1
            self.assertTrue(pex._check_pm_work_hours())


if __name__ == "__main__":
    unittest.main()
