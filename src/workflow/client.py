"""workflow/client.py — CCS 角色使用的工作流客户端。"""

import json
import os
import time
import warnings
from pathlib import Path
from typing import Optional

from paths import BUS_CLIENT, SESSION_LAUNCHER_SRC
from workflow.db import create_connection

CCS_CLI = SESSION_LAUNCHER_SRC / "ccs.py"


class WorkflowClient:
    """CCS 角色使用的工作流客户端。"""

    def __init__(self, role: str, db_path: str = None):
        self.role = role
        self.db_path = db_path
        self._conn = create_connection(db_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _log(self, wf_id: str = None, task_id: str = None,
             action: str = "", detail: str = ""):
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, action, actor, detail, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)", (wf_id, task_id, action, self.role, detail, time.time())
        )
        self._conn.commit()

    # ── Template ──

    def list_templates(self) -> list[dict]:
        rows = self._conn.execute("SELECT * FROM workflow_templates ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def find_template(self, name: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM workflow_templates WHERE name=?", (name,)).fetchone()
        return dict(row) if row else None

    # ── Task CRUD ──

    def create_task(self, title: str, description: str = "",
                    assignee: str = None, priority: int = 0,
                    template_id: str = None) -> str:
        if template_id is None:
            raise ValueError(
                "template_id is required. Use create_task_v2(title, assignee, template_id, initiator_role)."
            )
        return self._create_task_impl(title, description, assignee, priority, template_id)

    def create_task_v2(self, title: str, assignee: str,
                       template_id: str = "", initiator_role: str = "",
                       description: str = "", parent_task_id: str = "",
                       bus_category: str = "") -> tuple:
        # 自动选择模板: 未指定时按任务标题 + 发起角色 + bus 分类匹配
        if not template_id:
            from template_registry import TemplateRegistry
            _reg = TemplateRegistry(str(self.db_path) if hasattr(self, 'db_path') and self.db_path else None)
            _candidates = _reg.recommend(title, initiator_role=initiator_role, assignee=assignee,
                                         bus_category=bus_category)
            _reg.close()
            if not _candidates:
                raise ValueError(f"无法为任务 '{title}' 匹配到工作流模板，请显式指定 template_id")
            template_id = _candidates[0]["template_id"]
        from workflow.gateway import Gate
        gate = Gate(str(self.db_path) if hasattr(self, 'db_path') and self.db_path else None)
        gate.validate_create_task(template_id, initiator_role, assignee)
        task_id = self._create_task_impl(title, description, assignee, 0, template_id)
        if parent_task_id:
            # 写入父子关联
            self._conn.execute(
                "UPDATE tasks SET parent_task_id=? WHERE task_id=?",
                (parent_task_id, task_id))
            self._conn.commit()
        wf_id = self._create_workflow_instance(task_id, template_id, assignee, initiator_role)
        gate.route_task(task_id, initiator_role, assignee)
        return (task_id, wf_id)

    def _create_task_impl(self, title: str, description: str,
                          assignee: str, priority: int,
                          template_id: str = None) -> str:
        import uuid
        import subprocess
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        now = time.time()
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if template_id and "template_id" in cols:
            self._conn.execute("""
                INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee,
                                   priority, status, created_at, updated_at, template_id)
                VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?, ?)
            """, (task_id, title, description, self.role, assignee,
                  priority, now, now, template_id))
        else:
            self._conn.execute("""
                INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee,
                                   priority, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?)
            """, (task_id, title, description, self.role, assignee,
                  priority, now, now))
        self._conn.commit()
        self._log(task_id=task_id, action="created", detail=f"title={title}, template_id={template_id}")
        # 测试模式检测: 环境变量 SESSION_PIPELINE_TEST=1 时跳过 ccs send 和 bus notify
        # 不依赖 db_path 判断，因为自定义 DB 路径也可能是生产用途
        _is_test = os.environ.get("SESSION_PIPELINE_TEST") == "1"
        ccs_ok = True
        if not _is_test and assignee and assignee != self.role:
            result = subprocess.run(
                ["python3", str(CCS_CLI), "send", assignee,
                 f"[{self.role}] 你有新任务: {title} — check_task() 查看详情",
                 "--from", self.role],
                capture_output=True, timeout=15,
            )
            ccs_ok = result.returncode == 0
        evidence = f"assignee={assignee}, task_id={task_id}"
        if not ccs_ok:
            evidence += ", ccs_send_failed=true"
        if not _is_test:
            self.notify("task_spec", f"创建任务: {title}", evidence=evidence)
        return task_id

    def _create_workflow_instance(self, task_id: str, template_id: str, assignee: str,
                                   initiator_role: str = None) -> str:
        # ponytail: anti-bypass secondary guard — validates even if called outside create_task_v2
        from workflow.gateway import Gate
        gate = Gate(str(self.db_path) if hasattr(self, 'db_path') and self.db_path else None)
        gate.validate_create_task(template_id, initiator_role or self.role, assignee)

        import uuid as _uuid
        wf_id = f"wf_{_uuid.uuid4().hex[:10]}"
        now = time.time()
        self._conn.execute("""
            INSERT INTO workflow_instances (instance_id, template_id, task_id,
                                            assigner, assignee, status,
                                            current_step_id, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', 's1', ?)
        """, (wf_id, template_id, task_id, self.role, assignee, now))
        self._conn.commit()
        self._conn.execute(
            "UPDATE tasks SET current_workflow_id=?, status='in_progress' WHERE task_id=?",
            (wf_id, task_id))
        self._conn.commit()
        self._log(wf_id=wf_id, task_id=task_id, action="created", detail=f"template_id={template_id}")
        return wf_id

    def get_task(self, task_id: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            return None
        task = dict(row)
        task["progress"] = self._compute_progress(task_id)
        return task

    def _compute_progress(self, task_id: str) -> dict:
        rows = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE task_id=? ORDER BY created_at", (task_id,)
        ).fetchall()
        if not rows:
            return {"workflow_count": 0, "completed": 0, "current": None}
        completed = sum(1 for r in rows if dict(r)["status"] == "completed")
        running = [dict(r) for r in rows if dict(r)["status"] == "running"]
        pending = [dict(r) for r in rows if dict(r)["status"] == "pending"]
        current = running[0] if running else (pending[0] if pending else None)
        total = len(rows)
        return {
            "workflow_count": total, "completed": completed,
            "current": {"instance_id": current["instance_id"] if current else None,
                        "template_id": current["template_id"] if current else None,
                        "current_step": current["current_step_id"] if current else None,
                        } if current else None,
            "percent": int((completed / total) * 100) if total > 0 else 0,
        }

    def list_tasks(self, status: str = None, assignee: str = None) -> list[dict]:
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"; params.append(status)
        if assignee:
            query += " AND assignee=?"; params.append(assignee)
        query += " ORDER BY priority DESC, created_at DESC"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def delete_task(self, task_id: str) -> bool:
        task = self.get_task(task_id)
        if not task:
            return False
        if task['assigner'] == self.role:
            return False
        if task.get('assignee') and task['assignee'] != self.role:
            return False
        self._log(task_id=task_id, action="deleted")
        self._conn.execute("DELETE FROM workflow_instances WHERE task_id=?", (task_id,))
        self._conn.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))
        self._conn.commit()
        return True

    # ── Workflow Instance CRUD ──

    def create(self, assignee: str, task_description: str,
               workflow_json: dict = None, task_id: str = None) -> str:
        wf_id = f"wf_{int(time.time()*1000) % 100000000}"
        now = time.time()
        if not task_id:
            task_id = f"task_{int(now * 1000) % 100000000}"
            self._conn.execute("""
                INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee,
                                             status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (task_id, task_description, task_description, self.role, assignee, now, now))
        self._conn.execute("""
            INSERT OR IGNORE INTO workflow_instances (instance_id, task_id, assigner, assignee,
                                            status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (wf_id, task_id, self.role, assignee, now))
        self._conn.commit()
        if task_id:
            self._conn.execute(
                "UPDATE tasks SET current_workflow_id=?, status='in_progress' WHERE task_id=?",
                (wf_id, task_id))
            self._conn.commit()
        self._log(wf_id=wf_id, task_id=task_id, action="created", detail=f"assignee={assignee}")
        return wf_id

    def get(self, wf_id: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
        return dict(row) if row else None

    def check_task(self) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT wi.*, t.priority FROM workflow_instances wi "
            "JOIN tasks t ON wi.task_id = t.task_id "
            "WHERE wi.assignee=? AND wi.status IN ('pending', 'running') "
            "ORDER BY t.priority DESC, wi.created_at ASC LIMIT 1", (self.role,)
        ).fetchone()
        return dict(row) if row else None

    def list_my_tasks(self, status: str = None) -> list[dict]:
        query = "SELECT * FROM workflow_instances WHERE assignee=?"
        params = [self.role]
        if status:
            query += " AND status=?"; params.append(status)
        query += " ORDER BY created_at DESC"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def start(self, wf_id: str, step_id: str):
        self._conn.execute(
            "UPDATE workflow_instances SET status='running', current_step_id=? WHERE instance_id=?",
            (step_id, wf_id)
        )
        self._conn.commit()
        wf = self.get(wf_id)
        task_id = wf.get("task_id", "") if wf else ""
        evidence = f"task={task_id}" if task_id else ""
        self.notify("workflow", f"已接单 {wf_id}", evidence=evidence)
        self._log(wf_id=wf_id, action="started", detail=f"step={step_id}")

    def complete(self, wf_id: str, summary: str, files: list = None):
        files_str = ", ".join(files) if files else ""
        self._conn.execute(
            "UPDATE workflow_instances SET status='completed', completed_at=?, "
            "step_results=? WHERE instance_id=?",
            (time.time(), json.dumps({"summary": summary, "files": files_str}), wf_id)
        )
        self._conn.commit()
        wf = self.get(wf_id)
        if wf and wf.get('task_id'):
            self._sync_task_from_workflows(wf['task_id'])
        self._log(wf_id=wf_id, action="completed", detail=f"summary={summary}")

    def _sync_task_from_workflows(self, task_id: str):
        from workflow.sync import sync_task_status
        sync_task_status(self._conn, task_id)
        self._conn.commit()

    def fail(self, wf_id: str, reason: str):
        self._conn.execute(
            "UPDATE workflow_instances SET status='failed', completed_at=?, "
            "step_results=? WHERE instance_id=?",
            (time.time(), json.dumps({"reason": reason}), wf_id)
        )
        self._conn.commit()
        wf = self.get(wf_id)
        if wf and wf.get('task_id'):
            self._sync_task_from_workflows(wf['task_id'])
        self._log(wf_id=wf_id, action="failed", detail=f"reason={reason}")

    def cancel(self, wf_id: str, reason: str = ""):
        self._conn.execute(
            "UPDATE workflow_instances SET status='cancelled', completed_at=?, "
            "step_results=? WHERE instance_id=?",
            (time.time(), json.dumps({"reason": reason}), wf_id)
        )
        self._conn.commit()
        wf = self.get(wf_id)
        if wf and wf.get('task_id'):
            self._sync_task_from_workflows(wf['task_id'])
        self._log(wf_id=wf_id, action="cancelled", detail=f"reason={reason}")

    def delete(self, wf_id: str) -> bool:
        wf = self.get(wf_id)
        if not wf:
            return False
        if wf['assigner'] == self.role:
            return False
        if wf.get('assignee') and wf['assignee'] != self.role:
            return False
        self._log(wf_id=wf_id, action="deleted")
        self._conn.execute("DELETE FROM workflow_instances WHERE instance_id=?", (wf_id,))
        self._conn.commit()
        return True

    def archive(self, wf_id: str) -> bool:
        self._conn.execute("UPDATE workflow_instances SET status='archived' WHERE instance_id=?", (wf_id,))
        self._conn.commit()
        self._log(wf_id=wf_id, action="archived")
        return True

    def list_all(self, status: str = None, limit: int = 50) -> list[dict]:
        query = "SELECT * FROM workflow_instances WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"; params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ── 日志 ──

    def get_logs(self, wf_id: str = None, task_id: str = None) -> list[dict]:
        query = "SELECT * FROM workflow_logs WHERE 1=1"
        params = []
        if wf_id:
            query += " AND workflow_instance_id=?"; params.append(wf_id)
        if task_id:
            query += " AND task_id=?"; params.append(task_id)
        query += " ORDER BY ts DESC LIMIT 50"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ── 通知 ──

    def notify(self, category: str, title: str, evidence: str = ""):
        import subprocess
        cmd = ["python3", str(BUS_CLIENT), "write", category,
               f"[{self.role}] {title}", "--src", self.role]
        if evidence:
            cmd.extend(["--evidence", evidence])
        subprocess.run(cmd, capture_output=True, timeout=15)

    # ── 委派方法（经 subprocess 调用 launcher partner 模块） ──

    def confirm_delivery(self, task_id: str, target_role: str, timeout: int = 300) -> dict:
        cmd = ["python3", str(CCS_CLI), "partner", "confirm", task_id, target_role, "--as", self.role]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return {"ok": r.returncode == 0, "output": r.stdout}

    def check_wake_permission(self, target: str) -> bool:
        cmd = ["python3", str(CCS_CLI), "partner", "check-wake", self.role, target]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return r.returncode == 0

    def resolve_partner(self, role: str) -> dict:
        cmd = ["python3", str(CCS_CLI), "partner", "resolve", role]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return {"ok": r.returncode == 0, "output": r.stdout}

    def wake_partner(self, role: str, context: str = "", force: bool = False) -> dict:
        cmd = ["python3", str(CCS_CLI), "partner", "wake", role, "--as", self.role]
        if context:
            cmd.extend(["--context", context])
        if force:
            cmd.append("--force")
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return {"ok": r.returncode == 0, "output": r.stdout}

    # ── Kanban ──

    def kanban_board(self) -> list[dict]:
        lanes = {"backlog": [], "in_progress": [], "blocked": [], "completed": [], "failed": [], "cancelled": []}
        rows = self._conn.execute(
            "SELECT instance_id, template_id, status, current_step_id, assignee, created_at, "
            "COALESCE(completed_at, created_at) as updated_at "
            "FROM workflow_instances ORDER BY updated_at DESC LIMIT 50"
        ).fetchall()
        for row in rows:
            wf = dict(row); status = wf["status"]
            if status in ("pending", "created"):
                lanes["backlog"].append(wf)
            elif status in ("running", "step_done_ready"):
                lanes["in_progress"].append(wf)
            elif status == "completed":
                lanes["completed"].append(wf)
            elif status == "failed":
                lanes["failed"].append(wf)
            elif status == "cancelled":
                lanes["cancelled"].append(wf)
            elif status in ("blocked", "archived"):
                lanes["blocked"].append(wf)  # archived → blocked is still active concern
            else:
                lanes["blocked"].append(wf)
        return [{"lane": k, "count": len(v), "items": v} for k, v in lanes.items()]

    def workflow_stats(self) -> dict:
        stats = {}
        for lane in ("pending", "running", "completed", "failed", "cancelled"):
            row = self._conn.execute("SELECT COUNT(*) as c FROM workflow_instances WHERE status=?", (lane,)).fetchone()
            stats[lane] = row["c"] if row else 0
        stats["total"] = sum(stats.values())
        stats["completion_rate"] = round(stats["completed"] / max(stats["total"], 1) * 100, 1)
        return stats

    def close(self):
        self._conn.close()


def get_ccs_cli():
    """返回 CCS CLI 路径（供 lifecycle.manager 等模块使用）。"""
    return CCS_CLI
