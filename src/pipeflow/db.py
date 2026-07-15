#!/usr/bin/env python3
"""
Workflow Database — SQLite 存储工作流模板、实例和任务记录。

三层架构：
  Workflow Template（可复用模板）→ Workflow Instance（具体执行）→ Task（目标）

任务生命周期：
  created → assigned → in_progress → completed/failed/cancelled

工作流生命周期：
  created → pending → running → completed/failed/cancelled
  ↓
  step: pending → running → completed/failed
"""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from paths import WORKFLOWS_DB as DB_PATH


class WorkflowDB:
    """工作流 + 任务 SQLite 数据库。"""

    def __init__(self, db_path: str | Path = DB_PATH):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _init_schema(self):
        self._conn.executescript("""
            -- 工作流模板（可复用）
            CREATE TABLE IF NOT EXISTS workflow_templates (
                template_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                steps_json TEXT NOT NULL,
                steps_mermaid TEXT,
                created_at REAL NOT NULL
            );

            -- 工作流实例（具体执行）
            CREATE TABLE IF NOT EXISTS workflow_instances (
                instance_id TEXT PRIMARY KEY,
                template_id TEXT,
                task_id TEXT NOT NULL,
                assigner TEXT NOT NULL,
                assignee TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                current_step_id TEXT,
                step_results TEXT DEFAULT '{}',
                context TEXT DEFAULT '{}',
                created_at REAL NOT NULL,
                completed_at REAL,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id),
                FOREIGN KEY (template_id) REFERENCES workflow_templates(template_id)
            );
            CREATE INDEX IF NOT EXISTS idx_instances_task ON workflow_instances(task_id);
            CREATE INDEX IF NOT EXISTS idx_instances_assignee ON workflow_instances(assignee);
            CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);

            -- 任务（目标）
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                assigner TEXT NOT NULL,
                assignee TEXT,
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'created',
                current_workflow_id TEXT,
                progress TEXT DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL,
                tags TEXT DEFAULT '[]',
                context TEXT DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee);
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_assigner ON tasks(assigner);
            CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority);

            -- 操作日志
            CREATE TABLE IF NOT EXISTS workflow_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_instance_id TEXT,
                task_id TEXT,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                detail TEXT,
                ts REAL NOT NULL
            );
        """)
        self._conn.commit()

    def _log(self, workflow_instance_id: str = None, task_id: str = None,
             action: str = "", actor: str = "", detail: str = ""):
        """写入操作日志。"""
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, action, actor, detail, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (workflow_instance_id, task_id, action, actor, detail, time.time())
        )
        self._conn.commit()

    # ── Workflow Template CRUD ──────────────────────────────────

    def create_template(self, name: str, description: str,
                        steps_json: dict, steps_mermaid: str = "") -> str:
        """创建工作流模板，返回 template_id。"""
        template_id = f"tmpl_{uuid.uuid4().hex[:8]}"
        self._conn.execute("""
            INSERT INTO workflow_templates (template_id, name, description, steps_json, steps_mermaid, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (template_id, name, description,
              json.dumps(steps_json, ensure_ascii=False), steps_mermaid, time.time()))
        self._conn.commit()
        return template_id

    def get_template(self, template_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_templates WHERE template_id=?", (template_id,)
        ).fetchone()
        return dict(row) if row else None

    def find_template(self, name: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_templates WHERE name=?", (name,)
        ).fetchone()
        return dict(row) if row else None

    def list_templates(self) -> list[dict]:
        rows = self._conn.execute("SELECT * FROM workflow_templates ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def delete_template(self, template_id: str) -> bool:
        self._conn.execute("DELETE FROM workflow_templates WHERE template_id=?", (template_id,))
        self._conn.commit()
        return True

    # ── Task CRUD ──────────────────────────────────────────────

    def create_task(self, title: str, description: str = "",
                    assigner: str = "system", assignee: str = None,
                    priority: int = 0, tags: list = None, context: dict = None) -> str:
        """创建任务，返回 task_id。"""
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        now = time.time()
        self._conn.execute("""
            INSERT INTO tasks (task_id, title, description, assigner, assignee,
                               priority, status, created_at, updated_at, tags, context)
            VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?, ?, ?)
        """, (task_id, title, description, assigner, assignee, priority, now, now,
              json.dumps(tags or [], ensure_ascii=False),
              json.dumps(context or {}, ensure_ascii=False)))
        self._conn.commit()
        self._log(task_id=task_id, action="created", actor=assigner, detail=f"title={title}")
        return task_id

    def get_task(self, task_id: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            return None
        task = dict(row)
        # 从 workflow_instances 推导进度
        task["progress"] = self._compute_progress(task_id)
        return task

    def _compute_progress(self, task_id: str) -> dict:
        """从 workflow_instances 推导任务进度。"""
        rows = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE task_id=? ORDER BY created_at",
            (task_id,)
        ).fetchall()
        if not rows:
            return {"workflow_count": 0, "completed": 0, "current": None}

        completed = sum(1 for r in rows if dict(r)["status"] == "completed")
        running = [dict(r) for r in rows if dict(r)["status"] == "running"]
        pending = [dict(r) for r in rows if dict(r)["status"] == "pending"]

        current = running[0] if running else (pending[0] if pending else None)
        total = len(rows)

        return {
            "workflow_count": total,
            "completed": completed,
            "current": {
                "instance_id": current["instance_id"] if current else None,
                "template_id": current["template_id"] if current else None,
                "current_step": current["current_step_id"] if current else None,
            } if current else None,
            "percent": int((completed / total) * 100) if total > 0 else 0,
        }

    def update_task(self, task_id: str, **kwargs) -> bool:
        """更新任务字段。"""
        allowed = {'title', 'description', 'assignee', 'priority', 'status', 'tags', 'context'}
        updates = []
        params = []
        for k, v in kwargs.items():
            if k not in allowed:
                continue
            if k in ('tags', 'context') and isinstance(v, (list, dict)):
                v = json.dumps(v, ensure_ascii=False)
            updates.append(f"{k} = ?")
            params.append(v)
        if not updates:
            return False
        updates.append("updated_at = ?")
        params.append(time.time())
        params.append(task_id)
        self._conn.execute(f"UPDATE tasks SET {', '.join(updates)} WHERE task_id=?", params)
        self._conn.commit()
        self._log(task_id=task_id, action="updated", detail=f"fields={list(kwargs.keys())}")
        return True

    def delete_task(self, task_id: str, actor: str = "system") -> bool:
        task = self.get_task(task_id)
        if not task:
            return False
        # 只有接收者或系统可以删除，下达者不能删
        if task['assigner'] == actor and actor != "system":
            return False
        self._log(task_id=task_id, action="deleted", actor=actor)
        self._conn.execute("DELETE FROM workflow_instances WHERE task_id=?", (task_id,))
        self._conn.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))
        self._conn.commit()
        return True

    def list_tasks(self, status: str = None, assigner: str = None,
                   assignee: str = None, limit: int = 50) -> list[dict]:
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        if assigner:
            query += " AND assigner=?"
            params.append(assigner)
        if assignee:
            query += " AND assignee=?"
            params.append(assignee)
        query += " ORDER BY priority DESC, created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    # ── Workflow Instance CRUD ──────────────────────────────────

    def create_workflow(self, task_id: str, template_name: str,
                        assigner: str, assignee: str,
                        context: dict = None) -> Optional[str]:
        """从模板创建工作流实例，返回 instance_id。"""
        template = self.find_template(template_name)
        if not template:
            return None

        instance_id = f"wf_{uuid.uuid4().hex[:8]}"
        now = time.time()
        steps_json = template["steps_json"]
        first_step = json.loads(steps_json).get("steps", [{}])[0] if steps_json else {}

        self._conn.execute("""
            INSERT INTO workflow_instances (instance_id, template_id, task_id, assigner, assignee,
                                            status, current_step_id, created_at, context)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """, (instance_id, template["template_id"], task_id, assigner, assignee,
              first_step.get("id"), now, json.dumps(context or {}, ensure_ascii=False)))
        self._conn.commit()

        # 更新 Task 的 current_workflow_id
        self._conn.execute("UPDATE tasks SET current_workflow_id=?, status='in_progress' WHERE task_id=?",
                           (instance_id, task_id))
        self._conn.commit()
        self._log(workflow_instance_id=instance_id, task_id=task_id, action="created",
                  actor=assigner, detail=f"template={template_name}")
        return instance_id

    def get_workflow(self, instance_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE instance_id=?", (instance_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_workflows_for_task(self, task_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE task_id=? ORDER BY created_at",
            (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def list_workflows(self, status: str = None, assignee: str = None,
                       limit: int = 50) -> list[dict]:
        query = "SELECT * FROM workflow_instances WHERE 1=1"
        params = []
        if status:
            query += " AND status=?"
            params.append(status)
        if assignee:
            query += " AND assignee=?"
            params.append(assignee)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def update_workflow(self, instance_id: str, **kwargs) -> bool:
        """更新工作流实例状态。"""
        allowed = {'status', 'current_step_id', 'step_results', 'completed_at'}
        updates = []
        params = []
        for k, v in kwargs.items():
            if k not in allowed:
                continue
            if k == 'step_results' and isinstance(v, dict):
                v = json.dumps(v, ensure_ascii=False)
            updates.append(f"{k} = ?")
            params.append(v)
        if not updates:
            return False
        params.append(instance_id)
        self._conn.execute(f"UPDATE workflow_instances SET {', '.join(updates)} WHERE instance_id=?", params)
        self._conn.commit()

        # 如果工作流完成，更新 Task 状态
        if 'status' in kwargs and kwargs['status'] in ('completed', 'failed', 'cancelled'):
            wf = self.get_workflow(instance_id)
            if wf:
                self._sync_task_from_workflows(wf['task_id'])
                self._log(workflow_instance_id=instance_id, task_id=wf['task_id'],
                         action=kwargs['status'], detail=f"step={kwargs.get('current_step_id', '')}")
        return True

    def _sync_task_from_workflows(self, task_id: str):
        """从 workflow_instances 同步 Task 状态。"""
        rows = self._conn.execute(
            "SELECT status FROM workflow_instances WHERE task_id=?", (task_id,)
        ).fetchall()
        statuses = [dict(r)["status"] for r in rows]

        if not statuses:
            return

        # 如果所有工作流都完成，Task 完成
        if all(s == "completed" for s in statuses):
            task_status = "completed"
        # 如果任何工作流失败，Task 失败
        elif any(s == "failed" for s in statuses):
            task_status = "failed"
        # 如果有 running 或 pending，Task 进行中
        elif any(s in ("running", "pending") for s in statuses):
            task_status = "in_progress"
        else:
            task_status = "completed"

        self._conn.execute("UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
                          (task_status, time.time(), task_id))
        self._conn.commit()

    def delete_workflow(self, instance_id: str, actor: str = "system") -> bool:
        """删除工作流（只有接收者或系统可以删除，下达者不能删）。"""
        wf = self.get_workflow(instance_id)
        if not wf:
            return False
        if wf['assigner'] == actor and actor != "system":
            return False
        self._log(workflow_instance_id=instance_id, task_id=wf['task_id'], action="deleted", actor=actor)
        self._conn.execute("DELETE FROM workflow_instances WHERE instance_id=?", (instance_id,))
        self._conn.commit()
        self._sync_task_from_workflows(wf['task_id'])
        return True

    def chain_workflows(self, task_id: str, template_names: list[str],
                        assigner: str, assignee: str) -> list[str]:
        """创建工作流链，返回 instance_id 列表。"""
        instance_ids = []
        for name in template_names:
            wf_id = self.create_workflow(task_id, name, assigner, assignee)
            if wf_id:
                instance_ids.append(wf_id)
        return instance_ids

    # ── 日志 ──────────────────────────────────────────────────

    def get_logs(self, workflow_instance_id: str = None, task_id: str = None,
                 limit: int = 50) -> list[dict]:
        query = "SELECT * FROM workflow_logs WHERE 1=1"
        params = []
        if workflow_instance_id:
            query += " AND workflow_instance_id=?"
            params.append(workflow_instance_id)
        if task_id:
            query += " AND task_id=?"
            params.append(task_id)
        query += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self._conn.close()
