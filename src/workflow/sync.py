"""
workflow/sync.py — 工作流实例 → 任务状态同步

4 处重复的 _sync_task_from_workflows 合并到此一处：
  - pipeflow/db.py:351-375  (WorkflowDB._sync_task_from_workflows)
  - workflow/client.py:282-304  (WorkflowClient._sync_task_from_workflows)
  - lifecycle/manager.py:738-758  (_sync_task_unsafe)
  - pipeflow/db.py:351-375 (WorkflowDB._sync_task_from_workflows)
"""

import logging
import time
from typing import Optional


def sync_task_status(conn, task_id: Optional[str]) -> None:
    """从 workflow_instances 推导 task 状态并更新。

    所有工作流 completed → task completed
    任一工作流 failed → task failed
    任一工作流 cancelled → task cancelled
    任一工作流 running/pending → task in_progress
    否则 → task 保持原样

    conn: 已有事务/连接的 sqlite3.Connection
    """
    if not task_id:
        return
    rows = conn.execute(
        "SELECT status FROM workflow_instances WHERE task_id=?", (task_id,)
    ).fetchall()
    statuses = [dict(r)["status"] for r in rows] if rows else []
    if not statuses:
        return

    if all(s == "completed" for s in statuses):
        task_status = "completed"
    elif any(s == "failed" for s in statuses):
        task_status = "failed"
    elif any(s == "cancelled" for s in statuses):
        task_status = "cancelled"
    elif any(s in ("running", "pending", "step_done_ready") for s in statuses):
        task_status = "in_progress"
    else:
        task_status = "completed"
        logging.getLogger("workflow.sync").warning(
            "sync_task_status(%s): unmatched statuses %s, default completed",
            task_id, statuses)

    conn.execute(
        "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
        (task_status, time.time(), task_id))
    # 注意: 调用方负责 COMMIT/ROLLBACK，此处不 commit
    # sync_task_status 被 LifecycleManager._sync_task_unsafe 调用
    # 后者包裹在 BEGIN IMMEDIATE...COMMIT/ROLLBACK 中
