"""workflow.client — 工作流客户端"""
import os
from pathlib import Path
from typing import Optional

CCS_CLI = Path(__file__).resolve().parent.parent.parent.parent / "session-launcher" / "ccs.py"

class WorkflowClient:
    """工作流客户端"""
    
    def __init__(self, role: str, db_path: str = None):
        self.role = role
        self.db_path = db_path
        self._conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def check_task(self) -> Optional[dict]:
        """检查角色是否有待处理任务"""
        return None
    
    def create_task_v2(self, title: str, assignee: str, template_id: str, initiator_role: str):
        """创建工作流"""
        from lifecycle.manager import LifecycleManager
        lm = LifecycleManager(initiator_role)
        wf_id = lm.start_wf_from_template(template_id, assignee)
        return f"task_{wf_id}", wf_id
    
    def close(self):
        if self._conn:
            self._conn.close()
