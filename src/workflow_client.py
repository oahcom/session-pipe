"""thin wrapper — re-export from workflow.client"""
from typing import Optional
from workflow.client import CCS_CLI, WorkflowClient

def check(role: str) -> Optional[dict]:
    """Shorthand: check if a role has a pending task.

    Referenced in 15+ CLAUDE.md CCS role files.
    """
    with WorkflowClient(role) as wf:
        return wf.check_task()

__all__ = ['CCS_CLI', 'WorkflowClient', 'check']
