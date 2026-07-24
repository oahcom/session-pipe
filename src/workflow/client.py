"""workflow.client — 工作流客户端

实际逻辑在 session-launcher/src/workflow/client.py
此文件保留向后兼容。
"""
import sys
from pathlib import Path
from typing import Optional

# 确保 session-launcher 在 path 中
_launcher_src = str(Path(__file__).resolve().parent.parent.parent.parent / "session-launcher" / "src")
if _launcher_src not in sys.path:
    sys.path.insert(0, _launcher_src)

# 延迟导入避免循环
def _get_real_wc():
    from workflow.client import WorkflowClient as _RealWC
    return _RealWC

def _get_ccs_cli():
    # 从 session-launcher 的 client.py 导入真实路径
    from workflow.client import CCS_CLI as _cli
    # 如果 CCS_CLI 还没设（shim 层加载顺序导致 None），直接从 session-launcher 取
    if _cli is None:
        from pathlib import Path
        _cli = str(Path(__file__).resolve().parent.parent.parent.parent / "session-launcher" / "src" / "ccs.py")
    return _cli

CCS_CLI = None  # 延迟设置

class WorkflowClient:
    """工作流客户端 — 委托给 session-launcher"""

    def __init__(self, role: str, db_path: str = None):
        RealWC = _get_real_wc()
        self._wc = RealWC(role, db_path)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def check_task(self) -> Optional[dict]:
        return self._wc.check_task()

    def create_task_v2(self, title: str, assignee: str,
                       template_id: str, initiator_role: str,
                       description: str = "") -> tuple:
        return self._wc.create_task_v2(title, assignee, template_id, initiator_role, description)

    def close(self):
        self._wc.close()


def get_ccs_cli():
    global CCS_CLI
    if CCS_CLI is None:
        CCS_CLI = _get_ccs_cli()
    return CCS_CLI


__all__ = ["WorkflowClient", "get_ccs_cli"]
