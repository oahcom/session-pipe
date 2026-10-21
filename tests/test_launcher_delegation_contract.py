"""契约：session-launcher 通过子进程委托 pipeline 的方法，不得再被删。

codex P1（2f45115）：launcher/src/workflow/client.py 用
`sys.path.insert(SESSION_PIPELINE_SRC); from workflow.client import WorkflowClient`
去调 create_task_v2 / confirm / pull_approval。删掉任何一个，launcher 的
wf approval/create/confirm 就在运行时 AttributeError，编译期看不出来。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
from workflow.client import WorkflowClient  # noqa: E402


def test_launcher_delegated_methods_exist():
    for name in ("create_task_v2", "confirm", "pull_approval"):
        assert callable(getattr(WorkflowClient, name, None)), (
            f"{name} 被删：session-launcher 的 wf 子命令依赖它")
