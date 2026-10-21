"""契约：session-launcher 通过子进程委托 pipeline 的方法，不得被删。

codex P1（2f45115）：launcher/src/workflow/client.py 用
`sys.path.insert(SESSION_PIPELINE_SRC); from workflow.client import WorkflowClient`
去调 create_task_v2 / confirm / pull_approval。删掉任何一个，launcher 的
wf approval/create/confirm 就在运行时 AttributeError，编译期看不出来。

实现用**读源码**而不是 import：全套测试里 `workflow` 包可能已被 pipeflow.engine
从别的路径导入（sys.modules 缓存），再 sys.path.insert 不会生效——那样这个测试
断言的就不是本仓的代码了（codex 复核发现）。
"""
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src" / "workflow" / "client.py"


def test_launcher_delegated_methods_exist():
    text = _SRC.read_text(encoding="utf-8")
    for name in ("create_task_v2", "confirm", "pull_approval"):
        assert f"    def {name}(" in text, (
            f"{name} 被删：session-launcher 的 wf 子命令用子进程委托它")
