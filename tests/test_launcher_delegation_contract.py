"""契约：session-launcher 通过子进程委托 pipeline 的方法，不得被删。

codex P1（2f45115）：launcher/src/workflow/client.py 用
`sys.path.insert(SESSION_PIPELINE_SRC); from workflow.client import WorkflowClient`
去调 create_task_v2 / confirm / pull_approval。删掉任何一个，launcher 的
wf approval/create/confirm 就在运行时 AttributeError，编译期看不出来。

实现用 ast 解析本仓源码，而不是 import：全套测试里 `workflow` 包可能已被
pipeflow.engine 从别的路径导入（sys.modules 缓存），sys.path.insert 不生效，
断言的就成了线上仓。也不用文本匹配——把方法注释掉、挪到别的类、或写进文档串
都会骗过纯文本检查（codex 复审 P2）。
"""
import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src" / "workflow" / "client.py"
_DELEGATED = ("create_task_v2", "confirm", "pull_approval")


def test_launcher_delegated_methods_exist():
    tree = ast.parse(_SRC.read_text(encoding="utf-8"))
    cls = next((n for n in tree.body
                if isinstance(n, ast.ClassDef) and n.name == "WorkflowClient"), None)
    assert cls is not None, f"{_SRC} 里没有 WorkflowClient 类"
    defined = {n.name for n in cls.body
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    missing = [n for n in _DELEGATED if n not in defined]
    assert not missing, f"{missing} 被删：session-launcher 用子进程委托它们"
