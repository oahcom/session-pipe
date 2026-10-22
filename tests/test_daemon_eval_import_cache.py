#!/usr/bin/env python3
"""常驻进程的模块缓存：新增跨模块常量在首轮导入即 ImportError（2026-10-22 10:27 实测）。

journal 原话：`eval_consistency 预检异常: cannot import name CRITERIA_WINDOW from
eval_shared_loader` —— daemon 05:49 启动时缓存的 eval_shared_loader 还没有该常量，
而预检只在热重载之后才被清缓存，于是第一轮预检整段被跳过。

必须在**子进程**里做：清缓存会改写 sys.modules，同进程后续用例会看到两套模块对象，
把 tests/test_eval_consistency_parity.py 的身份断言打成假红（实测 34 passed / 1 failed）。
"""
import os
import subprocess
import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")

_CHILD = """
import sys, types, importlib
sys.modules["eval_shared_loader"] = types.ModuleType("eval_shared_loader")  # 旧版本：没有 CRITERIA_WINDOW
try:
    importlib.import_module("eval_consistency")
except ImportError:
    pass
else:
    raise AssertionError("旧 eval_shared_loader 居然能导入 eval_consistency —— 失败模式没复现")
import routing_daemon
routing_daemon._forget_eval_modules()
m = importlib.import_module("eval_consistency")
assert hasattr(m, "main") and m.CRITERIA_WINDOW == 3, vars(m).get("CRITERIA_WINDOW")
print("rescued")
"""


def test_forget_eval_modules_rescues_stale_loader():
    """先制造旧模块 → 导入必失败；清缓存后必须成功（预检路径靠这个恢复）。"""
    env = dict(os.environ, PYTHONPATH=SRC)
    out = subprocess.run([sys.executable, "-c", _CHILD], capture_output=True, text=True,
                         env=env, timeout=90)
    assert out.returncode == 0, out.stderr[-800:]
    assert "rescued" in out.stdout