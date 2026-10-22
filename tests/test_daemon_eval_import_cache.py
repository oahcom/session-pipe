#!/usr/bin/env python3
"""常驻进程的模块缓存：新增跨模块常量在首轮导入即 ImportError（2026-10-22 10:27 实测）。

journal 原话：`eval_consistency 预检异常: cannot import name CRITERIA_WINDOW from
eval_shared_loader` —— daemon 05:49 启动时缓存的 eval_shared_loader 还没有该常量，
而预检只在热重载之后才被清缓存，于是第一轮预检整段被跳过。
"""
import importlib
import sys
import types


def test_forget_eval_modules_rescues_stale_loader():
    """先制造旧模块 → 导入必失败；清缓存后必须成功。"""
    sys.modules["eval_shared_loader"] = types.ModuleType("eval_shared_loader")
    sys.modules.pop("eval_consistency", None)
    sys.modules.pop("routing_daemon", None)
    try:
        importlib.import_module("eval_consistency")
    except ImportError:
        pass
    else:
        raise AssertionError("旧 eval_shared_loader 居然能导入 eval_consistency —— 失败模式没被复现")

    import routing_daemon

    routing_daemon._forget_eval_modules()
    mod = importlib.import_module("eval_consistency")
    assert hasattr(mod, "main")
    assert "eval_shared_loader" in sys.modules and mod.CRITERIA_WINDOW == 3