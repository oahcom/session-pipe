#!/usr/bin/env python3
"""
Workflow Daemon — 驱动工作流引擎，主动推进工作流步骤。

使用 WorkflowEngine 处理 running/pending 工作流：
- pending -> 自动启动 + push prompt
- running -> 检测 exit_condition + 超时升级 + 提醒心跳

用法：
  python3 workflow_daemon.py --interval 30
"""

import os
import signal
import sys
import threading
import time
from pathlib import Path

# 确保 session-pipeline/src 优先
# 不能依赖 ensure_paths() 因为它会把 session-launcher/src 加在前面
# 而 launcher 的 lifecycle/manager.py 已被删除
_src = str(Path(__file__).resolve().parent.parent)
if _src not in sys.path:
    sys.path.insert(0, _src)

# 手动确保 pipeline 的 lifecycle 包已被加载（阻止 launcher 的 lifecycle 占位）
_pl = str(Path(_src) / "lifecycle")
if _pl not in sys.path:
    sys.path.insert(0, _pl)

from pipeflow.engine import WorkflowEngine

INTERVAL = 10  # 秒

_PID_FILE = Path("/tmp/workflow-daemon.pid")


def _acquire_pid_lock() -> bool:
    """获取 PID 锁，防止双进程竞争。"""
    if _PID_FILE.exists():
        try:
            old_pid = int(_PID_FILE.read_text())
            os.kill(old_pid, 0)
            print(f"[daemon] 已有实例 PID={old_pid} 在运行")
            return False
        except (OSError, ValueError):
            _PID_FILE.unlink(missing_ok=True)
    _PID_FILE.write_text(str(os.getpid()))
    return True


def daemon_loop(interval: int):
    """守护进程主循环：驱动 engine.run_once 推进所有工作流。"""
    if not _acquire_pid_lock():
        sys.exit(1)

    eng = WorkflowEngine()
    shutdown = False

    def _handler(s, f):
        nonlocal shutdown; shutdown = True

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    print(f"[daemon] Workflow Daemon 启动 PID={os.getpid()}, 间隔 {interval}s")

    try:
        while not shutdown:
            try:
                eng.run_once()
            except Exception as e:
                print(f"[daemon] run_once 异常: {e}")
            try:
                eng.tick()
            except Exception as e:
                print(f"[daemon] tick 异常: {e}")
            time.sleep(interval)
    finally:
        _PID_FILE.unlink(missing_ok=True)
        print("[daemon] 已停止")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()

    daemon_loop(args.interval)
