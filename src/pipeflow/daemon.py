#!/usr/bin/env python3
"""
Workflow Daemon — 驱动工作流引擎，主动推进工作流步骤。

使用 WorkflowEngine 处理 running/pending 工作流：
- pending -> 自动启动 + push prompt
- running -> 检测 exit_condition + 超时升级 + 提醒心跳

用法：
  python3 pipeflow/daemon.py --interval 30

sys.path 由 paths.py 集中管理，本文件不修改 sys.path。
"""

import os
import signal
import sys
import threading
import time
from pathlib import Path

# 自愈 sys.path: 从文件位置推导 src/ 路径，支持从任意目录直接执行
_src = Path(__file__).resolve().parent.parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from paths import ensure_paths
ensure_paths()

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
            # NOTE: engine.tick() 是 run_once() 的别名，不再重复调用
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
