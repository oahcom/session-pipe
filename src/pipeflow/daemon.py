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

import fcntl
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
LOGGER = logging.getLogger("pipeline.daemon")

# 自愈 sys.path: 从文件位置推导 src/ 路径，支持从任意目录直接执行
_src = Path(__file__).resolve().parent.parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from paths import ensure_paths
ensure_paths()

from pipeflow.engine import WorkflowEngine

INTERVAL = 10  # 秒

_LOCK_FILE = Path.home() / ".hermes" / "run" / "workflow-daemon.lock"
_LOCK_FD = None


def _acquire_flock() -> bool:
    """fcntl.flock 互斥锁，防止双进程竞争（不依赖 PID，兼容 WSL2）。"""
    global _LOCK_FD
    _LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = _LOCK_FILE.open("w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fd.write(str(os.getpid()))
        fd.flush()
        _LOCK_FD = fd
        return True
    except (IOError, OSError):
        fd.close()
        old_pid = _LOCK_FILE.read_text().strip() if _LOCK_FILE.exists() else "?"
        LOGGER.warning("已有实例在运行 (lock held by pid=%s)", old_pid)
        return False


def daemon_loop(interval: int):
    """守护进程主循环：驱动 engine.run_once 推进所有工作流。"""
    if not _acquire_flock():
        sys.exit(1)

    eng = WorkflowEngine()
    shutdown = False

    def _handler(s, f):
        nonlocal shutdown; shutdown = True

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    LOGGER.info("Workflow Daemon 启动 PID=%d, 间隔 %ds", os.getpid(), interval)

    idled = 0
    try:
        while not shutdown:
            try:
                eng.run_once()
                idled = 0
            except Exception as e:
                LOGGER.error("run_once 异常: %s", e, exc_info=True)
                idled += 1
                if idled % 10 == 0:
                    LOGGER.error("连续失败 %d 轮，最后异常: %s", idled, e)
            time.sleep(interval)
    finally:
        if _LOCK_FD is not None:
            fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
            _LOCK_FD.close()
            _LOCK_FILE.unlink(missing_ok=True)
        LOGGER.info("Workflow Daemon 已停止")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()

    daemon_loop(args.interval)
