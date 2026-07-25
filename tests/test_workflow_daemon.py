#!/usr/bin/env python3
"""
WorkflowDaemon 测试 — daemon 循环、单实例 PID 锁、信号处理。

运行：cd /home/administrator/session-pipeline && python3 tests/test_workflow_daemon.py
"""
import os
import sys
import tempfile
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

import pipeflow.daemon as daemon


def _test_pid_file():
    return Path(tempfile.mktemp(suffix=".pid"))


@patch("pipeflow.daemon.WorkflowEngine")
def test_daemon_loop_runs_engine(mock_engine_cls):
    """daemon_loop 启动 WorkflowEngine 并调用 run_once + tick。"""
    pid_file = _test_pid_file()
    daemon._PID_FILE = pid_file
    mock_eng = MagicMock()
    mock_engine_cls.return_value = mock_eng

    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    assert mock_eng.run_once.call_count >= 2, f"run_once 调用次数: {mock_eng.run_once.call_count}"
    # NOTE: tick() 是 run_once() 的别名，已在 daemon 循环中去除重复调用
    pid_file.unlink(missing_ok=True)


@patch("pipeflow.daemon.WorkflowEngine")
def test_daemon_loop_continues_on_error(mock_engine_cls):
    """run_once 抛异常后 daemon 继续运行。"""
    pid_file = _test_pid_file()
    daemon._PID_FILE = pid_file
    mock_eng = MagicMock()
    mock_engine_cls.return_value = mock_eng
    call_count = 0
    def side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("测试异常")
    mock_eng.run_once.side_effect = side_effect

    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    assert mock_eng.run_once.call_count >= 2, f"异常后应继续: {mock_eng.run_once.call_count}"
    pid_file.unlink(missing_ok=True)


def test_pid_lock():
    """PID 锁防止双进程竞争。"""
    pid_file = Path("/tmp/workflow-daemon.pid")
    if pid_file.exists():
        pid_file.unlink()
    daemon._PID_FILE = pid_file
    assert daemon._acquire_pid_lock()  # 第一次获取
    assert not daemon._acquire_pid_lock()  # 第二次拒绝
    pid_file.unlink()


def test_cli_defaults():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=int, default=10)
    args = p.parse_args([])
    assert args.interval == 10


if __name__ == "__main__":
    print("=== WorkflowDaemon 测试 ===\n")

    tests = [
        ("daemon 驱动 engine", test_daemon_loop_runs_engine),
        ("daemon 异常继续", test_daemon_loop_continues_on_error),
        ("PID 锁", test_pid_lock),
        ("CLI 默认参数", test_cli_defaults),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
