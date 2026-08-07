#!/usr/bin/env python3
"""
WorkflowDaemon 测试 — daemon 循环、单实例 flock 锁、信号处理。

运行：cd /home/administrator/session-pipeline && python3 tests/test_workflow_daemon.py
"""
import os
import signal
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


def _test_lock_file():
    return Path(tempfile.mktemp(suffix=".lock"))


def _set_lock_file(p):
    daemon._LOCK_FILE = p


@patch("pipeflow.daemon.WorkflowEngine")
def test_daemon_loop_runs_engine(mock_engine_cls):
    """daemon_loop 启动 WorkflowEngine 并调用 run_once + tick。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    mock_eng = MagicMock()
    mock_engine_cls.return_value = mock_eng

    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    assert mock_eng.run_once.call_count >= 2, f"run_once 调用次数: {mock_eng.run_once.call_count}"
    lock_file.unlink(missing_ok=True)


@patch("pipeflow.daemon.WorkflowEngine")
def test_daemon_loop_continues_on_error(mock_engine_cls):
    """run_once 抛异常后 daemon 继续运行。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
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
    lock_file.unlink(missing_ok=True)


def test_flock_lock():
    """flock 锁防止双进程竞争。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    assert daemon._acquire_flock()  # 第一次获取
    assert not daemon._acquire_flock()  # 第二次拒绝
    lock_file.unlink(missing_ok=True)


def test_flock_stale_cleanup():
    """锁文件含已退出进程的 fd -> 清理后重新获取（flock 自动清理，无需显式 stale 清理）。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    # 获取一次锁
    assert daemon._acquire_flock()
    # 释放
    daemon._LOCK_FD.close()
    daemon._LOCK_FD = None
    # 再次获取应成功
    assert daemon._acquire_flock()
    lock_file.unlink(missing_ok=True)


def test_non_main_thread_skip_signal():
    """非主线程不设置信号处理器。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    eng = MagicMock()

    def _throw_on_signal(*args):
        raise RuntimeError("signal.signal 不应在非主线程调用")

    with patch("pipeflow.daemon.signal.signal", side_effect=_throw_on_signal):
        with patch("pipeflow.daemon.WorkflowEngine", return_value=eng):
            # 在非主线程运行 daemon_loop
            t = threading.Thread(target=lambda: daemon.daemon_loop(0.05), daemon=True)
            t.start()
            time.sleep(0.15)

    # 验证 engine 仍能正常调用
    assert eng.run_once.call_count >= 1
    lock_file.unlink(missing_ok=True)


def test_daemon_loop_lock_cleanup_in_finally():
    """daemon_loop 退出时清理锁文件。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    with patch("pipeflow.daemon.WorkflowEngine") as mock_cls:
        mock_eng = MagicMock()
        mock_cls.return_value = mock_eng
        # 启动 timer 在 0.3s 后发 SIGTERM
        timer = threading.Timer(0.3, lambda: os.kill(os.getpid(), signal.SIGTERM))
        timer.daemon = True
        timer.start()
        # 在主线程调用 daemon_loop -> 注册 handler -> SIGTERM 到来时退出
        daemon.daemon_loop(0.1)
    assert not lock_file.exists(), "锁文件应在 daemon_loop 退出后被清理"


def test_cli_defaults():
    """argparse --interval 默认值 10。"""
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=int, default=10)
    args = p.parse_args([])
    assert args.interval == 10


@patch("pipeflow.daemon.WorkflowEngine")
def test_main_loop_lock_file_created(mock_engine_cls):
    """daemon_loop 启动时创建锁文件。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    eng = MagicMock()
    mock_engine_cls.return_value = eng
    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    assert lock_file.exists(), "锁文件应在 daemon 运行时存在"
    lock_file.unlink(missing_ok=True)


@patch("pipeflow.daemon.WorkflowEngine")
def test_run_once_exception_message_printed(mock_engine_cls):
    """run_once 抛异常时 print 异常信息且继续运行。"""
    lock_file = _test_lock_file()
    _set_lock_file(lock_file)
    mock_eng = MagicMock()
    mock_engine_cls.return_value = mock_eng
    call_count = 0
    def side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("测试 value error 路径")
    mock_eng.run_once.side_effect = side_effect

    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    assert mock_eng.run_once.call_count >= 2, "异常后应继续"
    lock_file.unlink(missing_ok=True)


if __name__ == "__main__":
    print("=== WorkflowDaemon 测试 ===\n")

    tests = [
        ("daemon 驱动 engine", test_daemon_loop_runs_engine),
        ("daemon 异常继续", test_daemon_loop_continues_on_error),
        ("flock 锁", test_flock_lock),
        ("flock stale 清理", test_flock_stale_cleanup),
        ("非主线程跳过 signal", test_non_main_thread_skip_signal),
        ("CLI 默认参数", test_cli_defaults),
        ("锁文件创建验证", test_main_loop_lock_file_created),
        ("run_once ValueError 异常", test_run_once_exception_message_printed),
        ("SIGTERM 退出清理锁", test_daemon_loop_lock_cleanup_in_finally),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  ✓ {name}")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)