"""优雅关闭必须有硬上限：信号只置位，卡住的 tick 让进程停在 deactivating。

事故背景（2026-10-22 实测两次）：systemctl restart 后旧进程停在 deactivating 最长 >4min，
systemd 的 TimeoutStopSec=15 没生效，新进程拉不起来 —— 只能手动 kill -s SIGKILL。
根因是关闭只是置位（daemon.py 的 _handler），正在跑的那次 run_once 要自己走到 while 条件
才看得到标志，而单次 tick 实测 190–317s。
修法：置位的同时起看门狗线程，到点 os._exit（WAL 保证未提交事务回滚）。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_daemon_shutdown_grace.py -q
"""
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

PROG = textwrap.dedent("""
    import os, sys, time
    from pathlib import Path
    sys.path.insert(0, "src")
    import pipeflow.daemon as d

    class Slow:
        def __getattr__(self, name):
            return lambda *a, **k: 0
        def run_once(self):
            time.sleep(60)      # 模拟卡住的 tick：60s >> 优雅关闭上限

    d.WorkflowEngine = lambda *a, **k: Slow()
    d._LOCK_FILE = Path(os.environ["LOCK_PATH"])
    d.daemon_loop(0)
""")


def test_sigterm_force_exits_when_tick_stuck():
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ, LOCK_PATH=str(Path(tmp) / "daemon.lock"),
                   TICK_STACK_DUMP_SEC="0", TICK_SLOW_SEC="0")
        p = subprocess.Popen([sys.executable, "-c", PROG], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True)
        time.sleep(4)                      # 确保已进入 run_once 的长睡
        p.send_signal(signal.SIGTERM)
        t0 = time.time()
        try:
            out, _ = p.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()
            raise AssertionError("SIGTERM 后 30s 仍未退出：优雅关闭没有上限")
        elapsed = time.time() - t0
    assert p.returncode == 0, f"退出码 {p.returncode}: {(out or '')[-500:]}"
    assert elapsed < 15, f"关闭耗时 {elapsed:.0f}s（期望 ≤{_SHUTDOWN_GRACE_SEC}+余量）"
    assert "强制退出" in (out or ""), f"看门狗没打强制退出日志:\n{(out or '')[-600:]}"


_SHUTDOWN_GRACE_SEC = 10  # 与 src/pipeflow/daemon.py 的 _SHUTDOWN_GRACE_SEC 对齐
