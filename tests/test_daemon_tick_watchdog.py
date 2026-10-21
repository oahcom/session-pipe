"""tick 看门狗：run_once 卡住时 daemon 必须自己把栈打到 stderr。

事故背景（2026-10-22）：单 tick 静默 18 分钟，进程无日志、无子进程、CPU 近零，
无 root 拿不到栈 → 四种归因都无法证伪。daemon_loop 现在用 faulthandler 定时代打。
"""
import os
import subprocess
import sys
import tempfile
import textwrap

PROG = textwrap.dedent("""
    import os, sys, time
    from pathlib import Path
    sys.path.insert(0, "src")
    import pipeflow.daemon as d

    class Slow:
        def __getattr__(self, name):
            return lambda *a, **k: 0
        def run_once(self):
            time.sleep(3)      # 比 TICK_STACK_DUMP_SEC=1 长 → 途中必须打栈
            raise SystemExit(0)   # 走 finally，验证慢 tick 计时日志

    d.WorkflowEngine = lambda *a, **k: Slow()
    d._LOCK_FILE = Path(os.environ["LOCK_PATH"])
    d.daemon_loop(0)
""")


def test_stuck_tick_dumps_stack():
    with tempfile.TemporaryDirectory() as tmp:
        env = dict(os.environ, TICK_STACK_DUMP_SEC="1", TICK_SLOW_SEC="0",
                   LOCK_PATH=os.path.join(tmp, "daemon.lock"))
        r = subprocess.run([sys.executable, "-c", PROG], env=env, timeout=60,
                           capture_output=True, text=True)
    err = r.stdout + r.stderr
    assert "run_once" in err, f"卡住的 tick 没打出栈:\n{err[-800:]}"
    assert "tick 耗时" in err, f"慢 tick 没有计时日志:\n{err[-800:]}"
