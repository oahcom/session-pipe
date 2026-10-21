#!/usr/bin/env python3
"""tmux 卡死告警 —— tick 变慢时必须能指出根因，且不能每轮重复报警。

生产证据（2026-10-22）：tmux 服务端单线程卡死，has-session 13~15s /
list-sessions 18s / 单 tick 399s，5 条链卡 step_done_ready 1.5~2.4h。
journal 里只有"tick 耗时 399s"，不指向 tmux。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_tmux_wedge_alarm.py -q
"""
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow import daemon as d  # noqa: E402


def _reset():
    d._TMUX_ALARM_AT = 0.0


def test_probe_timeout_means_wedged():
    _reset()
    with patch.object(d.subprocess, "run", side_effect=subprocess.TimeoutExpired("tmux", 6)):
        assert d._tmux_wedged() is True


def test_probe_without_tmux_is_not_wedged():
    _reset()
    with patch.object(d.subprocess, "run", side_effect=FileNotFoundError):
        assert d._tmux_wedged() is False


def test_alarm_writes_one_blocker_per_window():
    """卡死持续期间只报一次：报警本身要走子进程写 bus，每 tick 一条就是噪音。"""
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    with patch.object(d, "_tmux_wedged", return_value=True), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
        d._tmux_wedge_alarm()  # 窗口内第二次：不应再探、不再写

    assert len(calls) == 1, calls
    assert calls[0][2] == "write" and calls[0][3] == "blocker", calls[0]
    assert "tmux" in calls[0][4]


def test_no_alarm_when_responsive():
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, b"", b"")

    with patch.object(d, "_tmux_wedged", return_value=False), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
    assert calls == [], calls


if __name__ == "__main__":
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            _f()
            print(f"PASS {_n}")
