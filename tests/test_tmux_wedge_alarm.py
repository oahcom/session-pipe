#!/usr/bin/env python3
"""tmux 卡死自愈 + 告警 —— 卡死先自愈（杀 utempter），失败才升级。

生产证据（2026-10-22）：tmux 单线程 server 停在 waitpid(utempter)，
所有 tmux 命令排队 12~50s（has-session 13~15s / list-sessions 18s /
display 51.8s），单 tick 399s，5 条链卡 step_done_ready 1.5~2.4h。
utempter 是 libutempter fork 出来写 /var/run/utmp 的 setuid 助手。

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


def _cp(cmd, rc=0, out=b"", err=b""):
    return subprocess.CompletedProcess(cmd, rc, out, err)


def test_probe_timeout_means_wedged():
    _reset()
    with patch.object(d.subprocess, "run", side_effect=subprocess.TimeoutExpired("tmux", 6)):
        assert d._tmux_wedged() is True


def test_probe_without_tmux_is_not_wedged():
    _reset()
    with patch.object(d.subprocess, "run", side_effect=FileNotFoundError):
        assert d._tmux_wedged() is False


def test_stuck_utempter_parsed():
    """pgrep -x utempter 的输出要解析成 pid 列表（-x 只匹配进程名，
    避免把命令行里带 utempter 的自家 shell 也算进去）。"""
    with patch.object(d.subprocess, "run",
                      return_value=_cp(["pgrep"], out=b"1234\n5678\n")):
        assert d._tmux_stuck_utempter() == [1234, 5678]


def test_recover_kills_utempter_then_reprobes():
    killed = []
    with patch.object(d, "_tmux_stuck_utempter", return_value=[42]), \
            patch.object(d.os, "kill", side_effect=lambda p, s: killed.append(p)), \
            patch.object(d, "_tmux_ok", return_value=True):
        assert d._tmux_recover() is True
    assert killed == [42]


def test_alarm_self_heals_without_bus_write():
    """自愈成功就不再升级：升级是给人工看的最后手段。"""
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return _cp(cmd)

    with patch.object(d, "_tmux_wedged", return_value=True), \
            patch.object(d, "_tmux_ok", return_value=True), \
            patch.object(d, "_tmux_stuck_utempter", return_value=[7]), \
            patch.object(d.os, "kill"), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
    assert calls == [], calls


def test_alarm_escalates_when_recover_fails():
    """自愈无效仍卡死 → 必须升级（写 blocker），否则现场没人知道。"""
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return _cp(cmd)

    with patch.object(d, "_tmux_wedged", return_value=True), \
            patch.object(d, "_tmux_ok", return_value=False), \
            patch.object(d, "_tmux_stuck_utempter", return_value=[7]), \
            patch.object(d.os, "kill"), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
    assert len(calls) == 1, calls
    assert calls[0][2] == "write" and calls[0][3] == "blocker", calls[0]


def test_alarm_writes_one_blocker_per_window():
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return _cp(cmd)

    with patch.object(d, "_tmux_wedged", return_value=True), \
            patch.object(d, "_tmux_ok", return_value=False), \
            patch.object(d, "_tmux_stuck_utempter", return_value=[]), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
        d._tmux_wedge_alarm()  # 窗口内第二次：不再探、不再写
    assert len(calls) == 1, calls


def test_no_alarm_when_responsive():
    _reset()
    calls = []

    def _fake_run(cmd, **kw):
        calls.append(list(cmd))
        return _cp(cmd)

    with patch.object(d, "_tmux_wedged", return_value=False), \
            patch.object(d.subprocess, "run", side_effect=_fake_run):
        d._tmux_wedge_alarm()
    assert calls == [], calls


if __name__ == "__main__":
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            _f()
            print(f"PASS {_n}")


def test_ok_requires_positive_answer():
    """自愈判定必须看 rc==0：tmux 二进制缺失或服务端已死（rc!=0）时
    not _tmux_wedged() 会误判成「已恢复」，从而吞掉升级（review P1）。"""
    with patch.object(d.subprocess, "run", return_value=_cp(["tmux"], rc=1)):
        assert d._tmux_ok() is False
    with patch.object(d.subprocess, "run", return_value=_cp(["tmux"], rc=0)):
        assert d._tmux_ok() is True
    with patch.object(d.subprocess, "run", side_effect=FileNotFoundError):
        assert d._tmux_ok() is False


def test_only_helpers_older_than_30s_are_killed():
    """pgrep 必须带 --older：正常助手活不过几十毫秒，不能误杀健康的
    （review P2）。"""
    seen = {}

    def _fake(cmd, **kw):
        seen["cmd"] = list(cmd)
        return _cp(cmd, out=b"9\n")

    with patch.object(d.subprocess, "run", side_effect=_fake):
        assert d._tmux_stuck_utempter() == [9]
    assert "--older" in seen["cmd"] and "30" in seen["cmd"], seen["cmd"]
