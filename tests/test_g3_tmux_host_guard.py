"""G3 自动 stop 不得带走 tmux server 的房东 unit（2026-11-01 12:14 全员中断 66s）。

tmux server 是进程间单例，cgroup 由创建它的 unit 固定。ccs@pm 承载了它，
G3 停 pm 的 session 时 panes 消失 → pm unit 连带 exit → KillMode=control-group
清空 cgroup → 9 个 ccs@* unit 一起重启。误杀成本是全员中断，漏杀只是 idle
角色多活 30 分钟，所以判不了的时候必须按房东处理。
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import routing_daemon  # noqa: E402

_HOST_CG = "/user.slice/user-1000.slice/user@1000.service/app.slice/app-ccs.slice/ccs@pm.service"
_OTHER_CG = "/user.slice/user-1000.slice/user@1000.service/app.slice/app-ccs.slice/ccs@qa.service"


class _R:
    def __init__(self, out="", rc=0):
        self.stdout, self.returncode = out, rc


def _fake_run(server_pid="1092103", control_group=_HOST_CG, raise_on=None):
    """按命令形状回放 tmux / systemctl 的输出，其它子命令一律 rc=1。"""
    calls = []

    def run(cmd, **kw):
        calls.append(list(cmd))
        if raise_on and raise_on in cmd:
            raise OSError("boom")
        if cmd[:2] == ["tmux", "display-message"]:
            return _R(server_pid if server_pid else "", 0 if server_pid else 1)
        if "systemctl" in cmd:
            return _R(control_group, 0)
        return _R("", 1)

    return run, calls


def _patch_run(monkeypatch, run):
    monkeypatch.setattr(routing_daemon.subprocess, "run", run)


def test_host_role_is_skipped(monkeypatch):
    run, _ = _fake_run(control_group=_HOST_CG)
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("pm") is True


def test_non_host_role_is_stopped(monkeypatch):
    run, _ = _fake_run(control_group=_OTHER_CG)
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("qa") is False


def test_inactive_unit_has_no_cgroup_to_take_down(monkeypatch):
    """unit 已 inactive → ControlGroup 为空串 → 停它连坐不了谁，可以 stop。"""
    run, _ = _fake_run(control_group="")
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("consumer") is False


def test_no_tmux_server_means_no_host(monkeypatch):
    """没有 server 时 stop 本来就是空操作（kill-session 走同一 socket）。"""
    run, _ = _fake_run(server_pid=None)
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("pm") is False


def test_probe_failure_fails_safe(monkeypatch):
    """systemctl 都调不起来的宿主上仍要按房东处理——判不了不许误杀。"""
    run, _ = _fake_run(raise_on="systemctl")
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("pm") is True


def test_unreadable_proc_cgroup_fails_safe(monkeypatch):
    run, _ = _fake_run(server_pid="999999999")
    _patch_run(monkeypatch, run)
    assert routing_daemon._role_hosts_tmux_server("pm") is True


def test_autostop_skips_host_and_keeps_content_void(monkeypatch):
    """真正的分叉在调用点：房东不 stop、content_void 不 stop、普通 idle 仍 stop。"""
    run, calls = _fake_run(control_group=_OTHER_CG)
    _patch_run(monkeypatch, run)
    monkeypatch.setattr(routing_daemon, "_role_hosts_tmux_server", lambda role: role == "pm")
    routing_daemon._autostop_idle_roles([
        {"_action": "idle", "role": "pm"},
        {"_action": "idle", "role": "qa"},
        {"_action": "content_void", "role": "scout"},
    ])
    stopped = [c[-1] for c in calls if c[:2] == [sys.executable, str(routing_daemon._paths_mod.CCS_CLI)]]
    assert stopped == ["qa"], stopped
