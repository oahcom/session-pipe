"""角色存活探测的缓存与超时容忍（RoleLifecycleMixin._ensure_role_alive）。

背景（实测 2026-10-22）：一次 tmux has-session 常态 2.4s、压力下 >5s；
_ensure_guard_roles 每 tick 对活跃链的全部 guard/wait 角色各探一次 → 每 tick 几十秒纯开销。
本用例锁住两件事：正向结果被缓存（同一角色第二次不再探）、探针超时按未知处理（不判死、不抛）。
"""
import subprocess
import types

from pipeflow.role_lifecycle import RoleLifecycleMixin


class _FakeSP:
    TimeoutExpired = subprocess.TimeoutExpired

    def __init__(self, returncode=0, raise_exc=None):
        self.calls = 0
        self._rc = returncode
        self._exc = raise_exc

    def run(self, *a, **k):
        self.calls += 1
        if self._exc:
            raise self._exc
        return types.SimpleNamespace(returncode=self._rc, stdout="", stderr="")


class _FakeEngine(RoleLifecycleMixin):
    def __init__(self, sp):
        self._sp = sp
        self.alive = True

    def _is_agent_alive(self, role):
        return self.alive


def _patch_engine(monkeypatch, sp):
    import pipeflow.role_lifecycle as rl
    monkeypatch.setattr(rl, "_engine_mod", types.SimpleNamespace(_sp=sp))


def test_alive_result_is_cached(monkeypatch):
    sp = _FakeSP(returncode=0)
    _patch_engine(monkeypatch, sp)
    e = _FakeEngine(sp)
    assert e._ensure_role_alive("reviewer") is True
    assert e._ensure_role_alive("reviewer") is True
    assert sp.calls == 1, f"第二次探测不该再打 tmux，实际 calls={sp.calls}"


def test_probe_timeout_is_not_death(monkeypatch):
    sp = _FakeSP(raise_exc=subprocess.TimeoutExpired(cmd="tmux", timeout=5))
    _patch_engine(monkeypatch, sp)
    e = _FakeEngine(sp)
    # 探针卡住 ≠ 角色死：按未知处理，不抛异常（上层按未知走，不重启）
    assert e._ensure_role_alive("reviewer") is True
