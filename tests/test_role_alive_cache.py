"""角色存活探测的缓存（RoleLifecycleMixin._ensure_role_alive）。

背景（实测 2026-10-22）：存活判据此前是 has-session + _is_agent_alive 两次往返
（各 2.4s、压力下 >5s），_ensure_guard_roles 每 tick 对全部 guard/wait 角色各探一次。
现在判据统一为 pane 快照，本用例锁住：正向结果被缓存（同一角色第二次不再探）。
"""
from pipeflow.role_lifecycle import RoleLifecycleMixin


class _FakeEngine(RoleLifecycleMixin):
    def __init__(self, alive=True):
        self.alive = alive
        self.probes = 0

    def _is_agent_alive(self, role, fresh=False):
        self.probes += 1
        return self.alive


def test_alive_result_is_cached():
    e = _FakeEngine()
    assert e._ensure_role_alive("reviewer") is True
    assert e._ensure_role_alive("reviewer") is True
    assert e.probes == 1, f"第二次探测不该再探，实际 probes={e.probes}"
