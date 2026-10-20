"""子链创建必须受 per-role 并发上限约束 —— 否则全部堆在一个角色上。

现场（2026-10-21 06:30）：product_architect 有 9 个 running 实例、5 条活跃子链，
6 个父链（WL-full_dev_pipeline / WF-architect_review / WL-02）的 s1 全部在排队，
queued 145~514。`MAX_RUNNING_PER_ROLE=3` 的背压写在 `_can_assign_role`（任务分配层），
而 `_subflow_takeover` 是**第二条派发路径**，从不走那里 —— 角色每空出一瞬就建一条
新子链，堆到 9 条。

本契约：目标角色已有 >= MAX_RUNNING_PER_ROLE 条活跃子链时，不创建新子链，
返回 falsy 让父步自投（等现有子链消化完自然恢复建链）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_subflow_concurrency_cap.py -q
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.task_generator import MAX_RUNNING_PER_ROLE  # noqa: E402
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402


class _FakeConn:
    """只回应 _subflow_takeover 会发的那几条查询。"""

    def __init__(self, active_subflows: int, subflow_template_exists: bool = True):
        self._n = active_subflows
        self._has_tpl = subflow_template_exists

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        m = MagicMock()
        if "COUNT(*)" in s and "parent_wf_id IS NOT NULL" in s:
            m.fetchone.return_value = {"c": self._n}
        elif "SELECT step_results FROM workflow_instances" in s:
            m.fetchone.return_value = {"step_results": "{}"}
        elif "WHERE parent_wf_id=? AND subflow_source_step_id=?" in s:
            m.fetchone.return_value = None          # 无权威链接回退
        elif "SELECT assigner FROM workflow_instances" in s:
            m.fetchone.return_value = {"assigner": "pm"}
        else:
            m.fetchone.return_value = None
        return m

    def commit(self):
        pass


class _Tx:
    def __init__(self, conn):
        self._c = conn

    def __enter__(self):
        return self._c

    def __exit__(self, *a):
        return False


class _LM:
    def __init__(self, conn):
        self._conn_tx = lambda: _Tx(conn)


class _Host(WorkflowLifecycleMixin):
    def __init__(self, conn, tpl_role="product_architect"):
        self._lifecycle = _LM(conn)
        self._workflows = {}
        step = MagicMock()
        step.target_role = tpl_role
        wf = MagicMock()
        wf.steps = [step]
        self._workflows["architect_full_design"] = wf

    def _is_role_busy(self, *a, **kw):
        return False        # 角色空闲：让并发上限成为唯一拦路者


def _step():
    s = MagicMock()
    s.id = "s1"
    s.target_role = "product_architect"
    s.subflow_template = "architect_full_design"
    s.title = "需求分析"
    return s


@pytest.mark.parametrize("n,blocked", [
    (0, False), (1, False), (2, False),
    (MAX_RUNNING_PER_ROLE, True), (MAX_RUNNING_PER_ROLE + 5, True),
])
def test_subflow_creation_respects_per_role_cap(n, blocked):
    """活跃子链数 >= 上限 → 不创建（返回 falsy）。"""
    conn = _FakeConn(active_subflows=n)
    h = _Host(conn)
    out = h._subflow_takeover(_LM(conn), "wf_parent", "WL-x", _step(), -1, {}, "s0")
    if blocked:
        assert not out, f"{n} 条活跃子链（上限 {MAX_RUNNING_PER_ROLE}）仍创建了新子链"
    else:
        # 未满额时会走到建链（INSERT ... workflow_instances）→ 返回 True
        assert out is True, f"{n} 条时应当创建子链"


def test_cap_query_counts_only_active_subflows():
    """上限只统计 parent_wf_id 非空的活跃实例，不把父链算进来。"""
    conn = _FakeConn(active_subflows=0)
    h = _Host(conn)
    captured = []
    orig = conn.execute

    def spy(sql, params=()):
        captured.append(" ".join(sql.split()))
        return orig(sql, params)

    conn.execute = spy
    h._subflow_takeover(_LM(conn), "wf_parent", "WL-x", _step(), -1, {}, "s0")
    cap_sql = [s for s in captured if "parent_wf_id IS NOT NULL" in s]
    assert cap_sql, "未发并发上限查询"
    assert "assignee=?" in cap_sql[0], "上限必须按角色统计"


if __name__ == "__main__":
    import tempfile
    for n, blocked in [(0, False), (2, False), (3, True), (8, True)]:
        test_subflow_creation_respects_per_role_cap(n, blocked)
        print(f"  OK  {n} 条活跃子链 → {'拦截' if blocked else '创建'}")
    test_cap_query_counts_only_active_subflows()
    print("  OK  上限查询只统计子链")
    print("全部通过")
