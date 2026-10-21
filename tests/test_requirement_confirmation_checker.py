"""RequirementConfirmationChecker 的回归：判据来自 consume 记录，不按角色。

背景（2026-10-22）：旧实现用 bus.search("role:x cat:y") 找需求——FTS5 不支持
这种限定符，恒返回 0 条，于是这个检查自诞生起一直"通过"；它还传了不存在的
unconsumed(limit=1)，每轮 eval 静默抛 TypeError 被吞。下面的断言锁住新口径。
"""
import sys
import types

import pytest

sys.path.insert(0, "src")

from eval_checker import RequirementConfirmationChecker  # noqa: E402


class _FakeFact:
    def __init__(self, fid, ts):
        self.id, self.ts, self.cat = fid, ts, "task_spec"


class _FakeBB:
    def __init__(self, facts=None, boom_on=None):
        self._facts = facts or []
        self._boom_on = boom_on
        self.calls = []

    def unconsumed(self, *, consumer=None, cat=None, role=None, ttl_days=30):
        self.calls.append({"cat": cat, "consumer": consumer, "ttl_days": ttl_days})
        if cat == self._boom_on:
            raise RuntimeError("db locked")
        return list(self._facts) if cat == "task_spec" else []


def _checker(bb):
    c = object.__new__(RequirementConfirmationChecker)
    c._bb = bb
    return c


def test_counts_all_unconsumed_without_truncation():
    import time
    bb = _FakeBB([_FakeFact(1000 + i, time.time()) for i in range(25)])
    r = _checker(bb).check()
    assert r["checked_requirements"] == 25, "超过 20 条必须全数计入，否则告警口径偏小"
    assert r["passed"] is False


def test_query_error_fails_closed():
    """查询异常不能变成"通过"——旧实现正是这样假绿的。"""
    bb = _FakeBB(boom_on="task_spec")
    r = _checker(bb).check()
    assert r["passed"] is False
    assert any("check error" in f["issue"] for f in r["findings"])


def test_clean_bus_passes():
    r = _checker(_FakeBB([])).check()
    assert r["passed"] is True and r["checked_requirements"] == 0


def test_window_aligned_with_bus_retention():
    bb = _FakeBB([])
    _checker(bb).check()
    assert bb.calls and all(c["ttl_days"] == 90 for c in bb.calls), (
        "窗口必须与 prune 保留期对齐，30 天会把更老的未确认需求静默漏掉"
    )
