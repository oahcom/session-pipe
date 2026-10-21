"""RequirementConfirmationChecker 的回归：判据来自 consume 记录，全局一次。

背景（2026-10-22）：旧实现用 bus.search("role:x cat:y") 找需求——FTS5 不支持
这种限定符，恒返回 0 条，于是这个检查自诞生起一直"通过"；它还传了不存在的
unconsumed(limit=1)，每轮 eval 静默抛 TypeError 被吞。下面的断言锁住新口径。
"""
import sys

sys.path.insert(0, "src")

import eval_checker as ec  # noqa: E402
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


def _checker(bb, ttl_days=90):
    c = object.__new__(RequirementConfirmationChecker)
    c._bb, c.ttl_days = bb, ttl_days
    return c


def test_counts_all_unconsumed_without_truncation():
    import time
    bb = _FakeBB([_FakeFact(1000 + i, time.time()) for i in range(25)])
    r = _checker(bb).check()
    assert r["checked_requirements"] == 25, "超过 20 条必须全数计入，否则告警口径偏小"
    assert r["passed"] is False


def test_query_error_fails_closed_and_is_not_a_requirement():
    """查询异常不能变成"通过"，也不能冒充成"27 条需求没人消费"。"""
    r = _checker(_FakeBB(boom_on="task_spec")).check()
    assert r["passed"] is False
    assert r["checked_requirements"] == 0, "查询失败不是需求条目，不能计入需求数"
    assert r["errors"] and "task_spec" in r["errors"][0]


def test_clean_bus_passes():
    r = _checker(_FakeBB([])).check()
    assert r["passed"] is True and r["checked_requirements"] == 0 and r["errors"] == []


def test_window_covers_every_requirement_category():
    bb = _FakeBB([])
    _checker(bb, ttl_days=90).check()
    assert {c["cat"] for c in bb.calls} == RequirementConfirmationChecker.REQUIREMENT_CATS, "5 个分类都要扫，一个都不能省"
    assert all(c["ttl_days"] == 90 for c in bb.calls), "窗口必须透传给 bus"


def _stub_pipeline(monkeypatch, counter):
    """把 run_eval_check 里除需求检查外的重活（真跑命令、读转录）全部短路。"""
    monkeypatch.setattr(ec, "_load_roles", lambda: [
        {"name": "engineer", "eval_criteria": []},
        {"name": "qa", "eval_criteria": []},
    ])
    monkeypatch.setattr(ec, "_write_bus", lambda *a, **k: None)

    class _Stub:
        def __init__(self, *a, **k):
            pass

        def check(self, *a, **k):
            return {"passed": True, "findings": [], "checked": 0}

    for name in ("OutputSemanticChecker", "TrajectoryQualityChecker"):
        monkeypatch.setattr(ec, name, _Stub)

    class _FakeReqc:
        def __init__(self, *a, **k):
            counter["ctor"] += 1

        def check(self):
            counter["check"] += 1
            return {"passed": True, "findings": [], "errors": [], "checked_requirements": 0}

    monkeypatch.setattr(ec, "RequirementConfirmationChecker", _FakeReqc)


def test_runs_once_per_round_not_per_role(monkeypatch):
    """采样到 2 个角色时，需求检查仍只跑一次（消费是全局事实）。"""
    counter = {"ctor": 0, "check": 0}
    _stub_pipeline(monkeypatch, counter)
    monkeypatch.setattr(ec.random, "choices", lambda roles, weights, k: roles[:2])
    ec.run_eval_check()
    assert counter == {"ctor": 1, "check": 1}


def test_skipped_when_role_filter_scopes_the_run(monkeypatch):
    """role_filter 是"只看某角色"，全局扫描与之冲突 → 不跑。"""
    counter = {"ctor": 0, "check": 0}
    _stub_pipeline(monkeypatch, counter)
    ec.run_eval_check(role_filter="engineer")
    assert counter == {"ctor": 0, "check": 0}


def _checker_with_real_ctor(monkeypatch, bb, ttl_days):
    """走真实构造器（stub 掉 Blackboard），验 ttl_days 的边界语义。"""
    import types
    fake = types.ModuleType("hermes_bus.blackboard_legacy")
    fake.Blackboard = lambda *a, **k: bb
    monkeypatch.setitem(sys.modules, "hermes_bus.blackboard_legacy", fake)
    return RequirementConfirmationChecker(ttl_days=ttl_days)


def test_non_positive_ttl_means_no_window(monkeypatch):
    """ttl<=0 = 关掉年龄过滤，必须真的无窗口（inf）。

    换成"很大的天数"只是近似；负数更糟：cutoff 被推到未来 → 一条都查不到，
    而"一条都查不到"会被读成"全部已确认"——正是本轮要消灭的假绿。
    """
    bb = _FakeBB([])
    _checker_with_real_ctor(monkeypatch, bb, 0).check()
    assert bb.calls[0]["ttl_days"] == float("inf")


def test_negative_ttl_also_means_no_window(monkeypatch):
    bb = _FakeBB([])
    _checker_with_real_ctor(monkeypatch, bb, -5).check()
    assert bb.calls[0]["ttl_days"] == float("inf")


def test_nan_ttl_means_no_window(monkeypatch):
    """NaN 会让 SQL 变成 f.ts >= NULL → 一条都查不到 → 假通过。必须映射成 inf。"""
    bb = _FakeBB([])
    _checker_with_real_ctor(monkeypatch, bb, float("nan")).check()
    assert bb.calls[0]["ttl_days"] == float("inf")

