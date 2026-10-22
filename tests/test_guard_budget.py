#!/usr/bin/env python3
"""守护巡检的预算必须落到 ccs start 子进程上，而不只是角色之间。

实测（2026-10-22 09:5x）：run_once() → _ensure_guard_roles(deadline) →
_ensure_role_alive() → _check_and_start_role() → `ccs start` timeout=60 + 30×1s 轮询。
deadline 只在**角色之间**检查，单个角色就能吃光整轮 tick——全量套件因此在
tests/test_chain_dispatch_contract.py 卡住 15 分钟、日志 0 前进（进程 hrtimer_nanosleep）。

判据：给定 deadline，ccs start 的 subprocess timeout 必须 ≤ 剩余预算；
无 deadline 时保持 60s（生产既有行为）。删掉夹逼后本用例必须变红。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_guard_budget.py -q
"""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow import role_lifecycle as rl


class _Probe(rl.RoleLifecycleMixin):
    def __init__(self):
        self._last_restart = {}
        self._bb = MagicMock()

    def _is_agent_alive(self, role, fresh=False):
        return False   # 判死才会走 ccs start —— 本用例测的正是它的 timeout


class _Clock:
    """sleep 打空：不夹逼时 30×1s 轮询会真等 30 秒。"""
    time = staticmethod(time.time)
    sleep = staticmethod(lambda _s: None)


def _record(monkeypatch):
    seen = []

    def run(args, **kw):
        seen.append((list(args), kw.get("timeout")))
        m = MagicMock()
        m.returncode = 0   # 探针已被桩掉（_is_agent_alive=False）→ 直接走 ccs start
        m.stdout, m.stderr = b"", b""
        return m

    monkeypatch.setattr(rl._engine_mod._sp, "run", run)
    monkeypatch.setattr(rl, "_invalidate_tmux", lambda: None, raising=False)
    monkeypatch.setattr(rl, "time", _Clock)
    return seen


def _starts(seen):
    return [t for a, t in seen if len(a) > 2 and a[2] == "start"]


def test_ccs_start_timeout_clamped_by_deadline(monkeypatch):
    seen = _record(monkeypatch)
    _Probe()._check_and_start_role("probe_budget", deadline=time.time() + 6)
    starts = _starts(seen)
    assert starts, f"没有观察到 ccs start 调用: {seen}"
    assert min(starts) <= 6, f"ccs start 超时未按预算夹逼: {starts}"


def test_exhausted_budget_does_not_start_role(monkeypatch):
    """预算已耗尽时不得拉起：拉起会先盖 300s 冷却戳，掐断后既不在缓存又踩冷却。"""
    seen = _record(monkeypatch)
    assert _Probe()._check_and_start_role("probe_dead", deadline=time.time() - 1) is False
    assert not _starts(seen), f"预算耗尽仍真拉起: {seen}"


def test_ccs_start_timeout_defaults_to_60_without_deadline(monkeypatch):
    seen = _record(monkeypatch)
    _Probe()._check_and_start_role("probe_budget2")
    starts = _starts(seen)
    assert starts and max(starts) == 60, f"无 deadline 时应保持 60s: {starts}"
