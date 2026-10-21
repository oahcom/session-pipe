#!/usr/bin/env python3
"""告警冷却必须跨进程重启生效 —— 否则同一条 verify traceback 会反复注入。

生产证据（2026-10-22）：reviewer 转录 f706ce80 的 1114/1482/1561/1606 行是同一条
「[workflow 告警] … 闸门未通过」注入，33 分钟内 4 次；同期 engine 重启 2 次。
原实现 _ALERT_TS 是进程内字典，重启即清空 → 冷却形同虚设。

运行: python3 -m pytest tests/test_alert_cooldown_persist.py -q
"""

import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow import role_lifecycle as rl  # noqa: E402

KEY = ("reviewer", "wf_x s4 闸门未通过")


@pytest.fixture
def state(tmp_path, monkeypatch):
    """冷却表指到 tmp，并自动还原——不碰真实 ~/.hermes/run，也不污染后续用例。"""
    monkeypatch.setattr(rl, "_ALERT_STATE", tmp_path / "alert_cooldowns.json")
    rl.RoleLifecycleMixin._ALERT_TS.clear()
    yield tmp_path
    rl.RoleLifecycleMixin._ALERT_TS.clear()


def _fake(sent_ok=True):
    fake = SimpleNamespace()
    fake._bb = MagicMock()
    for attr in ("_ALERT_TS", "_ALERT_COOLDOWN_S", "_ALERT_TTL_S", "_ALERT_RETRY_S"):
        setattr(fake, attr, getattr(rl.RoleLifecycleMixin, attr))
    fake.notify = rl.RoleLifecycleMixin._notify_role.__get__(fake)
    rl._engine_mod = SimpleNamespace(
        _sp=SimpleNamespace(run=MagicMock(return_value=SimpleNamespace(returncode=0 if sent_ok else 7)))
    )
    return fake


def test_same_alert_suppressed_within_window(state):
    fake = _fake()
    fake.notify(*KEY, "...")
    fake.notify(*KEY, "...")
    assert rl._engine_mod._sp.run.call_count == 1, "同一 (role,title) 30min 内只该投一次"
    assert fake._bb.write.call_count == 1, "被抑制的那次不该再写 blocker"


def test_cooldown_survives_restart(state):
    """关键用例：进程重启后（内存表清空，只剩磁盘）仍然抑制。"""
    _fake().notify(*KEY, "...")
    assert (state / "alert_cooldowns.json").exists(), "占位后必须落盘"
    rl.RoleLifecycleMixin._ALERT_TS.clear()          # 模拟 daemon 重启
    _fake().notify(*KEY, "...")
    assert rl._engine_mod._sp.run.call_count == 0, "重启后仍应抑制同一告警"


def test_stale_snapshot_does_not_clobber_other_process(state):
    """两个进程各持旧快照：后写的不能把对方刚占的位抹掉（codex 20 维 P1）。"""
    path = state / "alert_cooldowns.json"
    rl._save_alert_ts(path, {("a", "t1"): 100.0}, 100.0, ttl=10_000)
    # 进程 B 的内存表里没有 a/t1（它是重启后加载失败的旧快照），直接回写
    rl._save_alert_ts(path, {("b", "t2"): 200.0}, 200.0, ttl=10_000)
    saved = rl._load_alert_ts(path, 200.0, ttl=10_000)
    assert set(saved) == {("a", "t1"), ("b", "t2")}, "合并写：谁都不该丢"


def test_save_prunes_memory_and_caps_entries(state):
    """内存表跟着裁剪，且文件有条数上限（长驻 daemon 不能无界增长）。"""
    path = state / "alert_cooldowns.json"
    live = {("r", f"t{i}"): 1000.0 + i for i in range(rl._ALERT_MAX_ENTRIES + 30)}
    live[("r", "expired")] = 1.0
    rl.RoleLifecycleMixin._ALERT_TS.clear()
    rl.RoleLifecycleMixin._ALERT_TS.update(live)
    rl._save_alert_ts(path, rl.RoleLifecycleMixin._ALERT_TS, 1001.0, ttl=100)
    assert ("r", "expired") not in rl.RoleLifecycleMixin._ALERT_TS, "过期条目要移出内存表"
    assert len(rl._load_alert_ts(path, 1001.0, ttl=10_000)) <= rl._ALERT_MAX_ENTRIES


def test_different_title_still_delivered(state):
    fake = _fake()
    fake.notify(*KEY, "...")
    fake.notify("reviewer", "wf_y s2 返工链等待超 60 分钟", "...")
    assert rl._engine_mod._sp.run.call_count == 2


def test_expired_entry_no_longer_suppresses(state):
    rl._save_alert_ts(state / "alert_cooldowns.json", {KEY: 1.0}, 1e9, ttl=1e9)
    _fake().notify(*KEY, "...")
    assert rl._engine_mod._sp.run.call_count == 1, "过期条目不该继续抑制"


def test_corrupt_state_file_fails_open(state):
    """坏文件只该让冷却失效，不该让告警丢失。"""
    (state / "alert_cooldowns.json").write_text("{not json")
    _fake().notify(*KEY, "...")
    assert rl._engine_mod._sp.run.call_count == 1


def test_failed_delivery_retries_soon(state):
    """投递失败退避 600s，不还回整窗 30min。"""
    _fake(sent_ok=False).notify(*KEY, "...")
    saved = rl._load_alert_ts(state / "alert_cooldowns.json", time.time(), ttl=1e9)
    gap = time.time() - saved[KEY]
    assert 1100 < gap < 1300, f"失败后应占位 now-1800+600（gap≈1200），实测 {gap:.0f}"
