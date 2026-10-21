#!/usr/bin/env python3
"""告警冷却必须跨进程重启生效 —— 否则同一条 verify traceback 会反复注入。

生产证据（2026-10-22）：reviewer 转录 f706ce80 的 1114/1482/1561/1606 行是同一条
「[workflow 告警] … 闸门未通过」注入，33 分钟内 4 次；同期 engine 重启 2 次。
原实现 _ALERT_TS 是进程内字典，重启即清空 → 冷却形同虚设，每次注入都要角色花
一整个回合重读同一段话。

运行: python3 -m pytest tests/test_alert_cooldown_persist.py -q
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow import role_lifecycle as rl  # noqa: E402


def _fresh(tmp_path, *, sent_ok=True):
    """造一个只会被调一次告警的 self，并把冷却表指到 tmp（不碰真实 run 目录）。"""
    rl._ALERT_STATE = tmp_path / "alert_cooldowns.json"
    rl.RoleLifecycleMixin._ALERT_TS.clear()
    rl.RoleLifecycleMixin._ALERT_STATE_LOADED = False
    fake = SimpleNamespace()
    fake._bb = MagicMock()
    fake._ALERT_TS = rl.RoleLifecycleMixin._ALERT_TS
    fake._ALERT_STATE_LOADED = False
    fake._ALERT_COOLDOWN_S = rl.RoleLifecycleMixin._ALERT_COOLDOWN_S
    fake._ALERT_TTL_S = rl.RoleLifecycleMixin._ALERT_TTL_S
    fake._ALERT_RETRY_S = rl.RoleLifecycleMixin._ALERT_RETRY_S
    fake.notify = rl.RoleLifecycleMixin._notify_role.__get__(fake)
    rl._engine_mod = SimpleNamespace(
        _sp=SimpleNamespace(run=MagicMock(return_value=SimpleNamespace(returncode=0 if sent_ok else 7)))
    )
    return fake


def test_same_alert_suppressed_within_window(tmp_path):
    fake = _fresh(tmp_path)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "缺 REVIEW.md")
    fake.notify("reviewer", "wf_x s4 闸门未通过", "缺 REVIEW.md")
    assert rl._engine_mod._sp.run.call_count == 1, "同一 (role,title) 30min 内只该投一次"
    assert fake._bb.write.call_count == 1, "被抑制的那次不该再写 blocker"


def test_cooldown_survives_restart(tmp_path):
    """关键用例：进程重启后（内存表清空）仍然抑制。"""
    fake = _fresh(tmp_path)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "缺 REVIEW.md")
    assert tmp_path.joinpath("alert_cooldowns.json").exists(), "占位后必须落盘"

    # 模拟 daemon 重启：内存表清空，只剩磁盘
    rl.RoleLifecycleMixin._ALERT_TS.clear()
    again = _fresh(tmp_path)          # 新 self，等价于新进程
    again._ALERT_TS.clear()
    again.notify("reviewer", "wf_x s4 闸门未通过", "缺 REVIEW.md")
    assert rl._engine_mod._sp.run.call_count == 0, "重启后仍应抑制同一告警"


def test_different_title_still_delivered(tmp_path):
    """冷却按 (role, title) 走，换了标题就是新信息，必须投。"""
    fake = _fresh(tmp_path)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "...")
    fake.notify("reviewer", "wf_y s2 返工链等待超 60 分钟", "...")
    assert rl._engine_mod._sp.run.call_count == 2


def test_expired_entry_no_longer_suppresses(tmp_path):
    fake = _fresh(tmp_path)
    rl._save_alert_ts(tmp_path / "alert_cooldowns.json",
                      {("reviewer", "wf_x s4 闸门未通过"): 1.0}, 1e9, ttl=1e9)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "...")
    assert rl._engine_mod._sp.run.call_count == 1, "过期条目不该继续抑制"


def test_corrupt_state_file_fails_open(tmp_path):
    """坏文件只该让冷却失效，不该让告警丢失。"""
    p = tmp_path / "alert_cooldowns.json"
    p.write_text("{not json")
    fake = _fresh(tmp_path)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "...")
    assert rl._engine_mod._sp.run.call_count == 1


def test_failed_delivery_retries_soon(tmp_path):
    """投递失败退避 600s，不还回整窗 30min。"""
    fake = _fresh(tmp_path, sent_ok=False)
    fake.notify("reviewer", "wf_x s4 闸门未通过", "...")
    saved = rl._load_alert_ts(tmp_path / "alert_cooldowns.json", rl.time.time(), ttl=1e9)
    ts = saved[("reviewer", "wf_x s4 闸门未通过")]
    gap = rl.__dict__["time"].time() - ts
    assert 1100 < gap < 1300, f"失败后应占位 now-1800+600（gap≈1200），实测 {gap:.0f}"
