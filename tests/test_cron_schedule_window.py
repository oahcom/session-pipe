"""routing_daemon._check_cron_schedules 的窗口判定与账本剔除契约。

三条缺陷各一组用例：
  1. 采样 300s vs 判据 60s → `0 4 * * *` 每天只有 20% 概率被采到（#458204）
  2. croniter 对 float 种子按 UTC 解析 → `0 4 * * *` 错位成本地 12:00
  3. 账本整体读写，从不删已退场角色的键 → 冻结条目永不复位

`_is_role_alive` 是 `_check_cron_schedules` 内的嵌套函数，不是模块属性，
所以只能通过替换 `subprocess.run` 按 `status-role` 子命令喂返回值。
"""
import json
import sys
import time as _time
import types
from pathlib import Path

import pytest

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

import routing_daemon as rd  # noqa: E402


def _at(stamp: str) -> float:
    return _time.mktime(_time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """隔离角色目录 / 账本 / 时间 / 子进程，返回 (roles_dir, tracker, sent)。

    `rd.time` 换成桩对象而不是 monkeypatch `time.time` —— 后者会改全局 time 模块，
    连带影响 pytest 自己的计时。
    """
    roles = tmp_path / "personas" / "session-roles"
    roles.mkdir(parents=True)
    tracker = tmp_path / "tracker.json"
    sent: list[str] = []
    state = {"now": 0.0, "alive": True}

    def fake_run(cmd, **kw):
        if "status-role" in cmd:
            return types.SimpleNamespace(
                returncode=0, stdout=json.dumps({"alive": state["alive"]}))
        if "send" in cmd:
            sent.append(cmd[3])          # [exe, launcher, "send", role, msg, ...]
            return types.SimpleNamespace(returncode=0, stdout="")
        raise AssertionError(f"未预期的子进程调用: {cmd}")

    stub = types.ModuleType("session_loader")
    stub.get_roles_dir = lambda: tmp_path
    monkeypatch.setitem(sys.modules, "session_loader", stub)
    monkeypatch.setattr(rd, "CRON_SCHEDULE_TRACKER", tracker)
    monkeypatch.setattr(rd, "time", types.SimpleNamespace(time=lambda: state["now"]))
    # 换 rd.subprocess 这个名字，而不是 patch 它的 run 属性：subprocess 是进程级
    # 共享模块，就地替换 run 会让 pytest 自己的 check_output 也走进 fake_run
    # （实测整套 collection 抛异常）。保留两个异常类，_is_role_alive 要用它们做 except。
    import subprocess as _sp
    monkeypatch.setattr(rd, "subprocess", types.SimpleNamespace(
        run=fake_run, TimeoutExpired=_sp.TimeoutExpired,
        SubprocessError=_sp.SubprocessError))

    def add(role: str, expr: str) -> None:
        (roles / f"persona_{role}.json").write_text(
            json.dumps({"name": role, "cron_schedule": expr}), encoding="utf-8")

    return types.SimpleNamespace(
        tracker=tracker, sent=sent, state=state, add=add,
        run=lambda now: (state.update(now=now), rd._check_cron_schedules())[1],
        ledger=lambda: json.loads(tracker.read_text()),
        seed=lambda d: tracker.write_text(json.dumps(d)),
    )


def test_daily_window_fires_when_sample_misses_the_minute(sandbox):
    """04:03 采样时 04:00 的调度点已出窗 —— 仍必须触发（旧判据要求 prev 在当前这一分钟）。"""
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({"optimizer:0 4 * * *": _at("2026-10-26 04:00:05")})
    assert sandbox.run(_at("2026-10-27 04:03:00")) == 1
    assert sandbox.sent == ["optimizer"]


def test_same_schedule_point_fires_once(sandbox):
    """同一调度点被两次采样覆盖时只触发一次（区间判据必须自带幂等）。"""
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({"optimizer:0 4 * * *": _at("2026-10-26 04:00:05")})
    first = sandbox.run(_at("2026-10-27 04:01:00"))
    second = sandbox.run(_at("2026-10-27 04:04:00"))
    assert (first, second) == (1, 0), f"同一调度点重复触发: {first}, {second}"
    assert sandbox.sent == ["optimizer"]


def test_hourly_expression_uses_local_time(sandbox):
    """`0 4 * * *` 认本地 04:00。

    float 种子被 croniter 按 UTC 解析 → 本地 04:02 时 prev 是【昨天 12:00】。
    账本里恰好记着昨天 12:00（UTC 错位下的上一次触发）时，旧代码判定「已发过」
    而不再触发；datetime 种子下 prev=今天 04:00，大于账本值，必须补发。
    """
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({"optimizer:0 4 * * *": _at("2026-10-26 12:00:00")})
    assert sandbox.run(_at("2026-10-27 04:02:00")) == 1, "本地 04:00 的班被 UTC 错位吃掉"
    assert sandbox.sent == ["optimizer"]


def test_same_day_does_not_fire_again_at_noon(sandbox):
    """同一自然日只发一班：04:02 发过之后，12:02 不得再发。"""
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({})
    assert sandbox.run(_at("2026-10-27 04:02:00")) == 1
    assert sandbox.run(_at("2026-10-27 12:02:00")) == 0, "同一天上午已发，中午又发了一次"
    assert sandbox.sent == ["optimizer"]


def test_retired_role_key_is_pruned(sandbox):
    """已退场角色的键必须从账本删掉；在役角色的键不得被误删。"""
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({"optimizer:0 4 * * *": _at("2026-10-26 04:00:05"),
                  "curator:*/15 * * * *": _at("2026-10-21 17:30:00")})
    sandbox.run(_at("2026-10-27 04:03:00"))
    left = sandbox.ledger()
    assert "curator:*/15 * * * *" not in left, f"退场角色键仍在: {left}"
    assert "optimizer:0 4 * * *" in left, f"在役角色键被误删: {left}"


def test_dead_role_is_skipped_and_ledger_not_advanced(sandbox):
    """角色没在跑时不发消息，且账本不推进 —— 否则这一班被永久消费，活了也补不到。"""
    sandbox.add("optimizer", "0 4 * * *")
    sandbox.seed({})
    sandbox.state["alive"] = False
    assert sandbox.run(_at("2026-10-27 04:03:00")) == 0
    assert sandbox.sent == []
    assert sandbox.ledger() == {}, "死角色不该推进账本"

    # 同一窗口内角色活过来 → 仍应收得到这一班
    sandbox.state["alive"] = True
    assert sandbox.run(_at("2026-10-27 04:05:00")) == 1, "窗口内复活却补不到这一班"
    assert sandbox.sent == ["optimizer"]
