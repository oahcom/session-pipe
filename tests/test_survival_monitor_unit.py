#!/usr/bin/env python3
"""
test_survival_monitor_unit.py — SurvivalMonitor 三层存活检测单元测试

覆盖：
- L1: 进程存活 (tmux has-session + 哨兵文件)
- L2: 思考存活 (pane 活动时间 + 9Router token)
- L3: 产出存活 (bus 最近产出)
- overall 综合判断
- tick 主入口
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_SRC = (Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest

# Use a fixture to isolate SENTINEL_DIR and _ROLE_TARGETS_CACHE
from routing import survival_monitor as sm


@pytest.fixture(autouse=True)
def isolate_state(tmp_path, monkeypatch):
    """隔离哨兵 + 健康目录，清除角色 target 缓存。

    health 目录必须一并隔离：tick() 会调 _write_health()，原先它硬编码
    真实路径，测试会把 survival_* 字段写进生产 ccs-health/{role}.json
    （实测 2026-10-16，qa.json 被改写）。
    """
    sentinel_dir = tmp_path / "ccs-sentinels"
    sentinel_dir.mkdir(parents=True, exist_ok=True)
    health_dir = tmp_path / "ccs-health"
    health_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sm, "SENTINEL_DIR", sentinel_dir)
    monkeypatch.setattr(sm, "HEALTH_DIR", health_dir)
    # lifecycle 也必须隔离：tick() 现在额外枚举 infinite 角色以保证「从未启动」的
    # 角色也进巡检；不隔离会读到生产 personas → 测试驱动 _ensure_role_alive
    # 去拉起/杀真实 tmux 会话（实测 2026-10-20，一次 pytest 尝试重启 10 个角色）。
    monkeypatch.setattr(sm, "_load_role_lifecycles", lambda: {})
    # Clear cache
    sm._ROLE_TARGETS_CACHE = {}
    sm._ROLE_TARGETS_TS = 0.0
    # Remove BUS_SCRIPT dependency (just guard)
    yield
    sm._ROLE_TARGETS_CACHE = {}
    sm._ROLE_TARGETS_TS = 0.0


def _make_sentinel(role, sentinel_dir):
    """创建一个真实哨兵文件。"""
    s = {"role": role, "tmux_session": f"ccs-{role}", "pid": 12345, "started_at": time.time()}
    (sentinel_dir / f"{role}.json").write_text(json.dumps(s))


@pytest.fixture
def monitor():
    return sm.SurvivalMonitor()


class TestL1Check:
    """L1: 进程存活 —— 判据是 tmux 快照里的 pane；探针不可用不等于死。"""

    def test_l1_alive_with_sentinel(self, monitor, monkeypatch):
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {"qa": ("1", "0")})
        _make_sentinel("qa", sm.SENTINEL_DIR)

        result = monitor._l1_check("qa")
        assert result == {"status": "ALIVE", "tmux_alive": True, "has_sentinel": True}

    def test_l1_orphan_when_session_gone_but_sentinel_left(self, monitor, monkeypatch):
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {})
        _make_sentinel("qa", sm.SENTINEL_DIR)

        assert monitor._l1_check("qa")["status"] == "ORPHAN"

    def test_l1_dead_when_session_without_sentinel(self, monitor, monkeypatch):
        """会话在但哨兵缺失 = 状态丢失（DEAD 走 _cleanup_dead 重建）。"""
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {"qa": ("1", "0")})

        assert monitor._l1_check("qa")["status"] == "DEAD"

    def test_l1_unknown_when_neither(self, monitor, monkeypatch):
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {})

        assert monitor._l1_check("qa")["status"] == "UNKNOWN"

    def test_l1_probe_unavailable_is_not_death(self, monitor, monkeypatch):
        """快照不可用（tmux 卡死）→ UNKNOWN + tmux_alive=None：不判死、不进恢复队列。

        旧实现用「连续 3 次失败」兜这个窗口，但状态读的是 cache 顶层（tick 写的是
        {"l1": …}）→ 计数从未生效，一次超时即判 ORPHAN（2026-10-22 16:39 实测
        stale/dead 与两分钟后「会话本就在（L1 存活）」并存）。
        """
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: None)
        _make_sentinel("qa", sm.SENTINEL_DIR)

        r = monitor._l1_check("qa")
        assert r["status"] == "UNKNOWN"
        assert r["tmux_alive"] is None


class TestL2Check:
    """L2: 思考存活"""

    def test_l2_thinking_via_window_activity(self, monitor, monkeypatch):
        """pane 活动在 5 分钟内 → thinking=True（读快照的 window_activity 列）。"""
        monkeypatch.setattr(
            "routing.survival_monitor.tmux_snapshot",
            lambda *a, **kw: {"qa": ("12345", str(time.time() - 60))})

        result = monitor._l2_check("qa")
        assert result["thinking"] is True

    def test_l2_stale_pane(self, monitor, monkeypatch):
        """pane 活动超过 5 分钟 → thinking=False。"""
        monkeypatch.setattr(
            "routing.survival_monitor.tmux_snapshot",
            lambda *a, **kw: {"qa": ("12345", str(time.time() - 600))})

        result = monitor._l2_check("qa")
        assert result["thinking"] is False

    def test_l2_role_absent_from_snapshot_is_unknown(self, monitor, monkeypatch):
        """快照里没有该角色 → 探测不可用（thinking=None），不误判成「不思考」。"""
        monkeypatch.setattr("routing.survival_monitor.tmux_snapshot",
                            lambda *a, **kw: {"other": ("1", "0")})

        assert monitor._l2_check("qa")["thinking"] is None

    def test_l2_probe_unavailable_is_unknown(self, monitor, monkeypatch):
        """tmux 快照不可用 → 不崩、thinking=None（交给 L3 兜底）。"""
        monkeypatch.setattr("routing.survival_monitor.tmux_snapshot",
                            lambda *a, **kw: None)

        result = monitor._l2_check("qa")
        assert result["thinking"] is None


class TestL3Check:
    """L3: 产出存活"""

    def test_l3_recent_output(self, monitor, monkeypatch):
        """最近有产出 → producing=True。

        L3 已从 bus CLI 子进程改为 Blackboard 进程内查询：read 子命令无 --src，
        原调用必然 argparse 退出 2 → 恒 producing=False（角色被误判 idle 击杀）。
        """
        def fake_read(*, src=None, limit=5):
            return [
                SimpleNamespace(cat="code_fix", src="qa", ts=time.time() - 60),
                SimpleNamespace(cat="test_report", src="qa", ts=time.time() - 120),
            ]
        monkeypatch.setattr(monitor, "_bb", SimpleNamespace(read=fake_read))

        result = monitor._l3_check("qa")
        assert result["producing"] is True
        assert result["recent_count"] == 2
        assert "code_fix" in result["categories_found"]

    def test_l3_no_output(self, monitor, monkeypatch):
        """无产出 → producing=False。"""
        monkeypatch.setattr(monitor, "_bb", SimpleNamespace(read=lambda **kw: []))

        result = monitor._l3_check("qa")
        assert result["producing"] is False

    def test_l3_bus_error(self, monitor, monkeypatch):
        """bus 查询异常 → producing=None（不可判，不得当成"没产出"）。"""
        def raise_error(**kw):
            raise FileNotFoundError("bus not found")
        monkeypatch.setattr(monitor, "_bb", SimpleNamespace(read=raise_error))

        result = monitor._l3_check("qa")
        assert result["producing"] is None

    def test_l3_blackboard_unavailable(self, monitor, monkeypatch):
        """Blackboard 不可用 → producing=None，不能误判为"没产出"。"""
        monkeypatch.setattr(monitor, "_bb", None)

        result = monitor._l3_check("qa")
        assert result["producing"] is None


class TestOverall:
    """_overall 综合判断"""

    def test_overall_dead(self, monitor):
        assert monitor._overall({"status": "DEAD"}, {}, {}) == "dead"

    def test_overall_orphan(self, monitor):
        assert monitor._overall({"status": "ORPHAN"}, {}, {}) == "orphan"

    def test_overall_unknown(self, monitor):
        assert monitor._overall({"status": "UNKNOWN"}, {}, {}) == "unknown"

    def test_overall_stale(self, monitor):
        assert monitor._overall({"status": "ALIVE"}, {"thinking": False}, {}) == "stale"

    def test_overall_thinking_false_but_recent_output_is_healthy(self, monitor):
        # 回归锁定：pane 静默（thinking=False）不应掩盖最近的 bus 产出。
        # 实测：qa 交付 112 passed 后仍被判 stale（pane 静默 >300s，但 L3 producing=True）。
        assert monitor._overall(
            {"status": "ALIVE"}, {"thinking": False}, {"producing": True}
        ) == "healthy"

    def test_overall_idle(self, monitor):
        assert monitor._overall({"status": "ALIVE"}, {"thinking": True}, {"producing": False}) == "idle"

    def test_overall_healthy(self, monitor):
        assert monitor._overall({"status": "ALIVE"}, {"thinking": True}, {"producing": True}) == "healthy"


class TestTick:
    """tick 主入口"""

    def test_tick_empty_dir(self, monitor):
        """无语兵文件 → { }。"""
        result = monitor.tick()
        assert result == {}

    def test_tick_single_role(self, monitor, monkeypatch):
        _make_sentinel("qa", sm.SENTINEL_DIR)

        # Mock L1 → ALIVE（判据是 pane 快照，不再是逐角色 has-session）
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {"qa": ("1", "0")})
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: MagicMock(returncode=0, stdout=""))
        # Mock bus
        monkeypatch.setattr(sm, "_load_role_targets", lambda: {})

        result = monitor.tick()
        assert "qa" in result
        assert result["qa"]["overall"] in ("healthy", "stale", "idle")

    def test_tick_caches_l2_l3(self, monitor, monkeypatch):
        _make_sentinel("qa", sm.SENTINEL_DIR)
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {"qa": ("1", "0")})
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: MagicMock(returncode=0, stdout=""))
        monkeypatch.setattr(sm, "_load_role_targets", lambda: {})

        # First tick populates cache
        r1 = monitor.tick()
        # Second tick reuses cache (L2/L3 not re-run if within cache TTL)
        r2 = monitor.tick()
        assert r2["qa"]["overall"] == r1["qa"]["overall"]

    def test_tick_includes_infinite_role_without_sentinel(self, monitor, monkeypatch):
        """infinite 角色即使无哨兵也进巡检——否则「从未启动」与「健康」无法区分。"""
        monkeypatch.setattr(sm, "_load_role_lifecycles",
                            lambda: {"scout": "infinite", "qa": "ondemand"})
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {})
        monkeypatch.setattr(sm, "subprocess", MagicMock(
            run=lambda *a, **kw: MagicMock(returncode=1, stdout="")))
        monkeypatch.setattr(monitor, "_ensure_role_alive", lambda r: True, raising=False)

        result = monitor.tick()
        assert "scout" in result, "infinite 角色必须进巡检"
        assert "qa" not in result, "ondemand 且无哨兵不应被巡检"


class TestWriteBus:
    """写 bus 通知"""

    def test_write_bus_success(self, monitor, monkeypatch):
        called_with = []

        def fake_run(cmd, **kw):
            called_with.append(cmd)
            return MagicMock(returncode=0)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_run)

        monitor._write_bus("architecture", "test message", evidence="detail")
        assert any("architecture" in str(c) for c in called_with)

    def test_write_bus_error_silent(self, monitor, monkeypatch):
        def raise_error(*a, **kw):
            raise Exception("bus error")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        # should not raise
        monitor._write_bus("architecture", "test")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
