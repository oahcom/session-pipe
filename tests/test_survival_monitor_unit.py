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

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest
import http.client

# Use a fixture to isolate SENTINEL_DIR and _ROLE_TARGETS_CACHE
from routing import survival_monitor as sm


@pytest.fixture(autouse=True)
def isolate_state(tmp_path, monkeypatch):
    """隔离哨兵目录 + 清除角色 target 缓存。"""
    sentinel_dir = tmp_path / "ccs-sentinels"
    sentinel_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sm, "SENTINEL_DIR", sentinel_dir)
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
    """L1: 进程存活"""

    def test_l1_alive_with_sentinel(self, monitor, monkeypatch):
        tmux_alive = MagicMock(returncode=0)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: tmux_alive)
        _make_sentinel("qa", sm.SENTINEL_DIR)

        result = monitor._l1_check("qa")
        assert result["status"] == "ALIVE"
        assert result["tmux_alive"] is True
        assert result["has_sentinel"] is True

    def test_l1_orphan(self, monitor, monkeypatch):
        tmux_dead = MagicMock(returncode=1)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: tmux_dead)
        _make_sentinel("qa", sm.SENTINEL_DIR)

        result = monitor._l1_check("qa")
        assert result["status"] == "ORPHAN"

    def test_l1_no_sentinel_unknown(self, monitor, monkeypatch):
        tmux_dead = MagicMock(returncode=1)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: tmux_dead)
        # no sentinel file

        result = monitor._l1_check("qa")
        assert result["status"] == "UNKNOWN"

    def test_l1_tmux_error(self, monitor, monkeypatch):
        def raise_error(*a, **kw):
            raise FileNotFoundError("tmux not found")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        _make_sentinel("qa", sm.SENTINEL_DIR)

        result = monitor._l1_check("qa")
        assert "tmux_alive" in result
        assert result["has_sentinel"] is True

    def test_l1_transient_probe_failure_keeps_alive(self, monitor, monkeypatch):
        """红线3: socket 瞬时抖动(连续<3次失败)沿用前态存活, 不翻转 → 抑制0↔9噪声。"""
        def raise_error(*a, **kw):
            raise FileNotFoundError("tmux socket 抖动")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        _make_sentinel("qa", sm.SENTINEL_DIR)
        # 前态: 存活
        monitor._cache["qa"] = {"tmux_alive": True, "status": "ALIVE", "l1_fail_streak": 0}

        r1 = monitor._l1_check("qa")
        assert r1["status"] == "ALIVE"          # 第1次失败, 沿用存活
        assert r1["tmux_alive"] is True
        assert r1["l1_fail_streak"] == 1
        monitor._cache["qa"] = r1               # tick() 会写回 cache, 模拟之

        r2 = monitor._l1_check("qa")
        assert r2["status"] == "ALIVE"          # 第2次失败, 仍沿用
        assert r2["l1_fail_streak"] == 2
        monitor._cache["qa"] = r2

        # 第3次连续失败 → 降级判 ORPHAN(有哨兵但tmux不可达)
        r3 = monitor._l1_check("qa")
        assert r3["status"] == "ORPHAN"
        assert r3["l1_fail_streak"] == 3

    def test_l1_transient_failure_no_prior_alive_not_suppressed(self, monitor, monkeypatch):
        """无前态存活记录时, 探测失败按原逻辑判(不掩盖真死亡)。"""
        def raise_error(*a, **kw):
            raise FileNotFoundError("tmux not found")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        _make_sentinel("qa", sm.SENTINEL_DIR)
        # 前态无 tmux_alive=True 记录 → 不抑制

        r1 = monitor._l1_check("qa")
        assert r1["status"] == "ORPHAN"          # 无前态存活, 正常判
        assert r1["tmux_alive"] is False

    def test_l1_probe_exception_keeps_prior_orphan_not_flip(self, monitor, monkeypatch):
        """探测异常(socket隔离/抖动)且前态为 ORPHAN → 保留前态, 不刷 ORPHAN 噪声。"""
        def raise_error(*a, **kw):
            raise FileNotFoundError("tmux socket 隔离")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        _make_sentinel("qa", sm.SENTINEL_DIR)
        # 前态: ORPHAN (已存在但 tmux 不可达)
        monitor._cache["qa"] = {"tmux_alive": False, "status": "ORPHAN",
                                 "has_sentinel": True, "l1_fail_streak": 0}

        r1 = monitor._l1_check("qa")
        assert r1["status"] == "ORPHAN"          # 沿用前态, 不翻转
        assert r1["tmux_alive"] is None          # 探测异常=无法判定, 非 False
        assert r1["l1_fail_streak"] == 1

    def test_l1_probe_exception_keeps_prior_unknown(self, monitor, monkeypatch):
        """探测异常且前态为 UNKNOWN → 保留 UNKNOWN, 不误判 ORPHAN/DEAD。"""
        def raise_error(*a, **kw):
            raise subprocess.TimeoutExpired(cmd="tmux", timeout=3)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        # 前态: UNKNOWN, 无哨兵
        monitor._cache["qa"] = {"tmux_alive": False, "status": "UNKNOWN",
                                 "has_sentinel": False, "l1_fail_streak": 0}

        r1 = monitor._l1_check("qa")
        assert r1["status"] == "UNKNOWN"          # 沿用前态
        assert r1["tmux_alive"] is None


class TestL2Check:
    """L2: 思考存活"""

    def test_l2_thinking_via_pane_activity(self, monitor, monkeypatch):
        """pane 活动在 5 分钟内 → thinking=True。"""
        def fake_run(cmd, **kw):
            m = MagicMock()
            m.returncode = 0
            if "list-panes" in cmd:
                m.stdout = f"{time.time() - 60}\n"  # 1 分钟前
            elif "list-sessions" in cmd:
                m.stdout = "ccs-qa\n"
            else:
                m.stdout = ""
            return m
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_run)
        # Disable HTTP call
        monkeypatch.setattr(http.client.HTTPConnection, "request", lambda *a, **kw: None)
        monkeypatch.setattr(http.client.HTTPConnection, "getresponse", lambda *a: MagicMock(status=404))

        result = monitor._l2_check("qa")
        assert result["thinking"] is True

    def test_l2_stale_pane(self, monitor, monkeypatch):
        """pane 活动超过 5 分钟 + 无 token → thinking=False。"""
        def fake_run(cmd, **kw):
            m = MagicMock()
            m.returncode = 0
            if "list-panes" in cmd:
                m.stdout = f"{time.time() - 600}\n"  # 10 分钟前
            elif "list-sessions" in cmd:
                m.stdout = "ccs-qa\n"
            else:
                m.stdout = ""
            return m
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_run)
        monkeypatch.setattr(http.client.HTTPConnection, "request", lambda *a, **kw: None)
        monkeypatch.setattr(http.client.HTTPConnection, "getresponse", lambda *a: MagicMock(status=404))

        result = monitor._l2_check("qa")
        assert result["thinking"] is False

    def test_l2_token_activity_shows_thinking(self, monitor, monkeypatch):
        """0 token 但 pane 在活动 → thinking=True。"""
        def fake_run(cmd, **kw):
            m = MagicMock()
            m.returncode = 0
            if "list-panes" in cmd:
                m.stdout = f"{time.time() - 10}\n"
            return m
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_run)
        # Mock 9Router returns tokens > 0
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"total_tokens": 500}'
        mock_conn = MagicMock()
        mock_conn.getresponse.return_value = mock_resp
        monkeypatch.setattr(http.client, "HTTPConnection", lambda *a, **kw: mock_conn)

        result = monitor._l2_check("qa")
        assert result["thinking"] is True

    def test_l2_tmux_error_fallback(self, monitor, monkeypatch):
        """tmux 命令出错不崩溃。"""
        def raise_error(*a, **kw):
            raise FileNotFoundError("no tmux")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)
        monkeypatch.setattr(http.client.HTTPConnection, "request", lambda *a, **kw: None)

        result = monitor._l2_check("qa")
        assert "thinking" in result  # no crash


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

        # Mock L1 → ALIVE
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: MagicMock(returncode=0, stdout=""))
        # Disable HTTP
        monkeypatch.setattr(http.client.HTTPConnection, "request", lambda *a, **kw: None)
        monkeypatch.setattr(http.client.HTTPConnection, "getresponse", lambda *a: MagicMock(status=404))
        # Mock bus
        monkeypatch.setattr(sm, "_load_role_targets", lambda: {})

        result = monitor.tick()
        assert "qa" in result
        assert result["qa"]["overall"] in ("healthy", "stale", "idle")

    def test_tick_caches_l2_l3(self, monitor, monkeypatch):
        _make_sentinel("qa", sm.SENTINEL_DIR)
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", lambda *a, **kw: MagicMock(returncode=0, stdout=""))
        monkeypatch.setattr(http.client.HTTPConnection, "request", lambda *a, **kw: None)
        monkeypatch.setattr(http.client.HTTPConnection, "getresponse", lambda *a: MagicMock(status=404))
        monkeypatch.setattr(sm, "_load_role_targets", lambda: {})

        # First tick populates cache
        r1 = monitor.tick()
        # Second tick reuses cache (L2/L3 not re-run if within cache TTL)
        r2 = monitor.tick()
        assert r2["qa"]["overall"] == r1["qa"]["overall"]


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
