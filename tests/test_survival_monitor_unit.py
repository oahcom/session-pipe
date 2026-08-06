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
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

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
        """最近有产出 → producing=True。"""
        def fake_bus(*a, **kw):
            m = MagicMock()
            m.returncode = 0
            m.stdout = json.dumps([
                {"cat": "code_fix", "src": "qa", "created_at": time.time() - 60},
                {"cat": "test_report", "src": "qa", "created_at": time.time() - 120},
            ])
            return m
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_bus)

        result = monitor._l3_check("qa")
        assert result["producing"] is True
        assert result["recent_count"] == 2
        assert "code_fix" in result["categories_found"]

    def test_l3_no_output(self, monitor, monkeypatch):
        """无产出 → producing=False。"""
        def fake_bus(*a, **kw):
            m = MagicMock()
            m.returncode = 0
            m.stdout = "[]"
            return m
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", fake_bus)

        result = monitor._l3_check("qa")
        assert result["producing"] is False

    def test_l3_bus_error(self, monitor, monkeypatch):
        """bus 命令异常 → producing=None。"""
        def raise_error(*a, **kw):
            raise FileNotFoundError("bus not found")
        monkeypatch.setattr("routing.survival_monitor.subprocess.run", raise_error)

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
