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
from unittest.mock import MagicMock, patch

_SRC = str(Path(__file__).resolve().parent.parent / "src")
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
    monkeypatch.setattr(sm, "TRANSCRIPT_ROOT", tmp_path / "transcripts")
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


class TestL4Check:
    """L4: transcript / workflow 进展 —— bus 静默时的“有活”证据。"""

    def test_l4_workflow_recent_progress(self, monitor, tmp_path, monkeypatch):
        import sqlite3
        db = tmp_path / "workflows.db"
        con = sqlite3.connect(db)
        con.execute(
            "create table workflow_instances (instance_id text, status text, assignee text, "
            "created_at real, step_results text)"
        )
        con.execute(
            "insert into workflow_instances values (?,?,?,?,?)",
            ("wf_x", "running", "qa", time.time() - 600,
             json.dumps({"s1": {"completed_at": time.time() - 60}})),
        )
        con.commit()
        con.close()
        monkeypatch.setattr(sm, "WORKFLOWS_DB", db)
        result = monitor._l4_check("qa")
        assert result["available"] is True
        assert result["work_evidence"] is True
        assert result["workflow_age_seconds"] < 120

    def test_l4_engine_progress_keys_are_not_work_evidence(self, monitor, tmp_path, monkeypatch):
        """notified_at/poll_since/kick_stalled_at 是引擎刷新，不是角色产出。"""
        import sqlite3
        db = tmp_path / "workflows.db"
        con = sqlite3.connect(db)
        con.execute(
            "create table workflow_instances (instance_id text, status text, assignee text, "
            "created_at real, step_results text)"
        )
        now = time.time()
        con.execute(
            "insert into workflow_instances values (?,?,?,?,?)",
            ("wf_x", "running", "qa", now - 600, json.dumps({"s1": {
                "status": "notified",
                "notified_at": now - 30,
                "poll_since": now - 30,
                "kick_stalled_at": now - 100,
                "bus_anchor": now - 30,
            }})),
        )
        con.commit()
        con.close()
        monkeypatch.setattr(sm, "WORKFLOWS_DB", db)
        result = monitor._l4_check("qa")
        assert result["work_evidence"] is not True

    def test_l4_bus_anchor_with_exit_messages_is_evidence(self, monitor, tmp_path, monkeypatch):
        """bus_anchor + exit_messages 是真实匹配产出，算有活。"""
        import sqlite3
        db = tmp_path / "workflows.db"
        con = sqlite3.connect(db)
        con.execute(
            "create table workflow_instances (instance_id text, status text, assignee text, "
            "created_at real, step_results text)"
        )
        now = time.time()
        con.execute(
            "insert into workflow_instances values (?,?,?,?,?)",
            ("wf_x", "running", "qa", now - 600, json.dumps({"s1": {
                "status": "step_done_ready",
                "bus_anchor": now - 60,
                "exit_messages": [{"text": "s1-complete wf_x"}],
            }})),
        )
        con.commit()
        con.close()
        monkeypatch.setattr(sm, "WORKFLOWS_DB", db)
        result = monitor._l4_check("qa")
        assert result["work_evidence"] is True
        assert result["workflow_age_seconds"] < 120

    def test_overall_l4_probe_failure_is_unknown_not_stale(self, monitor):
        """L4 查询失败（error 存在且 work_evidence=None）不得落 stale。"""
        assert monitor._overall(
            {"status": "ALIVE"}, {"thinking": False}, {"producing": False},
            {"work_evidence": None, "error": "workflow_probe_failed: x"},
        ) == "unknown"

    def test_l4_missing_db_is_unknown_not_false(self, monitor, tmp_path, monkeypatch):
        monkeypatch.setattr(sm, "WORKFLOWS_DB", tmp_path / "missing.db")
        result = monitor._l4_check("qa")
        assert result["available"] is False
        assert result["work_evidence"] is None


class TestOverall:
    """_overall 综合判断"""

    def test_overall_dead(self, monitor):
        assert monitor._overall({"status": "DEAD"}, {}, {}) == "dead"

    def test_overall_orphan(self, monitor):
        assert monitor._overall({"status": "ORPHAN"}, {}, {}) == "orphan"

    def test_overall_unknown(self, monitor):
        assert monitor._overall({"status": "UNKNOWN"}, {}, {}) == "unknown"

    def test_overall_stale(self, monitor):
        # stale 必须建立在“确定没产出”上；L3={} 是不可判，不是没产出。
        assert monitor._overall({"status": "ALIVE"}, {"thinking": False},
                                {"producing": False}) == "stale"

    def test_overall_unavailable_l3_is_not_stale(self, monitor):
        """L3 不可判（空 dict / producing=None）不得落 stale。

        误报根因：pane 静默时旧缓存跳过 L3，首轮 l3={} → producing=None →
        兜底 stale。查不到 ≠ 没产出，必须返回 unknown 交给 L4/人工。
        """
        assert monitor._overall({"status": "ALIVE"}, {"thinking": False}, {}) == "unknown"
        assert monitor._overall(
            {"status": "ALIVE"}, {"thinking": False}, {"producing": None}) == "unknown"

    def test_overall_l4_work_evidence_overrides_stale(self, monitor):
        """transcript/workflow 仍在进展 → 不得判 stale。"""
        assert monitor._overall(
            {"status": "ALIVE"}, {"thinking": False}, {"producing": False},
            {"work_evidence": True}) == "healthy"

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

    def test_tick_unknown_role_warns_throttled(self, monitor, monkeypatch):
        """unknown 角色要有 600s 节流告警——否则永久静默（unknown 不进任何恢复/提醒队列）。"""
        _make_sentinel("qa", sm.SENTINEL_DIR)
        monkeypatch.setattr(sm, "tmux_snapshot", lambda *a, **k: {"qa": ("1", "0")})
        monkeypatch.setattr("routing.survival_monitor.subprocess.run",
                            lambda *a, **kw: MagicMock(returncode=0, stdout=""))
        monkeypatch.setattr(sm, "_load_role_targets", lambda: {})
        # L3 不可判 + L4 查询失败 → overall=unknown
        monkeypatch.setattr(monitor, "_l3_check", lambda r: {"producing": None})
        monkeypatch.setattr(monitor, "_l4_check",
                            lambda r: {"work_evidence": None, "error": "workflow_probe_failed: x"})

        with patch.object(sm.LOGGER, "warning") as warning:
            monitor.tick()
            monitor.tick()
        unknown_warnings = [c for c in warning.call_args_list
                            if "unknown" in str(c.args[0] if c.args else "")]
        assert len(unknown_warnings) == 1, \
            f"两次 tick 应恰好 1 条 unknown 告警（节流），实际 {len(unknown_warnings)}"
        assert "qa" in str(unknown_warnings[0])


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


class TestIdleReclaimTmuxHost:
    """ondemand idle 回收不得带走 tmux server 的房东 unit（#476559）。

    tmux server 是进程内单例，cgroup 由创建它的 unit 固定。kill 房东的 session →
    pane 消失 → unit 连带 exit → KillMode=control-group 清空 cgroup → 所有角色连坐
    （2026-11-01 12:14 实测 9 个 unit 一起重启 66s）。同一 tick 里两个角色都 idle，
    只差「谁是房东」，用来证明跳过是按角色判的、不是整段停摆。
    """

    def _idle_tick(self, monitor, monkeypatch, host_role):
        for _r in ("pm", "qa"):
            _make_sentinel(_r, sm.SENTINEL_DIR)
        monkeypatch.setattr(sm, "_load_role_lifecycles",
                            lambda: {"pm": "ondemand", "qa": "ondemand"})
        monkeypatch.setattr(monitor, "_l1_check",
                            lambda r: {"status": "ALIVE", "tmux_alive": True,
                                       "has_sentinel": False})
        monkeypatch.setattr(monitor, "_l2_check", lambda r: {"thinking": False})
        monkeypatch.setattr(monitor, "_l3_check", lambda r: {"producing": False})
        monkeypatch.setattr(monitor, "_l4_check", lambda r: {"work_evidence": False})
        monkeypatch.setattr(monitor, "_overall", lambda *a, **k: "idle")
        monkeypatch.setattr(monitor, "_rescue_failed_units", lambda: None)
        monkeypatch.setattr(sm, "role_hosts_tmux_server", lambda r: r == host_role,
                            raising=False)   # 修复前该名字还不存在，红要红在断言上
        for _r in ("pm", "qa"):
            monitor._state[_r] = {"idle_count": 5}

        calls = []
        monkeypatch.setattr("routing.survival_monitor.subprocess.run",
                            lambda cmd, **kw: calls.append(list(cmd)) or MagicMock(returncode=0))
        monitor.tick()
        return [c for c in calls if c[:2] == ["tmux", "kill-session"]]

    def test_host_role_idle_reclaim_is_skipped(self, monitor, monkeypatch):
        kills = self._idle_tick(monitor, monkeypatch, host_role="pm")
        assert ["tmux", "kill-session", "-t", "ccs-pm"] not in kills, \
            f"房东 unit 的 session 被 kill，全员会连坐断线；实际 kills={kills}"

    def test_non_host_idle_role_is_still_reclaimed(self, monitor, monkeypatch):
        kills = self._idle_tick(monitor, monkeypatch, host_role="pm")
        assert ["tmux", "kill-session", "-t", "ccs-qa"] in kills, \
            f"非房东的 ondemand 角色仍须回收；实际 kills={kills}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
