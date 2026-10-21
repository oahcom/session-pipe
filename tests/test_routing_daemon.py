"""test_routing_daemon.py — routing_daemon 核心函数测试覆盖。"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from routing_daemon import _any_live_targets, _check_cron_schedules


class TestAnyLiveTargets:
    """_any_live_targets 业务逻辑（不启动 daemon main loop）。"""

    @patch("routing_daemon.SENTINEL_DIR")
    def test_no_sentinels(self, mock_dir):
        mock_dir.glob.return_value = []
        assert _any_live_targets() is False

    @patch("routing_daemon.SENTINEL_DIR")
    def test_live_sentinel(self, mock_dir):
        sentinel = MagicMock()
        sentinel.read_text.return_value = json.dumps({"pid": 12345, "tmux_session": "ccs-engineer-0"})
        sentinel.name = "engineer.json"
        mock_dir.glob.return_value = [sentinel]
        assert _any_live_targets() is True

    @patch("routing_daemon.SENTINEL_DIR")
    def test_sentinel_no_pid(self, mock_dir):
        sentinel = MagicMock()
        sentinel.read_text.return_value = json.dumps({"pid": 0, "tmux_session": "ccs-engineer-0"})
        sentinel.name = "engineer.json"
        mock_dir.glob.return_value = [sentinel]
        assert _any_live_targets() is False

    @patch("routing_daemon.SENTINEL_DIR")
    def test_sentinel_no_tmux(self, mock_dir):
        sentinel = MagicMock()
        sentinel.read_text.return_value = json.dumps({"pid": 12345, "tmux_session": ""})
        sentinel.name = "engineer.json"
        mock_dir.glob.return_value = [sentinel]
        assert _any_live_targets() is False

    @patch("routing_daemon.SENTINEL_DIR")
    def test_mixed_sentinels(self, mock_dir):
        dead = MagicMock()
        dead.read_text.return_value = json.dumps({"pid": 0, "tmux_session": ""})
        dead.name = "dead.json"
        alive = MagicMock()
        alive.read_text.return_value = json.dumps({"pid": 42, "tmux_session": "ccs-live-1"})
        alive.name = "live.json"
        mock_dir.glob.return_value = [dead, alive]
        assert _any_live_targets() is True

    @patch("routing_daemon.SENTINEL_DIR")
    def test_corrupted_sentinel_skipped(self, mock_dir):
        sentinel = MagicMock()
        sentinel.read_text.side_effect = json.JSONDecodeError("bad", "", 0)
        sentinel.name = "bad.json"
        mock_dir.glob.return_value = [sentinel]
        assert _any_live_targets() is False


class TestCheckCronSchedules:
    """_check_cron_schedules 边界条件。"""

    def test_croniter_unavailable_returns_0(self):
        """croniter 不可用时返回 0 不抛异常。"""
        import builtins
        real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "croniter":
                raise ImportError("no croniter")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_mock_import):
            result = _check_cron_schedules()
            assert result == 0

    def test_no_roles_dir_returns_0(self):
        """角色目录不存在时返回 0。"""
        with patch("routing_daemon.Path.home") as mock_home:
            mock_home.return_value = Path("/nonexistent_path_xyz")
            result = _check_cron_schedules()
            assert result == 0


class _Fact:
    def __init__(self, fid, title, cat, ts):
        self.id, self.t, self.cat, self.ts = fid, title, cat, ts


class TestAgedQueueAlert:
    """_alert_aged_queue：只算定向、超龄、且该角色未消费的消息；6h 限频。"""

    NOW = 1_000_000_000.0

    def setup_method(self):
        import routing_daemon as _rd
        _rd._QUEUE_ALERT_LAST = 0.0
        self._rd = _rd

    def _run(self, facts_by_role, now=None):
        bb = MagicMock()
        bb.unconsumed.side_effect = lambda consumer: facts_by_role.get(consumer, [])
        router = MagicMock()
        router.routing = {r: [] for r in facts_by_role}
        with patch("bus_protocol.Blackboard", return_value=bb), \
             patch("routing.router.get_router", return_value=router):
            n = self._rd._alert_aged_queue(now=self.NOW if now is None else now)
        return n, bb

    def test_aged_directed_alerts(self):
        old = self.NOW - 25 * 3600
        n, bb = self._run({"engineer": [_Fact(7, "@engineer 需要处理", "notice", old)]})
        assert n == 1
        title = bb.write.call_args[0][1]
        assert "1 个角色" in bb.write.call_args[1]["evidence"]
        assert "engineer×1" in bb.write.call_args[1]["evidence"]
        assert "queue" in title

    def test_fresh_message_silent(self):
        n, bb = self._run({"engineer": [_Fact(7, "@engineer x", "notice", self.NOW - 3600)]})
        assert n == 0
        bb.write.assert_not_called()

    def test_undirected_message_ignored(self):
        old = self.NOW - 25 * 3600
        n, bb = self._run({"engineer": [_Fact(7, "无收件人的广播", "notice", old)]})
        assert n == 0
        bb.write.assert_not_called()

    def test_workflow_owned_categories_excluded(self):
        old = self.NOW - 25 * 3600
        facts = [_Fact(7, "@engineer 工作流任务: wf_x/s0", "task_spec", old)]
        n, bb = self._run({"engineer": facts})
        assert n == 0
        bb.write.assert_not_called()

    def test_rate_limited_within_gap(self):
        old = self.NOW - 25 * 3600
        facts = {"engineer": [_Fact(7, "@engineer x", "notice", old)]}
        self._run(facts)
        n, bb = self._run(facts, now=self.NOW + 60)
        assert n == 0
        bb.write.assert_not_called()
        n, bb = self._run(facts, now=self.NOW + 6 * 3600 + 1)
        assert n == 1
        assert bb.write.call_count == 1

    def test_own_alert_fact_not_counted(self):
        """告警自身也带 @ccs-monitor；下一轮不得把它算进积压（否则证据自污染）。"""
        old = self.NOW - 25 * 3600
        own = _Fact(99, self._rd._QUEUE_ALERT_TITLE, "notice", old)
        facts = {"engineer": [_Fact(7, "@engineer x", "notice", old)],
                 "ccs-monitor": [own]}
        n, bb = self._run(facts)
        assert n == 1
        assert "engineer×1" in bb.write.call_args[1]["evidence"]
        assert "ccs-monitor" not in bb.write.call_args[1]["evidence"]


if __name__ == "__main__":
    import sys as _sys
    import pytest as _pt
    _sys.exit(_pt.main([__file__, "-v"]))
