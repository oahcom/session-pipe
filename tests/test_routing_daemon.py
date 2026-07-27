"""test_routing_daemon.py — routing_daemon 核心函数测试覆盖。"""
import json
import sys
import time
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


if __name__ == "__main__":
    import sys as _sys
    import pytest as _pt
    _sys.exit(_pt.main([__file__, "-v"]))
