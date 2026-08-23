"""test_cron_scheduler.py — CronScheduler 调度逻辑测试覆盖。"""
import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cron_scheduler import _parse_interval, CronScheduler


class TestParseInterval:
    def test_none_expr(self):
        assert _parse_interval(None) is None

    def test_empty_expr(self):
        assert _parse_interval("") is None

    def test_star_slash(self):
        assert _parse_interval("*/5 * * * *") == 300

    def test_fixed_minute(self):
        assert _parse_interval("15 * * * *") == 900

    def test_star_slash_30(self):
        assert _parse_interval("*/30 * * * *") == 1800

    def test_invalid_returns_default(self):
        assert _parse_interval("not-a-cron") == 900


class TestCronScheduler:
    @patch("cron_scheduler.PERSONAS_DIR")
    def test_load_no_dir(self, mock_dir):
        mock_dir.is_dir.return_value = False
        s = CronScheduler()
        assert s._roles == []

    @patch("cron_scheduler.PERSONAS_DIR")
    def test_load_filters_cron_roles(self, mock_dir):
        f1 = MagicMock(spec=Path)
        f1.name = "persona_01_maintainer.json"
        f1.read_text.return_value = json.dumps({"name": "maintainer", "drive": "cron", "cron_schedule": "*/5 * * * *"})
        f2 = MagicMock(spec=Path)
        f2.name = "persona_02_ondemand.json"
        f2.read_text.return_value = json.dumps({"name": "ondemand", "drive": "ondemand", "cron_schedule": ""})
        # sorted() needs comparable objects; use __lt__ on mocks
        f1.__lt__ = lambda self, other: self.name < other.name
        f2.__lt__ = lambda self, other: self.name < other.name
        mock_dir.glob.return_value = [f1, f2]
        mock_dir.is_dir.return_value = True

        s = CronScheduler()
        assert len(s._roles) == 1
        assert s._roles[0]["name"] == "maintainer"

    @patch("cron_scheduler.PERSONAS_DIR")
    @patch("cron_scheduler.CronScheduler._fire")
    def test_tick_fires_expired(self, mock_fire, mock_dir):
        mock_dir.is_dir.return_value = True
        f = MagicMock()
        f.name = "persona_01_test.json"
        f.read_text.return_value = json.dumps({
            "name": "test", "drive": "cron", "cron_schedule": "*/1 * * * *"
        })
        mock_dir.glob.return_value = [f]

        s = CronScheduler()
        fired = s.tick()
        assert "test" in fired
        mock_fire.assert_called_once_with("test")

    @patch("cron_scheduler.PERSONAS_DIR")
    @patch("cron_scheduler.CronScheduler._fire")
    def test_tick_does_not_repeat_within_interval(self, mock_fire, mock_dir):
        mock_dir.is_dir.return_value = True
        f = MagicMock()
        f.name = "persona_01_test.json"
        f.read_text.return_value = json.dumps({
            "name": "test", "drive": "cron", "cron_schedule": "*/5 * * * *"
        })
        mock_dir.glob.return_value = [f]

        s = CronScheduler()
        s.tick()  # first tick fires
        assert mock_fire.call_count == 1
        s.tick()  # second tick within interval — should not fire
        assert mock_fire.call_count == 1

    @patch("cron_scheduler.PERSONAS_DIR")
    @patch("cron_scheduler.CronScheduler._fire")
    def test_tick_fires_again_after_interval(self, mock_fire, mock_dir):
        mock_dir.is_dir.return_value = True
        f = MagicMock()
        f.name = "persona_01_test.json"
        f.read_text.return_value = json.dumps({
            "name": "test", "drive": "cron", "cron_schedule": "*/1 * * * *"
        })
        mock_dir.glob.return_value = [f]

        s = CronScheduler()
        s._last_fired["test"] = time.time() - 120  # last fired 2 min ago
        fired = s.tick()
        assert "test" in fired


if __name__ == "__main__":
    import pytest as _p
    import sys as _s
    _s.exit(_p.main([__file__, "-v"]))
