"""Unit tests for routing/auto.py core functions.
Mocks external dependencies (bus, sentinel, launcher)."""
import sys, os, tempfile, json
from pathlib import Path

# Ensure src is on path
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

# Mock external dependencies before importing auto
import unittest
from unittest.mock import patch, MagicMock


class TestAutoStatus(unittest.TestCase):
    """Test routing/auto.py status() function with mocked deps."""

    @patch("routing.auto.poll_unconsumed")
    def test_status_idle_when_no_messages(self, mock_poll):
        mock_poll.return_value = []
        from routing import auto
        result = auto.status()
        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["total"], 0)

    @patch("routing.auto.poll_unconsumed")
    def test_status_counts_by_category(self, mock_poll):
        mock_poll.return_value = [
            {"id": 1, "category": "code_fix"},
            {"id": 2, "category": "code_fix"},
            {"id": 3, "category": "architecture"},
        ]
        from routing import auto
        result = auto.status()
        self.assertEqual(result["status"], "active")
        self.assertEqual(result["total"], 3)
        self.assertIn("code_fix", result.get("by_category", {}))
        self.assertIn("architecture", result.get("by_category", {}))

    @patch("routing.auto.poll_unconsumed")
    def test_status_error_handling(self, mock_poll):
        mock_poll.return_value = [{"error": "something broke"}]
        from routing import auto
        result = auto.status()
        self.assertEqual(result["status"], "error")


class TestPollUnconsumed(unittest.TestCase):

    @patch("routing.polling.poll_unconsumed")
    def test_poll_delegates(self, mock_poll):
        mock_poll.return_value = [{"id": 1}]
        from routing import auto
        result = auto.poll_unconsumed(category="test")
        mock_poll.assert_called_once()
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
