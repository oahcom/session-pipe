"""Test contract_updater.py — 合约更新器 e2e 测试。"""

import json, subprocess, sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from contract_updater import (
    apply_all_suggestions,
    suggest_improvements,
    write_suggestions,
)


class TestSuggestImprovements:
    """测试 suggest_improvements。"""

    def test_suggest_improvements_skipped(self):
        """有 skipped > 0 时生成建议。"""
        history = [
            {"role": "tester", "skipped": 1, "checked": 0},
            {"role": "tester", "skipped": 1, "checked": 0},
            {"role": "tester", "skipped": 1, "checked": 0},
        ]
        s = suggest_improvements(history)
        assert len(s) > 0
        assert any(x["type"] == "add_verification_cmd" for x in s)

    def test_suggest_improvements_empty(self):
        """无 skipped 时生成空列表。"""
        assert suggest_improvements([]) == []


class TestApplySuggestions:
    """测试 apply_all_suggestions。"""

    @patch("contract_updater._apply_suggestion", return_value=True)
    def test_apply_suggestion_existing(self, mock_apply):
        """对现有角色应用建议。"""
        suggestions = [
            {"role": "tester", "criterion": "some text", "type": "add_verification",
             "verification_command": "echo ok", "expected": "ok"},
        ]
        r = apply_all_suggestions(suggestions)
        assert r["applied"] == 1
        assert r["failed"] == 0
        mock_apply.assert_called_once_with(suggestions[0])

    @patch("contract_updater._apply_suggestion")
    def test_apply_all_suggestions_noop(self, mock_apply):
        """空列表应用。"""
        r = apply_all_suggestions([])
        assert r["applied"] == 0
        assert r["failed"] == 0
        mock_apply.assert_not_called()


class TestWriteSuggestions:
    """测试 write_suggestions。"""

    def test_write_suggestions_tag(self):
        """写入时带 tag=contract_improvement。"""
        suggestions = [
            {"type": "add_verification_cmd", "role": "tester",
             "suggestion": "Add verification command for tester",
             "criterion": "eval_criteria"},
        ]
        with patch.object(subprocess, "run") as mock_run:
            n = write_suggestions(suggestions)
            assert n == 1
            args = mock_run.call_args[0][0]
            tags_idx = args.index("--tags")
            assert args[tags_idx + 1] == "contract_improvement"
