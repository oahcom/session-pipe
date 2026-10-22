"""task_evidence 产出评分：真实产物必须优先于「角色回了话」。

回归：exit_messages 几乎每条完成的链都有，判在最前会让唯一据实产物判 positive 的规则
几乎不可达 —— 实测最近 120 条完成任务中 85 条（71%）有 ≥3 个 ≥1KB 产出
（如 pm 19 文件/84KB）却被判「回复≠产出」。
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import task_evidence as te
from task_evidence import _classify_outputs


def test_real_artifacts_win_over_exit_messages():
    assert _classify_outputs(19, 84 * 1024, "exit_messages", False) == "positive"


def test_real_artifacts_win_over_summary():
    assert _classify_outputs(4, 4096, "summary", False) == "positive"


def test_three_small_files_count_as_real_output():
    assert _classify_outputs(3, 3 * 1024, "", False) == "positive"


def test_single_big_file_counts_as_real_output():
    assert _classify_outputs(1, 3000, "", False) == "positive"


def test_size_boundary_must_exceed_2048():
    assert _classify_outputs(1, 2048, "", False) == "neutral"


def test_exit_messages_alone_is_neutral():
    assert _classify_outputs(0, 0, "exit_messages", False) == "neutral"


def test_bus_evidence_alone_is_neutral():
    assert _classify_outputs(0, 0, "", True) == "neutral"


def test_small_files_are_neutral_not_positive():
    assert _classify_outputs(1, 1500, "", False) == "neutral"


def test_nothing_at_all_is_negative():
    assert _classify_outputs(0, 0, "", False) == "negative"


def test_non_output_dirs_are_not_artifacts(tmp_path, monkeypatch):
    """缓存/版本库目录里的文件不是产出。实测 engineer 窗口内只有 .coverage 与
    .pytest_cache —— 旧口径会把这 73KB 判成"有实质产出"。"""
    ws = tmp_path / "role"
    (ws / "__pycache__").mkdir(parents=True)
    (ws / ".pytest_cache").mkdir()
    (ws / "PRD.md").write_text("x" * 2048)
    (ws / "__pycache__" / "a.pyc").write_text("y" * 4096)
    (ws / ".pytest_cache" / "nodeids").write_text("z" * 4096)
    monkeypatch.setattr(te, "WORKSPACES", tmp_path)
    names = [f.name for f in te._output_files("role", time.time() - 60, time.time())]
    assert names == ["PRD.md"], names


def test_output_files_survives_missing_path(tmp_path, monkeypatch):
    monkeypatch.setattr(te, "WORKSPACES", tmp_path)
    assert te._output_files("nosuchrole", time.time() - 60, time.time()) == []
