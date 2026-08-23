#!/usr/bin/env python3
"""
TrajectoryQualityChecker 自检：构造 fixture transcri，验证只标记盲改。

运行：cd session-pipeline && python3 tests/test_trajectory_checker.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from eval_checker import TrajectoryQualityChecker, _TRANSCRIPTS_DIR


def _entry(role: str, content: list[dict], mtype: str = "assistant") -> str:
    return json.dumps({
        "type": mtype,
        "message": {
            "id": "msg_" + str(_entry._n),
            "type": "message",
            "role": role,
            "content": content,
        },
    }) + "\n"
_entry._n = 0


def _make_transcript(path: Path) -> None:
    """写好序列 + 盲改序列 到 jsonl。"""
    lines = []
    n = 0
    # ── 好序列：Read 后再 Edit ──
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "c1", "name": "Read",
             "input": {"file_path": "/tmp/foo.py"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "c1", "is_error": False, "content": "x=1"},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "c2", "name": "Edit",
             "input": {"file_path": "/tmp/foo.py", "old_string": "x=1", "new_string": "x=2"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "c2", "is_error": False, "content": "ok"},
        ]},
    }))
    n += 1
    # ── 盲改：Edit 一个从未 Read 的文件 ──
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "c3", "name": "Write",
             "input": {"file_path": "/tmp/blind.py", "content": "y=1"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "c3", "is_error": False, "content": "ok"},
        ]},
    }))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_blind_edit_only_flagged() -> None:
    # 在 _TRANSCRIPTS_DIR 下创建临时角色会话目录（含 "engineer" 子串）
    sess = _TRANSCRIPTS_DIR / "-home-administrator-ccs-workspaces-testfixture-role"
    sess.mkdir(parents=True, exist_ok=True)
    try:
        jf = sess / "fixture.jsonl"
        _make_transcript(jf)

        checker = TrajectoryQualityChecker()
        result = checker.check("testfixture", "eval_testfixture")

        issues = {f["issue"] for f in result["findings"]}
        # 好序列不应触发任何问题；盲改应被标记
        assert "blind_edit" in issues, f"expected blind_edit flagged, got {result['findings']}"
        assert "orphan_call" not in issues, f"good sequence wrongly flagged orphan: {issues}"
        assert "schema_missing_keys" not in issues, f"schema wrongly flagged: {issues}"
        assert "thrash_loop" not in issues, f"thrash wrongly flagged: {issues}"
        # 仅 1 条 blind_edit 发现
        blind = [f for f in result["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"expected exactly 1 blind_edit, got {len(blind)}"
        assert blind[0]["file"] == "/tmp/blind.py"
        assert result["checked_traces"] >= 1
    finally:
        jf.unlink(missing_ok=True)
        # 尝试清理空目录
        try:
            sess.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    test_blind_edit_only_flagged()
    print("PASS: TrajectoryQualityChecker flags only blind edit")
