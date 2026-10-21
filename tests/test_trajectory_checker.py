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


def _make_bash_read_transcript(path: Path) -> None:
    """角色先 Bash 读文件，再 Edit 它——不应判 blind_edit。

    回归：原判据只认 Read 工具，而角色规范《Large File / Log Analysis》
    要求大文件先 grep 定位、日志用 tail/grep。实测 scout/engineer/lr
    共 16 处误杀，目标文件全部已被 Bash 读过。
    """
    lines = []
    n = 0
    for cid, cmd in (
        ("b1", "grep -n 'TODO' /tmp/bashread.py | head -5"),
        ("b2", "cat /tmp/bashread.py | tail -20"),
    ):
        lines.append(json.dumps({
            "type": "assistant",
            "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
                {"type": "tool_use", "id": cid, "name": "Bash",
                 "input": {"command": cmd}},
            ]},
        }))
        n += 1
        lines.append(json.dumps({
            "type": "user",
            "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
                {"type": "tool_result", "tool_use_id": cid, "is_error": False, "content": "x=1"},
            ]},
        }))
        n += 1
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "b3", "name": "Edit",
             "input": {"file_path": "/tmp/bashread.py", "old_string": "x=1", "new_string": "x=2"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "b3", "is_error": False, "content": "ok"},
        ]},
    }))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_bash_read_not_flagged_as_blind_edit() -> None:
    sess = _TRANSCRIPTS_DIR / "-home-administrator-ccs-workspaces-bashfixture-role"
    sess.mkdir(parents=True, exist_ok=True)
    jf = sess / "fixture_bash.jsonl"
    try:
        _make_bash_read_transcript(jf)
        result = TrajectoryQualityChecker().check("bashfixture", "eval_bashfixture")
        blind = [f for f in result["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"Bash 读过文件后 Edit 不应判 blind_edit，实际: {blind}"
        assert "orphan_call" not in {f["issue"] for f in result["findings"]}
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def _make_tail_orphan_transcript(path: Path) -> None:
    """会话在工具执行中结束：末尾连续多个 tool_use 无 result。

    回归：orphan_call 原判据把这类"会话边界"报成"角色漏收结果"。
    实测 scout 的 4 处 orphan_call 全部是各自 jsonl 末条 tool_use——
    4/4 误判。且这些历史条目会持续占据 bus 报告的 findings[:3] 名额。

    同一轮 Claude 会并行发多个 tool_use（实测 dfd563da 末尾连发 2 条），
    故构造 3 条连续无 result 的 use，验证整个尾部段都被豁免。
    """
    lines = []
    n = 0
    # 1 条已配对的正常调用：它之前的一切都算"会话中"
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "Bash",
             "input": {"command": "ls"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "is_error": False, "content": "ok"},
        ]},
    }))
    n += 1
    # 末尾连续 3 条无 result（会话边界）
    for cid in ("t2", "t3", "t4"):
        lines.append(json.dumps({
            "type": "assistant",
            "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
                {"type": "tool_use", "id": cid, "name": "Bash",
                 "input": {"command": f"echo {cid}"}},
            ]},
        }))
        n += 1
    path.write_text("\n".join(lines), encoding="utf-8")


def test_tail_orphans_not_flagged() -> None:
    """transcript 末尾的未配对 tool_use 是会话边界，不应判 orphan_call。"""
    sess = _TRANSCRIPTS_DIR / "-home-administrator-ccs-workspaces-tailfixture-role"
    sess.mkdir(parents=True, exist_ok=True)
    jf = sess / "fixture_tail.jsonl"
    try:
        _make_tail_orphan_transcript(jf)
        result = TrajectoryQualityChecker().check("tailfixture", "eval_tailfixture")
        orphans = [f for f in result["findings"] if f["issue"] == "orphan_call"]
        assert not orphans, f"会话末尾的未配对调用不应判 orphan_call，实际: {orphans}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def _make_mid_orphan_transcript(path: Path) -> None:
    """角色中途漏收结果后**继续调用**——这是真违规，必须仍被报出。"""
    lines = []
    n = 0
    # 中途一条无 result 的调用
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "mid1", "name": "Bash",
             "input": {"command": "echo dropped"}},
        ]},
    }))
    n += 1
    # 紧接着又有一次已配对的调用 → 证明会话没结束，mid1 是真漏收
    lines.append(json.dumps({
        "type": "assistant",
        "message": {"id": f"m{n}", "type": "message", "role": "assistant", "content": [
            {"type": "tool_use", "id": "t9", "name": "Bash",
             "input": {"command": "echo after"}},
        ]},
    }))
    n += 1
    lines.append(json.dumps({
        "type": "user",
        "message": {"id": f"m{n}", "type": "message", "role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t9", "is_error": False, "content": "ok"},
        ]},
    }))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_mid_stream_orphan_still_flagged() -> None:
    """中途漏收结果（其后仍有已配对调用）必须仍被判 orphan_call。"""
    sess = _TRANSCRIPTS_DIR / "-home-administrator-ccs-workspaces-midfixture-role"
    sess.mkdir(parents=True, exist_ok=True)
    jf = sess / "fixture_mid.jsonl"
    try:
        _make_mid_orphan_transcript(jf)
        result = TrajectoryQualityChecker().check("midfixture", "eval_midfixture")
        orphans = [f for f in result["findings"] if f["issue"] == "orphan_call"]
        assert len(orphans) == 1 and orphans[0]["id"] == "mid1", \
            f"中途漏收结果应判 orphan_call 且只报那一条，实际: {orphans}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_thrash_merged_per_transcript() -> None:
    """同一次爆发被变体命令切段时只报 1 条，次数累加。

    实测 closer：一条 assistant 消息里 298 次完全相同的 sqlite3 查询，
    被 3 条 "locallocal" 变体切成 4 段，原实现报 4 条 finding（白占报告名额）。
    """
    sess = _TRANSCRIPTS_DIR / "-home-administrator-ccs-workspaces-thrashmerge-role"
    sess.mkdir(parents=True, exist_ok=True)
    jf = sess / "fixture_thrash.jsonl"
    cmd = "sqlite3 ~/.hermes/sister_bus/blackboard.db 'select 1'"
    lines, n = [], 0
    for batch in ([cmd] * 5, ["locallocal"], [cmd] * 6):
        for c_ in batch:
            lines.append(json.dumps({
                "type": "assistant",
                "message": {"id": f"t{n}", "type": "message", "role": "assistant", "content": [
                    {"type": "tool_use", "id": f"th{n}", "name": "Bash", "input": {"command": c_}},
                ]},
            }))
            n += 1
    jf.write_text("\n".join(lines), encoding="utf-8")
    try:
        result = TrajectoryQualityChecker().check("thrashmerge", "eval_thrashmerge")
        thrash = [f for f in result["findings"] if f["issue"] == "thrash_loop"]
        assert len(thrash) == 1, f"同一次爆发应只报 1 条，实际 {len(thrash)}: {thrash}"
        assert thrash[0]["count"] == 11, f"次数应累加为 11，实际 {thrash[0]['count']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    test_blind_edit_only_flagged()
    print("PASS: TrajectoryQualityChecker flags only blind edit")
    test_bash_read_not_flagged_as_blind_edit()
    print("PASS: Bash-read file not flagged as blind edit")
    test_tail_orphans_not_flagged()
    print("PASS: tail orphans (session boundary) not flagged")
    test_thrash_merged_per_transcript()
    print("PASS: thrash merged per transcript")
    test_mid_stream_orphan_still_flagged()
    print("PASS: mid-stream orphan still flagged")
