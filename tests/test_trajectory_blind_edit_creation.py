#!/usr/bin/env python3
"""盲改豁免：角色**新建**文件不算盲改。

判据要求 Write/Edit 前先 Read，但新建文件此前不存在，根本无法 Read。
实测 engineer 3 条 blind_edit (bus #437645/#437646, 2026-10-21 17:04) 全是
"File created successfully at: ..." 的新建写，却让角色耗整轮去自证清白。

运行：cd session-pipeline && python3 tests/test_trajectory_blind_edit_creation.py
"""
import json
import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from eval_checker import TrajectoryQualityChecker, _TRANSCRIPTS_DIR


def _turn(n: int, role: str, content: list) -> str:
    return json.dumps({
        "type": "assistant" if role == "assistant" else "user",
        "message": {"id": f"m{n}", "type": "message", "role": role,
                    "content": content},
    })


def _write_seq(n: int, cid: str, path: str, result: str) -> list:
    """一次 Write 及其结果；result 决定它是否被认作"新建"。"""
    return [
        _turn(n, "assistant", [
            {"type": "tool_use", "id": cid, "name": "Write",
             "input": {"file_path": path, "content": "x=1"}},
        ]),
        _turn(n + 1, "user", [
            {"type": "tool_result", "tool_use_id": cid, "is_error": False,
             "content": result},
        ]),
    ]


def _make(role_dir: Path, lines: list) -> Path:
    role_dir.mkdir(parents=True, exist_ok=True)
    jf = role_dir / "probe.jsonl"
    jf.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return jf


def test_created_file_not_flagged() -> None:
    sess = _TRANSCRIPTS_DIR / "-probe-selfcreate-role"
    jf = _make(sess, _write_seq(
        0, "c1", "/tmp/probe_created.py",
        "File created successfully at: /tmp/probe_created.py"
        " (file state is current in your context — no need to Read it back)"))
    try:
        res = TrajectoryQualityChecker().check("selfcreate", "t")
        assert res["findings"] == [], f"新建写被误判: {res['findings']}"
        assert res["passed"] is True
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_non_creation_write_still_flagged() -> None:
    """对照：结果不含"新建"标记的写仍判盲改（豁免不能扩大成放过一切写）。"""
    sess = _TRANSCRIPTS_DIR / "-probe-selfupdate-role"
    jf = _make(sess, _write_seq(0, "c9", "/tmp/probe_updated.py", "ok"))
    try:
        res = TrajectoryQualityChecker().check("selfupdate", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"盲改未被标记: {res['findings']}"
        assert blind[0]["file"] == "/tmp/probe_updated.py"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    test_created_file_not_flagged()
    test_non_creation_write_still_flagged()
    print("PASS: 2 tests")
