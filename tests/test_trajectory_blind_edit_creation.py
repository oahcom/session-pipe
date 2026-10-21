#!/usr/bin/env python3
"""盲改豁免：角色**新建**文件不算盲改。

判据要求 Write/Edit 前先 Read，但新建文件此前不存在，根本无法 Read。
实测 engineer 3 条 blind_edit (bus #437645/#437646, 2026-10-21 17:04) 全是
"File created successfully at: ..." 的新建写，却让角色耗整轮去自证清白。

运行：cd session-pipeline && python3 tests/test_trajectory_blind_edit_creation.py
"""
import json
import os
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


def test_list_shaped_result_content() -> None:
    """tool_result content 可能是 list[str] / list[dict]：两种形态都要认出"新建"。"""
    for shape in ("list-str", "list-dict"):
        sess = _TRANSCRIPTS_DIR / f"-probe-selfcreate-{shape}-role"
        path = "/tmp/probe_shape.py"
        msg = f"File created successfully at: {path}"
        content = [msg] if shape == "list-str" else [{"type": "text", "text": msg}]
        lines = [
            _turn(0, "assistant", [
                {"type": "tool_use", "id": "cs", "name": "Write",
                 "input": {"file_path": path, "content": "x=1"}},
            ]),
            _turn(1, "user", [
                {"type": "tool_result", "tool_use_id": "cs", "is_error": False,
                 "content": content},
            ]),
        ]
        jf = _make(sess, lines)
        try:
            res = TrajectoryQualityChecker().check(f"selfcreate-{shape}", "t")
            assert res["findings"] == [], f"{shape} 形态误判: {res['findings']}"
        finally:
            jf.unlink(missing_ok=True)
            try:
                sess.rmdir()
            except OSError:
                pass


def test_path_mismatch_and_error_echo_not_exempted() -> None:
    """豁免不能扩大成放过一切写：路径不符 / 结果无创建标记 都要照判盲改。"""
    cases = [
        ("-mismatch", "other.py", f"File created successfully at: {os.path.join(os.sep, 'tmp', 'other.py')}"),
        ("-pathonly", "/tmp/probe_echo.py", "/tmp/probe_echo.py"),  # 结果只回显路径，无创建标记
    ]
    for tag, target, result in cases:
        sess = _TRANSCRIPTS_DIR / f"-probe-selfcreate{tag}-role"
        lines = [
            _turn(0, "assistant", [
                {"type": "tool_use", "id": "cx", "name": "Write",
                 "input": {"file_path": target, "content": "x=1"}},
            ]),
            _turn(1, "user", [
                # 写已成功落地（is_error=False）才谈得上盲改；失败的写不改动文件，
                # 另有用例覆盖（test_failed_write_is_not_blind_edit）。
                {"type": "tool_result", "tool_use_id": "cx", "is_error": False,
                 "content": result},
            ]),
        ]
        jf = _make(sess, lines)
        try:
            res = TrajectoryQualityChecker().check(f"selfcreate{tag}", "t")
            # 只断言本用例的目标：角色名是子串匹配，别的用例留下的目录也会被扫到，
            # 按总数断言会随执行顺序抖动。
            blind = [f for f in res["findings"]
                     if f["issue"] == "blind_edit" and f.get("file") == target]
            assert len(blind) == 1, f"{tag} 不该豁免: {res['findings']}"
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


def test_unparsed_retry_duplicate_not_blind_edit() -> None:
    """解析失败的重试残影不是盲改。

    同一调用在 transcript 里会留两条同 id 记录（一条可解析、一条
    __unparsedToolInput），若按"最近一条 Write"配号，新建结果就配到残影上，
    可解析那条被误判盲改。实测全库 2760 对，占 blind_edit 误报的绝大多数。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-retrydup-role"
    path = "/tmp/probe_retry_dup.py"
    ok_result = f"File created successfully at: {path} (file state is current in your context)"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "cd", "name": "Write",
             "input": {"file_path": path, "content": "x=1"}},
        ]),
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "cd", "name": "Write",
             "input": {"__unparsedToolInput": {"raw": "{\"file_path\": ..."}}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "cd", "is_error": False,
             "content": ok_result},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "cd", "is_error": True,
             "content": "<tool_use_error>InputValidationError: Write was called with "
                        "input that could not be parsed as JSON.</tool_use_error>"},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("retrydup", "t")
        assert res["findings"] == [], f"重试残影被当成盲改: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_failed_write_is_not_blind_edit() -> None:
    """写没发生就不算盲改：pg 的「No such tool available: Write」曾被报成 blind_edit。"""
    sess = _TRANSCRIPTS_DIR / "-probe-nowrite-role"
    lines = _write_seq(0, "cw", "/tmp/probe_nowrite.py", "ok")
    lines[1] = _turn(1, "user", [
        {"type": "tool_result", "tool_use_id": "cw", "is_error": True,
         "content": "<tool_use_error>Error: No such tool available: Write. "
                    "Write is disabled for this session, in subagents as well as here."
                    "</tool_use_error>"},
    ])
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("nowrite", "t")
        assert res["findings"] == [], f"未发生的写被报成盲改: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_one_success_result_keeps_blind_edit() -> None:
    """同 id 一成功一失败（重试残影）：只要有一次写成了，照判盲改。

    反之（只看 error 就跳过）会变成「先失败一次，后面盲写全部免检」的后门。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-mixedretry-role"
    lines = _write_seq(0, "cm", "/tmp/probe_mixed.py", "ok")
    lines.append(_turn(1, "user", [
        {"type": "tool_result", "tool_use_id": "cm", "is_error": True,
         "content": "<tool_use_error>InputValidationError: Write was called with "
                    "input that could not be parsed as JSON.</tool_use_error>"},
    ]))
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("mixedretry", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"有成功结果却放过盲改: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_edit_of_own_written_file_not_flagged() -> None:
    """自己刚写过的文件，随后 Edit 不算盲改（内容就在上下文里）。

    只豁免「写之后」的编辑：写之前的 Edit 仍要判——否则盲改判据被自己未来的写洗白。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-ownwrite-role"
    path = "/tmp/probe_own.py"
    lines = _write_seq(0, "cw1", path,
                       f"File created successfully at: {path} (file state is current)")
    lines += [
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "ce1", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "ce1", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("ownwrite", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"自建文件的后续 Edit 被误判: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_edit_before_own_write_still_flagged() -> None:
    """顺序不能反：先 Edit 后 Write 时，那次 Edit 仍算盲改。"""
    sess = _TRANSCRIPTS_DIR / "-probe-ownwrite-order-role"
    path = "/tmp/probe_own_order.py"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "ce2", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "ce2", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    lines += _write_seq(2, "cw2", path,
                        f"File created successfully at: {path} (file state is current)")
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("ownwrite-order", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"写之前的 Edit 不该被豁免: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_mcp_write_then_edit_own_file_not_flagged() -> None:
    """mcp__*__write_file 也是一次写：随后 Edit 不算盲改。"""
    sess = _TRANSCRIPTS_DIR / "-probe-mcpown-role"
    path = "/tmp/probe_mcp.py"
    lines = [
        json.dumps({"type": "assistant", "message": {"id": "mm1", "type": "message", "role": "assistant",
            "content": [{"type": "tool_use", "id": "mw1", "name": "mcp__hex-line__write_file",
                         "input": {"file_path": path, "content": "x=1\n"}}]}}),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "mw1", "is_error": False,
             "content": "wrote /tmp/probe_mcp.py (1 line)"},
        ]),
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "me1", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "me1", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("mcpown", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"MCP 自建文件的后续 Edit 被误判: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    for _t in (test_created_file_not_flagged,
               test_list_shaped_result_content,
               test_path_mismatch_and_error_echo_not_exempted,
               test_non_creation_write_still_flagged,
               test_unparsed_retry_duplicate_not_blind_edit,
               test_failed_write_is_not_blind_edit,
               test_one_success_result_keeps_blind_edit,
               test_edit_of_own_written_file_not_flagged,
               test_edit_before_own_write_still_flagged,
               test_mcp_write_then_edit_own_file_not_flagged):
        _t()
    print("PASS: 10 tests")