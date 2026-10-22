#!/usr/bin/env python3
"""盲改豁免：角色**新建**文件不算盲改。

判据要求 Write/Edit 前先 Read，但新建文件此前不存在，根本无法 Read。
实测 engineer 3 条 blind_edit (bus #437645/#437646, 2026-10-21 17:04) 全是
"File created successfully at: ..." 的新建写，却让角色耗整轮去自证清白。

运行：cd session-pipeline && python3 tests/test_trajectory_blind_edit_creation.py
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from eval_checker import TrajectoryQualityChecker, _TRANSCRIPTS_DIR


def _turn(n: int, role: str, content: list, ts: str | None = None) -> str:
    row = {
        "type": "assistant" if role == "assistant" else "user",
        "message": {"id": f"m{n}", "type": "message", "role": role,
                    "content": content},
    }
    if ts:                      # 轨迹判据的时间窗按记录 timestamp 判，不用文件 mtime
        row["timestamp"] = ts
    return json.dumps(row)


def _write_seq(n: int, cid: str, path: str, result: str, ts: str | None = None) -> list:
    """一次 Write 及其结果；result 决定它是否被认作"新建"，ts 用于时间窗用例。"""
    return [
        _turn(n, "assistant", [
            {"type": "tool_use", "id": cid, "name": "Write",
             "input": {"file_path": path, "content": "x=1"}},
        ], ts=ts),
        _turn(n + 1, "user", [
            {"type": "tool_result", "tool_use_id": cid, "is_error": False,
             "content": result},
        ], ts=ts),
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
        ("-pathonly", "/data/probe_echo.py", "/data/probe_echo.py"),  # 结果只回显路径，无创建标记
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
    jf = _make(sess, _write_seq(0, "c9", "/data/probe_updated.py", "ok"))
    try:
        res = TrajectoryQualityChecker().check("selfupdate", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"盲改未被标记: {res['findings']}"
        assert blind[0]["file"] == "/data/probe_updated.py"
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
    lines = _write_seq(0, "cm", "/data/probe_mixed.py", "ok")
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
    path = "/data/probe_own_order.py"
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


def test_same_file_different_path_form_not_flagged() -> None:
    """同一文件的不同写法要归一化：Read `/tmp/a.py` 后编辑 `/tmp/./a.py` 不算盲改。

    未归一化时豁免是字符串比对（codex 审 R13 P0），角色换个写法就白读一遍。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-pathform-role"
    real = "/tmp/probe_pathform.py"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "r1", "name": "Read",
             "input": {"file_path": real}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "r1", "is_error": False,
             "content": "1\tx=1"},
        ]),
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "e1", "name": "Edit",
             "input": {"file_path": "/tmp/./probe_pathform.py",
                       "old_string": "x=1", "new_string": "x=2"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "e1", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("pathform", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"同一文件换写法被误判盲改: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_bash_basename_must_be_path_component() -> None:
    """bash 里出现同名前缀的**别的**文件不算读过：`myx_probe.md` 不豁免 `x_probe.md`。"""
    sess = _TRANSCRIPTS_DIR / "-probe-bashbase-role"
    target = "/data/x_probe.md"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "b1", "name": "Bash",
             "input": {"command": "cat /data/myx_probe.md"}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "b1", "is_error": False,
             "content": "OTHER FILE CONTENT"},
        ]),
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "e2", "name": "Edit",
             "input": {"file_path": target, "old_string": "a", "new_string": "b"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "e2", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("bashbase", "t")
        blind = [f for f in res["findings"]
                 if f["issue"] == "blind_edit" and f.get("file") == target]
        assert len(blind) == 1, f"同名前缀的别的文件被当成读过: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_bash_absolute_path_must_point_at_same_file() -> None:
    """带绝对路径的 bash 读取必须指向同一文件：同名异目录的 decoy 不豁免。"""
    sess = _TRANSCRIPTS_DIR / "-probe-bashpath-role"
    decoy, target = "/data/decoy/x_path.md", "/data/other/x_path.md"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "b3", "name": "Bash",
             "input": {"command": f"cat {decoy}"}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "b3", "is_error": False,
             "content": "DECOY CONTENT"},
        ]),
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "e3", "name": "Edit",
             "input": {"file_path": target, "old_string": "a", "new_string": "b"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "e3", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("bashpath", "t")
        blind = [f for f in res["findings"]
                 if f["issue"] == "blind_edit" and f.get("file") == target]
        assert len(blind) == 1, f"读同名异目录文件不该豁免: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_bash_same_absolute_path_still_exempts() -> None:
    """对照：bash 读同一个绝对路径的文件仍豁免（别把 grep 大文件的合规行为判成盲改）。"""
    sess = _TRANSCRIPTS_DIR / "-probe-bashsame-role"
    path = "/tmp/x_same.md"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "b4", "name": "Bash",
             "input": {"command": f"grep -n TODO {path} | head -20"}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "b4", "is_error": False,
             "content": "3:TODO x"},
        ]),
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "e4", "name": "Edit",
             "input": {"file_path": path, "old_string": "a", "new_string": "b"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "e4", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("bashsame", "t")
        blind = [f for f in res["findings"]
                 if f["issue"] == "blind_edit" and f.get("file") == path]
        assert not blind, f"grep 过同一文件的 Edit 被误判: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_second_write_of_own_file_not_flagged() -> None:
    """重写自己写过的文件不算盲改：同一 transcript 里内容就在上下文。

    实测全库 15 处（pm 4 / product_architect 3 / maintainer 2 …），原先只豁免
    Edit，这些全被判盲改。跨 transcript 仍不豁免（compact 续跑会开新 transcript）。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-rewrite-role"
    path = "/tmp/probe_rewrite.py"
    lines = _write_seq(0, "cw3", path,
                       f"File created successfully at: {path} (file state is current)")
    lines += [
        _turn(2, "assistant", [
            {"type": "tool_use", "id": "cw4", "name": "Write",
             "input": {"file_path": path, "content": "x=2"}},
        ]),
        _turn(3, "user", [
            {"type": "tool_result", "tool_use_id": "cw4", "is_error": False,
             "content": "The file /tmp/probe_rewrite.py has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("rewrite", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"重写自己写过的文件被误判盲改: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass



def test_scratch_write_not_flagged() -> None:
    """草稿文件（/tmp）的写不算盲改：没有既有内容可被覆盖。"

    实测 24h 全 19 角色只剩 2 处 blind_edit，全在 /tmp（engineer /tmp/eng_sim2.py、
    knowledge_curator /tmp/inspect_rw.py），仓内 0 处；结果文案是
    'The file … has been updated successfully'，查不到"新建"标记，所以照判，
    角色每轮都要多花一轮自证。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-scratch-role"
    path = "/tmp/probe_scratch.py"
    lines = _write_seq(0, "ct1", path,
                       f"The file {path} has been updated successfully. "
                       '"(file state is current in your context)"')
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("scratch", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"草稿文件的写不该判盲改: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass




def test_scratch_edit_still_flagged() -> None:
    """草稿目录只豁免 Write：Edit 仍判盲改。

    /tmp 里 353 个被写路径中有 5 个是"起稿后 cp 进仓/系统目录"的
    （/tmp/20-memory-tuning.conf → systemd drop-in、/tmp/ccs-monitor/pass.sh →
    在跑的巡检脚本）；首写无害，之后对它们的无 Read Edit 是真盲改。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-scratchedit-role"
    path = "/tmp/probe_scratch_edit.py"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "ce9", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ]),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "ce9", "is_error": False,
             "content": "The file has been updated successfully."},
        ]),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("scratchedit", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"草稿目录的 Edit 不该被豁免: {res['findings']}"
        assert blind[0]["file"] == path
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass




def test_own_workspace_write_not_flagged() -> None:
    """角色写自己 ccs-workspaces 里的报告不算盲改：那是它自己的产出物。

    实测 maintainer 每天整篇重写 HEALTH_CHECK / DIAGNOSIS / FIX_REPORT。
    角色名与会话目录必须**真匹配**：上一版 role=ownws--probe 与目录 -probe-ownws-role
    不匹配，checked_traces=0 也算 passed，于是"豁免恒不生效"溜过去了（codex 审 P1-3）。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-ownws-role"
    path = "/home/administrator/ccs-workspaces/ownws/REPORT.md"
    lines = _write_seq(0, "co1", path,
                       f"The file {path} has been updated successfully.")
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("ownws", "t")
        assert res["checked_traces"] >= 1, "用例必须真的扫到转录，否则等于没测"
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"自己 workspace 的报告写不该判盲改: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_underscore_role_workspace_exempt() -> None:
    """带下划线的角色名也要认：真实目录是 product_architect（下划线），不是连字符。

    codex 审 P0：只按 role.replace("_","-") 拼路径时，product_architect /
    knowledge_curator / security_auditor 三个角色的豁免恒不生效。
    """
    sess = _TRANSCRIPTS_DIR / "-probe-pa_probe-role"
    path = "/home/administrator/ccs-workspaces/pa_probe/REPORT.md"
    lines = _write_seq(0, "cu1", path,
                       f"The file {path} has been updated successfully.")
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("pa_probe", "t")
        assert res["checked_traces"] >= 1, "用例必须真的扫到转录"
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"下划线角色的自有 workspace 也该豁免: {blind}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass



def test_stale_record_not_judged() -> None:
    """超过 24h 的**记录**不参与轨迹判据：历史事件不能把角色永久钉红。

    窗口按记录 timestamp 判（skill 21 节），不按文件 mtime：活动文件里混着几天前的
    记录时，mtime 看不出来（codex 审 P1-2）。
    """
    old_ts = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    sess = _TRANSCRIPTS_DIR / "-probe-stale-role"
    path = "/data/probe_stale.py"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "so1", "name": "Write",
             "input": {"file_path": path, "content": "x=1"}},
        ], ts=old_ts),
        _turn(1, "assistant", [
            {"type": "tool_use", "id": "so2", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ], ts=old_ts),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("stale", "t")
        assert res["checked_traces"] >= 1, "用例必须真的扫到转录"
        assert res["findings"] == [], f"过期记录仍被判定: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_fresh_record_still_judged() -> None:
    """对照：24h 内的记录照判（窗口不能把所有东西都放过）。"""
    now_ts = datetime.now(timezone.utc).isoformat()
    sess = _TRANSCRIPTS_DIR / "-probe-freshrec-role"
    path = "/data/probe_fresh.py"
    lines = [
        _turn(0, "assistant", [
            {"type": "tool_use", "id": "fr1", "name": "Edit",
             "input": {"file_path": path, "old_string": "x=1", "new_string": "x=2"}},
        ], ts=now_ts),
        _turn(1, "user", [
            {"type": "tool_result", "tool_use_id": "fr1", "is_error": False,
             "content": "The file has been updated successfully."},
        ], ts=now_ts),
    ]
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("freshrec", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"窗口内的盲改必须照判: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_scratch_created_now_is_exempt() -> None:
    """这次写才诞生的 /tmp 草稿豁免（诞生时间 ≈ 写时间）。"""
    now = datetime.now(timezone.utc).isoformat()
    path = f"/tmp/probe_born_{os.getpid()}.sh"
    Path(path).write_text("# scratch\n", encoding="utf-8")
    sess = _TRANSCRIPTS_DIR / "-probe-born-role"
    lines = _write_seq(0, "cb1", path,
                       f"The file {path} has been updated successfully.", ts=now)
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("born", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"刚诞生的草稿不该判盲改: {blind}"
    finally:
        Path(path).unlink(missing_ok=True)
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_scratch_overwrite_preexisting_is_flagged() -> None:
    """覆盖**预存**的 /tmp 文件要判盲改：诞生年龄 > 120s。

    codex 第二轮复核的 P1：只按目录放过会漏掉被覆盖的在跑脚本（真机 /tmp/al.sh，
    诞生早于写时间 5h54m，实测被放过）。birthtime 无法回拨，故用打桩把 stat 的
    诞生时间置为 1 小时前，其余全走真实路径（真文件、真 transcript、真判定）。
    上一版用例把"写时间"回拨 1 小时、文件却是现在建的 —— 那是物理不可能的情形，
    改了方向比较之后它才暴露出来（第三轮复核指出）。
    """
    now = datetime.now(timezone.utc)
    path = f"/tmp/probe_preexist_{os.getpid()}.sh"
    Path(path).write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
    born = str((now - timedelta(hours=1)).timestamp())
    real_run = subprocess.run

    class _R:
        stdout = born

    def _fake(cmd, *a, **k):
        if isinstance(cmd, list) and cmd[-1] == path:
            return _R()
        return real_run(cmd, *a, **k)

    sess = _TRANSCRIPTS_DIR / "-probe-preexist-role"
    lines = _write_seq(0, "cp1", path,
                       f"The file {path} has been updated successfully.",
                       ts=now.isoformat())
    jf = _make(sess, lines)
    subprocess.run = _fake
    try:
        res = TrajectoryQualityChecker().check("preexist", "t")
        blind = [f for f in res["findings"]
                 if f["issue"] == "blind_edit" and f.get("file") == path]
        assert len(blind) == 1, f"覆盖预存 /tmp 文件必须判盲改: {res['findings']}"
    finally:
        subprocess.run = real_run
        Path(path).unlink(missing_ok=True)
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_preexisting_real_file_without_monkeypatch() -> None:
    """真机预存文件、不打桩：写时间比诞生晚 200s → 必须判盲改。

    与"文件诞生于 1 小时前"在数值上等价（age=200 > 120），但不需要伪造 stat，
    走的是真实 stat 与真实文件。上一版唯一"预存"用例靠 monkeypatch，覆盖不到这里。
    """
    now = datetime.now(timezone.utc)
    path = f"/tmp/probe_real_pre_{os.getpid()}.sh"
    Path(path).write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
    late = (now + timedelta(seconds=200)).isoformat()   # 写时间晚于诞生 200s
    sess = _TRANSCRIPTS_DIR / "-probe-realpre-role"
    lines = _write_seq(0, "cr1", path,
                       f"The file {path} has been updated successfully.", ts=late)
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("realpre", "t")
        blind = [f for f in res["findings"]
                 if f["issue"] == "blind_edit" and f.get("file") == path]
        assert len(blind) == 1, f"预存文件被覆盖必须判盲改: {res['findings']}"
    finally:
        Path(path).unlink(missing_ok=True)
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_birth_tolerance_boundary() -> None:
    """120s 容差边界：age=60 放过、age=200 判盲改（同一个真文件，只改写时间）。"""
    now = datetime.now(timezone.utc)
    path = f"/tmp/probe_bound_{os.getpid()}.sh"
    Path(path).write_text("#!/bin/sh\n", encoding="utf-8")
    sess = _TRANSCRIPTS_DIR / "-probe-bound-role"
    for offset, expect_flagged in ((60, False), (200, True)):
        ts = (now + timedelta(seconds=offset)).isoformat()
        lines = _write_seq(0, f"cf{offset}", path,
                           f"The file {path} has been updated successfully.", ts=ts)
        if offset == 200:
            lines = _write_seq(10, f"cf{offset}", path,
                               f"The file {path} has been updated successfully.", ts=ts)
        jf = _make(sess, lines)
        try:
            res = TrajectoryQualityChecker().check("bound", "t")
            blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
            if expect_flagged:
                assert blind, f"age={offset}s 超过容差必须判盲改"
            else:
                assert not blind, f"age={offset}s 在容差内不该判盲改: {blind}"
        finally:
            jf.unlink(missing_ok=True)
    Path(path).unlink(missing_ok=True)
    try:
        sess.rmdir()
    except OSError:
        pass


def test_fail_open_is_logged_not_silent(caplog) -> None:
    """fail-open（文件已删）必须留 WARNING，不能静默放过。

    第四轮复核 P1-1：三路 fail-open 全无日志，与"豁免恒不生效却无输出"同型。
    """
    import logging
    path = f"/tmp/probe_gone_{os.getpid()}.sh"      # 不创建 → 取不到诞生时间
    now = datetime.now(timezone.utc).isoformat()
    sess = _TRANSCRIPTS_DIR / "-probe-gone-role"
    lines = _write_seq(0, "cg1", path,
                       f"The file {path} has been updated successfully.", ts=now)
    jf = _make(sess, lines)
    try:
        with caplog.at_level(logging.WARNING, logger="session-pipeline.eval_checker"):
            res = TrajectoryQualityChecker().check("gone", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert not blind, f"取不到诞生时间仍按新建放过: {blind}"
        msgs = [r.getMessage() for r in caplog.records]
        assert any("fail-open" in m for m in msgs), f"fail-open 必须打 WARNING，实际: {msgs}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


def test_relative_path_gets_no_scratch_exemption() -> None:
    """相对路径不享受草稿豁免：否则判据在 /tmp 下跑时 "x.py" 会 realpath 成 /tmp/x.py。"""
    now = datetime.now(timezone.utc).isoformat()
    sess = _TRANSCRIPTS_DIR / "-probe-relpath-role"
    lines = _write_seq(0, "ch1", "probe_rel.py",
                       "The file probe_rel.py has been updated successfully.", ts=now)
    jf = _make(sess, lines)
    try:
        res = TrajectoryQualityChecker().check("relpath", "t")
        blind = [f for f in res["findings"] if f["issue"] == "blind_edit"]
        assert len(blind) == 1, f"相对路径不该拿草稿豁免: {res['findings']}"
    finally:
        jf.unlink(missing_ok=True)
        try:
            sess.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    _tests = (
        test_created_file_not_flagged,
        test_list_shaped_result_content,
        test_path_mismatch_and_error_echo_not_exempted,
        test_non_creation_write_still_flagged,
        test_unparsed_retry_duplicate_not_blind_edit,
        test_failed_write_is_not_blind_edit,
        test_one_success_result_keeps_blind_edit,
        test_edit_of_own_written_file_not_flagged,
        test_edit_before_own_write_still_flagged,
        test_mcp_write_then_edit_own_file_not_flagged,
        test_same_file_different_path_form_not_flagged,
        test_bash_basename_must_be_path_component,
        test_bash_absolute_path_must_point_at_same_file,
        test_bash_same_absolute_path_still_exempts,
        test_second_write_of_own_file_not_flagged,
        test_scratch_write_not_flagged,
        test_scratch_edit_still_flagged,
        test_scratch_created_now_is_exempt,
        test_scratch_overwrite_preexisting_is_flagged,
        test_preexisting_real_file_without_monkeypatch,
        test_birth_tolerance_boundary,
        test_fail_open_is_logged_not_silent,
        test_relative_path_gets_no_scratch_exemption,
        test_own_workspace_write_not_flagged,
        test_underscore_role_workspace_exempt,
        test_stale_record_not_judged,
        test_fresh_record_still_judged,
    )
    for _t in _tests:
        _t()
    print(f"PASS: {len(_tests)} tests")
