"""schema_missing_keys 自纠判定：真违规仍应被抓，已自纠应豁免。

回归（2026-10-16）：scout 3 次把 Read 的 file_path 写成 path（Claude Code
运行时返回 InputValidationError），随后立刻用正确键名重试同样三个文件并成功，
但判据仍让整轨 FAIL 并向 bus 写 blocker/notice。

本测试确保豁免只覆盖"已自纠"，不会放过真·不会用工具的角色。
"""
import json
import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from eval_checker import TrajectoryQualityChecker, _TRANSCRIPTS_DIR


def _use(cid, name, inp):
    return json.dumps({
        "type": "assistant",
        "message": {"id": f"m{cid}", "type": "message", "role": "assistant",
                    "content": [{"type": "tool_use", "id": cid, "name": name, "input": inp}]},
    })


def _result(cid, is_error=False):
    return json.dumps({
        "type": "user",
        "message": {"id": f"r{cid}", "type": "message", "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": cid,
                                 "is_error": is_error, "content": "x"}]},
    })


def _write(sess: Path, name: str, lines: list[str]):
    (sess / name).write_text("\n".join(lines), encoding="utf-8")


def test_selfhealed_schema_error_is_exempt() -> None:
    """错键名后立即用正确键名重试 → 豁免（不判 FAIL）。"""
    sess = _TRANSCRIPTS_DIR / "-probe-selfheal-role"
    sess.mkdir(parents=True, exist_ok=True)
    try:
        _write(sess, "a.jsonl", [
            _use("c1", "Read", {"path": "/tmp/x.py"}),      # 错键名
            _result("c1", is_error=True),
            _use("c2", "Read", {"file_path": "/tmp/x.py"}),  # 自纠
            _result("c2"),
        ])
        r = TrajectoryQualityChecker().check("probe-selfheal", "t")
        bad = [f for f in r["findings"] if f["issue"] == "schema_missing_keys"]
        assert not bad, f"已自纠不应判 FAIL，实际: {bad}"
    finally:
        for p in sess.glob("*.jsonl"):
            p.unlink()
        try:
            sess.rmdir()
        except OSError:
            pass


def test_unhealed_schema_error_still_flagged() -> None:
    """始终用错键名（从不自纠）→ 仍应被抓。"""
    sess = _TRANSCRIPTS_DIR / "-probe-noselfheal-role"
    sess.mkdir(parents=True, exist_ok=True)
    try:
        _write(sess, "a.jsonl", [
            _use("c1", "Read", {"path": "/tmp/x.py"}),
            _result("c1", is_error=True),
            _use("c2", "Read", {"path": "/tmp/y.py"}),      # 又一次错键名
            _result("c2", is_error=True),
        ])
        r = TrajectoryQualityChecker().check("probe-noselfheal", "t")
        bad = [f for f in r["findings"] if f["issue"] == "schema_missing_keys"]
        assert len(bad) == 2, f"未自纠应全被抓，实际 {len(bad)}: {bad}"
    finally:
        for p in sess.glob("*.jsonl"):
            p.unlink()
        try:
            sess.rmdir()
        except OSError:
            pass


def test_other_tool_does_not_mask() -> None:
    """另一工具的成功调用不应豁免本工具的错误。"""
    sess = _TRANSCRIPTS_DIR / "-probe-crosstool-role"
    sess.mkdir(parents=True, exist_ok=True)
    try:
        _write(sess, "a.jsonl", [
            _use("c1", "Read", {"path": "/tmp/x.py"}),        # Read 错
            _result("c1", is_error=True),
            _use("c2", "Bash", {"command": "ls"}),            # 另一工具成功
            _result("c2"),
        ])
        r = TrajectoryQualityChecker().check("probe-crosstool", "t")
        bad = [f for f in r["findings"] if f["issue"] == "schema_missing_keys"]
        assert len(bad) == 1, f"跨工具不应豁免，实际 {len(bad)}: {bad}"
        assert bad[0]["tool"] == "Read"
    finally:
        for p in sess.glob("*.jsonl"):
            p.unlink()
        try:
            sess.rmdir()
        except OSError:
            pass


def test_missing_tool_is_not_a_schema_violation() -> None:
    """工具不存在 → 调用从未执行，缺键不算"不会用该工具"（maintainer 的 Grep 实例）。"""
    sess = _TRANSCRIPTS_DIR / "-probe-ccs-workspaces-probe-notool"
    sess.mkdir(parents=True, exist_ok=True)
    try:
        _write(sess, "a.jsonl", [
            _use("c1", "Grep", {"path": "", "query": "bus_client.py"}),
            json.dumps({"type": "user", "message": {
                "id": "r1", "type": "message", "role": "user", "content": [{
                    "type": "tool_result", "tool_use_id": "c1", "is_error": True,
                    "content": "<tool_use_error>Error: No such tool available: Grep. "
                               "Grep is not available in this session</tool_use_error>"}]}}),
        ])
        r = TrajectoryQualityChecker().check("probe-notool", "t")
        bad = [f for f in r["findings"] if f["issue"] == "schema_missing_keys"]
        assert not bad, f"不存在的工具不该判 schema 违规: {bad}"
    finally:
        for p in sess.glob("*.jsonl"):
            p.unlink()
        try:
            sess.rmdir()
        except OSError:
            pass


def test_role_sessions_match_exact_segment() -> None:
    """curator 不得把 knowledge-curator 的转录算成自己的（同一条 finding 记给两个角色）。"""
    owner = _TRANSCRIPTS_DIR / "-tmp-probe-ccs-workspaces-probe-curator"
    other = _TRANSCRIPTS_DIR / "-tmp-probe-ccs-workspaces-probe-knowledge-curator"
    for d in (owner, other):
        d.mkdir(parents=True, exist_ok=True)
        _write(d, "a.jsonl", [_use("c1", "Bash", {"command": "true"}), _result("c1")])
    try:
        r = TrajectoryQualityChecker().check("probe-curator", "t")
        assert r["checked_traces"] == 1, f"串到别的角色目录: {r['checked_traces']} 份"
    finally:
        for d in (owner, other):
            for p in d.glob("*.jsonl"):
                p.unlink()
            try:
                d.rmdir()
            except OSError:
                pass


if __name__ == "__main__":
    test_selfhealed_schema_error_is_exempt()
    print("PASS: 已自纠被豁免")
    test_unhealed_schema_error_still_flagged()
    print("PASS: 未自纠仍被抓")
    test_other_tool_does_not_mask()
    print("PASS: 跨工具不豁免")
    test_missing_tool_is_not_a_schema_violation()
    print("PASS: 不存在的工具不判 schema")
    test_role_sessions_match_exact_segment()
    print("PASS: 角色目录精确匹配")
