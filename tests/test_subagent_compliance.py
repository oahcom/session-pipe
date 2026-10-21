#!/usr/bin/env python3
"""
SubagentCompliance 自检：引擎强制子代理指令 → 该轮是否派了 Agent。

运行：cd session-pipeline && python3 tests/test_subagent_compliance.py
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from eval_checker import _SUBAGENT_DIRECTIVE_MARK, TrajectoryQualityChecker

_DIRECTIVE = f"派单任务…\n\n## {_SUBAGENT_DIRECTIVE_MARK}\n\n本步执行方式：…"


def _ts(offset_s: float) -> str:
    """距今 offset_s 秒的 transcript 时间戳（UTC ISO，…Z）。"""
    dt = datetime.fromtimestamp(time.time() + offset_s, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _say(text: str, ts: str) -> dict:
    return {"kind": "say", "name": None, "input": {}, "id": None,
            "error": None, "order": 0, "text": text, "src": "f", "ts": ts}


def _use(name: str) -> dict:
    return {"kind": "use", "name": name, "input": {}, "id": "x",
            "error": None, "order": 0, "src": "f"}


def _result() -> dict:
    return {"kind": "result", "name": None, "input": {}, "id": "x",
            "error": False, "order": 0, "src": "f", "text": "ok"}


def test_followed_round_counts() -> None:
    calls = [_say(_DIRECTIVE, _ts(-60)), _use("Agent"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 1, "followed": 1}, "派了 Agent 应记 1/1"


def test_ignored_round_counts() -> None:
    calls = [_say(_DIRECTIVE, _ts(-60)), _use("Bash"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 1, "followed": 0}, "只跑 Bash 应记 1 轮 0 遵从"


def test_agent_before_directive_not_counted() -> None:
    """上一轮的 Agent 不能算到这一轮头上。"""
    calls = [_use("Agent"), _result(), _say(_DIRECTIVE, _ts(-60)), _use("Bash"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 1, "followed": 0}, "指令前的 Agent 不得计入"


def test_round_stops_at_next_input() -> None:
    """下一次输入之后才派 Agent，属下一轮。"""
    calls = [_say(_DIRECTIVE, _ts(-60)), _use("Bash"), _say("别的派单", _ts(-30)),
             _use("Agent"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 1, "followed": 0}, "跨轮次不应串味"


def test_stale_directive_excluded() -> None:
    """窗口外的旧指令不计入，避免一次旧轮次永久拉低比例。"""
    calls = [_say(_DIRECTIVE, _ts(-48 * 3600)), _use("Bash"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 0, "followed": 0}, "窗口外指令应被排除"


def test_unparseable_ts_treated_as_stale() -> None:
    calls = [_say(_DIRECTIVE, "not-a-timestamp"), _use("Agent"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 0, "followed": 0}, "时间不可定的不计入"


def test_non_directive_input_ignored() -> None:
    calls = [_say("普通消息", _ts(-60)), _use("Bash"), _result()]
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 0, "followed": 0}, "非指令轮次不计入"


def test_end_to_end_transcript(tmp_path: Path) -> None:
    """从真实 jsonl 走一遍：字符串 content 要被收成 say。"""
    jf = tmp_path / "probe.jsonl"
    rows = [
        {"type": "user", "timestamp": _ts(-60),
         "message": {"role": "user", "content": _DIRECTIVE}},
        {"type": "assistant", "timestamp": _ts(-55),
         "message": {"role": "assistant", "content": [
             {"type": "tool_use", "id": "a1", "name": "Agent", "input": {}}]}},
        {"type": "user", "timestamp": _ts(-50),
         "message": {"role": "user", "content": [
             {"type": "tool_result", "tool_use_id": "a1", "is_error": False,
              "content": "launched"}]}},
    ]
    jf.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    calls = TrajectoryQualityChecker()._load_tool_calls([tmp_path])
    assert TrajectoryQualityChecker()._subagent_compliance(calls) == \
        {"rounds": 1, "followed": 1}, "端到端应识别字符串输入里的指令并计到 Agent"


def main() -> int:
    n = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        if name == "test_end_to_end_transcript":
            import tempfile
            with tempfile.TemporaryDirectory() as d:
                fn(Path(d))
        else:
            fn()
        n += 1
    print(f"PASS: {n} subagent compliance tests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
