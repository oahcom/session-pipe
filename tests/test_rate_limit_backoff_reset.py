#!/usr/bin/env python3
"""退避状态何时可以清：只认「错误之后出现了带 tool_use 的轮」。

R143 缺陷：旧判据是「最后一条 assistant 不是 API 错误」。注入「继续」后 CLI 会
先落一条**纯文本** assistant 行（无 tool_use），旧判据当场判定已恢复 → 清掉
_last_rate_recover → attempt 归 1 → 下一次 429 走首档 120s？不，走的是"last=0
立即发"。实测 3h 内 21 次唤醒，错误→唤醒间隔 1/3/4/9/15s，退避曲线整体失效，
19/55 轮以 API 错误收尾、其中 12 轮零产出。

Run: cd ~/wt-pipeline && python3 -m pytest tests/test_rate_limit_backoff_reset.py -q
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeflow.role_lifecycle as rl


def _err(ts="2026-10-21T20:00:00.000Z"):
    return json.dumps({"type": "assistant", "isApiErrorMessage": True,
                       "timestamp": ts,
                       "message": {"content": [{"type": "text",
                                                "text": "API Error: upstream connection lost"}]}})


def _assistant(blocks, ts="2026-10-21T20:00:10.000Z"):
    return json.dumps({"type": "assistant", "timestamp": ts,
                       "message": {"content": blocks}})


def _patch_tail(monkeypatch, lines):
    """lines 按"最新在前"传入，与 _read_transcript_tail 的返回顺序一致。"""
    monkeypatch.setattr(rl, "_read_transcript_tail", lambda role, nbytes=65536: lines)


def test_error_turn_alone_is_not_progress(monkeypatch):
    _patch_tail(monkeypatch, [_err()])
    assert rl._transcript_progress_after_error("qa") is False


def test_plain_text_shell_after_error_is_not_progress(monkeypatch):
    """这就是旧判据被骗的那一形态：错误之上多了一条纯文本 assistant 行。"""
    lines = [_assistant([{"type": "text", "text": "收到，继续处理"}]), _err()]
    # 旧判据认为已恢复（最后一条不是错误）—— 正是缺陷来源
    assert rl._transcript_last_turn_is_api_error("qa", max_age=float("inf")) is False
    # 新判据要求真实进展：纯文本不算
    assert rl._transcript_progress_after_error("qa") is False


def test_tool_use_after_error_is_progress(monkeypatch):
    lines = [_assistant([{"type": "tool_use", "name": "Bash", "input": {}}]), _err()]
    _patch_tail(monkeypatch, lines)
    assert rl._transcript_progress_after_error("qa") is True


def test_no_transcript_is_not_progress(monkeypatch):
    """取不到转录（路径不存在）→ 不算进展，保守保留退避状态。"""
    _patch_tail(monkeypatch, [])
    assert rl._transcript_progress_after_error("qa") is False


def test_user_line_does_not_count(monkeypatch):
    lines = [json.dumps({"type": "user", "message": {"content": "继续"}}), _err()]
    assert rl._transcript_progress_after_error("qa") is False
