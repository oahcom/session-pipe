#!/usr/bin/env python3
"""配额耗尽（FreeUsageLimitError）的注入与记账口径。

24h 实测（bus #455493）：255 次「已发继续」里 223 次 600s 内又报同一错（87.5%），
只有 3.5% 恢复；这些注入写了 255 条 [rate_limit] 事实 = 同期 bus 的 68.7%。
配额态仍要探活（额度回来能自愈），但不该每 30 分钟一次、每次写一条 bus。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_quota_nudge.py -q
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow import role_lifecycle as _rl
from pipeflow.engine import WorkflowEngine, _WORKFLOWS_DIR

ROLE = "quota_probe"
QUOTA_LINE = ("● API Error: Request rejected (429) · {\"error\":{\"type\":"
              "\"FreeUsageLimitError\",\"message\":\"Free usage limit reached\"}}")
TRANSIENT_LINE = "● API Error: 429 Too Many Requests · Retrying in 5s"


def _engine():
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    eng._lm.query.return_value = []          # 角色只从 tmux 兜底来
    eng._last_rate_recover = {}
    eng._bb = MagicMock()
    return eng


def _run(eng, pane_text):
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout=f"ccs-{ROLE}\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=pane_text)
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run), \
         patch.object(_rl, "_load_rate_state", return_value={}), \
         patch.object(_rl, "_save_rate_state"):
        eng._recover_rate_limited_roles()
    return [c for c in sent if c[-1] == "Enter"]      # C-u 与正文是两次 send-keys


def _facts(eng):
    return [c for c in eng._bb.write.call_args_list]


def test_quota_nudges_once_an_hour_and_reports_once():
    """配额态：仍探活（额度会回来），但 1h 内不重复敲，6h 内不重复记账。"""
    eng = _engine()
    assert len(_run(eng, QUOTA_LINE)) == 1, "首次应发一次「继续」探活"
    assert len(_run(eng, QUOTA_LINE)) == 0, "1h 内第二次不应再敲（_QUOTA_RETRY_SEC）"
    with patch.object(_rl, "_QUOTA_RETRY_SEC", 0):
        assert len(_run(eng, QUOTA_LINE)) == 1, "1h 后应再探活一次"

    facts = _facts(eng)
    assert len(facts) == 1, f"配额态只留一条审计, got {facts}"
    cat, title = facts[0].args
    assert cat == "monitor_audit" and title.startswith("[quota]"), title
    assert "FreeUsageLimitError" in facts[0].kwargs["evidence"], "审计必须带错误原文"


def test_transient_429_still_uses_backoff_curve():
    """非配额 429 保持原语义：发「继续」并记 [rate_limit]。"""
    eng = _engine()
    assert len(_run(eng, TRANSIENT_LINE)) == 1
    facts = _facts(eng)
    assert facts and facts[0].args[1].startswith("[rate_limit]"), facts
    assert "429" in facts[0].kwargs["evidence"], "证据不能是 UI 壳"


def test_quota_detected_from_transcript_when_pane_has_no_hint(monkeypatch, tmp_path):
    """pane 只剩截断的 429 行时，配额归类必须从转录取原文，否则会被当瞬时重试。"""
    d = tmp_path / f"-home-administrator-ccs-workspaces-{ROLE.replace('_', '-')}"
    d.mkdir()
    rec = {"type": "assistant", "isApiErrorMessage": True,
           "timestamp": "2030-01-01T00:00:00.000Z",
           "message": {"content": [{"type": "text", "text": "API Error: 429 " + QUOTA_LINE}]}}
    (d / "s.jsonl").write_text(json.dumps(rec) + "\n", encoding="utf-8")
    monkeypatch.setattr(_rl, "_TRANSCRIPT_ROOT", tmp_path)

    assert "FreeUsageLimitError" in _rl._last_api_error_text(ROLE)
    assert _rl._last_api_error_text("no_such_role") == ""
