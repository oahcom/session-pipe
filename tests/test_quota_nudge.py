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


def _write_transcript(root, role, text):
    d = root / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
    d.mkdir(exist_ok=True)
    rec = {"type": "assistant", "isApiErrorMessage": True,
           "timestamp": "2030-01-01T00:00:00.000Z",
           "message": {"content": [{"type": "text", "text": text}]}}
    (d / "s.jsonl").write_text(json.dumps(rec) + "\n", encoding="utf-8")


def test_quota_from_transcript_when_pane_is_clean(monkeypatch, tmp_path):
    """生产主路径：错误早滚出 pane（0.67% 可见），归类只能靠转录。

    真实 pane 会带 `Retrying … attempt N/M`，那条会在 :575 早退 —— 所以配额判定
    必须能在「pane 干净 + 转录是配额错误」时生效，否则本改动等于没上线。
    """
    monkeypatch.setattr(_rl, "_TRANSCRIPT_ROOT", tmp_path)
    _write_transcript(tmp_path, ROLE, "API Error: 429 · FreeUsageLimitError: "
                                      "Free usage limit reached")
    eng = _engine()
    assert len(_run(eng, "❯ \n⏵⏵ bypass permissions on")) == 1
    assert _facts(eng)[0].args[1].startswith("[quota]"), _facts(eng)


def test_egress_short_limit_stays_transient():
    """`Free usage limit reached on the current egress` 是短窗口限流（原文让按提示重试），
    不能被 1h 硬限闸门压住。"""
    eng = _engine()
    line = ("● API Error: Request rejected (429) · FreeUsageLimitError: Free usage limit "
            "reached on the current egress. Retry after the indicated delay")
    assert len(_run(eng, line)) == 1
    assert _facts(eng)[0].args[1].startswith("[rate_limit]"), _facts(eng)


def test_latest_error_line_wins_over_stale_quota_line():
    """窗口上方残留的旧配额行不该覆盖下方新的 403：取最新命中行。"""
    eng = _engine()
    pane = "\n".join([QUOTA_LINE, "✻ 工作中", "● Please run /login · API Error: 403"])
    assert len(_run(eng, pane)) == 1
    assert _facts(eng)[0].args[1].startswith("[rate_limit]"), _facts(eng)


def test_failed_audit_is_retried_not_remembered():
    """写 bus 失败不能记 6h 冷却：否则一次 bus 抖动让这个角色 6h 没有配额审计。

    反向：把 `_rep[role] = now` 移到 write 之前，本用例必须红（第二次不再尝试写）。
    """
    eng = _engine()
    eng._bb.write.side_effect = RuntimeError("bus down")
    with patch.object(_rl, "_QUOTA_RETRY_SEC", 0):
        _run(eng, QUOTA_LINE)
        _run(eng, QUOTA_LINE)
    assert eng._bb.write.call_count == 2, "写失败后下一次注入仍应尝试记账"

    eng2 = _engine()
    with patch.object(_rl, "_QUOTA_RETRY_SEC", 0):
        _run(eng2, QUOTA_LINE)
        _run(eng2, QUOTA_LINE)
    assert eng2._bb.write.call_count == 1, "写成功后 6h 内不再重复记账"

def test_quota_audit_cooldown_survives_restart():
    """重启（新引擎实例=空字典）后仍受 6h 冷却约束：从 bus 回填最近一条 [quota]。"""
    now = _rl.time.time()
    fact = MagicMock()
    fact.ts = now - 600                      # 10 分钟前刚记过账
    fact.t = f"[quota] {ROLE} 额度/出口硬限，改 1h 探活（第 1 次 2026-10-26 22:00）"

    eng = _engine()
    eng._bb.read.return_value = [fact]
    _run(eng, QUOTA_LINE)
    assert _facts(eng) == [], "同角色 6h 内已记过账 → 不应再写一条"

    # 旧记录超过 6h → 允许再记一条
    eng2 = _engine()
    fact_old = MagicMock()
    fact_old.ts = now - 7 * 3600
    fact_old.t = fact.t
    eng2._bb.read.return_value = [fact_old]
    _run(eng2, QUOTA_LINE)
    assert len(_facts(eng2)) == 1, "超过 6h 的旧账不该压住新账"


def test_transient_title_does_not_claim_quota_rejection():
    """else 分支是「非硬限」：标题不得声称被额度拒绝。

    今日 9/9 条该标题的 evidence 全是 upstream connection lost 这类流中断，
    下游按「额度耗尽」处置会换路由 / 等 1h 探活，方向完全错。
    """
    eng = _engine()
    assert len(_run(eng, "● API Error: upstream connection lost")) == 1
    facts = _facts(eng)
    assert facts[0].args[1].startswith("[rate_limit]"), facts
    assert "被额度拒绝" not in facts[0].args[1], facts[0].args[1]
    assert "upstream connection lost" in facts[0].kwargs["evidence"], facts
