#!/usr/bin/env python3
"""评审结论三态（approved/rejected/unknown）fail-closed 回归。

R134：旧判据 is_rejected 只有布尔，unknown（评审人只回「已完成」/空文本）落成
False = 通过 —— 未评审的产出带着链条继续走（P0-2）；末步整段跳过门禁，驳回仍
把实例置 completed（P0-3）。

Run: cd ~/wt-pipeline && python3 -m pytest tests/test_review_gate_verdicts.py -q
"""
import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import review_gate
from pipeflow.engine import WorkflowEngine


def _make_env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _write_wf(wf_dir, name, steps):
    (wf_dir / f"{name}.json").write_text(json.dumps(
        {"name": name, "title": name, "description": "", "steps": steps}, ensure_ascii=False))


def _step(step_id, **overrides):
    base = {
        "id": step_id, "title": step_id, "target_role": "test_role",
        "prompt_template": f"do {step_id}",
        "exit_condition": {"bus_category": "notice",
                           "text_contains": f"TOK_{step_id.upper()}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(overrides)
    return base


def _ticks(eng, n=4):
    for _ in range(n):
        time.sleep(0.05)
        eng.run_once()


def _blocker_deliveries(eng, needle):
    """同一条 blocker 的投递次数：facts 是 UPSERT（同文本只一行），
    真实播报次数看 log 表 —— 每次 write 插一行 log。"""
    conn = sqlite3.connect(eng._bb.db_path)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM log l JOIN facts f ON f.id = l.fid WHERE f.t LIKE ?",
            (f"%{needle}%",)).fetchone()[0]
    finally:
        conn.close()


def test_review_verdict_three_states():
    assert review_gate.review_verdict("") == "unknown"
    assert review_gate.review_verdict("已完成") == "unknown"
    assert review_gate.review_verdict("APPROVED: 0 P0") == "approved"
    assert review_gate.review_verdict("CHANGES_REQUESTED: 3 P1") == "rejected"
    assert review_gate.is_rejected("CHANGES_REQUESTED") is True
    # 存量语义保留：unknown 对 is_rejected 仍是 False（fail-open 面保持不变）
    assert review_gate.is_rejected("已完成") is False


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_unknown_verdict_blocks_review_step_and_notifies_once(mock_run):
    """reviewer 完成但没写结论 → 不推进、留在 step_done_ready、blocker 只播一次。"""
    d = _make_env()
    _write_wf(d, "rev_unknown", [_step("s1", type="review"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("rev_unknown", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 已完成", src="test_role")
    _ticks(eng)

    s = eng.status(rid)
    assert s["current_step"] != "s2", f"unknown 不得推进，实际到 {s['current_step']}"
    assert s["results"]["s1"].get("status") == "step_done_ready", s["results"]["s1"]
    assert not s["results"]["s1"].get("rework_ids"), "unknown 不是驳回，不该建返工链"
    assert s["status"] == "running"

    n = _blocker_deliveries(eng, f"{rid[:12]}/s1 评审无结论")
    assert n == 1, f"同一 (实例,步骤,文本) 只能播报一次，实际 {n} 次"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_last_step_rejection_does_not_complete(mock_run):
    """末步 review 判 rejected → 不得把实例置 completed。"""
    d = _make_env()
    _write_wf(d, "rev_last", [_step("s1", type="review")])
    eng = WorkflowEngine(d)
    rid = eng.start("rev_last", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 — 代码审查 CHANGES_REQUESTED: 3 P1", src="test_role")
    # 末步收尾在下一个 tick 的 _advance_production_wf 里判：逐 tick 记实例状态，
    # 既要有 tick 判出 rejected，又要全程不含 completed（旧实现两 tick 后即 completed）
    _statuses, _saw_rejected = [], False
    for _ in range(8):
        _ticks(eng, 1)
        s = eng.status(rid)
        _statuses.append(s["status"])
        _saw_rejected = _saw_rejected or s["results"]["s1"].get("status") == "rejected"

    assert _saw_rejected, f"末步驳回必须判 rejected，状态序列 {_statuses}"
    assert "completed" not in _statuses, f"末步驳回不得完成，状态序列 {_statuses}"
