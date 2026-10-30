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


def test_evidence_replay_must_not_count_as_this_step_verdict():
    """evidence 里的"返工来源 CHANGES_REQUESTED"不是本步结论（实测 12/873 步被误判）。

    wf_lifecycle 的结论只拼 text、分类才拼 text+evidence，两条判据喂不同输入。
    这条钉住"拼进去就会翻成 rejected"——哪次合并图省事合成一份输入，它会红。
    """
    text = "已按 PRD 拆 6 个故事，明细见 bus #440123"
    evidence = "返工来源 CHANGES_REQUESTED — 上一轮 P1 两条"
    assert review_gate.review_verdict(text) == "unknown"
    assert review_gate.review_verdict(text + " " + evidence) == "rejected"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_unknown_verdict_blocks_review_step_and_notifies_once(mock_run):
    """reviewer 完成但没写结论 → 不推进、留在 step_done_ready、blocker 只播一次。"""
    d = _make_env()
    # 执行者必须是审查角色：unknown 判据 = review/gate 类型 + is_review_role(执行者)，
    # 只看 type 会把 maintainer 的健康检查（7 天 24 条）一起冻住。
    _write_wf(d, "rev_unknown", [_step("s1", type="review", target_role="reviewer"),
                                 _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("rev_unknown", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 已完成", src="reviewer")
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
def test_report_typed_review_step_advances_despite_unknown(mock_run):
    """type=review 但执行者是报告型角色（maintainer）→ unknown 不得拦。

    7 天生产库实测：review/gate 类型步骤 unknown+empty 41 条，其中 maintainer 24
    条全是健康检查/修复验证。若只按 type 判 unknown，维护循环整条冻死。
    """
    d = _make_env()
    _write_wf(d, "health_rev", [_step("s1", type="review", target_role="maintainer"),
                                _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("health_rev", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 服务全绿零僵尸 mem 54.9%", src="maintainer")
    _ticks(eng, 6)

    s = eng.status(rid)
    assert s["current_step"] == "s2", \
        f"报告型步骤不该被 unknown 卡住，实际 {s['current_step']} / {s['results']}"


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


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_last_step_rejection_fails_fast_without_retry(mock_run):
    """末步被驳回 → 直接按 failed 收口，不进重投与 2h 升级窗口。

    旧实现：末步驳回落进「无返工链 → 重投原步」，产物不变则必然再驳回，封顶
    _MAX_REJECTED_RETRY(3) 后升级 coordinator，再等 _REJECTED_ESCALATED_MAX(7200s)
    无人处置才 failed。实测 wf_971532460c9e/s2 正是这条轨道：
    retry=3 + rework_escalated_at 落锤后 7209s 由定时器判死。
    """
    d = _make_env()
    _write_wf(d, "rev_last_fast", [_step("s1", type="review")])
    eng = WorkflowEngine(d)
    rid = eng.start("rev_last_fast", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 — 代码审查 CHANGES_REQUESTED: 3 P1", src="test_role")
    _ticks(eng, 8)

    s = eng.status(rid)
    assert s["status"] == "failed", \
        f"末步驳回应立即 failed，实际 {s['status']} / {s['results'].get('s1')}"
    sr = s["results"]["s1"]
    assert not sr.get("rework_escalated_at"), "不该走 2h 升级窗口"
    assert (sr.get("rejected_retry") or 0) == 0, f"不该重投，retry={sr.get('rejected_retry')}"
    log = eng._lifecycle.query_one(
        "SELECT detail FROM workflow_logs WHERE workflow_instance_id=? AND action='wf_closed'",
        (rid,))
    assert "CHANGES_REQUESTED" in (log["detail"] or ""), f"终态 reason 丢失驳回原文: {log}"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_last_step_classified_rejection_builds_rework(mock_run):
    """单步链带分类的驳回 → 建返工链并保持运行，不得直接 failed。

    生产 24h 18 条 reviewer_pr_review 子链都是单步，全部走
    `last step s1 rejected (retry=0)`，0 条返工链；驳回文案里实际带
    【业务问题→engineer】，建链能力早已存在，只是被 `_has_next` 门拦住。
    """
    d = _make_env()
    _write_wf(d, "rev_last_rework", [_step("s1", type="review")])
    _write_wf(d, "engineer_feature", [_step("s1", target_role="engineer")])
    eng = WorkflowEngine(d)
    rid = eng.start("rev_last_rework", {})

    eng.run_once()
    eng._bb.write(
        "notice",
        "TOK_S1 — CHANGES_REQUESTED 0 P0 / 1 P1\n"
        "【业务问题→engineer】\n- P1 | 返回值错误 | chain.py:68",
        src="test_role",
    )
    _ticks(eng, 8)

    s = eng.status(rid)
    sr = s["results"]["s1"]
    assert s["status"] != "failed", f"有分类的单步驳回不得直接判死：{s}"
    assert sr.get("rework_ids"), f"必须建出返工链：{sr}"
    assert not sr.get("rework_escalated_at"), "刚建链不该进入升级终结期"
    rows = eng._lifecycle.query(
        "SELECT instance_id, template_id FROM workflow_instances "
        "WHERE parent_wf_id=? AND subflow_source_step_id='s1'", (rid,))
    assert len(rows) == 1, f"应只建一条返工链，实际 {rows}"
    assert rows[0]["template_id"] == "engineer_feature", rows


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_multi_step_last_rejection_still_fails_fast(mock_run):
    """多步链末步即使带分类也保持 fail-fast：R173 只放开单步链。"""
    d = _make_env()
    _write_wf(d, "multi_last", [_step("s1"), _step("s2", type="review")])
    _write_wf(d, "engineer_feature", [_step("s1", target_role="engineer")])
    eng = WorkflowEngine(d)
    rid = eng.start("multi_last", {})

    eng.run_once()
    eng._bb.write("notice", "TOK_S1 完成", src="test_role")
    _ticks(eng, 3)
    eng._bb.write(
        "notice",
        "TOK_S2 — CHANGES_REQUESTED\n【业务问题→engineer】\n- P1 | x | y.py:1",
        src="test_role",
    )
    _ticks(eng, 8)

    s = eng.status(rid)
    assert s["status"] == "failed", f"多步末步仍应 failed：{s}"
    rows = eng._lifecycle.query(
        "SELECT instance_id FROM workflow_instances "
        "WHERE parent_wf_id=? AND subflow_source_step_id='s2'", (rid,))
    assert rows == [], f"多步末步不应建返工链：{rows}"

