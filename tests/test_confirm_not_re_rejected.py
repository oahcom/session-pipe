#!/usr/bin/env python3
"""已确认步骤不得被自身的历史驳回文本二次判驳（P0 回归守卫）。

生产事故：wf_256be862ea86/s3 于 13:02 被 reviewer 驳回（exit_messages 文本含
"REJECTED: 18 FINDING"）。17:32 与 17:36 reviewer 两次 `wf confirm` 通过，
DB 中 confirmed_at / approval_consumed_at 均已落库，但 status 停在 "rejected"
长达 5 小时，父链再未推进。

机制：confirm_step 先写 status=completed，同一事务内调 _advance_unsafe；后者
重读**同一份** step_results，其中 exit_messages 是第一轮留下的驳回文本（确认
时不清除）→ 关键词命中 "rejected" → 把刚落的 completed 当场改写回 rejected。
后果是步骤一旦被驳回过，此后永远无法通过审批——审批通道整体失效。

两条推进路径（manager._advance_unsafe 与 wf_lifecycle._advance_production_wf）
都装同款门禁，故必须各有一例覆盖。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_confirm_not_re_rejected.py -q
"""
import json
import signal
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lifecycle.manager import LifecycleManager
from pipeflow.engine import WorkflowEngine

_TEST_TIMEOUT = 20

# 生产原形文本：含 "REJECTED" 且带【业务问题】分类段（分类段决定门禁是否落库）
_REJECT_TEXT = (
    "技术实现/s3-complete — 代码审查 REJECTED: 18 FINDING "
    "(4 P1: 3接口未落地+命令注入+测试假绿), 违反验收'无P0/P1'"
)
_REJECT_EVIDENCE = "【业务问题→engineer】\n- P1 | skill_dir 未转义入 bash -c | src/skill_layout.py:75"


def _env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d


def _write_wf(wf_dir, name, steps):
    (wf_dir / f"{name}.json").write_text(
        json.dumps({"name": name, "title": name, "description": "", "steps": steps},
                   ensure_ascii=False))


def _step(sid, **ov):
    base = {
        "id": sid, "title": sid, "target_role": "test_role",
        "prompt_template": f"do {sid}",
        "exit_condition": {"bus_category": "notice", "text_contains": f"T_{sid}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "review",
    }
    base.update(ov)
    return base


def _seed_rejected_step(wf_id, lm, step_id, status="step_done_ready"):
    """复刻生产现场：step_done_ready + 历史驳回 exit_messages（无 approval_token）。"""
    sr = {step_id: {
        "status": status,
        "ts": time.time(),
        "notified_at": time.time() - 600,
        "exit_messages": [{"text": _REJECT_TEXT, "evidence": _REJECT_EVIDENCE,
                           "src": "reviewer", "ts": time.time() - 700}],
    }}
    lm.execute("UPDATE workflow_instances SET status='step_done_ready', step_results=? "
               "WHERE instance_id=?", (json.dumps(sr, ensure_ascii=False), wf_id))


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_confirmed_step_not_flipped_back_by_manager_advance(mock_run):
    """manager._advance_unsafe：已确认 + 历史驳回文本 → 必须推进，不得翻回 rejected。

    这是生产事故的直接回归：s3 确认后 status 被自己第一轮的驳回文本改写回
    rejected（confirmed_at 与 rejected 并存）。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _env()
    _write_wf(d, "conf_m", [_step("s1"), _step("s2", type="single")])
    eng = WorkflowEngine(d)
    rid = eng.start("conf_m", {})
    _seed_rejected_step(rid, eng._lifecycle, "s1")

    assert eng._lifecycle.confirm_step(rid, "s1", approved=True,
                                       reason="0 P0; 上轮 P1 已修") is True

    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?",
        (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    assert s1["status"] == "completed", \
        f"已确认步骤不得被历史驳回文本翻回，实际 status={s1['status']}（confirmed_at={s1.get('confirmed_at')}）"
    assert row["current_step_id"] == "s2", \
        f"确认后应推进到 s2，实际停在 {row['current_step_id']}"

    import shutil
    shutil.rmtree(d, ignore_errors=True)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_unconfirmed_rejection_still_blocks(mock_run):
    """防修过头：**未**确认的驳回必须照旧拦截（门禁本体不能失效）。

    与上例同为驳回文本，唯一区别是没有 confirmed_at。若修复把门禁整个删掉，
    真实驳回会被放行推进——比原缺陷更危险。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _env()
    _write_wf(d, "conf_u", [_step("s1"), _step("s2", type="single")])
    eng = WorkflowEngine(d)
    rid = eng.start("conf_u", {})
    _seed_rejected_step(rid, eng._lifecycle, "s1")

    eng._advance_production_wf(rid, eng._lifecycle, "conf_u", "s1",
                               json.loads(eng._lifecycle.query_one(
                                   "SELECT step_results FROM workflow_instances "
                                   "WHERE instance_id=?", (rid,))["step_results"]))

    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?",
        (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    assert s1["status"] == "rejected", f"未确认的驳回必须拦下，实际 {s1['status']}"
    assert row["current_step_id"] == "s1", "未确认的驳回不得推进"

    import shutil
    shutil.rmtree(d, ignore_errors=True)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_wf_lifecycle_advance_also_skips_gate_when_confirmed(mock_run):
    """wf_lifecycle._advance_production_wf 是第二条推进路径，需同款放行。

    两条实现历史上多次只装一条（R41 注释记录了 handoff/single 的驳回被吞），
    故本用例单独锁死 tick 路径的行为。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _env()
    _write_wf(d, "conf_w", [_step("s1"), _step("s2", type="single")])
    eng = WorkflowEngine(d)
    rid = eng.start("conf_w", {})

    # 已确认 + 历史驳回文本：模拟"角色回写 step_results 带旧 exit_messages"的现场
    sr = {"s1": {"status": "step_done_ready", "confirmed_at": time.time(),
                 "confirmed_by": "reviewer",
                 "exit_messages": [{"text": _REJECT_TEXT, "evidence": _REJECT_EVIDENCE}]}}
    eng._advance_production_wf(rid, eng._lifecycle, "conf_w", "s1", sr)

    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?",
        (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    assert s1["status"] != "rejected", \
        f"tick 路径同样不得把已确认步骤翻回 rejected，实际 {s1}"
    assert row["current_step_id"] == "s2", \
        f"tick 路径确认后应推进，实际停在 {row['current_step_id']}"

    import shutil
    shutil.rmtree(d, ignore_errors=True)


def test_requeue_drop_clears_confirmed_marker():
    """重投必须清 confirmed_at/by —— 否则新一轮真实驳回被误判为「已确认」放行。

    重投后旧确认记录若残留，两条路径的 `not _confirmed` 判据为假 → 门禁整体
    失效，驳回文本被静默放行。本用例锁死该不变量。
    """
    from pipeflow.engine import _REQUEUE_DROP
    assert "confirmed_at" in _REQUEUE_DROP, "重投未清 confirmed_at → 新一轮驳回失效"
    assert "confirmed_by" in _REQUEUE_DROP, "重投未清 confirmed_by → 残留审批人"


if __name__ == "__main__":
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, lambda s, f: (
            print(f"\nTIMEOUT after {_TEST_TIMEOUT}s"), sys.exit(1)))
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
