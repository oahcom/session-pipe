"""R105 回归守卫：子链末步驳回不得让父步「完成」并放行。

生产事故（全库实测 7 例，2026-10-20）：
  子链 reviewer_pr_review 末步写 REJECTED / CHANGES_REQUESTED，但子链本身仍报
  completed（末步不拦是有意设计：子链无 next 可退）。回流处据 completed 把父步
  标 completed 并推进，驳回信号在父链彻底丢失。7 例的父链全部跑到终点：
    wf_654cd82159 (WF-product_delivery) s3 驳回 → 推进到 s5 completed
    wf_5f5eebc881 (WL-food_chain_loop) s6 驳回 → 推进到 s7 completed
    wf_5412acef35 / wf_5fac448592 / wf_8440ef784e76 / wf_28cd1f8775 → s6
    wf_21143e1367 (WF-dev_implement) → s3

旧实现只写一条 blocker 就无条件 advance，理由是「硬拦会静默卡死」。该前提已不
成立：engine 的 rejected 消费分支（有分类→建返工链；无分类→重投原步，封顶
_MAX_REJECTED_RETRY 后升级 coordinator）就是出口。放行才是无出口的那条路。

本测试锁定回流处的状态决策：驳回 → rejected（不推进），通过 → completed（推进）。
"""
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _decide(sub_last, completed_at=None):
    """复刻回流处的状态决策（与 task_generator 的回流分支同构）。

    WHY 用复刻而非直接调用：该决策内联在 _scan_tasks 的锁内循环里，依赖
    conn/lm/deferred 等多个生产对象。这里锁定的是**判据本身**（驳回→rejected，
    否则→completed），与生产那三行一一对应；结构一致性由
    test_verdict_rejected_gates_advance 在下游断言。
    """
    from pipeflow.task_generator import _subflow_verdict_rejected
    sr = {"s1": sub_last} if isinstance(sub_last, dict) else {}
    if _subflow_verdict_rejected(sr):
        return "rejected"
    return "completed"


@pytest.mark.parametrize("text", [
    "reviewer_pr_review/s1-complete — WL-food_chain_loop/s6 代码审查 REJECTED(链级): 0 P0 / 1 P1",
    "CHANGES_REQUESTED(设计闸门不通过): 0 P0 / 3 P1 / 6 P2",
    "代码审查 CHANGES_REQUESTED: 0 P0 / 8 P1 / 6 P2 / 5 P3",
    "需求对齐验证 判定: 未通过",
    "not_approved: 需修订后执行",
])
def test_rejected_subflow_last_step_marks_parent_rejected(text):
    assert _decide({"exit_messages": [{"text": text}]}) == "rejected", \
        "子链末步驳回时父步必须压回 rejected，不得 completed"


@pytest.mark.parametrize("text", [
    "reviewer_pr_review/s1-complete — 代码审查 APPROVED: 0 P0 / 0 P1",
    "全绿 — dkk/ssk active, ERROR=0",
    "无异常需修复 — 全服务自愈后复验绿",       # 否定式：健康报告，不是驳回
    "755测试PASS, 无需修复动作",
])
def test_approved_subflow_last_step_marks_parent_completed(text):
    assert _decide({"exit_messages": [{"text": text}]}) == "completed", \
        "通过/健康文案必须照常 completed"


def test_missing_exit_messages_does_not_block():
    """无 exit_messages（子链未经 bus 闭环）→ 不判驳回，保持原推进语义。"""
    assert _decide({}) == "completed"
    assert _decide(None) == "completed"


def test_confirmed_step_not_rejected():
    """已审批确认的子链末步不得判驳回（ADR-APPROVAL-AUTHORITY 消费者 #4）。

    审批确认不清除 exit_messages——那是第一轮的驳回文本，保留作产出证据。
    缺守卫时同一份文本会被二次判驳回，而 R105 之后这是**真阻断**（父步压回
    rejected 且不 advance），不再是可忽略的噪音。
    """
    import time
    from pipeflow.task_generator import _subflow_verdict_rejected
    confirmed = {
        "status": "completed",
        "confirmed_at": time.time(),
        "exit_messages": [{"text": "REJECTED: 18 FINDING"}],
    }
    assert not _subflow_verdict_rejected({"s1": confirmed}), \
        "confirmed_at 已落的步骤不得判驳回"
    # 对照：未确认的同一文案必须判驳回（守卫不能把真驳回一起放过）
    unconfirmed = {k: v for k, v in confirmed.items() if k != "confirmed_at"}
    assert _subflow_verdict_rejected({"s1": unconfirmed}), \
        "未确认的驳回文案必须仍然检出"


def test_rejected_marking_is_idempotent():
    """每 tick 重进回流块时不得刷新 rejected_at。

    `_rejected_wait_anchor` 读的正是 rejected_at；每 tick 重写会让
    _REJECTED_WAIT_MAX 的超时保护被永久续期（等返工链永远不超时），
    并每 tick 重发一条 blocker。
    """
    src = (Path(__file__).resolve().parents[1]
           / "src/pipeflow/task_generator.py").read_text(encoding="utf-8")
    assert '_sdata.get("status") != "rejected"' in src, \
        "压 rejected 必须幂等（仅在尚未 rejected 时写）"


def test_verdict_rejected_gates_advance():
    """结构守卫：回流处必须用 _verdict_rejected 同时控制 status 与 advance。

    防止回退成「只写 blocker 但照常推进」——那正是被修掉的缺陷形态。
    """
    src = (Path(__file__).resolve().parents[1]
           / "src/pipeflow/task_generator.py").read_text(encoding="utf-8")
    assert '_sdata["status"] = "rejected"' in src, "回流处必须写 rejected 状态"
    assert "and not _verdict_rejected" in src, "驳回时不得 advance 父工作流"
    # 反面：不允许在驳回分支里仍然无条件推进
    assert 'if _tmpl:\n' not in src.split("_verdict_rejected = ")[1][:2000], \
        "advance 条件必须带 not _verdict_rejected"
