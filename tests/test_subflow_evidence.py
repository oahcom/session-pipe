#!/usr/bin/env python3
"""子链交付证据贯通（B8）回归守卫。

事故：food_chain_loop/s4 是容器步骤（子链 engineer_feature 干活），子链回流
只把父步盖 completed、不写父步 exit_messages → s5 的 qa 收到
「[任务描述] 测试验证（qa）」，不知道测什么，只能自己翻 DB/bus。

两条通路都必须带上游证据：
1. _fill_prompt_vars 的 {findings}/{target}/… 变量
2. _subflow_takeover 创建子链时的 tasks.description

Run: cd ~/session-pipeline && python3 -m pytest tests/test_subflow_evidence.py -q
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, _WORKFLOWS_DIR
from pipeflow.engine import Step


def _engine():
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    return eng


def test_container_step_drills_into_subflow():
    """父步无 exit_messages，但有 subflow_id → 取子链末步的产出。"""
    eng = _engine()
    eng._lifecycle.query_one.return_value = {
        "current_step_id": "s2",
        "step_results": json.dumps({
            "s1": {"status": "completed"},
            "s2": {"status": "completed", "exit_messages": [
                {"text": "engineer_feature/s2-complete — 落地页 AC 门禁",
                 "evidence": "gates.py 16 条二值判定 PASS=15"}]},
        }),
    }
    msgs = eng._step_deliverables("s4", {"s4": {"status": "completed", "subflow_id": "wf_sub_abc"}})
    text = eng._render_msgs(msgs)
    assert "gates.py 16 条二值判定" in text, f"应下钻取子链末步产出, got {msgs!r}"


def test_own_exit_messages_win_over_subflow():
    """父步自己写了 exit_messages → 不查库、不下钻。"""
    eng = _engine()
    eng._lifecycle.query_one.side_effect = AssertionError("不应查库")
    msgs = eng._step_deliverables("s1", {"s1": {
        "exit_messages": [{"text": "own"}]}})
    assert eng._render_msgs(msgs) == "own"


def test_no_evidence_returns_empty_not_raise():
    """既无 exit_messages 也无 subflow → 空表（调用方回退到 fallback）。"""
    eng = _engine()
    assert eng._step_deliverables("s1", {"s1": {"status": "completed"}}) == []
    assert eng._step_deliverables("missing", {}) == []
    assert eng._step_deliverables("s1", None) == []


def test_prompt_vars_use_subflow_evidence():
    """{findings} 等变量被上游（容器）证据填充，而不是回退到任务描述。"""
    eng = _engine()
    eng._lifecycle.query_one.return_value = {
        "current_step_id": "s1",
        "step_results": json.dumps({"s1": {"exit_messages": [
            {"text": "上游产出：site/index.html 12KB"}]}}),
    }
    step = Step(id="s5", title="测试验证", target_role="qa",
                prompt_template="发现: {findings}", exit_condition={})
    out = eng._fill_prompt_vars(
        step.prompt_template, step, "WL-food_chain_loop", "任务", "描述",
        {"s4": {"status": "completed", "subflow_id": "wf_sub_x"}},
        prev_step_id="s4", wf_id="wf_parent")
    assert "site/index.html 12KB" in out, f"应填上游证据, got {out!r}"


def test_prompt_carries_instance_scoped_artifact_convention():
    """带 wf_id 的 prompt 必须说明产物按实例取副本。

    模板写的上游路径是固定名（~/ccs-workspaces/pm/PRD.md），而同角色常有
    多条链并发写同名文件：实测 pm 的 PRD.md 被 scout 链覆盖，s2 architect 据此
    设计出无关对象，s3 reviewer 判「设计闸门失去可评对象」（#417441 vs #417534）。
    """
    eng = _engine()
    step = Step(id="s2", title="架构设计", target_role="product_architect",
                prompt_template="读 PRD 后产出 DESIGN", exit_condition={})
    out = eng._fill_prompt_vars(step.prompt_template, step, "product_delivery",
                                "任务", "描述", {}, prev_step_id="s1", wf_id="wf_abc123")
    assert "[产物归属]" in out, f"缺产物归属约定: {out!r}"
    assert "wf_abc123" in out, "约定里必须含本实例 id（否则角色不知道读哪份副本）"
    assert "读 PRD 后产出 DESIGN" in out, "原始 prompt 不得被覆盖"




def test_render_includes_evidence_body():
    """渲染带 evidence：分类段/细节在正文里，只拼 title 会丢。"""
    eng = _engine()
    out = eng._render_msgs([{"text": "标题", "evidence": "【业务问题→engineer】P1"}])
    assert "【业务问题→engineer】P1" in out


if __name__ == "__main__":
    for fn in (test_container_step_drills_into_subflow,
               test_own_exit_messages_win_over_subflow,
               test_no_evidence_returns_empty_not_raise,
               test_prompt_vars_use_subflow_evidence,
               test_render_includes_evidence_body,
               test_prompt_carries_instance_scoped_artifact_convention):
        fn()
