"""链条收敛回归（2026-10-22）：返工派对人、系统链不阻断、推进 prompt 带判据。

三条断点都有生产现场（同一批 4 条 WF-product_delivery 主链）：
  1. DESIGN 缺陷被默认派给 engineer_feature → 工程改不了文档 → 重审原样驳回；
  2. 系统问题链（maintainer）与业务链混在一个阻断集合里 → 维护队列 91 深时主链扣 8h+；
  3. 推进 prompt 只有 [实例]，{acceptance_criteria} 被空话顶替 → 角色写不出契约标题。
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeflow.engine import WorkflowEngine, _blocking_rework_sts, _rework_exit
from test_workcycle_e2e import _cleanup, _make_env, _step, _write_wf


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_biz_issues_without_arrow_go_to_subject_role(_mock):
    """有分类但没标流向 → 返工对象是被审产物的人，不是默认 engineer。

    生产原形 wf_85faf589be90/s3：reviewer 的 4 条 P1 全是 DESIGN.md 缺陷，
    未标箭头 → 建 engineer_feature → 重审原样驳回 → 3 次封顶升级，链停摆。
    """
    d = _make_env()
    _write_wf(d, "chain", [_step("s1", target_role="product_architect"),
                           _step("s2", target_role="reviewer", type="review")])
    _write_wf(d, "architect_full_design", [_step("s1", target_role="product_architect")])
    _write_wf(d, "engineer_feature", [_step("s1", target_role="engineer")])
    eng = WorkflowEngine(d)
    rid = eng.start("chain", {})
    review = eng._workflows["chain"].steps[1]
    eng._create_rework_subflow(
        rid, eng._lifecycle, "chain", "s2", review, None,
        {"s2": {"exit_messages": [{"text":
          "CHANGES_REQUESTED\n【业务问题】\n- P1 | AC3 统计无法回传 | DESIGN.md:47"}]}})
    row = eng._lifecycle.query_one(
        "SELECT step_results FROM workflow_instances WHERE instance_id=?", (rid,))
    ids = json.loads(row["step_results"])["s2"]["rework_ids"]
    inst = eng._lifecycle.query_one(
        "SELECT template_id, assignee FROM workflow_instances WHERE instance_id=?", (ids[0],))
    assert inst["template_id"] == "architect_full_design", \
        f"DESIGN 缺陷应回流 product_architect，实际 {inst['template_id']}"
    assert inst["assignee"] == "product_architect"
    _cleanup(d)


def test_maintainer_rework_does_not_block_requeue():
    """系统问题链不得阻断重审；业务链仍在跑时必须等。"""
    rows = [{"status": "running", "template_id": "maintainer_health_monitor"},
            {"status": "completed", "template_id": "architect_full_design"}]
    assert _blocking_rework_sts(rows) == ["completed"]
    assert _rework_exit(_blocking_rework_sts(rows)) == "requeue"
    only_sys = [{"status": "running", "template_id": "maintainer_health_monitor"}]
    assert _rework_exit(_blocking_rework_sts(only_sys)) == "requeue", \
        "只有维护链时产品链必须能继续重审"
    biz_running = [{"status": "running", "template_id": "architect_full_design"}]
    assert _rework_exit(_blocking_rework_sts(biz_running)) == "wait"


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_advance_prompt_carries_step_and_contract(_mock):
    """推进 prompt 必须带步骤号 + 模板声明的匹配串与产出文件。"""
    d = _make_env()
    _write_wf(d, "chain", [_step("s1"), _step(
        "s2", target_role="engineer",
        exit_condition={"bus_category": "code_fix", "text_contains": "chain/s2-complete",
                        "source_contains": "engineer", "timeout_minutes": 30},
        exit_schema={"required": ["*.py"]})])
    eng = WorkflowEngine(d)
    step = eng._workflows["chain"].steps[1]
    out = eng._fill_prompt_vars(step.prompt_template, step, "chain", "t", "d", {},
                                prev_step_id="s1", wf_id="wf_test")
    assert "[工作流] chain | 步骤: s2" in out
    assert "chain/s2-complete" in out, "契约匹配串必须原样出现，否则角色写不中"
    assert "*.py" in out
    _cleanup(d)
