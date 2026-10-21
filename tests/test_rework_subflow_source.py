"""子链驳回 → 返工链必须能从子链末步的分类段建出。

现场（R110，2026-10-21）：
  wf_bf5721557a/s4 的子链 wf_sub_61581e191d0a 已 completed，末步 exit_messages
  里的 bus evidence（#428488）含完整【系统问题】1 条 +【业务问题】4 条。
  task_generator 判定驳回时**有意不搬**子链文案进父步（搬了 tick-rescue 会把
  rejected 抹回 completed），而 _create_rework_subflow 只读父步 → 解析必空 →
  建不出返工链 → 退回重投那条已完成的子链 → 产物未变必然再驳回 → 3 次封顶。

  本条契约：父步无分类段时，读子链末步（只读，不回写父步）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_rework_subflow_source.py -q
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402


class _Host(WorkflowLifecycleMixin):
    """只测纯读取路径：绕过 __init__，不触碰 DB/bus。"""

    def __init__(self, sub_row, evidence):
        self._sub_row = sub_row
        self._evidence = evidence
        self._bb = MagicMock()

    def _lifecycle_query(self, *a, **kw):
        return self._make_lifecycle()

    def _make_lifecycle(self):
        lm = MagicMock()
        lm.query.return_value = [self._sub_row]
        return lm

    def _bb_evidence(self, ids):
        return [self._evidence for _ in ids]


SUB_OK = {
    "current_step_id": "s1",
    "step_results": json.dumps({"s1": {"status": "completed", "exit_messages": [
        {"id": 428488, "text": "reviewer_pr_review/s1-complete — CHANGES_REQUESTED"}]}}),
}

EVIDENCE = """结论: CHANGES_REQUESTED

【系统问题】
- P1 | 任务描述首行引用已取消实例 | tasks.description

【业务问题】
- P2 | --all --report 静默失效 | disk_alert_judge.py:243
- P2 | 上游用例断言宿主状态 | tests/test_disk_alert_judge.py:88
"""


def _host(sub=SUB_OK, evidence=EVIDENCE):
    h = _Host(sub, evidence)
    h._lifecycle = h._make_lifecycle()
    return h


def test_parent_missing_text_falls_back_to_subflow_last_step():
    """父步无文案时，子链末步的 bus evidence 必须被取到并解析出分类。"""
    h = _host()
    text = h._subflow_last_step_text("wf_x", "s4", {"subflow_id": "wf_sub_1"})
    assert "【系统问题】" in text and "【业务问题】" in text, \
        f"没取到子链分类段: {text[:120]!r}"
    sys_issues, biz_issues, _, _ = h._parse_review_issues(text)
    assert len(sys_issues) == 1 and len(biz_issues) == 2, \
        f"分类解析丢失: sys={sys_issues} biz={biz_issues}"


def test_no_subflow_id_is_noop():
    """没有子链的步骤不得凭空去查库。"""
    h = _host()
    assert h._subflow_last_step_text("wf_x", "s4", {}) == ""


def test_missing_last_step_returns_empty():
    """子链末步不是 dict（状态被写坏）→ 返回空，不抛穿。"""
    h = _host(sub={"current_step_id": "s1", "step_results": '{"reason": "旧格式"}'})
    assert h._subflow_last_step_text("wf_x", "s4", {"subflow_id": "wf_sub_1"}) == ""


def test_text_with_classification_skips_bus_lookup():
    """新记录已带 evidence 时不再查 bus（省一次子进程/查询）。"""
    sub = {"current_step_id": "s1", "step_results": json.dumps({"s1": {
        "status": "completed",
        "exit_messages": [{"id": 1, "text": "x", "evidence": "【业务问题】\n- P2 | a | b\n"}]}})}
    h = _host(sub=sub)
    h._bb_evidence = MagicMock(side_effect=AssertionError("不应回查 bus"))
    text = h._subflow_last_step_text("wf_x", "s4", {"subflow_id": "wf_sub_1"})
    assert "【业务问题】" in text


if __name__ == "__main__":
    for name, fn in [
        ("父步空 → 读子链末步", test_parent_missing_text_falls_back_to_subflow_last_step),
        ("无 subflow_id 不查库", test_no_subflow_id_is_noop),
        ("末步损坏返回空", test_missing_last_step_returns_empty),
        ("已有分类不查 bus", test_text_with_classification_skips_bus_lookup),
    ]:
        fn(); print(f"  OK  {name}")
    print("全部通过")
