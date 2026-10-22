"""子链驳回路径必须能找到真正的评审子链，才能把明细补进返工提示。

事故（2026-10-22）：父步被压成 rejected 时 step_results **不写 subflow_id**，
只留 rejection_reason「子链 wf_sub_xxx 末步驳回: …」。旧实现只读 subflow_id →
直接 return ""→ 返工提示只剩一行摘要。按父步反查也不行：s3 下有 7 条子链，
最新的那条是返工链（wf_rw_*），不是产出评审的那条。
"""
import json

import pytest

import pipeflow.wf_lifecycle as wl

REVIEW = """# 审查报告

### 【业务问题】
**P1-1 | ADR-2 归因口径错 | DESIGN.md:247**
**P2-1 | 结构自检指向规范名 | TASKS.json:162**
### 【系统问题】
**S-P1-1 | 规范名被兄弟链覆盖 | GATE_REPORT.md:21**
"""

CHILD = "wf_sub_e348fe52d86e"


class _Lifecycle:
    def query(self, sql, params):
        if "current_step_id, step_results" in sql:
            sr = {"s1": {"status": "completed",
                         "exit_messages": [{"id": 437592,
                                            "text": f"reviewer_pr_review/s1-complete {CHILD} — CHANGES_REQUESTED 0P0/3P1"}]}}
            return [{"current_step_id": "s1", "step_results": json.dumps(sr)}]
        return []


class _BB:
    def __init__(self):
        self.calls = 0

    def _conn(self):
        raise AssertionError("本用例不查 bus")

    def write(self, *a, **k):
        raise AssertionError


class _Engine(wl.WorkflowLifecycleMixin):
    def __init__(self):
        self._lifecycle = _Lifecycle()
        self._bb = _BB()

    def _bb_evidence(self, ids):
        return []


@pytest.fixture()
def workspaces(tmp_path, monkeypatch):
    d = tmp_path / "reviewer"
    d.mkdir()
    (d / f"REVIEW-{CHILD}.md").write_text(REVIEW, encoding="utf-8")
    monkeypatch.setattr(wl, "_CCS_WORKSPACES", tmp_path)


def test_subflow_id_missing_still_reads_report(workspaces):
    """subflow_id 缺失时按 rejection_reason 里的实例 id 找子链，再读报告明细。"""
    sr = {"status": "rejected", "rejected_by": "subflow_verdict",
          "rejection_reason": f"子链 {CHILD} 末步驳回: reviewer_pr_review/s1-complete {CHILD} — CHANGES_REQUESTED 0P0/3P1"}
    txt = _Engine()._subflow_last_step_text("wf_0c5e3d807ede", "s3", sr)
    sys_i, biz_i, _, _ = wl.WorkflowLifecycleMixin._parse_review_issues(txt)
    assert len(biz_i) == 2, txt
    assert len(sys_i) == 1, txt
    assert "247" in biz_i[0]
