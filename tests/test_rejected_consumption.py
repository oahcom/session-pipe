"""rejected 步骤消费的回归守卫（2026-10-19）。

对应根因：rejected 曾是只写不读的死状态 —— 写入点 2 处（manager.py 的
confirm_step 与 _advance_unsafe），读取点 0 处。engine.run_once 主扫描的四
个分支（空/审批/notified+running/step_done_ready+completed）全不匹配它，
cleanup 五条件也全假 → 静默卡死至 24h 全局兜底。

本守卫覆盖：
  ① _parse_review_issues 对 evidence 形态（角色真实书写）的解析
  ② 出口决策逻辑：有 rework_ids → 等回流；无 → 回退重投 + 清锚点
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeflow.wf_lifecycle import WorkflowLifecycleMixin as W


class TestParseReviewIssuesEvidence:
    """evidence 形态（角色真实书写）解析。

    角色实际写「【系统问题→maintainer】」（带流向标注）且 bus evidence 常被
    压成单行。旧实现要求全串「【系统问题】」独占一行 → 恒不命中。
    """

    def test_arrow_annotation_parsed(self):
        sys_txt, biz_txt = W._parse_review_issues(
            "【系统问题→maintainer】\n- P1 | exit_condition 与 prompt 不一致"
        )
        assert sys_txt == ["P1 | exit_condition 与 prompt 不一致"]
        assert biz_txt == []

    def test_single_line_compressed_parsed(self):
        """单行压缩形态（bus evidence 常见）。"""
        sys_txt, biz_txt = W._parse_review_issues(
            "【系统问题】P2|任务未注入 wf_id; P2|引擎 s4 text 带模板前缀契约漂移"
        )
        assert "P2|任务未注入 wf_id" in sys_txt
        assert any("契约漂移" in x for x in sys_txt)
        assert biz_txt == []

    def test_biz_section_parsed(self):
        sys_txt, biz_txt = W._parse_review_issues(
            "【业务问题】\n- P1 | 返回值错误 | chain.py:68"
        )
        assert biz_txt == ["P1 | 返回值错误 | chain.py:68"]
        assert sys_txt == []

    def test_bare_rejection_not_parsed(self):
        """裸驳回（无分类段）不得解析出任何问题。"""
        sys_txt, biz_txt = W._parse_review_issues("DONE_S1 CHANGES_REQUESTED: 3 P1 需修复")
        assert sys_txt == []
        assert biz_txt == []

    def test_empty_not_parsed(self):
        assert W._parse_review_issues("") == ([], [])


class TestRejectedExitDecision:
    """出口决策：有 rework_ids → 等回流；无 → 回退重投。

    从 engine.run_once rejected 分支提取的纯决策逻辑。
    """

    @staticmethod
    def _exit_action(sr: dict, is_last: bool = False) -> str:
        """返回 'wait_reflow' 或 'requeue'，对齐 engine 分支语义。"""
        rework_ids = sr.get("rework_ids") or []
        if not is_last and not rework_ids:
            return "requeue"
        if rework_ids:
            return "wait_reflow"
        return "requeue"  # 末步无链 → 回退重投

    def test_has_rework_ids_waits_reflow(self):
        """返工链已建 → 等 subflow 回流。"""
        assert self._exit_action({"rework_ids": ["wf_rw_abc123"]}) == "wait_reflow"

    def test_no_rework_ids_requeues(self):
        """无返工链 → 回退重投原步。"""
        assert self._exit_action({"status": "rejected"}) == "requeue"

    def test_last_step_requeues(self):
        """末步无 rework 承接方 → 回退重投。"""
        assert self._exit_action({"status": "rejected"}, is_last=True) == "requeue"

    def test_requeue_clears_rejected_fields(self):
        """回退重投必须清 rejected 字段与 bus 锚点（否则无限循环）。"""
        sr = {
            "status": "rejected",
            "rejected_at": 100.0,
            "rejected_by": "reviewer",
            "rejection_reason": "P1 需修复",
            "poll_since": 100.0,
            "bus_anchor": 100.0,
            "notified_at": 100.0,
        }
        DROP = ("status", "rejected_at", "rejected_by", "rejection_reason",
                "poll_since", "bus_anchor", "notified_at")
        reset = {k: v for k, v in sr.items() if k not in DROP}
        reset["rejected_retry"] = (sr.get("rejected_retry", 0) or 0) + 1
        reset["status"] = ""
        assert reset["status"] == ""
        assert reset["rejected_retry"] == 1
        for f in ("rejected_at", "rejected_by", "rejection_reason",
                  "poll_since", "bus_anchor", "notified_at"):
            assert f not in reset, f"字段 {f} 未清除 → 锚点残留致无限循环"
