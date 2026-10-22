#!/usr/bin/env python3
"""R159 回归守卫：qa bug_report 的三条引擎缺陷（bus #441999）。

B-01 「子链被强行取消」的真实根因不是 queue-cap（s2 预算 120min，实际排队
      27min），而是父链末步收尾时用裸 UPDATE 取消子链：不写 completed_at、
      不写 workflow_logs、不同步 task —— 7d 实测 cancelled 行 completed_at=NULL
      130 条、无取消日志 77 条，事后无法归因。改为走 close_wf。

B-02 一条完成消息闭合多条实例：`if wf_id in ids: return False` 让「并列两个
      同模板兄弟 id」的消息对两边都放行（#441838）。加 siblings 判据；父子
      两层记账（#413744/#415150）自身 id 不是同模板兄弟，仍须放行。

B-03 `{title}`/`{description}` 未替换：子链 description 是父模板正文的截断，
      自带字面占位符，replace 把键写回值里 → _send_to_role 守卫拦下 → blocker
      (#441958)。首步路径 engine.py:1238 已清洗，_fill_prompt_vars 与
      integrity 的提醒路径没有。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_r159_engine_three_defects.py -q
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.step_handler import _claims_own_step_and_foreign_instance  # noqa: E402


# ── B-01 ──────────────────────────────────────────────────────────────

def _drive_last_step(sub_status: str, created_offset: float) -> list:
    """跑末步收尾分支，返回 close_wf 收到的 (wf_id, status) 序列。"""
    import time

    from pipeflow.engine import WorkflowEngine, WorkflowDef, Step

    tmp = Path(tempfile.mkdtemp())
    eng = WorkflowEngine(workflows_dir=tmp)
    wf = WorkflowDef(
        name="tpl", title="t", description="d",
        steps=[Step(id="s5", title="验收", target_role="qa",
                    prompt_template="x", exit_condition={})])
    eng._workflows = {"tpl": wf}

    closed = []
    _sub_results = {"s4": {"status": "completed", "subflow_id": "wf_sub_dead"}}

    class _Lm:
        def close_wf(self, wfid, status="completed", **kw):
            closed.append((wfid, status))

        def _conn_tx(self):
            class _Ctx:
                def __enter__(self):
                    c = MagicMock()
                    c.execute.return_value.fetchone.return_value = {
                        "status": "running",
                        "step_results": json.dumps(_sub_results),
                        "created_at": time.time() - created_offset,
                    }
                    c.in_transaction = False
                    return c

                def __exit__(self, *a):
                    return False
            return _Ctx()

    eng._lm = _Lm()
    eng._advance_production_wf("wf_parent", eng._lifecycle, "tpl", "s5",
                               {"s4": {"status": "completed"}})
    return closed


def test_forced_subflow_cancel_goes_through_close_wf():
    """超宽限的子链必须经 close_wf 收尾（写 completed_at / 日志 / 同步 task）。"""
    closed = _drive_last_step("running", created_offset=3600)
    assert ("wf_sub_dead", "cancelled") in closed, \
        f"子链强杀必须走 close_wf，实际收到 {closed}"
    assert ("wf_parent", "completed") in closed, "父链仍按原语义 completed"


def test_young_subflow_not_force_cancelled():
    """5 分钟宽限内的子链不得被杀，父链也不收尾。"""
    closed = _drive_last_step("running", created_offset=10)
    assert closed == [], f"宽限内不应 close 任何链，实际 {closed}"


# ── B-02 ──────────────────────────────────────────────────────────────

def test_parallel_sibling_ids_rejected():
    """#441838 原文：两条同模板兄弟 id 并列，不得同时闭合两边。"""
    text = ("TEST_REPORT: qa_test_execute/s1-complete wf_sub_02b1a193e34e + "
            "wf_sub_673fc23bc46d — 准入 9/10=90.0% Go(带条件)")
    siblings = {"wf_sub_673fc23bc46d"}
    assert _claims_own_step_and_foreign_instance(
        text, "wf_sub_02b1a193e34e", "qa_test_execute", "s1",
        siblings=siblings) is True
    assert _claims_own_step_and_foreign_instance(
        text, "wf_sub_673fc23bc46d", "qa_test_execute", "s1",
        siblings={"wf_sub_02b1a193e34e"}) is True


def test_parent_child_double_bookkeeping_still_passes():
    """#413744：子链 id + 父链 id 并列是合法记账，不是兄弟串味。"""
    text = ("[test_report] qa_test_execute/s1-complete + full_dev_pipeline/s3-complete "
            "(wf_sub_7dc92af6480c/s1, 父链 wf_5412acef35/s3): 用例16 pass15")
    # 子链收 s1：父链 wf_5412 不是同模板兄弟（siblings 只含 qa_test_execute 同父实例）
    assert _claims_own_step_and_foreign_instance(
        text, "wf_sub_7dc92af6480c", "qa_test_execute", "s1",
        siblings=set()) is False
    # 父链收 s3：子链 id 属别的模板，同样不是兄弟
    assert _claims_own_step_and_foreign_instance(
        text, "wf_5412acef35", "WL-full_dev_pipeline", "s3",
        siblings=set()) is False


def test_multi_id_without_sibling_still_passes():
    """没有 siblings 上下文时保持旧行为：多 id 不一律拦（105/164 条裸匹配无 id）。"""
    text = "wf_other_inst / s2-complete 已产出，见 wf_mine_inst"
    assert _claims_own_step_and_foreign_instance(
        text, "wf_mine_inst", "pm_requirements", "s2") is False


# ── B-03 ──────────────────────────────────────────────────────────────

def _fill(desc: str, title: str = "落地页") -> str:
    from pipeflow.engine import Step, WorkflowEngine
    tpl = "【本次任务】\n标题: {title}\n描述: {description}\n"
    eng = WorkflowEngine(workflows_dir=Path(tempfile.mkdtemp()))
    step = Step(id="s5", title="验证与验收", target_role="qa",
                prompt_template=tpl, exit_condition={})
    return eng._fill_prompt_vars(tpl, step, "tpl", title, desc, {}, "")


def test_fill_prompt_vars_scrubs_nested_placeholders():
    """description 自带 {title}/{description} 时不得把它们写回 prompt。"""
    import re
    desc = "验证与验收 | 【本次任务】 | 标题: {title} | 描述: {description} | 【怎么做】"
    out = _fill(desc)
    assert not re.search(r"\{[a-z_]+\}", out), f"残留未替换变量: {out!r}"
    assert "落地页" in out


def test_fill_prompt_vars_keeps_plain_desc():
    """普通 description 不受清洗影响。"""
    out = _fill("目标: 交付单文件落地页")
    assert "交付单文件落地页" in out


if __name__ == "__main__":
    for fn in (test_forced_subflow_cancel_goes_through_close_wf,
               test_young_subflow_not_force_cancelled,
               test_parallel_sibling_ids_rejected,
               test_parent_child_double_bookkeeping_still_passes,
               test_multi_id_without_sibling_still_passes,
               test_fill_prompt_vars_scrubs_nested_placeholders,
               test_fill_prompt_vars_keeps_plain_desc):
        fn()
        print(f"OK {fn.__name__}")
