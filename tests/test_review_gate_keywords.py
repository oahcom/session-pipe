"""R105 回归守卫：驳回词表的精确性与单点收敛。

生产事故（2026-10-20 全库 900 实例实测）：
  1. `需修复` 是 `无需修复` 的子串。maintainer 健康巡检报告固定写「无异常需修复」
     「无系统级异常需修复」，18 例命中全为误报——门禁把"一切正常"判成"被驳回"。
  2. `回退` 在生产文案里作动词常态出现：「回退重投第4次」（引擎自己的重投计数）、
     「代码回退 time.time()」、「5章节+门禁+回退落地」，2 例皆误报。
  3. 同一份词表在 wf_lifecycle / lifecycle.manager / task_generator 三处各写一份，
     靠注释约定同步——"未通过"曾在后者缺失，同一条文案在两条推进路径上判定相反
     （wf_3565289ca783/s3）。

本测试锁定：词表的精确语义 + 三处调用点必须共用同一声明（防再次各写一份）。
"""
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from review_gate import (  # noqa: E402
    APPROVED, REJECT_KEYWORDS, is_rejected, review_verdict, step_verdict,
)


# ── 必须判为驳回（真驳回文案，生产实测样本）──

@pytest.mark.parametrize("text", [
    "reviewer_pr_review/s1-complete — 代码审查 REJECTED(链级): 0 P0 / 1 P1",
    "CHANGES_REQUESTED(设计闸门不通过): 0 P0 / 3 P1 / 6 P2",
    "设计评审闸门 CHANGES_REQUESTED: 12 FINDING",
    "需求对齐验证 (wf_3565289ca783) 判定: 未通过",
    "not_approved: 需修订后执行",
])
def test_real_rejections_detected(text):
    assert is_rejected(text), "真驳回文案必须判为驳回: %s" % text


# ── 必须不判为驳回（误报源，生产实测样本）──

@pytest.mark.parametrize("text", [
    "无异常需修复 — 全服务自愈后复验绿, 端口 8890/20128/8767 三通",
    "无系统级异常需修复, 服务全绿 (dkk/ssk NRestarts=1)",
    "结论: 无需修复, 工作流 s2 可关闭.",
    "755测试PASS, 无需修复动作",
    "异常诊断: 重推漏填提示变量根因已消除. 无系统级异常(services 全绿)",
    "wf_3f71a2a78 回退重投第4次 / 2 个 wf_rw_* 排队10次被回收",
    "实际: 代码回退time.time()但测试仍断言拒绝(None)",
    "文档驱动5阶段工作流技术方案 — 5章节+门禁+回退落地",
    "快速健康检查: 全绿, 4 观察项无故障",
])
def test_false_positives_not_detected(text):
    assert not is_rejected(text), "健康/叙述文案不得判为驳回: %s" % text


def test_empty_and_none_safe():
    assert not is_rejected("")
    assert not is_rejected(None)


def test_dropped_keywords_are_not_in_table():
    """`需修复` 与 `回退` 已确证为纯误报源，不得回流。

    它们正是事故根因：前者是 `无需修复` 的子串，后者是生产高频动词。
    重新加入前必须先证明存在「仅靠它们命中」的真驳回——全库实测为 0 例。
    """
    assert "需修复" not in REJECT_KEYWORDS
    assert "回退" not in REJECT_KEYWORDS


@pytest.mark.parametrize("text", [
    "RC 判定 NO-GO(闸门1/2/3 未过)",
    "No-Go: 3 blocking findings, 需返工",
    "验收结论 nogo",
])
def test_no_go_verdict_detected(text):
    """"no-go" 是生产里真实的驳回结论，词表缺它 → 判 unknown → 被放行。

    7d 实测：含 No-Go 的步骤 16 条，其中 5 条判 unknown、4 条已推进下游
    （wf_092c460259/s4、wf_7fc3e87130/s4、wf_59263c95f86/s3、wf_sub_45bdd808e/s1）。
    """
    assert is_rejected(text), "NO-GO 结论必须判为驳回: %s" % text


def test_pass_rate_is_not_a_verdict():
    """"通过率" 是指标名词，不是"通过"结论。

    实测假批准：wf_02192e3126/s4「通过率81.8% < 90% 验收未达标 No-Go」被
    _PASS_WORD 的「通过」判成 approved —— 未达标的产物被当通过。
    """
    assert review_verdict("通过率81.8% < 90% 验收未达标 No-Go") == "rejected", \
        "未达标 + No-Go 不得判通过"
    assert review_verdict("通过率 95%，全部达标") != "approved", \
        "只报通过率不构成通过结论"
    assert review_verdict("APPROVED: 0 P0") == "approved", "真正的结论词不受影响"


def test_case_insensitive_for_english():
    """英文关键词生产里出现过 changes_requested / CHANGES_REQUESTED / REJECTED。"""
    assert is_rejected("changes_requested")
    assert is_rejected("CHANGES_REQUESTED")
    assert is_rejected("Rejected")


# ── 单点收敛：三处调用点必须共用同一份声明 ──

def test_all_call_sites_share_single_source():
    """wf_lifecycle / task_generator / manager 必须 import review_gate，
    不得再各自内联词表元组（这正是三方漂移的成因）。"""
    repo = Path(__file__).resolve().parents[1]
    for rel in ("src/pipeflow/wf_lifecycle.py",
                "src/pipeflow/task_generator.py",
                "src/lifecycle/manager.py"):
        src = (repo / rel).read_text(encoding="utf-8")
        assert "review_gate" in src, "%s 未接入 review_gate 单一真相源" % rel
        assert '"需修复", "回退"' not in src, "%s 仍内联旧词表元组" % rel


# ── 否定式语境（R105 round2：codex P1 #2）──
# 移除 fail-open 后，误判不再是"多写一条 blocker"，而是真的阻断流水线。
# 这类文案在生产里合法（APPROVED: 0 rejected findings），必须排除。

@pytest.mark.parametrize("text", [
    "APPROVED: 0 rejected findings",
    "无驳回项",
    "0 rejected",
    "全部通过，无未通过项",
    "Review: APPROVED, no changes_requested",
    "nothing rejected",
    "未发现驳回项",
])
def test_negated_mentions_are_not_rejections(text):
    assert not is_rejected(text), "否定式提及不得判为驳回: %s" % text


@pytest.mark.parametrize("text", [
    "APPROVED 但 CHANGES_REQUESTED",
    "无 P0，但 review 判定 REJECTED",
    "nothing fixed; CHANGES_REQUESTED: 3 P1",
    # codex round3 实测：这些曾被"否定上下文"整体吞掉
    "P0 rejected",
    "P0 驳回",
    "P0 changes_requested",
    "10 rejected",
    "No P0 findings — CHANGES_REQUESTED",
    "0 rejected findings and CHANGES_REQUESTED",
    "无 P0 驳回",
    # 叙述式："未"修饰的是"达标"，不是驳回词
    "未达标 CHANGES_REQUESTED",
    "审批=不通过/CHANGES_REQUESTED → 需整改",
])
def test_negation_does_not_leak_across_clauses(text):
    """否定只作用于**紧邻的**驳回词：任何独立成立的驳回词都必须判驳回。

    这条是 R105 最贵的一课：前两版用"分句/距离窗口"整体判否定，codex 连续
    证伪出 9 种形态（P0 里的 0 被当计数否定、未达标的"未"、破折号切分…）。
    现方案只认「否定词紧邻驳回词」这一种否认形态，其余一律判驳回。
    """
    assert is_rejected(text), "存在未否定的驳回词时必须判驳回: %s" % text


# ── 显式结论优先（codex round4 P0）──
# 「提及 ≠ 结论」：APPROVED 的评审报告会在正文里引用驳回词作为缺陷描述。
# 只看"全文出现过驳回词"会把通过报告判成驳回——真阻断后，等于让
# "报告里提到 bug"变成"报告本身就是 bug"。

@pytest.mark.parametrize("text", [
    # 生产实测：reviewer 判 APPROVED，正文引用 CHANGES_REQUESTED 作为它发现的缺陷
    "reviewer_pr_review/s1-complete [wf_sub_6ef7274373cf] — 设计评审闸门 "
    "APPROVED: 0 P0 / 0 P1 / 2 P2 / 1 P3（业务）",
    "APPROVED\n\n【系统问题】CHANGES_REQUESTED 无法阻止 s4 启动",
    "APPROVED — reviewer found: prior CHANGES_REQUESTED blocked s4",
    # 生产实测：qa 验收报告首词即通过结论
    "qa_test_execute/s1-complete [wf_sub_62469da4d13a] passed=17 failed=0 total=17 rate=100%",
])
def test_explicit_pass_verdict_wins(text):
    assert not is_rejected(text), "显式通过结论优先于正文里的驳回词提及"


@pytest.mark.parametrize("text", [
    # 2026-10-22：全库 1318 条实测 10 条「通过指标在前、No-Go 在句末」被吞。
    # `PASS`/`通过率` 先命中即返回 True，句末驳回词根本没机会被看到。
    "wf_02192e3126/s4-complete — pass=9/fail=2/total=11 通过率81.8% <90% 验收未达标 No-Go",
    "wf_sub_62469da4d13a/s1 — passed=17 failed=0 rate=100% ... 建议 No-Go",
    "wf_sub_b02ef7bd7f5f/s1 — cleanup三件套 13/18 PASS(72.2%) ... No-Go",
    "wf_dea5cd6891ea/s4 — 29用例 23 passed/6 failed (79.3%) ... No-Go：4×P1 未修",
])
def test_pass_metric_before_reject_verdict_is_rejection(text):
    assert is_rejected(text), "通过指标在前的 No-Go 结论不得被第一个 PASS 命中吞掉"


@pytest.mark.parametrize("text", [
    "审批=不通过/CHANGES_REQUESTED → @product_architect 需整改",
    "reviewer_pr_review/s1-complete [wf_sub_x] — 代码审查 REJECTED(链级): 0 P0 / 1 P1",
    "判定: 未通过",
])
def test_explicit_reject_verdict_wins(text):
    assert is_rejected(text), "显式驳回结论必须判驳回"

def test_conditional_approval_counts_as_pass():
    """条件性批准算通过：`conditional_approved` 不得落成 unknown（unknown 不推进）。

    评审人写「有条件通过」在 R134 fail-closed 之后会卡死链条；PASS 词表因此
    显式收 `conditional[ _-]APPROVED`。
    """
    assert review_verdict("结论: conditional_approved — 2 P2 待补") == APPROVED
    assert review_verdict("APPROVED (conditional): 2 P2") == APPROVED


def test_release_no_go_in_execution_step_is_not_a_rejection_ticket():
    """执行步的发布建议（no-go）不是"本步产出不合格"的驳回票。

    qa_test_execute / devops_deploy_plan 的 prompt_template 明确要求产出带
    「发布建议（go/no-go）」，但 s1/s2 是 type=handoff 的执行步。全库 1001 个
    有文本的步骤实测：4 条真实例因此被判 rejected 并重投到上限
    （wf_sub_f5a5d278becc/s1、wf_sub_a9c847a20d8c/s1、wf_sub_e73bb912c637/s1、
    wf_971532460c9e/s2），产物却都在盘上。
    """
    titles = {
        "qa_test_execute/s1-complete wf_sub_f5a5d278becc 9router SSRF: "
        "17 passed / 2 failed / 1 未跑 (89.5%, 未达 90%); No-Go": APPROVED,
        "qa_test_execute/s1-complete wf_sub_a9c847a20d8c cbf2aa31 测试报告: "
        "18 用例 17 passed / 1 failed (94.4%), No-Go": APPROVED,
        "test_report: qa_test_execute/s1-complete wf_sub_e73bb912c637 —— "
        "三份产出齐备(pass 100% 判定表11/11); 修复'已生效'主张No-Go(daemon未加载)": APPROVED,
        "devops_deploy_plan/s2-complete wf_971532460c9e — 发布策略: NO-GO, "
        "不创建 DEPLOY_SUCCESS(四闸门 0/4)": "unknown",
    }
    for text, want in titles.items():
        assert review_verdict(text) == "rejected", "context-free 判据保持原样: %s" % text
        assert step_verdict(text, "handoff") == want, "执行步不按发布建议判驳回: %s" % text


def test_review_step_keeps_full_word_list():
    """review/gate 步的 no-go 仍是驳回结论，且无类型时行为不变（保守回退）。

    钉住 R41：只剔执行步的发布用语，评审步与未知类型一律走原词表——
    按 type 收窄驳回判定曾是 21/35 条真驳回被吞的原因。
    """
    text = "代码审查 No-Go — 0 P0 / 3 P1"
    assert step_verdict(text, "review") == "rejected"
    assert step_verdict(text, "gate") == "rejected"
    assert step_verdict(text, None) == "rejected"
    assert step_verdict("判定: 未通过", "handoff") == "rejected", "中文驳回词在执行步照样拦"
    assert step_verdict("结论: CHANGES_REQUESTED — 2 P1", "handoff") == "rejected"

def test_reject_word_mentions_with_fix_marker_are_not_this_step_verdict():
    """标题在描述「某驳回已修」时，那个驳回词不是本步结论。

    实测本轮三条同名完成消息（CODE_FIX: … 执行步 no-go 误判驳回已修）被引擎判成
    对自己的驳回票，s1 直接 failed —— 标题既提 no-go 又提 驳回，两个词都在讲
    别人的缺陷已闭环。判据与 task_generator._FIX_MARK 同源（返工记录不算结论）。
    """
    text = ("CODE_FIX: engineer_feature/s1-complete wf_x — 执行步 no-go 误判"
            "驳回已修 (1b12468, 61→41 全库复算)")
    assert review_verdict(text) == "rejected", "context-free 判据保持原样"
    assert step_verdict(text, "single") == "unknown"
    # 未被标记的驳回提及仍然算驳回（不是整段豁免）
    assert step_verdict("qa_test_execute/s3-complete — 驳回 2 项未修", "handoff") == "rejected"
    assert step_verdict("qa_test_execute/s3-complete — 驳回 2 项已修复", "handoff") != "rejected"
    # 评审步不吃这条豁免
    assert step_verdict("代码审查 — 驳回 3 项已修复", "review") == "rejected"
