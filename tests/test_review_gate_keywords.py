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

from review_gate import REJECT_KEYWORDS, is_rejected  # noqa: E402


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
    "审批=不通过/CHANGES_REQUESTED → @product_architect 需整改",
    "reviewer_pr_review/s1-complete [wf_sub_x] — 代码审查 REJECTED(链级): 0 P0 / 1 P1",
    "判定: 未通过",
])
def test_explicit_reject_verdict_wins(text):
    assert is_rejected(text), "显式驳回结论必须判驳回"
