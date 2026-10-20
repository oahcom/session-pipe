"""评审驳回判据的唯一真相源（workflow / lifecycle 两侧共用）。

为什么需要单独一个模块：同一份词表此前在 wf_lifecycle._advance_production_wf、
lifecycle/manager._advance_unsafe、pipeflow/task_generator._subflow_verdict_rejected
三处各写一份，靠注释约定「三方一致」。任何一处单独调整都会让同一条文案在一条
推进路径上判驳回、在另一条上判通过——实测 wf_98a18e738d 正是如此（角色调
complete_step 走 manager，引擎 tick 走 wf_lifecycle，前者拦后者放）。

词表取舍（2026-10-20 全库 900 实例实测）：
- 收 `changes_requested` / `rejected` / `not_approved` / `驳回` / `未通过`。
- **不收 `需修复`**：`无需修复` 的子串。maintainer 健康巡检固定写「无异常需修复」，
  全库 18 例命中全为误报。
- **不收 `回退`**：生产里作动词常态出现（「回退重投第4次」「代码回退 time.time()」）。

## 判定模型：锚点结论 → 全文回退（五轮 codex 复审收敛而来）

核心难点是**「提及」≠「结论」**。角色产出里既有
  `APPROVED: 0 P0`（结论）
也有
  `上一轮 CHANGES_REQUESTED 已修复`（引用）
两者都含驳回词，语义相反。

前四版试图用"否定窗口/分句作用域/首个 verdict 优先"统一处理，被 codex 连续
证伪出 12 种形态（P0 里的 0 被当计数否定、未达标、"APPROVED: 0 rejected
findings; CHANGES_REQUESTED: 3 P1" 的后者被吞、引用语境…）。

最终方案分两级，规则可以逐条列举：

**第一级：锚点结论**。结论词必须出现在锚点位置才被采信：
  - 行首（`^`）
  - `判定:` / `审批=` / `结论:` 之后
  - `—` / `--` 之后（生产标题的固定格式 `xxx/s1-complete — 代码审查 XXX`）
锚点后的第一个结论词即本步结论；它被否定修饰（`NOT APPROVED`）则语义反转。

**第二级：全文回退**。没有任何锚点结论时，全文扫描；但**引用语境**里的命中
不计数——它前面紧邻 `上一轮/前次/原/引用/历史/此前` 等词，说明是在复述旧结论。

这个形状的价值：锚点是**结构化**的（位置），不是靠模式猜语义；引用语境的
词表是封闭的（实测生产里只有这几种写法）。
"""
from __future__ import annotations

import re

# 判定「该步骤产出表达的是驳回结论」的关键词。
# 改动本表即同时改动 wf_lifecycle / lifecycle.manager / task_generator 三处判据。
REJECT_KEYWORDS = ("changes_requested", "rejected", "not_approved", "驳回", "未通过")

# ── 第一级：锚点结论 ──

# 锚点：行首、判定/审批/结论标记之后、破折号之后。
# 破折号是生产标题的固定分隔（`[wf_sub_x] — 代码审查 REJECTED`）。
_ANCHOR = re.compile(
    r"(?:^|\n|判定\s*[:：]|审批\s*[=:：]|结论\s*[:：]|—|–|\s--\s)", re.M)

# 结论词。PASS 系列排除左侧否定（NOT APPROVED / 未通过 不是 PASS）。
_PASS_WORD = re.compile(r"\bAPPROVED\b|\bPASS(?:ED)?\b|通过|合格|就绪", re.I)
# `不通过` 必须显式列出：它含"通过"二字，若不先匹配整个词，_PASS_WORD 会把
# 其中的"通过"当成通过结论（`审批=不通过/CHANGES_REQUESTED` 实测踩到）。
_REJECT_WORD = re.compile(
    r"\bREJECTED\b|CHANGES_REQUESTED|\bnot_approved\b|驳回|未通过|不通过", re.I)
# 结论词左侧的否定修饰（`NOT APPROVED` / `未通过` 里的"未"）。
_NEG_BEFORE = re.compile(
    r"(?:(?:^|[\s(（])(?:not|never|no)|未|不|非)\s*$", re.I)

# ── 第二级：全文回退 ──

# 引用语境：命中处左侧紧邻这些词 → 是在复述旧结论，不构成本步判断。
_QUOTE_CONTEXT = re.compile(
    r"(?:上一轮|前次|此前|之前|原|历史|引用|原文|旧|prior|previous|earlier|"
    r"无法阻止|不能阻止|曾|已被)\s*(?:的|的结论|判定|判)?\s*$", re.I)

# 转折连词：`APPROVED 但 CHANGES_REQUESTED` 的结论在转折之后。
_CONTRAST = re.compile(r"(?<!不)但|不过|然而|\bhowever\b|\bbut\b|\byet\b", re.I)

# 紧邻否定（`无驳回项` / `0 rejected` / `not rejected`）。
_NEG_CN = re.compile(
    r"(?:(?<![A-Za-z0-9])(?:0|零)|无|没有|未见|未发现|未有任何|不存在)"
    r"\s*(?:任何|相关|的|个|项|条)?\s*"
    r"(?:驳回|未通过|rejected|changes_requested)", re.I)
_NEG_EN = re.compile(
    r"(?:\bno\b|\bnone\b|\bzero\b|\bnothing\b|\bwithout\b|\bnever\b)"
    r"\s+(?:\w+\s+)?"
    r"(?:rejected|changes_requested|not_approved)", re.I)


def _anchored_verdict(text: str) -> bool | None:
    """锚点处的结论 → True=通过 / False=驳回 / None=锚点无有效结论。

    锚点后逐个检查结论词（不只是第一个）：
    - 被紧邻否定的驳回词跳过（`无驳回项` 不是结论）
    - 处于引用语境的跳过（`上一轮 CHANGES_REQUESTED` 是复述）
    这样 `APPROVED: 0 rejected findings; CHANGES_REQUESTED: 3 P1` 里
    第一个 APPROVED 被 `_PASS_WORD` 命中但同锚点内还有未否定的
    CHANGES_REQUESTED——后者才是结论。
    """
    for m in _ANCHOR.finditer(text):
        # 锚点的作用域到**本行结束**：`APPROVED\n\n【系统问题】CHANGES_REQUESTED…`
        # 的正文是下一行的论据，不是同一结论的一部分。不限行会把它们并进
        # 首个锚点，让正文提及覆盖真实结论（codex round5 的 P0 形态）。
        _tail = text[m.end():m.end() + 160]
        _nl = _tail.find("\n")
        rest = _tail if _nl < 0 else _tail[:_nl]
        # 先定位驳回词区间，PASS 词若落在其中就是它的一部分
        # （「不通过」里的"通过"、「CHANGES_REQUESTED(设计闸门不通过)」里的后者）。
        rej_spans = [(mm.start(), mm.end()) for mm in _REJECT_WORD.finditer(rest)]
        hits = []
        for mm in _PASS_WORD.finditer(rest):
            if any(s <= mm.start() < e for s, e in rej_spans):
                continue                              # 是驳回词的组成，不是通过
            if _NEG_BEFORE.search(rest[:mm.start()]):
                hits.append((mm.start(), False))      # NOT APPROVED → 驳回
            else:
                hits.append((mm.start(), True))
        for mm in _REJECT_WORD.finditer(rest):
            left = rest[max(0, mm.start() - 20):mm.start()]
            if _NEG_CN.search(left + mm.group(0)) or _NEG_EN.search(left + mm.group(0)):
                continue
            if _QUOTE_CONTEXT.search(left):
                continue
            hits.append((mm.start(), False))
        if not hits:
            continue
        hits.sort()
        # 锚点内取**最先**的结论词：结论是锚点后第一个谓词
        # （`— CHANGES_REQUESTED(设计闸门不通过)` 的结论是 CHANGES_REQUESTED，
        #  括号里的"不通过"是它的释文，不是第二个结论）。
        # 例外：`APPROVED: 0 rejected findings; CHANGES_REQUESTED: 3 P1`
        # 这类"先通过后驳回"的复合句，分号后才是最终结论 → 取最后。
        # 转折词同理：`APPROVED 但 CHANGES_REQUESTED` 的结论在"但"之后。
        contrast = _CONTRAST.search(rest)
        if ((contrast and hits[0][0] < contrast.start() < hits[-1][0])
                or (any(s in rest for s in (";", "；")) and hits[0][1] != hits[-1][1])):
            return hits[-1][1]
        return hits[0][1]
    return None


def _full_scan(text: str) -> bool:
    """全文回退：任一未被「紧邻否定」且未被「引用语境」标记的命中 → 驳回。"""
    low = text.lower()
    for kw in REJECT_KEYWORDS:
        q = 0
        while True:
            i = low.find(kw, q)
            if i < 0:
                break
            q = i + len(kw)
            left = low[max(0, i - 20):i]
            if _NEG_CN.search(left + kw) or _NEG_EN.search(left + kw):
                continue
            if _QUOTE_CONTEXT.search(left):
                continue
            return True
    return False


def is_rejected(text: str) -> bool:
    """文本是否表达了驳回结论。锚点结论优先，无锚点则全文回退。"""
    text = text or ""
    verdict = _anchored_verdict(text)
    if verdict is not None:
        return not verdict
    return _full_scan(text)
