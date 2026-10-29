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
例外：分号或转折词（`但`/`however`）之后的相反结论才是最终结论。

**第二级：全文回退**。没有任何锚点结论时，全文扫描；但**引用语境**里的命中
不计数——它前面紧邻 `上一轮/前次/原/引用/历史/此前` 等词，说明是在复述旧结论。

这个形状的价值：锚点是**结构化**的（位置），不是靠模式猜语义；引用语境的
词表是封闭的（实测生产里只有这几种写法）。
"""
from __future__ import annotations

import re

# 判定「该步骤产出表达的是驳回结论」的关键词。
# 改动本表即同时改动 wf_lifecycle / lifecycle.manager / task_generator 三处判据。
REJECT_KEYWORDS = ("changes_requested", "rejected", "not_approved", "驳回", "未通过",
                   "no-go", "nogo")

# 「必须出结论」的角色（unknown 才拦得住）。7 天生产库实测，review/gate 类型步骤里
# unknown+empty 共 41 条，按角色分：maintainer 24（健康检查）、product_architect 9
# （影响评估/Triage）、reviewer 4（真的是代码审查没写结论）、其余 4。按 type 拦
# 会把 maintainer 的健康检查循环整条冻住 —— 那些步骤的产出是报告，永远不会有
# APPROVED/CHANGES_REQUESTED。判据因此按角色收窄：只有审查角色才有"必须表态"。
REVIEW_ROLES = ("reviewer",)

# 角色自述「拒绝产出/无法对齐」的标题不是产出（R163：wf_bc4112d2767f 的 s3/s4 以
# 拒产/无法对齐标题通过契约 text_contains 被判完成）。只扫**标题**不扫正文：正文命中
# 多为元讨论（"宽松推进把 blocker 当产出"）。不含裸"零产出"——维护/审计角色会在合法
# 完成的标题里报告别人的零产出（436491 maintainer_health_monitor/s2-complete）。
# 末尾否定前瞻：拒绝词后 12 字内出现「已修复/已通过…」说明那是问题描述而非本轮结论
# （"零输入测试通过"、"拒绝审批意见已修复"），不算拒产。
# 实测：全库 10006 条 fact 标题命中 7 条，其中命中步骤契约的 2 条都是真拒绝。
# 定在这里而不是 step_handler：wf_lifecycle 的 stale rescue 也要用，互相 import 成环。
REFUSAL_T = re.compile(
    r"(?:拒产|拒审|拒绝(?:产出|审|交付|设计|对齐)|无法(?:对齐|产出|交付|开始)|零文档|零输入|输入为零)"
    r"(?!.{0,12}(?:已修复|已解决|已通过|已验证|测试通过))")


def is_refusal_not_verdict(text) -> bool:
    """自述拒绝，且拒产词**先于**结论词才算拒产。

    审查角色的合法交付物就是驳回结论，标题天然带拒产词（实测形态
    `s3-complete 代码审查 CHANGES_REQUESTED — 接口无法对齐 3 处`）：结论词在前 →
    这是驳回，照拒产丢掉就吞掉真实驳回、返工链建不出来。位置定语义，与 _anchored_verdict
    里"结论是锚点后第一个谓词"同一条规则。结论词表复用下面 _REJECT_WORD/_PASS_WORD，
    不另立第二份（第二份必然漏词：REJECTED/不通过/No-Go 都曾被漏掉）。
    """
    t = text if isinstance(text, str) else str(text or "")
    m = REFUSAL_T.search(t)
    if not m:
        return False
    _v = [x.start() for x in (_REJECT_WORD.search(t), _PASS_WORD.search(t)) if x]
    return not _v or m.start() < min(_v)


def refusal_hit(text) -> bool:
    """标题是否自述拒绝。非 str 先转字符串：SQLite 动态类型可能给出 int/bytes，
    直接喂给 search 会抛 TypeError（codex R163 第三轮指出）。"""
    return bool(REFUSAL_T.search(text if isinstance(text, str) else str(text or "")))


def is_review_role(role: str) -> bool:
    return (role or "").strip().lower() in REVIEW_ROLES

# ── 第一级：锚点结论 ──

# 锚点：行首、判定/审批/结论标记之后、破折号之后。
# 破折号是生产标题的固定分隔（`[wf_sub_x] — 代码审查 REJECTED`）。
_ANCHOR = re.compile(
    r"(?:^|\n|判定\s*[:：]|审批\s*[=:：]|结论\s*[:：]|—|–|\s--\s)", re.M)

# 结论词。PASS 系列排除左侧否定（NOT APPROVED / 未通过 不是 PASS）。
_PASS_WORD = re.compile(
    # 通过(?!率)：`通过率81.8% < 90% 验收未达标 No-Go` 曾被判 APPROVED —— 指标名词不是结论。
    r"\b(?:conditional[ _-])?APPROVED\b|\bPASS(?:ED)?\b|通过(?!率)|合格|就绪", re.I)
# `不通过` 必须显式列出：它含"通过"二字，若不先匹配整个词，_PASS_WORD 会把
# 其中的"通过"当成通过结论（`审批=不通过/CHANGES_REQUESTED` 实测踩到）。
_REJECT_WORD = re.compile(
    # 边界用 (?<![A-Za-z]) 而不是 \b：汉字也算 word char，`建议No-Go` 用 \b
    # 会判它前面"没有边界"而漏掉（实测 wf_670c2a67d001/s1 因此仍判通过）。
    r"\bREJECTED\b|CHANGES_REQUESTED|\bnot_approved\b|驳回|未通过|不通过|"
    r"(?<![A-Za-z])no[ _-]?go(?![A-Za-z])", re.I)

# 「通过指标在前、硬判决词在后」才翻转结论用。中文 `驳回`/`不通过` 不收：它们
# 在「驳回项全部闭环」「修复 2 处不通过项」里是名词宾语，不是判决。
_HARD_VERDICT = re.compile(
    r"\bREJECTED\b|CHANGES_REQUESTED|\bnot_approved\b|(?<![A-Za-z])no[ _-]?go(?![A-Za-z])", re.I)
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
                hits.append((mm.start(), False, True))   # NOT APPROVED → 驳回
            else:
                hits.append((mm.start(), True, False))
        for mm in _REJECT_WORD.finditer(rest):
            left = rest[max(0, mm.start() - 20):mm.start()]
            if _NEG_CN.search(left + mm.group(0)) or _NEG_EN.search(left + mm.group(0)):
                continue
            if _QUOTE_CONTEXT.search(left):
                continue
            # 第三元素：该命中是不是「硬判决词」（判句而非名词宾语）
            hits.append((mm.start(), False, bool(_HARD_VERDICT.fullmatch(mm.group(0)))))
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
        if hits[0][1] and any(h[2] for h in hits[1:]):
            # 先通过、后跟**显式判决词**：`pass=9/fail=2 通过率81.8% <90% 未达标
            # No-Go` 的结论在句末，第一个 PASS 只是指标。限定为硬判决词——中文
            # `驳回`/`不通过` 在「驳回项全部闭环」「修复 2 处不通过项」里是名词
            # 宾语，翻转会把已通过的步骤判成驳回（实测 2 例）。
            return False
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


APPROVED, REJECTED, UNKNOWN = "approved", "rejected", "unknown"


def review_verdict(text: str) -> str:
    """三态判定：approved / rejected / unknown（空文本或无锚点无关键词）。

    消费方必须区分 unknown：评审人什么都没写不是"通过"。GitHub required
    review / Temporal 都是 fail-closed——没有明确 APPROVED 就不放行。
    """
    text = text or ""
    verdict = _anchored_verdict(text)
    if verdict is not None:
        return APPROVED if verdict else REJECTED
    return REJECTED if _full_scan(text) else UNKNOWN


# 发布用语：`go / no-go` 是模板要求 qa、devops 在**产出里**写的发布建议
# （qa_test_execute 的 prompt_template：「通过率≥90%，发布建议（go/no-go）」），
# 不是"本步产出是否合格"的判断。混进驳回词表后，执行步的完成标题「… 17 passed /
# 2 failed … No-Go」被引擎当成本步的驳回票：wf_sub_f5a5d278becc/s1 重投到
# _MAX_REJECTED_RETRY(3) 后升级、2h 后被 close_wf(failed) 收口，而产物在盘上。
_RELEASE_NOGO = re.compile(r"(?<![A-Za-z])no[ _-]?go(?![A-Za-z])", re.I)

# 驳回词后紧跟「已修/已处理」= 在描述**别人**的返工已闭环，不是本步结论
# （task_generator._FIX_MARK 同款判据，这里补 `已修`：生产标题写「误判驳回已修」）。
_FIX_MENTION = re.compile(r"^[^\s，。;；]{0,6}已(?:修复|修|处理|合并|关闭|解决)")


def _strip_mentions(text: str) -> str:
    out, last = [], 0
    for m in _REJECT_WORD.finditer(text or ""):
        gap = re.sub(r"\s+", "", text[m.end():m.end() + 20])
        if _FIX_MENTION.match(gap):
            out.append(text[last:m.start()])
            last = m.end()
    out.append((text or "")[last:])
    return "".join(out)


def step_verdict(text: str, step_type: str | None = None) -> str:
    """带步骤类型的结论判定：review/gate 步保持全词表，其余步只看本步结论。

    与 review_verdict 分开而不是改它：后者是 context-free 的文本判据，被
    task_generator 的子链快照判据复用，语义不该随步骤身份漂移。

    执行步（handoff/single/notify）多两处削词：
    1. 发布用语 no-go —— 模板 prompt_template 要求 qa/devops 在产出里写
       「发布建议（go/no-go）」，它是对产物的评估，不是对本步的驳回。
    2. 驳回词紧跟「已修/已处理」的提及 —— 标题在描述别人的环已闭环。
    全库 1005 个带文本步骤实测：rejected 64→41，23 条翻转（20 handoff 全是
    no-go、3 single 全是「误判驳回已修」式自述），review/gate 0 变化，
    非 review/gate 的其余 41 条驳回照旧判驳回（R41 案例保留）。
    """
    if step_type in (None, "review", "gate"):
        return review_verdict(text)
    return review_verdict(_strip_mentions(_RELEASE_NOGO.sub(" ", text or "")))


# 二态判据的公开名字：task_generator 只问"这条消息算不算驳回"，用 bool|None 比三态
# 更直接（`verdict(x) is False`）。合并时这名字被 review_verdict 取代，调用点没跟改 →
# AttributeError（test_subflow_verdict_freshness 11 红）。别名保持两条入口语义一致。
verdict = _anchored_verdict


def is_rejected(text: str) -> bool:
    """存量判据：只问"是否驳回"，unknown 仍返回 False（fail-open）。"""
    return review_verdict(text) == REJECTED
