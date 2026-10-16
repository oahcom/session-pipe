"""完整性提醒：步骤完成时 daemon 重复提醒 CCS 两个核心问题。

用户原话：「当工作流步骤完成的时候，daemon需要重复提醒ccs
（你真的解决用户的问题了吗？你真的有严格按用户的提示去完成任务吗）」

设计文档：~/design-doc-step-completion-integrity-check.md v2
审查记录：~/design-review-merge-report.md 与 ~/integrity-implementation-review-final.md

关键约束（来自三方审查）：
- 每维度一条独立消息，不合成一个 prompt（用户明确要求"分开提示词"）
- 复用原步骤 bus_category，不新建 cat=integrity（否则 _check_exit 匹配失败）
- daemon _tick 循环驱动重复发送，非引擎事务阻塞（引擎哲学：不评判内容语义）
"""

import time
import logging
from dataclasses import dataclass

LOGGER = logging.getLogger("workflow.integrity")

# ── 完整性审查维度（用户原话两个核心问题）──
# 用户约束：每维度一条独立消息，不合并
DIMENSIONS = [
    # 用户意图匹配（核心）——用户原话的两个问题
    {"id": "dim01", "category": "意图匹配",
     "prompt": (
         "【完整性审查 1/{total}】你真的解决了用户的问题吗？\n"
         "对比需求原文与当前步骤产出，回答是/否并列出未解决部分。\n\n"
         "需求原文：{goal}\n产出摘要：{output}")},
    {"id": "dim02", "category": "意图匹配",
     "prompt": (
         "【完整性审查 2/{total}】你真的有严格按用户的提示去完成任务吗？\n"
         "对比用户指令与实际执行，回答是/否并列出偏离点。\n\n"
         "用户指令：{goal}\n实际执行：{output}")},
]

# DIMENSIONS 前两条即用户核心问题的标准文本；bypass 复用其问题主干（P1-1 统一）
# 提取问题句（去掉前缀标记，如【完整性审查 1/2】）供 bypass 复用
def _question_text(dim: dict) -> str:
    """从维度 prompt 提取核心问题句（去除编号前缀与模板变量段）。"""
    _t = dim["prompt"]
    # 去掉【...】前缀和 {xxx} 变量段
    import re as _re
    _t = _re.sub(r"^【[^】]*】", "", _t).strip()
    _t = _re.sub(r"\{[a-z_]+\}", "", _t).strip()
    return _t.split("\n")[0].strip()


# 两个核心问题（bypass 复用，与 DIMENSIONS 前两条同源）
CORE_QUESTIONS = [_question_text(DIMENSIONS[0]), _question_text(DIMENSIONS[1])]

# ── 默认配置 ──
DEFAULT_INTERVAL_SEC = 30
DEFAULT_MAX_RETRIES = 3


# ── 状态数据类（持久化在 step_results[step_id] 的 integrity_* 字段中）──

@dataclass
class IntegrityState:
    pending: bool = False
    dim_index: int = 0
    last_sent_at: float = 0.0
    interval_sec: float = DEFAULT_INTERVAL_SEC
    complete: bool = False
    retry_count: int = 0
    max_retries: int = DEFAULT_MAX_RETRIES
    reminder_sent_bypass: bool = False


# ── 持久化 ──

def load_state(step_results: dict, step_id: str) -> IntegrityState:
    sr = step_results.get(step_id, {})
    return IntegrityState(
        pending=sr.get("integrity_pending", False),
        dim_index=sr.get("integrity_dim_index", 0),
        last_sent_at=sr.get("integrity_last_sent_at", 0.0),
        interval_sec=sr.get("integrity_interval_sec", DEFAULT_INTERVAL_SEC),
        complete=sr.get("integrity_complete", False),
        retry_count=sr.get("integrity_retry_count", 0),
        max_retries=sr.get("integrity_max_retries", DEFAULT_MAX_RETRIES),
        reminder_sent_bypass=sr.get("integrity_reminder_sent_bypass", False),
    )


def save_state(step_results: dict, step_id: str, state: IntegrityState) -> None:
    sr = step_results.setdefault(step_id, {})
    sr["integrity_pending"] = state.pending
    sr["integrity_dim_index"] = state.dim_index
    sr["integrity_last_sent_at"] = state.last_sent_at
    sr["integrity_interval_sec"] = state.interval_sec
    sr["integrity_complete"] = state.complete
    sr["integrity_retry_count"] = state.retry_count
    sr["integrity_max_retries"] = state.max_retries
    sr["integrity_reminder_sent_bypass"] = state.reminder_sent_bypass


# ── 激活 ──

def activate(step_results: dict, step_id: str,
             interval_sec: float = DEFAULT_INTERVAL_SEC) -> None:
    state = load_state(step_results, step_id)
    if state.pending or state.complete:
        return  # 幂等：已激活不重复
    state.pending = True
    state.dim_index = 0
    state.last_sent_at = 0.0  # 立即发第一条
    state.interval_sec = interval_sec
    save_state(step_results, step_id, state)
    LOGGER.info("integrity: activate %s, interval=%ds", step_id, interval_sec)


# ── 轮询判断 ──

def should_send_now(state: IntegrityState, now: float = None) -> bool:
    if not state.pending or state.complete:
        return False
    now = now if now is not None else time.time()
    return (now - state.last_sent_at) >= state.interval_sec


def get_next_dimension(state: IntegrityState) -> dict | None:
    if state.dim_index >= len(DIMENSIONS):
        return None
    return DIMENSIONS[state.dim_index]


# ── 推进 ──

def advance_index(step_results: dict, step_id: str, now: float = None) -> None:
    state = load_state(step_results, step_id)
    state.dim_index += 1
    state.last_sent_at = now if now is not None else time.time()
    if state.dim_index >= len(DIMENSIONS):
        state.complete = True
        LOGGER.info("integrity: %s all %d dimensions sent, awaiting reply", step_id, len(DIMENSIONS))
    save_state(step_results, step_id, state)


# ── 回复/重试 ──

def on_reply_received(step_results: dict, step_id: str) -> None:
    state = load_state(step_results, step_id)
    state.pending = False
    state.complete = False
    state.reminder_sent_bypass = False  # 重置 bypass flag（rollback 后二次执行允许重新提醒）
    save_state(step_results, step_id, state)
    LOGGER.info("integrity: %s reply received, clearing pending", step_id)


def on_retry_or_timeout(step_results: dict, step_id: str) -> bool:
    state = load_state(step_results, step_id)
    if state.retry_count < state.max_retries:
        state.retry_count += 1
        state.complete = False
        state.dim_index = 0
        state.last_sent_at = 0.0
        state.reminder_sent_bypass = False  # 重试=新轮次，允许重新提醒
        save_state(step_results, step_id, state)
        LOGGER.info("integrity: %s retry %d/%d, restarting from dim01",
                    step_id, state.retry_count, state.max_retries)
        return True
    # 超过上限 → fail-open，不阻塞工作流
    state.pending = False
    state.complete = False
    save_state(step_results, step_id, state)
    LOGGER.warning("integrity: %s max retries exceeded, fail-open", step_id)
    return False


# ── bypass 路径统一辅助函数 ──
# 用户约束：每个步骤分开提示词（两个问题各一条消息）
def send_bypass_reminder(run, step, path: str, send_to_role: callable,
                         sync_step_results: callable = None) -> None:
    """4条 bypass 路径（tick rescue/busy/relaxed/completion_check）统一补发提醒。
    两个核心问题分开发送（用户约束：分开提示词），发完标记 bypass flag。
    任一环节失败静默不阻塞完成（bypass 是旁路通知）。"""
    step_results = run.step_results
    # 幂等 + 节流：先落库再发送。
    # 旧实现只写内存 flag，且在发送后才置位；而 11 处调用点各自持有 run 副本、
    # complete_step 又在提醒之后才写库 → 下一 tick 读库看不到 flag，同一秒可重复
    # 注入（实测 s1 被提醒 62 次 / s2 30 次，仅 4 个唯一 step）。
    import time as _t
    _srec = step_results.setdefault(step.id, {})
    _last = float(_srec.get("integrity_reminder_sent_bypass_at", 0.0))
    if _srec.get("integrity_reminder_sent_bypass") or (_t.time() - _last) < 60:
        return  # 已发过（或 60s 节流窗内）
    _srec["integrity_reminder_sent_bypass"] = True
    _srec["integrity_reminder_sent_bypass_at"] = _t.time()
    if sync_step_results:
        sync_step_results(run.id, run.step_results)
    # 剥离 /goal 前缀：模板存的 prompt_template 自带该前缀，直接截断填进
    # 「用户指令：」会让角色收到字面的 /goal（bus #374984 实证）。
    # 剥完为空时退回 step.title —— 至少是有语义的步骤名，而非空占位。
    #
    # 变量填充：prompt_template 是**模板原文**（含 {description}/{title}/{assignee} 等
    # 占位符）。直接把原文塞进提醒 → 角色收到"用户指令：## 诊断结果 {description}"，
    # 拿未填充的占位符无法回答"是否解决了用户的问题"（实测 2026-10-17：product_architect
    # /maintainer 的提醒里 {description}/{assignee} 原样出现，且该未替换文本被
    # _UNMATCHED_VAR_RE 拦下 → 提醒投递失败 → 用户要求的"重复提醒"机制整体失效）。
    # 用 run.context 做最小填充——与 _fill_prompt_vars 同源，但不依赖 engine 实例。
    goal_text = _re.sub(r"^/goal(?:\s+|$)", "",
                        getattr(step, 'prompt_template', '') or '',
                        count=1, flags=_re.IGNORECASE).strip()
    _ctx = getattr(run, 'context', None) or {}
    for _var, _val in (
        ("{description}", _ctx.get("description", "")),
        ("{title}", _ctx.get("title", "")),
        ("{assignee}", getattr(step, 'target_role', '') or ''),
        ("{topic}", _ctx.get("title", "")),
    ):
        if _var in goal_text and _val:
            goal_text = goal_text.replace(_var, str(_val))
    if not goal_text:
        goal_text = getattr(step, 'title', '') or ''
    # 复用 CORE_QUESTIONS（与 DIMENSIONS 前两条同源，P1-1 统一）
    _questions = [
        f"步骤 {step.id} 即将完成（{path}），请回答：\n{CORE_QUESTIONS[0]}（简答）\n需求：{goal_text[:300]}",
        f"{CORE_QUESTIONS[1]}（简答）\n用户指令：{goal_text[:300]}",
    ]
    for i, q in enumerate(_questions, 1):
        try:
            send_to_role(
                step.target_role,
                f"/goal\n## 完整性提醒（快速确认 {i}/{len(_questions)}）\n{q}",
                wf_id=run.id, step_id=step.id, force=True
            )
        except Exception:
            LOGGER.debug("integrity: bypass %s send %d/%d failed for %s",
                         path, i, len(_questions), step.id)
            continue  # 单条失败不阻塞后续，继续发下一条
    LOGGER.info("integrity: bypass %s reminder sent for %s", path, step.id)


# ── 格式化 ──

import re as _re  # 模块级导入（工程 P2：避免每调用重复 import）

def format_dim_prompt(dim: dict, step_goal: str = "", output_summary: str = "",
                      **extra: str) -> str:
    """填充维度提示词变量。加提示词时在 DIMENSIONS 的 prompt 里用 {goal}/{output}/{total}/{xxx} 占位符，
    extra 传入额外键值对即可。缺少的占位符填空串（不 KeyError）。"""
    # 先收集 prompt 中所有占位符，缺键的补空串，避免 format_map 缺键 KeyError
    _placeholders = set(_re.findall(r"\{(\w+)\}", dim["prompt"]))
    defaults = {"goal": step_goal, "output": output_summary, "total": str(len(DIMENSIONS)), **extra}
    for _p in _placeholders:
        defaults.setdefault(_p, "")
    return dim["prompt"].format_map(defaults)


def make_full_prompt(run: object, step: object, step_results: dict) -> str:
    """从 run/step 构造用户需求原文（context.title + step.prompt_template）。"""
    _title = ""
    _goal = ""
    if hasattr(run, "context") and isinstance(run.context, dict):
        _title = run.context.get("title", "")
    if hasattr(step, "prompt_template"):
        # 剥离模板自带的 /goal 前缀：它是指令语法，不是需求原文。
        # 原样输出会把字面的 /goal 混进「步骤目标：」喂给角色。
        _goal = _re.sub(r"^/goal(?:\s+|$)", "", step.prompt_template or "",
                        count=1, flags=_re.IGNORECASE).strip()
    parts = []
    if _title:
        parts.append(f"工作流目标：{_title}")
    if _goal:
        parts.append(f"步骤目标：{_goal}")
    return "\n".join(parts) if parts else "（需求原文不可用）"
