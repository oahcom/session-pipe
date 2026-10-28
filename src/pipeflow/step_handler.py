#!/usr/bin/env python3
"""StepHandlerMixin — 步骤级 tick 逻辑 + exit 条件匹配 + schema 校验。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import os
import re
import time
import subprocess
import shutil
import tempfile
import logging
import pathlib
from pathlib import Path

# 只有 is_refusal_not_verdict（不是 refusal_hit）：审查角色的合法驳回标题天然带
# 「无法对齐」这类拒产词，有结论词（CHANGES_REQUESTED/驳回…）时以结论为准，
# 否则真实驳回被当成拒产丢掉、返工链建不出来（codex 第四轮 P1）。
from review_gate import REFUSAL_T, is_refusal_not_verdict as _refusal_not_verdict
from pipeflow import engine as _engine_mod  # 属性访问 _engine_mod._sp，patch("pipeflow.engine._sp") 生效
from pipeflow.task_generator import _event_key  # DESIGN §2.1 事件去重键（fan-out 第三层）
from pipeflow.engine import Step, WorkflowRun, _fail_parent_step
from pipeflow.wf_lifecycle import _value_audit_failed, _role_producing_recently
from paths import CCS_WORKSPACES as _CCS_WORKSPACES
from conversation_monitor import get_conversation_signal

# codex 三方审查超时（秒）。实测标定：21 文件/734 行 diff 需 6m19s（2026-10-17），
# 故不能用 300s——该规模下必然超时，闸门回落人工审查。
import os as _os
_CODEX_REVIEW_TIMEOUT = int(_os.environ.get("CODEX_REVIEW_TIMEOUT", "900"))

# text_contains 必须保持子串语义，不要加词边界/lookaround。两条实证（2026-10-19）：
# (1) dual_node 宽匹配用 re.sub(r"-complete$", "-") 生成 filter（下方 _wide_filter），
#     "s1-" 靠后面的 "c" 属于 \w 才匹配成功；加 (?!\w) 后 node2 消息恒不命中 →
#     food_chain_loop/WL-food_chain_loop 的 dual_node 步骤永久卡死。
# (2) 词边界并不拦跳步：bus #411347「security_audit_scan/s2-complete s3-complete — …」
#     两个 filter 在词边界语义下依然同时命中（该消息确实含两个标记）。防跳步靠
#     _check_exit 的 created_after 屏障（生产 272 实例满足、0 违反），不靠匹配算法。
from pipeflow.integrity import (
    DIMENSIONS as _integ_dimensions,
    load_state as _integ_load,
    activate as _integ_activate, should_send_now as _integ_should_send,
    get_next_dimension as _integ_next_dim, advance_index as _integ_advance,
    on_reply_received as _integ_reply, on_retry_or_timeout as _integ_retry,
    format_dim_prompt as _integ_format, make_full_prompt as _integ_full_prompt,
    send_bypass_reminder as _integ_send_bypass,
)

LOGGER = logging.getLogger("workflow.engine")

# 排队降级阈值：排队分支原本永久 return（不递增 timeout_count），但 busy 判据
# 自带 `timeout_count < 3` 前提（liveness.py:211），同角色两个卡死实例互判 busy →
# 双方 timeout_count 恒 0 → 永远 busy。此阈值把"被真占用"与"整链卡死"分开。
_QUEUE_DEGRADE_AT = 3

# 排队预算下限（秒）。角色正常执行一步就要几分钟，低于此值会把合法排队误杀
# （2026-10-19 实测 queued_since→cancel 仅 97~121s）。上限由步声明推导，见下。
_QUEUE_BUDGET_FLOOR = 1800


# 闸门连续未通过多少次后升级给模板所有者（10s/tick ≈ 5 分钟）。
# 只升级一次：判据不修时每 tick 再喊一遍不带来新信息。
_GATE_ESCALATE_AT = 30

# dual_node 第二节点（codex 审查通过）的完成证据标记。manager.complete_step
# 只认 step_results[step_id]["node2_approved"]：node1_done 的语义是"第一节点收到"，
# node2_sent 是引擎注入指令时与 node1_done 同毫秒写入的（30d 实测 10/10 相同），
# 两者都不证明审过。30d 内 10 个到达 node1 的 dual_node 步骤有 9 个无 approved 事实
# 却被判完成 —— 闸门过去是纯标签，判据只能落在真正到达的审查结论上。
_APPROVED_MARK = "-codex-approved"

# 负向终态：codex 跑完但**未达零问题**（ADR-2026-10-28-dual-node-terminal 决策 B）。
# 没有这个标记时，"审了但不通过" 与 "没审" 无法区分，步骤唯一出口是超时回收：
# 实测 5 个实例 × timeout_minutes=45 × timeout_count=5 等死，其中
# wf_a682df8dc6b5 的 scout 7 轮 codex 里 5 轮 429、诚实上报未达成（bus #463402）。
# 用超时表达"诚实上报"会训练角色谎报通过，所以必须有显式负向标记。
_BLOCKED_MARK = "-codex-blocked"


def _codex_review_prompt(wf_name: str, step_id: str, cat: str) -> str:
    """注入给角色的 codex 审查指令（两个完成站点共用，避免文案漂移）。"""
    return (
        "/goal\n## codex CLI 三方审查\n"
        f"对 {wf_name}/{step_id} 的产出执行 codex review --uncommitted：\n"
        "1. 执行 codex review --uncommitted -c model=9router_hermes\n"
        "2. 若发现问题修复后重跑，直到连续两次零问题\n"
        "3. 通过后写 bus cat=" + cat + "，标题含 " + step_id + _APPROVED_MARK + "\n"
        "## 完成\n"
        "写 bus cat=" + cat + "，标题含 " + step_id + _APPROVED_MARK + "。"
    )


def _has_codex_approved(matched_msgs: list, step_id: str) -> bool:
    """匹配到的消息里是否真有本步的 codex-approved 结论。

    dual_node 的宽过滤 s{N}- 会同时命中 s{N}-complete（第一节点消息本身），
    只看"匹配到了"会把第一节点消息误当第二节点。
    """
    _needle = step_id + _APPROVED_MARK
    return any(isinstance(m, dict) and _needle in str(m.get("text", ""))
               for m in (matched_msgs or []))

def _has_codex_blocked(matched_msgs: list, step_id: str) -> bool:
    """匹配到的消息里是否真有本步的 codex-blocked 结论。

    与 _has_codex_approved 同一口径（宽过滤 s{N}- 会把两族标记都带进来）。
    """
    _needle = step_id + _BLOCKED_MARK
    return any(isinstance(m, dict) and _needle in str(m.get("text", ""))
               for m in (matched_msgs or []))


def _codex_verdict(matched_msgs: list, step_id: str):
    """本步的 codex 终态：'approved' / 'blocked' / None。**同一批里先到者定局**。

    两个标记出现在不同消息里，但宽过滤 s{N}- 会一次带回一批：如果按 if 顺序
    判，谁写在前面谁赢，就成了隐式优先级（"approve 永远压过 blocked"）。按 ts
    取最早的那条，两族才是真互斥。
    """
    _best, _best_ts = None, None
    for _m in (matched_msgs or []):
        if not isinstance(_m, dict):
            continue
        _t = str(_m.get("text", ""))
        for _mark, _verdict in ((_APPROVED_MARK, "approved"), (_BLOCKED_MARK, "blocked")):
            if step_id + _mark in _t:
                _ts = _m.get("ts") or 0.0
                if _best_ts is None or _ts < _best_ts:
                    _best, _best_ts = _verdict, _ts
                break
    return _best


def _queue_budget_sec(ec: dict, attempts: int, parent_minutes: float = 0.0) -> float:
    """排队上限 = 该步声明的 timeout_minutes × 可重试次数，不低于下限。

    bus #425410 判据 2：原实现是全局硬编码 1800s，与模板声明完全脱耦——
    声明 5min 的 architect_full_design/s4 与声明 120min 的 WL-full_dev_pipeline/s2
    共用同一个 30 分钟上限，长步被短上限夹碎（实测父步 60min 预算被两条
    更短的上限各杀一次）。改为按声明计算，并把 1800 作为下限保留：
    它防的是"排队几秒即杀"，不是业务预算。
    parent_minutes: 子链传入父步声明的分钟数，保证子链预算不低于父步（判据 2）。
    """
    declared = max(float(ec.get("timeout_minutes", 30) or 30), float(parent_minutes or 0))
    return max(_QUEUE_BUDGET_FLOOR, declared * 60 * max(int(attempts), 1))


# tmux 信号是**运维催办**，不是任务派发。两个 helper 原先只传 (role, msg)，
# force 默认 False → _send_to_role 给它加上 `/goal ` 前缀，而 /goal 是会话级
# Stop hook 硬条件：「请继续完成当前步骤」永远无法自证，角色因此被永久锁死
# （R41 实测 qa 215 条该 hook、全角色累计 943 条）。越界停止指令同理。
#
# 不带 wf_id：忙门控的条件是 `not force and wf_id`，force 已开，wf_id 只为
# 连带效果而存在（重写 TASKS.md）。催办不该改角色的任务文件。
# 代价（实测）：wf_id 为空时 role_lifecycle:983 的 bus 去重整段被跳过 → 每次
# 调用都写一条 `@<role> 工作流任务` task_spec。生产 24h 仅 1 条（越界信号本就
# 稀少），不值得为此引入 wf_id 的副作用。
#
# 不带 scheduler_noise：它在 role_lifecycle:891 对 coordinator 是**直接 return
# False**（调度噪音拦截）。而 coordinator 确会作步骤目标（生产库 WL-full_dev_pipeline
# 7 个实例 assignee=coordinator）→ 发给它的越界停止指令会被静默吞掉，安全护栏
# 变回摆设。催办要的是「送达」，不是「被当成噪音拦下」。
_TMUX_SIGNAL_KW = {"force": True}


def _remind_tmux_idle(run, step, evidence: str, send_to_role) -> None:
    """角色 tmux pane 显示空闲 → 发一轮提醒 prompt（不 cancel，不递增超时）。"""
    msg = (f"[workflow] {run.workflow_name}/{step.id} 检测到角色空闲（pane 证据: {evidence[:80]}）。"
           f"请继续完成当前步骤 {step.id}。")
    send_to_role(step.target_role, msg, **_TMUX_SIGNAL_KW)


def _mentions(fact, wf_id: str, fields: str = "te") -> bool:
    """消息是否真正提及该工作流实例（完整 id，词边界）。

    fields 决定查标题(t)、正文(e)或两者，默认两者；严格归属路径传 "t"
    保持既有标题语义，避免正文顺带提及本 id 就放行。

    只看标题会漏掉把实例 id 写在 evidence 里的产出；前缀匹配会在
    wf_sub_4…/wf_sub_b… 这类同前缀实例间串味（实测 31 次宽松推进有 29 次
    是别的工作流的消息，含 wf_sub_b548 借用 wf_sub_4e63 的完成声明）。
    边界用负向断言而非分隔符白名单：角色照抄的提示是 `[实例] wf_xxx（…）`，
    全角括号紧邻，白名单永远列不全。
    """
    if not wf_id:
        return False
    bare = wf_id.replace('wf_', '', 1)
    pat = rf'(?<![\w-])({re.escape(wf_id)}|{re.escape(bare)})(?![\w-])'
    body = "\n".join(
        str(v) for k, v in (("t", getattr(fact, "t", "")), ("e", getattr(fact, "e", "") or ""))
        if k in fields
    )
    return bool(re.search(pat, body))


_WF_ID_RE = re.compile(r'(?<![\w-])(wf_[A-Za-z0-9_]+)(?![\w-])')

# 自述拒绝判据的唯一真相源在 review_gate（wf_lifecycle 的 stale rescue 也用同一份）。
_REFUSAL_T = REFUSAL_T


def _claims_own_step_and_foreign_instance(text: str, wf_id: str, template: str,
                                          step_id: str, siblings=()) -> bool:
    """完成消息是否"点名别的工作流实例，却声称完成了本模板本步骤"。

    这正是同模板并发链串味的指纹：兄弟实例 wf_sub_ad604 的 s2 完成消息把
    wf_sub_7afc1 的 s2 一起闭合了（实测 #421072）。

    比 require_wf 宽松，因为它不要求消息带本实例 id —— 生产 105/164 条裸匹配
    完成消息（no-op 巡检类）本来就不带 id，硬校验会把它们全卡成超时。
    也刻意不拦"提到别实例 id 但没声称本步完成"的消息：coordinator 派发、
    closer 建任务天然会引用别的工作流 id，那是真实产出（历史 24 条）。
    """
    if not wf_id or not template or not step_id:
        return False
    ids = set(_WF_ID_RE.findall(text or ""))
    foreign = ids - {wf_id}
    if not foreign:
        return False
    # 模板名带不带 WL-/WF- 前缀都要认：父链注册名是 WL-full_dev_pipeline，而
    # 角色照抄提示词发出的完成标题是 full_dev_pipeline/s4-complete（前缀由
    # engine 生成 id 时加，不进提示词）。旧实现按全名精确匹配恒不命中 → 守卫
    # 恒失效 → 8.7 小时前、属兄弟实例 wf_sub_61581e191d0a 的消息把本步闭合。
    _tpl = re.sub(r'^(?:WL-|WF-)', '', str(template))
    # 登记名里的纯数字模板（WL-01/WL-02/WL-09）没有可读名：显示名长在另一行模板上
    # （WL-02 与「需求流转」的 steps_json 逐字相同），剥前缀只剩 '02'，与角色标题
    # 里的显示名永远对不上 → 模板名分支恒失效（实测 40 条显示名完成标题 0 条命中）。
    # 这类退化为匹配任意模板名：能走到最后一行时 foreign 非空、本实例 id 缺席，
    # 本就不需要模板名再收窄一步。
    _tpl_pat = (r'[^\s/]+' if _tpl.isdigit()
                else r'(?:WL-|WF-)?' + re.escape(_tpl))
    # 尾部断言用 (?![\\w]) 不用 (?![\\w-])：角色完成标题固定写 `s4-complete`，
    # 紧邻的连字符会被 (?![\\w-]) 拒掉 → 即便前缀修好也恒不命中；step_id=s4 仍不会误匹配 s40。
    # 前缀认两种形态：模板名（`pm_requirements/s1-complete`）与实例 id
    # （`wf_sub_db062/s1-complete`）。后者才是引擎让角色发的标题——任务提示里
    # 给的是 `[实例] wf_xxx`，角色照抄。只认模板名时守卫对真实消息恒失效：
    # 实测一条 db062 的 s1-complete 同时闭合了 5a130 与 b0f58 的 s1（#439707）。
    # 只匹配**别的**实例：消息里出现本实例 id 不等于它没有同时替别的实例报工，
    # `wf_sub_other/s1-complete 交付给 wf_sub_mine` 必须仍判串味。
    # 后缀认两种真实完成形态：`-complete`（模板要求的标题格式，#439707）与
    # 中文「完成」（角色自发通知写 `wf_xxx/s2 完成`；2026-10-22 全库 1291 条
    # 完成类标题中 26 条是该形态 —— 只认 -complete 会让这 26 条串味消息逃过
    # 守卫，既有 TestSoftAttribution 用例锁的正是它）。中文形态还要认
    # 已/了/于时间 三种收尾：`wf_other/s1 已完成`、`s1 完成了`、`s1 完成于 10:00`
    # 都是真实写法，旧后缀一条都不拦（reviewer 返工 s6）。
    # 两种都不要求时，`wf_sub_other/s1/README 已更新` 这种路径引用会被当成
    # 完成声明，守卫就从拦截变成误伤，所以「完成」后仍要求词边界。
    _suffix = (re.escape(step_id)
               + r'(?:-complete|\s*已?完成(?:了|于\s*\d[\d:]*)?)(?![\w])')
    # 斜杠两侧允许空格：`wf_other / s1 完成` 这种排版绕过了旧的字面 `id/sN`
    # 匹配（reviewer 返工 s6 实测为 False）。
    _sep = r'\s*/\s*'
    # 别的**实例**点名本步：即便消息里也有本实例 id，也算替别人报工
    # （wf_sub_other/s1-complete 交付给 wf_sub_mine）。
    if re.search(r'(?<![\w-])(?:' + '|'.join(map(re.escape, sorted(foreign))) + r')'
                 + _sep + _suffix, text or ""):
        return True
    # 同模板兄弟实例点名：两个同模板实例不可能共享同一份产出，而消息把两条
    # 实例 id 并列后都会命中本模板/本步的完成声明 → 两边同时闭合（实测
    # #441838：qa_test_execute/s1-complete wf_sub_02b1a + wf_sub_673fc 同时
    # 闭合两条子链，其中一条的产出并不存在）。不能改成「多 id 一律拦」：
    # 合法形态是父子两层记账（`qa_test_execute/s1-complete (wf_sub_x/s1,
    # 父链 wf_y/s3)`）——父子模板不同，siblings 里查不到，仍放行（实测
    # #413744/#415150）。siblings 由调用方给：同一父链下的同模板兄弟。
    if wf_id in ids and siblings and ids & set(siblings):
        return True
    if wf_id in ids:
        return False
    return bool(re.search(r'(?<![\w-])' + _tpl_pat + _sep + _suffix, text or ""))


def _stop_tmux_violation(run, step, commands: list[str], role: str, send_to_role) -> None:
    """角色 tmux pane 显示越界操作 → 注入停止指令（安全护栏，不 cancel 工作流）。"""
    ops = "、".join(commands[:3])
    msg = (f"[workflow] {run.workflow_name}/{step.id} 检测到越界操作: {ops}。"
           f"请立即停止并撤销该操作，仅在当前工作流 {step.id} 范围内工作。")
    send_to_role(role, msg, **_TMUX_SIGNAL_KW)


# mustContain 的中英别名：qa 报告按语言习惯写「17 passed / 1 failed」或「通过 17 / 失败 1」，
# 而模板 token 只写英文 → 同一份合格报告换个语言就被判缺内容（bus #413783）。
# 引擎侧兜底：命中别名即视为该 token 已满足，模板不必逐条改数据。
_TOKEN_ALIASES = {
    "passed": ("通过", "PASS", "PASSED"),
    "failed": ("失败", "FAIL", "FAILED"),
    "coverage": ("覆盖率",),
}


def _token_hit(token: str, content: str) -> bool:
    if token in content:
        return True
    return any(alias in content for alias in _TOKEN_ALIASES.get(token.lower(), ()))


def _bump_violation_count(state: dict, commands: list) -> int:
    """越界计数：同一批命令只算一次。

    pane 回显会保留旧命令多轮，逐 tick 累加会让「一次越界」在 5 个 tick 内
    触发 cancel_step（bus #418065 要求去重）。命令集合变化才视为新一次越界。
    """
    key = "|".join(sorted(set(commands or [])))[:500]
    if key and key == state.get("violation_last_key"):
        return state.get("violation_count", 0)
    state["violation_last_key"] = key
    count = state.get("violation_count", 0) + 1
    state["violation_count"] = count
    return count


class StepHandlerMixin:
    """WorkflowEngine 的步骤处理 mixin：_tick / _sync_step_results / _validate_exit_schema / _check_exit。"""

    @staticmethod
    def _step_sd(run, step) -> dict:
        """取 step_results[step.id] 并归一化为 dict。

        step_results 的值不保证是 dict：生产 DB 实测 413 个非 dict 值
        （'summary'/'files'/'approval_token' 等键，亦有 'done' 简写形）。
        裸链式 .get 会抛 AttributeError，冒泡到 engine.run_once 的 except
        → 该轮 for 循环整体中止，后续所有工作流当轮不再 tick（静默失败）。
        所有读写 step_results[step.id] 的站点都走此函数，避免逐处补漏。
        """
        _v = run.step_results.get(step.id, {})
        return _v if isinstance(_v, dict) else {}

    def _fail_codex_blocked(self, run, step, matched_msgs: list, source: str) -> None:
        """codex 审过但未达零问题 → 步骤与实例都以 failed 收尾（ADR-2026-10-28 决策 B）。

        不用 manager.fail_step：它的 allow_retry 分支整份覆写 step_results[step_id]
        （{"status":"failed",...}），会把刚落章的 node2_blocked 一起抹掉 —— 而判据
        要求 blocked 与 failed 同时可见。
        引擎里没有「failed 步骤重试」机制（fail_step 的唯一调用点是判父步失败），
        所以不把实例晾给回收器条件 0 收成 cancelled，而是自己按 failed 收口：
        步骤判失败 = 这条链的结论就是失败，不是"被回收"。
        """
        _needle = step.id + _BLOCKED_MARK
        reason = next((str(m.get("text", "")) for m in (matched_msgs or [])
                       if isinstance(m, dict) and _needle in str(m.get("text", ""))), "")[:300]
        run.step_results[step.id] = {
            **self._step_sd(run, step),
            "node2_blocked": True,
            "node2_blocked_at": time.time(),
            "node2_blocked_by": source,
            "status": "failed",
            "failed_at": time.time(),
            "failed_reason": reason,
        }
        self._sync_step_results(run.id, run.step_results)
        LOGGER.warning("双节点: %s/%s codex 未达零问题 → 步骤判失败（%s）",
                       run.workflow_name, step.id, reason[:120])
        try:
            # 复用 close_wf：task 状态同步与子链级联都在里面，裸 UPDATE 会漏两件。
            self._lifecycle.close_wf(
                run.id, status="failed",
                reason=f"dual_node {step.id} codex 未达零问题")
        except Exception as _e:
            LOGGER.warning("blocked close_wf 失败 %s/%s: %s", run.id[:8], step.id, _e)
        try:
            self._bb.write("blocker",
                f"[workflow] {run.workflow_name}/{run.id[:12]}/{step.id} "
                f"codex 未达零问题，步骤判失败",
                evidence=(f"实例: {run.id}\n步骤: {step.id}\n结论来源: {source}\n"
                          f"角色消息: {reason}\n"
                          f"复跑: sqlite3 -readonly ~/.hermes/state/workflows.db "
                          f"\"select step_results from workflow_instances "
                          f"where instance_id='{run.id}'\""),
                src="workflow_engine")
        except Exception as _e:
            LOGGER.debug("blocked blocker 写入失败: %s", _e)

    def _parent_step_minutes(self, run) -> float:
        """子链所在父步声明的 timeout_minutes；非子链返回 0。

        子链预算不得低于父步：子链模板声明 15min 的步骤，在同一条父链里干的
        活对应父步声明 60min，按 15×次数算出来的预算必然在父步预算内先耗尽
        （bus #425410 命令 7 实测：父步 60min 被两个更短上限各杀一次）。
        """
        # 调用方两种形态：_tick 传 WorkflowRun，engine.run_once 传 DB 行 dict
        _iid = getattr(run, "id", None) or (run.get("instance_id") if isinstance(run, dict) else None)
        if not _iid:
            return 0.0
        try:
            _row = self._lifecycle.query_one(
                "SELECT wi.parent_wf_id, wi.subflow_source_step_id, p.template_id "
                "FROM workflow_instances wi "
                "LEFT JOIN workflow_instances p ON p.instance_id = wi.parent_wf_id "
                "WHERE wi.instance_id=?", (_iid,))
        except Exception as _e:
            LOGGER.debug("parent step minutes 查询失败 %s: %s", str(_iid)[:12], _e)
            return 0.0
        if not _row or not _row["parent_wf_id"]:
            return 0.0
        # 模板名兼容：实例存裸名、_workflows 以 WF- 前缀注册（manager.py:174 同款回退）。
        # 缺回退时 _parent_wf 恒 None → 父步预算保护静默失效，子链按自己的
        # 短声明算预算（实测 architect_full_design/s1 15min 被当成上限，
        # 父步 WL-full_dev_pipeline/s1 实际 30min，排队 60min 即被杀）。
        _ptid = _row["template_id"] or ""
        _parent_wf = (self._workflows.get(_ptid)
                      or (self._workflows.get(f"WF-{_ptid}") if _ptid else None))
        if not _parent_wf or not _parent_wf.steps:
            return 0.0
        _src = _row["subflow_source_step_id"] or ""
        for _s in _parent_wf.steps:
            if _s.id == _src:
                return float((_s.exit_condition or {}).get("timeout_minutes", 30) or 30)
        return 0.0

    def _eff_role(self, run, step) -> str:
        """步骤的实际执行角色：实例 effective_roles 覆盖优先，否则模板值。

        engineer/pg 是等价开发角色，但主链模板写死 engineer → 实例全压
        engineer、pg 闲置。分流决定必须随实例持久化，否则下游消费点
        （exit_messages 过滤 / busy 门控 / exit_schema 的 workspace /
        codex cwd）会按模板角色判，而产出落在另一角色处 → 步骤必超时。
        """
        from liveness import effective_role as _er
        _inst = getattr(run, "_inst_row", None) or getattr(run, "effective_roles", None)
        if _inst is None and hasattr(run, "id") and getattr(self, "_lifecycle", None):
            try:
                _rows = self._lifecycle.query(
                    "SELECT effective_roles FROM workflow_instances WHERE instance_id=?",
                    (run.id,))
                _inst = _rows[0] if _rows else None
            except Exception:
                _inst = None
        return _er(_inst, step.id, fallback=step.target_role)

    def _has_evidence(self, run, step) -> tuple:
        """status 为空的步骤是否已有满足 exit_condition 的完成消息（DESIGN T2 / AC1）。

        `status` 同时承担两个语义：投递结果（notified=角色收到）与可消费前提
        （只有 notified/running 才进 _tick）。角色忙时 status 恒空，于是即使完成
        消息早已到达也永不消费，直到排队预算耗尽被回收——A2 的 5 例实证消息
        都写在回收之前。

        判据**完全沿用** _check_exit（cat/src/text_contains/require_wf/软归属），
        不新增口径。时间下界取实例创建时刻：从未 notified 的步骤没有
        notified_at/bus_anchor，而实例创建前的消息不可能属于它。

        dual_node 不在此抢跑（node1/node2 两段式由 _tick 管）。
        返回 (最早匹配 ts, 匹配消息)；无匹配返回 (0.0, [])。
        """
        ec = step.exit_condition or {}
        cat = ec.get("bus_category", "")
        if not cat or getattr(step, "dual_node", False):
            return (0.0, [])
        if isinstance(run, dict):
            iid, created = run.get("instance_id") or "", run.get("created_at") or 0.0
            template = run.get("template_id") or ""
            from liveness import effective_role as _er
            eff = _er(run, step.id, fallback=step.target_role)
        else:
            iid = getattr(run, "id", "") or ""
            created = getattr(run, "created_at", 0) or 0.0
            template = getattr(run, "workflow_name", "") or ""
            eff = self._eff_role(run, step)
        try:
            return self._check_exit(
                cat, ec.get("source_contains", ""), ec.get("text_contains", ""),
                created_after=created, wf_id=iid if ec.get("require_wf") else "",
                own_wf_id=iid, template=template, step_id=step.id,
                wait_roles=getattr(step, "wait_roles", None), eff_role=eff)
        except Exception as _e:
            # 探测失败按「无证据」处理：走原有投递/回收路径，不让一次查询异常
            # 把回收变成永久豁免。
            LOGGER.debug("evidence check failed %s/%s: %s", str(iid)[:12], step.id, _e)
            return (0.0, [])

    def _has_live_subflow(self, run, step) -> bool:
        """当前步骤是否有"未死"的子链（父步的产出由子链承担）。

        父步骤下的子链运行期间，角色 pane 里没有"父步新产出"是正常态，
        不是卡死——据此触发的 tmux 空闲 cancel 会误杀整条父链：
        实测 wf_3f6128f1b3db/s2 于 11:31:58 被取消，而其子链
        wf_sub_c5274c45c967 (pm_requirements) 的 s2/s3 分别 11:32:27/
        11:34:57 才完成，即被取消时子链仍在跑。
        更糟：cancel_step 会整体覆写 step_results[step]（manager.py:503）
        从而抹掉 subflow_id，父链再也收不到子链完成回流通道
        （task_generator._scan_tasks 只扫 status='running' 的父链）。

        返回 True = 子链存在且未死 → 不可按空闲取消父步。
        子链 cancelled/failed = 已死，父步空闲取消维持原语义。
        """
        try:
            _lm = self._lifecycle
            _sf = self._step_sd(run, step).get("subflow_id", "")
            if not _sf:
                # subflow_id 可能已被历史 cancel 覆写抹掉，退回用 parent 链接查
                # （parent_wf_id/subflow_source_step_id 权威且不被覆写）。
                _row = _lm.query_one(
                    "SELECT instance_id FROM workflow_instances "
                    "WHERE parent_wf_id=? AND subflow_source_step_id=?",
                    (run.id, step.id))
                _sf = _row["instance_id"] if _row else ""
            if not _sf:
                return False
            _st = _lm.query_one(
                "SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,))
            return bool(_st) and _st["status"] not in ("cancelled", "failed")
        except Exception as _e:
            # 检查失败回退原行为（可取消），避免守卫本身成为新的静默卡死源
            LOGGER.debug("live-subflow check failed %s/%s: %s",
                         run.workflow_name, step.id, _e)
            return False


    def _retry_grace(self, run, step, eff: str, timeout: float) -> bool:
        """CLI 卡在传输层退避时延一个窗口、不记超时。返回 True = 本轮已延期。

        传输层故障（Connection refused / 429 客户端退避）与角色无关：角色既没工具
        调用也没产出，但它在等上游回来。记一次超时只会让链在故障期里被回收 ——
        实测 2026-10-27 11:28 food_chain_loop/wf_59463da67abf/s0：engineer 卡在
        `Connection refused … Retrying in 0s · attempt 10/15`，2 小时后 4 次超时回收。

        宽限上限 2 个窗口（用 retry_since 记真实起点，ts 每次延期都会刷新），
        超过就按原路径回收 —— 上游挂两小时以上，链也不该无限等。
        判据只看 pane 底 6 行且要求「retrying in」+「attempt」同时出现，
        角色正文里贴日志的误伤最多损失一个窗口。
        """
        from liveness import pane_cli_retrying
        if not pane_cli_retrying(eff):
            return False
        _sd = self._step_sd(run, step)
        _since = _sd.get("retry_since") or time.time()
        if time.time() - _since >= timeout * 2:
            return False
        run.step_results[step.id] = {**_sd, "ts": time.time(), "retry_since": _since}
        try:
            self._sync_step_results(run.id, run.step_results)
        except Exception as _e:
            LOGGER.debug("退避宽限同步失败 %s/%s: %s", run.id[:8], step.id, _e)
        LOGGER.info("步骤 %s/%s 角色 %s 卡在传输退避，延一个窗口（已延 %.0f 分钟）",
                    run.workflow_name, step.id, eff, (time.time() - _since) / 60)
        return True

    def _write_timeout_forensics(self, run, step, role: str, timeout_count: int) -> None:
        """超时回收前的现场快照（best-effort，失败不影响回收）。

        WHY：7 天 147 个非完成实例里，多数死在「步骤超时 N 次、该角色全程零工具调用」，
        回收时只留一句「超时自动回收」判不了因 —— 是 CLI 卡在重试？pane 活着但没读
        消息？还是角色从未被唤起？这些都能从 CLI/pane 现场区分，代价只有一次
        capture-pane。
        """
        try:
            from liveness import pane_cli_busy, tmux_pane_active
            _r = subprocess.run(["tmux", "capture-pane", "-p", "-t", f"ccs-{role}"],
                                capture_output=True, text=True, timeout=3)
            _tail = "\n".join([ln for ln in (_r.stdout or "").splitlines() if ln.strip()][-5:])
            _decl = int((step.exit_condition or {}).get("timeout_minutes", 30))
            self._bb.write("blocker",
                f"[workflow] {run.workflow_name}/{run.id[:12]}/{step.id} "
                f"{role} 超时 {timeout_count} 次且全程零产出 → 回收",
                evidence=(f"步骤声明 {_decl} 分钟/次，已超时 {timeout_count} 次\n"
                          f"角色: {role}\n"
                          f"CLI busy: {pane_cli_busy(role)}（None=探测失败）\n"
                          f"pane 存活: {tmux_pane_active(role)}\n"
                          f"transcript: 本步全程无工具调用\n"
                          f"pane 尾 5 行:\n{_tail}"),
                src="workflow_engine")
        except Exception as _e:
            LOGGER.debug("超时现场写入失败 %s/%s: %s", run.id[:8], step.id, _e)

    def _tick(self, run: WorkflowRun, step: Step):
        """推进单个步骤：exit 匹配/协作门/track/完整性提醒/超时处理。"""
        # ponytail: skip completed/cancelled runs — prevents stale step-escalation noise
        if run.status in ('completed', 'cancelled', 'step_done_ready'):
            return
        ec = step.exit_condition
        cat = ec.get("bus_category", "")
        src_filter = ec.get("source_contains", "")
        text_filter = ec.get("text_contains", "")
        timeout = ec.get("timeout_minutes", 30) * 60
        max_retries = step.max_retries
        # 实际执行角色（实例 effective_roles 覆盖优先）。tick 开头解析一次，
        # 避免每一处消费点各自查 DB；engineer↔pg 等等价角色池的分流决定
        # 存在实例上，必须让本函数内全部判定（exit 过滤/busy/workspace）一致。
        _eff = self._eff_role(run, step)

        # ── 双锚点：persisted.poll_since 与 bus_anchor，避免 tick 内消息被跳 ──
        # bus_anchor：永久记录已匹配消息的时间戳，只增不减；poll_since：轮询游标，仅在 exit_condition 匹配后同步
        # ponytail: fallback 用 time.time() 而非 run.created_at——当 wf complete 全量覆写
        # step_results 擦除 poll_since/bus_anchor 时，fall back 到 workflow 创建时间
        # 会导致 created_after 过滤器跳过所有近期 bus 消息，流水线永久卡死。
        # isinstance 守卫：step_results 的值不保证是 dict（生产 DB 实测 413 个
        # 非 dict 值：'summary'/'files'/'approval_token' 等键）。缺守卫时下面三行
        # 链式 .get 抛 AttributeError，冒泡到 engine.run_once 的 except → 该轮
        # for 循环整体中止，后续所有工作流当轮不再 tick（静默失败）。
        _sd = self._step_sd(run, step)
        _persisted = _sd.get("poll_since", time.time())
        _bus_anchor = _sd.get("bus_anchor")
        # 初始化时用 notified_at（步骤首次通知时间），确保不跳过消息
        if _bus_anchor is None:
            _bus_anchor = _sd.get("notified_at", _persisted)
        # 时间下限 = max(锚定, 工作流创建时间)：bus_anchor 可能被旧工作流残留值污染
        # （wf_7f37 匹配到 wf_cde32 的旧消息，bus_anchor=旧消息 ts, 2026-08-10）。
        # 工作流创建前绝对不存在属于它的消息 → 硬下限。
        last_ts = max(_bus_anchor or 0.0, run.created_at or 0.0)

        # ── 旧引擎产物抢救: exit_messages 已存在但 tick 未闭合 → 直接 complete ──
        _sdata = self._step_sd(run, step)
        # 抢救路径只读已落库的 exit_messages，不经 _check_exit —— 历史写进去的
        # 「拒产/拒审」标题会在这里二次生效（R163 残留），同一谓词过滤一遍。
        # 生产库存在畸形值（int/str），历史上只判真值不迭代；这里开始迭代，必须挡。
        _raw_emsgs = _sdata.get("exit_messages")
        _emsgs = ([m for m in _raw_emsgs if isinstance(m, dict)
                   and not _refusal_not_verdict(m.get("text"))]
                  if isinstance(_raw_emsgs, list) else [])
        if _emsgs and _sdata.get("status") != "completed" and not (getattr(step, 'dual_node', False) and not _sdata.get("node2_approved")):
            # 抢救路径同样过质量闸门：exit_messages 只证明"角色写了消息"，不证明
            # 产出落盘。此前直接 complete → 文件契约缺失的步骤照样推进（与宽松推进
            # 同一类假完成，R119 已在宽松推进处堵住，这里漏了同一扇门）。
            if self._quality_gate_blocks(step, run):
                # 闸门反复不过 = 判据可能本身不可满足：实测 WL-full_dev_pipeline/s4
                # 的 "P0" not in 判据对任何合格报告恒假，实例空转 18.3h 直到 24h 全局
                # 回收，期间角色被反复催一条永远做不到的指令。计数并在第
                # _GATE_ESCALATE_AT 次升级一次给模板所有者：角色侧已交付，再催它
                # 没有意义，要改的是判据本身。
                _n = int(_sdata.get("gate_block_count") or 0) + 1
                _sdata["gate_block_count"] = _n
                if _n >= _GATE_ESCALATE_AT and not _sdata.get("gate_escalated_at"):
                    _sdata["gate_escalated_at"] = time.time()
                    self._bb.write(
                        "blocker",
                        f"[workflow] {run.workflow_name}/{step.id} 闸门连续 {_n} 次未通过"
                        f" —— 判据可能不可满足，待模板所有者改判据",
                        evidence=(f"实例 {run.id}：角色已交付"
                                  f"（exit_messages={len(_sdata.get('exit_messages') or [])}），"
                                  f"但 schema/verify 每 tick 仍拦下。"
                                  f"先验模板 verify 是否对合格产出恒假（本次现场形态），"
                                  f"再考虑催角色。"),
                        src="workflow_engine")
                self._sync_step_results(run.id, run.step_results)
                return
            # bypass 补发完整性提醒（tick rescue 直接 complete，跳过双节点审查）
            # 两个问题分开发送（用户约束：每个步骤分开提示词）
            _integ_send_bypass(run, step, "tick rescue",
                               send_to_role=self._send_to_role,
                               sync_step_results=self._sync_step_results)
            try:
                self._lifecycle.complete_step(run.id, step.id)
                LOGGER.info("tick rescue: %s/%s exit_messages pre-existing → completed", run.workflow_name, step.id)
            except Exception as _e:
                LOGGER.debug("tick rescue failed for %s/%s: %s", run.workflow_name, step.id, _e)
            return  # P2-1 修复：complete 后不 fall-through 到 integrity/timeout（防重复 complete）
        # ── tmux 信号补充 ──
        # conversation_monitor 从角色 tmux pane 提取 3 信号，只消费两类：
        #   idle_complaint → 发提醒（不 cancel）
        #   violation      → 注入停止指令（越界操作）
        # step_completed 信号**不推进步骤**：pane 文本是角色的叙述，不是产出证据。
        # 实测 _COMPLETION_PATTERNS 的 (s\d+ .*完成) 会命中「s1 闭环（已完成）」
        # 「s2 还没完成」「s1 完成了吗？」；7 天内 116 步零证据完成（exit_messages
        # 空、无 bus_anchor）全部由此推进（24h 44 例）。步骤完成只认 bus 匹配 /
        # 已落盘的 exit_schema / 超时窗口后的降级推进（726 值门、892 宽松推进）。
        try:
            sig = get_conversation_signal(_eff)
        except Exception as _e:
            LOGGER.debug("conversation_monitor 信号获取失败: %s", _e)
            sig = None
        # ── tmux 信号节流与升级 ──
        # 计数存于 step_results["tmux_signals"]（跨 tick 持久化），
        # 防高频骚扰：闲置提醒 ≥3 次/越界 ≥1 次后各自冷却 5 分钟；
        # 升级：闲置/越界累计 ≥5 次 → cancel 步骤（角色不响应，继续等待无意义）。
        _sig_state = self._step_sd(run, step).get("tmux_signals") or {}
        _now = time.time()
        if sig and sig.idle_complaint:
            _idle_n = _sig_state.get("idle_count", 0) + 1
            _idle_cool = _sig_state.get("idle_cool_until", 0)
            # 与排队分支（下方 _queued>=10 那处）同源的活体门：角色正在写转录 = 在推理，
            # pane 静默是长思考的正常形态，不是卡死。不接这道门时实测两个实例的完成消息
            # 分别在 cancel 后 +27.6s（wf_e688c4fafc/s4）与 +235s（wf_sub_c5620e70dd31/s1）
            # 才落 bus —— 产出已写好却被丢弃，父链重建子链多花 35.6 分钟。
            if (_idle_n >= 5 and not self._has_live_subflow(run, step)
                    and not _role_producing_recently(_eff)):
                try:
                    # 只有真取消了才记 info：cancel_step 已幂等，返回 False 表示实例早
                    # 已终态 —— 此时还打"取消成功"会让运维分不清首取消与空操作
                    # （实测同一实例 31 分钟内 49 行"取消"，全是 10.7s 一 tick 的空转）。
                    if self._lifecycle.cancel_step(run.id, step.id):
                        LOGGER.info("tmux 信号取消: %s/%s 连续 %d 次空闲", run.workflow_name, step.id, _idle_n)
                    else:
                        LOGGER.debug("tmux 空闲取消空操作(实例已终态): %s/%s", run.workflow_name, step.id)
                    # 必须 return：cancel_step 直接写库，而本函数末尾的 _sync_step_results
                    # 用的是本轮开头载入的内存快照，会把它覆盖回未取消状态 → 步骤复活，
                    # 下 tick 再次 cancel，日志刷屏且计数无上限增长。
                    return
                except Exception as _e:
                    LOGGER.debug("tmux 空闲 cancel_step 失败: %s", _e)
                _sig_state["idle_count"] = _idle_n
            elif _now >= _idle_cool or _idle_n == 1:
                try:
                    _remind_tmux_idle(run, step, sig.idle_evidence, send_to_role=self._send_to_role)
                except Exception as _e:
                    LOGGER.debug("tmux 空闲提醒失败: %s", _e)
                _sig_state["idle_cool_until"] = _now + 300  # 5 分钟冷却
            _sig_state["idle_count"] = _idle_n
        if sig and sig.violation_found:
            _viol_n = _bump_violation_count(_sig_state, sig.violation_commands)
            # 去重键不变时 _viol_n 恒等于旧值（见 _bump_violation_count），所以
            # 「同一批越界命令一直留在 pane 的 80 行窗口里」既升不到 cancel 阈值，
            # 又会每 tick 重发一次停止指令 —— 两个分支互相抵消。冷却按**越界批次**
            # 而不是按 tick：同一批只提醒一次，新一批（key 变）立即再提醒。
            _viol_key = "|".join(sorted(set(sig.violation_commands or [])))[:500]
            _viol_cool = _sig_state.get("violation_cool_until", 0)
            _viol_last = _sig_state.get("violation_cool_key", "")
            if _viol_n >= 5 and not self._has_live_subflow(run, step):
                try:
                    self._lifecycle.cancel_step(run.id, step.id)
                    LOGGER.info("tmux 信号取消: %s/%s 连续 %d 次越界", run.workflow_name, step.id, _viol_n)
                    # 同上方 idle 分支：cancel_step 的写必须在此中止本轮，
                    # 否则末尾 _sync_step_results 用陈旧快照覆盖回未取消状态。
                    return
                except Exception as _e:
                    LOGGER.debug("tmux 越界 cancel_step 失败: %s", _e)
            elif _viol_key != _viol_last or _now >= _viol_cool:
                try:
                    _stop_tmux_violation(run, step, sig.violation_commands, sig.violation_role,
                                         send_to_role=self._send_to_role)
                except Exception as _e:
                    LOGGER.debug("tmux 越界停止指令失败: %s", _e)
                _sig_state["violation_cool_key"] = _viol_key
                _sig_state["violation_cool_until"] = _now + 300
            # 计数写在 if/else 之外，与上方 idle 分支同构：达阈值时 cancel_step
            # 已接管，随即 return；未达阈值（或 cancel 失败）则累加，供下轮判定。
            _sig_state["violation_count"] = _viol_n
        if _sig_state:
            run.step_results[step.id] = {
                **self._step_sd(run, step),
                "tmux_signals": _sig_state,
            }
            try:
                self._sync_step_results(run.id, run.step_results)
            except Exception as _e:
                LOGGER.debug("tmux 信号状态同步失败: %s", _e)
        # ── Timeout 检查（每次 tick 都执行，不受 schema/verify 影响）──
        # 超时后自动升级到 coordinator，不会被 schema 失败 return 阻断。
        # 超限阈值: max(max_retries+2, 3) 次超时后自动 cancel，防止无限重试堆积。
        _start_ts = self._step_sd(run, step).get("ts", run.created_at)
        # None 守卫：ts 或 created_at 缺失时不能相减（TypeError），按未开始处理
        elapsed = time.time() - _start_ts if _start_ts is not None else 0.0
        # 超时自动回收阈值: 默认 4 次超时后 cancel，防无限重推写 monitor_audit 噪音。
        # delivery_blocked 的子流（投递连败/角色无响应）4 次×timeout 太长（实测 ~40min 刷 bus），
        # 收紧到 2 次即回收——角色 2 个 timeout 窗口无响应即可判定卡死，断死循环。
        _auto_cancel_at = 2 if run.status == "delivery_blocked" else max(max_retries + 3, 4)
        if elapsed >= timeout:
            # 传输层退避（底栏 Retrying in … attempt n/m）→ 延窗口，不计超时
            if self._retry_grace(run, step, _eff, timeout):
                return
            # 角色忙（有其他任务执行中）→ 排队语义：不递增 timeout_count，
            # 仅延长响应窗口（避免"忙时被塞任务→未响应→超时回收"的假阳性）
            if self._is_role_busy(_eff, exclude_wf_id=run.id):
                # 角色忙 ≠ 任务未完成：先做 exit 匹配，产出已存在则直接推进
                # （否则角色完成产出但忙其他任务时，本工作流无限排队，bus #167081）
                if cat:
                    # dual_node 第二节点：宽过滤 s{N}- 才能命中 approved 消息
                    # （否则 text_filter=s{N}-complete 永远匹配不到审查结论 →
                    #  角色审完也推进不了，30d 4/9 步曾从这里直接越过闸门）。
                    _b_sd = self._step_sd(run, step)
                    _b_filter = text_filter
                    if getattr(step, "dual_node", False) and _b_sd.get("node2_sent"):
                        _b_filter = re.sub(r"-complete$", "-", text_filter)
                    _busy_match_ts, _busy_match_msgs = self._check_exit(cat, src_filter, _b_filter, created_after=last_ts,
                        wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                        template=run.workflow_name, step_id=step.id,
                        wait_roles=getattr(step, "wait_roles", None),
                        eff_role=_eff)
                    _b_verdict = (_codex_verdict(_busy_match_msgs, step.id)
                                  if (_busy_match_ts and getattr(step, "dual_node", False)
                                      and not _b_sd.get("node2_approved")
                                      and not _b_sd.get("node2_blocked")) else None)
                    if _b_verdict == "approved":
                        run.step_results[step.id] = {
                            **self._step_sd(run, step),
                            "node2_approved": True,
                            "node2_approved_at": time.time(),
                            "node2_approved_by": "role_bus",
                        }
                        self._sync_step_results(run.id, run.step_results)
                        _b_sd = self._step_sd(run, step)
                    elif _b_verdict == "blocked":
                        # 负向终态：角色审完了但未达零问题 → 不再空等到超时。
                        # 两个终态互斥：先到者定局，后到的不覆盖（见 _codex_verdict）。
                        self._fail_codex_blocked(run, step, _busy_match_msgs, "role_bus")
                        return
                    # 闸门：dual_node 未拿到审查结论前，忙分支不得推进
                    # （原实现只有一处（:426）带守卫，此处漏了 → 4/9 步由此越过）
                    if getattr(step, "dual_node", False) and not _b_sd.get("node2_approved"):
                        _busy_match_ts = 0.0
                    if _busy_match_ts:
                        # exit_schema 校验与主路径一致：busy 不绕过文件约束
                        # （2026-08-12 codex P1：schema 要求不能因忙检测分支成空文）
                        _schema_ok = True
                        if step.exit_schema:
                            _schema_ok, _ = self._validate_exit_schema(step, run)
                        if _schema_ok:
                            run.step_results[step.id] = {
                                **self._step_sd(run, step),
                                "bus_anchor": _busy_match_ts,
                                "exit_messages": _busy_match_msgs,
                            }
                            self._sync_step_results(run.id, run.step_results)
                            # bypass 补发完整性提醒（busy exit-match 直接推进，跳过双节点审查）
                            _integ_send_bypass(run, step, "busy",
                                               send_to_role=self._send_to_role,
                                               sync_step_results=self._sync_step_results)
                            _completed_via_busy = False
                            try:
                                self._lifecycle.complete_step(run.id, step.id)
                                _completed_via_busy = True
                            except Exception as _e:
                                LOGGER.debug("busy exit-match complete_step 失败: %s", _e)
                            # 已 complete 却继续往下走，会落到排队/auto-cancel/超时路径：
                            # 同一 tick 内既"步骤完成"又"工作流被取消"（实测
                            # close_wf 收到 ['cancelled','cancelled'] 双次调用）。
                            # 姐妹站点（tick-rescue/relaxed/integrity/dual_node/
                            # completion_check）均有 return，仅此处漏。
                            if _completed_via_busy:
                                return
                _queued = self._step_sd(run, step).get("queued", 0) + 1
                # 判据必须按**时长**而非次数：本分支每 tick（10s）递增一次，
                # `_queued>=10` 实际只有 ~100 秒，而角色正常执行一步就是几分钟。
                # 实测 2026-10-19 17:37-17:43 三条返工链在角色被别的任务占用
                # ~70s 后即被自动取消（queued_since→cancel 97~121s），子链全灭。
                # 与 engine.py 的排队回收共用 _queue_budget_sec（queued_since 不随 ts 刷新）。
                _q_since = self._step_sd(run, step).get("queued_since") or time.time()
                # 排队告警只有下面这条 auto-cancel blocker（带证据）。原来的"排队 6 次
                # 请 coordinator 介入"是死代码：scheduler_noise=True 对 coordinator 在
                # role_lifecycle:1093 直接 return（既不投递也不写 bus），且 6 次 = 约 60 秒，
                # 远短于角色正常一步的耗时 —— 2026-10-22 24h 内 coordinator 转录零命中。
                # 排队上限 = 角色被其他工作流长期占用，子链无法执行 → auto-cancel
                # 但「被占用」≠「卡死」：实测 8h 内 product_architect 名下 14 条子链
                # 全部在排队满 60min 时被取消，而同期它真正跑完的子链耗时 113~237min。
                # 单角色串行处理时，排队预算（声明×次数）永远小于实际服务时间 → 排队
                # 者永远等不到轮次就被杀，而取消并不释放任何产能，是纯损失（活锁）。
                # 只在角色确实不在产出时才判死：_role_producing_recently 看 transcript
                # mtime（与 wf_lifecycle 条件 4 同源），产出中的角色排队属正常等待。
                _q_budget = _queue_budget_sec(
                    ec, max(max_retries + 3, 4),
                    parent_minutes=self._parent_step_minutes(run))
                if (_queued >= 10 and time.time() - _q_since >= _q_budget
                        and not _role_producing_recently(_eff)):
                    _q_age_min = int((time.time() - _q_since) / 60)
                    LOGGER.warning("auto-cancel subflow: %s queued=%d 已排队 %d 分钟，角色 %s 长期无产出",
                                   run.id[:12], _queued, _q_age_min, _eff)
                    try:
                        self._lifecycle.close_wf(
                            run.id, status="cancelled",
                            reason=f"subflow queued {_queued}x, {_q_age_min}min >= {int(_q_budget // 60)}min")
                        # 判据 3（bus #425410）：回收必须留证。此前 blocker 只把正文写进
                        # 标题，evidence 为空 → 事后按 e 字段检索的核验一条也取不到，
                        # 今日 10 条 cap blocker 全部踩中。
                        self._bb.write("workflow",
                            f"[workflow] {run.workflow_name} 排队超限自动取消: "
                            f"角色 {_eff} 被其他工作流占用，"
                            f"排队 {_queued} 次后自动回收",
                            evidence=(f"实例: {run.id}\n步骤: {run.current_step}\n"
                                      f"角色: {_eff}\n排队次数: {_queued}\n"
                                      f"排队起点: {time.strftime('%F %T', time.localtime(_q_since))}\n"
                                      f"已排队: {_q_age_min} 分钟\n"
                                      f"本步声明预算: {ec.get('timeout_minutes', 30)} 分钟\n"
                                      f"判定上限: {int(_q_budget // 60)} 分钟"
                                      f"（= 声明 × 次数，下限 30 分钟）\n"
                                      f"复跑: sqlite3 -readonly ~/.hermes/state/workflows.db "
                                      f"\"select step_results from workflow_instances "
                                      f"where instance_id='{run.id}'\""),
                            src="workflow_engine")
                    except Exception as _cancel_e:
                        LOGGER.debug("auto-cancel subflow failed: %s", _cancel_e)
                    # 子链排队超时同样要判父步失败：父步没有终态可回写，只回收
                    # 子链等于让它在同一队列里重建一条（需求.md「修法优先级」1）。
                    _fail_parent_step(
                        self._lifecycle, self._bb, run.id,
                        reason=(f"子链排队 {_q_age_min}min 未认领 >= "
                                f"{int(_q_budget // 60)}min 预算，角色 {_eff} 长期无产出"))
                    # 已取消的工作流不应再继续排队递增与 timeout_count，
                    # 否则达阈值时会再 close_wf 一次，并对已取消实例发"超时自动回收"假告警。
                    return
                run.step_results[step.id] = {
                    **self._step_sd(run, step),
                    "ts": time.time(),  # 重置计时器，等待角色空闲
                    "queued": _queued,
                    # 首次排队时刻，**不随 ts 重置**：ts 每轮排队都被刷新，故它无法反映
                    # "排队了多久"——回收器据此判定会永远看到刚排队的假象。queued_since
                    # 只在首次排队时写入（后续排队沿用原值），给回收器一个真实的排队起点。
                    "queued_since": self._step_sd(run, step).get(
                        "queued_since", time.time()),
                }
                self._sync_step_results(run.id, run.step_results)
                # 排队豁免只在"另一个工作流真占着角色"时成立。但 busy 判据自身含
                # `timeout_count < 3`（liveness.py:211），同角色的两个卡死实例会互相
                # 判对方 busy → 双方都走排队分支 → timeout_count 恒 0 → 永不到 3
                # → 永远 busy（自我互锁）。实测积压 153 个 running 实例 timeout_count≡0。
                # 破环：排队够多仍无进展，说明不是"被占用"而是"整条链卡死"，
                # 降级走超时路径递增计数，让回收器能开工。
                # 必须同时等满排队预算：降级路径会每 tick 递增 timeout_count，
                # 4 次（默认 _auto_cancel_at）即 cancel —— 若只看次数，排队
                # ~50s 就绕过了上面的预算门槛，误杀原样复现（codex P1）。
                # 互锁破环本身不受影响：预算耗尽后仍会降级并回收。
                #
                # 完整性 fail-open 必须在这里做：角色忙时本分支要么 return 要么降级，
                # 后面的完整性块永远到不了；已声明完成、只等 integrity 回复的步骤就
                # 再也 fail-open 不了，只能被排队预算杀掉（codex review 2026-10-21）。
                # 只清状态、不推进，等角色空出来下一 tick 照常走 exit 匹配。
                self._integ_fail_open_if_due(run, step, ec)
                if (_queued >= _QUEUE_DEGRADE_AT
                        and time.time() - _q_since >= _q_budget):
                    LOGGER.warning("排队 %d 次、%d 分钟无进展 → %s/%s 降级为超时计数解锁回收",
                                   _queued, int((time.time() - _q_since) / 60),
                                   run.id[:12], step.id)
                else:
                    return  # 排队路径不递增 timeout_count（防误回收）

            timeout_count = self._step_sd(run, step).get("timeout_count", 0) + 1
            run.step_results[step.id] = {
                **self._step_sd(run, step),
                "timeout_count": timeout_count,
                "ts": time.time(),  # 重置计时器，给角色响应窗口
            }
            self._sync_step_results(run.id, run.step_results)
            # 末位值门禁对齐：最后一步（如 food_chain_loop/s7）的真实完成证据是角色写进
            # workspace 的审计文件（lr 写 VALUE_AUDIT.md），而非 bus。exit_condition 等 bus
            # 时角色只写文件 → 步骤永远 step_done_ready 卡死。此处超时后读值门禁文件，
            # PASS 即直接收尾 close（与 _handle_wf_loop 同逻辑），避免 17h 死锁。
            _steps = self._workflows.get(run.workflow_name)
            _is_last = bool(_steps and _steps.steps and _steps.steps[-1].id == step.id)
            if _is_last:
                _vg = (_steps.loop or {}).get("value_gate") or {} if _steps else {}
                _vg_file, _vg_ws = _vg.get("file", ""), _vg.get("workspace", "")
                if _vg_file and _vg_ws:
                    _audit = _CCS_WORKSPACES / _vg_ws / _vg_file
                    if _audit.exists() and not _value_audit_failed(
                            _audit.read_text(encoding="utf-8", errors="ignore")):
                        LOGGER.info("tick 末位值门禁 PASS %s/%s，直接收尾", run.id[:8], step.id)
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                            self._lifecycle.close_wf(run.id, status="completed",
                                                     reason="value gate PASS")
                        except Exception as _e:
                            LOGGER.warning("tick 末位收尾失败 %s/%s: %s", run.id[:8], step.id, _e)
            # 超限 → 自动 cancel，阻止死循环升级
            if timeout_count >= _auto_cancel_at:
                if not _role_producing_recently(_eff):
                    self._write_timeout_forensics(run, step, _eff, timeout_count)
                try:
                    self._lifecycle.close_wf(
                        run.id, status="cancelled",
                        reason=f"timeout_count={timeout_count} >= {_auto_cancel_at}")
                except Exception as _e:
                    LOGGER.warning("auto-cancel close_wf failed: %s", _e)
                self._notify_role("maintainer",
                    f"工作流超时自动回收: {run.workflow_name}/{run.id}",
                    f"step={run.current_step} timeout_count={timeout_count} >= {_auto_cancel_at}\n请排查该步骤为何持续超时。")
            # 重推任务（本路只在第 1 次超时重推，避免每窗口刷屏）
            # 角色忙（有其他任务在跑）→ 不强制重推，通知 coordinator 排队
            # 架构 P1-2：完整性阶段进行中时跳过重推（避免 /goal 覆盖完整性提醒）
            # 注意：此处在完整性阶段的 if not cat: return 之后，_integ_state 需重新读取
            # count>=2 的补位交给 _kick_stalled_roles（600s 冷却 + 产出/忙检测）：
            # 此前它带 `_tc > 0: continue` 跳过一切已计数步骤，与这里的 `== 1`
            # 合起来制造无派发空窗（实测 closer 30min21s 零派发，#431798）。
            _integ_retry_guard = _integ_load({step.id: self._step_sd(run, step)}, step.id)
            if timeout_count == 1 and not _integ_retry_guard.pending and not _integ_retry_guard.complete:
                if self._is_role_busy(_eff, exclude_wf_id=run.id):
                    # 角色在忙，延长超时窗口：通知 coordinator 而非重推
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"超时1次但{_eff}忙碌（有其他任务执行中），自动排队等待",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                else:
                    # 重推必须走 _fill_prompt_vars：直接投 prompt_template 会把
                    # {assignee}/{title} 等原样发出，被 _send_to_role 的未替换守卫
                    # 拦下 → 重推恒失败（bus #375006 实测 {assignee}）。与首推、
                    # kick-stalled 三条路径共用同一填充，保证重推拿到的 prompt 与首推一致。
                    _rt_row = self._lifecycle.query(
                        "SELECT title, description FROM tasks WHERE task_id="
                        "(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                        (run.id,))
                    _rt = _rt_row[0] if _rt_row else None
                    _rt_title = _rt["title"] if _rt else run.workflow_name
                    _rt_desc = _rt["description"] if _rt else ""
                    _rt_wf = self._workflows.get(run.workflow_name)
                    _rt_idx = next((i for i, s in enumerate(_rt_wf.steps) if s.id == step.id), -1) if _rt_wf else -1
                    _rt_prev = _rt_wf.steps[_rt_idx - 1].id if _rt_wf and _rt_idx > 0 else ""
                    self._send_to_role(
                        _eff,
                        self._fill_prompt_vars(
                            step.prompt_template, step, run.workflow_name,
                            _rt_title, _rt_desc, run.step_results, _rt_prev,
                            wf_id=run.id),
                        wf_id=run.id, step_id=step.id, force=True)
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} 已超时 1 次，等待角色响应",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                    # 审计信号：记录轮次超时（供 monitor_audit 查看 SLA）
                    # 写失败不阻断超时处理（审计是旁路，DB 锁/连接异常不应中断 tick）
                    try:
                        self._bb.write("monitor_audit",
                            f"决策: 轮次超时 → {run.workflow_name}/{run.current_step} "
                            f"step={step.id} role={_eff} 超时 {timeout}s，已重推+升级",
                            src="workflow_engine")
                    except Exception:
                        LOGGER.debug("monitor_audit write failed for %s/%s", run.workflow_name, step.id)
            # ── dual_node 超时抢救：node1_done 后角色完成审查但未推进，直接匹配 exit 推进 ──
            elif step.dual_node and _sdata.get("node1_done") and not _sdata.get("node2_sent"):
                # 超时但审查指令已发、角色可能已完成 → 检查 exit 匹配
                _wide = re.sub(r"-complete$", "-", text_filter)
                _match_ts2, _match_msgs2 = self._check_exit(cat, src_filter, _wide, created_after=last_ts,
                    wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                    template=run.workflow_name, step_id=step.id,
                    wait_roles=getattr(step, "wait_roles", None),
                    eff_role=_eff)
                if _match_ts2:
                    # 防御：__new__ 实例化（测试路径）可能无此属性，timeout 段先于初始化引用
                    if not hasattr(self, "_consumed_exit_ids"):
                        self._consumed_exit_ids = set()
                    _match_msgs2 = [m for m in _match_msgs2 if m["id"] not in self._consumed_exit_ids]
                    if _match_msgs2:
                        _match_ts2 = _match_msgs2[0]["ts"]
                        self._consumed_exit_ids.update(m["id"] for m in _match_msgs2)
                        _sdata["exit_messages"] = _match_msgs2
                        run.step_results[step.id] = _sdata
                        self._sync_step_results(run.id, run.step_results)
                        # bypass: dual_node 超时抢救补发完整性提醒
                        _integ_send_bypass(run, step, "dual_node_timeout_rescue",
                                           send_to_role=self._send_to_role,
                                           sync_step_results=self._sync_step_results)
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception:
                            pass
                # 未匹配第二条 → 继续等，return

            # ── 宽松推进：角色已写同工作流 bus 消息但分类不匹配（如 evolution_report
            #    写成了 architecture）→ 产出存在即推进，避免"产出已存在却死锁"。
            #    触发条件：步骤已超时 >=1 次（角色确实执行过）且非首步。
            #    匹配策略：先找引擎发的 task_spec（含 wf 前缀）→ 再匹配该角色在
            #    task_spec 之后写的任意消息（标题可能不含 wf 前缀，按时间窗匹配）。
            # codex P2-1：wait_roles 协作步骤不走 relaxed-advance（避免绕过协作门控）
            # 评审步骤与其他类型同权（2026-10-22）：密钥退场后不再有"等真人审批"
            # 这条豁免；review 也走宽松推进兜底，否则只能等超时 cancel。
            # handoff 不在此列（2026-10-19）：宽松推进是 exit 未匹配时的兜底，
            # 把 handoff 挡在这里 → 角色写了产出但分类不匹配时既不推进也不兜底，
            # 直接等超时 auto-cancel（与 R95 要根除的"每步白等 90min"同一症状）。
            # 排队降级路径（_QUEUE_DEGRADE_AT）下来的 timeout_count 首次变 1，此时
            # 角色并未"超时未响应"，只是排了很久队。若不排除，会立即命中下面的
            # relaxed-advance（它只要求角色写过任意同 wf 消息，不限分类）→ 误推进。
            # 读持久化字段而非 locals()：_queued 只在 busy 分支（:272）定义，
            # 非 busy 路径根本不存在该局部变量，locals() 探测会拿到旧值或抛错。
            _from_queue_degrade = (self._step_sd(run, step).get("queued", 0)
                                   >= _QUEUE_DEGRADE_AT)
            if (timeout_count >= 1 and not step.dual_node
                    and not getattr(step, "wait_roles", None)
                    and not _from_queue_degrade):
                try:
                    # 全量读：_has_anchor 需跨分类找 task_spec 锚点（cat 过滤会漏掉），
                    # limit 提升到 500 覆盖更大消息窗口，降低超长负载漏匹配风险
                    _role_msgs = self._bb.read(limit=500)
                    _wf_id = run.id
                    _notified_ts = self._step_sd(run, step).get("notified_at", 0)
                    # 归属校验必须看消息正文（t+e）：角色产出标题常不含实例 id，
                    # 只在 evidence 里写「实例 wf_xxx」。只看标题会把自家产出当别人的。
                    # 且必须匹配**完整 id**：wf_sub_4…/wf_sub_b… 这类前缀在
                    # [:8] 截断下大面积碰撞（实测 31 次宽松推进 29 次是别的工作流
                    # 的消息，含 wf_sub_b548 借用 wf_sub_4e63 的完成声明）。
                    # evidence 一并带上：驳回分类段【系统问题→x】写在 evidence 里，
                    # 只取标题会让 _parse_review_issues 解析为空（与 :1662 同源）。
                    _relaxed = [
                        {"id": f.id, "text": f.t[:200],
                         "evidence": (getattr(f, "e", "") or "")[:2000],
                         "ts": f.ts, "src": f.src}
                        for f in _role_msgs
                        if f.src == _eff
                        and f.src != "workflow_engine"  # 排除引擎自产告警，防止误判角色产出
                        and getattr(f, "cat", "") != "blocker"  # 拒产/拒审的 blocker 不是产出（R163）
                        and not _refusal_not_verdict(f.t)  # 自述拒绝同样不是产出（R163；带结论词的驳回除外）
                        and f.ts > _notified_ts
                        and f.ts < time.time() + 60  # 防未来时间戳
                        and _mentions(f, _wf_id)
                    ]
                    # task_spec 锚点确认确实是本工作流任务（同样是完整 id，不做前缀匹配）
                    _has_anchor = any(
                        f.cat == "task_spec" and _mentions(f, _wf_id)
                        for f in _role_msgs
                    )
                    if _relaxed and _has_anchor:
                        # 宽松推进不得绕过质量闸门：exit_schema/verify 是"产出真的
                        # 落盘"的唯一判据，与主路径同一把尺子。缺这道门时角色写了
                        # 任意同工作流消息就算完成（7 天 6 次：writer/reviewer/
                        # engineer/closer 各 1~2 次），下游拿不到契约件才回头返工。
                        if self._quality_gate_blocks(step, run):
                            return
                        LOGGER.info("relaxed-advance %s/%s: 角色已写同工作流 bus 消息 (cat 不匹配, %d 条)",
                                    run.workflow_name, step.id, len(_relaxed))
                        run.step_results[step.id] = {
                            **self._step_sd(run, step),
                            "bus_anchor": _relaxed[0]["ts"],
                            "exit_messages": _relaxed,
                            "relaxed_advance": True,
                        }
                        self._sync_step_results(run.id, run.step_results)
                        self._bb.write("blocker",
                            f"[workflow] {run.workflow_name} {step.id} 宽松推进: "
                            f"角色写了同工作流消息但分类不匹配（期望 {cat}）",
                            src="workflow_engine")
                        # bypass 补发完整性提醒（relaxed-advance cat 不匹配直接推进）
                        _integ_send_bypass(run, step, "relaxed",
                                           send_to_role=self._send_to_role,
                                           sync_step_results=self._sync_step_results)
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception as _e:
                            LOGGER.debug("relaxed-advance complete_step 失败: %s", _e)
                        return
                except Exception as _relax_err:
                    LOGGER.debug("relaxed-advance 检查失败: %s", _relax_err)


        # ── 完整性提醒阶段（在 timeout 之前，任何步骤完成时都触发）──
        # 用户原话：「daemon需要重复提醒ccs（你真的解决用户的问题了吗？
        #            你真的有严格按用户的提示去完成任务吗）」
        # fire-and-forget：发提醒但不 return/不阻塞 exit 匹配（codex 审查：#5 阻塞风险）
        # dual_node 未到 node2 时跳过（双节点机制自行管理审查，避免冲突）
        if not hasattr(self, '_consumed_exit_ids'):
            self._consumed_exit_ids = set()  # 完整性阶段也用，先于 exit 匹配初始化
        # 防无限增长（P2 修复）：超过 1000 条时 LRU 淘汰保留最近 500 条，
        # 避免全量清空导致跨实例消息去重失效（bus #162146 防护）
        if len(self._consumed_exit_ids) > 1000:
            self._consumed_exit_ids = set(list(self._consumed_exit_ids)[-500:])
        _integ_state = _integ_load({step.id: self._step_sd(run, step)}, step.id)
        _integ_dual_pending = (getattr(step, 'dual_node', False)
                               and not self._step_sd(run, step).get("node2_sent"))
        if _integ_state.pending and not _integ_state.complete and not _integ_dual_pending:
            if _integ_should_send(_integ_state):
                _dim = _integ_next_dim(_integ_state)
                if _dim:
                    _goal = _integ_full_prompt(run, step, run.step_results)
                    _output_summary = str(
                        self._step_sd(run, step).get("exit_messages", [""])
                    )[:200]
                    _prompt_text = _integ_format(_dim, step_goal=_goal, output_summary=_output_summary)
                    _bus_msg = (
                        f"[{_dim['id']}/{len(_integ_dimensions)}] {run.workflow_name}/{step.id}\n"
                        f"{_prompt_text}\n\n"
                        f"请逐条回答上述完整性审查维度，完成后写 bus "
                        f"cat={cat}，标题含 {step.id}-integrity-approved"
                    )
                    try:
                        self._send_to_role(
                            _eff,
                            f"/goal\n## 完整性审查\n{_bus_msg}",
                            wf_id=run.id, step_id=step.id, force=True
                        )
                        _integ_advance(run.step_results, step.id)
                        self._sync_step_results(run.id, run.step_results)
                        LOGGER.info("integrity: %s/%s sent %s/%d",
                                   run.workflow_name, step.id, _dim['id'], len(_integ_dimensions))
                    except Exception as _e:
                        LOGGER.warning("integrity: send failed %s/%s: %s",
                                     run.workflow_name, step.id, _e)
                return  # pending阶段：dims 未发完，return 等下一 tick 继续发送

        if _integ_state.complete and not _integ_dual_pending:
            # 维度已发完，等待角色回复（{step_id}-integrity-approved）
            _integ_filter = f"{step.id}-integrity-approved"
            # integrity 有意不传 wait_roles：integrity 是 target_role 1:1 回复协议（非多角色协作门），
            # 若传 wait_roles，integrity 回复本身也会被协作门死锁（2026-08-11 codex 审查确认）
            _integ_ts, _integ_msgs = self._check_exit(
                cat, src_filter, _integ_filter, created_after=last_ts,
                wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                template=run.workflow_name, step_id=step.id,
                eff_role=_eff,
            )
            if _integ_ts:
                _integ_msgs = [m for m in _integ_msgs if m["id"] not in self._consumed_exit_ids]
                if _integ_msgs:
                    self._consumed_exit_ids.update(m["id"] for m in _integ_msgs)
                    _integ_reply(run.step_results, step.id)
                    self._sync_step_results(run.id, run.step_results)
                    LOGGER.info("integrity: %s/%s reply matched, proceeding",
                               run.workflow_name, step.id)
                    # codex P1 修复：直接 complete_step 而非 fall-through 到 exit 匹配。
                    # fall-through 时 exit_messages 已被 _consumed_exit_ids 过滤，
                    # 本轮不 complete → 下轮 tick rescue 又补发 bypass 造成重复提问。
                    try:
                        self._lifecycle.complete_step(run.id, step.id)
                    except Exception as _e:
                        LOGGER.debug("integrity reply complete_step 失败: %s", _e)
                    return
            else:
                _integ_elapsed = time.time() - self._step_sd(run, step).get(
                    "integrity_last_sent_at", time.time())
                _integ_timeout = ec.get("timeout_minutes", 30) * 60
                _fail_open = False
                if _integ_elapsed >= _integ_timeout:
                    if not _integ_retry(run.step_results, step.id):
                        self._sync_step_results(run.id, run.step_results)
                        LOGGER.error("integrity: %s/%s fail-open, bypassing",
                                   run.workflow_name, step.id)
                        _fail_open = True  # 不 return → 继续走 exit 匹配/超时（避免悬死）
                    else:
                        self._sync_step_results(run.id, run.step_results)
                if not _fail_open:
                    return  # 仍在等回复，return 等下一 tick 检查

            # 无 bus_category 的步骤不走 exit 匹配（纯超时 + 角色 wf complete 驱动），
            # 否则空 cat 匹配所有 bus 消息，导致 engine 自产告警被当作角色产出完成步骤。
            if not cat:
                return
            # exit 匹配必须在 timeout 检查之后、return 之前执行，
            # 否则 notified/running 步骤永远无法通过 exit 条件推进（bus #162804）。
            # 消息去重：同一消息不应被多个工作流同时消费（bus #162146 QA 4任务共享测试结果）
            # _consumed_exit_ids 已在上方完整性阶段初始化（P1-3 死代码已删）
            # dual_node：node2_sent 后将 filter 改为 s{N}-（匹配 s2-complete 和 s2-codex-approved）
            _node2_sent = self._step_sd(run, step).get("node2_sent", False)
            # 协作门：wait_roles 声明的角色都产出匹配消息才推进（多角色协作）
            _wait_roles = getattr(step, "wait_roles", None)
            if step.dual_node and _node2_sent:
                # 宽匹配：第二节点消息（codex-approved）到达时也能匹配
                _wide_filter = re.sub(r"-complete$", "-", text_filter)
                _match_ts, _match_msgs = self._check_exit(cat, src_filter, _wide_filter, created_after=last_ts,
                            wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                            template=run.workflow_name, step_id=step.id, wait_roles=_wait_roles,
                            eff_role=_eff)
            else:
                _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts,
                            wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                            template=run.workflow_name, step_id=step.id, wait_roles=_wait_roles,
                            eff_role=_eff)
            if _match_ts:
                # 过滤已被其他工作流消费的消息
                _match_msgs = [m for m in _match_msgs if m["id"] not in self._consumed_exit_ids]
                _match_ts = _match_msgs[0]["ts"] if _match_msgs else 0.0
            if _match_ts:
                # 标记消息已消费，防止其他工作流重复匹配（bus #162146）
                self._consumed_exit_ids.update(m["id"] for m in _match_msgs)
                _old = self._step_sd(run, step)
                run.step_results[step.id] = {
                    **_old,
                    "bus_anchor": _match_ts,
                    "exit_messages": _match_msgs,
                    # 双节点状态必须持久化，不能被 exit 匹配覆盖
                    "node1_done": _old.get("node1_done", False),
                    "node1_at": _old.get("node1_at"),
                    "node2_sent": _old.get("node2_sent", False),
                }
                self._sync_step_results(run.id, run.step_results)

                # ── 双节点机制：第一节点消息到达 → 注入 codex CLI 审查指令 ──
                # 用户需求：角色完成任务写 bus 后，引擎发 codex CLI 三方 20 维度审查，
                # 连续两次零问题后角色再写第二条 bus，两条都到才算完成。
                # dual_node 步骤走此路径；非 dual_node 走 integrity 20 维问答。
                if getattr(step, 'dual_node', False):
                    _sdata = self._step_sd(run, step)
                    if not _sdata.get("node2_sent"):
                        _codex_prompt = _codex_review_prompt(
                            run.workflow_name, step.id, cat)
                        run.step_results[step.id] = {
                            **_sdata,
                            "node1_done": True, "node1_at": time.time(),
                            "node2_sent": True, "node2_sent_at": time.time(),
                        }
                        self._sync_step_results(run.id, run.step_results)
                        LOGGER.info("双节点: %s/%s 第一节点完成，已发 codex 审查指令", run.workflow_name, step.id)
                        # R5: 自动执行 codex review --uncommitted (三方20维审查自动化)
                        _codex_result = self._run_codex_review(run, step)
                        if _codex_result.get("success"):
                            # 引擎自己跑出的零问题结论就是审查证据，比角色自述更强：
                            # 置 node2_approved 才是"第二节点真到"（manager.complete_step
                            # 只认这个字段，node1_done/node2_sent 都无判别力）。
                            run.step_results[step.id] = {
                                **self._step_sd(run, step),
                                "node2_approved": True,
                                "node2_approved_at": time.time(),
                                "node2_approved_by": "engine_codex",
                            }
                            self._sync_step_results(run.id, run.step_results)
                            LOGGER.info("双节点: %s/%s codex review 通过，node2 已批",
                                        run.workflow_name, step.id)
                        else:
                            LOGGER.warning("双节点: %s/%s codex review 失败: %s，仍发提示让角色人工审查",
                                           run.workflow_name, step.id, _codex_result.get("error", "未知错误"))
                        self._send_to_role(_eff, _codex_prompt,
                                           wf_id=run.id, step_id=step.id, force=True)
                        return
                    else:
                        # 第二节点消息到达：宽过滤（s{N}-）会同时命中 s{N}-complete
                        # 与 s{N}-codex-approved，必须看标题是否真有结论标记才认。
                        _n2_sd = self._step_sd(run, step)
                        _n2_verdict = (_codex_verdict(_match_msgs, step.id)
                                       if not _n2_sd.get("node2_approved")
                                       and not _n2_sd.get("node2_blocked") else None)
                        if _n2_verdict == "blocked":
                            self._fail_codex_blocked(run, step, _match_msgs, "role_bus")
                            return
                        if _n2_verdict == "approved":
                            run.step_results[step.id] = {
                                **self._step_sd(run, step),
                                "node2_approved": True,
                                "node2_approved_at": time.time(),
                                "node2_approved_by": "role_bus",
                            }
                            self._sync_step_results(run.id, run.step_results)
                        # 第二节点（codex-approved）消息到达 → 完成步骤
                        # 用户要求"每个步骤"都提醒 → dual_node 完成路径也补发
                        _integ_send_bypass(run, step, "dual_node_codex_approved",
                                           send_to_role=self._send_to_role,
                                           sync_step_results=self._sync_step_results)
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception as _e:
                            LOGGER.debug("双节点 codex-approved complete_step 失败: %s", _e)
                        return
                else:
                    # 非 dual_node → 激活 integrity（两个核心问题审查）
                    # P0 修复：激活后 return，让下一 tick 走完整性阶段（发 dims）。
                    # 若 fire-and-forget 直接 complete → integrity 孤儿化，用户核心需求失效。
                    # interval 可配置：从 exit_condition.integrity_interval_sec 读取，默认 30s
                    _integ_interval = ec.get("integrity_interval_sec", 30)
                    _integ_activate(run.step_results, step.id, interval_sec=_integ_interval)
                    return  # 完整性阶段接管：下 tick 发 dim01/dim02，等回复后推进
        if not cat:
            return
        # dual_node 第二节点：宽 filter 为 s{N}-，同时命中 s{N}-complete 与
        # s{N}-codex-approved。必须在这里宽化而不是只在上方 :1129 宽化 ——
        # 那一段整体嵌在 `if _integ_state.complete and not _integ_dual_pending:`
        # 里，而 `_integ_activate` 只在**非** dual_node 分支调用（:1225），
        # dual_node 步的 integrity_complete 永远是默认 False → 那段是死代码。
        # 生产实测：13/13 dual_node 步停在 node1_done+node2_sent 且无 approved，
        # 其中 4 个实例角色**已经写了** s1-codex-approved 事实（#439995/459762/
        # 461553/462996），引擎收不到 —— 不是角色没写，是写对了也匹配不到。
        _dn_wide = bool(getattr(step, "dual_node", False)
                        and self._step_sd(run, step).get("node2_sent"))
        _exit_filter = (re.sub(r"-complete$", "-", text_filter)
                        if _dn_wide else text_filter)
        _match_ts, _match_msgs = self._check_exit(
                        cat, src_filter, _exit_filter, created_after=last_ts,
                        wf_id=run.id if ec.get("require_wf") else "", own_wf_id=run.id,
                        template=run.workflow_name, step_id=step.id,
                        wait_roles=getattr(step, "wait_roles", None),
                        eff_role=_eff)
        _dn_sd = self._step_sd(run, step)
        _dn_verdict = (_codex_verdict(_match_msgs, step.id)
                       if (_dn_wide and _match_ts
                           and not _dn_sd.get("node2_approved")
                           and not _dn_sd.get("node2_blocked")) else None)
        if _dn_verdict == "blocked":
            # 负向终态优先于本段其余逻辑：判失败后不得再落 bus_anchor / complete_step。
            self._fail_codex_blocked(run, step, _match_msgs, "role_bus")
            return
        if _dn_verdict == "approved":
            # 放宽可达性 ≠ 放宽判据：宽 filter 把第一节点自己的 s{N}-complete
            # 也带进来了，只有标题真含 s{N}-codex-approved 才算审查到。
            run.step_results[step.id] = {
                **self._step_sd(run, step),
                "node2_approved": True,
                "node2_approved_at": time.time(),
                "node2_approved_by": "role_bus",
            }
            self._sync_step_results(run.id, run.step_results)
            LOGGER.info("双节点: %s/%s 收到 codex-approved 结论，node2 已批",
                        run.workflow_name, step.id)
        if _match_ts:
            # ── 锚定已匹配消息的时间戳，以后只检更新消息 ──
            # 同时保存 exit_messages 作为角色产出证据（task_evidence 依赖此字段判定收益）
            run.step_results[step.id] = {
                **self._step_sd(run, step),
                "bus_anchor": _match_ts,
                "exit_messages": _match_msgs,
            }
            self._sync_step_results(run.id, run.step_results)

            # schema/verify 失败 → 写 blocker 且不推进，等角色补产出后重试
            if self._quality_gate_blocks(step, run):
                return

            try:
                self._lifecycle.complete_step(run.id, step.id)
            except Exception as e:
                LOGGER.error("[wf] LM complete_step 失败: %s", e)
                # 步骤不匹配说明 lifecycle 已推进到下一步 → 跳过不重试
                if "步骤不匹配" in str(e):
                    return
                # DB 瞬态故障 → 有 timeout_count 间接阻止无限重试
                _prev = self._step_sd(run, step)
                run.step_results[step.id] = {
                    **_prev,
                    "timeout_count": _prev.get("timeout_count", 0) + 1,
                }
                self._sync_step_results(run.id, run.step_results)
                return

            # ── 空转检测: exit_messages 命中空报告模式 → 角色冷却 ──
            try:
                from cron_scheduler import _IDLE_PATTERNS
                from cron_scheduler import CronScheduler
                _msgs_text = " ".join(
                    m.get("text", "") for m in _match_msgs if isinstance(m, dict)
                ).lower()
                if any(pat in _msgs_text for pat in _IDLE_PATTERNS):
                    _cs = CronScheduler()
                    _cs.report_idle(_eff)
            except Exception as _idle_err:
                LOGGER.debug("idle detection skipped: %s", _idle_err)

            # 评审步骤不再在 tick 里等审批（2026-10-22）：密钥机制已退场，
            # 结论由 _advance_production_wf 的 review_gate 判据消费。

            # lifecycle.manager 已写入 step_results，此处不再重复写入
            return

        # 退出条件未匹配（无 bus 消息，timeout 已在顶部检查）
        # poll_since 推进 = now - 30s 安全窗口，覆盖慢角色写 bus 的延迟（qa 写消息可达数分钟）。
        # bus_anchor 保持不动——它是"已匹配消息"的锚点，只在匹配时更新。
        # 修复：之前 no-match 也推进 bus_anchor，导致 tick 间消息被永久跳过。
        _now = time.time()
        _new_poll = _now - 30
        run.step_results[step.id] = {
            **self._step_sd(run, step),
            "poll_since": _new_poll,
        }
        self._sync_step_results(run.id, run.step_results)
        # bus 未匹配 → 检查 completion_check 文件判据（handoff/gate 通用）
        # 防止角色完成产出文件但忘了写 bus 导致工作流永久卡死（2026-08-09 观测：
        # scout 写了 SCOUT_REPORT.md 但未写 evolution_report bus → pm 步骤永远等不到）
        _cc = getattr(step, 'completion_check', None) or ec.get('completion_check')
        if _cc and _cc.get("file"):
            # 支持 glob（与 exit_schema 一致，工程 P1-6）：含 * 时用 glob 匹配，否则 exists()
            _cc_file = _cc["file"]
            _min = _cc.get("min_size", 0)
            _fp = pathlib.Path(_cc_file).expanduser()
            # 新鲜度下限（与 _validate_exit_schema 同范式）：只查 exists()+size 会被
            # workspace 里的历史产物永久满足 —— 实测 SCOUT_REPORT.md 停在 6 周前，
            # s1 靠它虚假通过（bus 无 s1-complete 证据），下游全链空转。
            _cc_floor = (getattr(run, 'created_at', 0) or 0) - 2
            if "*" in _cc_file:
                _matches = sorted(_fp.parent.glob(_fp.name))
                _fresh = [m for m in _matches
                          if _cc_floor <= 0 or m.stat().st_mtime >= _cc_floor]
                _fp_ok = bool(_fresh) and _fresh[0].stat().st_size >= _min
            else:
                _fp_ok = (_fp.exists() and _fp.stat().st_size >= _min
                          and (_cc_floor <= 0 or _fp.stat().st_mtime >= _cc_floor))
            if _fp_ok:
                LOGGER.info("completion_check: %s 文件存在且 ≥%d 字节 → 推进 %s",
                            _fp.name, _min, step.id)
                if step.dual_node:
                    # dual_node：文件存在 = 第一节点完成，注入第二节点审查指令。
                    # 这里必须有 else：node1_done 一旦为真旧代码就直落下面的
                    # complete_step——第二 tick 无条件完成，注入的审查指令根本没跑
                    # （30d 6/9 步走的就是这条，实测 node1→completed 仅 10.7s，
                    # 而 codex review 单次 6m19s 物理上不可能审完）。
                    _sdata = self._step_sd(run, step)
                    if not _sdata.get("node1_done"):
                        _sdata["node1_done"] = True
                        _sdata["node1_at"] = time.time()
                        _sdata["ts"] = time.time()
                        # 发 codex CLI 审查指令（第二节点）+ 设 node2_sent 阻断重复注入
                        _codex_prompt = _codex_review_prompt(
                            run.workflow_name, step.id, cat)
                        _sdata["node2_sent"] = True
                        _sdata["node2_sent_at"] = time.time()
                        run.step_results[step.id] = _sdata
                        self._sync_step_results(run.id, run.step_results)
                        try:
                            _sent = self._send_to_role(_eff, _codex_prompt,
                                                       wf_id=run.id, step_id=step.id,
                                                       force=True)
                            if _sent is True:
                                LOGGER.info("双节点: %s/%s 第一节点完成，codex 审查指令已发送",
                                            run.workflow_name, step.id)
                            else:
                                # "busy"/False = 指令没到角色手上。旧代码无条件写
                                # "已发送"，日志与事实相反（08:31:23 现场：同毫秒
                                # 既"未投递"又"已发送"）。node2_sent 保持真值以免每 tick
                                # 重复注入；证据靠 node2_approved，不靠这句日志。
                                LOGGER.warning("双节点: %s/%s 审查指令未投递 (%s)，等角色自行审查",
                                               run.workflow_name, step.id, _sent)
                        except Exception as _send_err:
                            LOGGER.exception("双节点: %s/%s 审查指令发送失败: %s",
                                             run.workflow_name, step.id, _send_err)
                        return
                    # node1 已落章但审查未过 → 本 tick 不推进，继续等 node2
                    if not _sdata.get("node2_approved"):
                        return
                # bypass 补发完整性提醒（不阻塞完成）：
                # completion_check 文件判据直接推进，跳过 dual_node 审查流程。
                _integ_send_bypass(run, step, "completion_check",
                                   send_to_role=self._send_to_role,
                                   sync_step_results=self._sync_step_results)
                try:
                    self._lifecycle.complete_step(run.id, step.id)
                except Exception as _e:
                    LOGGER.debug("completion_check complete_step 失败: %s", _e)
                return


        return

    def _codex_scope_dir(self, run, step) -> tuple:
        """建一个只含本步产出的临时目录，返回 (目录, 产出文件, 错误)。

        codex review 没有任何路径过滤参数：--uncommitted/--base/--commit 都吃整仓
        diff，且 --uncommitted 与 [PROMPT] 互斥（实测 EXIT=2），所以"用 prompt
        限定范围"这条路不存在。角色 workspace 又只是 home 的子目录
        （`git -C ~/ccs-workspaces/<role> rev-parse --show-toplevel` 全部返回
        /home/administrator）→ 在那里跑等于把整个 home 的未提交改动送审
        （实测 1938 处 / 97 文件）。范围只能靠"仓库里只有本步产出"来限定。
        """
        req = list((getattr(step, "exit_schema", None) or {}).get("required") or [])
        if not req:
            return (None, [], "本步无 exit_schema.required，无法限定审查范围")
        ws = _CCS_WORKSPACES / self._eff_role(run, step)
        ws_real = os.path.realpath(ws)
        floor = (getattr(run, "created_at", 0) or 0) - 2
        picked = []
        for name in req:
            src = ws / name
            hits = []
            for m in (sorted(src.parent.glob(src.name)) if "*" in name else [src]):
                # glob 结果要再验一遍：schema 里的字面路径已验过，但通配符能
                # 匹配到指向 ws 外的符号链接（realpath 才是真正落点）。
                real = os.path.realpath(m)
                # 必须带分隔符边界：startswith("/ws/dev") 会放行兄弟目录 /ws/devops
                if not (real == ws_real or real.startswith(ws_real + os.sep)):
                    return (None, [], f"路径越权: {name} -> {real}")
                if Path(real).is_file() and os.stat(real).st_mtime >= floor:
                    hits.append(Path(real))
            if not hits:
                # 缺一项就审 = 用残缺产出换"通过"，node2_approved 是假证据
                return (None, [], f"缺少本实例产出: {name}")
            picked.extend(hits)
        tmp = Path(tempfile.mkdtemp(prefix="codex-scope-"))
        for m in picked:
            dst = tmp / os.path.relpath(m, ws_real)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(m, dst)
        return (tmp, [str(m) for m in picked], "")

    def _run_codex_review(self, run, step) -> dict:
        """自动执行 codex review --uncommitted (三方20维审查自动化，R5 增长).

        审查范围由本步 exit_schema.required + 本实例产出文件决定（见
        _codex_scope_dir），与角色 workspace 的 git 根无关。
        """
        scope = None
        try:
            scope, _files, _err = self._codex_scope_dir(run, step)
            if scope is None:
                return {"success": False, "error": _err}
            # timeout 实测标定：21 文件/734 行 diff 在本机需 6m19s（2026-10-17 实测），
            # 原值 300s 在该规模下必然超时 → 闸门回落人工审查。值可经
            # CODEX_REVIEW_TIMEOUT 调整（大 diff 上调，无需改代码）。
            subprocess.run(["git", "init", "-q", str(scope)],
                           capture_output=True, timeout=60)
            # 必须先 stage：空仓库里复制进来的文件是 untracked，若 codex 只取
            # `git diff` 则 diff 为空 → 恒判"无发现"= 假通过（比审整个 home 更坏）。
            # --uncommitted 的 help 写的是 staged+unstaged+untracked，stage 一遍
            # 让两种实现都看得到内容。
            subprocess.run(["git", "-C", str(scope), "add", "-A"],
                           capture_output=True, timeout=60)
            result = subprocess.run(
                ["codex", "review", "--uncommitted", "-c", "model=9router_hermes"],
                capture_output=True, text=True, timeout=_CODEX_REVIEW_TIMEOUT,
                cwd=str(scope),
            )
            if result.returncode == 0:
                out = result.stdout + result.stderr
                # 通过判据：codex 0.148.0 的实际措辞是 "No findings"（实测），
                # 原判据只认 "No issues"/"0 issues"/"0 problems"（全部 0 命中）
                # → 干净 diff 也被判失败，引擎回落"让角色人工审查"，闸门形同虚设。
                # 改为：否定词表匹配 + 必须无严重级别标记（两条同时满足才算通过）。
                _clean = out.lower()
                _passed_phrase = any(s in _clean for s in (
                    "no findings", "no issues", "no problems", "0 issues",
                    "0 problems", "no security issues", "nothing to report"))
                _has_finding = any(s in _clean for s in (
                    "[p0]", "[p1]", "[p2]", "critical", "vulnerability",
                    "security issue", "bug:", "error:"))
                if _passed_phrase and not _has_finding:
                    return {"success": True, "output": out[:500]}
                return {"success": False, "error": f"codex 发现问题: {out[:300]}", "output": out[:500]}
            else:
                return {"success": False, "error": f"codex 返回码 {result.returncode}: {result.stderr[:300]}"}
        except subprocess.TimeoutExpired:
            return {"success": False,
                    "error": f"codex review 超时 ({_CODEX_REVIEW_TIMEOUT}s)"}
        except FileNotFoundError:
            return {"success": False, "error": "codex CLI 未找到 (请确认已安装)"}
        except Exception as e:
            return {"success": False, "error": f"codex review 异常: {e}"}
        finally:
            if scope is not None:
                shutil.rmtree(scope, ignore_errors=True)


    def _sync_step_results(self, wf_id: str, step_results: dict):
        """将步骤结果持久化到 DB（事务内更新 step_results 列）。"""
        try:
            lm = self._lifecycle
            lm.begin()
            lm.execute_raw(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(step_results, ensure_ascii=False), wf_id)
            )
            lm.commit()
        except Exception as _e:
            LOGGER.exception("_sync_step_results 写入失败")
            try:
                lm.rollback()
            except Exception as _e2:
                LOGGER.exception("_sync_step_results rollback 也失败")
                try:
                    self._bb.write("code_fix", f"pipeflow: DB write+rollback 双重失败 wf={wf_id}",
                                   evidence=f"write_err={_e}, rollback_err={_e2}", src="pipeflow")
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("bus write fail in _sync_step_results fallback")

    def _integ_fail_open_if_due(self, run, step: Step, ec: dict) -> None:
        """角色忙期间到期的完整性审查直接 fail-open（清状态，不推进）。

        _tick 的排队分支在完整性块之前 return，所以"角色忙 + 等 integrity 回复"
        的步骤永远等不到 fail-open（codex review 实测：把 _is_role_busy 打成 True，
        原 fail-open 用例就只能读到测试自己 set 的 complete=True）。
        """
        _st = _integ_load({step.id: self._step_sd(run, step)}, step.id)
        if not _st.complete:
            return
        _elapsed = time.time() - self._step_sd(run, step).get(
            "integrity_last_sent_at", time.time())
        if _elapsed < ec.get("timeout_minutes", 30) * 60:
            return
        # 返回 True = 只是重开一轮审查（pending 仍为 True，状态没清）；
        # False = 真正的 fail-open。日志必须分支，否则运维会以为闸门被绕过。
        _reopened = _integ_retry(run.step_results, step.id)
        self._sync_step_results(run.id, run.step_results)
        if _reopened:
            LOGGER.info("integrity busy: %s/%s 重开一轮审查（角色忙，状态保留）",
                        run.id[:12], step.id)
        else:
            LOGGER.warning("integrity fail-open(busy): %s/%s 角色忙期间已超时，清状态等空闲",
                           run.id[:12], step.id)

    def _gate_prompt(self, run, step: Step, msg: str) -> None:
        """闸门失败：写 bus 之外**直接提示执行角色**补哪个文件。

        只写 bus 时角色根本不去读——实测 wf_rw_6f51b0ff030e/s1 18:55 已声明完成、
        闸门判缺产出，原样挂到 20:12（77min）父链停在 s4 rejected；
        wf_rw_50eb6ec9d6fd 同样挂 3h20m 直到 timeout_count=3。
        """
        # 走 _notify_role 而不是 _send_to_role：后者会 _wait_for_role_ready（最多
        # 90s）并可能把运维停掉的角色拉起来，而 engine 单线程逐实例 tick
        # （engine.py:1419）→ N 个卡步叠加成 90s×N/5min 的整轮阻塞。
        # 闸门提示是旁路通知，不该拖住 tick；_notify_role 的 ccs send 有 15s 上限
        # 且自带 (role,title) 30min 冷却与「先投递后记账」，异常也不会中断本轮。
        # 标题必须带失败详情：_notify_role 的 30min 冷却按 (role, title) 命中，
        # 一旦把标题换成稳定键，同一 (role,wf,step) 的**另一种失败原因**在 30min
        # 内会被整条吞掉（连 bus 都不写）——实测 test_tick_paths 的 schema 用例先
        # 占了键，verify 用例的消息再也发不出去，主分支直接变红。频率控制交给
        # _gate_alert 的退避（5/10/20/40/80min），不靠吞消息。
        # 标题契约要照抄 exit_condition.text_contains：模板要求的是
        # "<模板>/<step>-complete"，只写 "<step>-complete" 时角色照做也永不匹配。
        _tc = ((getattr(step, 'exit_condition', None) or {}).get('text_contains')
               or step.id + '-complete')
        # exit_messages 落库后，:426 tick rescue 先命中并在 :450 return，主路径 :1080
        # 再也走不到 → 重写声明不会被消费（实测 wf_37b76f7b32/s3 空转 1679 tick 到
        # 24h 回收）。但宽松推进 :868 时 exit_messages 尚未落库，改对标题重写仍有效。
        _tail = ("补/修上述产出即可（闸门每 tick 自动重评，重写完成声明不会推进本步）。"
                 if self._step_sd(run, step).get("exit_messages") else
                 f"补齐上述产出后重新写 bus 完成声明（标题必须含 {_tc}），否则本步无法推进。")
        try:
            self._notify_role(
                self._eff_role(run, step), msg,
                f"{run.workflow_name}/{step.id}（实例 {run.id}）闸门未通过：{_tail}")
        except Exception as _e:
            LOGGER.debug("gate prompt to role failed: %s", _e)

    @staticmethod
    def _gate_text_id(text: str) -> str:
        """失败文本的稳定身份：verify 的 stderr 每次都带时长/临时路径/行号，
        原文比较会把同一条失败当成"新原因"，退避永远不生效。"""
        return re.sub(r"(?<![A-Za-z])[0-9a-f]{6,}(?![A-Za-z])|\d+(?:\.\d+)?", "#", text)

    def _gate_alert(self, key: str, text: str, write) -> None:
        """闸门失败播报：新原因立即报；同一条按 5/10/20/40/80min 退避，第 6 次停发并升级。

        旧实现只做固定 5 分钟节流：同一条 verify/schema 失败每 5 分钟无限重发，
        实测 reviewer 四轮收到逐字相同的「缺少 REVIEW.md」告警，每轮重跑 3~14 次
        取证（30 次工具调用），零新增结论。失败原因变了（新文本）仍立即播报。
        """
        _now = time.time()
        _last = getattr(self, "_gate_alert_at", None)
        if _last is None:
            _last = self._gate_alert_at = {}
        if len(_last) > 1000:      # 常驻 daemon 防增长：只裁掉 4h 前的旧记录。
            _cut = _now - 14400    # 整体 clear 会让已停发的键重新武装（重发 6 轮）。
            for _k in [k for k, v in _last.items() if v[0] < _cut]:
                _last.pop(_k, None)
        _tid = self._gate_text_id(text)
        _rec = _last.get(key)
        if _rec and _rec[1] == _tid:
            _n = _rec[2]
            if _n >= 6:           # 已停发：只留升级记录，不再刷角色
                return
            if _now - _rec[0] < 300 * (2 ** min(_n - 1, 4)):
                return
            _n += 1
        else:
            _n = 1               # 新文本 = 新信息，立即播报
        _last[key] = (_now, _tid, _n)
        if _n >= 6:
            try:
                self._bb.write("blocker",
                    f"[workflow] 闸门连续 {_n} 次未通过，停止重复提示: {key}",
                    evidence=text[:2000], src="workflow_engine")
            except Exception as _e:
                LOGGER.debug("gate escalation write failed: %s", _e)
            return
        write()

    def _quality_gate_blocks(self, step: Step, run: WorkflowRun) -> bool:
        """exit_schema + verify 闸门：不通过写 blocker 并返回 True（调用方不推进）。

        三个调用点共用一把尺子：exit 匹配主路径、宽松推进（分类不匹配的兜底）、
        tick 抢救（exit_messages 已落库）。后两处此前直接 complete_step，文件契约
        缺了也照样算完成——下游拿不到产出才回头返工（7 天 6 次宽松推进实测）。
        verify 来自模板注册表（可信内部源），context 值已 shlex.quote 保护。
        """
        if step.exit_schema:
            ok, errs = self._validate_exit_schema(step, run)
            if not ok:
                _msg = f"[workflow] {run.workflow_name} {step.id} schema: {'; '.join(errs)}"
                self._gate_alert(f"{run.id}/{step.id}/schema", _msg,
                                 lambda: self._gate_prompt(run, step, _msg))
                self._sync_step_results(run.id, run.step_results)
                return True
        if step.verify:
            import shlex as _shlex
            ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
            vcmd = step.verify
            # 对 context 值做 shlex.quote，防止模板变量被 shell 解释
            for k, val in ctx.items():
                vcmd = vcmd.replace(f"{{{k}}}", _shlex.quote(str(val)))
            # verify 可能含 && / 管道，shlex.split 会把它们当参数 → shell=True
            try:
                ver = _engine_mod._sp.run(vcmd, shell=True, capture_output=True, timeout=30)
            except Exception as _e:
                # 闸门自身故障不算通过：按失败留痕交下一轮重试（同 schema 节流）
                _msg = f"[workflow] {run.workflow_name} {step.id} verify 执行异常: {_e}"
                self._gate_alert(f"{run.id}/{step.id}/verify", _msg,
                                 lambda: self._gate_prompt(run, step, _msg))
                self._sync_step_results(run.id, run.step_results)
                return True
            if ver.returncode != 0:
                _msg = f"[workflow] {run.workflow_name} {step.id} verify: {ver.stderr.decode()[:200] or 'failed'}"
                self._gate_alert(f"{run.id}/{step.id}/verify", _msg,
                                 lambda: self._gate_prompt(run, step, _msg))
                self._sync_step_results(run.id, run.step_results)
                return True
        # 闸门已通过 → 退避计数归零：否则同一条确定性失败第 6 次后对该实例
        # 永久静默，原因消失又复发时角色再也收不到提示。
        _ga = getattr(self, "_gate_alert_at", None)
        if _ga:
            _ga.pop(f"{run.id}/{step.id}/schema", None)
            _ga.pop(f"{run.id}/{step.id}/verify", None)
        return False

    def _validate_exit_schema(self, step: Step, run: WorkflowRun) -> tuple[bool, list[str]]:
        """校验 exit_schema 定义的文件约束。返回 (ok, error_list)。"""
        schema = step.exit_schema
        if not schema:
            return (True, [])
        # 校验**实际执行者**的 workspace：分流后产出落在该角色目录，
        # 按模板角色校验会必失败（或让陈旧文件误过）。
        ws = _CCS_WORKSPACES / self._eff_role(run, step)
        ws_real = os.path.realpath(ws)
        errs: list[str] = []

        def _inside(real: str) -> bool:
            # 必须带分隔符边界：startswith("/ws/dev") 会放行兄弟目录 /ws/devops
            # （本机存在 dev/devops、architect/architecture、research/researcher）。
            return real == ws_real or real.startswith(ws_real + os.sep)

        for req in schema.get("required", []):
            fpath = ws / req
            fpath_real = os.path.realpath(fpath)
            if not _inside(fpath_real):
                errs.append(f"路径越权: {req}")
                continue
            # 支持通配符: mc_*/ * → 检查 glob 匹配
            if "*" in req:
                matches = sorted(fpath.parent.glob(fpath.name))
                if not matches:
                    errs.append(f"缺少产出: {req} 无 glob 匹配")
            elif not fpath.exists():
                errs.append(f"缺少产出: {req}")

        for fname, props in schema.get("properties", {}).items():
            fpath = ws / fname
            # 旧实现只在 required 分支做了越界检查，properties 分支直接按
            # `ws / fname` 读内容：schema 写 `../devops/X.md` 或
            # `../../../../tmp/X.md` 即可让引擎把兄弟 workspace / 工作区外文件
            # 当本步产出校验（2026-10-21 实测 required 越界 + properties 无检查）。
            if not _inside(os.path.realpath(fpath)):
                errs.append(f"路径越权: {fname}")
                continue
            # 通配符一次解析：所有检查器共享匹配列表
            # 2s 容差：fs 时间戳精度与 time.time() 不保证有序，同一瞬间写出的
            # 文件 mtime 可能早于 run.created_at 数微秒（实测约 100µs 量级）。
            # 无容差会把"本步刚写的文件"判成历史遗留。
            _wf_floor = (getattr(run, 'created_at', 0) or 0) - 2
            if "*" in fname:
                import glob as _gl
                # `**` 才递归：角色常把产出写进 src//tools//cli（7 天 24 个 .py
                # 里 20 个不在根目录），非递归 glob 会把真产出判成没有。
                all_matches = [Path(m) for m in _gl.glob(str(fpath), recursive=True)]
                if "**" not in fname and _wf_floor > 0:
                    # 根目录没有"新"匹配才把子目录并进来（契约只说产出该类型文件，
                    # 没有目录语义；根目录 44 个旧 .py + 新产出在 src/ 是实测形态）。
                    # 并集只可能让新文件被看到，旧文件仍然因 mtime 落在 all_matches
                    # 里参与 _stale_only 判定，不会放松"历史文件不算产出"。
                    if not any(m.stat().st_mtime >= _wf_floor for m in all_matches):
                        seen = {str(m) for m in all_matches}
                        all_matches += [Path(m) for m in _gl.glob(
                            str(fpath.parent / "**" / fpath.name), recursive=True)
                            if str(m) not in seen]
                # 过滤：只检查工作流创建后被修改/新建的文件（本次任务产出），
                # 排除 workspace 历史遗留文件导致的 minLength/mustContain 误报
                # 计数检查（minCount）不依赖 matched——即使 matched 为空也需执行
                matched = [m for m in all_matches if m.stat().st_mtime >= _wf_floor] if _wf_floor > 0 else all_matches
            else:
                # 固定文件名同样过滤 mtime：历史文件（如 41 天前的 SCOUT_REPORT.md）
                # 只查 exists() 会把它当成本次产出，让 step 靠陈旧文件虚假通过。
                all_matches = [fpath] if fpath.exists() else []
                matched = [m for m in all_matches if m.stat().st_mtime >= _wf_floor] if _wf_floor > 0 else all_matches

            # 通配符有匹配但全是工作流创建前的旧文件 = 本步没有产出。
            # 旧实现把这种形态当"无本次产出可校验"而跳过内容检查，于是
            # `*.py` + minLength（无 minCount）的 schema 在零新文件时返回 ok：
            # 实测两条 engineer 子链 11:30-11:45 workspace 0 写入，11:39:39 仍
            # 判完成（各 469s），父步 DESIGN.md/*.py 契约同时未达成。
            _stale_only = bool(all_matches) and not matched and "*" in fname
            if not matched and fpath.is_dir():
                matched = []  # minFiles 走单独检查，不混入内容检查
            elif not matched and (_stale_only or not ("*" in fname and all_matches)):
                # 无匹配，或匹配到的全是历史文件：都算"本步没有产出"，内容判据全部失败。
                # 旧行为对第二种跳过内容检查 → 零新文件也通过；minCount 计数仍会执行。
                # 必须覆盖全部"内容判据"属性：漏掉任何一个，以它为唯一判据的
                # exit_schema 在文件完全不存在时也会通过（mustContainAnyOf 实测
                # 返回 ok=True，步骤直接盖戳完成）。
                for prop_key in ("minLength", "mustContain", "mustContainAnyOf",
                                 "mustContainUrl", "mustNotContain", "checksum",
                                 "maxAgeMinutes"):
                    if prop_key in props:
                        # 区分"从未存在"与"只有旧文件"：后者要么是角色改了文件名，
                        # 要么是 schema 写的模式和实际落盘位置对不上（子目录/分槽名），
                        # 两种都必须让超时 blocker 可读，否则只能靠猜。
                        errs.append(f"缺少产出: {fname}" + (
                            f"（{len(all_matches)} 个匹配均早于本实例创建）"
                            if _stale_only else ""))
                        break
                # minCount 必须在此处单独判：零匹配时 continue 会跳过下方计数，
                # fresh=0 的 schema 会凭空通过（实测 mc_zero_probe => (True, [])）。
                if "*" in fname and "minCount" in props:
                    errs.append(f"{fname} 本次产出 glob 匹配不足 (0<{props['minCount']})")
                continue

            if "minLength" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    if len(content) < props["minLength"]:
                        errs.append(f"{mf.name} 内容不足 ({len(content)}<{props['minLength']})")

            if "mustContain" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    missing = [kw for kw in props["mustContain"] if not _token_hit(kw, content)]
                    if missing:
                        errs.append(f"{mf.name} 缺少必需内容: {missing}")
            if "mustNotContain" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    forbidden = [kw for kw in props["mustNotContain"] if kw in content]
                    if forbidden:
                        errs.append(f"{mf.name} 含禁止内容（可能为历史遗留）: {forbidden}")
            if "mustContainAnyOf" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    hit = [kw for kw in props["mustContainAnyOf"] if kw in content]
                    if not hit:
                        errs.append(f"{mf.name} 缺少评估深度证据: 必须含其一 {props['mustContainAnyOf']}")

            if "mustContainUrl" in props and props["mustContainUrl"]:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    if not re.search(r'https?://', content):
                        errs.append(f"{mf.name} 缺少 URL 链接 (mustContainUrl)")

            if "checksum" in props:
                import hashlib as _hl
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace").encode()
                    actual = _hl.md5(content).hexdigest()
                    if actual != props["checksum"]:
                        errs.append(f"{mf.name} checksum mismatch: {actual[:8]}≠{props['checksum'][:8]}")

            if "minFiles" in props:
                if fpath.is_dir():
                    ext = props.get("extension", "")
                    files = [f for f in fpath.iterdir() if f.is_file() and (not ext or f.name.endswith(ext))]
                    if len(files) < props["minFiles"]:
                        ext_label = ext or "文件"
                        errs.append(f"{fname} {ext_label} 文件不足 ({len(files)}<{props['minFiles']})")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "minCount" in props:
                # 复用上方 matched（已按 run.created_at-2s 过滤）：旧实现用
                # _gl.glob() 全量计数，历史遗留文件会让本次 fresh=0 也判通过。
                if "*" in fname:
                    if len(matched) < props["minCount"]:
                        errs.append(f"{fname} 本次产出 glob 匹配不足 ({len(matched)}<{props['minCount']})")
                elif fpath.exists():
                    pass  # file exists but no glob → ok

            if "maxAgeMinutes" in props:
                if fpath.exists():
                    age_min = (time.time() - fpath.stat().st_mtime) / 60
                    if age_min > props["maxAgeMinutes"]:
                        errs.append(f"{fname} 文件过旧 ({age_min:.0f}min > {props['maxAgeMinutes']}min)，疑似非本任务产出")
                elif "*" in fname:
                    import glob as _gl
                    matches = _gl.glob(str(fpath))
                    for m in matches:
                        age_min = (time.time() - os.path.getmtime(m)) / 60
                        if age_min > props["maxAgeMinutes"]:
                            errs.append(f"{m} 文件过旧 ({age_min:.0f}min > {props['maxAgeMinutes']}min)，疑似非本任务产出")

        return (len(errs) == 0, errs)

    def _refusal_note(self, refused, wf_id: str, step_id: str) -> bool:
        """拒产/拒审消息被排除出完成判定时在 bus 留痕。

        标题写死（不含计数/时间戳）→ bus 按标题 UPSERT，同一处反复命中只留一行。
        """
        try:
            self._bb.write("blocker",
                f"[refusal] {wf_id or '?'} {step_id or '?'} 自述拒绝不算完成",
                evidence="\n".join(f"#{f.id} {f.src}: {f.t[:160]}" for f in refused[-3:]),
                src="workflow_engine")
            return True
        except Exception as _e:
            LOGGER.debug("refusal note failed: %s", _e)
            return False

    def _check_exit(self, cat: str, src_filter: str, text_filter: str, created_after: float = None,
                    wf_id: str = "", wait_roles: list = None,
                    eff_role: str = "", own_wf_id: str = "",
                    template: str = "", step_id: str = "") -> tuple:
        """查 exit 是否有匹配消息。返回 (timestamp, matched_msgs)。

        wf_id 非空时过滤"消息必须含当前工作流实例 id"——防止新工作流
        匹配旧工作流的同分类消息（2026-08-09 观测：wf_4c51d890c082/s2
        匹配到 wf_6e338fd3 的 s2-complete，跨实例串消息）。
        防止旧工作流的同分类消息（不带 wf_id）串入 require_wf 步骤
        （2026-08-10 观测：wf_7f37 s1 匹配到 wf_cde32 的 s1-complete）。
        wait_roles（协作门）：非空时要求所有声明角色都写了匹配消息才返回匹配。
        多角色协作（仲裁模式）核心：等 qa 和 reviewer 都产出 test_report 才推进。

        src=claude 双路径语义（2026-08-11 codex 审查）：
        - 主查询路径（limit=200）：放行 src=claude —— bus CLI 写消息默认 src=claude，
          角色未显式指定时消息仍可被匹配（兜底）。
        - 补查路径（per-role limit=200）：不放行 src=claude —— 否则一条 claude-src
          消息可同时满足所有缺失角色，协作门被一票绕过。
        生产实测（2026-08-11 blackboard.db）：协作角色用角色名写 bus（test_report
        分类 6 条全部 src=qa），精确匹配可行。
        """
        # 自述拒绝不是产出（R163：wf_bc4112d2767f 的 s3/s4 以「拒产/无法对齐」标题通过
        # 契约 text_contains 被判完成，下游拿不到契约件才回头返工）。丢掉的消息必须在
        # bus 留痕：返工链第一来源就是这里的 exit_messages，静默丢弃会让"为什么没推进"
        # 无据可查（只剩通用 timeout 文案）。
        facts, _refused = [], []
        _wf_id_note = own_wf_id or wf_id
        for f in (self._bb.read(cat=cat, limit=200) if cat else self._bb.read(limit=200)):
            # 显式负向终态是结论，不是自述拒绝（ADR-2026-10-28 §3 已否决「用措辞猜意图」）。
            # 负向结论的自然表述恰恰带拒产词：实测
            # is_refusal_not_verdict('s1-codex-blocked: 接口无法对齐 3 处') 为 True，
            # 不豁免就静默吞掉，步骤退回等超时 —— 正是该 ADR 要消除的失败模式。
            # 只豁免**本步**的 blocked 标记：只认裸标记会让
            # `s3-complete: 无法交付（s2-codex-blocked 未清）` 这种替别步记账的拒产
            # 也跳过 R163，那正是该过滤要拦的（R163 原意：自述拒绝不是产出）。
            # approved 侧不豁免：「无法交付，未写 s1-codex-approved」是自述失败，
            # 一并放行就把它当成审查通过。
            _ft = str(f.t or "")
            if _refusal_not_verdict(f.t) and (step_id + _BLOCKED_MARK) not in _ft:
                # 只记"本来会判完成"的那几条：标题过契约 token，且**确实是本实例的**。
                # 不收窄到本实例时，同类别并发链里 B 实例的拒产会被 A 的 tick 抓走，
                # 留痕写成 [refusal] <A>/<A's step>，把排障指到错的链上（codex 第四轮 P1）。
                _t = str(f.t or "")  # SQLite 动态类型：int/bytes 直接做 in 会抛 TypeError
                if (not text_filter or text_filter in _t) and (
                        not _wf_id_note or _mentions(f, _wf_id_note)):
                    _refused.append(f)
                continue
            facts.append(f)
        if _refused:
            # 同一批拒绝消息每 tick 都会重新命中：进程内去重，别让 bus 的 log 表按 tick 膨胀。
            _key = (_wf_id_note, step_id, tuple(sorted(f.id for f in _refused)))  # read 按 ts DESC 无 tie-breaker，同 ts 顺序会变
            _noted = getattr(self, "_refusal_noted", None)
            if _noted is None:
                _noted = self._refusal_noted = set()
            if _key not in _noted:
                if len(_noted) > 500:
                    _noted.clear()
                # 只有写成功才记账：写失败被吞掉时下次 tick 还有机会重试（codex 第三轮）
                if self._refusal_note(_refused, _wf_id_note, step_id):
                    _noted.add(_key)
        # 屏障锚定「首次写入」而非 facts.ts：facts.t 是 UNIQUE，同文本重写走 UPSERT
        # 会把 ts/cat 原地抬高（blackboard_legacy.py:238-242）。实测 1698 条 fact 被重写、
        # 36 条 exit_message 因此从屏障前移到屏障后 → 旧消息复活被后序步骤消费。
        # log 表留存每次写入，取 MIN(ts) 即真实产生时刻。
        _birth = {}
        if created_after:
            _ids = [f.id for f in facts]
            if _ids:
                _ph = ",".join("?" * len(_ids))
                try:
                    # log 表在 blackboard.db，不是 lifecycle 的 workflows.db
                    with self._bb._conn() as _c:
                        for _row in _c.execute(
                                f"SELECT fid, MIN(ts) AS b FROM log WHERE typ IN ('write','add','upd') "
                                f"AND fid IN ({_ph}) GROUP BY fid", tuple(_ids)):
                            _birth[_row["fid"]] = _row["b"]
                except Exception:
                    # 查不到首次写入时退化为 facts.ts（旧行为），不阻断匹配
                    LOGGER.debug("birth-time lookup failed; falling back to facts.ts")
        earliest = 0.0
        matched = []
        # 同模板兄弟实例：同一父链下跑同一模板的其它实例 id。给软归属守卫用，
        # 判断「消息里并列的本实例 id 是不是同模板兄弟」。只查一次，失败则空集
        # （守卫退化为旧行为：放行）。
        _siblings: set = set()
        if own_wf_id and not wf_id:
            try:
                for _r in self._lifecycle.query(
                        "SELECT s.instance_id FROM workflow_instances s "
                        "JOIN workflow_instances o ON o.instance_id=? "
                        "WHERE s.template_id=o.template_id "
                        "AND s.parent_wf_id IS o.parent_wf_id "
                        "AND s.instance_id<>o.instance_id LIMIT 50", (own_wf_id,)):
                    _siblings.add(_r["instance_id"])
            except Exception as _e:
                LOGGER.debug("sibling lookup failed %s: %s", str(own_wf_id)[:12], _e)
        for f in facts:
            # src 精确匹配；src=claude（bus 默认值）视为角色未设 src，不排除
            # wait_roles 协作门：协作角色的消息也放行（否则 src 过滤器拦死门控）
            # eff_role：engineer↔pg 等价角色池分流后实际执行者可能是 pg，但模板
            # src_filter 写死 engineer → pg 的 code_fix 永不匹配（实测
            # wf_sub_4e63ad633d39/s1 的 effective_roles={"s1":"pg"} 卡 48min）。
            # 模板语义是"由该步的开发角色完成"，故实际执行者同等放行。
            if src_filter and f.src != src_filter and f.src != "claude":
                _src_ok = bool(eff_role) and f.src == eff_role
                if not _src_ok and (not wait_roles or f.src not in wait_roles):
                    continue
            if text_filter and text_filter not in str(f.t or ""):
                continue
            if created_after and _birth.get(f.id, f.ts) < created_after:
                continue
            # wf 归属校验（require_wf 严格模式）：消息必须提到本实例 id。
            # 旧实现只在标题含 'wf_' 时才校验 —— 标题不带 wf_ 的消息一律放行，
            # 于是两条同模板的返工链（wf_rw_fd0826/wf_rw_439523）被同一条
            # pg 的 code_fix 同时闭合，第二条的文档缺陷未修即通过（实测 #415562）。
            # 现在：require_wf=true 时“不含本 id”就是一个不匹配，而不是放行。
            # 归属边界统一由 _mentions 判定（标题+正文、完整 id）。
            # 严格归属：只认标题，与旧语义一致（正文顺带提及不放行）。
            if wf_id and not _mentions(f, wf_id, fields="t"):
                continue
            # 软归属（非 require_wf 步骤）：拦下"点名别的实例却声称完成了本步"的消息。
            # 裸匹配步骤过去没有归属校验，同模板并发链会互相闭合（#421072：
            # wf_sub_ad604 的 s2 完成消息闭合了 wf_sub_7afc1 的 s2，自有产出晚 7min）。
            # 不改成 require_wf=true 的原因：105/164 条历史裸匹配完成消息本就不带
            # 实例 id（no-op 巡检），硬校验会让它们全部超时。
            # 只读标题（f.t）不读 evidence：实测改读正文会新拦 47 条真实完成消息
            # （reviewer 30 / product_architect 6 / engineer 4 / devops 2 / writer 2 /
            # security_auditor 2 / knowledge_curator 1），它们的正文提到别实例 id 是
            # 记账不是串味 → 那些步骤会全卡到超时。判据边界待 product_architect 裁定。
            if not wf_id and _claims_own_step_and_foreign_instance(
                    f.t, own_wf_id, template, step_id, siblings=_siblings):
                continue
            # 负向终态的归属要更严：判错一次就把别人的链整条关成 failed（不可恢复），
            # 与 completed 的多放行一次不对称。消息点名了别的实例、却没点名本实例时，
            # 即便标题带本步的 blocked 标记，也不算本步结论 —— 实测同模板 5 条链
            # 可以并发卡在同一步，一条 `s1-codex-blocked: … (wf_other)` 会全杀掉。
            # 不带任何实例 id 的裸消息仍放行：注入文案与模板 D 段都没要求角色写实例 id，
            # 强制要求会让负向终态永不触发，退回等超时（ADR 要消除的正是这个）。
            if step_id and (step_id + _BLOCKED_MARK) in str(f.t or ""):
                _bid = set(_WF_ID_RE.findall(str(f.t or "")))
                if _bid and own_wf_id not in _bid:
                    continue

            # evidence 必须一并带上：驳回分类段【系统问题→maintainer】写在 evidence 里，
            # 旧实现只取 t（标题）→ _parse_review_issues 拿不到分类 → 返工链恒建不出
            # （实测生产 3/3 条真实 reviewer 驳回全部解析为空）。
            matched.append({"id": f.id, "text": f.t[:200],
                            "evidence": (getattr(f, "e", "") or "")[:2000],
                            "ts": f.ts, "src": f.src})
            if earliest == 0 or f.ts < earliest:
                earliest = f.ts
        # 多角色协作门：所有声明角色都有匹配消息才通过（仲裁模式）
        # 高流量分类下 50 条窗口可能滑掉早写入的协作消息 → 按角色补查（滚动窗口）
        # ponytail: per-role 补查限 200 条，极端流量仍可能滑出；升级路径：按角色+时间戳游标
        if wait_roles:
            have = {m["src"] for m in matched}
            missing = set(wait_roles) - have
            for _r in missing:
                _rfacts = self._bb.read(cat=cat, limit=200) if cat else self._bb.read(limit=200)
                _rfacts = [f for f in _rfacts if not _refusal_not_verdict(f.t)]  # 已在上面留痕，补查不再重复
                _rmsg = None
                for _f in _rfacts:
                    # 精确匹配目标角色 src；不放宽到 claude（否则一条
                    # claude-src 消息可满足所有缺失角色，削弱协作门）
                    if _f.src != _r:
                        continue
                    if text_filter and text_filter not in str(_f.t or ""):
                        continue
                    if created_after and _birth.get(_f.id, _f.ts) < created_after:
                        continue
                    # 与主路径同源：同用 _mentions（旧 "'wf_' in t" 会放行无 id 消息）。
                    if wf_id and not _mentions(_f, wf_id, fields="t"):
                        continue
                    _rmsg = {"id": _f.id, "text": _f.t[:200],
                             "evidence": (getattr(_f, "e", "") or "")[:2000],
                             "ts": _f.ts, "src": _f.src}
                    break
                if _rmsg is None:
                    return (0.0, [])  # 还有角色没产出，不推进
                matched.append(_rmsg)
                if earliest == 0 or _rmsg["ts"] < earliest:
                    earliest = _rmsg["ts"]
        return (earliest, matched)
