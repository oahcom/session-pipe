#!/usr/bin/env python3
"""WorkflowLifecycleMixin — 工作流级推进：僵尸回收、推进、子工作流、loop、重推、变量填充。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import filecmp
import json
import time
import uuid
import logging
import re
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from pipeflow.engine import Step, WorkflowDef
import review_gate as _review_gate  # 驳回词表唯一真相源，与 manager.py 共用
import paths as _paths_mod
from paths import CCS_CLI as _CCS_CLI
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")

# 评审显式标注「→角色」时，返工链改投该角色的模板。
# 只收真实出现过的对象（bus #413382/#415040/#415594）；未列角色回退按问题类型默认。
_REWORK_TPL_FOR_ROLE = {
    "maintainer": ("maintainer_health_monitor", "maintainer"),
    "engineer": ("engineer_feature", "engineer"),
    "pg": ("engineer_feature", "engineer"),
    "product_architect": ("architect_full_design", "product_architect"),
}


def _value_audit_failed(text: str) -> bool:
    """判定价值审计结论是否为 FAIL —— 仅认结构化结论，杜绝模板文字误伤。

    反馈用：文档首行严格等于 FAIL/REJECT，或存在 `verdict: FAIL|REJECT` 字段。
    旧实现 `"FAIL" in _text.upper()` 把"说明 FAIL 情形"的模板说明段误判为结论，
    导致 food_chain_loop 第 3 轮永久死锁（blocker #292530）。
    """
    if not text:
        return False
    _lines = text.strip().splitlines()
    if _lines and _lines[0].strip().upper() in ("FAIL", "REJECT"):
        return True
    # 扫完前 50 行：首个 verdict 行不一定是结论（模板里可能先写
    # "verdict: PASS|FAIL" 说明再由真正的结论行覆盖）。任一 FAIL/REJECT 即判负。
    for _line in _lines[:50]:
        if _line.lower().startswith("verdict:"):
            _v = _line.split(":", 1)[1].strip().upper()
            if _v in ("FAIL", "REJECT"):
                return True
    return False


def _resolve_value_gate_paths(vg_file: str, vg_ws: str) -> list:
    """把 value_gate.file 解析为 (workspace, 文件名) 列表。

    file 字段实际是逗号分隔的多文件名（food_chain_loop 配
    "VALUE_AUDIT.md,delivery_chain_summary.txt"），旧代码直接当单个文件名拼路径，
    拼出 "VALUE_AUDIT.md,delivery_chain_summary.txt" 这样的不存在路径 →
    值门禁永远不通过 → 收尾判定退化到子链扫描分支。此处按逗号切分并去空白。
    """
    if not vg_file or not vg_ws:
        return []
    return [(_CCS_WORKSPACES / vg_ws, _fn.strip())
            for _fn in str(vg_file).split(",") if _fn.strip()]



def _live_subflow(lm, step_sd: dict) -> bool:
    """当前步骤挂的子链是否还活着。

    父步的活由子链承担：子链运行期间父步没有新产出是正常态，不是卡死。
    缺此判据时，子链结束的瞬间冻结的 queued_since 让父步「停滞 = 从建
    子链至今」，整条父链被回收（实测 8 例，138min 子链工时无接收方）。
    """
    # ponytail: 每候选一次主键查询（当轮 running 实例 <100），与旧实现同频（旧版在
    # 条件 4 内做同一条）；量级上升时改主 SELECT LEFT JOIN 子链一行带出 status。
    _sf = step_sd.get("subflow_id") or ""
    if not _sf:
        return False
    from pipeflow.role_lifecycle import _ACTIVE_WF_STATUSES
    _rows = lm.query(
        "SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,))
    return bool(_rows) and _rows[0]["status"] in _ACTIVE_WF_STATUSES


def _bus_evidence_ready(engine, row: dict) -> bool:
    """回收前最后一道检查：bus 上是否已有满足本步 exit_condition 的完成消息。

    回收器此前只认 step_results 的 exit_messages，而 status 恒空的步骤（角色忙/
    投递失败）根本没有该字段——完成消息早已在 bus 也照判"卡死"（实测 closer s2
    在上一步完成后 50 分钟才写出完成消息，途中被条件 4 判死，子链连烧 3 条）。

    判据复用 _has_evidence，与 engine.run_once 的完成证据门同源，不新开口径。
    只放行不闭合：下一 tick 由证据门置 notified，统一走 exit_schema/verify。
    """
    _cur = row.get("current_step_id") or ""
    try:
        # 模板取内存注册表（JSON 优先、每 tick 热加载）而非 workflow_templates 表：
        # 表里只有 _upsert_template 落过的模板，测试/新模板查不到会静默放行回收。
        _wf = engine._workflows.get(row.get("template_id") or "")
        _step = next((s for s in _wf.steps if s.id == _cur), None) if _wf else None
        if _step is None:
            return False
        _ts, _ = engine._has_evidence(row, _step)
        if _ts:
            LOGGER.info("stale-reclaim 拦下 %s/%s：bus 完成证据已就位 (ts=%.0f)",
                        str(row.get("instance_id", ""))[:12], _cur, _ts)
            return True
    except Exception as _e:
        # 探测失败按无证据处理：不让一次查询异常把回收变成永久豁免
        LOGGER.debug("stale-reclaim evidence check failed %s: %s",
                     row.get("instance_id"), _e)
    return False


def _role_producing_recently(role: str, window: float = 300.0) -> bool:
    """角色的会话转录最近 window 秒内是否在写入 —— 还在干活的强信号。

    为什么 pane 静默不足以判死：Claude 深度推理时不产生终端输出，而 review 类
    任务本身就是长推理（读代码 / 跑 gate / 写报告），pane 静默 5 分钟是常态。
    实测 21:12:49 auto-cancel 掉 wf_sub_8de33cd1559e 时，reviewer 的转录在持续
    写入（mtime 21:06:17），只是 pane 静默 465s > PANE_ACTIVE_WINDOW(300s)。
    template=reviewer_pr_review 历史 cancelled 占 22%（5/23）。

    转录文件在推理与每次工具调用时均写入，比 pane 更贴近在不在工作。
    同源思路见 role_lifecycle._recently_producing（bus 产出判活）与
    survival_monitor.py:230 的同款修法注释 —— 本处将同一原则补到判死路径。

    探测不可用（目录缺失 / 无转录 / stat 失败）时返回 False：调用方把它当
    「另一路证据」与 pane/DB 判据并列（见 _cleanup_stale_workflows 条件 4），
    返回 True 会让离线角色永不被回收，只能等 24h 全局阈值。代价是目录被
    临时移走时会误杀一条排队实例 —— 可重试，且远比永久占位轻。
    """
    if not role:
        return False
    try:
        # 目录名由工作区路径转义而来：ccs-workspaces-product-architect（下划线→连字符）。
        # 不做替换时带下划线的角色（product_architect 等）恒找不到目录 → 恒 False，
        # 第二判活信号对这些角色完全失效（实测 2026-10-20，本机 projects/ 下无下划线变体）。
        _slug = role.replace("_", "-")
        _d = Path.home() / ".claude" / "projects" / f"-home-administrator-ccs-workspaces-{_slug}"
        if not _d.is_dir():
            return False
        _mtime = max((f.stat().st_mtime for f in _d.glob("*.jsonl")), default=0.0)
        return bool(_mtime) and (time.time() - _mtime) < window
    except OSError as _e:
        # 目录存在但不可读（权限/IO）时，静默 False 会让「探测失败」与
        # 「确实没产出」无法区分。至少留痕，便于事后归因误杀。
        LOGGER.debug("role_producing_recently(%s) 探测失败: %s", role, _e)
        return False


def _resolve_loop_max_iter(max_iter: int, value_gate_file: str, wf_id: str, logger) -> int:
    """解循环迭代上限。

    正常路径 max_iter>0 或配了 value_gate 时原样返回（food_chain_loop 属后者，不受影响）。
    仅当「无迭代上限(max_iter<=0) 且 无价值门禁」才兜底降到 1，堵住无限自旋事故
    (bus #336367: maintainer 健康巡检 max_iterations=0 且无 value_gate → 永久刷 HEALTH_CHECK.md)。
    ponytail: 兜底值 1 故意不做成 config.yaml 可调——把它设回 0 会复活原事故。
    """
    if (max_iter or 0) <= 0 and not value_gate_file:  # ponytail: None/0 等均视为「无上限」
        logger.info("wf %s loop: 防御性兜底 max_iter=1 (无上限且无 value_gate)", wf_id[:8])
        return 1
    return max_iter or 0


class WorkflowLifecycleMixin:
    """WorkflowEngine 的工作流生命周期 mixin。"""

    def _cleanup_orphan_tasks(self, lm) -> int:
        """回收孤儿任务：落库了但 workflow_instance 从未建成的残骸。

        成因是 client.py 的 Gate 顺序缺陷（校验旧 assignee → 落库 → 对齐 → 二次
        校验失败）；该缺陷已修，但存量的 39 条 created 孤儿无任何回收路径——
        _scan_tasks 用 INNER JOIN workflow_instances、_cleanup_stale_workflows
        只扫实例表，两者都看不见它们。只增不减会污染背压计数。

        判据保守：非终态 + 无任何实例 + 超过 1h。终态任务不动（它们是正常历史）。
        """
        _where = ("status NOT IN ('completed','failed','cancelled') "
                  "AND current_workflow_id IS NULL AND created_at < ? "
                  "AND NOT EXISTS (SELECT 1 FROM workflow_instances wi "
                  "                WHERE wi.task_id = tasks.task_id)")
        try:
            # 先数后改：LifecycleManager.execute 不返回 cursor，拿不到 rowcount。
            n = lm.query_one(f"SELECT COUNT(*) FROM tasks WHERE {_where}",
                             (time.time() - 3600,))
            n = n[0] if n else 0
            if n:
                lm.execute(f"UPDATE tasks SET status='cancelled', updated_at=? WHERE {_where}",
                           (time.time(), time.time() - 3600))
                LOGGER.info("孤儿任务回收: %d 条（落库但无实例）", n)
            return n
        except Exception as _e:
            LOGGER.warning("孤儿任务回收失败: %s", _e)
            return 0


    def _cleanup_stale_workflows(self, lm):
        """自动取消僵尸 workflow。

        3 级回收策略:
          - 开发/测试模板（_ZOMBIE_TEMPLATES）> 2h → cancel
          - 所有模板 > 24h → cancel（step_done_ready 等待审批 → 72h）
          - 有 timeout_count >= 6（约 3 小时超时）→ cancel
        step_done_ready 是"步骤完成等待审批"的正常状态，用更长阈值避免
        24h 无人审批即被误杀（可能是跨周末的手工审批）。
        """
        from pipeflow.engine import ZOMBIE_TEMPLATES as _ZOMBIE_TEMPLATES
        now = time.time()
        _recycled: list[tuple[str, str, str]] = []  # (template, wf_id, reason) —— 循环外汇总通知
        try:
            for row in lm.query(
                # current_step_id 必须选：:137/:148 的 d.get("current_step_id", "")
                # 依赖它，漏选则恒为 "" → 抢救已产出步骤的分支与条件4 双双恒不触发
                "SELECT instance_id, template_id, created_at, step_results, status, "
                "current_step_id, assignee, effective_roles "
                "FROM workflow_instances WHERE status IN ('running', 'step_done_ready')"
            ):
                d = dict(row)
                wf_id = d["instance_id"]
                tpl = d.get("template_id", "?")
                created = d.get("created_at", 0)
                age_h = (now - created) / 3600 if created else 0
                sr = json.loads(d.get("step_results") or "{}")

                max_tc = max(
                    (v.get("timeout_count", 0) for v in sr.values() if isinstance(v, dict)),
                    default=0,
                )

                # 最小存活时间保护：创建于 5 分钟之内的 workflow 不自动回收。
                # 防止"刚通知角色就被 stale cleanup 误杀"（bus #158145：32s 被 cancel）。
                age_s = now - created if created else 0
                if age_s < 300:
                    continue

                # 当前行状态：step_done_ready 等待审批用更长阈值 72h
                _st = d.get("status", "running")
                _global_limit_h = 72 if _st == "step_done_ready" else 24

                # 食链循环工作流豁免：loop.enabled 的长跑工作流跨轮次间隙可能静默 >24h，
                # 用 72h 回收阈值（与 step_done_ready 一致）防误杀。loop 配置仅存内存
                # `_workflows` 中，DB templates 表无此列。
                # ponytail: 72h 天花板；升级路径→加 loop.max_duration_hours 覆盖模板级阈值
                _loop_wf = self._workflows.get(tpl) if hasattr(self, "_workflows") else None
                if _loop_wf is not None:
                    # 默认必须为 False：24 个模板无 loop 字段（loop=None → {}），默认 True 会让
                    # 它们全部命中豁免 → 24h 兜底从未对普通模板生效（实测 28/28 全部拿到 72h）。
                    # 豁免的本意只给显式声明 loop.enabled 的长跑工作流（food_chain_loop 等）。
                    _loop_cfg = _loop_wf.loop or {} if isinstance(_loop_wf.loop, dict) else {}
                    if _loop_cfg.get("enabled", False):
                        _global_limit_h = 72

                _auto_cancel_at = 6
                reason = ""
                # 判死来源：只有"空闲/卡死"类条件（2 测试模板超时、4 停滞）允许被
                # 完成证据放行；终态(0)/超时耗尽(1)/全局上限(3) 是硬上限，证据再齐
                # 也照回收——否则一条 exit_schema 永远不过的实例会永久免死。
                _cond = ""
                # 条件0: 状态对账——当前步骤已是终态(cancelled/failed)但行级仍 running。
                # 语义上该实例不会再推进，属"僵尸行"。这类残留由历史 cancel_step 缺陷造成
                # （只写步骤级 status 不写行级，已修），但不修存量会永久占用 running 计数
                # 并污染积压告警。实测 2026-10-17：21 台卡 5.5-6.1h，是全库 running 最大块。
                _cur_id = d.get("current_step_id", "")
                _cur_step = sr.get(_cur_id, {}) if isinstance(sr, dict) else {}
                if (isinstance(_cur_step, dict)
                        and _cur_step.get("status") in ("cancelled", "failed")):
                    _cond = "0"
                    reason = (f"当前步骤 {_cur_id} 已终态"
                              f"({_cur_step.get('status')})但行级仍 running")
                # 子链活着 → 父步没有新产出是正常态，条件 1-4 一律不判死。
                # 条件 0 例外：步骤自身已终态，子链本就该随之终止。缺这道门时
                # 24h 兜底会连同活着的子链一起被级联杀掉（在途产出直接丢失）。
                elif _live_subflow(lm, _cur_step if isinstance(_cur_step, dict) else {}):
                    pass
                # 条件1: 超时重试耗尽（对 step_done_ready 无意义——等待审批期间
                # 角色已产出，timeout_count 不会递增；保留该分支对二者都无害）
                elif max_tc >= _auto_cancel_at:
                    _cond = "1"
                    reason = f"timeout_count={max_tc}（已达自动回收阈值{_auto_cancel_at}）"
                # 条件2: 测试/开发模板超2小时
                elif tpl in _ZOMBIE_TEMPLATES and age_h > 2:
                    _cond = "2"
                    reason = f"测试模板 {tpl} 运行{age_h:.0f}h > 2h阈值"
                # 条件3: 全局阈值（running=24h，step_done_ready=72h）
                elif age_h > _global_limit_h:
                    _cond = "3"
                    reason = f"{_st} 运行{age_h:.0f}h > {_global_limit_h}h全局阈值"
                # 条件4: running 且当前步骤确已停滞 >1h
                # 关键修正：仅空 step_results 不足以判 stale——长任务 /goal 步骤完成前 step_results
                # 本就为空，1h 墙钟误杀正常推进中的角色（bus #728c6e430 误报根因）。
                # 故要求 timeout_count >= 1（引擎真实超时标记）或 queued >= _Q 排队降级标记。
                #
                # 为何加 queued 判据：同角色多实例并发时全部走"排队"分支——该分支只递增
                # queued 且重置 ts，不递增 timeout_count（step_handler.py:297-305）。而
                # liveness busy 判据自带 `timeout_count < 3` 前提，两个互判 busy 的实例
                # timeout_count 都恒为 0 → 条件4 永不成立 → 永不回收。实测 95 台
                # WL-01@s1 全部 status=notified/queued=2/tc 缺失，卡死 4.5h 无人回收。
                #
                # 停滞时长以**当前步骤**的 ts 计（引擎每 tick 重推时刷新），不用实例年龄：
                # 实例跑完 5 步到 s4 时创建已 >1h，但当前步骤可能刚排队几分钟——用 age_h
                # 会误杀（独立审查 2026-10-17 实测反例：age_h=1.5 的实例刚排队 2 次即被回收）。
                # 且 queued 阈值必须与 step_handler 的升级链对齐（queued>=3 降级为
                # timeout_count、>=6 通知 coordinator、>=10 auto-cancel）——在此另设
                # 更低阈值会绕过那条链，使升级与通知形同虚设。
                elif _st == "running" and age_h > 1:
                    _cur = d.get("current_step_id", "")
                    _cur_sr = sr.get(_cur, {}) if isinstance(sr, dict) else {}
                    if isinstance(_cur_sr, dict):
                        _cur_tc = _cur_sr.get("timeout_count", 0)
                        _cur_q = _cur_sr.get("queued", 0) or 0
                        # 停滞时长起点优先用 queued_since（首次排队时刻，不随 ts 重置），
                        # 退回 ts（每次重推刷新），再退回实例创建时间。
                        # 为什么必须用 queued_since：排队分支每轮都重置 ts，用 ts 算停滞
                        # 恒为"刚排队"→ 卡死实例永不满足阈值（这正是原 98 台卡 4.5h 的机制）。
                        _step_ts = (_cur_sr.get("queued_since") or _cur_sr.get("ts")
                                    or created or 0)
                        _stall_h = (now - _step_ts) / 3600 if _step_ts else 0
                        from pipeflow.step_handler import (
                            _QUEUE_DEGRADE_AT as _Q_DEGRADE, _queue_budget_sec)
                        # tc>=1 是引擎已确认超时；queued 达降级阈值且真实停滞 >1h 属同等情况。
                        # 但 tc 递增会把 ts 重置为 now（step_handler 给角色响应窗口那次重写），
                        # _stall_h 上限恒为一个超时窗（<1h）—— tc 分支结构上看不到真实停滞时长，
                        # 角色持续工作时该分支仍成立（实测误杀正在写报告的 reviewer）。
                        # 判死需两个信号**同时**静默：pane 无输出 AND 会话转录无写入。
                        # 只看 pane 会误杀正在深度推理的角色（见 _role_producing_recently）。
                        # None=探测不可用、空 assignee=解析失败，都不算角色已死。
                        # 排队判死必须与 step_handler 同源：用排队预算（步声明 ×
                        # 可重试次数，下限 30min），不用固定 1h。实测 232 台回收的
                        # 停滞中位正好 1.0h、p90 1.4h，而预算 150min —— 排队者在预算
                        # 40% 处被判死，杀掉的是"还没轮到自己"，不是"卡死"。
                        _wf4 = self._workflows.get(tpl)
                        _st4 = next((s for s in _wf4.steps if s.id == _cur), None) if _wf4 else None
                        # 模板不在内存（DB-only）时退回旧阈值 3600s：放宽改动面，
                        # 不在未建模的场景上引入更严的判死。
                        _q_sec = (_queue_budget_sec(
                            _st4.exit_condition or {},
                            max(int(getattr(_st4, "max_retries", 0) or 0) + 3, 4),
                            parent_minutes=self._parent_step_minutes(d))
                            if _st4 else 3600)
                        if _cur_tc >= 1 or (_cur_q >= _Q_DEGRADE
                                            and _stall_h * 3600 >= _q_sec):
                            from liveness import tmux_pane_active
                            _role = (d.get("assignee") or "").strip()
                            if (_role and tmux_pane_active(_role) is False
                                    and not _role_producing_recently(_role)):
                                _cond = "4"
                                _reason_step = _cur_sr.get("step", _cur)
                                reason = (f"running 状态 {_reason_step} 无推进记录 "
                                          f"步骤停滞 {_stall_h:.1f}h / 实例 {age_h:.1f}h "
                                          f"(timeout_count={_cur_tc}, queued={_cur_q})")

                if reason:
                    # 完成证据优先于回收：status 空的步骤没有 exit_messages，只看落库
                    # 字段会把"已完成"判成"卡死"。证据门在 run_once 的实例循环里、
                    # 排在本回收之后 —— 不在此放行，它永远没机会消费这条消息。
                    # 只放行空闲类判死（2/4）：超时耗尽与全局上限是硬上限，必须保留。
                    if _cond in ("2", "4") and _bus_evidence_ready(self, d):
                        continue
                    LOGGER.warning("auto-cancel: %s (template=%s) — %s", wf_id, tpl, reason)
                    # ── 回收前抢救: 当前步骤已有 exit_messages(产出证据) → 先尝试闭合 ──
                    try:
                        _cur = d.get("current_step_id", "")
                        _sdata = sr.get(_cur, {}) if isinstance(sr, dict) else {}
                        if isinstance(_sdata, dict) and _sdata.get("exit_messages"):
                            # 用户要求"每个步骤"都提醒 → stale rescue 也补发完整性提醒
                            # steps 定义在 workflow_templates 表（workflow_instances 无 steps 列）
                            try:
                                _step_tpl = None
                                _tmpl = lm.query(
                                    "SELECT steps_json FROM workflow_templates WHERE template_id=?",
                                    (tpl,)
                                )
                                if _tmpl:
                                    _tsteps = json.loads(_tmpl[0]["steps_json"] or "[]")
                                    for _s in _tsteps:
                                        if _s.get("id") == _cur:
                                            _step_tpl = _s
                                            break
                                if _step_tpl:
                                    from pipeflow.integrity import send_bypass_reminder as _sbr
                                    from types import SimpleNamespace as _SN
                                    _sbr_run = _SN(id=wf_id, step_results=sr)
                                    _sbr_step = _SN(
                                        id=_cur,
                                        target_role=_step_tpl.get("target_role", ""),
                                        prompt_template=_step_tpl.get("prompt_template", ""),
                                    )
                                    def _persist_sr(*args):
                                        # 真落库：bypass flag 写入 DB（否则 daemon 重启后重复提醒）
                                        try:
                                            _sr_merged = sr  # 已在 send_bypass_reminder 内 setdefault 更新
                                            lm.execute(
                                                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                                (json.dumps(_sr_merged, ensure_ascii=False), wf_id)
                                            )
                                        except Exception:
                                            LOGGER.debug("stale rescue persist sr failed for %s", wf_id)
                                    _sbr(_sbr_run, _sbr_step, "stale_rescue",
                                         send_to_role=self._send_to_role,
                                         sync_step_results=_persist_sr)
                            except Exception:
                                LOGGER.debug("stale rescue integrity send failed for %s", wf_id)
                            self._lifecycle.complete_step(wf_id, _cur)
                            LOGGER.info("stale rescue: %s step %s exit_messages 已就绪 → 已闭合",
                                        wf_id, _cur)
                            continue
                    except Exception as _re:
                        LOGGER.debug("stale rescue failed for %s: %s", wf_id, _re)
                    try:
                        # 走 close_wf 而非裸 UPDATE：回收路径漏掉了 task 状态同步与
                        # 子链级联，任务永远停在 running、子链继续占角色（实测 76 条）。
                        # reason 用条件分支算出的具体原因（timeout_count / 阈值 / 步骤已终态）：
                        # 写死 "stale rescue" 只能证明"被回收"，说不出为什么被回收。
                        self._lifecycle.close_wf(wf_id, status="cancelled",
                                                 reason=f"stale rescue: {reason}")
                        # 这条 action='cancelled' 行是 writer/TROUBLESHOOTING.md 与排障脚本
                        # 的既有查询锚点，保留不动。
                        lm.execute(
                            "INSERT INTO workflow_logs (workflow_instance_id, task_id, action, actor, detail, ts) "
                            "VALUES (?, ?, 'cancelled', 'workflow_engine', ?, ?)",
                            (wf_id, "", f"僵尸回收: {reason}", time.time())
                        )
                        # 逐台留痕已在 workflow_logs（可查），但通知必须聚合：
                        # 批量回收时逐台 _notify_role 会在同一秒写 N 条 blocker+ccs send，
                        # 制造总线尖峰（实测 2026-10-17 06:30 一次回收 21 台 → 22 条同秒
                        # blocker）。改为循环外汇总一条，各台原因按模板归并。
                        _recycled.append((tpl, wf_id, reason))
                    except Exception as e:
                        # warning 而非 debug：close_wf 级联失败会整体回滚，父链留下、
                        # 子链继续占角色；静默重试时无人知道孤儿还在。
                        LOGGER.warning("stale cleanup write failed for %s: %s", wf_id, e)
            if _recycled:
                _by_tpl: dict[str, list[str]] = {}
                for _t, _w, _r in _recycled:
                    _by_tpl.setdefault(_t, []).append(_w)
                _summary = "; ".join(
                    f"{_t}×{len(_ws)} ({_ws[0]}…)" for _t, _ws in sorted(_by_tpl.items()))
                self._notify_role(
                    "maintainer",
                    f"僵尸工作流自动回收: {len(_recycled)} 台",
                    f"按模板: {_summary}\n"
                    f"首台原因示例: {_recycled[0][2]}\n"
                    f"逐台明细见 workflow_logs(action=cancelled)。")
            LOGGER.info("stale cleanup done")
        except Exception as e:
            LOGGER.warning("stale cleanup error: %s", e)

    def _advance_production_wf(self, wf_id: str, lm, wf_name: str, step_id: str,
                                results: dict = None):
        """推进生产工作流到下一步，并通知目标角色。

        锁内只做短 DB 事务，subprocess/IO 全部移到锁外，避免持 RLock 最长
        30s 导致其他线程/进程 busy_timeout=5s 抛 database is locked。
        两阶段:
          1) with lm._conn_tx(): 读守卫、子流程检查、UPDATE+commit（锁内）
          2) 锁外: 完成后部署 + loop 处理；推进后 _send_to_role + 建子工作流
        """
        wf = self._workflows.get(wf_name)
        if not wf:
            return
        steps = wf.steps
        idx = next((i for i, s in enumerate(steps) if s.id == step_id), -1)
        if idx < 0:
            return
        is_last = idx + 1 >= len(steps)
        next_step = None if is_last else steps[idx + 1]

        # 返工门禁（在推进前判）：评审被驳回 → 落库 rejected 后返回，
        # 后续由 engine.py 的 rejected 消费分支接管——建返工链、等回流、
        # 逾限升级，都在那一处实现（本函数不直接建链，避免两套实现分歧）。
        # 若不落库只 return，step 仍是 step_done_ready → 消费分支永不进入 →
        # 返工链无回流入口（codex D10/D20）。
        # （回归守卫 tests/test_workcycle_e2e.py::test_gate_rejection_blocks_advance）。
        # 姊妹路径 lifecycle/manager._advance_unsafe 有同款门禁 —— 两条推进
        # 实现必须都装，只装一条的另一条会放行驳回（实测 wf_98a18e738d）。
        # 关键词 "fix" 不列入：正常文案高频出现（bugfix/prefix/fixture）会误判。
        #
        # 不限定 step.type（R41, 2026-10-19）：旧实现只查 review/gate，实测近 800
        # 实例中 21/35 条驳回票因此被吞——单一 type 白名单让 handoff/notify/single
        # 的驳回无人消费（如 wf_3565289ca783/s3 handoff 判定"未通过"后，s4/s5 照常
        # 推进至 completed）。驳回语义由 exit_messages 文本判定，与 type 无关。
        # 末步仍不拦——无 next_step 可退回，且无 rework 承接方。
        if not is_last:
            _step_sr = ((results or {}).get(step_id, {}) or {})
            # evidence 也要拼入：分类段在其中，只拼 text 会让 _has_issue_classification
            # 恒为 False → 门禁 fail-open 放行真实驳回（与 _create_rework_subflow 同源缺陷）。
            _msgs = " ".join(
                (m.get("text", "") or "") + " " + (m.get("evidence", "") or "")
                for m in (_step_sr.get("exit_messages") or [])
                if isinstance(m, dict)
            ).lower()

            # 已确认的步骤不再判驳回：exit_messages 是第一轮的驳回文本，审批确认
            # 不会清除它 → 同一份文本被二次判成驳回。与 manager._advance_unsafe
            # 同一缺陷（实测 wf_256be862ea86/s3 确认后卡死 5h）。
            _confirmed = bool(_step_sr.get("confirmed_at"))
            if not _confirmed and _review_gate.is_rejected(_msgs):
                LOGGER.warning("review rejected: %s/%s → 返工门禁触发，不推进",
                               wf_id[:8], step_id)
                # 无条件落库 rejected，不再按「有无问题分类」fail-open 放行。
                # 旧实现把「解析不出【系统问题】/【业务问题】」当作放行依据，理由是
                # 「硬拦会两不落 → 静默卡死」。该前提已不成立：engine 的 rejected
                # 消费分支（engine.py:1146+）对无分类情形有完整出口——重投原步，
                # 封顶 _MAX_REJECTED_RETRY(3) 后升级 coordinator。放行才是无出口的
                # 那条，实测 19 例真实驳回被据此放行（WL-01/wf_cd4a9c21df 等），
                # 产物一字未改却让下游拿不存在的东西继续做。
                # 分类只决定返工链建成哪一类；建不出链不是通过审查的理由。
                if not self._has_issue_classification(_msgs):
                    LOGGER.warning(
                        "rejected 但产出无【系统问题】/【业务问题】分类 %s/%s"
                        " → 交 rejected 消费分支（重投原步，不推进）", wf_id[:8], step_id)
                # 持久化 rejected 再返回：只 return 不落库时 step 仍是
                # step_done_ready → 下轮又走这里、engine 的 rejected 消费
                # 分支永远看不见 → 返工链建了但无回流入口（codex D10/D20）。
                _rej = dict(((results or {}).get(step_id) or {}))
                _rej.update({"status": "rejected", "rejected_at": time.time(),
                             "rejected_by": "workflow_engine"})
                results = {**(results or {}), step_id: _rej}
                lm.execute(
                    "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                    (json.dumps(results, ensure_ascii=False), wf_id))
                return

        completed_now = False
        _vg_fail_wf = ""   # 价值审计 FAIL 收尾标记：bus 写入放在锁外阶段
        try:
            with lm._conn_tx() as conn:
                # 防重闭合守卫：launcher wf complete 全量覆写 step_results 会把已
                # completed 的工作流打回 step_done_ready，这里拒绝二次推进。
                _row = conn.execute(
                    "SELECT status FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                if _row and _row["status"] == "completed":
                    LOGGER.debug("advance skipped: %s already completed", wf_id)
                    return

                if is_last:
                    # 最后一步 → 检查所有子工作流是否全部完成
                    # 从 DB 实时读取 step_results（不依赖传入的 results 快照，避免
                    # 主循环与 _scan_tasks 双路径传入不同副本导致竞态误判）
                    _db_res = conn.execute(
                        "SELECT step_results FROM workflow_instances WHERE instance_id=?", (wf_id,)
                    ).fetchone()
                    # 值门禁对齐：末位步骤（如 food_chain_loop/s7）的真实完成证据是
                    # 角色写进 workspace 的审计文件（lr 写 VALUE_AUDIT.md），而非 bus。
                    # exit_condition 等 bus 时角色只写文件 → 步骤永远 step_done_ready 卡死。
                    # 此处复用 _handle_wf_loop 的文件门禁判定：文件存在且非 FAIL → 允许 close。
                    _vg = (wf.loop or {}).get("value_gate") or {}
                    _vg_passed = False
                    _vg_failed = False
                    for _vg_dir, _vg_name in _resolve_value_gate_paths(
                            _vg.get("file", ""), _vg.get("workspace", "")):
                        _audit_path = _vg_dir / _vg_name
                        if not _audit_path.exists():
                            continue
                        if _value_audit_failed(
                                _audit_path.read_text(encoding="utf-8", errors="ignore")):
                            _vg_failed = True
                            break
                        _vg_passed = True
                        LOGGER.info("wf %s [%s] 末位值门禁 PASS（文件 %s），允许收尾",
                                    wf_id[:8], wf_name, _audit_path.name)
                        # 不 break：value_gate.file 可逗号分隔多份审计，任一 FAIL
                        # 都要判失败（food_chain_loop 旧配置即多文件形态）。
                    if _vg_passed and not _vg_failed:
                        # 任一文件 FAIL 即整体失败，不得被先扫到的 PASS 覆盖
                        # 直接跳到 close（跳过子链扫描——子链 cancelled 已在下面兼容）
                        LOGGER.info("wf %s [%s] 完成: %d 步", wf_id[:8], wf_name, len(steps))
                        self._lifecycle.close_wf(wf_id, status='completed',
                                                 reason="value gate PASS")
                        conn.commit()
                        completed_now = True
                        return
                    if _vg_failed:
                        # 审计明确写 FAIL → 本轮无收益，不能盖 completed。旧行为
                        # 落到下方子链扫描后照样 completed：实测 wf_5f5eebc881 于
                        # 05:29 收尾 completed，而 lr/VALUE_AUDIT.md 首行是 FAIL
                        # （"无实质收益；s5 测另一条工作流的产物"）——状态与审计
                        # 结论直接矛盾，下游据此以为收益已达成。交 coordinator 处置。
                        # 仅认「文件存在且判 FAIL」；文件缺失走原有子链扫描语义，
                        # 缺失本身由 exit_schema 的 required 约束兜底。
                        LOGGER.warning("wf %s [%s] 价值审计 FAIL → 按 failed 收尾（不推进为 completed）",
                                       wf_id[:8], wf_name)
                        # close_wf 已内置子链级联（除 completed 外的终态），无需预调。
                        self._lifecycle.close_wf(wf_id, status='failed',
                                                 reason=f"value audit FAIL: {_audit_path.name}")
                        conn.commit()
                        _vg_fail_wf = wf_id
                        return
                    _all_subs_done = True
                    _forced: list = []  # 被强制取消的子链 (step_id, subflow_id, 原状态)
                    for _sid, _sdata in (json.loads(_db_res["step_results"]) if _db_res and _db_res["step_results"] else {}).items():
                        if not isinstance(_sdata, dict):
                            continue
                        _sf = _sdata.get("subflow_id", "")
                        if _sf:
                            _st = conn.execute("SELECT status, created_at FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                            if not _st:
                                # 子链不存在（创建失败）→ 直接阻止主链完成，等重试创建，
                                # 防"子链从未执行却完成"掩盖产出缺失（原 _st=None 时跳过宽限期直接完成）
                                _all_subs_done = False
                                break
                            if _st['status'] not in ('completed', 'step_done_ready'):
                                # 子链未完成：已超宽限期（5 分钟）→ 强制完成主链，不无限等待
                                # （2026-08-10 观测：lr 子链未跑完导致主链 s7 永远卡在收尾）
                                _sub_created = _st['created_at'] or 0
                                if time.time() - _sub_created < 300:
                                    _all_subs_done = False
                                    break
                                # 超宽限期 → 一并关闭子链，不留孤儿 running
                                # （codex P2：到期只关主链，子链继续通知角色/拉起 guard）
                                conn.execute(
                                    "UPDATE workflow_instances SET status='cancelled' "
                                    "WHERE instance_id=? AND status NOT IN ('completed')",
                                    (_sf,))
                                # 强制关闭子链 = 该步产出未达成的信号。原实现静默取消后
                                # 父链照判 completed，导致「父链完成」不蕴含「产出存在」
                                # （实测 wf_ae31fd1c4c11：4/6 产出物缺失仍算完成）。
                                # 此处记账 + 写 bus，让缺失可被外部观测（不改完成语义，
                                # 避免复活 2026-08-10「主链无限等待」事故）。
                                _forced.append((_sid, _sf, _st['status']))
                    # bus 写入必须在事务外：_bb.write 自带连接/事务，
                    # 在 _conn_tx 内调用会撞 "cannot start a transaction within a transaction"。
                    for _fsid, _fsf, _fst in _forced:
                        try:
                            self._bb.write(
                                "blocker",
                                f"[workflow] {wf_id[:12]}/{_fsid} 子链超时强制取消，"
                                f"该步产出未达成（子链 {_fsf[:12]} status={_fst}）",
                                src="workflow_engine",
                                evidence=f"父链 {wf_name} 末步收尾时子链超 5 分钟宽限未完成；"
                                         f"父链仍按原语义判 completed，但此步产出缺失。"
                                         f"请核查 {_fsid} 的产出物是否已写入 workspace。")
                            LOGGER.warning(
                                "wf %s/%s 子链 %s 超宽限强制取消（status=%s），产出可能缺失",
                                wf_id[:8], _fsid, _fsf[:12], _fst)
                        except Exception as _bwe:
                            LOGGER.debug("forced-subflow blocker 写入失败: %s", _bwe)
                    if not _all_subs_done:
                        return
                    LOGGER.info("wf %s [%s] 完成: %d 步", wf_id[:8], wf_name, len(steps))
                    self._lifecycle.close_wf(wf_id, status='completed',
                                             reason="all steps done")
                    conn.commit()
                    completed_now = True
                else:
                    # 推进到下一步
                    # 条件更新：仅当当前步骤仍是 step_id 时才推进（并发防重）
                    # assignee 必须一并更新：它只在建实例时设一次（首步角色），
                    # 不更新则背压计数（query DISTINCT assignee）与任务生成配额长期失真
                    # ——实测 7 个活跃实例 5 个不一致（71%），s2 要求 product_architect
                    # 却仍归 pm → pm 记 3 个活跃、架构师记 0 个。姊妹路径
                    # manager._advance_unsafe:1024 已在做这件事，此处是遗漏的另一半。
                    _adv = conn.execute(
                        "UPDATE workflow_instances SET current_step_id=?, assignee=? "
                        "WHERE instance_id=? AND current_step_id=?",
                        (next_step.id, next_step.target_role or None, wf_id, step_id))
                    if _adv.rowcount == 0:
                        LOGGER.debug("advance skipped: %s/%s 已被其他路径推进", wf_id[:12], step_id)
                        return
                    # 推进后行 status 归位 running，避免 _scan_tasks 路径推进后
                    # 主循环看到旧 step_done_ready 再次触发 advance
                    conn.execute("UPDATE workflow_instances SET status='running' WHERE instance_id=?",
                                (wf_id,))
                    new_results = dict(results or {})
                    # 旧步骤强制 completed：残留 notified/step_done_ready 会让
                    # _is_role_busy 误判角色忙 → 下棒角色被堵（与 _advance_unsafe 同源修复）
                    if isinstance(new_results.get(step_id), dict):
                        new_results[step_id]["status"] = "completed"
                    new_results[next_step.id] = {"status": "notified", "ts": time.time(), "notified_at": time.time(),
                                                  "poll_since": time.time(), "bus_anchor": time.time()}
                    conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                (json.dumps(new_results, ensure_ascii=False), wf_id))
                    # 通知目标角色（先在未提交事务里试跑 variable 检查——
                    # 未替换变量时回滚推进，DB 保持原状，避免步骤卡死在 'notified'
                    # 且角色收不到通知，timeout_count 递增到自动取消）
                    task = conn.execute(
                        "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                        (wf_id,)
                    ).fetchone()
                    task_title = task["title"] if task else wf_name
                    task_desc = task["description"] if task else ""
                    prompt = self._fill_prompt_vars(
                        next_step.prompt_template, next_step, wf_name,
                        task_title, task_desc, results, prev_step_id=step_id,
                        wf_id=wf_id)
                    if _engine_mod.UNMATCHED_VAR_RE.search(prompt):
                        unmatched = _engine_mod.UNMATCHED_VAR_RE.findall(prompt)
                        if conn.in_transaction:
                            conn.rollback()
                        LOGGER.warning("advance skip notify: %s/%s step=%s 含未替换变量 %s，已回滚推进",
                                      wf_id[:8], wf_name, next_step.id, unmatched)
                        return
                    conn.commit()
        except Exception as e:
            LOGGER.warning("_advance_production_wf failed %s: %s", wf_id, e)
            return

        # ── 锁外阶段：subprocess / IO 不再持锁 ──
        if _vg_fail_wf:
            # bus 必须在锁外写：Blackboard 用独立连接、timeout=10s，锁内写会把
            # 同进程其他推进/收尾一起阻塞。结论只有 coordinator/lr 能处置，
            # 而他们读 bus 不读 journal。
            try:
                self._bb.write(
                    "blocker",
                    f"[workflow] {_vg_fail_wf[:12]}/{wf_name} 价值审计 FAIL，"
                    f"已按 failed 收尾（不是 completed）",
                    src="workflow_engine",
                    evidence=f"价值门禁文件 {_vg.get('file')} 判定 FAIL。"
                             f"本轮无收益，需 coordinator 决定重排或降级需求。")
            except Exception as _bwe:
                LOGGER.debug("value-gate blocker 写入失败: %s", _bwe)
            return
        if completed_now:
            # 部署自动闭环：把产出 cli/*.py 安装到 ~/.hermes/bin/，让工具可被
            # cron/daemon/角色直接调用（"有实际收益"的最后一环——部署）。
            self._deploy_production_outputs(wf_id)
            self._bb.write("workflow",
                f"[workflow] {wf_name} 完成: {len(steps)} 步",
                src="workflow_engine")
            # 食物链循环：loop 配置存在 → 检查是否继续下一轮
            # 子 agent 价值审计文件存在且标记 FAIL → 停止循环（收益不足）
            # 否则重启生产者角色（思路干净）并启动新迭代
            if wf.loop:
                self._handle_wf_loop(wf, wf_id, lm)
            return

        # 产出审计：记录该步骤产出文件 + git 改动，供回溯追溯
        try:
            _done_step = steps[idx]  # 刚完成的步骤
            self._record_step_output(wf_id, _done_step, results.get(_done_step.id, {}))
        except Exception as _audit_err:
            LOGGER.debug("step_output audit failed %s/%s: %s", wf_id[:8], step_id, _audit_err)

        # （返工门禁已前置到本函数上半部：驳回时不会走到这里。）

        # 变量完整 → 已提交推进，通知目标角色
        # 修复：_send_to_role 因角色忙/变量未替换返回 False 时，投递并未发生。
        # 原逻辑丢弃返回值直接盖戳 notified → 角色没收到任务却被判"已通知"，
        # 靠 timeout 兜底重推（bus #728c6e4307c1 根因）。此处据返回值回退 step
        # 状态为 pending（清 notified_at），让引擎下轮重新走 advance 重发。
        # 有子链的步骤：先建子链，由子链接管；建不出才回退投主链步骤。
        # 用户需求：「主链第一步生成二级工作流，二级工作流严格要求该角色完成任务」。
        if next_step.subflow_template and self._subflow_takeover(
                lm, wf_id, wf_name, next_step, idx, results, step_id):
            return  # 子链已接管：不投主链步骤

        # 实际执行角色：与 engine 首步同一套解析（实例 effective_roles 覆盖
        # 优先）—— 分流决定必须对推进步与首步一致，否则下游按模板角色判、
        # 产出落另一角色处 → exit 条件永不匹配。
        _eff_role = self._resolve_effective_role(
            {"instance_id": wf_id}, next_step, results)
        _delivered = self._send_to_role(_eff_role, prompt,
                                         wf_id=wf_id, step_id=next_step.id)
        # 用 `is not True` 而非 not："busy"（角色忙，truthy 字符串）必须同样走失败分支，
        # 否则会跳过 _mark_step_delivery_failed 直接往下推进，把"未投递"当"已投递"。
        # busy 与 False 同路径回退。原注释称"busy 是暂时态 → 下轮 advance 重发"，
        # 但该重发不存在：next_step 的状态在上面（:404）于 _send_to_role **之前**就已
        # commit 为 notified，而 _advance_production_wf 只在当前步骤
        # step_done_ready/completed 时被调（engine.py:854），下一步既已 notified，
        # run_once 的首发分支（`if not step_status`）不会重走 → 角色没收到却被判
        # 已通知，只剩 30min 超时重推兜底（codex 三方审查 P1-2，2026-10-17）。
        # _mark_step_delivery_failed 内部判据是 `status == "notified"`，恰好适用。
        if _delivered is not True:
            if _delivered == "busy":
                LOGGER.debug("advance skip: %s 角色忙，已回退 step 待下轮重推",
                             next_step.target_role)
            self._mark_step_delivery_failed(lm, wf_id, next_step.id)
            return


    def _subflow_takeover(self, lm, wf_id: str, wf_name: str, tgt_step,
                          idx: int, results: dict, prev_step_id: str) -> bool:
        """为即将投递的步骤创建二级工作流（子链）。True = 子链已接管该步。

        WHY 顺序：原实现把子链创建放在 _send_to_role **之后** → 角色刚被投递
        即判忙 → 紧随的 _is_role_busy 守卫必然命中 → 子链永不创建。实测
        11/12 条带 subflow 的链零子链（WL-full_dev_pipeline 历史最佳 0/6 步）。

        用户需求：「主链第一步生成二级工作流，二级工作流严格要求该角色完成
        任务」—— 主链步骤是容器，干活的是子链。首步（engine.py run_once 的
        `if not step_status` 分支）与推进步（_advance_production_wf）是两条
        独立投递路径，都需调用本方法。
        """
        try:
            # 防重：DB 中该步骤已有 subflow_id → 不重复创建
            # （否则每 tick 推进同一历史步骤都会新建 subflow，垃圾任务堆积）
            with lm._conn_tx() as conn2:
                _db_sr = conn2.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                    (wf_id,)).fetchone()
                _db_sdata = (json.loads(_db_sr["step_results"]) if _db_sr and _db_sr["step_results"]
                             else {}).get(tgt_step.id, {})
                if isinstance(_db_sdata, dict) and _db_sdata.get("subflow_id"):
                    # 子链终态不回流 → 交还父步自投，否则该步永久悬空。
                    # 裸 return 是 falsy，调用方会当"未接管"重复投父步 → 与子链抢
                    # 同一角色（实测 wf_5d6bf3bf79/s1 queued=241）。
                    #
                    # 用**活跃状态白名单**而非「非 failed/cancelled 即活着」：
                    # 生产步骤级 status 实测 7 种取值，delivery_blocked(233 条)是
                    # 投递连败/角色无响应的停滞态——它非 failed 也非 cancelled，若当
                    # 活着则父步永不自投，而子链要等 _recover_delivery_blocked 恢复
                    # （角色持续离线时永不恢复）→ 父步永久悬空。
                    _sub_row = conn2.execute(
                        "SELECT status FROM workflow_instances WHERE instance_id=?",
                        (_db_sdata["subflow_id"],)).fetchone()
                    if _sub_row and _sub_row["status"] in (
                            "pending", "running", "step_done_ready", "completed"):
                        return True
                # 指针不可信时的回退：子链失败重试会主动 pop 掉 subflow_id
                # （task_generator.py 的重建分支），此后上面那道防重失明，每轮
                # 新建一条兄弟链（实测 wf_bf5721557a/s1 同挂 cancelled 与 running）。
                # 权威链接 (parent_wf_id, subflow_source_step_id) 无人改写，用它兜底：
                # 已有活跃子链就写回指针并接管，而不是再造一条。
                #
                # 只认活跃状态：带上 completed 会让 R105 的「pop 指针 = 交还父步
                # 自投」失效——驳回重投时旧 completed 子链被重新写回，_scan_tasks
                # 读其末步驳回文案又压回 rejected，白烧 _MAX_REJECTED_RETRY 预算。
                # 排除 wf_rw_：返工链同样写 parent_wf_id+source_step_id，被这里认领
                # 会和 engine.py 的 rework_ids 通道双消费同一条链。
                _link = conn2.execute(
                    "SELECT instance_id FROM workflow_instances "
                    "WHERE parent_wf_id=? AND subflow_source_step_id=? "
                    "AND status IN ('pending','running','step_done_ready') "
                    "AND instance_id NOT LIKE 'wf_rw_%' "
                    "ORDER BY created_at DESC LIMIT 1", (wf_id, tgt_step.id)).fetchone()
                if _link:
                    if not isinstance(results.get(tgt_step.id), dict):
                        results[tgt_step.id] = {}
                    _psr = results[tgt_step.id]
                    if _psr.get("subflow_id") != _link["instance_id"]:
                        _psr["subflow_id"] = _link["instance_id"]
                        _psr["queued"] = 0
                        _psr.pop("queued_since", None)
                        conn2.execute(
                            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(results, ensure_ascii=False), wf_id))
                        conn2.commit()
                    return True
                # 防污染：目标角色已有其他活跃任务（子链 target 与主链一致），
                # 此时角色在执行主链步骤，子链创建只会双线占用角色 → 跳过创建。
                # 观测案例：pm 子链 queued=17（主链 s2 与子链 s1 同时推给 pm）。
                #
                # 判据（2026-10-17 修正）：只排除**本主链自身的占用**。
                # 沿用 exclude="" 会把"本主链刚投递本步"也算成忙 → 恒真 →
                # 子链永不创建（实测 11/12 条带 subflow 的链零子链，
                # WL-full_dev_pipeline 历史最佳 0/6 步）。
                # exclude_wf_id=wf_id：角色被**别的**工作流占用仍跳过（防争抢），
                # 仅本主链的占用不阻止。
                if self._is_role_busy(tgt_step.target_role, exclude_wf_id=wf_id,
                                      pane_fallback=False):
                    LOGGER.info("subflow skip: %s 忙于其他工作流，子链 %s 不创建",
                                tgt_step.target_role, tgt_step.subflow_template)
                    return
                # 并发上限：忙判据只看"当下是否在跑"，看不到"已经堆了多少条"。
                # 角色每空出来一瞬就建一条新子链，堆到 9 条 running（实测
                # product_architect）→ 6 个父链的 s1 全部排队等同一个角色。
                # 与 _can_assign_role 的 MAX_RUNNING_PER_ROLE 同源：子链创建是
                # 第二条派发路径，不走那里，所以必须在这里自己查。
                # 满额时 return（falsy）→ 父步自投；等现有子链消化完自然恢复建链。
                from pipeflow.task_generator import MAX_RUNNING_PER_ROLE
                _active_sub = conn2.execute(
                    "SELECT COUNT(*) c FROM workflow_instances "
                    "WHERE assignee=? AND parent_wf_id IS NOT NULL "
                    "AND status IN ('pending','running','step_done_ready')",
                    (tgt_step.target_role,)).fetchone()["c"]
                if _active_sub >= MAX_RUNNING_PER_ROLE:
                    LOGGER.info("subflow skip: %s 已有 %d 条活跃子链（上限 %d），%s 不创建",
                                tgt_step.target_role, _active_sub,
                                MAX_RUNNING_PER_ROLE, tgt_step.subflow_template)
                    return
                # 模板名兼容：同仓两条命名并存（裸名 architect_full_design /
                # WF- 前缀 WF-architect_full_design，见 manager.py:174 同款回退）。
                # 只按裸名查会让 WL-full_dev_pipeline 这类"裸名声明 + WF- 注册"
                # 的组合静默失配 → 子链永不创建 → 主链停在首步（实测该链
                # 历史最佳 0/6 步，7+ 实例全部 delivery_blocked @s1）。
                _sub_wf = (self._workflows.get(tgt_step.subflow_template)
                           or self._workflows.get(f"WF-{tgt_step.subflow_template}"))
                if _sub_wf and _sub_wf.steps and _sub_wf.steps[0].target_role == tgt_step.target_role:
                    _prev_done = ""
                    if idx >= 0:
                        _prev_done = results.get(prev_step_id, {}).get("completed_by", "")
                    if not _prev_done:
                        _prev_wf = conn2.execute("SELECT assigner FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
                        _prev_done = _prev_wf["assigner"] if _prev_wf else "pm"
                    _task_id = f"task_sub_{uuid.uuid4().hex[:8]}"
                    _wf_id = f"wf_sub_{uuid.uuid4().hex[:12]}"
                    # 子链任务描述 = 本步标题 + 上游交付证据。
                    # 上游证据必须带过来：子链 s1 的 prompt 只由 tasks.description
                    # 提供上下文，只写标题时角色收到「测试验证（qa）」这类空指令，
                    # 只能自己翻 DB/bus 猜测什么（实测 food_chain_loop/s5 → qa）。
                    # 不带父模板正文：未替换的 {title}/{description} 字面量会被
                    # engine 首步分支再次注入 → 守卫拦下 → delivery_blocked
                    # （实测 task_sub_80bd6be1 逐字含「标题: {title}」，blocker #397347）。
                    _up = (self._render_msgs(self._step_deliverables(prev_step_id, results), 1500)
                           if idx >= 0 else "")
                    # 父任务需求正文必须下传：子链 s1 的 prompt 主体是**子模板自己的**
                    # 正文（如 pm_requirements s1 硬编码「读 scout 报告，筛 1-3 个方向」），
                    # 不带用户需求时角色会把模板示例当成任务——实测「做一个高质量
                    # 产品落地页」被 pm 按 scout 情报方向立项。父任务描述是唯一的需求真相源。
                    _root = conn2.execute(
                        "SELECT t.title, t.description FROM tasks t "
                        "JOIN workflow_instances wi ON wi.task_id=t.task_id "
                        "WHERE wi.instance_id=?", (wf_id,)).fetchone()
                    _req = ""
                    if _root:
                        _req = (f"用户需求：{_root['title']}\n"
                                f"{(_root['description'] or '').strip()}")
                    _desc = "\n".join(x for x in (
                        _req,
                        f"本步：{tgt_step.title}",
                        (f"上游交付证据：\n{_up}" if _up else ""),
                    ) if x)
                    _desc = _engine_mod.UNMATCHED_VAR_RE.sub("", _desc)
                    conn2.execute("INSERT OR IGNORE INTO tasks (task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                        (_task_id, f"{wf_name}/{tgt_step.id}: {tgt_step.title}",
                         _desc,
                         _prev_done, tgt_step.target_role, time.time(), time.time(), tgt_step.subflow_template))
                    # 兼容迁移前的表（template_id 列可能缺失）——子工作流 tasks 行的 template_id
                    # 只做溯源用途，缺列时静默降级为 parent_task_id 记录父工作流（2026-08-10 观测）
                    try:
                        conn2.execute("SELECT template_id FROM tasks LIMIT 1")
                    except Exception:
                        conn2.execute("UPDATE tasks SET parent_task_id=? WHERE task_id=?", (wf_id, _task_id))
                        conn2.commit()
                    conn2.execute("INSERT OR IGNORE INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) VALUES (?,?,?,?,?,'pending','s1',?,?,?,?)",
                        (_wf_id, tgt_step.subflow_template, _task_id, _prev_done, tgt_step.target_role, json.dumps({}), time.time(), wf_id, tgt_step.id))
                    conn2.commit()
                    # 写 subflow_id 到该步骤的 step_results。用方法参数 results，
                    # **不是** _advance_production_wf 的局部 new_results —— 那是
                    # 另一个作用域，方法内引用会 NameError 被 except 吞掉，
                    # 导致子链创建静默失败（实测生产 journal：
                    # "subflow creation failed: name 'new_results' is not defined"）。
                    _psr = results.setdefault(tgt_step.id, {})
                    _psr["subflow_id"] = _wf_id
                    # 清排队账本（bus #425593 修法①）：子链接管后父步不再排队，
                    # 冻结的 queued_since 会被条件4 当成"父步停滞至今"→ 子链一
                    # 结束就整链判死（实测 8 例，138min 子链工时无接收方）。
                    # 与投递成功路径（engine.py 清 queued+queued_since）同语义。
                    _psr["queued"] = 0
                    _psr.pop("queued_since", None)
                    conn2.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id))
                    conn2.commit()
                    return True   # 子链已接管
        except Exception as _e:
            LOGGER.error("subflow creation failed for %s: %s", wf_id, _e)
        return False


    def _mark_step_delivery_failed(self, lm, wf_id: str, step_id: str):
        """投递失败回退：清掉 notified_at，把该 step 状态归 pending。

        锁外调用（_send_to_role 已被进程外 ccs send 占用）。重读 DB 当前
        step_results 仅改目标 step，避免覆盖其他 step 并发进度。引擎下轮
        _advance_production_wf 重新推进时角色若已空闲即正常投递。
        """
        try:
            with lm._conn_tx() as conn:
                _row = conn.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                if not _row or not _row["step_results"]:
                    return
                _sr = json.loads(_row["step_results"])
                _sd = _sr.get(step_id)
                if not isinstance(_sd, dict):
                    return
                # 仅当仍停在 notified（未被 tick 推进/完成）才回退，避免与并发 path 冲突
                if _sd.get("status") not in ("notified",):
                    return
                _sd.pop("notified_at", None)
                _sd.pop("poll_since", None)
                _sd.pop("bus_anchor", None)
                # status 置空（与 engine.run_once 的 if not step_status 首步重发分支对齐），
                # 引擎下轮对该 step 重新走 _send_to_role，角色若已空闲即正常投递。
                _sd["status"] = ""
                _sd["delivery_failed_at"] = time.time()
                conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                             (json.dumps(_sr, ensure_ascii=False), wf_id))
                conn.commit()
            LOGGER.warning("delivery_failed 回退 %s/%s → pending，下轮重发", wf_id[:8], step_id)
            self._bb.write("blocker",
                f"[workflow] {wf_id[:12]}/{step_id} 投递失败已回退 pending（角色忙或变量未替换）",
                evidence=f"下一步将重新 advance 重发", src="workflow_engine")
        except Exception as _e:
            LOGGER.warning("_mark_step_delivery_failed 失败 %s/%s: %s", wf_id[:8], step_id, _e)

    def _deploy_production_outputs(self, wf_id: str):
        """全链路完成后自动部署产出物到 ~/.hermes/bin/。

        扫描 engineer workspace 下的 cli/*.py，内容有变化才复制；
        有文件实际部署时写 bus cat=code_fix 通知维护者。
        """
        try:
            eng_ws = _CCS_WORKSPACES / "engineer" / "cli"
            if not eng_ws.exists():
                return
            deploy_dir = Path.home() / ".hermes" / "bin"
            deploy_dir.mkdir(parents=True, exist_ok=True)
            deployed = []
            for f in eng_ws.glob("*.py"):
                if f.name.startswith("_") or f.name.startswith("test_"):
                    continue
                import shutil
                dest = deploy_dir / f.name
                # 按内容字节比对，不能按 mtime：copy2 保留源 mtime，目标文件
                # 的 mtime 永远等于源的旧时间，判据会永久失真。原实现无条件
                # 复制 + 无条件广播：实测 [deploy] 共 182 条、全部是同一组 4 个
                # 文件；源文件最后变更 2026-10-17 12:34，此后 70 条皆为字节
                # 相同的空部署（bus #417679/#418378）。
                if dest.exists() and filecmp.cmp(str(f), str(dest), shallow=False):
                    continue
                shutil.copy2(str(f), str(dest))
                deployed.append(f.name)
            if deployed:
                self._bb.write("code_fix",
                    f"[deploy] {wf_id[:12]} 产出已部署: {', '.join(deployed)}",
                    evidence=f"路径: ~/.hermes/bin/{'/'.join(deployed)}",
                    src="workflow_engine")
                LOGGER.info("deployed %d outputs from %s", len(deployed), wf_id[:12])
        except Exception as e:
            LOGGER.debug("deploy_production_outputs failed: %s", e)

    def _create_rework_subflow(self, wf_id: str, lm, wf_name: str, review_step_id: str,
                               review_step, next_step, results: dict) -> bool:
        """review 被拒绝 → 解析【系统问题】/【业务问题】分类，分别创建修复子链。

        系统问题 → maintainer_health_monitor（维护者修引擎/模板缺陷）
        业务问题 → engineer_feature（开发修功能实现）

        reviewer 模板要求在完成消息中用 【系统问题】【业务问题】标记分类列出，
        引擎据此分别创建子链——同一份 review 可同时触发两条返工链。
        防重：review 步骤的 step_results 若已有 rework_ids → 不重复创建。

        返回是否真的创建了至少一条返工链。调用方据此决定要不要拦下推进：
        解析不出分类（非 review 模板的产出）时返回 False，让步骤正常推进，
        避免"既不返工也不推进"的静默卡死。
        """
        # 分类段在 bus evidence，而 exit_messages 只存了 text（标题）+ id。
        # 两条取用路径：新记录已带 evidence（step_handler 一并写入）；旧记录没有，
        # 按 id 回查 bus。只读 text 则分类永远解析不出（生产 3/3 复现）。
        _done_sr = results.get(review_step_id, {}) or {}
        _msgs_text = "\n".join(
            (m.get("text", "") or "") + "\n" + (m.get("evidence", "") or "")
            for m in _done_sr.get("exit_messages", [])
            if isinstance(m, dict)
        )
        # text 只有标题，分类段在 bus evidence —— 存量记录的 exit_messages
        # （写于本修复之前）不含 evidence 字段，只能按 id 回查 bus 补齐。
        # 必须放在解析调用之前：放在之后等于没补，实测仍 parse=False。
        if not self._has_issue_classification(_msgs_text):
            _ids = [m.get("id") for m in _done_sr.get("exit_messages", [])
                    if isinstance(m, dict) and m.get("id")]
            if _ids:
                try:
                    _msgs_text += "\n" + "\n".join(self._bb_evidence(_ids))
                except Exception as _ev_e:
                    LOGGER.debug("rework: bus evidence 回查失败 %s: %s", wf_id[:8], _ev_e)
        # 子链判定驳回时（task_generator 压 status=rejected + rejected_by=subflow_verdict），
        # 父步 exit_messages **必然为空**——那个路径有意只压状态、不搬子链文案
        # （搬了 tick-rescue 会把 rejected 当场抹成 completed）。于是上面两条取用路径
        # 都是空字符串 → 分类解析必空 → 建不出返工链 → 退回重投已完成子链 →
        # 产物未变必然再驳回 → 3 次封顶升级。实测 wf_bf5721557a/s4：子链已 completed，
        # 驳回文案在子链末步 exit_messages（bus #428488）且含完整【系统问题】段，
        # 返工链可从 1 条系统问题+4 条业务问题建出，却因读错来源全部丢失。
        # 只读子链，不往父步搬文案（理由同上）。
        if not self._has_issue_classification(_msgs_text):
            _msgs_text += self._subflow_last_step_text(wf_id, review_step_id, _done_sr)
        # 第四条来源：人工驳回只写 rejection_reason（manager.py:541），不进
        # exit_messages、也不在子链末步。缺此来源时 `wf confirm --reject` 的意见
        # 100% 解析不出 → 不建返工链 → 退回重投 → 3 次封顶后步骤永久停摆
        # （coordinator 2026-10-20 13:15 实录：retry=3、无返工链）。
        if not self._has_issue_classification(_msgs_text):
            _rej = str(_done_sr.get("rejection_reason") or "")
            if _rej:
                _msgs_text += "\n" + _rej
        # 从结构化分类中提取问题列表
        _sys_issues, _biz_issues, _sys_role, _biz_role = self._parse_review_issues(_msgs_text)
        if not _sys_issues and not _biz_issues:
            LOGGER.info("rework: %s/%s review 无分类问题，跳过返工", wf_id[:8], review_step_id)
            return False
        # 问题类型 → (模板, 角色, 分类标签, 评审注明的返工对象)
        _rework_map = [
            (_sys_issues, "maintainer_health_monitor", "maintainer", "系统/引擎问题", _sys_role),
            (_biz_issues, "engineer_feature", "engineer", "业务问题", _biz_role),
        ]
        try:
            with lm._conn_tx() as conn:
                _db_res = conn.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                if _db_res is None:
                    LOGGER.warning("rework: %s 不存在于 DB，跳过返工", wf_id[:8])
                    return False
                _sr = json.loads(_db_res["step_results"]) if _db_res["step_results"] else {}
                _review_sdata = _sr.get(review_step.id, {})
                _existing_reworks = set(
                    _review_sdata.get("rework_ids", []) if isinstance(_review_sdata, dict)
                    else ([_review_sdata["rework_id"]] if isinstance(_review_sdata, dict) and _review_sdata.get("rework_id") else [])
                )
                _new_reworks = []
                _created_tpls = set()  # 本轮已建链的模板：两类问题可能指向同一模板
                for _issues, _tpl, _role, _type_label, _target in _rework_map:
                    if not _issues:
                        continue
                    # 评审标了「→角色」时按它派发：真实驳回里【业务问题→product_architect】
                    # 指的是文档缺陷（DESIGN.md），旧实现一律派给 engineer_feature →
                    # 工程改代码、文档没动 → 必然再驳回（bus #415594）。
                    # 无对应模板的角色保持默认模板，仅在任务描述里注明。
                    _tpl_x, _role_x = _REWORK_TPL_FOR_ROLE.get(_target, ("", ""))
                    if _tpl_x:
                        _tpl, _role = _tpl_x, _role_x
                    # 逐类防重（P0 修复）：按模板判断该类 rework 是否已存在，
                    # 而非总数>=模板数——某类模板缺失时另一类不会无限堆积。
                    # _created_tpls 补本轮：两类问题可能都指向同一模板，只建一条。
                    _tpl_exists = any(
                        str(_rw).startswith("wf_rw_") and
                        self._rework_tpl_for(_rw) == _tpl
                        for _rw in _existing_reworks
                    )
                    if _tpl_exists or _tpl in _created_tpls:
                        LOGGER.info("rework 已存在 %s/%s 类型=%s，不重复创建",
                                    wf_id[:8], _type_label, _tpl)
                        continue
                    _sub_wf = self._workflows.get(_tpl)
                    if not _sub_wf or not _sub_wf.steps:
                        LOGGER.warning("rework: 模板 %s 不存在，跳过 %s", _tpl, _type_label)
                        continue
                    _task_id = f"task_rework_{uuid.uuid4().hex[:8]}"
                    _wf_id_rw = f"wf_rw_{uuid.uuid4().hex[:12]}"
                    _issue_summary = "; ".join(_issues[:5])  # 任务描述中列问题摘要（最多5条）
                    _review_title = getattr(review_step, "title", "review")
                    _task_desc = (
                        f"返工任务（{_type_label}）：[{review_step.id}] {_review_title} 审查被拒，"
                        f"修复后重新提交审查。\n"
                        f"问题清单:\n" + "\n".join(f"  - {i}" for i in _issues[:10]) +
                        f"\n\n评审意见见 bus cat=code_review。"
                    )
                    if _target and not _tpl_x:
                        _task_desc += (f"\n评审显式指向 {_target}，但无对应返工模板，"
                                       f"按问题类型派发。")
                    conn.execute(
                        "INSERT OR IGNORE INTO tasks "
                        "(task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) "
                        "VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                        (_task_id, f"{wf_name}/{review_step.id}: 返工({_type_label})", _task_desc,
                         "reviewer", _role, time.time(), time.time(), _tpl))
                    _created_tpls.add(_tpl)
                    conn.execute(
                        "INSERT OR IGNORE INTO workflow_instances "
                        "(instance_id,template_id,task_id,assigner,assignee,status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
                        "VALUES (?,?,?,?,?,'pending','s1',?,?,?,?)",
                        (_wf_id_rw, _tpl, _task_id, "reviewer", _role,
                         json.dumps({}), time.time(), wf_id, review_step.id))
                    _new_reworks.append(_wf_id_rw)
                    LOGGER.info("rework 子链: %s → %s (%s, %d 条问题)",
                                wf_id[:12], _wf_id_rw[:12], _type_label, len(_issues))
                # 更新 review 步骤的 rework_ids（list，支持多条）
                # CAS 写回（P2 并发修复）：WHERE 带上旧 step_results，行数=0 说明
                # 并发调用已改过（含我们刚创建的 rework）→ 重读合并，防后写覆盖
                if _new_reworks:
                    # 深拷贝: 浅拷贝 dict(_sr) 时 setdefault 返回同一嵌套 dict,
                    # 修改会污染 _sr → CAS WHERE 恒相等 → 并发防重分支永不触发
                    _new_sr = json.loads(json.dumps(_sr))
                    _existing = _review_sdata.get("rework_ids", []) if isinstance(_review_sdata, dict) else []
                    _merged = list(dict.fromkeys(_existing + _new_reworks))
                    _new_sr.setdefault(review_step.id, {})["rework_ids"] = _merged
                    _cur = conn.execute(
                        "UPDATE workflow_instances SET step_results=? "
                        "WHERE instance_id=? AND step_results=?",
                        (json.dumps(_new_sr, ensure_ascii=False), wf_id,
                         json.dumps(_sr, ensure_ascii=False)))
                    if _cur.rowcount == 0:
                        # 并发冲突：重读并合并（保留双方创建的 rework）
                        _fresh = conn.execute(
                            "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                            (wf_id,)).fetchone()
                        if _fresh and _fresh["step_results"]:
                            _fresh_sr = json.loads(_fresh["step_results"])
                            _fresh_sdata = _fresh_sr.get(review_step.id, {})
                            _fresh_ids = _fresh_sdata.get("rework_ids", []) if isinstance(_fresh_sdata, dict) else []
                            _fresh_sdata["rework_ids"] = list(dict.fromkeys(_fresh_ids + _new_reworks))
                            conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                         (json.dumps(_fresh_sr, ensure_ascii=False), wf_id))
                conn.commit()
            return bool(_new_reworks)
        except Exception as _e:
            # 建了一半的 tasks/instances 不能留在库里：不回滚时返工链处在
            # pending 但 rework_ids 未写回 → 后续每轮重试都会再建一条。
            # 走 lm.rollback() 而非 conn.rollback()：后者在 with 外已释放锁。
            try:
                lm.rollback()
            except Exception:
                pass
            LOGGER.warning("rework subflow creation failed %s: %s", wf_id[:8], _e)
            return False

    @staticmethod
    def _has_issue_classification(msgs_text: str) -> bool:
        """产出里是否含【系统问题】/【业务问题】分类段——返工路由的判据。

        走 _parse_review_issues 而非直接在文本里找标记：它已封装「带 →角色 流向
        标注」「行内叙述形态」等生产实际写法，另写一套匹配会与它漂移。
        """
        _sys, _biz, _, _ = WorkflowLifecycleMixin._parse_review_issues(msgs_text)
        return bool(_sys or _biz)

    def _bb_evidence(self, fact_ids: list) -> list:
        """按 fact id 从 bus 回查 evidence。

        exit_messages 只存 text+id（分类段在 evidence）——存量记录靠这条补齐；
        新记录由 step_handler._check_exit 一并写入，不走这里。
        """
        if not fact_ids:
            return []
        _ph = ",".join("?" * len(fact_ids))
        with self._bb._conn() as _c:
            _rows = _c.execute(
                f"SELECT e FROM facts WHERE id IN ({_ph})", tuple(fact_ids)).fetchall()
        return [r["e"] for r in _rows if r["e"]]

    def _subflow_last_step_text(self, wf_id: str, step_id: str, step_sdata: dict) -> str:
        """取父步名下子链**末步**的 exit_messages 文本（text + bus evidence）。

        只为返工分类解析服务：子链判定驳回时父步没有文案，真正的审查结论在子链
        末步。只读，不回写父步——回写会让 tick-rescue 把 rejected 抹成 completed。
        """
        _sf = (step_sdata or {}).get("subflow_id")
        if not _sf:
            return ""
        try:
            _rows = self._lifecycle.query(
                "SELECT current_step_id, step_results FROM workflow_instances "
                "WHERE instance_id=?", (_sf,))
        except Exception as _e:
            LOGGER.debug("rework: 子链 %s 读取失败: %s", _sf[:12], _e)
            return ""
        if not _rows:
            return ""
        _last_id = _rows[0]["current_step_id"]
        try:
            _all = json.loads(_rows[0]["step_results"] or "{}")
        except (TypeError, ValueError):
            return ""
        _last = _all.get(_last_id) if isinstance(_all, dict) else None
        if not isinstance(_last, dict):
            return ""
        _msgs = [m for m in (_last.get("exit_messages") or []) if isinstance(m, dict)]
        _text = "\n".join((m.get("text", "") or "") + "\n" + (m.get("evidence", "") or "")
                           for m in _msgs)
        if self._has_issue_classification(_text):
            return _text
        # 旧记录只有 text+id，分类段在 bus evidence
        _ids = [m.get("id") for m in _msgs if m.get("id")]
        if not _ids:
            return _text
        try:
            return _text + "\n" + "\n".join(self._bb_evidence(_ids))
        except Exception as _ev_e:
            LOGGER.debug("rework: 子链 evidence 回查失败 %s: %s", _sf[:12], _ev_e)
            return _text

    def _rework_tpl_for(self, rework_id: str) -> str:

        """从 DB 查 rework 工作流实例的模板（用于逐类防重判断）。"""
        try:
            rows = self._lifecycle.query(
                "SELECT template_id FROM workflow_instances WHERE instance_id=?",
                (rework_id,)
            )
            return rows[0]["template_id"] if rows else ""
        except Exception:
            return ""

    @staticmethod
    def _parse_review_issues(msgs_text: str) -> tuple:
        """从 review 产出文本里解析问题清单与显式返工对象。

        返回 (sys_issues, biz_issues, sys_role, biz_role)。role 是标记里
        「→maintainer」注明的返工对象，未注明则空串——调用方据此决定
        返工链派给谁。

        标记用前缀匹配：角色实际写【系统问题→maintainer】（带流向标注），
        旧实现要求全串「【系统问题】」独占一行 —— 生产 3/3 条真实驳回全部
        解析为空。另有行内叙述形态「系统问题: ...」（bus #415484，无括号
        也无列表前缀），按冒号切段整行作为一条问题。
        段内取条目先按行；单行压缩形态（bus evidence 常被压成一行）再按分号切。
        """
        _sys, _biz = [], []
        _roles = {"sys": "", "biz": ""}
        _hits = []
        for _mark, _kind, _br in (
                ("【系统问题", "sys", True), ("【业务问题", "biz", True),
                ("系统问题:", "sys", False), ("业务问题:", "biz", False),
                ("系统问题：", "sys", False), ("业务问题：", "biz", False)):
            _p = 0
            while True:
                _i = msgs_text.find(_mark, _p)
                if _i < 0:
                    break
                _hits.append((_i, _kind, _mark, _br))
                _p = _i + len(_mark)
        _hits.sort(key=lambda h: h[0])
        for _n, (_i, _kind, _mark, _br) in enumerate(_hits):
            _end = _hits[_n + 1][0] if _n + 1 < len(_hits) else len(msgs_text)
            _close = msgs_text.find("】", _i)
            _body = _i + len(_mark)
            if _br and 0 <= _close < _end:
                # 箭头角色写在标记与闭合括号之间：【业务问题→product_architect】
                _m = re.search(r"(?:→|->|=>)\s*([A-Za-z_][A-Za-z0-9_]*)",
                               msgs_text[_body:_close])
                if _m and not _roles[_kind]:
                    _r = _m.group(1).lower()
                    _roles[_kind] = {"pa": "product_architect",
                                     "arch": "product_architect"}.get(_r, _r)
                _body = _close + 1
            if not _br:
                # 行内叙述形态：整行到行尾（或下一个标记）算一条问题
                _eol = msgs_text.find("\n", _body)
                _end = min(_end, _eol if _eol >= 0 else len(msgs_text))
                _line = msgs_text[_body:_end].strip().lstrip(":：").strip()
                if len(_line) >= 10:
                    (_sys if _kind == "sys" else _biz).append(_line)
                continue
            _seglines = [x.strip() for x in msgs_text[_body:_end].splitlines() if x.strip()]
            if len(_seglines) == 1:
                _seglines = [x.strip() for x in re.split("[;；]", _seglines[0]) if x.strip()]
            for _line in _seglines:
                _l = _line.strip()
                if not _l or _l.startswith(("##", "#", "---", "|--", "｜--")):
                    continue
                _is_p = len(_l) > 1 and _l[0] == "P" and _l[1].isdigit()
                if not (_l.startswith(("-", "|", "｜", "·")) or _is_p):
                    continue
                _item = _l.lstrip("-|｜· ").strip()
                if len(_item) < 10:
                    continue
                (_sys if _kind == "sys" else _biz).append(_item)
        return (_sys, _biz, _roles["sys"], _roles["biz"])


    def _record_step_output(self, wf_id: str, step, step_data: dict):
        """记录步骤产出审计：产出文件列表 + 内容预览 + git 改动，写 workflow_logs。

        回溯追溯"哪个工作流/步骤产出了什么、改了什么代码"：
          - 产出文件: 该角色 workspace 下最新修改的 3 个文件（名称+大小+首行预览）
          - git 改动: ccs-workspaces git 仓库中近 10 分钟未提交的改动文件
            （排除系统文件 TASKS.md/CLAUDE.md，那是引擎自己写的）
        """
        if not step or not step_data:
            return
        role = getattr(step, "target_role", "") or ""
        detail = {}
        # 产出时间窗：从步骤完成时刻回溯 2 小时，只统计窗口内新增/修改的文件，
        # 避免角色 workspace 历史文件干扰（同一角色多任务共用 workspace）
        _step_ts = step_data.get("completed_at", step_data.get("ts", 0)) or 0
        _window_start = _step_ts - 2 * 3600 if _step_ts else 0
        if role:
            ws = _CCS_WORKSPACES / role
            if ws.exists():
                try:
                    _files = []
                    for f in (f for f in ws.rglob("*") if ".git" not in f.parts):
                        _st = f.stat()
                        if not f.is_file() or _st.st_size <= 0:
                            continue
                        _mtime = _st.st_mtime
                        if _window_start and _mtime < _window_start:
                            continue  # 超出本步骤时间窗，跳过
                        # 相对 _CCS_WORKSPACES 记录，与 home 解耦（测试环境）
                        try:
                            _name = str(f.relative_to(_CCS_WORKSPACES))
                        except ValueError:
                            _name = str(f.relative_to(Path.home()))
                        if _name.startswith("ccs-workspaces/"):
                            _name = _name[len("ccs-workspaces/"):]
                        if any(_s in _name for _s in ("TASKS.md", "CLAUDE.md", "base.md", "settings.json",
                                                       ".claude/", ".pytest_cache/", "__pycache__/", ".pyc")):
                            continue
                        _files.append({
                            "f": _name,
                            "size": _st.st_size,
                            "mtime": _mtime,
                            "preview": f.read_text(encoding="utf-8", errors="ignore")[:80].replace("\n", " "),
                        })
                    # 按修改时间降序取窗口内最新 3 个
                    _files.sort(key=lambda x: x["mtime"], reverse=True)
                    detail["outputs"] = _files[:3]
                except Exception as _fe:
                    LOGGER.debug("output scan failed: %s", _fe)
        # git 改动：ccs-workspaces 仓库未提交的改动（含 diffstat 行级统计）
        try:
            _git = _engine_mod._sp.run(
                ["git", "-C", str(_CCS_WORKSPACES),
                 "status", "--porcelain"],
                capture_output=True, text=True, timeout=10)
            if _git.returncode == 0:
                _changed_files = []
                for _line in _git.stdout.splitlines():
                    # porcelain 格式: "XY filename" 或 rename "XY old -> new"
                    if " -> " in _line:
                        _f = _line.split(" -> ", 1)[-1].strip()
                    else:
                        _f = _line[3:].strip()
                    if not _f or any(_s in _f for _s in ("TASKS.md", "CLAUDE.md", "base.md")):
                        continue
                    _changed_files.append(_f)
                # diffstat：受影响文件 + 行数增减（更精确的"改了什么"）
                _diffstat = []
                if _changed_files:
                    _df = _engine_mod._sp.run(
                        ["git", "-C", str(_CCS_WORKSPACES),
                         "diff", "--stat", "--no-color"],
                        capture_output=True, text=True, timeout=10)
                    if _df.returncode == 0 and _df.stdout.strip():
                        for _dl in _df.stdout.splitlines():
                            if "|" not in _dl:
                                continue
                            _parts = _dl.rsplit("|", 1)
                            _fname = _parts[0].strip()
                            _stat = _parts[1].strip().split()[0] if _parts[1].strip() else ""
                            if _fname and not _fname.startswith((" ", "-")):
                                _diffstat.append(f"{_fname} | {_stat}")
                    elif not _df.stdout.strip():
                        # 无 diff（可能全部 staged 或全新文件）→ 退化为仅文件名
                        _diffstat = _changed_files[:10]
                detail["git_changed"] = _changed_files[:10]
                if _diffstat:
                    detail["git_diffstat"] = _diffstat[:10]
        except Exception as _ge:
            LOGGER.debug("git scan failed: %s", _ge)
        if not detail.get("outputs") and not detail.get("git_changed"):
            return
        detail["step"] = getattr(step, "id", "")
        detail["title"] = getattr(step, "title", "")
        detail["role"] = role
        # 子工作流聚合：向上查 parent_wf（主食物链 ID），让主查询可聚合子链产出
        try:
            _parent_rows = self._lifecycle.query(
                "SELECT instance_id FROM workflow_instances WHERE step_results LIKE ?",
                (f'%"subflow_id": "{wf_id}"%',))
            if _parent_rows:
                _pwf = _parent_rows[0]["instance_id"]
                if isinstance(_pwf, str):
                    detail["parent_wf"] = _pwf[:12]
        except Exception:
            pass
        # exit_messages 摘要作为产出内容证据
        _msgs = []
        for m in (step_data.get("exit_messages") or []):
            if isinstance(m, dict):
                _msgs.append(str(m.get("text", ""))[:100])
        if _msgs:
            detail["exit_messages"] = _msgs[:3]
        try:
            self._bb.write("workflow",
                f"[step_output] {wf_id[:12]}/{detail['step']} {role} 产出审计",
                evidence=json.dumps(detail, ensure_ascii=False)[:1500],
                src="workflow_engine")
        except (TypeError, ValueError):
            # exit_messages 含不可序列化对象（测试 mock）→ 去掉后重试
            detail.pop("exit_messages", None)
            try:
                self._bb.write("workflow",
                    f"[step_output] {wf_id[:12]}/{detail['step']} {role} 产出审计",
                    evidence=json.dumps(detail, ensure_ascii=False)[:1500],
                    src="workflow_engine")
            except Exception as _we2:
                LOGGER.debug("step_output bus write failed: %s", _we2)
        except Exception as _we:
            LOGGER.debug("step_output bus write failed: %s", _we)

    def _handle_wf_loop(self, wf: WorkflowDef, wf_id: str, lm):
        """食物链循环处理：检查价值门禁 → 创建新迭代（锁内）→ 重启生产者（锁外）。

        loop 配置格式：
          {
            "enabled": true,
            "max_iterations": 3,          # 0=无限
            "producer_roles": ["scout"],   # 每轮重启的角色（思路干净）
            "value_gate": {
              "file": "VALUE_AUDIT.md",     # 审计角色产出物
              "workspace": "lr"             # 审计角色 workspace
            }
          }
        防御性兜底：max_iterations<=0 且无 value_gate 时强制降到 1 迭代
        （见 _resolve_loop_max_iter，堵住「0=∞ 且无门禁」无限自旋事故 bus #336367）。
        """
        loop_cfg = wf.loop or {}
        if not loop_cfg.get("enabled", True):
            return
        _producers = loop_cfg.get("producer_roles") or []
        created_new = False
        _iteration = 0
        _new_wf_id = ""
        try:
            with lm._conn_tx() as conn:
                # 从实例 context 读取当前迭代数
                _ctx_row = conn.execute(
                    "SELECT context FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                _ctx = json.loads(_ctx_row["context"]) if _ctx_row and _ctx_row["context"] else {}
                _iteration = int(_ctx.get("loop_iteration", 0) or 0)
                _max_iter = int(loop_cfg.get("max_iterations", 0) or 0)
                # 价值门禁：仅认结构化结论（首行 / verdict:标记），杜绝模板文字误伤。
                # ponytail: 旧实现 "FAIL" in _text.upper() 把"说明 FAIL 情形"的模板文字
                # 当作结论 FAIL，导致 food_chain_loop 第 3 轮永久死锁。升级：结论须出现在
                # 文档首行或 `verdict: FAIL|REJECT` 字段；后续可迁至 JSON/YAML 结构化产出。
                _vg = loop_cfg.get("value_gate") or {}
                _vg_file = _vg.get("file", "")
                _vg_ws = _vg.get("workspace", "")
                # 防御式兜底必须先于停止条件：原 raw _max_iter<=0 时 `>0` 守卫永不触发，
                # 若不先解算，兜底值 1 永远来不及生效，会复活 #336367 无限自旋事故。
                _max_iter = _resolve_loop_max_iter(_max_iter, _vg_file, wf_id, LOGGER)
                if _max_iter > 0 and _iteration >= _max_iter:
                    LOGGER.info("wf %s loop: 达到最大迭代 %d，停止", wf_id[:8], _max_iter)
                    self._bb.write("workflow",
                        f"[workflow] {wf.name} 循环结束: 达到最大迭代 {_max_iter}",
                        src="workflow_engine")
                    return
                for _vg_dir, _vg_name in _resolve_value_gate_paths(_vg_file, _vg_ws):
                    _audit_path = _vg_dir / _vg_name
                    if _audit_path.exists():
                        _text = _audit_path.read_text(encoding="utf-8", errors="ignore")
                        if _value_audit_failed(_text):
                            LOGGER.info("wf %s loop: 价值审计 FAIL，停止循环", wf_id[:8])
                            self._bb.write("workflow",
                                f"[workflow] {wf.name} 循环停止: 价值审计未达标",
                                src="workflow_engine")
                            return

                # 创建新迭代：新任务 + 新工作流实例（同一模板）——锁内 DB 写
                _task_id = f"task_loop_{uuid.uuid4().hex[:8]}"
                _new_wf_id = f"wf_{uuid.uuid4().hex[:12]}"
                _task_title = f"{wf.title} — 迭代 {_iteration + 1}"
                _task_desc = f"{wf.description}\n[loop] 迭代 {_iteration + 1}/{_max_iter or '∞'}"
                _now = time.time()
                try:
                    conn.execute(
                        "INSERT INTO tasks (task_id, title, description, assigner, assignee, "
                        "priority, status, created_at, updated_at) VALUES (?,?,?,?,?,0,'in_progress',?,?)",
                        (_task_id, _task_title, _task_desc, "workflow_engine",
                         wf.allowed_executors[0] if wf.allowed_executors else "", _now, _now))
                    conn.execute(
                        "INSERT INTO workflow_instances (instance_id, template_id, task_id, assigner, "
                        "assignee, status, current_step_id, step_results, context, created_at) "
                        "VALUES (?,?,?,?,?,'pending','s1',?,?,?)",
                        (_new_wf_id, wf.name, _task_id, "workflow_engine",
                         wf.allowed_executors[0] if wf.allowed_executors else "",
                         json.dumps({}),
                         json.dumps({"loop_iteration": _iteration + 1, "parent_loop_wf": wf_id}),
                         _now))
                    conn.commit()
                    created_new = True
                except Exception as _e:
                    LOGGER.error("loop new iteration failed %s: %s", wf_id, _e)
                    if conn.in_transaction:
                        conn.rollback()
        except Exception as _e:
            LOGGER.warning("_handle_wf_loop failed %s: %s", wf_id, _e)
            return

        # ── 锁外：仅当新迭代成功创建才重启生产者并广播 ──
        if not created_new:
            return
        for _role in _producers:
            try:
                # start 与兄弟站点 role_lifecycle.py:404 对齐给 60s：ccs.py start
                # 内部要等 tmux 就绪，30s 会结构性超时；且 TimeoutExpired 不在
                # ValueError/KeyError/TypeError 继承链上，原先会冒泡出 run_once
                # 中止整个 tick（与 R99 投递超时穿透同族），此处一并收口为告警。
                _engine_mod._sp.run(["python3", str(_CCS_CLI), "stop", _role],
                        capture_output=True, timeout=30)
                _engine_mod._sp.run(["python3", str(_CCS_CLI), "start", _role, "--no-attach",
                         "--drive", "infinite"],
                        capture_output=True, timeout=60)
                LOGGER.info("wf %s loop: 生产者 %s 已重启 (iteration %d)",
                            wf_id[:8], _role, _iteration + 1)
            except (_engine_mod._sp.TimeoutExpired, ValueError, KeyError, TypeError) as _e:
                LOGGER.warning("loop producer restart failed %s: %s", _role, _e)
        self._bb.write("workflow",
            f"[workflow] {wf.name} 新一轮迭代 {_iteration + 1} 已启动: {_new_wf_id}",
            src="workflow_engine")

    def _step_deliverables(self, step_id: str, results: dict) -> list:
        """步骤的交付证据；容器步骤（子链接管）下钻到子链末步取真实产出。

        WHY 必须下钻：子链回流只把父步盖成 completed，从不写父步的
        exit_messages（task_generator._scan_tasks）。直接读父步 = 下游拿到空
        上下文——实测 food_chain_loop/s5 的 qa 只收到「[任务描述] 测试验证（qa）」，
        花整轮翻 DB/bus 找"测什么"，还撞上 403。

        不回写父步 exit_messages 是有意的：驳回门禁按父步 exit_messages 判驳回，
        把子链文案复制进来会让子链的 rejected 变成父步硬拦（改变完成语义）。
        """
        _sr = (results or {}).get(step_id) or {}
        if not isinstance(_sr, dict):
            return []
        if _sr.get("exit_messages"):
            return _sr["exit_messages"]
        _sf = _sr.get("subflow_id")
        if not _sf:
            return []
        try:
            _row = self._lifecycle.query_one(
                "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?",
                (_sf,))
            if not _row:
                return []
            _sub_sr = json.loads(_row["step_results"] or "{}")
            for _sid in [_row["current_step_id"], *reversed(list(_sub_sr))]:
                _sd = _sub_sr.get(_sid)
                if isinstance(_sd, dict) and _sd.get("exit_messages"):
                    return _sd["exit_messages"]
        except Exception as _e:
            LOGGER.debug("subflow deliverables read failed %s: %s", _sf[:12], _e)
        return []

    @staticmethod
    def _render_msgs(msgs: list, limit: int = 3000) -> str:
        """把 exit_messages 渲染成 prompt 文本（标题 + 证据正文）。"""
        return "\n".join(
            (str(m.get("text", "")) +
             ("\n" + str(m["evidence"]) if m.get("evidence") else ""))
            if isinstance(m, dict) else str(m)
            for m in msgs)[:limit]

    def _fill_prompt_vars(self, prompt: str, step: Step, wf_name: str,
                          task_title: str, task_desc: str, results: dict,
                          prev_step_id: str = "", wf_id: str = "") -> str:
        """填充 prompt 变量：任务上下文 + 步骤间数据传递（上一步 exit_messages）。
        供 _advance_production_wf / _kick_stalled_roles / run_once 复用，保证
        重推与首推拿到的 prompt 一致（避免重推丢上下文）。

        wf_id 非空时在头部注入实例 id：exit_condition 带 require_wf 的步骤
        只认含本 id 的完成消息，角色不知道 id 就永远写不出匹配消息。"""
        if wf_id:
            # 产物归属约定：模板里的上游路径是固定名（~/ccs-workspaces/pm/PRD.md），
            # 而同一角色常有 2+ 条链并发写同名文件 —— 实测 pm 的 PRD.md 被 scout 链
            # 覆盖，s2 product_architect 据此设计出完全无关的对象，s3 reviewer 判
            # 「设计闸门失去可评对象」0P0/3P1 | exit #417441 vs #417534。
            # 角色已自发按实例写副本（PRD-wf_654cd82159.md）并标注权威，但下游
            # prompt 不知道去找它。此处把该约定说清，不改模板 JSON 与 exit_schema
            # （标准名仍须写，契约不变），只补读取侧与标注侧。
            prompt = (
                f"[实例] {wf_id}（完成消息标题必须含此 id）\n"
                f"[产物归属] 同一角色目录会被多条链并发写入固定名文件（如 PRD.md）。"
                f"读上游产物时优先读带本实例 id 的副本（如 PRD-{wf_id}.md），"
                f"并在完成消息里指明哪一份是本实例的权威副本；"
                f"写产出时标准文件名照常写（契约不变），"
                f"同时另存一份带本实例 id 的副本。\n"
            ) + prompt

        prompt = prompt.replace("{title}", task_title)
        prompt = prompt.replace("{description}", task_desc)
        prompt = prompt.replace("{assignee}", step.target_role)
        prompt = prompt.replace("{task_definition}", task_title or task_desc or wf_name)
        prompt = prompt.replace("{acceptance_criteria}", task_desc or "按模板要求完成产出并写入对应bus分类")
        prompt = prompt.replace("{topic}", task_title)
        # workspace 摘要：给角色上下文（首步生产扫描在调用前已替换，这里统一兜底）
        try:
            _ws_dir = _CCS_WORKSPACES / step.target_role
            prompt = prompt.replace("{workspace_summary}",
                                    self._collect_workspace_summary(_ws_dir))
        except Exception:
            pass
        # 步骤间数据传递：上一步骤的交付证据填充后续步骤变量。
        # _step_deliverables 会在上一步是容器（子链接管）时下钻取子链产出。
        prev_msgs = self._step_deliverables(prev_step_id, results) if prev_step_id else []
        fallback = task_desc or task_title or wf_name
        if prev_msgs:
            prev_text = self._render_msgs(prev_msgs)
            # 上一步消息用于目标/发现/结果变量；{project}/{changes}/{task_list} 仍用 fallback 填充
            # （不填充会导致 _UNMATCHED_VAR_RE 拦截推进，architect/qa 子链 s2 卡死）
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}"):
                prompt = prompt.replace(var, prev_text)
            for var in ("{changes}", "{task_list}", "{assignments}",
                        "{workspace_summary}", "{project}"):
                if var in prompt:
                    prompt = prompt.replace(var, fallback)
        else:
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}", "{changes}",
                        "{task_list}", "{assignments}", "{workspace_summary}", "{project}"):
                if var in prompt:
                    prompt = prompt.replace(var, fallback)
        return prompt

    def _write_step_prompt(self, run, step: Step, extra_prompt: str = ""):
        ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
        # assignee 无条件赋值：它只是 step.target_role，不依赖 DB 查询。
        # 旧位置在下方 `if "{title}" or "{description}"` 块内 —— 模板含 {assignee}
        # 但不含 {title}/{description} 时（WL-01 s1 实测）该块整体跳过，变量原样
        # 投出，被 _UNMATCHED_VAR_RE 拦下 → 投递恒失败（bus #375006 同一现象）。
        ctx["assignee"] = step.target_role
        if "{workspace_summary}" in step.prompt_template:
            # 运行时读属性而非 import 期冻结：测试用 conftest monkeypatch 该常量隔离。
            ws_dir = _paths_mod.ITERATION_WORKSPACES / ctx.get("project_name", "")
            ctx["workspace_summary"] = self._collect_workspace_summary(ws_dir)
        # 从 SQLite 补 task 上下文
        if "{title}" in step.prompt_template or "{description}" in step.prompt_template:
            try:
                lm = self._lifecycle
                task_rows = lm.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (run.id,)
                )
                task = task_rows[0] if task_rows else None
                if task:
                    ctx["title"] = task["title"]
                    ctx["description"] = task["description"]
            except Exception:
                LOGGER.exception("_write_step_prompt task context fetch failed for %s", run.id)
        prompt = step.prompt_template + "\n" + extra_prompt if extra_prompt else step.prompt_template
        for k, v in ctx.items():
            prompt = prompt.replace(f"{{{k}}}", str(v))
        # 语义兜底（与生产扫描块一致）：context 缺失时防止未替换变量导致发送跳过
        fallback = ctx.get("title") or ctx.get("task_title") or run.workflow_name or step.title or ""
        prompt = prompt.replace("{focus_area}", step.title or fallback)
        for var in ("{target}", "{findings}", "{results}", "{backlog}",
                    "{exception_info}", "{title}", "{description}", "{topic}",
                    "{task_definition}", "{acceptance_criteria}", "{project}",
                    "{changes}", "{task_list}", "{assignments}"):
            if var in prompt:
                prompt = prompt.replace(var, fallback)
        self._send_to_role(step.target_role, prompt)

    def _collect_workspace_summary(self, ws_dir: Path) -> str:
        if not ws_dir.exists():
            return "workspace 不存在"
        parts = []
        for fname in ["PRD.md", "DESIGN.md", "TASKS.json", "INTAKE.md"]:
            fpath = ws_dir / fname
            if fpath.exists():
                content = fpath.read_text()
                parts.append(f"[{fname}] {content[:200]}...")
        return "\n".join(parts) if parts else "workspace 为空"
