#!/usr/bin/env python3
"""StepHandlerMixin — 步骤级 tick 逻辑 + exit 条件匹配 + schema 校验。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import os
import re
import time
import subprocess
import logging
import pathlib
from pathlib import Path

from pipeflow import engine as _engine_mod  # 属性访问 _engine_mod._sp，patch("pipeflow.engine._sp") 生效
from pipeflow.engine import Step, WorkflowRun
from pipeflow.wf_lifecycle import _value_audit_failed
from paths import CCS_WORKSPACES as _CCS_WORKSPACES
from conversation_monitor import get_conversation_signal
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


def _remind_tmux_idle(run, step, evidence: str, send_to_role) -> None:
    """角色 tmux pane 显示空闲 → 发一轮提醒 prompt（不 cancel，不递增超时）。"""
    msg = (f"[workflow] {run.workflow_name}/{step.id} 检测到角色空闲（pane 证据: {evidence[:80]}）。"
           f"请继续完成当前步骤 {step.id}。")
    send_to_role(step.target_role, msg)


def _stop_tmux_violation(run, step, commands: list[str], role: str, send_to_role) -> None:
    """角色 tmux pane 显示越界操作 → 注入停止指令（安全护栏，不 cancel 工作流）。"""
    ops = "、".join(commands[:3])
    msg = (f"[workflow] {run.workflow_name}/{step.id} 检测到越界操作: {ops}。"
           f"请立即停止并撤销该操作，仅在当前工作流 {step.id} 范围内工作。")
    send_to_role(role, msg)


class StepHandlerMixin:
    """WorkflowEngine 的步骤处理 mixin：_tick / _sync_step_results / _validate_exit_schema / _check_exit。"""

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

        # ── 双锚点：persisted.poll_since 与 bus_anchor，避免 tick 内消息被跳 ──
        # bus_anchor：永久记录已匹配消息的时间戳，只增不减；poll_since：轮询游标，仅在 exit_condition 匹配后同步
        # ponytail: fallback 用 time.time() 而非 run.created_at——当 wf complete 全量覆写
        # step_results 擦除 poll_since/bus_anchor 时，fall back 到 workflow 创建时间
        # 会导致 created_after 过滤器跳过所有近期 bus 消息，流水线永久卡死。
        _persisted = run.step_results.get(step.id, {}).get("poll_since", time.time())
        _bus_anchor = run.step_results.get(step.id, {}).get("bus_anchor")
        # 初始化时用 notified_at（步骤首次通知时间），确保不跳过消息
        if _bus_anchor is None:
            _bus_anchor = run.step_results.get(step.id, {}).get("notified_at", _persisted)
        # 时间下限 = max(锚定, 工作流创建时间)：bus_anchor 可能被旧工作流残留值污染
        # （wf_7f37 匹配到 wf_cde32 的旧消息，bus_anchor=旧消息 ts, 2026-08-10）。
        # 工作流创建前绝对不存在属于它的消息 → 硬下限。
        last_ts = max(_bus_anchor or 0.0, run.created_at or 0.0)

        # ── 旧引擎产物抢救: exit_messages 已存在但 tick 未闭合 → 直接 complete ──
        _sdata = run.step_results.get(step.id, {})
        if isinstance(_sdata, dict) and _sdata.get("exit_messages") and _sdata.get("status") != "completed" and not (getattr(step, 'dual_node', False) and not _sdata.get("node2_sent")):
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
        # ── tmux 信号补充（bus 信号延迟/丢失时的兜底）──
        # conversation_monitor 从角色 tmux pane 提取 3 信号，作为 bus exit_condition 补充：
        #   step_completed → 直接推进（产出已在 pane 可见）
        #   idle_complaint → 发提醒（不 cancel）
        #   violation      → 注入停止指令（越界操作）
        try:
            sig = get_conversation_signal(step.target_role)
        except Exception as _e:
            LOGGER.debug("conversation_monitor 信号获取失败: %s", _e)
            sig = None
        if sig and sig.step_completed:
            # wait_roles 协作门：tmux 信号不能绕过协作门控（2026-08-10 codex P1），
            # 等所有声明角色都产出匹配消息才允许推进
            _wr = getattr(step, "wait_roles", None)
            if not _wr or set(_wr).issubset({m.get("src", "") for m in (run.step_results.get(step.id, {}) or {}).get("exit_messages", [])}):
                try:
                    self._lifecycle.complete_step(run.id, step.id)
                    LOGGER.info("tmux 信号推进: %s/%s step_completed", run.workflow_name, step.id)
                except Exception as _e:
                    LOGGER.debug("tmux 信号 complete_step 失败: %s", _e)
                return  # 产出已确认，跳过 timeout/催办
        # ── tmux 信号节流与升级 ──
        # 计数存于 step_results["tmux_signals"]（跨 tick 持久化），
        # 防高频骚扰：闲置提醒 ≥3 次/越界 ≥1 次后各自冷却 5 分钟；
        # 升级：闲置/越界累计 ≥5 次 → cancel 步骤（角色不响应，继续等待无意义）。
        _sig_state = (run.step_results.get(step.id, {}) or {}).get("tmux_signals") or {}
        _now = time.time()
        if sig and sig.idle_complaint:
            _idle_n = _sig_state.get("idle_count", 0) + 1
            _idle_cool = _sig_state.get("idle_cool_until", 0)
            if _idle_n >= 5:
                try:
                    self._lifecycle.cancel_step(run.id, step.id)
                    LOGGER.info("tmux 信号取消: %s/%s 连续 %d 次空闲", run.workflow_name, step.id, _idle_n)
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
            _viol_n = _sig_state.get("violation_count", 0) + 1
            if _viol_n >= 5:
                try:
                    self._lifecycle.cancel_step(run.id, step.id)
                    LOGGER.info("tmux 信号取消: %s/%s 连续 %d 次越界", run.workflow_name, step.id, _viol_n)
                except Exception as _e:
                    LOGGER.debug("tmux 越界 cancel_step 失败: %s", _e)
            else:
                try:
                    _stop_tmux_violation(run, step, sig.violation_commands, sig.violation_role,
                                         send_to_role=self._send_to_role)
                except Exception as _e:
                    LOGGER.debug("tmux 越界停止指令失败: %s", _e)
            _sig_state["violation_count"] = _viol_n
        if _sig_state:
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
                "tmux_signals": _sig_state,
            }
            try:
                self._sync_step_results(run.id, run.step_results)
            except Exception as _e:
                LOGGER.debug("tmux 信号状态同步失败: %s", _e)
        # ── Timeout 检查（每次 tick 都执行，不受 schema/verify 影响）──
        # 超时后自动升级到 coordinator，不会被 schema 失败 return 阻断。
        # 超限阈值: max(max_retries+2, 3) 次超时后自动 cancel，防止无限重试堆积。
        _start_ts = run.step_results.get(step.id, {}).get("ts", run.created_at)
        # None 守卫：ts 或 created_at 缺失时不能相减（TypeError），按未开始处理
        elapsed = time.time() - _start_ts if _start_ts is not None else 0.0
        # 超时自动回收阈值: 默认 4 次超时后 cancel，防无限重推写 monitor_audit 噪音。
        # delivery_blocked 的子流（投递连败/角色无响应）4 次×timeout 太长（实测 ~40min 刷 bus），
        # 收紧到 2 次即回收——角色 2 个 timeout 窗口无响应即可判定卡死，断死循环。
        _auto_cancel_at = 2 if run.status == "delivery_blocked" else max(max_retries + 3, 4)
        if elapsed >= timeout:
            # 角色忙（有其他任务执行中）→ 排队语义：不递增 timeout_count，
            # 仅延长响应窗口（避免"忙时被塞任务→未响应→超时回收"的假阳性）
            if self._is_role_busy(step.target_role, exclude_wf_id=run.id):
                # 角色忙 ≠ 任务未完成：先做 exit 匹配，产出已存在则直接推进
                # （否则角色完成产出但忙其他任务时，本工作流无限排队，bus #167081）
                if cat:
                    _busy_match_ts, _busy_match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts,
                        wf_id=run.id if ec.get("require_wf") else "", strict_wf=False,
                        wait_roles=getattr(step, "wait_roles", None))
                    if _busy_match_ts:
                        # exit_schema 校验与主路径一致：busy 不绕过文件约束
                        # （2026-08-12 codex P1：schema 要求不能因忙检测分支成空文）
                        _schema_ok = True
                        if step.exit_schema:
                            _schema_ok, _ = self._validate_exit_schema(step, run)
                        if _schema_ok:
                            run.step_results[step.id] = {
                                **run.step_results.get(step.id, {}),
                                "bus_anchor": _busy_match_ts,
                                "exit_messages": _busy_match_msgs,
                            }
                            self._sync_step_results(run.id, run.step_results)
                            # bypass 补发完整性提醒（busy exit-match 直接推进，跳过双节点审查）
                            _integ_send_bypass(run, step, "busy",
                                               send_to_role=self._send_to_role,
                                               sync_step_results=self._sync_step_results)
                            try:
                                self._lifecycle.complete_step(run.id, step.id)
                            except Exception as _e:
                                LOGGER.debug("busy exit-match complete_step 失败: %s", _e)
                _queued = run.step_results.get(step.id, {}).get("queued", 0) + 1
                # 排队上限: 连续排队超过上限（约 3 小时未响应）说明角色
                # 活跃但卡死（pm 类故障）→ 升级 coordinator 而非无限排队
                if _queued >= 6:
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"排队 {_queued} 次仍未响应（角色 {step.target_role} 活跃但可能卡死），请介入",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                    # 排队上限 = 角色被其他工作流占用，子链无法执行 → auto-cancel
                    if _queued >= 10:
                        LOGGER.warning("auto-cancel subflow: %s queued=%d，角色 %s 活跃占用",
                                       run.id[:12], _queued, step.target_role)
                        try:
                            self._lifecycle.close_wf(run.id, status="cancelled")
                            self._bb.write("workflow",
                                f"[workflow] {run.workflow_name} 排队超限自动取消: "
                                f"角色 {step.target_role} 被其他工作流占用，"
                                f"排队 {_queued} 次后自动回收",
                                src="workflow_engine")
                        except Exception as _cancel_e:
                            LOGGER.debug("auto-cancel subflow failed: %s", _cancel_e)
                run.step_results[step.id] = {
                    **run.step_results.get(step.id, {}),
                    "ts": time.time(),  # 重置计时器，等待角色空闲
                    "queued": _queued,
                }
                self._sync_step_results(run.id, run.step_results)
                return  # 排队路径不递增 timeout_count（防误回收）

            timeout_count = run.step_results.get(step.id, {}).get("timeout_count", 0) + 1
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
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
                            self._lifecycle.close_wf(run.id, status="completed")
                        except Exception as _e:
                            LOGGER.warning("tick 末位收尾失败 %s/%s: %s", run.id[:8], step.id, _e)
            # 超限 → 自动 cancel，阻止死循环升级
            if timeout_count >= _auto_cancel_at:
                try:
                    self._lifecycle.close_wf(run.id, status="cancelled")
                except Exception as _e:
                    LOGGER.warning("auto-cancel close_wf failed: %s", _e)
                self._notify_role("maintainer",
                    f"工作流超时自动回收: {run.workflow_name}/{run.id}",
                    f"step={run.current_step} timeout_count={timeout_count} >= {_auto_cancel_at}\n请排查该步骤为何持续超时。")
            # 重推任务（最多重推 1 次，不自动完成）
            # 角色忙（有其他任务在跑）→ 不强制重推，通知 coordinator 排队
            # 架构 P1-2：完整性阶段进行中时跳过重推（避免 /goal 覆盖完整性提醒）
            # 注意：此处在完整性阶段的 if not cat: return 之后，_integ_state 需重新读取
            _integ_retry_guard = _integ_load(run.step_results, step.id)
            if timeout_count == 1 and not _integ_retry_guard.pending and not _integ_retry_guard.complete:
                if self._is_role_busy(step.target_role, exclude_wf_id=run.id):
                    # 角色在忙，延长超时窗口：通知 coordinator 而非重推
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"超时1次但{step.target_role}忙碌（有其他任务执行中），自动排队等待",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                else:
                    self._send_to_role(step.target_role, step.prompt_template,
                                       wf_id=run.id, step_id=step.id, force=True)
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} 已超时 1 次，等待角色响应",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                    # 审计信号：记录轮次超时（供 monitor_audit 查看 SLA）
                    # 写失败不阻断超时处理（审计是旁路，DB 锁/连接异常不应中断 tick）
                    try:
                        self._bb.write("monitor_audit",
                            f"决策: 轮次超时 → {run.workflow_name}/{run.current_step} "
                            f"step={step.id} role={step.target_role} 超时 {timeout}s，已重推+升级",
                            src="workflow_engine")
                    except Exception:
                        LOGGER.debug("monitor_audit write failed for %s/%s", run.workflow_name, step.id)
            # ── dual_node 超时抢救：node1_done 后角色完成审查但未推进，直接匹配 exit 推进 ──
            elif step.dual_node and _sdata.get("node1_done") and not _sdata.get("node2_sent"):
                # 超时但审查指令已发、角色可能已完成 → 检查 exit 匹配
                _wide = re.sub(r"-complete$", "-", text_filter)
                _match_ts2, _match_msgs2 = self._check_exit(cat, src_filter, _wide, created_after=last_ts,
                    wf_id=run.id if ec.get("require_wf") else "", strict_wf=False,
                    wait_roles=getattr(step, "wait_roles", None))
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
            if timeout_count >= 1 and not step.dual_node and not getattr(step, "wait_roles", None):
                try:
                    # 全量读：_has_anchor 需跨分类找 task_spec 锚点（cat 过滤会漏掉），
                    # limit 提升到 500 覆盖更大消息窗口，降低超长负载漏匹配风险
                    _role_msgs = self._bb.read(limit=500)
                    _wf_id = run.id
                    _notified_ts = run.step_results.get(step.id, {}).get("notified_at", 0)
                    _relaxed = [
                        {"id": f.id, "text": f.t[:200], "ts": f.ts, "src": f.src}
                        for f in _role_msgs
                        if f.src == step.target_role
                        and f.src != "workflow_engine"  # 排除引擎自产告警，防止误判角色产出
                        and f.ts > _notified_ts
                        and f.ts < time.time() + 60  # 防未来时间戳
                    ]
                    # task_spec 锚点确认确实是本工作流任务
                    _has_anchor = any(
                        _wf_id[:8] in str(f.t) and f.cat == "task_spec"
                        for f in _role_msgs
                    )
                    if _relaxed and _has_anchor:
                        LOGGER.info("relaxed-advance %s/%s: 角色已写同工作流 bus 消息 (cat 不匹配, %d 条)",
                                    run.workflow_name, step.id, len(_relaxed))
                        run.step_results[step.id] = {
                            **run.step_results.get(step.id, {}),
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
        _integ_state = _integ_load(run.step_results, step.id)
        _integ_dual_pending = (getattr(step, 'dual_node', False)
                               and not run.step_results.get(step.id, {}).get("node2_sent"))
        if _integ_state.pending and not _integ_state.complete and not _integ_dual_pending:
            if _integ_should_send(_integ_state):
                _dim = _integ_next_dim(_integ_state)
                if _dim:
                    _goal = _integ_full_prompt(run, step, run.step_results)
                    _output_summary = str(
                        run.step_results.get(step.id, {}).get("exit_messages", [""])
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
                            step.target_role,
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
                wf_id=run.id if ec.get("require_wf") else "", strict_wf=False
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
                _integ_elapsed = time.time() - run.step_results.get(step.id, {}).get(
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
            _node2_sent = (run.step_results.get(step.id, {}) or {}).get("node2_sent", False)
            # 协作门：wait_roles 声明的角色都产出匹配消息才推进（多角色协作）
            _wait_roles = getattr(step, "wait_roles", None)
            if step.dual_node and _node2_sent:
                # 宽匹配：第二节点消息（codex-approved）到达时也能匹配
                _wide_filter = re.sub(r"-complete$", "-", text_filter)
                _match_ts, _match_msgs = self._check_exit(cat, src_filter, _wide_filter, created_after=last_ts,
                            wf_id=run.id if ec.get("require_wf") else "", strict_wf=False, wait_roles=_wait_roles)
            else:
                _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts,
                            wf_id=run.id if ec.get("require_wf") else "", strict_wf=False, wait_roles=_wait_roles)
            if _match_ts:
                # 过滤已被其他工作流消费的消息
                _match_msgs = [m for m in _match_msgs if m["id"] not in self._consumed_exit_ids]
                _match_ts = _match_msgs[0]["ts"] if _match_msgs else 0.0
            if _match_ts:
                # 标记消息已消费，防止其他工作流重复匹配（bus #162146）
                self._consumed_exit_ids.update(m["id"] for m in _match_msgs)
                _old = run.step_results.get(step.id, {})
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
                    _sdata = run.step_results.get(step.id, {}) or {}
                    if not _sdata.get("node2_sent"):
                        _codex_prompt = (
                            "/goal\n## codex CLI 三方审查\n"
                            f"对 {run.workflow_name}/{step.id} 的产出执行 codex review --uncommitted：\n"
                            "1. 执行 codex review --uncommitted -c model=9router_hermes\n"
                            "2. 若发现问题修复后重跑，直到连续两次零问题\n"
                            "3. 通过后写 bus cat=" + cat + "，标题含 " + step.id + "-codex-approved\n"
                            "## 完成\n"
                            "写 bus cat=" + cat + "，标题含 " + step.id + "-codex-approved。"
                        )
                        run.step_results[step.id] = {
                            **_sdata,
                            "node1_done": True, "node1_at": time.time(),
                            "node2_sent": True, "node2_sent_at": time.time(),
                        }
                        self._sync_step_results(run.id, run.step_results)
                        LOGGER.info("双节点: %s/%s 第一节点完成，已发 codex 审查指令", run.workflow_name, step.id)
                        # R5: 自动执行 codex review --uncommitted (三方20维审查自动化)
                        _codex_result = self._run_codex_review(run, step, cat)
                        if _codex_result.get("success"):
                            LOGGER.info("双节点: %s/%s codex review 通过，等待 node2 消息", run.workflow_name, step.id)
                        else:
                            LOGGER.warning("双节点: %s/%s codex review 失败: %s，仍发提示让角色人工审查",
                                           run.workflow_name, step.id, _codex_result.get("error", "未知错误"))
                        self._send_to_role(step.target_role, _codex_prompt,
                                           wf_id=run.id, step_id=step.id, force=True)
                        return
                    else:
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
        # track 消费：步骤执行期间的轮次追踪（旧 --bus-track 收编）
        # exit_condition 是一次性退出匹配，track 是步骤执行期间的持续轮询
        if step.track:
            track_cat = step.track.get("bus_category", "")
            if track_cat and track_cat != cat:
                # 与 exit_condition 不同的分类才单独追踪，避免重复
                track_timeout = step.track.get("timeout_sec", 300)
                notify_roles = step.track.get("notify_roles", [])
                latest = self._bb.read(cat=track_cat, limit=1)
                if latest:
                    # 作者过滤（旧 tracker 语义）：若最新消息作者是 target_role 自己，
                    # 说明角色没卡死（刚写过），不是死锁，跳过告警
                    if latest[0].src == step.target_role:
                        pass  # 自己写的消息，不报死锁
                    else:
                        # 超时基准：自步骤通知（notified_at）以来该分类无新消息才判超时
                        _notified = run.step_results.get(step.id, {}).get("notified_at", 0)
                        age = time.time() - max(latest[0].ts, _notified)
                        if age >= track_timeout:
                            # 节流：同一 tracking 周期只通知一次
                            _sdata = run.step_results.get(step.id, {}) or {}
                            _last = _sdata.get("track_notified_at", 0)
                            if time.time() - _last >= track_timeout:
                                _sdata["track_notified_at"] = time.time()
                                run.step_results[step.id] = _sdata
                                # 持久化：run 每 tick 从 DB 重建，不落库则 _last 恒为 0
                                self._sync_step_results(run.id, run.step_results)
                                try:
                                    self._bb.write("monitor_audit",
                                        f"决策: 轮次追踪超时 → {run.workflow_name}/{run.current_step} "
                                        f"track={track_cat} 超时 {int(age)}s",
                                        src="workflow_engine")
                                except Exception:
                                    LOGGER.debug("monitor_audit write failed for %s", run.workflow_name)
                                try:
                                    for nr in notify_roles:
                                        # 存活检查（旧 tracker 交叉验证语义）：先确保通知
                                        # 对象活着（死了由引擎拉起），再发提醒
                                        self._ensure_role_alive(nr)
                                        self._send_to_role(nr,
                                            f"[deadlock] {track_cat} 超时 {int(age)}s，请继续",
                                            wf_id=run.id, step_id=step.id, force=True)
                                except Exception:
                                    LOGGER.debug("track notify failed for %s", run.workflow_name)
        if not cat:
            return
        _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts,
                        wf_id=run.id if ec.get("require_wf") else "", strict_wf=False,
                        wait_roles=getattr(step, "wait_roles", None))
        if _match_ts:
            # ── 锚定已匹配消息的时间戳，以后只检更新消息 ──
            # 同时保存 exit_messages 作为角色产出证据（task_evidence 依赖此字段判定收益）
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
                "bus_anchor": _match_ts,
                "exit_messages": _match_msgs,
            }
            self._sync_step_results(run.id, run.step_results)

            # schema/verify 失败 → 写 blocker 且不推进，等角色补产出后重试
            if step.exit_schema:
                ok, errs = self._validate_exit_schema(step, run)
                if not ok:
                    self._bb.write("blocker",
                        f"[workflow] {run.workflow_name} {step.id} schema: {'; '.join(errs)}",
                        src="workflow_engine")
                    self._sync_step_results(run.id, run.step_results)
                    return

            if step.verify:
                import shlex as _shlex
                ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
                vcmd = step.verify
                # D3 P1 安全修复：对 context 值做 shlex.quote，防止模板变量被 shell 解释
                for k, val in ctx.items():
                    vcmd = vcmd.replace(f"{{{k}}}", _shlex.quote(str(val)))
                # verify 命令可能含 && / 管道等 shell 语法，shlex.split 会把 && 当
                # 参数传给第一个命令导致 "extra argument" 报错 → 用 shell=True 执行。
                # verify 命令来自模板注册表（可信内部源），context 值已 shlex.quote 保护。
                ver = _engine_mod._sp.run(vcmd, shell=True, capture_output=True, timeout=30)
                if ver.returncode != 0:
                    self._bb.write("blocker",
                        f"[workflow] {run.workflow_name} {step.id} verify: {ver.stderr.decode()[:200] or 'failed'}",
                        src="workflow_engine")
                    self._sync_step_results(run.id, run.step_results)
                    return

            try:
                self._lifecycle.complete_step(run.id, step.id)
            except Exception as e:
                LOGGER.error("[wf] LM complete_step 失败: %s", e)
                # 步骤不匹配说明 lifecycle 已推进到下一步 → 跳过不重试
                if "步骤不匹配" in str(e):
                    return
                # DB 瞬态故障 → 有 timeout_count 间接阻止无限重试
                _prev = run.step_results.get(step.id, {})
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
                    _cs.report_idle(step.target_role)
            except Exception as _idle_err:
                LOGGER.debug("idle detection skipped: %s", _idle_err)

            # ── 自动确认单步/通知步骤，推进到下一棒 ──
            # daemon 替代人工 approve，用 step 角色名作为密钥
            # handoff/review 步骤默认需等人工审批，不自动确认
            if step.type in ("handoff", "review", "subflow"):
                # 超时降级：approval_token 超过 TTL+30min grace 未消费 → 自动 approve 推进
                # （修复 lr_tech_decision 卡死根因：reviewer 未消费导致 24h+ stuck）
                try:
                    _token = self._lifecycle.get_approval_token(run.id, step.id)
                    if _token:
                        _db_results = self._lifecycle._parse_results(
                            self._lifecycle._get_wf_unsafe(run.id))
                        _ttl = _db_results.get(step.id, {}).get("approval_token_ttl", 0)
                        # _ttl=0 表示字段不存在或 token 无效，不触发降级
                        if _ttl > 0 and time.time() > _ttl + 1800:  # TTL + 30min grace
                            self._lifecycle.confirm_step(
                                run.id, step.id, token=_token, approved=True,
                                reason="daemon approval_timeout_fallback",
                                allow_expired=True)  # P0-A 修复: token 已过期时也允许确认
                            LOGGER.warning("approval_timeout_fallback %s/%s: token 超时 30min 未消费，自动 approve",
                                           run.workflow_name, step.id)
                            # 同步 step_results 到 DB（confirm_step 内部已写，这里确保 run 内存状态也刷新）
                            _db_after = self._lifecycle._parse_results(
                                self._lifecycle._get_wf_unsafe(run.id))
                            run.step_results[step.id] = _db_after.get(step.id, {})
                            self._sync_step_results(run.id, run.step_results)
                            return
                except Exception as _e:
                    LOGGER.debug("approval_timeout check skipped: %s", _e)
                return
            try:
                _token = self._lifecycle.get_approval_token(run.id, step.id)
                if _token:
                    self._lifecycle.confirm_step(run.id, step.id, token=_token, approved=True,
                                                  reason="daemon auto-advance")
                    LOGGER.info("auto-advance %s/%s -> next step", run.workflow_name, step.id)
            except Exception as e:
                LOGGER.debug("auto-advance skipped for %s/%s: %s", run.workflow_name, step.id, e)

            # lifecycle.manager 已写入 step_results，此处不再重复写入
            return

        # 退出条件未匹配（无 bus 消息，timeout 已在顶部检查）
        # poll_since 推进 = now - 30s 安全窗口，覆盖慢角色写 bus 的延迟（qa 写消息可达数分钟）。
        # bus_anchor 保持不动——它是"已匹配消息"的锚点，只在匹配时更新。
        # 修复：之前 no-match 也推进 bus_anchor，导致 tick 间消息被永久跳过。
        _now = time.time()
        _new_poll = _now - 30
        run.step_results[step.id] = {
            **run.step_results.get(step.id, {}),
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
            if "*" in _cc_file:
                _matches = sorted(_fp.parent.glob(_fp.name))
                _fp_ok = bool(_matches) and _matches[0].stat().st_size >= _min
            else:
                _fp_ok = _fp.exists() and _fp.stat().st_size >= _min
            if _fp_ok:
                LOGGER.info("completion_check: %s 文件存在且 ≥%d 字节 → 推进 %s",
                            _fp.name, _min, step.id)
                if step.dual_node:
                    # dual_node：文件存在 = 第一节点完成，注入第二节点审查指令
                    _sdata = run.step_results.get(step.id, {}) or {}
                    if not _sdata.get("node1_done"):
                        _sdata["node1_done"] = True
                        _sdata["node1_at"] = time.time()
                        _sdata["ts"] = time.time()
                        # 发 codex CLI 审查指令（第二节点）+ 设 node2_sent 阻断 complete_step
                        _codex_prompt = (
                            "/goal\n## codex CLI 三方审查\n"
                            f"对 {run.workflow_name}/{step.id} 的产出执行 codex review --uncommitted：\n"
                            "1. 执行 codex review --uncommitted -c model=9router_hermes\n"
                            "2. 若发现问题修复后重跑，直到连续两次零问题\n"
                            "3. 通过后写 bus cat=" + cat + "，标题含 " + step.id + "-codex-approved\n"
                            "## 完成\n"
                            "写 bus cat=" + cat + "，标题含 " + step.id + "-codex-approved。"
                        )
                        _sdata["node2_sent"] = True
                        _sdata["node2_sent_at"] = time.time()
                        run.step_results[step.id] = _sdata
                        self._sync_step_results(run.id, run.step_results)
                        try:
                            self._send_to_role(step.target_role, _codex_prompt,
                                               wf_id=run.id, step_id=step.id, force=True)
                            LOGGER.info("双节点: %s/%s 第一节点完成，codex 审查指令已发送", run.workflow_name, step.id)
                        except Exception as _send_err:
                            LOGGER.exception("双节点: %s/%s 审查指令发送失败: %s", run.workflow_name, step.id, _send_err)
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

    def _run_codex_review(self, run, step, bus_cat: str) -> dict:
        """自动执行 codex review --uncommitted (三方20维审查自动化，R5 增长).

        在 dual_node 第一节点完成后，自动运行 codex review --uncommitted
        代替手动提示角色执行。成功时返回 success=True，失败时返回 error 信息。

        R5 增长点: step_handler.py 原来只发 prompt 让角色手动跑 codex review；
        现在由引擎自动调用 codex CLI (codex review --uncommitted -c model=9router_hermes)。
        """
        try:
            # 运行 codex review --uncommitted -c model=9router_hermes
            # timeout 300s (5 min) 防卡死
            result = subprocess.run(
                ["codex", "review", "--uncommitted", "-c", "model=9router_hermes"],
                capture_output=True, text=True, timeout=300, cwd=run.workspace
            )
            if result.returncode == 0:
                # 解析输出：若有 "No issues" 或 "0 issues" 视为通过
                out = result.stdout + result.stderr
                if "No issues" in out or "0 issues" in out or "0 problems" in out:
                    return {"success": True, "output": out[:500]}
                # 有问题输出
                return {"success": False, "error": f"codex 发现问题: {out[:300]}", "output": out[:500]}
            else:
                return {"success": False, "error": f"codex 返回码 {result.returncode}: {result.stderr[:300]}"}
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "codex review 超时 (300s)"}
        except FileNotFoundError:
            return {"success": False, "error": "codex CLI 未找到 (请确认已安装)"}
        except Exception as e:
            return {"success": False, "error": f"codex review 异常: {e}"}


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

    def _validate_exit_schema(self, step: Step, run: WorkflowRun) -> tuple[bool, list[str]]:
        """校验 exit_schema 定义的文件约束。返回 (ok, error_list)。"""
        schema = step.exit_schema
        if not schema:
            return (True, [])
        ws = _CCS_WORKSPACES / step.target_role
        ws_real = os.path.realpath(ws)
        errs: list[str] = []

        for req in schema.get("required", []):
            fpath = ws / req
            fpath_real = os.path.realpath(fpath)
            if not fpath_real.startswith(ws_real):
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
            # 通配符一次解析：所有检查器共享匹配列表
            if "*" in fname:
                import glob as _gl
                all_matches = [Path(m) for m in _gl.glob(str(fpath))]
                # 过滤：只检查工作流创建后被修改/新建的文件（本次任务产出），
                # 排除 workspace 历史遗留文件导致的 minLength/mustContain 误报
                # 计数检查（minCount）不依赖 matched——即使 matched 为空也需执行
                _wf_created = getattr(run, 'created_at', 0) or 0
                matched = [m for m in all_matches if m.stat().st_mtime >= _wf_created] if _wf_created else all_matches
            else:
                matched = [fpath] if fpath.exists() else []

            if not matched and fpath.is_dir():
                matched = []  # minFiles 走单独检查，不混入内容检查
            elif not matched and not ("*" in fname and all_matches):
                # 无匹配且非目录、且非"通配符匹配到但全部是历史文件"：
                # 所有内容检查均失败。历史文件场景跳过内容检查（无本次产出可校验），
                # 但 minCount 计数仍会执行
                for prop_key in ("minLength", "mustContain", "mustContainUrl", "checksum", "maxAgeMinutes"):
                    if prop_key in props:
                        errs.append(f"缺少产出: {fname}")
                        break
                continue

            if "minLength" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    if len(content) < props["minLength"]:
                        errs.append(f"{mf.name} 内容不足 ({len(content)}<{props['minLength']})")

            if "mustContain" in props:
                for mf in matched:
                    content = mf.read_text(encoding="utf-8", errors="replace")
                    missing = [kw for kw in props["mustContain"] if kw not in content]
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
                if "*" in fname:
                    import glob as _gl
                    # ponytail: minCount 计数所有 glob 匹配，不受 mtime 过滤影响；
                    # 升级路径：增加 minCount.created_after 配置项按需过滤
                    all_cnt = len(_gl.glob(str(fpath)))
                    if all_cnt < props["minCount"]:
                        errs.append(f"{fname} glob 匹配不足 ({all_cnt}<{props['minCount']})")
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

    def _check_exit(self, cat: str, src_filter: str, text_filter: str, created_after: float = None,
                    wf_id: str = "", strict_wf: bool = False, wait_roles: list = None) -> tuple:
        """查 exit 是否有匹配消息。返回 (timestamp, matched_msgs)。

        wf_id 非空时过滤"消息必须含当前工作流实例 id"——防止新工作流
        匹配旧工作流的同分类消息（2026-08-09 观测：wf_4c51d890c082/s2
        匹配到 wf_6e338fd3 的 s2-complete，跨实例串消息）。
        strict_wf=True（require_wf=true 时）：消息必须含当前 wf_id，否则不匹配。
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
        facts = self._bb.read(cat=cat, limit=200) if cat else self._bb.read(limit=200)
        earliest = 0.0
        matched = []
        for f in facts:
            # src 精确匹配；src=claude（bus 默认值）视为角色未设 src，不排除
            # wait_roles 协作门：协作角色的消息也放行（否则 src 过滤器拦死门控）
            if src_filter and f.src != src_filter and f.src != "claude":
                if not wait_roles or f.src not in wait_roles:
                    continue
            if text_filter and text_filter not in f.t:
                continue
            if created_after and f.ts < created_after:
                continue
            # wf 归属校验：消息含别的 wf_id → 串消息 → 过滤；
            # 消息不含 wf_id 时，若 strict_wf（require_wf=true）→ 不匹配，
            # 否则时间窗口已保证属于当前实例，放行。
            # 角色写消息通常不带 wf_id（2026-08-10 观测：scout/pm 的完成消息无 wf 前缀），
            # 但 require_wf=true 是引擎硬校验，必须严格：无 wf_id 的消息不能匹配，
            # 否则旧工作流的同分类消息会串入新工作流（观测 wf_7f37 匹配到 wf_cde32 的
            # s1-complete，跨工作流误推进，2026-08-10）。
            # 归属性校验：消息含别的 wf_id → 串消息 → 过滤；
            # 用分隔符边界匹配，避免裸子串串扰（如 "abc123" 误中 "abc1234"）。
            # wf_id 形如 "wf_abc123"；同时允许去前缀的裸 id "abc123"，两种形态均需边界包裹。
            if wf_id and f.t and 'wf_' in f.t:
                import re as _re
                _bare = wf_id.replace('wf_', '', 1)
                _pat = r'(?:^|[\s"\',:;/])(' + _re.escape(wf_id) + r'|' + _re.escape(_bare) + r')(?:[\s"\',:;/]|$)'
                if not _re.search(_pat, f.t):
                    continue
            matched.append({"id": f.id, "text": f.t[:200], "ts": f.ts, "src": f.src})
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
                _rmsg = None
                for _f in _rfacts:
                    # 精确匹配目标角色 src；不放宽到 claude（否则一条
                    # claude-src 消息可满足所有缺失角色，削弱协作门）
                    if _f.src != _r:
                        continue
                    if text_filter and text_filter not in _f.t:
                        continue
                    if created_after and _f.ts < created_after:
                        continue
                    if wf_id and _f.t and 'wf_' in _f.t and wf_id not in _f.t \
                            and wf_id.replace('wf_', '') not in _f.t:
                        continue
                    _rmsg = {"id": _f.id, "text": _f.t[:200], "ts": _f.ts, "src": _f.src}
                    break
                if _rmsg is None:
                    return (0.0, [])  # 还有角色没产出，不推进
                matched.append(_rmsg)
                if earliest == 0 or _rmsg["ts"] < earliest:
                    earliest = _rmsg["ts"]
        return (earliest, matched)
