#!/usr/bin/env python3
"""WorkflowLifecycleMixin — 工作流级推进：僵尸回收、推进、子工作流、loop、重推、变量填充。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import time
import uuid
import logging
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from pipeflow.engine import Step, WorkflowDef
import paths as _paths_mod
from paths import CCS_CLI as _CCS_CLI
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")


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
    for _line in _lines[:50]:
        if _line.lower().startswith("verdict:"):
            _v = _line.split(":", 1)[1].strip().upper()
            return _v in ("FAIL", "REJECT")
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
                "current_step_id "
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
                    _loop_cfg = _loop_wf.loop or {} if isinstance(_loop_wf.loop, dict) else {}
                    if _loop_cfg.get("enabled", True):
                        _global_limit_h = 72

                _auto_cancel_at = 6
                reason = ""
                # 条件0: 状态对账——当前步骤已是终态(cancelled/failed)但行级仍 running。
                # 语义上该实例不会再推进，属"僵尸行"。这类残留由历史 cancel_step 缺陷造成
                # （只写步骤级 status 不写行级，已修），但不修存量会永久占用 running 计数
                # 并污染积压告警。实测 2026-10-17：21 台卡 5.5-6.1h，是全库 running 最大块。
                _cur_id = d.get("current_step_id", "")
                _cur_step = sr.get(_cur_id, {}) if isinstance(sr, dict) else {}
                if (isinstance(_cur_step, dict)
                        and _cur_step.get("status") in ("cancelled", "failed")):
                    reason = (f"当前步骤 {_cur_id} 已终态"
                              f"({_cur_step.get('status')})但行级仍 running")
                # 条件1: 超时重试耗尽（对 step_done_ready 无意义——等待审批期间
                # 角色已产出，timeout_count 不会递增；保留该分支对二者都无害）
                elif max_tc >= _auto_cancel_at:
                    reason = f"timeout_count={max_tc}（已达自动回收阈值{_auto_cancel_at}）"
                # 条件2: 测试/开发模板超2小时
                elif tpl in _ZOMBIE_TEMPLATES and age_h > 2:
                    reason = f"测试模板 {tpl} 运行{age_h:.0f}h > 2h阈值"
                # 条件3: 全局阈值（running=24h，step_done_ready=72h）
                elif age_h > _global_limit_h:
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
                        from pipeflow.step_handler import _QUEUE_DEGRADE_AT as _Q_DEGRADE
                        # tc>=1 是引擎已确认超时；queued 达降级阈值且真实停滞 >1h 属同等情况
                        if _cur_tc >= 1 or (_cur_q >= _Q_DEGRADE and _stall_h > 1):
                            _reason_step = _cur_sr.get("step", _cur)
                            reason = (f"running 状态 {_reason_step} 无推进记录 "
                                      f"步骤停滞 {_stall_h:.1f}h / 实例 {age_h:.1f}h "
                                      f"(timeout_count={_cur_tc}, queued={_cur_q})")

                if reason:
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
                        lm.execute(
                            "UPDATE workflow_instances SET status='cancelled', completed_at=? "
                            "WHERE instance_id=?",
                            (now, wf_id)
                        )
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
                        LOGGER.debug("stale cleanup write failed: %s", e)
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
        from pipeflow.engine import UNMATCHED_VAR_RE as _UNMATCHED_VAR_RE
        wf = self._workflows.get(wf_name)
        if not wf:
            return
        steps = wf.steps
        idx = next((i for i, s in enumerate(steps) if s.id == step_id), -1)
        if idx < 0:
            return
        is_last = idx + 1 >= len(steps)
        next_step = None if is_last else steps[idx + 1]

        completed_now = False
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
                    for _vg_dir, _vg_name in _resolve_value_gate_paths(
                            _vg.get("file", ""), _vg.get("workspace", "")):
                        _audit_path = _vg_dir / _vg_name
                        if _audit_path.exists() and not _value_audit_failed(
                                _audit_path.read_text(encoding="utf-8", errors="ignore")):
                            _vg_passed = True
                            LOGGER.info("wf %s [%s] 末位值门禁 PASS（文件 %s），允许收尾",
                                        wf_id[:8], wf_name, _audit_path.name)
                            break
                    if _vg_passed:
                        # 直接跳到 close（跳过子链扫描——子链 cancelled 已在下面兼容）
                        LOGGER.info("wf %s [%s] 完成: %d 步", wf_id[:8], wf_name, len(steps))
                        self._lifecycle.close_wf(wf_id, status='completed')
                        conn.commit()
                        completed_now = True
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
                    self._lifecycle.close_wf(wf_id, status='completed')
                    conn.commit()
                    completed_now = True
                else:
                    # 推进到下一步
                    # 条件更新：仅当当前步骤仍是 step_id 时才推进（并发防重）
                    _adv = conn.execute("UPDATE workflow_instances SET current_step_id=? WHERE instance_id=? AND current_step_id=?",
                                (next_step.id, wf_id, step_id))
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
                        task_title, task_desc, results, prev_step_id=step_id)
                    if _UNMATCHED_VAR_RE.search(prompt):
                        unmatched = _UNMATCHED_VAR_RE.findall(prompt)
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

        # ── 返工门禁：review 步骤被拒绝 → 不推进，s6 保持 notified，引擎下一轮 tick
        # 自动重查 exit 匹配（修复后新 review 结果会正常推进）。
        # 同时创建 engineer 返工子链（若已有则不重复创建）。
        _reject = False
        try:
            _done_sr = results.get(step_id, {}) if isinstance(results, dict) else {}
            _msgs_text = " ".join(
                m.get("text", "") for m in _done_sr.get("exit_messages", [])
                if isinstance(m, dict)
            ).lower()
            if any(kw in _msgs_text for kw in ("changes_requested", "rejected", "fix", "需修复", "回退")):
                _reject = True
        except Exception:
            pass
        if _reject and getattr(steps[idx], "type", "") == "review":
            LOGGER.warning("review rejected: %s/%s → 返工门禁触发，s%s 保持 notified",
                           wf_id[:8], step_id, step_id)
            # 创建 engineer 返工子链（复用 engineer_feature 模板）
            try:
                self._create_rework_subflow(wf_id, lm, wf_name, step_id,
                                            steps[idx], next_step, results)
            except Exception as _rw_err:
                LOGGER.warning("rework subflow 创建失败 %s: %s", wf_id[:8], _rw_err)
            return  # 不推进到下一步，等修复后重新触发

        # 变量完整 → 已提交推进，通知目标角色
        # 修复：_send_to_role 因角色忙/变量未替换返回 False 时，投递并未发生。
        # 原逻辑丢弃返回值直接盖戳 notified → 角色没收到任务却被判"已通知"，
        # 靠 timeout 兜底重推（bus #728c6e4307c1 根因）。此处据返回值回退 step
        # 状态为 pending（清 notified_at），让引擎下轮重新走 advance 重发。
        _delivered = self._send_to_role(next_step.target_role, prompt,
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
        # 如果步骤有子工作流模板 → 自动创建子工作流
        if next_step.subflow_template:
            try:
                # 防重：DB 中该步骤已有 subflow_id → 不重复创建
                # （否则每 tick 推进同一历史步骤都会新建 subflow，垃圾任务堆积）
                with lm._conn_tx() as conn2:
                    _db_sr = conn2.execute(
                        "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                        (wf_id,)).fetchone()
                    _db_sdata = (json.loads(_db_sr["step_results"]) if _db_sr and _db_sr["step_results"]
                                 else {}).get(next_step.id, {})
                    if isinstance(_db_sdata, dict) and _db_sdata.get("subflow_id"):
                        return
                    # 防污染：目标角色已有其他活跃任务（子链 target 与主链一致），
                    # 此时角色在执行主链步骤，子链创建只会双线占用角色 → 跳过创建。
                    # 观测案例：pm 子链 queued=17（主链 s2 与子链 s1 同时推给 pm）。
                    #
                    # 注意 exclude_wf_id 传空：原实现传 wf_id（"排除自己"），但这样
                    # 会把"主链 next_step 刚被投递、正占用该角色"这个事实一起排除掉
                    # → 守卫恒 False → 照建同角色子流 → 与主链步骤争抢同一角色
                    # → 子流反复投递失败 → delivery_blocked → 恢复 → 再失败（实测
                    # wf_sub_ee176a2b55a1 的 delivery_fail_count 涨到 2）。
                    # 实测对比：exclude=wf_id → False（漏判）；exclude='' → True（真实）。
                    # 子流是新增占用者，必须看真实占用状态，故不排除本主链。
                    if self._is_role_busy(next_step.target_role, exclude_wf_id="",
                                          pane_fallback=False):
                        LOGGER.info("subflow skip: %s 忙于其他工作流，子链 %s 不创建",
                                    next_step.target_role, next_step.subflow_template)
                        return
                    # 模板名兼容：同仓两条命名并存（裸名 architect_full_design /
                    # WF- 前缀 WF-architect_full_design，见 manager.py:174 同款回退）。
                    # 只按裸名查会让 WL-full_dev_pipeline 这类"裸名声明 + WF- 注册"
                    # 的组合静默失配 → 子链永不创建 → 主链停在首步（实测该链
                    # 历史最佳 0/6 步，7+ 实例全部 delivery_blocked @s1）。
                    _sub_wf = (self._workflows.get(next_step.subflow_template)
                               or self._workflows.get(f"WF-{next_step.subflow_template}"))
                    if _sub_wf and _sub_wf.steps and _sub_wf.steps[0].target_role == next_step.target_role:
                        _prev_done = ""
                        if idx >= 0:
                            _prev_done = results.get(step_id, {}).get("completed_by", "")
                        if not _prev_done:
                            _prev_wf = conn2.execute("SELECT assigner FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
                            _prev_done = _prev_wf["assigner"] if _prev_wf else "pm"
                        _task_id = f"task_sub_{uuid.uuid4().hex[:8]}"
                        _wf_id = f"wf_sub_{uuid.uuid4().hex[:12]}"
                        conn2.execute("INSERT OR IGNORE INTO tasks (task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                            (_task_id, f"{wf_name}/{next_step.id}: {next_step.title}", task_desc, _prev_done, next_step.target_role, time.time(), time.time(), next_step.subflow_template))
                        # 兼容迁移前的表（template_id 列可能缺失）——子工作流 tasks 行的 template_id
                        # 只做溯源用途，缺列时静默降级为 parent_task_id 记录父工作流（2026-08-10 观测）
                        try:
                            conn2.execute("SELECT template_id FROM tasks LIMIT 1")
                        except Exception:
                            conn2.execute("UPDATE tasks SET parent_task_id=? WHERE task_id=?", (wf_id, _task_id))
                            conn2.commit()
                        conn2.execute("INSERT OR IGNORE INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) VALUES (?,?,?,?,?,'pending','s1',?,?,?,?)",
                            (_wf_id, next_step.subflow_template, _task_id, _prev_done, next_step.target_role, json.dumps({}), time.time(), wf_id, next_step.id))
                        conn2.commit()
                        new_results[next_step.id]["subflow_id"] = _wf_id
                        conn2.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(new_results, ensure_ascii=False), wf_id))
                        conn2.commit()
            except Exception as _e:
                LOGGER.error("subflow creation failed for %s: %s", wf_id, _e)

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

        扫描 engineer workspace 下的 cli/*.py，复制到生产路径。
        部署后写 bus cat=code_fix 通知维护者。
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
                               review_step, next_step, results: dict):
        """review 被拒绝 → 解析【系统问题】/【业务问题】分类，分别创建修复子链。

        系统问题 → maintainer_health_monitor（维护者修引擎/模板缺陷）
        业务问题 → engineer_feature（开发修功能实现）

        reviewer 模板要求在完成消息中用 【系统问题】【业务问题】标记分类列出，
        引擎据此分别创建子链——同一份 review 可同时触发两条返工链。
        防重：review 步骤的 step_results 若已有 rework_ids → 不重复创建。
        """
        _done_sr = results.get(getattr(review_step, "id", ""), {}) if isinstance(results, dict) else {}
        _msgs_text = "\n".join(
            m.get("text", "") for m in _done_sr.get("exit_messages", [])
            if isinstance(m, dict)
        )
        # 从结构化分类中提取问题列表
        _sys_issues, _biz_issues = self._parse_review_issues(_msgs_text)
        if not _sys_issues and not _biz_issues:
            LOGGER.info("rework: %s/%s review 无分类问题，跳过返工", wf_id[:8], review_step_id)
            return
        # 问题类型 → (模板, 角色, 分类标签)
        _rework_map = [
            (_sys_issues, "maintainer_health_monitor", "maintainer", "系统/引擎问题"),
            (_biz_issues, "engineer_feature", "engineer", "业务问题"),
        ]
        try:
            with lm._conn_tx() as conn:
                _db_res = conn.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?", (wf_id,)
                ).fetchone()
                if _db_res is None:
                    LOGGER.warning("rework: %s 不存在于 DB，跳过返工", wf_id[:8])
                    return
                _sr = json.loads(_db_res["step_results"]) if _db_res["step_results"] else {}
                _review_sdata = _sr.get(review_step.id, {})
                _existing_reworks = set(
                    _review_sdata.get("rework_ids", []) if isinstance(_review_sdata, dict)
                    else ([_review_sdata["rework_id"]] if isinstance(_review_sdata, dict) and _review_sdata.get("rework_id") else [])
                )
                _new_reworks = []
                for _issues, _tpl, _role, _type_label in _rework_map:
                    if not _issues:
                        continue
                    # 逐类防重（P0 修复）：按模板判断该类 rework 是否已存在，
                    # 而非总数>=模板数——某类模板缺失时另一类不会无限堆积
                    _tpl_exists = any(
                        str(_rw).startswith("wf_rw_") and
                        self._rework_tpl_for(_rw) == _tpl
                        for _rw in _existing_reworks
                    )
                    if _tpl_exists:
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
                    conn.execute(
                        "INSERT OR IGNORE INTO tasks "
                        "(task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) "
                        "VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                        (_task_id, f"{wf_name}/{review_step.id}: 返工({_type_label})", _task_desc,
                         "reviewer", _role, time.time(), time.time(), _tpl))
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
        except Exception as _e:
            LOGGER.warning("rework subflow creation failed %s: %s", wf_id[:8], _e)

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
        """从 review 完成消息中解析【系统问题】和【业务问题】两个分类的问题列表。

        返回 (system_issues, business_issues)。
        """
        _sys = []
        _biz = []
        _sys_keywords = (
            "exit_condition", "变量", "busy", "polling", "rework", "daemon",
            "引擎", "engine", "exit schema", "verify", "timeout", "deadlock",
            "tmux", "pane", "survival", "workflow_engine", "模板", "template",
            "竞态", "race", "卡死", "死循环", "防污", "推送", "notify", "step_result",
        )
        in_sys = False
        in_biz = False
        for line in msgs_text.splitlines():
            _line = line.strip()
            if "【系统问题】" in _line:
                in_sys = True
                in_biz = False
                continue
            if "【业务问题】" in _line:
                in_sys = False
                in_biz = True
                continue
            if not _line or _line.startswith(("##", "#", "---")):
                continue
            # 跳过分类标记行（【系统问题】|无 ）和表头分隔行
            if _line.startswith("【") or _line.startswith("|--"):
                continue
            # 提取问题条目（跳过表头/空行）
            if not (_line.startswith("-") or _line.startswith("|")):
                continue
            _item = _line.lstrip("-|").strip()
            if not _item or len(_item) < 10:
                continue
            # 根据位置（已在系统/业务块中）直接归类；位置不明则用关键词判断
            if in_sys:
                _sys.append(_item)
            elif in_biz:
                _biz.append(_item)
            elif any(kw in _item.lower() for kw in _sys_keywords):
                _sys.append(_item)
            else:
                _biz.append(_item)
        return _sys, _biz

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
                _engine_mod._sp.run(["python3", str(_CCS_CLI), "stop", _role],
                        capture_output=True, timeout=30)
                _engine_mod._sp.run(["python3", str(_CCS_CLI), "start", _role, "--no-attach",
                         "--drive", "infinite"],
                        capture_output=True, timeout=30)
                LOGGER.info("wf %s loop: 生产者 %s 已重启 (iteration %d)",
                            wf_id[:8], _role, _iteration + 1)
            except (ValueError, KeyError, TypeError) as _e:
                LOGGER.warning("loop producer restart failed %s: %s", _role, _e)
        self._bb.write("workflow",
            f"[workflow] {wf.name} 新一轮迭代 {_iteration + 1} 已启动: {_new_wf_id}",
            src="workflow_engine")

    def _fill_prompt_vars(self, prompt: str, step: Step, wf_name: str,
                          task_title: str, task_desc: str, results: dict,
                          prev_step_id: str = "") -> str:
        """填充 prompt 变量：任务上下文 + 步骤间数据传递（上一步 exit_messages）。
        供 _advance_production_wf / _kick_stalled_roles / run_once 复用，保证
        重推与首推拿到的 prompt 一致（避免重推丢上下文）。"""
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
        # 步骤间数据传递：上一步骤的 exit_messages 填充后续步骤变量
        prev_sr = (results or {}).get(prev_step_id, {}) if prev_step_id else {}
        prev_msgs = prev_sr.get("exit_messages", []) if isinstance(prev_sr, dict) else []
        fallback = task_desc or task_title or wf_name
        if prev_msgs:
            prev_text = "\n".join(str(m) for m in prev_msgs)[:3000]
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
