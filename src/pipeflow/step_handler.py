#!/usr/bin/env python3
"""StepHandlerMixin — 步骤级 tick 逻辑 + exit 条件匹配 + schema 校验。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import os
import re
import time
import logging
from pathlib import Path

from pipeflow import engine as _engine_mod  # 属性访问 _engine_mod._sp，patch("pipeflow.engine._sp") 生效
from pipeflow.engine import Step, WorkflowRun
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")


class StepHandlerMixin:
    """WorkflowEngine 的步骤处理 mixin：_tick / _sync_step_results / _validate_exit_schema / _eval_cond / _check_exit。"""

    def _tick(self, run: WorkflowRun, step: Step):
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
        last_ts = _bus_anchor
        _skip_before = run.step_results.get(step.id, {}).get("bus_skip_before", 0)

        # ── 旧引擎产物抢救: exit_messages 已存在但 tick 未闭合 → 直接 complete ──
        _sdata = run.step_results.get(step.id, {})
        if isinstance(_sdata, dict) and _sdata.get("exit_messages") and _sdata.get("status") != "completed":
            try:
                self._lifecycle.complete_step(run.id, step.id)
                LOGGER.info("tick rescue: %s/%s exit_messages pre-existing → completed", run.workflow_name, step.id)
                return
            except Exception as _e:
                LOGGER.debug("tick rescue failed for %s/%s: %s", run.workflow_name, step.id, _e)

        # exit_schema 校验（文件/内容约束）— 放在退出条件匹配之后，
        # 避免 schema 在步骤完成之前就阻塞推进。
        # ── Timeout 检查（每次 tick 都执行，不受 schema/verify 影响）──
        # 超时后自动升级到 coordinator，不会被 schema 失败 return 阻断。
        # 超限阈值: max(max_retries+2, 3) 次超时后自动 cancel，防止无限重试堆积。
        elapsed = time.time() - (run.step_results.get(step.id, {}).get("ts", run.created_at))
        _auto_cancel_at = max(max_retries + 3, 4)
        if elapsed >= timeout:
            # 角色忙（有其他任务执行中）→ 排队语义：不递增 timeout_count，
            # 仅延长响应窗口（避免"忙时被塞任务→未响应→超时回收"的假阳性）
            if self._is_role_busy(step.target_role, exclude_wf_id=run.id):
                # 角色忙 ≠ 任务未完成：先做 exit 匹配，产出已存在则直接推进
                # （否则角色完成产出但忙其他任务时，本工作流无限排队，bus #167081）
                if cat:
                    _busy_match_ts, _busy_match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts)
                    if _busy_match_ts:
                        run.step_results[step.id] = {
                            **run.step_results.get(step.id, {}),
                            "bus_anchor": _busy_match_ts,
                            "exit_messages": _busy_match_msgs,
                        }
                        self._sync_step_results(run.id, run.step_results)
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception as _e:
                            LOGGER.debug("busy exit-match complete_step 失败: %s", _e)
                        return
                _queued = run.step_results.get(step.id, {}).get("queued", 0) + 1
                # 排队上限: 连续排队超过上限（约 3 小时未响应）说明角色
                # 活跃但卡死（pm 类故障）→ 升级 coordinator 而非无限排队
                if _queued >= 6:
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"排队 {_queued} 次仍未响应（角色 {step.target_role} 活跃但可能卡死），请介入",
                        wf_id=run.id, step_id=run.current_step, force=True, scheduler_noise=True)
                run.step_results[step.id] = {
                    **run.step_results.get(step.id, {}),
                    "ts": time.time(),  # 重置计时器，等待角色空闲
                    "queued": _queued,
                }
                self._sync_step_results(run.id, run.step_results)
                return

            timeout_count = run.step_results.get(step.id, {}).get("timeout_count", 0) + 1
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
                "timeout_count": timeout_count,
                "ts": time.time(),  # 重置计时器，给角色响应窗口
            }
            self._sync_step_results(run.id, run.step_results)
            # 超限 → 自动 cancel，阻止死循环升级
            if timeout_count >= _auto_cancel_at:
                try:
                    self._lifecycle.close_wf(run.id, status="cancelled")
                except Exception as _e:
                    LOGGER.warning("auto-cancel close_wf failed: %s", _e)
                self._notify_role("maintainer",
                    f"工作流超时自动回收: {run.workflow_name}/{run.id}",
                    f"step={run.current_step} timeout_count={timeout_count} >= {_auto_cancel_at}\n请排查该步骤为何持续超时。")
                return
            # 重推任务（最多重推 1 次，不自动完成）
            # 角色忙（有其他任务在跑）→ 不强制重推，通知 coordinator 排队
            if timeout_count == 1:
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

            # ── 宽松推进：角色已写同工作流 bus 消息但分类不匹配（如 evolution_report
            #    写成了 architecture）→ 产出存在即推进，避免"产出已存在却死锁"。
            #    触发条件：步骤已超时 >=1 次（角色确实执行过）且非首步。
            #    匹配策略：先找引擎发的 task_spec（含 wf 前缀）→ 再匹配该角色在
            #    task_spec 之后写的任意消息（标题可能不含 wf 前缀，按时间窗匹配）。
            if timeout_count >= 1:
                try:
                    _role_msgs = self._bb.read(limit=100)
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
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception as _e:
                            LOGGER.debug("relaxed-advance complete_step 失败: %s", _e)
                        return
                except Exception as _relax_err:
                    LOGGER.debug("relaxed-advance 检查失败: %s", _relax_err)

            # 无 bus_category 的步骤不走 exit 匹配（纯超时 + 角色 wf complete 驱动），
            # 否则空 cat 匹配所有 bus 消息，导致 engine 自产告警被当作角色产出完成步骤。
            if not cat:
                return
            # exit 匹配必须在 timeout 检查之后、return 之前执行，
            # 否则 notified/running 步骤永远无法通过 exit 条件推进（bus #162804）。
            # 消息去重：同一消息不应被多个工作流同时消费（bus #162146 QA 4任务共享测试结果）
            if not hasattr(self, '_consumed_exit_ids'):
                self._consumed_exit_ids = set()
            _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts)
            if _match_ts:
                # 过滤已被其他工作流消费的消息
                _match_msgs = [m for m in _match_msgs if m["id"] not in self._consumed_exit_ids]
                _match_ts = _match_msgs[0]["ts"] if _match_msgs else 0.0
            if _match_ts:
                # 标记消息已消费，防止其他工作流重复匹配（bus #162146）
                self._consumed_exit_ids.update(m["id"] for m in _match_msgs)
                run.step_results[step.id] = {
                    **run.step_results.get(step.id, {}),
                    "bus_anchor": _match_ts,
                    "exit_messages": _match_msgs,
                }
                self._sync_step_results(run.id, run.step_results)
                try:
                    self._lifecycle.complete_step(run.id, step.id)
                except Exception as _e:
                    LOGGER.debug("exit-match complete_step 失败: %s", _e)
                return
            return

        # 无 bus_category 的步骤不走 exit 匹配（纯超时 + 角色 wf complete 驱动），
        # 否则空 cat 匹配所有 bus 消息，导致 engine 自产告警被当作角色产出完成步骤。
        if not cat:
            return
        _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts)
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
                ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
                vcmd = step.verify
                for k, val in ctx.items():
                    vcmd = vcmd.replace(f"{{{k}}}", str(val))
                # verify 命令可能含 && / 管道等 shell 语法，shlex.split 会把 && 当
                # 参数传给第一个命令导致 "extra argument" 报错 → 用 shell=True 执行。
                # verify 命令来自模板注册表（可信内部源），非外部输入，无注入面。
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

            # ── 自动确认 handoff 步骤，推进到下一棒 ──
            # daemon 替代人工 approve，用 step 角色名作为密钥
            # handoff 步骤需等人工审批，不自动确认
            if step.type == "handoff":
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
        return

    def _sync_step_results(self, wf_id: str, step_results: dict):
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

    def _eval_cond(self, expr: str, run: WorkflowRun) -> bool:
        """简朴条件求值: s1.status == 'done' 格式。"""
        import re as _re
        m = _re.match(r"s(\d+)\.status\s*==\s*'(\w+)'", expr.strip())
        if not m:
            return False
        step_id = f"s{m.group(1)}"
        expected = m.group(2)
        sr = run.step_results.get(step_id)
        if not sr:
            return False
        return sr.get("status") == expected

    def _check_exit(self, cat: str, src_filter: str, text_filter: str, created_after: float = None) -> tuple:
        """检查 bus 是否有匹配 exit_condition 的消息。返回 (timestamp, matched_msgs)"""
        facts = self._bb.read(cat=cat, limit=50) if cat else self._bb.read(limit=50)
        earliest = 0.0
        matched = []
        for f in facts:
            # src 精确匹配；src=claude（bus 默认值）视为角色未设 src，不排除
            if src_filter and f.src != src_filter and f.src != "claude":
                continue
            if text_filter and text_filter not in f.t:
                continue
            if created_after and f.ts < created_after:
                continue
            matched.append({"id": f.id, "text": f.t[:200], "ts": f.ts, "src": f.src})
            if earliest == 0 or f.ts < earliest:
                earliest = f.ts
        return (earliest, matched)
