#!/usr/bin/env python3
"""RoleLifecycleMixin — 角色存活管理、忙检测、tmux 操作、通知/推送。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import time
import logging
import threading
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from paths import CCS_CLI as _CCS_CLI

LOGGER = logging.getLogger("workflow.engine")


class RoleLifecycleMixin:
    """WorkflowEngine 的角色管理 mixin。"""
    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"
    _OVERFLOW_THRESHOLD_LINES = 2000   # pane scrollback 行数超过此值触发 /compact（备用指标）
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    _last_compact: dict[str, float] = {}
    # check-then-act 竞态防护：cooldown 检查与写入之间加锁
    _COMPACT_LOCK = threading.Lock()

    def _check_context_overflow(self):
        """检测角色 tmux pane 中 Claude 的上下文溢出信号，自动发 /compact。

        检测两个信号：
        - "Context limit reached"（Claude 阻塞等待 /compact）
        - "Context low"（Claude 提示上下文不足）

        覆盖两个来源：
        - DB 中 running/pending workflow 的角色
        - 所有实际存在的 ccs-* tmux session（覆盖非 workflow 启动的角色）
        """
        roles = set()
        # 来源1: DB
        try:
            for row in self._lifecycle.query(
                "SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"
            ):
                if row["assignee"]:
                    roles.add(row["assignee"])
        except Exception:
            pass

        # 来源2: 实际 tmux 会话（兜底：覆盖手动启动或 DB 查不到的角色）
        try:
            r = _engine_mod._sp.run(
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                for name in r.stdout.strip().split("\n"):
                    name = name.strip()
                    if name.startswith("ccs-"):
                        role_name = name[4:]
                        if role_name:
                            roles.add(role_name)
        except Exception:
            pass

        if not roles:
            return

        now = time.time()
        for role in roles:
            tmux_name = f"ccs-{role}"
            with self._COMPACT_LOCK:
                last = self._last_compact.get(role, 0.0)
                if now - last < self._OVERFLOW_COOLDOWN:
                    continue

            # 抓最近 500 行输出，搜索 Claude 的上下文溢出信号
            try:
                r = _engine_mod._sp.run(
                    ["tmux", "capture-pane", "-p", "-t", f"{tmux_name}:0.0", "-S", "-500"],
                    capture_output=True, text=True, timeout=5,
                )
            except Exception:
                continue
            if r.returncode != 0:
                continue
            output = r.stdout or ""

            # 信号匹配：Context limit reached / Context low / Context (数字% remaining)
            has_overflow = (
                "Context limit reached" in output
                or "Context low" in output
                or ("Context" in output and "remaining" in output and "compact" in output.lower())
            )

            if not has_overflow:
                continue

            LOGGER.info("上下文溢出: %s 检测到溢出信号，发送 /compact", role)
            try:
                _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "/compact", "Enter"],
                    capture_output=True, timeout=5,
                )
                with self._COMPACT_LOCK:
                    self._last_compact[role] = now
                self._bb.write("architecture",
                    f"[context_overflow] {role} → /compact",
                    src="workflow_engine")
            except Exception as e:
                LOGGER.warning("compact send failed for %s: %s", role, e)

    def _kick_stalled_roles(self):
        """检测有 running workflow 但 steps 停滞超时的角色，重推任务。

        条件: step 处于 notified 状态超过 3 分钟，且无 timeout_count 递增
        (timeout_count 由 tick 处理，这里补 tick 照顾不到的冷启动停滞)
        """
        from pipeflow.engine import _UNMATCHED_VAR_RE
        lm = self._lifecycle
        now = time.time()
        try:
            rows = lm.query(
                "SELECT instance_id, template_id, current_step_id, step_results, assignee "
                "FROM workflow_instances WHERE status='running'"
            )
            for row in rows:
                _results = json.loads(row["step_results"] or "{}")
                _sid = row["current_step_id"]
                _sr = _results.get(_sid, {})
                _status = _sr.get("status", "")
                if _status != "notified":
                    continue
                _notified_at = _sr.get("notified_at", 0)
                if not _notified_at or now - _notified_at < 180:
                    continue
                # 停滞超过 10 分钟 + 无 timeout_count → 重推
                _tc = _sr.get("timeout_count", 0)
                if _tc > 0:
                    continue  # tick 已经在处理
                _wf = self._workflows.get(row["template_id"])
                if not _wf:
                    continue
                _step = next((s for s in _wf.steps if s.id == _sid), None)
                if not _step:
                    continue
                # 用当前步骤的 target_role（同其他修复一致），不是 assignee
                _role = _step.target_role
                if not _role or _role in ("coordinator",):
                    continue
                # 角色已有挂起的任务 → 不打扰
                if self._is_role_busy(_role):
                    continue
                LOGGER.info("kick-stalled: %s/%s role=%s notified=%ds ago",
                            row["template_id"], row["instance_id"], _role, now - _notified_at)
                # 重推用统一变量填充（任务上下文 + 上一步产出），与首推一致
                _task_row = lm.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (row["instance_id"],)
                )
                _task = _task_row[0] if _task_row else None
                _tt = _task["title"] if _task else row["template_id"]
                _td = _task["description"] if _task else ""
                _prev_sid = ""
                _wf_steps = _wf.steps
                _idx = next((i for i, s in enumerate(_wf_steps) if s.id == _sid), -1)
                if _idx > 0:
                    _prev_sid = _wf_steps[_idx - 1].id
                _prompt = self._fill_prompt_vars(
                    _step.prompt_template, _step, row["template_id"],
                    _tt, _td, _results, _prev_sid)
                if _UNMATCHED_VAR_RE.search(_prompt):
                    LOGGER.warning("kick-stalled 跳过 %s/%s: 含未替换变量 %s",
                                   row["template_id"], _sid, _UNMATCHED_VAR_RE.findall(_prompt))
                    continue
                self._send_to_role(_role, _prompt,
                                   wf_id=row["instance_id"], step_id=_sid)
        except Exception as _e:
            LOGGER.debug("kick_stalled error: %s", _e)

    @staticmethod
    def _is_agent_alive(role: str) -> bool:
        """检查 CCS tmux pane 内是否有 claude agent 进程在运行。"""
        try:
            r = _engine_mod._sp.run(
                ["tmux", "list-panes", "-t", f"ccs-{role}",
                 "-F", "#{pane_current_command}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode != 0 or not r.stdout.strip():
                return False
            cmd = r.stdout.strip().split("\n")[0]
            return cmd in ("claude", "claude-code", "python3", "python")
        except (ValueError, KeyError, TypeError):
            return False

    def _ensure_role_alive(self, role: str) -> bool:
        """检查角色的 CCS 是否存活（tmux + claude 进程），不存活则拉起。"""
        alive = _engine_mod._sp.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=5,
        ).returncode == 0
        if alive and not self._is_agent_alive(role):
            # tmux 会话存活但无 claude 进程 → 重启
            LOGGER.info("CCS %s: tmux alive but no claude, restarting", role)
            _engine_mod._sp.run(["tmux", "kill-session", "-t", f"ccs-{role}"],
                    capture_output=True, timeout=5)
            alive = False
        if alive:
            # 代码变更检测已禁用：workspace 的 .py 是角色自己的产出（正常开发），
            # 全量 md5 扫描会误判"代码变更"→ 杀死正在工作的角色（2026-08-01 观测 8 次误杀）。
            # 如需热替换 launcher 代码，由运维手动 ccs restart 完成。
            return True

        # 拉起 CCS（infinite 模式，agent 常驻执行任务）
        # ponytail: ondemand 模式只写哨兵不创建 tmux 会话，必须用 infinite 让 agent 持续运行
        try:
            _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "infinite"],
                capture_output=True, timeout=30,
            )
            for _ in range(30):
                time.sleep(1)
                if _engine_mod._sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=5).returncode == 0:
                    time.sleep(3)  # 等 agent 进程启动
                    if self._is_agent_alive(role):
                        return True
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("CCS role alive check failed for %s", role)
        return False

    def _is_role_busy(self, role: str, exclude_wf_id: str = "", pane_fallback: bool = True) -> bool:
        """检查角色是否真的在工作。

        信号1: survival monitor 健康文件（survival_monitor.py 每 120s 更新）
          - stale/idle/unknown → 角色不忙，跳过数据库检查直接返回 False
        信号2: workflow_instances 表中是否有该角色的其他活跃步骤（兜底）
        pane_fallback: True=tmux pane 存活即视为 busy（超时保护），False=仅 DB 检查（首次投递）
        """
        # 信号1: survival health 文件（直接读，不重跑检测）
        try:
            hp = self._HEALTH_DIR / f"{role}.json"
            if hp.exists():
                h = json.loads(hp.read_text())
                overall = h.get("survival_overall", "unknown")
                # idle/unknown → 查 tmux pane 兜底：tmux 3.4 的 pane_activity 恒为空，
                # survival L2 信号失效，忙任务被误判 idle → 超时误回收。pane 存活即视为 busy。
                if overall in ("idle", "stale", "dead", "orphan", "unknown"):
                    if pane_fallback and self._tmux_pane_alive(role):
                        LOGGER.info("health=%s 但 tmux pane 存活 → 视为 busy (survival L2 兜底)", overall)
                        return True
                    return False
                # healthy: 角色可能在工作，继续信号2确认
                # stale/l2=false: 角色空闲，放行
                if h.get("survival_l2_thinking") is False:
                    if pane_fallback and self._tmux_pane_alive(role):
                        LOGGER.info("l2_thinking=false 但 tmux pane 存活 → 视为 busy (survival L2 兜底)")
                        return True
                    return False
        except Exception:
            pass  # 健康文件损坏，退化到信号2
        # 信号2: DB 兜底——角色健康但 workflow 层面已有别的活跃步骤
        # 按当前步骤的 target_role 匹配（同 _role_has_pending_assignment 语义）
        try:
            rows = self._lifecycle.query(
                "SELECT instance_id, template_id, current_step_id, step_results "
                "FROM workflow_instances "
                "WHERE status='running'"
            )
            for r in rows:
                if exclude_wf_id and r["instance_id"] == exclude_wf_id:
                    continue
                _wf = self._workflows.get(r["template_id"])
                if not _wf:
                    continue
                _step = next((s for s in _wf.steps if s.id == r["current_step_id"]), None)
                if not _step or _step.target_role != role:
                    continue
                sr = json.loads(r["step_results"] or "{}")
                sdata = sr.get(r["current_step_id"], {})
                if isinstance(sdata, dict) and sdata.get("status") in ("notified", "running"):
                    _q = sdata.get("queued", 0)
                    if _q >= 6:
                        LOGGER.info("%s queued=%d >= 6 → 不再视为 busy", role, _q)
                        continue
                    if sdata.get("timeout_count", 0) < 3:
                        LOGGER.info("skip: %s busy with %s/%s (step=%s)",
                                    role, r["instance_id"][:8], r["current_step_id"], sdata.get("status"))
                        return True
            return False
        except Exception:
            return False

    def _tmux_pane_alive(self, role: str) -> bool:
        """tmux pane 兜底信号：pane 存活即视为角色在忙。

        tmux 3.4 的 pane_activity 恒为空导致 survival L2 失效（详见 survival_monitor._l2_check），
        这里用 pane 存在 + active 作为低误报兜底。"""
        try:
            r = _engine_mod._sp.run(
                ["tmux", "list-panes", "-t", f"ccs-{role}", "-F", "#{pane_active}"],
                capture_output=True, text=True, timeout=3,
            )
            if r.returncode == 0 and "1" in r.stdout:
                return True
        except Exception:
            pass
        return False

    def _notify_role(self, role: str, title: str, evidence: str):
        """告警通知：写 blocker bus + ccs send 唤醒角色（绕过并发检查）。"""
        self._bb.write("blocker", title, evidence=evidence, src="workflow_engine")
        try:
            _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "send", role,
                 f"[workflow 告警] {title}\n\n{evidence}",
                 "--from", "workflow_engine"],
                capture_output=True, timeout=15,
            )
        except Exception:
            pass

    def _send_to_role(self, role: str, prompt: str,
                       wf_id: str = "", step_id: str = "", force: bool = False,
                       scheduler_noise: bool = False):
        """确保角色 CCS 存活，写完整 task_spec 到 bus，再 ccs send 推送全文。
        force=True 时跳过 pending/busy 检查，用于超时重推。"""
        # 并发保护：角色已有另一个 workflow 的活跃步骤时，跳过推送
        # pane_fallback=False：首次投递仅查 DB，不因 tmux 存活而阻塞
        if not force and wf_id and self._is_role_busy(role, exclude_wf_id=wf_id, pane_fallback=False):
            LOGGER.info("send-to-role %s 跳过: 有其他 workflow 在进行中 (wf=%s step=%s)",
                        role, wf_id[:12], step_id)
            return
        # 未替换变量检测：含 {xxx} 模板变量的 prompt 不应发送，否则角色 /goal 用
        # 这些内容作 StopHook 条件时 bool([]) 为 False → 死循环
        from pipeflow.engine import _UNMATCHED_VAR_RE
        if _UNMATCHED_VAR_RE.search(prompt):
            unmatched = _UNMATCHED_VAR_RE.findall(prompt)
            LOGGER.warning("send-to-role %s 跳过: prompt 含未替换变量 %s (wf=%s step=%s)",
                          role, unmatched, wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] {role} prompt 含未替换变量: {unmatched}",
                evidence=prompt[:500], src="workflow_engine")
            return
        self._ensure_role_alive(role)
        # ponytail: prompt_template 已含 /goal 前缀，避免重复叠加导致畸形
        import re as _re
        if not prompt.startswith(("/goal", "/GOAL", "/Goal")):
            prompt = "/goal " + prompt
        # 第二层防护：拒绝空目标或前缀污染（bus #141691 根因：q/goal [] 导致死循环）
        # 用正则剥掉 /goal 前缀（lstrip 字符集会误伤内容开头的 g/o/a/l）
        _goal_content = _re.sub(r"^/goal\s*", "", prompt, flags=_re.IGNORECASE).strip()
        if len(_goal_content) < 10:
            LOGGER.warning("send-to-role %s 拒绝: goal 内容过短 (len=%d, wf=%s step=%s)",
                          role, len(_goal_content), wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] {role} goal 内容被拒绝: len={len(_goal_content)}",
                evidence=prompt[:300], src="workflow_engine")
            return
        # coordinator 只接收真实任务（task_spec 语义），排队/超时/告警升级消息
        # 写 blocker 即可，不 ccs send —— 避免 /goal 前缀注入 coordinator 死锁
        # （bus #141691 根因：q/goal [] 循环。coordinator 是调度者不是目标执行者）
        # 噪声由调用点显式标记（scheduler_noise=True），不再按消息内容猜——
        # 内容含"超时/排队"的真实任务（如"检查超时配置合理性"）会被旧启发式误杀
        if scheduler_noise and role == "coordinator":
            LOGGER.info("send-to-role coordinator 跳过: 调度噪音消息 (wf=%s step=%s)",
                        wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] coordinator 调度噪音已拦截: {wf_id}/{step_id}",
                evidence=prompt[:200], src="workflow_engine")
            return
        # 写 TASKS.md 到角色 workspace，让模板中"读 TASKS.json"等指令能找到具体任务
        if wf_id:
            try:
                ws_dir = Path.home() / "ccs-workspaces" / role
                ws_dir.mkdir(parents=True, exist_ok=True)
                task_rows = self._lifecycle.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (wf_id,)
                ) if hasattr(self, '_lifecycle') else []
                if task_rows:
                    t = task_rows[0]
                    (ws_dir / "TASKS.md").write_text(
                        f"# 工作流任务\n\n## 标题\n{t['title'] or '?'}\n\n"
                        f"## 描述\n{t['description'] or t['title'] or '?'}\n\n"
                        f"## 来源\n工作流 {wf_id} / 步骤 {step_id}\n",
                        encoding="utf-8")
            except (ValueError, KeyError, TypeError):
                pass
        # 超时/告警通知走 blocker 而非 task_spec，防止 coordinator 误认为新任务
        _is_warning = any(kw in prompt for kw in ["持续超时", "异常", "超时自动回收"])
        if _is_warning:
            _title = f"[workflow] {role} 告警: {wf_id}/{step_id}" if wf_id else f"[workflow] {role} 告警"
            self._bb.write("blocker", _title, evidence=prompt, src="workflow_engine")
        else:
            _title = f"needs_implementation @{role} 工作流任务: {wf_id}/{step_id}" if wf_id else f"@{role} 工作流任务"
            self._bb.write("task_spec", _title, evidence=prompt, src="workflow_engine")
        # ccs send 推送全文（参数顺序：ccs.py send <target_role> <message> --from <source>）
        try:
            _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "send", role, prompt,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=30,
            )
        except (ValueError, KeyError, TypeError):
            try:
                ccs_cli = Path.home() / "session-launcher" / "src" / "ccs.py"
                _engine_mod._sp.run(
                    ["python3", str(ccs_cli), "send", role, prompt[:2000]],
                    capture_output=True, timeout=10,
                )
            except Exception as _e:
                LOGGER.exception("fallback CCS send failed for %s", role)
                try:
                    self._bb.write("code_fix", "pipeflow: fallback CCS send failed role=" + role,
                                   evidence=str(_e)[:200], src="pipeflow")
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("bus write fail after fallback CCS send")
