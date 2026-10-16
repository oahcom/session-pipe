#!/usr/bin/env python3
"""RoleLifecycleMixin — 角色存活管理、忙检测、tmux 操作、通知/推送。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import calendar
import json
import re
import sys
import time
import logging
import threading
import os as _os
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from paths import CCS_CLI as _CCS_CLI
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")

# kick-stalled 重推冷却（秒）：原实现每个 daemon tick 都重推，实测 33 分钟
# 398 次（约 5s 一次），把同一 prompt 灌满角色队列 —— pane 显示
# "Press up to edit queued messages"，角色被自己的重复指令淹没。
# 停滞是真实状态，但不该靠刷屏解决：同一 step 每 10 分钟最多重推 1 次。
_KICK_COOLDOWN_SEC = 600


class RoleLifecycleMixin:
    """WorkflowEngine 的角色管理 mixin。"""
    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"
    _OVERFLOW_THRESHOLD_LINES = 2000   # pane scrollback 行数超过此值触发 /compact（备用指标）
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    _last_compact: dict[str, float] = {}
    # check-then-act 竞态防护：cooldown 检查与写入之间加锁
    _COMPACT_LOCK = threading.Lock()
    # 角色 transcript 根目录（与 eval_checker._TRANSCRIPTS_DIR 同一处口子）
    _TRANSCRIPTS_ROOT = Path.home() / ".claude" / "projects"

    _OVERFLOW_ERR_MAX_AGE = 600.0   # API 错误多久内算"刚发生"（秒）

    def _transcript_shows_overflow(self, role: str) -> bool:
        """读角色最新 transcript 尾部，判断是否**最近**落过 API 错误（溢出）。

        按**时间**而非距文件尾的字节数判断。实测错误行距文件尾 7.8KB~10.8MB
        （35 个会话样本），且距末尾最多 4111 行 —— 任何固定字节/行数窗口都会漏。
        错误事件自带 timestamp，故读尾部一段、取其中最新错误行的 ts 与 now 比。

        任何异常返回 False —— 宁可不提醒，也不凭推测发 /compact。
        """
        try:
            # 分隔符归一化：persona 角色名用下划线（product_architect），transcript
            # 目录用连字符。不归一则该角色恒返回 False —— 溢出检测完全失效。
            _needle = f"ccs-workspaces-{role}".replace("_", "-").lower()
            dirs = [d for d in self._TRANSCRIPTS_ROOT.iterdir()
                    if d.is_dir() and _needle in d.name.replace("_", "-").lower()]
            files = [f for d in dirs for f in d.glob("*.jsonl")]
            if not files:
                return False
            newest = max(files, key=lambda p: p.stat().st_mtime)
            # 只读尾部 64KB：足够容纳最近若干轮，避免读 10MB+ 全文
            with newest.open(encoding="utf-8", errors="replace") as fh:
                fh.seek(max(0, newest.stat().st_size - 65536))
                tail = fh.read()
            newest_err = 0.0
            for line in tail.splitlines():
                if "isApiErrorMessage" not in line:
                    continue
                try:
                    ev = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                if not ev.get("isApiErrorMessage"):
                    continue
                # transcript ts 为 UTC（形如 2026-10-16T14:23:31.358Z），
                # 用 calendar.timegm 直接按 UTC 解析，避免 mktime 的本地时区偏移
                ts = ev.get("timestamp", "")[:19]
                try:
                    epoch = calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%S"))
                except (ValueError, TypeError):
                    continue
                newest_err = max(newest_err, epoch)
            return bool(newest_err) and (time.time() - newest_err) <= self._OVERFLOW_ERR_MAX_AGE
        except (OSError, ValueError):
            return False

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
            # "Prompt is too long" 是实测最高频的溢出措辞（全库 18 次 vs
            # Context limit reached 13 / Context low 10），此前漏检导致角色
            # 溢出后无人 compact（maintainer/scout/closer 各 1 次，closer 的
            # transcript 累积到 4MB 才爆）。措辞由 Claude Code 版本决定，故一并匹配。
            has_overflow = (
                "Context limit reached" in output
                or "Context low" in output
                or "Prompt is too long" in output
                or ("Context" in output and "remaining" in output and "compact" in output.lower())
            )

            # 备用信号：transcript 尾部的 API 错误。pane 信号有结构性盲区 ——
            # claude TUI 跑在备用屏（alternate_on=1），该屏**无 scrollback**：
            # `-S -500` 与 `-S -5000` 实测返回完全相同的字节数（2026-10-16 实测
            # curator/qa/pm 各 828/782/812 字符，两者逐字节相同）。故 pane 只能看到
            # 当前视口，溢出错误一旦滚出视口或 agent 已死即永久不可见。
            # 本检测每 10s 轮询，而 scout 实测在错误出现后 1s 才被检测到、此时
            # agent 已终止，send-keys 无处可发（bus #361060）。
            # transcript 是落盘事实，不受视口/进程存活影响，且只读文件尾 64KB。
            if not has_overflow:
                has_overflow = self._transcript_shows_overflow(role)

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
        from pipeflow.engine import UNMATCHED_VAR_RE as _UNMATCHED_VAR_RE
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
                # 角色最近有产出 → 正在干活，不打扰（2026-08-10 观测：
                # scout Orchestrating 中，kick-stalled 201s 即重推，浪费 token）
                # 用 bus 产出而非 pane_active: tmux 3.4 单 pane 会话 pane_active 恒 1
                # → pane 判断恒"存活"导致停滞重推整体失效
                if self._recently_producing(_role):
                    continue
                # 停滞超过 3 分钟 + pane 不存活 + 无 timeout_count → 重推
                _tc = _sr.get("timeout_count", 0)
                if _tc > 0:
                    continue  # tick 已经在处理
                # 角色已有挂起的任务 → 不打扰
                # 排除自己：检验中就是当前停滞步骤，若不排除会把自身判 busy，
                # 导致停滞步骤永不重推（2026-08-10 观测 wf_cde32/s1 skip 30 分钟）
                if self._is_role_busy(_role, exclude_wf_id=row["instance_id"]):
                    continue
                # 冷却：同一 step 每 _KICK_COOLDOWN_SEC 最多重推一次。
                # 时间戳存 step_results（跨 tick 持久化，daemon 重启仍有效），
                # 与 track_notified_at / last_heal 同范式。
                _last_kick = _sr.get("kick_stalled_at", 0)
                if _last_kick and now - _last_kick < _KICK_COOLDOWN_SEC:
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
                # 投递后立即落冷却戳。不落库则每 tick 重推（本函数每 tick 都跑，
                # 而 run 每 tick 从 DB 重建 → 内存里的 _sr 改动下一轮即丢）。
                _sr["kick_stalled_at"] = now
                _results[_sid] = _sr
                self._sync_step_results(row["instance_id"], _results)
        except Exception as _e:
            LOGGER.debug("kick_stalled error: %s", _e)

    @staticmethod
    def _is_agent_alive(role: str) -> bool:
        """检查 CCS tmux pane 是否存活。

        ponytail: 用 pane 存活判定而非进程名白名单——claude agent 执行 /goal 时
        pane_current_command 可能是子进程 wrapper（bash/sleep/node），不在
        白名单会误判"无 agent"→ kill-session 杀掉正在工作的角色（观测：pm
        工作时被杀 8 次，2026-08-09）。tmux 会话存在即视为 agent 存活，
        只有 pane 完全消失才判定失活。
        """
        try:
            r = _engine_mod._sp.run(
                ["tmux", "list-panes", "-t", f"ccs-{role}",
                 "-F", "#{pane_pid}"],
                capture_output=True, text=True, timeout=5,
            )
            # pane 存在且有 pid → agent 存活；pane 消失/无输出 → 失活
            return r.returncode == 0 and bool(r.stdout.strip())
        except (_engine_mod._sp.TimeoutExpired, OSError, ValueError, KeyError, TypeError):
            # 探测失败 ≠ 角色死亡。返回 None 让调用方区分"判死"与"判不出"，
            # 否则 tmux 瞬时不可达（实测存在 578ms 抖动）就会被当成失活。
            return None


    def _ensure_role_alive(self, role: str) -> bool:
        """检查角色的 CCS 是否存活（tmux + claude 进程），不存活则拉起。"""
        alive = _engine_mod._sp.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=5,
        ).returncode == 0
        if alive:
            # _is_agent_alive 返回 None = 探测失败（非"判死"）。tmux 瞬时不可达
            # 时若不区分，会 kill 掉正在工作的角色。宁可放过一次真死（下一 tick
            # 重查），不可误杀一次活角色。
            agent = self._is_agent_alive(role)
            if agent is None or agent:
                # 代码变更检测已禁用：workspace 的 .py 是角色自己的产出（正常开发），
                # 全量 md5 扫描会误判"代码变更"→ 杀死正在工作的角色（2026-08-01 观测 8 次误杀）。
                # 如需热替换 launcher 代码，由运维手动 ccs restart 完成。
                return True
            # 二次确认：kill 是破坏性动作，首判可能因探测抖动/启动竞态假阴性
            # （观测：scout 04:22-09:02 建 25+ 会话，含 1 秒即死实例）。
            time.sleep(1)
            if self._is_agent_alive(role) is not False:
                return True
            LOGGER.info("CCS %s: tmux alive but no claude, restarting", role)
            _engine_mod._sp.run(["tmux", "kill-session", "-t", f"ccs-{role}"],
                    capture_output=True, timeout=5)
            alive = False

        # 拉起 CCS（infinite 模式，agent 常驻执行任务）
        # ponytail: ondemand 模式只写哨兵不创建 tmux 会话，必须用 infinite 让 agent 持续运行
        # 关键修复: 增加超时到 60s，并移除 VIRTUAL_ENV 以加速启动（从 30s 超时优化）
        _env = _os.environ.copy()
        _env.pop("VIRTUAL_ENV", None)
        _env.pop("CONDA_PREFIX", None)
        try:
            _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "infinite", "--no-auto-send"],
                capture_output=True, timeout=60, env=_env,
            )
            for _ in range(30):
                time.sleep(1)
                if _engine_mod._sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=5).returncode == 0:
                    time.sleep(3)  # 等 agent 进程启动
                    if self._is_agent_alive(role):
                        # 审计信号：记录角色被 workflow 引擎拉起（供 monitor_audit 查看）
                        # 写失败不阻断拉起（审计是旁路，DB 锁/连接异常不应让角色拉起中断）
                        try:
                            self._bb.write("monitor_audit",
                                f"决策: 角色拉起 → {role} CCS 已重启 (workflow 驱动)",
                                src="workflow_engine")
                        except Exception:
                            LOGGER.debug("monitor_audit write failed for %s", role)
                        return True
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("CCS role alive check failed for %s", role)
        return False

    def _wait_for_role_ready(self, role: str, wf_id: str = "", step_id: str = "",
                              max_wait: int = 90, check_interval: int = 5) -> bool:
        """等待角色真正就绪：tmux 存活 + claude 进程存活 + 不在 busy 状态。

        这是 dispatch 前的关键检查，确保角色真正就绪才发送任务。
        """
        import time
        deadline = time.time() + max_wait
        check_interval = 5
        while time.time() < deadline:
            # 1. tmux session 存活
            try:
                alive = _engine_mod._sp.run(
                    ["tmux", "has-session", "-t", f"ccs-{role}"],
                    capture_output=True, timeout=5
                ).returncode == 0
            except Exception:
                alive = False

            if not alive:
                # 拉起角色
                LOGGER.info("_wait_for_role_ready: role %s not alive, starting...", role)
                try:
                    _env = _os.environ.copy()
                    _env.pop("VIRTUAL_ENV", None)
                    _env.pop("CONDA_PREFIX", None)
                    _engine_mod._sp.run(
                        ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                         "--drive", "infinite", "--no-auto-send"],
                        capture_output=True, timeout=60, env=_env
                    )
                    # 等待 tmux session 启动
                    for _ in range(10):
                        time.sleep(1)
                        if _engine_mod._sp.run(
                            ["tmux", "has-session", "-t", f"ccs-{role}"],
                            capture_output=True, timeout=5
                        ).returncode == 0:
                            time.sleep(3)
                            break
                except Exception as e:
                    LOGGER.warning("Failed to start role %s: %s", role, e)
                    time.sleep(check_interval)
                    continue

            # 2. 检查 claude 进程是否存活
            if not self._is_agent_alive(role):
                LOGGER.info("Role %s tmux alive but no claude process, waiting...", role)
                time.sleep(5)
                continue

            # 3. 检查是否 busy（如果 busy 说明正在处理其他任务，这其实是好的）
            busy = self._is_role_busy(role, exclude_wf_id=wf_id, pane_fallback=True)
            if busy:
                LOGGER.info("Role %s is busy (processing other work), ready to queue", role)
                return True  # busy 说明角色在工作，可以排队

            # 角色空闲且存活，可以发送任务
            LOGGER.info("Role %s is ready (idle & alive)", role)
            return True

            time.sleep(5)

        LOGGER.warning("_wait_for_role_ready: role %s not ready after %ds", role, max_wait)
        return False

    def _is_role_busy(self, role: str, exclude_wf_id: str = "", pane_fallback: bool = True) -> bool:
        """检查角色是否真的在工作。

        委托统合模块 liveness.role_liveness(level=4) —— 与旧实现的信号1（health 文件）
        + 信号2（DB 步骤级扫描）完全一致，见 liveness._health_gate / _s4_stepping。
        """
        from liveness import role_liveness
        try:
            res = role_liveness(
                role, level=4, exclude_wf_id=exclude_wf_id,
                db_conn=self._lifecycle, pane_fallback=pane_fallback,
                health_dir=self._HEALTH_DIR, workflows=self._workflows,
            )
            return res["busy"]
        except Exception:
            return False

    def _recently_producing(self, role: str) -> bool:
        """角色最近 180s 是否有 bus 产出（比 pane_active 可靠: 单 pane 恒 1）。

        原实现调 `bus_client.py read --src <role>`，而 read 子命令只接受
        --cat/--limit/--consume/--watch/--json（无 --src）→ argparse 退出 2 →
        异常被 except 吞 → 恒返回 False，停滞守卫形同虚设（实测 33 分钟内
        kick-stalled 398 次）。且时间戳字段名取 created_at/timestamp，而
        Fact 用 ts，双重取不到值。
        Blackboard.read() 原生支持 src=，直接走进程内查询，免子进程。
        """
        try:
            facts = self._bb.read(src=role, limit=5)
            if facts:
                recent_ts = max(float(getattr(f, "ts", 0) or 0) for f in facts)
                return time.time() - recent_ts < 180
        except Exception as _e:
            LOGGER.debug("_recently_producing(%s) failed: %s", role, _e)
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
            return False  # 投递未发生：调用方据此不可盖戳 notified，避免"未投递却标记已通知"掩盖死锁
        # 未替换变量检测：含 {xxx} 模板变量的 prompt 不应发送，否则角色 /goal 用
        # 这些内容作 StopHook 条件时 bool([]) 为 False → 死循环
        from pipeflow.engine import UNMATCHED_VAR_RE as _UNMATCHED_VAR_RE
        if _UNMATCHED_VAR_RE.search(prompt):
            unmatched = _UNMATCHED_VAR_RE.findall(prompt)
            LOGGER.warning("send-to-role %s 跳过: prompt 含未替换变量 %s (wf=%s step=%s)",
                          role, unmatched, wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] {role} prompt 含未替换变量: {unmatched}",
                evidence=prompt[:500], src="workflow_engine")
            return False  # 投递未发生
        # 幽灵 wf_id 检测：wf_id 声称是某个工作流实例，但权威库里查不到它。
        # 此时后续的 TASKS.md 写入会以该假 ID 占位，防污染逻辑随即把**真实**
        # 任务判为"角色已有其他任务"而排队 —— 角色于是永远收不到真任务。
        # 实测（2026-10-16 21:16-21:17）：scout 收到 wf_97c7f6a8e0ee/s1（库中
        # 零命中）后 35 分钟内零产出，真实实例 wf_6ff71a38016c 卡在 s1 直至超时。
        # 不阻断（测试与 subflow 路径会传合成 ID），但必须留下可见痕迹。
        if wf_id:
            try:
                _missing = not self._lifecycle.query_one(
                    "SELECT 1 FROM workflow_instances WHERE instance_id=?", (wf_id,))
            except Exception as _e:
                LOGGER.debug("ghost-wf-id check failed: %s", _e)
                _missing = False
            if _missing:
                LOGGER.error("send-to-role %s: wf_id %s 不在权威库（幽灵 ID），"
                             "step=%s —— 投递将污染 TASKS.md 并可能使真实任务排队",
                             role, wf_id, step_id)
                self._bb.write("blocker",
                    f"[workflow] 幽灵 wf_id: {wf_id}/{step_id} 不在权威库",
                    evidence=f"role={role} prompt_head={prompt[:200]}",
                    src="workflow_engine")
        # 关键修复：确保角色存活并就绪，不只是 tmux 存活
        # 1. 先确保 tmux session 存活（如不存在则拉起）
        # 2. 等待角色真正就绪（有 claude 进程且非 busy）
        if not self._wait_for_role_ready(role, wf_id=wf_id, step_id=step_id):
            LOGGER.warning("_send_to_role %s: role not ready after wait, skipping", role)
            return False

        # ponytail: prompt_template 已含 /goal 前缀，避免重复叠加导致畸形
        import re as _re
        if not prompt.startswith(("/goal", "/GOAL", "/Goal")):
            prompt = "/goal " + prompt
        # 第二层防护：拒绝空目标或前缀污染（bus #141691 根因：q/goal [] 导致死循环）
        # 用正则剥掉 /goal 前缀（lstrip 字符集会误伤内容开头的 g/o/a/l）
        # 阈值 10→4：10 会误杀「请调研 Python」这类 6 字合法中文任务；
        # 4 仍能拦住空/占位符（[]、p、x），继续防 #141691 死循环
        _goal_content = _re.sub(r"^/goal\s*", "", prompt, flags=_re.IGNORECASE).strip()
        if len(_goal_content) < 4:
            LOGGER.warning("send-to-role %s 拒绝: goal 内容过短 (len=%d, wf=%s step=%s)",
                          role, len(_goal_content), wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] {role} goal 内容被拒绝: len={len(_goal_content)}",
                evidence=prompt[:300], src="workflow_engine")
            return False  # 投递未发生：明确语义，避免返回 None 被 engine 计数为失败
        # 双要素第二关：goal 应含完成形态动词（输出/报告/修复/设计/实现/审查），
        # 缺少则 warning + bus notice 但不阻断（CrewAI expected_output 模式启发）
        _ACTION_VERBS = ("输出", "报告", "修复", "设计", "实现", "审查", "创建", "更新", "完成", "分析", "验证",
                         "write", "report", "fix", "design", "implement", "review", "create", "update", "complete")
        if not any(v in _goal_content.lower() for v in _ACTION_VERBS):
            LOGGER.info("send-to-role %s goal 缺完成形态描述 (wf=%s step=%s)", role, wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("notice",
                f"[workflow] {role} goal 缺完成形态动词，建议补充「输出XX」类描述",
                evidence=prompt[:200], src="workflow_engine")
        # 自动注入搜索提醒（非 coordinator、非 force 模式才注入）
        # 让所有角色在每次接收任务时，习惯性地用 WebSearch 查证信息
        if role != "coordinator" and not force:
            _search_prefix = "（网络求证）开始工作前先用 WebSearch 搜索1次关键信息：事实数据、最新进展、可行性确认。搜不到的标注为「假设」。\n\n"
            if not prompt.startswith("（网络求证）"):
                prompt = _search_prefix + prompt
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
            return False  # 投递未发生：明确语义，避免 engine 将 None 计为失败
        # 写 TASKS.md 到角色 workspace，让模板中"读 TASKS.json"等指令能找到具体任务
        # 防污染：角色已有其他活跃工作流的任务文件时，拒写本任务（并发任务应排队，
        # 否则后写者覆盖先写者 → 角色执行错误任务。观测案例：测试风暴覆盖 scout 真实任务）。
        if wf_id:
            try:
                # 走 paths.CCS_WORKSPACES 而非硬编码 Path.home()：该常量支持
                # SESSION_PIPELINE_CCS_WORKSPACES 覆盖，测试可隔离。硬编码会让
                # 每个跑测试的用例都写进生产 workspace（实测污染 scout/engineer）。
                ws_dir = _CCS_WORKSPACES / role
                ws_dir.mkdir(parents=True, exist_ok=True)
                task_rows = self._lifecycle.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (wf_id,)
                ) if hasattr(self, '_lifecycle') else []
                if task_rows:
                    t = task_rows[0]
                    tasks_file = ws_dir / "TASKS.md"
                    _skip_write = False
                    if tasks_file.exists():
                        _existing = tasks_file.read_text(encoding="utf-8", errors="ignore")
                        # 精确边界匹配：只在 "工作流 wf_id /" 格式中匹配，防止 UUID 前缀碰撞
                        # （如 wf_abc123 不误匹配 wf_abc123def456）
                        _wf_in_existing = f"工作流 {wf_id} /" in _existing or wf_id in _existing.split("## 来源")[-1] if "## 来源" in _existing else False
                        # 僵尸主任务检测：现有 TASKS.md 的主 wf 若不在权威库，说明它早就
                        # 被清理/从未落库（测试 fixture 残留等），不是"活跃占用"。
                        # 不加此判据时守卫只追加 [排队] 永不覆盖，而**全库无排空代码** →
                        # TASKS.md 无限增长、新任务标题/描述永不写入、模板"读 TASKS.md"
                        # 拿不到正文（scout #372332 次生风险，实测 9 角色 19 条幽灵残留）。
                        _main_wf = re.search(r"工作流 (wf_[0-9a-f]{12})", _existing)
                        _stale_main = False
                        if _main_wf:
                            _mw = _main_wf.group(1)
                            try:
                                _stale_main = not self._lifecycle.query(
                                    "SELECT 1 FROM workflow_instances WHERE instance_id=?", (_mw,))
                            except Exception:
                                _stale_main = False  # 查询失败 → 保守按活跃处理（不覆盖）
                        if (_existing and "工作流任务" in _existing and not _wf_in_existing
                                and not _stale_main):
                            # 已有其他工作流的任务文件 → 不覆盖，追加排队标记
                            LOGGER.info("TASKS.md 防污染: %s 已有其他任务，本任务 %s 排队（不覆盖）", role, wf_id)
                            tasks_file.write_text(
                                _existing.rstrip() +
                                f"\n\n---\n[排队] 新任务 {wf_id}/{step_id} 待当前任务完成后执行\n",
                                encoding="utf-8")
                            _skip_write = True
                        elif _stale_main:
                            # 不是简单的"覆盖掉就好"：TASKS.md 里出现权威库查不到的 wf，
                            # 说明有别处用幽灵 ID 投递过（实测 2026-10-16 21:16-21:17
                            # scout 收到 wf_97c7f6a8e0ee/s1、pm 收到 wf_d50ca8295b59/s1，
                            # 四库零命中）。覆盖只治症状，投递方仍未被发现 —— 故升级为
                            # blocker + ERROR，让"谁在发幽灵 ID"暴露出来而不是被静默清理。
                            LOGGER.error("TASKS.md 僵尸主任务 %s 不在权威库（幽灵 ID），"
                                         "覆盖为新任务 %s", _main_wf.group(1), wf_id)
                            self._bb.write("blocker",
                                f"[workflow] 幽灵 wf_id 污染 TASKS.md: {_main_wf.group(1)}"
                                f"（角色 {role}），已覆盖为 {wf_id}",
                                evidence=f"TASKS.md 原主任务 {_main_wf.group(1)} 不在权威库",
                                src="workflow_engine")
                    if not _skip_write:
                        tasks_file.write_text(
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
        # 投递成功与否以子进程退出码为准：returncode != 0 视为投递失败，必须返回 False，
        # 否则 engine 会错盖 notified（违反红线：投递失败必须返回 False，下轮重试，
        # 否则 step 永久停在空 step_results 的 running 状态被静默误杀）
        try:
            _result = _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "send", role, prompt,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=30,
            )
        except (ValueError, KeyError, TypeError):
            try:
                ccs_cli = _CCS_CLI
                _result = _engine_mod._sp.run(
                    ["python3", str(ccs_cli), "send", role, prompt[:2000],
                     "--from", "workflow_engine"],
                    capture_output=True, timeout=10,
                )
            except Exception as _e:
                LOGGER.exception("fallback CCS send failed for %s", role)
                try:
                    self._bb.write("code_fix", "pipeflow: fallback CCS send failed role=" + role,
                                   evidence=str(_e)[:200], src="pipeflow")
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("bus write fail after fallback CCS send")
                return False  # 投递链路异常：明确失败
        if getattr(_result, "returncode", None) is None or _result.returncode != 0:
            LOGGER.warning("send-to-role %s ccs send 退出码=%s (wf=%s step=%s)",
                           role, getattr(_result, "returncode", None),
                           wf_id[:12] if wf_id else "?", step_id)
            return False  # 非零退出码 = 投递失败：调用方据此不盖戳，下轮重试
        return True  # 投递成功（ccs send 正常退出）
