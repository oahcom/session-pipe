#!/usr/bin/env python3
"""RoleLifecycleMixin — 角色存活管理、忙检测、tmux 操作、通知/推送。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import re
import sys
import time
import logging
import threading
import os as _os
from datetime import datetime, timezone
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from paths import CCS_CLI as _CCS_CLI
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

LOGGER = logging.getLogger("workflow.engine")

# 工作流活跃状态：与 engine.py 的 _count_active 同集。用非活跃判据清理
# TASKS.md 主任务块时，"已终态"与"不在库"等价 —— 两者都不是活跃占用。
_ACTIVE_WF_STATUSES = ("pending", "running", "step_done_ready")

# kick-stalled 重推冷却（秒）：原实现每个 daemon tick 都重推，实测 33 分钟
# 398 次（约 5s 一次），把同一 prompt 灌满角色队列 —— pane 显示
# "Press up to edit queued messages"，角色被自己的重复指令淹没。
# 停滞是真实状态，但不该靠刷屏解决：同一 step 每 10 分钟最多重推 1 次。
_KICK_COOLDOWN_SEC = 600
# 角色拉起冷却（秒）：_ensure_role_alive 有 6 个调用点（含 engine.py:202 的
# on_advance 回调，每个步骤推进都触发），且是 check-then-act 无锁 → 并发 tick
# 各自判定"不存活"后同时拉起同一角色（实测 scout 建 116 个会话）。
# 同一角色在冷却窗内不重复拉起。
_RESTART_COOLDOWN_SEC = 300

# 上下文溢出措辞。pane 与 transcript 两条检测路径共用一份，避免措辞漂移：
# 2026-10-19 实测 reviewer 撞的是 "maximum context length"/"context_length_exceeded"
# （Anthropic 400 原文），旧清单只认另外三种 → 溢出角色永不自愈，投递 3 次
# 失败后子链 failed、父链空等。新措辞由上游 API 决定，宁可宽匹配。
_OVERFLOW_PATTERNS = (
    "Prompt is too long",
    "Context limit reached",
    "Context low",
    "maximum context length",
    "context_length_exceeded",
)

# 溢出报错的时效（秒）：超过此年龄不再触发 /compact。
# 按"多久前"而非"距文件尾多少字节"判断：实测错误行距文件尾 7.8KB~10.8MB
# （35 会话样本）、最远 4111 行，任何固定字节/行数窗口都会漏。
_OVERFLOW_ERR_MAX_AGE = 600

# 角色 transcript 根目录（测试可注入 tmp_path）。
# 目录名把 cwd 路径的每个 `/` 换成 `-`：/home/administrator/ccs-workspaces/<role>。
_TRANSCRIPT_ROOT = Path.home() / ".claude" / "projects"


def _transcript_shows_overflow(role: str, max_age: float = _OVERFLOW_ERR_MAX_AGE) -> bool:
    """从角色 transcript 判断近期是否发生上下文溢出。

    pane 走备用屏（alternate_on=1）时无 scrollback，溢出报错滚出视口即不可见；
    transcript 是落盘事实，不受视口与进程存活影响（scout 实测在 `Prompt is too long`
    终止后 1s 才被检测到，此时 agent 已死、send-keys 无处可发）。

    溢出事件形如：{"type":"assistant","isApiErrorMessage":true,
        "message":{"model":"<synthetic>","content":[{"type":"text","text":"Prompt is too long"}]}}
    """
    import glob as _glob
    try:
        _d = _TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        files = sorted(_glob.glob(str(_d / "*.jsonl")), key=_os.path.getmtime, reverse=True)
        if not files:
            return False
        _pat = _OVERFLOW_PATTERNS
        cutoff = time.time() - max_age
        # 尾读 64KB 即可覆盖"本轮"错误：溢出后角色无法继续产出，错误必落在文件尾
        with open(files[0], "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - 65536))
            tail = fh.read().decode("utf-8", "ignore")
        for line in reversed(tail.splitlines()):
            if '"isApiErrorMessage":true' not in line.replace(" ", ""):
                continue
            if not any(p in line for p in _pat):
                continue
            try:
                _ts = json.loads(line).get("timestamp", "")
                _dt = datetime.strptime(_ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            except (ValueError, TypeError, KeyError):
                return True  # 时间戳不可解析：保守按"刚发生"处理，宁可多 compact 一次
            return _dt.timestamp() > cutoff
        return False
    except (OSError, ValueError, KeyError, TypeError):
        return False


class RoleLifecycleMixin:
    """WorkflowEngine 的角色管理 mixin。"""
    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"
    _OVERFLOW_THRESHOLD_LINES = 2000   # pane scrollback 行数超过此值触发 /compact（备用指标）
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    # ── 模型额度拒绝（403/429）恢复 ──
    # 9router 免费层按出口 IP 计配额：角色 pane 只剩 "API Error: 403" /
    # "Rate limit exceeded"，进程活着但不产出。旧行为只把它排除出 idle 判定
    # （conversation_monitor._EXCLUDE_IDLE），**没有任何恢复动作** → 步骤一直
    # 等到超时取消。实测 pg 在 wf_sub_4e63ad633d39/s1 卡 78min。
    # 退避：首次 120s，之后每次翻倍到 1800s 封顶，避免在额度耗尽期刷屏。
    _RATE_LIMIT_BACKOFF_SEC = (120, 240, 480, 960, 1800)
    _last_rate_recover: dict[str, tuple[float, int]] = {}   # role -> (last_ts, attempt)
    _RATE_LIMIT_PATTERNS = (
        "API Error: 403", "API Error: 429",
        "Rate limit exceeded", "rate_limit_exceeded",
        "FreeTierError", "FreeUsageLimitError",
        "Please run /login",
    )

    _last_compact: dict[str, float] = {}
    # 上下文阈值告警：从 pane 状态栏 `↓ 117.1k tokens` 提取用量，超阈值写 blocker。
    # 与溢出检测（_check_context_overflow 的 has_overflow）分两路：溢出信号要等
    # 真正撞墙才出现，那时 /compact 已排在长任务队列尾（实测 engineer 117k 时
    # 正在跑 21 分钟的 pytest）。提前告警让 coordinator 能安排任务间隙。
    _CONTEXT_WARN_K = 100.0            # 触发告警的上下文用量（k tokens）
    _CONTEXT_WARN_COOLDOWN = 1800      # 同角色两次告警最小间隔（秒）
    _last_context_warn: dict[str, float] = {}
    _RESTART_LOCK = threading.Lock()
    # check-then-act 竞态防护：cooldown 检查与写入之间加锁
    _COMPACT_LOCK = threading.Lock()
    # bus task_spec 写入去重：同一 (wf_id, step_id) 在窗口内只写一条。
    # WHY: 重推路径（step_handler timeout/kick-stalled）带 force=True 绕过了
    # _is_role_busy 守卫，而 bus.write 的 UPSERT 会在 t 冲突时重置 ts=now，
    # 使同一条消息永远"新鲜"、永远回到积压里 —— 实测 103 条重复
    # "needs_implementation @product_architect ... wf_42c712fc21/s1"。
    # ccs send 不受影响，仍每轮推送全文。
    _BUS_WRITE_COOLDOWN = 1800
    _last_bus_write: dict[str, float] = {}
    _BUS_WRITE_LOCK = threading.Lock()


    def _active_roles(self) -> set[str]:
        """活跃角色集合：DB 工作流 assignee ∪ 实际存在的 ccs-* tmux 会话。"""
        roles = set()
        try:
            for row in self._lifecycle.query(
                "SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"
            ):
                if row["assignee"]:
                    roles.add(row["assignee"])
        except Exception:
            pass
        try:
            r = _engine_mod._sp.run(
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                roles |= {n[4:] for n in r.stdout.split() if n.startswith("ccs-") and len(n) > 4}
        except Exception:
            pass
        return roles

    def _recover_rate_limited_roles(self) -> None:
        """角色被模型额度拒绝（403/429）时按退避发「继续」重试。

        WHY 不能只判 pane 空闲：403/429 时角色进程活着、pane 有内容（错误文案），
        与"正在干活"在存活判据上完全一样；_EXCLUDE_IDLE 只是避免误判死，
        没有任何恢复动作 → 步骤空等到超时取消。

        WHY 发「继续」而不是重启：错误是上游额度/出口 IP 问题，重启进程
        不改变额度；角色上下文里的任务还在，一句「继续」就能让它重试该请求。

        WHY 角色来源不能只看 DB assignee：403 发生在“收到任务前的最后一次调用”
        时，该角色可能不在任何 running/pending 工作流里（curator/maintainer
        实测挂 403 数小时无人管），tmux 兜底才能覆盖到。
        """
        roles = self._active_roles()
        if not roles:
            return

        now = time.time()
        for role in roles:
            tmux_name = f"ccs-{role}"
            try:
                r = _engine_mod._sp.run(
                    ["tmux", "capture-pane", "-p", "-t", f"{tmux_name}:0.0", "-S", "-60"],
                    capture_output=True, text=True, timeout=5,
                )
            except Exception:
                continue
            if r.returncode != 0:
                continue
            output = r.stdout or ""
            if not any(p in output for p in self._RATE_LIMIT_PATTERNS):
                # 角色已恢复：清掉退避状态，否则下一次真 403 会从最大退避档
                # （1800s）起步，恢复要等半小时。
                self._last_rate_recover.pop(role, None)
                continue
            # 追加箭头只在**生成中**出现（空闲时底部只有 bypass 提示）：
            # -S -60 里残留的旧错误行不能当作"当前仍被拒"，否则会给正在干活的
            # 角色发 C-u +「继续」，把当前轮打断。
            _tail = [ln for ln in output.splitlines() if ln.strip()][-4:]
            if any("esc to interrupt" in ln for ln in _tail):
                continue

            with self._COMPACT_LOCK:
                last, attempt = self._last_rate_recover.get(role, (0.0, 0))
                wait = self._RATE_LIMIT_BACKOFF_SEC[
                    min(attempt, len(self._RATE_LIMIT_BACKOFF_SEC) - 1)]
                if now - last < wait:
                    continue
                self._last_rate_recover[role] = (now, attempt + 1)

            LOGGER.warning("额度拒绝: %s 命中 403/429，第 %d 次发「继续」重试", role, attempt + 1)
            try:
                _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "C-u"],
                    capture_output=True, timeout=5,
                )
                _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "继续", "Enter"],
                    capture_output=True, timeout=5,
                )
                self._bb.write("monitor_audit",
                    f"[rate_limit] {role} 被额度拒绝 → 已发继续（第 {attempt + 1} 次）",
                    evidence=output[-300:], src="workflow_engine")
            except Exception as e:
                LOGGER.warning("额度恢复发送失败 %s: %s", role, e)

    def _check_context_overflow(self):
        """检测角色 tmux pane 中 Claude 的上下文溢出信号，自动发 /compact。

        检测两个信号：
        - "Context limit reached"（Claude 阻塞等待 /compact）
        - "Context low"（Claude 提示上下文不足）

        覆盖两个来源：
        - DB 中 running/pending workflow 的角色
        - 所有实际存在的 ccs-* tmux session（覆盖非 workflow 启动的角色）
        """
        roles = self._active_roles()
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

            # 信号匹配走共享清单 _OVERFLOW_PATTERNS（与 transcript 路径同源）。
            # 另加状态栏百分比形态（"Context (数字% remaining)"）。
            has_overflow = (
                any(_p in output for _p in _OVERFLOW_PATTERNS)
                or ("Context" in output and "remaining" in output and "compact" in output.lower())
            )

            # `-S -500` 与 `-S -5000` 返回逐字节相同的视口内容，故溢出报错一旦滚出
            # 视口就再也抓不到。transcript 是落盘事实，不受视口与进程存活影响。
            #
            # 阈值告警（与溢出检测独立，先于它执行）：从状态栏 `↓ 117.1k tokens`
            # 提取当前用量，超阈值就写 blocker —— 溢出信号要等真正撞墙才出现，
            # 那时 /compact 已排在长任务队列尾（实测 engineer 117k 时正在跑 21 分钟
            # 的 pytest，compact 要等它结束）。提前告警让 coordinator 能安排任务间隙。
            if not has_overflow:
                _m = __import__("re").search(r"↓\s*([\d.]+)\s*k tokens", output)
                if _m:
                    _used_k = float(_m.group(1))
                    if _used_k >= self._CONTEXT_WARN_K:
                        with self._COMPACT_LOCK:
                            _last_warn = self._last_context_warn.get(role, 0.0)
                            if now - _last_warn >= self._CONTEXT_WARN_COOLDOWN:
                                self._last_context_warn[role] = now
                                LOGGER.warning("上下文预警: %s 已用 %.1fk tokens (阈值 %.0fk)",
                                               role, _used_k, self._CONTEXT_WARN_K)
                                self._bb.write("blocker",
                                    f"[context_warn] {role} 上下文已用 {_used_k:.0f}k（阈值 {self._CONTEXT_WARN_K:.0f}k）"
                                    f" — 建议在任务间隙安排 /compact，避免溢出后排队",
                                    src="workflow_engine")

            # 而本函数从未触发过一次 /compact——只有 pane 单路时角色溢出后永久卡死
            # （product-architect 90 次 / consumer 35 次）。CLAUDE.md 早已声明本路存在，
            # 实为缺失实现。
            if not has_overflow:
                has_overflow = _transcript_shows_overflow(role)
            if not has_overflow:
                continue

            LOGGER.info("上下文溢出: %s 检测到溢出信号，发送 /compact", role)
            try:
                # 先清输入行：溢出时角色常卡在未提交的半截 prompt 上，
                # 直接发 /compact 会被拼到该行尾变成普通文本而非斜杠命令
                # （实测遗留文本 + "/compact" → 不执行，角色继续溢出）。
                _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "C-u"],
                    capture_output=True, timeout=5,
                )
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
                "SELECT instance_id, template_id, current_step_id, step_results, assignee, "
                "effective_roles FROM workflow_instances WHERE status='running'"
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
                # 实际执行角色：等价角色池分流后可能是 pg 而非模板写死的 engineer。
                # 用模板角色判 busy 会拦错人 —— 实测 wf_sub_4e63ad633d39/s1
                # （target_role=engineer, effective_roles={"s1":"pg"}）在 s1 停 78min
                # 未重推，因为 engineer 正被另一条链占用；而真正该干的 pg 一直空闲。
                from liveness import effective_role as _er
                _role = _er(dict(row), _sid, fallback=_step.target_role)
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
                    _tt, _td, _results, _prev_sid, wf_id=row["instance_id"])
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
    def _is_agent_alive(role: str) -> bool | None:
        """检查 CCS tmux pane 是否存活。三态：True=活 / False=判死 / None=探测失败。

        ponytail: 用 pane 存活判定而非进程名白名单——claude agent 执行 /goal 时
        pane_current_command 可能是子进程 wrapper（bash/sleep/node），不在
        白名单会误判"无 agent"→ kill-session 杀掉正在工作的角色（观测：pm
        工作时被杀 8 次，2026-08-09）。tmux 会话存在即视为 agent 存活，
        只有 pane 完全消失才判定失活。

        None 与 False 必须区分：tmux 瞬时不可达（实测 578ms 抖动）时返回 False
        会让调用方 kill 掉正在工作的活角色。宁可放过一次真死，不可误杀活角色。
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
            return None  # 探测失败 ≠ 判死

    def _ensure_role_alive(self, role: str) -> bool:
        """检查角色的 CCS 是否存活（tmux + claude 进程），不存活则拉起。"""
        alive = _engine_mod._sp.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=5,
        ).returncode == 0
        if alive:
            agent = self._is_agent_alive(role)
            if agent is None or agent:
                # None=探测失败（不判死）；True=活。两种情况都不动它。
                return True
            # 二次确认：kill 是破坏性动作，首判可能因启动竞态假阴性
            # （观测：scout 建 25+ 会话，含 1 秒即死实例）。
            time.sleep(1)
            if self._is_agent_alive(role) is not False:
                return True
            LOGGER.info("CCS %s: tmux alive but no claude, restarting", role)
            _engine_mod._sp.run(["tmux", "kill-session", "-t", f"ccs-{role}"],
                    capture_output=True, timeout=5)
            alive = False
        if alive:
            # 代码变更检测已禁用：workspace 的 .py 是角色自己的产出（正常开发），
            # 全量 md5 扫描会误判"代码变更"→ 杀死正在工作的角色（2026-08-01 观测 8 次误杀）。
            # 如需热替换 launcher 代码，由运维手动 ccs restart 完成。
            return True

        # 拉起前加锁 + 冷却：check-then-act 的写入侧必须串行化。
        # 锁内不重复探测 tmux（外层刚判过）：并发 tick 的第二个只需看冷却戳即可，
        # 冷却戳由先到的 tick 写入，后者读到后直接退出。
        with self._RESTART_LOCK:
            # 实例级冷却戳：类级 dict 会跨实例共享（测试间污染）；生产只有一个
            # engine 实例，实例级足以挡并发。
            if not hasattr(self, "_last_restart"):
                self._last_restart = {}
            _now = time.time()
            if _now - self._last_restart.get(role, 0.0) < _RESTART_COOLDOWN_SEC:
                LOGGER.debug("CCS %s: 拉起冷却期内，跳过", role)
                return False
            self._last_restart[role] = _now
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
                    _st = _engine_mod._sp.run(
                        ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                         "--drive", "infinite", "--no-auto-send"],
                        capture_output=True, timeout=60, env=_env
                    )
                    # ccs start 的失败是**静默**的：返回码非 0 且 stderr/stdout 说明
                    # 原因（实测 writer：swap 仅剩 0MB，跳过启动），但原实现丢弃整个
                    # 返回值 → 上层只能看到"tmux 没起来"，日志还错报 tmux alive
                    # → 空转 90s → 排队 14 次 → 子链被回收 → 父链断链。
                    # 这里把真实原因抬到日志与 blocker，使资源类阻断可被外部观测。
                    _err = (_st.stderr or _st.stdout or b"").decode("utf-8", "replace")[:300]
                    if _st.returncode != 0 or "\"success\": false" in _err:
                        LOGGER.error("_wait_for_role_ready: ccs start %s 失败 (rc=%s): %s",
                                     role, _st.returncode, _err[:200])
                        try:
                            self._bb.write("blocker",
                                f"[workflow] 角色 {role} 启动失败，工作流无法投递",
                                evidence=_err, src="workflow_engine")
                        except Exception:
                            LOGGER.debug("role-start blocker 写入失败: %s", role)
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
                LOGGER.info("Role %s 无 claude 进程，等待就绪（若持续则角色未被拉起）", role)
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

        LOGGER.warning("_wait_for_role_ready: role %s not ready after %ds", role, max_wait)
        return False

    # 开发角色池（DEV_ROLE_POOL / _pick_dev_role）已删除 —— 见 _send_to_role
    # 处注释：投递层单点替换无法让下游步骤状态机一致，需在步骤/模板层实现。

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
        except Exception as _e:
            # 探测失败 → 保守判忙。调用方的默认动作是"投递"（role_lifecycle:446
            # 跳过排队守卫直接发、step_handler:244/357 排队判定、kick_stalled:191），
            # 返回 False 等于"探测不了就当它空闲"→ DB 抖动一次即打穿所有忙闲门控，
            # 正在执行任务的角色被叠加投递（正是 _KICK_COOLDOWN_SEC 注释记录的
            # "被自己的重复指令淹没"）。安全方向是宁可漏投一轮，不可重复打扰。
            LOGGER.warning("_is_role_busy 探测异常，保守判忙: %s (%s)", role, _e)
            return True

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

        返回: True=已投递；"busy"=角色忙（正常排队，非故障，调用方不应计失败）；
              False=真失败（变量残留/角色未就绪/投递异常）。
        force=True 时跳过 pending/busy 检查，用于超时重推。"""

        # 开发角色池：engineer 与 pg 职责重叠（都是"按方案编码、自测、提 PR"），
        # 但主链模板把开发步骤全写死 engineer → 4/6 实例压在 engineer 上、pg 闲置。
        #
        # 已回滚（ac76b9b/4c966bc）：只换投递目标而不改步骤状态读取，会在下游制造
        # 更严重的不一致 —— exit_messages 按 step.target_role 过滤（pg 的产出永不被
        # 认可 → 步骤超时）、busy 门控看 engineer（pg 的工作不可见）、exit_schema
        # 校验 engineer 的 workspace 而产出落在 pg（校验必失败）。
        # 三方审查结论：要么把重定向持久化到步骤定义（改 step.target_role），
        # 并在步骤状态机内完成，要么不做。半途实现比不做更糟。
        if not force and wf_id and self._is_role_busy(role, exclude_wf_id=wf_id, pane_fallback=False):
            LOGGER.info("send-to-role %s 跳过: 有其他 workflow 在进行中 (wf=%s step=%s)",
                        role, wf_id[:12], step_id)
            # "角色忙" 是正常排队，不是故障：单角色串行而 N 个同角色实例并发时，
            # 多数实例都会撞上此分支。调用方不可据此累计 delivery_fail_count
            # （否则 3 次即判死，实测 227 条 delivery_blocked 中多数属此）。
            return "busy"
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
        # 关键修复：确保角色存活并就绪，不只是 tmux 存活
        # 1. 先确保 tmux session 存活（如不存在则拉起）
        # 2. 等待角色真正就绪（有 claude 进程且非 busy）
        #
        # 未就绪是**临时**状态（忙/启动中/探测抖动），不是投递失败。返回 "busy"
        # 让 engine 走排队分支（不计数、下 tick 重试）；返回 False 会被 engine
        # 计入 delivery_fail_count，3 次即灌 delivery_blocked 终态且无恢复路径
        # （该方法唯一的 False 路径是 90s 空转超时，见 _wait_for_role_ready 末尾）。
        if not self._wait_for_role_ready(role, wf_id=wf_id, step_id=step_id):
            LOGGER.warning("_send_to_role %s: role not ready, queue for next tick", role)
            return "busy"

        # /goal 前缀语义：Claude Code 把它变成**会话级 Stop hook 硬条件**，
        # 条件不满足则角色无法停止。这对"完成任务"是正确语义，对运维催办是灾难——
        # 实测 [deadlock] test_report 超时 6769s 被投递后，age 每秒 +1 使条件永不成立，
        # 角色被永久锁死（qa 会话 215 条该 hook、助理文本 0 条；全角色累计 943 条）。
        # integrity 提醒同病：条件含"（简答）请回答"类无法自证的疑问句，且 60s 节流
        # 反复重推 → 同样永久锁定。故：仅真实任务 (force=False) 加前缀。
        import re as _re
        # 剥离**所有**位置的 /goal，不只行首：模板正文自带 /goal 时会被 header 顶到
        # 中段，`^` 锚定看不见它 → 下面再补一个前缀 → 角色收到双 /goal。
        # 实测全角色 jsonl 含 >=2 个 /goal 的消息 1239 条（R41, 2026-10-19）。
        # 全量剥离 + 重新加前缀后，无论调用方传什么形态都恰好一个。
        prompt = _re.sub(r"(?m)^/goal(?:\s+|$)", "", prompt).lstrip("\n")
        if not force:
            prompt = "/goal " + prompt
        # 第二层防护：拒绝空目标或前缀污染（bus #141691 根因：q/goal [] 导致死循环）
        # 用正则剥掉 /goal 前缀（lstrip 字符集会误伤内容开头的 g/o/a/l）
        # 旧实现按字符数 <4 判定，会误杀「调研x」这类合法短任务；改为判「有无实质内容」。
        # 仍拦空目标、单字符与纯占位符（[]、p、x），继续防 #141691 死循环。
        _goal_content = _re.sub(r"^/goal(?:\s+|$)", "", prompt, count=1, flags=_re.IGNORECASE).strip()
        _is_placeholder = (len(_goal_content) < 2
                           or _re.fullmatch(r"[\[\](){}<>\s*_\-./\\|]+", _goal_content) is not None)
        if _is_placeholder:
            LOGGER.warning("send-to-role %s 拒绝: goal 无实质内容 (len=%d, wf=%s step=%s)",
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
        # 让所有角色在每次接收任务时，习惯性地先查证信息。
        # WHY 用 CLI 而非 WebSearch：角色以 `claude --bare` 启动，该模式不提供
        # WebSearch/WebFetch 工具。旧文案教一个不存在的工具，实测角色目录 0 次调用、
        # 213 次自述"没有该工具"，engineer/pg/security_auditor 三方向空转声明 N/A。
        # 短查询（≤5 词）才有效：长整句（如 8 词以上）实测返回 NO_RESULTS。
        if role != "coordinator" and not force:
            _search_prefix = ("先查证：`python3 ~/.hermes/scripts/web_search.py \"关键词\" -n 5`"
                              "（关键词 2-5 词，长查询会 NO_RESULTS）。"
                              "搜不到的标注为「假设」。\n\n")
            if not prompt.startswith("先查证："):
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
                        # 本任务是否已在文中。原写法 `A or B if C else False` 被 Python
                        # 解析为 `(A or B) if C else False`：文件无 "## 来源" 段时整个表达式
                        # 恒为 False，**无论 wf_id 是否真在文件里** → 引擎永远当作"别的
                        # 任务占了文件" → 本任务只追加 [排队]、不写入 → ccs send 退出码 1
                        # → 投递失败 3 次 → 子链 failed → 父链断。
                        # 实测 reviewer/engineer/qa 的 TASKS.md 均无 "## 来源" 段（该段
                        # 只在角色产出含来源元数据时才有）→ 三角色中招率 100%。
                        _wf_in_existing = (f"工作流 {wf_id} /" in _existing
                                           or wf_id in _existing.split("## 来源")[-1])
                        # 僵尸主任务检测：主 wf 不在权威库，或在库但**已终态**，都
                        # 说明它不是"活跃占用"，允许被新任务覆盖。
                        # 只判"不在库"时，已完成的主任务会永久占住文件 → 新任务永远
                        # 只能追加 [排队]，而排队行无任何排空路径 → 无界增长
                        # （实测 engineer 458 行 / qa 211 行，全部指向终态 wf）。
                        _main_wf = re.search(r"工作流 (wf_[0-9a-f]{12})", _existing)
                        _stale_main = False
                        if _main_wf:
                            _mw = _main_wf.group(1)
                            try:
                                _row = self._lifecycle.query(
                                    "SELECT status FROM workflow_instances WHERE instance_id=?",
                                    (_mw,))
                                _stale_main = (not _row
                                               or _row[0]["status"] not in _ACTIVE_WF_STATUSES)
                            except Exception:
                                _stale_main = False  # 查询失败 → 保守按活跃处理（不覆盖）
                        if (_existing and "工作流任务" in _existing and not _wf_in_existing
                                and not _stale_main):
                            # 已有其他工作流的任务文件 → 不覆盖，追加排队标记。
                            # 幂等：引擎每 tick 重推同一步骤，无此守卫则同一 wf 被追加
                            # 数十行（实测 engineer/scout TASKS.md 各 27 行重复）。
                            # 尾随空格是必需边界：标记恒为 "{wf}/{step} 待当前任务..."，
                            # 无空格则 s1 会误命中 s10（实测 step_id 当前止于 s7，尚未触发）。
                            if f"[排队] 新任务 {wf_id}/{step_id} " in _existing:
                                _skip_write = True
                            else:
                                LOGGER.info("TASKS.md 防污染: %s 已有其他任务，本任务 %s 排队（不覆盖）", role, wf_id)
                                tasks_file.write_text(
                                    _existing.rstrip() +
                                    f"\n\n---\n[排队] 新任务 {wf_id}/{step_id} 待当前任务完成后执行\n",
                                    encoding="utf-8")
                                _skip_write = True
                        elif _stale_main:
                            LOGGER.info("TASKS.md 僵尸主任务 %s 不在权威库，覆盖为新任务 %s",
                                        _main_wf.group(1), wf_id)
                    if not _skip_write:
                        tasks_file.write_text(
                            f"# 工作流任务\n\n## 标题\n{t['title'] or '?'}\n\n"
                            f"## 描述\n{t['description'] or t['title'] or '?'}\n\n"
                            f"## 来源\n工作流 {wf_id} / 步骤 {step_id}\n",
                            encoding="utf-8")
            except (ValueError, KeyError, TypeError):
                pass
        # _send_to_role 只发任务，分类恒为 task_spec。
        #
        # 原实现用关键词猜告警（prompt 含"异常"/"持续超时"/"超时自动回收"→ 写 blocker），
        # 但 prompt 是**任务正文**：正常任务模板标题含"异常诊断"即命中 → 真实任务被写成
        # blocker，污染告警通道（实测 2026-10-17 05:50 maintainer/qa 的 6 条"告警"实为
        # 正常任务派发）。真告警有专用通道 _notify_role()（直接写 blocker，不经此函数），
        # 故此处无需猜测——按调用方语义（而非消息内容）分类，与 scheduler_noise 同范式。
        # 去重：同一 (wf_id, step_id) 在冷却窗内只写一条，重推不再刷新 ts。
        _dedup_key = f"{wf_id}/{step_id}"
        _should_write = True
        if wf_id and step_id:
            with self._BUS_WRITE_LOCK:
                _prev = self._last_bus_write.get(_dedup_key, 0)
                _should_write = not _prev or (time.time() - _prev) >= self._BUS_WRITE_COOLDOWN
                if _should_write:
                    # 顺手清过期项：key 是 wf_id/step_id（无界，每个工作流新 key），
                    # 不清理则 daemon 长跑内存持续增长。
                    if len(self._last_bus_write) > 512:
                        _cutoff = time.time() - self._BUS_WRITE_COOLDOWN
                        self._last_bus_write = {
                            k: v for k, v in self._last_bus_write.items() if v >= _cutoff
                        }
                    self._last_bus_write[_dedup_key] = time.time()
        if _should_write:
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
                # 窗口必须覆盖子进程真实最坏路径：长 prompt 分块写入（每块
                # 500 字符）+ tmux 自适应超时（负载 ≥12 时单次 20s，3 次重试）
                # 可超 60s。30s 会在投递途中杀掉子进程，留下半截输入。
                capture_output=True, timeout=120,
            )
        except _engine_mod._sp.TimeoutExpired as _te:
            # ccs send 超时（实测长 prompt 2299 字符 > 30s，19:53/19:57/20:54/21:19 共 8 次）：
            # TimeoutExpired 不是 ValueError/KeyError/TypeError 的子类，原先直接冒泡出
            # _send_to_role → 冒泡出 run_once → 被 engine.py:1332 的 sqlite_block 兜住，
            # **中断整个 tick 的生产工作流扫描**（所有工作流停一步），且本步拿不到
            # False → 不重投。按红线（投递失败必须返回 False）收口为正常失败分支：
            # 不盖戳、不计 delivery_fail_count，下轮重试。
            LOGGER.warning("send-to-role %s ccs send 超时 (wf=%s step=%s)：%s",
                           role, wf_id[:12] if wf_id else "?", step_id, _te)
            self._bb.write("blocker",
                f"[workflow] {role} ccs send 超时（prompt {len(prompt)} 字符）",
                evidence=str(_te)[:300], src="workflow_engine")
            return "busy"  # 超时是临时状态，非投递失败：排队等下轮，不灌 delivery_blocked
        except (ValueError, KeyError, TypeError):
            try:
                ccs_cli = _CCS_CLI
                # --from 不可省：缺省落到 ccs.py:128 的 default="cli"，而 cli 在
                # user_intent_reminder._USER_SOURCES 人类白名单里 → 被当人类请求注入
                # 提醒、写永不过期的 core.send 痕迹 → 角色被反复唤醒空转
                # （codex 三方审查 P2-1，2026-10-17）。主路径带此参数，兜底漏了。
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
