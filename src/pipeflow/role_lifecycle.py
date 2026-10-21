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


def _read_transcript_tail(role: str, nbytes: int = 65536) -> list[str]:
    """角色最新 transcript 的尾部行（倒序）。取不到一律 []，判据按"无信号"处理。

    只读尾部：transcript 实测 7.8MB，全量解析 ×20 角色 ×每轮会把 daemon 拖死。
    64KB 足够覆盖"本轮"错误——角色无法继续产出后，错误必落在文件尾。
    """
    import glob as _glob
    try:
        _d = _TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        files = sorted(_glob.glob(str(_d / "*.jsonl")), key=_os.path.getmtime, reverse=True)
        if not files:
            return []
        with open(files[0], "rb") as fh:
            fh.seek(0, 2)
            fh.seek(max(0, fh.tell() - nbytes))
            tail = fh.read().decode("utf-8", "ignore")
        return list(reversed(tail.splitlines()))
    except (OSError, ValueError):
        return []


def _transcript_shows_overflow(role: str, max_age: float = _OVERFLOW_ERR_MAX_AGE) -> bool:
    """从角色 transcript 判断近期是否发生上下文溢出。

    pane 走备用屏（alternate_on=1）时无 scrollback，溢出报错滚出视口即不可见；
    transcript 是落盘事实，不受视口与进程存活影响（scout 实测在 `Prompt is too long`
    终止后 1s 才被检测到，此时 agent 已死、send-keys 无处可发）。

    溢出事件形如：{"type":"assistant","isApiErrorMessage":true,
        "message":{"model":"<synthetic>","content":[{"type":"text","text":"Prompt is too long"}]}}
    """
    cutoff = time.time() - max_age
    for line in _read_transcript_tail(role):
        if '"isApiErrorMessage":true' not in line.replace(" ", ""):
            continue
        if not any(p in line for p in _OVERFLOW_PATTERNS):
            continue
        try:
            _ts = json.loads(line).get("timestamp", "")
            _dt = datetime.strptime(_ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError, KeyError):
            return True  # 时间戳不可解析：保守按"刚发生"处理，宁可多 compact 一次
        return _dt.timestamp() > cutoff
    return False


# API 错误族（额度/上游/鉴权）。"Prompt is too long" 不在其中：溢出归 /compact 那条
# 路，给它发「继续」只会永远重试同一段超长上下文。
# 时效：CLI 自己会重试——24h 1,342 条错误轮 85.8% 在 10 分钟内自愈，这条只补
# "停住不动"的那些。
_API_ERROR_PATTERNS = ("API Error:", "Please run /login")
_API_ERROR_AGE = 120

# 「继续」必须带信息：24h 1,247 次注入里 611 次的后续轮没有任何工具调用（另 520
# 次立刻又撞错），裸「继续」只能被角色复述。重试的单位是**被中断的那次请求**，
# 不是整个任务（gemini-cli checkpoint 恢复的是那条 tool call，不是重新规划）。
_RATE_LIMIT_NUDGE = "上一轮 API 错误中断（0 产出）。继续：重发被中断的那次请求，不要重新规划，不要写自检报告。"


def _transcript_last_turn_is_api_error(role: str, max_age: float = _API_ERROR_AGE) -> bool:
    """角色**最后一条** assistant 轮是否就是一条 API 错误（这类轮 output_tokens 恒为 0）。

    WHY 不靠 pane：15/15 ccs 会话 alternate_on=1，capture-pane 只有视口，错误文案
    几分钟就滚出视口（实测 24h 1,342 条错误轮，pane 只认出 9 次 = 0.67%；
    maintainer 216→0 / product-architect 119→0 / qa 107→0）。
    WHY 只看最后一条：错误之后只要再来一条正常轮，就说明已自愈，不许再打断它。
    """
    for line in _read_transcript_tail(role):
        _c = line.replace(" ", "")
        if '"type":"assistant"' not in _c:
            continue
        if '"isApiErrorMessage":true' not in _c:
            return False
        if not any(p in line for p in _API_ERROR_PATTERNS):
            return False
        try:
            _dt = datetime.strptime(json.loads(line)["timestamp"],
                                    "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError, KeyError):
            return True  # 时间戳不可解析：保守按"刚发生"，宁可多发一句
        return (time.time() - _dt.timestamp()) < max_age
    return False

# 生成中但 transcript 多久没落事件即判卡死（秒）。
# 实测 2026-10-21 coordinator：末事件是 05:22:13Z 的 tool_use，之后 31 分钟
# 无任何行（含 tool_result），pane 一直挂 `Compacting… 55% · esc to interrupt`。
# 取 300s：正常工具调用（pytest 21min 那次是**一个** tool_use 内部在跑，
# transcript 同样不落行）也会被误判——但误判的代价只是发一句「继续」，
# 而漏判的代价是角色永久占位、全链排队（实测 3 个实例 queued 387）。
_STOPPED_PROGRESS_SEC = 300


def _transcript_stalled(role: str, max_age: float = _STOPPED_PROGRESS_SEC) -> bool:
    """角色最新 transcript 是否已 max_age 秒没有新事件。

    pane 的 `esc to interrupt` 只证明 CLI 还挂着 spinner，不证明有进展：
    上游断连后工具调用悬空（tool_use 无 tool_result），spinner 照转。
    transcript 的 mtime 是落盘事实，能区分「在跑」与「卡死」。

    取不到 transcript（路径不存在/权限）→ False：宁可漏判，也不给
    正在干活的角色发 C-u 打断当前轮。
    """
    import glob as _glob
    try:
        d = _TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        files = sorted(_glob.glob(str(d / "*.jsonl")), key=_os.path.getmtime, reverse=True)
        if not files:
            return False
        return (time.time() - _os.path.getmtime(files[0])) >= max_age
    except OSError:
        return False


def _as_bytes(v) -> bytes:
    """子进程输出归一：生产是 bytes，测试/未来 text=True 可能是 str。"""
    return v.encode("utf-8", "replace") if isinstance(v, str) else (v or b"")


def _start_failed(st) -> bool:
    """ccs start 是否失败：非零返回码，或 stdout 协议明说 success=false。"""
    return getattr(st, "returncode", 0) != 0 or \
        b'"success": false' in _as_bytes(getattr(st, "stdout", b""))


def _start_error_text(stdout: bytes | None, stderr: bytes | None) -> str:
    """ccs start 失败时该报哪段输出：stdout 的协议 JSON，不是 stderr 的日志。

    ccs 的 core 模块一 import 就 basicConfig(INFO)，正常启动也会往 stderr 写一行
    "写入 settings.json"。旧实现 stderr 优先，于是 6 条 blocker 的证据全是这句
    INFO（bus #434228/#434257/#434307），真因 {"success": false, ...} 被吞。
    """
    out, err = _as_bytes(stdout), _as_bytes(stderr)
    # 只认失败协议：成功 JSON 同样含 "success"，按该字样选源会在 rc≠0 时把
    # stderr 的真因替换成一句 {"success": true}（codex 20 维 P1）。
    pick = out if b'"success": false' in out else (err or out)
    return pick.decode("utf-8", "replace")[:300]


# 告警冷却的 check+占位临界区（见 _notify_role）。进程级，与 _ALERT_TS 同寿命。
_ALERT_LOCK = threading.Lock()


class RoleLifecycleMixin:
    """WorkflowEngine 的角色管理 mixin。"""
    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"
    _OVERFLOW_THRESHOLD_LINES = 2000   # pane scrollback 行数超过此值触发 /compact（备用指标）
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    # 告警冷却：同一 (role, title) 30min 内只投一次（见 _notify_role）。
    # 状态放类属性（进程级）而非实例：与 _BACKLOG_ALERT_TS 同口径——限流是进程级
    # 事实，一个进程里多个 engine 实例各存一份等于没限流。
    # ponytail: 只在进程内生效；CLI 每次 tick 起新进程时最坏退化为"每次调用一条"。
    # 需要跨进程时改成写 ~/.hermes/run/alert_cooldown.json，先有证据再加。
    _ALERT_TS: dict[tuple, float] = {}
    _ALERT_COOLDOWN_S = 1800
    # 同一 (role, wf, step) 的**相同 prompt** 在窗口内不重发。
    # 实测 24h cross_role_send 24,284 条里 20,499 条（84%）是同 target 同文案重复，
    # 最重一条 product_architect 收 7,290 次、间隔恒 11s（= 每 tick 一次）：每次重发
    # 都要角色花一整个回合重读同一段话，而它早就读过了。
    # 进程级（同 _ALERT_TS）：限流是进程事实。窗口到期仍会重发，丢消息不永久悬空。
    # 窗口取 900s 而非 5min：实测重投节奏是每 tick 一次（间隔 300–310s），300s 的窗口
    # 刚好在下次重投前到期，守卫一次都拦不住（实测 2h 内 198 条里 31 条落在 301–310s）。
    # 同文案重发本来就没有信息增量，宁可少打扰，也不让窗口与 tick 撞线。
    _LAST_SEND: dict[tuple, tuple[int, float]] = {}
    _SAME_PROMPT_WINDOW_S = 900
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
        # 9router 当前 429 文案是 `API Error: Request rejected (429) · [ollama/...]`，
        # 不含上面两条 → 恢复分支整条不可达。实测 close/maintainer 各空等 16min，
        # 今日 [rate_limit] 审计 0 条（#432582）。
        "Request rejected (429)",
        "Rate limit exceeded", "rate_limit_exceeded",
        "FreeTierError", "FreeUsageLimitError",
        "Please run /login",
        # 上游流中断：pane 停在错误、进程活着，此前无任何恢复动作——
        # 实测 curator 12:39 起 21min 无产出，引擎视而不见。
        "upstream connection lost",
    )

    _last_compact: dict[str, float] = {}
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

            # 只看**近期**内容：-S -60 会捞回早已过去的错误，按全文匹配会
            # 在角色已恢复后仍每档退避重发「继续」——实测 6h 内 35 条
            # `[rate_limit] 已发继续`，每次换来一段无进展的自检长文。
            _recent = "\n".join(
                [ln for ln in output.splitlines() if ln.strip()][-20:])
            # 重置判据必须是**进展**，不是**新鲜度**：传 max_age=inf 让"最后一条
            # assistant 就是错误轮"无论多老都算未恢复。旧写法用默认 120s，角色卡在
            # 错误轮不再产新轮时 120s 后谓词翻 False → 清状态 → attempt 恒 1 →
            # 退避曲线失效（24h 实测 614 次注入、相邻间隔 median 248s = 120s 重置
            # 再等 120s 的形状，<120s 占 17.6%；attempt 从未爬到 5 档封顶）。
            # pane 门（前半）不变：真恢复的角色照样立刻清状态、不被拖到 1800s 起步。
            if not any(p in _recent for p in self._RATE_LIMIT_PATTERNS) \
                    and not _transcript_last_turn_is_api_error(role, max_age=float("inf")):
                # pane 未命中不代表已恢复：15/15 会话 alternate_on=1，capture-pane
                # 只有视口，错误文案几分钟就滚出视口（实测 24h 1,342 条错误轮里
                # pane 只认出 9 次 = 0.67%），转录才是落盘事实。
                # 角色已恢复：清掉退避状态，否则下一次真 403 会从最大退避档
                # （1800s）起步，恢复要等半小时。
                self._last_rate_recover.pop(role, None)
                continue
            _tail = [ln for ln in output.splitlines() if ln.strip()][-4:]
            # 429 客户端重试进行中：CLI 自己在退避，发 C-u +「继续」会把 attempt
            # 计数重置回 1，把快恢复的角色重新推回 10/10（实测 devops/closer/qa
            # 的 attempt 在收到「继续」后从 4 回升到 9）。
            # 判据扫 _recent（20 行）而非 _tail（4 行）：`Retrying in 5s · attempt 9/10`
            # 后面还跟着 tip/分隔线/输入框，实测 closer 的该行就不在最后 4 行内
            # → 漏检 → 误发「继续」（06:31:32 日志）。
            if any("Retrying" in ln and "attempt" in ln for ln in _recent.splitlines()):
                continue
            # 能走到这里说明 pane 或转录已给出错误信号（上方前置过滤），
            # 所以只需再判"生成中且 transcript 停滞"：两者同时成立才是卡死。
            # 纯生成中的长推理（consumer 813s、maintainer 686s）在前面就退出了。
            if any("esc to interrupt" in ln for ln in _tail):
                if not _transcript_stalled(role):
                    continue
                LOGGER.warning("角色卡死: %s 生成中+错误信号+transcript %.0fs 无事件",
                               role, _STOPPED_PROGRESS_SEC)
            with self._COMPACT_LOCK:
                last, attempt = self._last_rate_recover.get(role, (0.0, 0))
                wait = self._RATE_LIMIT_BACKOFF_SEC[
                    min(attempt, len(self._RATE_LIMIT_BACKOFF_SEC) - 1)]
                if now - last < wait:
                    continue
                self._last_rate_recover[role] = (now, attempt + 1)

            LOGGER.warning("额度拒绝: %s 命中 403/429，第 %d 次发「继续」重试", role, attempt + 1)
            try:
                _r1 = _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "C-u"],
                    capture_output=True, timeout=5,
                )
                _r2 = _engine_mod._sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", _RATE_LIMIT_NUDGE, "Enter"],
                    capture_output=True, timeout=5,
                )
            except Exception as e:
                LOGGER.warning("额度恢复发送失败 %s: %s", role, e)
                continue
            # subprocess.run 在 rc≠0 时不抛异常：会话在候选集之后死掉只会得到
            # RC=1（实测 tmux send-keys -t ccs-nonexistent → rc=1），不判 rc 就会
            # 写出「已发继续」假审计。
            # 负数 rc（-9 SIGKILL / -15 SIGTERM）与 None（无法判定）都不是成功：
            # max() 会把 -9 折成 0 → 写出「已发继续」假审计（审查 P2，实测可复现）。
            # 默认取 1：判不了失败时就当失败，宁可多等一轮也不要假审计。
            _rcs = [getattr(x, "returncode", 1) for x in (_r1, _r2)]
            if any(_rc != 0 for _rc in _rcs):
                LOGGER.warning("额度恢复发送失败 %s: tmux rc=%s", role, _rcs)
                continue
            # 审计与发送分家：发送异常不再吞掉审计，写入失败单独可见。
            # 标题必须带完整日期时刻——bus 的 facts.t 是 UNIQUE 且 ON CONFLICT
            # 保留首次 ts，而 attempt 随退避重置反复从 1 起算：不带日期时新一天
            # 的注入会覆盖昨天那行（实测今日 8 次注入在 facts 里查不到，最后可见
            # 行停在 2026-10-20 23:46；24h 902 次写只落到 91 行 ≈10:1 合并）。
            try:
                self._bb.write("monitor_audit",
                    f"[rate_limit] {role} 被额度拒绝 → 已发继续"
                    f"（第 {attempt + 1} 次 {time.strftime('%Y-%m-%d %H:%M:%S')}）",
                    evidence=output[-300:], src="workflow_engine")
            except Exception as e:
                LOGGER.warning("额度拒绝审计写入失败 %s: %s", role, e)

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
                # 带时刻：压缩频率是上下文压力的唯一时间线，UPSERT 会把它压成一行。
                self._bb.write("architecture",
                    f"[context_overflow] {role} → /compact {time.strftime('%m-%d %H:%M')}",
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
                # 停滞超过 3 分钟 + 角色近期无产出 + 不忙 → 重推。
                # 2026-10-21: 原先 `if _tc > 0: continue` 与 step_handler 的
                # `timeout_count == 1` 合起来制造无派发空窗（count 2..auto_cancel
                # 期间两路全关，实测 wf_1ce1cf0140/closer 30min21s 零派发，#431798）。
                # tick 侧已改为每次超时都重推，本路只在角色进入"有计数但无产出"
                # 时补位；kick_stalled_at 冷却（600s）保证不每 tick 重推。
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
            _st = _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "infinite", "--no-auto-send"],
                capture_output=True, timeout=60, env=_env,
            )
            # 同一条拉起路径的两个入口（daemon/engine tick 走这里）：返回值曾整体
            # 丢弃，于是"角色起不来"只有 tmux 层的假象，没有原因（codex 20 维 P1）。
            if _start_failed(_st):
                LOGGER.warning("CCS %s: ccs start 失败 (rc=%s): %s", role, _st.returncode,
                               " ".join(_start_error_text(_st.stdout, _st.stderr).split())[:200])
            for _ in range(30):
                time.sleep(1)
                if _engine_mod._sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=5).returncode == 0:
                    time.sleep(3)  # 等 agent 进程启动
                    if self._is_agent_alive(role):
                        # 审计信号：记录角色被 workflow 引擎拉起（供 monitor_audit 查看）
                        # 写失败不阻断拉起（审计是旁路，DB 锁/连接异常不应让角色拉起中断）
                        try:
                            # 带时刻：固定标题会把每次重启压成一行（#288618 写 21 次
                            # 仍只显示"重启 1 次"），崩溃循环在审计里不可见。
                            self._bb.write("monitor_audit",
                                f"决策: 角色拉起 → {role} CCS 已重启 (workflow 驱动)"
                                f" {time.strftime('%m-%d %H:%M:%S')}",
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
                    # 判定用未截断的 stdout：证据只留 300 字符，协议 JSON 若在
                    # 300 之后，按 _err 判会漏写 blocker（codex 20 维 P2）。
                    _err = _start_error_text(_st.stdout, _st.stderr)
                    if _start_failed(_st):
                        LOGGER.error("_wait_for_role_ready: ccs start %s 失败 (rc=%s): %s",
                                     role, _st.returncode, " ".join(_err.split())[:200])
                        try:
                            self._bb.write("blocker",
                                f"[workflow] 角色 {role} 启动失败 (rc={_st.returncode})，"
                                f"工作流无法投递",
                                evidence=_err, src="workflow_engine")
                        except Exception:
                            # blocker 是唯一外部产物：写失败不能只留 DEBUG。
                            LOGGER.warning("role-start blocker 写入失败: %s", role)
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
        """告警通知：写 blocker bus + ccs send 唤醒角色（绕过并发检查）。

        同一 (role, title) 30min 内只投一次：卡住是**持续状态**，每 tick 重发同一条
        只是往角色上下文里灌重复噪音（实测 maintainer 30h 收 265 次「工作流多步骤
        超时: wf_sub_71247」，正文 1→0 来回翻转，且其中 115 次落在 6 小时窗口内）。
        每条重复告警都要占一整个回合的上下文才能读完，状态没变就不该叫醒人。
        批量尖峰（同秒 N 台）由调用方聚合（wf_lifecycle:454 的 _recycled 汇总）。
        """
        _ts = self._ALERT_TS
        _key = (role, title)
        _now = time.time()
        # 锁只包 check+占位，不包 subprocess：8 线程池会同时看到同一条告警
        # （Barrier 20 线程实测放行 20 次）。先占位再投递，失败再还回窗口。
        with _ALERT_LOCK:
            _gap = _now - _ts.get(_key, 0.0)
            # 时间回拨时 _gap 为负，旧写法同样 < 窗口 → 最长静默 1.5h。
            if 0 <= _gap < self._ALERT_COOLDOWN_S:
                LOGGER.debug("notify suppressed (cooldown %ds): %s / %s",
                             self._ALERT_COOLDOWN_S, role, title)
                return
            _ts[_key] = _now
        self._bb.write("blocker", title, evidence=evidence, src="workflow_engine")
        _delivered = False
        try:
            _r = _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "send", role,
                 f"[workflow 告警] {title}\n\n{evidence}",
                 "--from", "workflow_engine"],
                capture_output=True, timeout=15,
            )
            # subprocess.run 在 rc≠0 时不抛异常（同 R121 的教训）：只看异常会把
            # "没叫醒"记成"已叫醒"，角色收不到而 30min 内不再重试。
            _delivered = getattr(_r, "returncode", 0) == 0
        except Exception as _e:
            LOGGER.warning("notify send 异常（窗口已还回，下轮重试）%s / %s: %s",
                           role, title, _e)
        # 冷却只在**真的叫醒之后**保留：bus 写失败 / send 抛错 / rc≠0 都不该吃掉
        # 30min 窗口（告警静默丢失而状态还在，wf_lifecycle 每轮都会再判到它）。
        if not _delivered:
            with _ALERT_LOCK:
                _ts.pop(_key, None)

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
        # 相同 prompt 已在窗口内投递过 → 不重发，按排队返回（不盖 notified、不计失败）。
        # 放在占位符校验之后：非法 prompt 仍走 False（快速暴露），不会被"排队"掩盖。
        _fp = hash((role, wf_id, step_id, prompt))
        _prev = RoleLifecycleMixin._LAST_SEND.get((role, wf_id, step_id))
        if _prev and _prev[0] == _fp and time.time() - _prev[1] < self._SAME_PROMPT_WINDOW_S:
            LOGGER.info("send-to-role %s 跳过: 同一 prompt 已在 %.0fs 内投递过 (wf=%s step=%s)",
                        role, time.time() - _prev[1], wf_id[:12], step_id)
            return "busy"
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
            # 忙 ≠ 失败：ccs.py 对 `{"success": false, "reason": "busy"}` 也以 rc=1
            # 退出（ccs.py:342 只判 success）。这里若按非零码一律判 False，角色每次
            # 长跑期间的重推都会累计 delivery_fail_count，最终灌成 delivery_blocked
            # ——正好抵消 _deliver_once 忙时拒投的意义。
            _out = getattr(_result, "stdout", "") or ""
            if isinstance(_out, bytes):
                _out = _out.decode("utf-8", "ignore")
            _resp = {}
            try:
                _resp = json.loads(_out) if _out.lstrip().startswith("{") else {}
            except (ValueError, TypeError):
                _resp = {}
            # 解析成功用 error 字段；失败则退回原始 stdout —— ccs.py 的 stdout 可能
            # 混入日志前缀/截断 JSON，纯 JSON 解析会丢掉 busy 信号（三方审查 P1）。
            _err = str(_resp.get("error") or "") or _out
            # 忙 ≠ 失败，另外两类「暂缓判定」也不能当失败：
            # core.send 对首次探测失败返回 "探测失败 1/3"、首次 tmux 投递失败返回
            # "tmux 投递失败 1/3"（连败计数未满 3，红线 3 防抖动误报），语义是稍后重试。
            # 计数落 ~/.hermes/run/send_fail_streak.json（跨进程持久），第 3 次即返回
            # "未运行" / "(3/3)" → 走 False，故暂缓分支不会永续。
            # 全按失败计则引擎 10s/tick 内三连推 → 角色一次抖动即被判 delivery_blocked
            # 终态（实测 wf_76d0bcc006/s1 冻结 daemon 后无人判死正是同一处判据的镜像）。
            # 只有连败确认（3/3 后返回 "未运行"）、门禁拒绝、红线拦截才是真失败。
            if (_resp.get("reason") == "busy" or '"reason": "busy"' in _out
                    or '"reason":"busy"' in _out or "暂缓 fallback" in _err):
                LOGGER.info("send-to-role %s 角色忙/探测暂缓，未投递（排队等下轮）", role)
                return "busy"
            _m = re.search(r"tmux 投递失败.*?\((\d+)/(\d+)\)", _err)
            if _m and int(_m.group(1)) < int(_m.group(2)):
                LOGGER.info("send-to-role %s tmux 投递暂缓 %s/%s（排队等下轮）",
                            role, _m.group(1), _m.group(2))
                return "busy"
            LOGGER.warning("send-to-role %s ccs send 退出码=%s (wf=%s step=%s) err=%s",
                           role, getattr(_result, "returncode", None),
                           wf_id[:12] if wf_id else "?", step_id, _err[:200])
            return False  # 非零退出码且非暂缓 = 投递失败：调用方据此不盖戳，下轮重试
        # 只有真投递成功才记账：失败/暂缓不占窗口，下轮照常重试。
        RoleLifecycleMixin._LAST_SEND[(role, wf_id, step_id)] = (_fp, time.time())
        return True  # 投递成功（ccs send 正常退出）
