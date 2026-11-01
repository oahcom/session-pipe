#!/usr/bin/env python3
"""RoleLifecycleMixin — 角色存活管理、忙检测、tmux 操作、通知/推送。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import fcntl
import json
import re
import sqlite3
import time
import logging
import threading
import os as _os
from datetime import datetime, timezone
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效
from paths import CCS_CLI as _CCS_CLI
from liveness import tmux_snapshot as _tmux_snapshot, invalidate_tmux_snapshot as _invalidate_tmux
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
# 存活正向缓存（秒）：一次 pane 快照探测实测 0.1~2.4s，tmux 卡顿时顶到 5s 超时
_ALIVE_CACHE_SEC = 120.0

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


def _transcript_mtime(role: str) -> float | None:
    """角色最新 transcript 的 mtime；没有转录文件时返回 None。

    WHY 单独一个函数：投递记账要判「rc=0 之后角色转录有没有长出新行」，
    只读 mtime 不必解析 7.8MB 文件（_read_transcript_tail 只读尾部也有开销）。
    返回 None 表示信号不可得，调用方必须退回旧行为，不能当成「没提交」。
    """
    import glob as _glob
    try:
        _d = _TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        files = _glob.glob(str(_d / "*.jsonl"))
        if not files:
            return None
        return max(_os.path.getmtime(f) for f in files)
    except OSError:
        return None


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
# 「0 产出」是假的：6h 内 11 次中断里 10 次被杀前已调 7~92 次工具（几乎全 Bash），
# 断言零产出会让角色重做已做过的动作。改成"先核对落盘，再重发被中断的那次请求"。
# 末句原为「不要写自检报告」：它连同证据一起禁掉了，而这条注入正是角色失忆后唯一的
# 上下文锚。改要一行可验证据（与 user_intent_reminder 同一口径），报告仍然不要。
_RATE_LIMIT_NUDGE = ("上一轮 API 错误中断。继续，但先核对已完成的动作有没有落盘（读/写可能只做了一半），"
                     "再重发被中断的那次请求；不要重新规划；收尾回一行可验证据（命令或文件:行），不要写报告。")

# 额度/出口硬限（重发不能恢复）与「被瞬时限流打断」是两件事：前者重发只会再撞
# 同一面墙——24h 实测 255 次注入里 223 次（87.5%）600s 内又报同一错，只有 3.5% 恢复。
# 每次 429 还会让 ASIP 判出口脏并轮换 IP（R115），高频重发是在制造抖动。
# 处置：硬限态改 1h 探活（额度回来仍能自愈），审计每角色 6h 最多一条。
# 词表按 1,039 份真实转录的末轮错误原文选（命中数）：FreeTierError 2029 /
# FreeUsageLimitError 701 / Free usage limit 681 / free-models-per-day 220。
# `current egress`（18 条）是短窗口限流，原文让客户端「按提示重试」→ 必须排除，
# 否则会把可秒级恢复的限流压成 1h。
_QUOTA_HINTS = ("FreeUsageLimitError", "FreeTierError", "free-models-per-day",
                "Free usage limit")
_QUOTA_TRANSIENT = ("current egress",)
_QUOTA_RETRY_SEC = 3600
_QUOTA_REPORT_COOLDOWN = 6 * 3600
# 重启后回填冷却用：monitor_audit 约 150 条/天，80 条覆盖 ~12h > 6h 冷却窗口。
_QUOTA_SEED_ROWS = 80

# 终态错误：重发永远不会恢复，只有换模型重启会话才行。与「限流/额度」是两件事——
# 后者等一会儿会好，前者等多久都撞同一面墙。实测 410 ModelDeprecated 被当成瞬时
# 限流走了 5 档退避，第 5 档后永久 1800s 重发，pm 输入框积 10 份 nudge、13.1h 零产出
# （#463020）。判据只认错误原文里的错误类型，不认 410 状态码本身：410 也可用于
# 别的一次性语义。
_TERMINAL_HINTS = ("ModelDeprecated",)


def _is_terminal_api_error(text: str) -> bool:
    """这条 API 错误重发能恢复吗？模型被下线 = 不能。"""
    return any(h in (text or "") for h in _TERMINAL_HINTS)


def _seed_report_ts(bb, role: str) -> float:
    """跨重启回填审计冷却：bus 里已有该角色的报告行就取最近一条的 ts。

    进程内字典在 execv/restart 后归零，实测 knowledge_curator 15:39 / 17:10 /
    20:26 三条同角色 [quota]（6h 冷却形同虚设）。bus 里已有这些行，回填最近一条，
    不再另开状态文件。
    """
    try:
        return max((f.ts for f in bb.read(cat="monitor_audit", limit=_QUOTA_SEED_ROWS)
                    if f.t.startswith(f"[quota] {role} ")
                    or f.t.startswith(f"[terminal] {role} ")), default=0.0)
    except Exception:      # 读不到就当没记过账，不阻断恢复
        return 0.0


def _type_and_submit(tmux_name: str, text: str) -> list[int]:
    """清空输入框 → 按字面量写入 text → **单独**发一次 Enter。返回各步 rc。

    文本与 Enter 必须分两次 send-keys：同一次调用里带 Enter 时，Enter 落在多行
    文本中间而非结尾，实测干净 pane 落地 0/3，分调用 3/3（#459931 A/B，真值取
    转录 user 轮）。`-l --` 让文本按字面量写入：文本含 `-` 开头、或含 tmux 键名
    （q / C-c / Space）时不被当成按键。rc 由调用方判——tmux 对"文本写进输入框
    但没提交"同样返回 0，rc=0 只证明按键送到了。
    """
    _sp = _engine_mod._sp
    rcs = []
    for cmd in (
        ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "C-u"],
        ["tmux", "send-keys", "-l", "-t", f"{tmux_name}:0.0", "--", text],
        ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "Enter"],
    ):
        rcs.append(getattr(_sp.run(cmd, capture_output=True, timeout=5),
                           "returncode", 1))
    return rcs


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


def _last_api_error_text(role: str) -> str:
    """最后一条 assistant 错误轮的正文；最后一轮不是错误 → ""。

    只用来分清「配额耗尽」和「瞬时限流」：两类都要唤醒，但前者 1h 一次就够，
    后者按原退避曲线。取不到正文按瞬时处理（宁可多试一次，不要漏掉可恢复的）。
    """
    for line in _read_transcript_tail(role):
        if '"type":"assistant"' not in line.replace(" ", ""):
            continue
        if '"isApiErrorMessage":true' not in line.replace(" ", ""):
            return ""
        try:
            content = json.loads(line).get("message", {}).get("content")
        except ValueError:
            return ""
        if isinstance(content, list):
            return " ".join(b.get("text") or "" for b in content
                            if isinstance(b, dict)).strip()
        return content if isinstance(content, str) else ""
    return ""


def role_erroring(role: str) -> bool:
    """角色是否停在 API 错误上（不可投递）—— 「转录在写」不等于「在干活」。

    复用 _transcript_last_turn_is_api_error：结构字段 isApiErrorMessage + 同一份
    错误词表（429/403/Please run /login/断连），不另立第二套判据。
    max_age=inf：错误不会因为「过了两分钟」就变成可用，恰恰相反——429 的角色
    往往几小时不再产出，按 120s 窗口判会把它们读成健康。
    """
    return _transcript_last_turn_is_api_error(role, max_age=float("inf"))


def _transcript_progress_after_error(role: str) -> bool:
    """最新一轮里是否**真的**有了进展（有工具调用的 assistant 轮）。

    这是「能不能清退避状态」的判据。旧判据（最后一条 assistant 不是 API 错误）会被
    错误轮自己的**纯文本半成品**骗过：注入「继续」后 CLI 先落一条无 tool_use 的
    assistant 行，旧判据当场清状态 → attempt 归 1 → 下一次 429 立刻再发（实测
    3h 内 21 次唤醒，错误→唤醒间隔 1/3/4/9/15s，首档 120s 完全没生效）。

    只认 tool_use：那才是"重发的那次请求真的跑起来了"。纯文本轮不证明任何进展，
    继续往前找，直到撞上错误轮（返回 False）或带 tool_use 的轮（返回 True）。
    """
    for line in _read_transcript_tail(role):
        _c = line.replace(" ", "")
        if '"type":"assistant"' not in _c:
            continue
        if '"isApiErrorMessage":true' in _c:
            return False
        if '"tool_use"' in _c:
            return True
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


def _tool_use_within(role: str, sec: float = 60) -> bool:
    """转录里最近一条 tool_use 是否在 sec 秒内 = 角色正在跑工具。

    正在跑工具时注入会被 CLI 吞掉（C-u 与工具输出撞车）：实测 6h 12 次注入有 1 次
    转录里完全没有落地，而 tmux rc=0 → audit 仍写「已发继续」= 假审计。而且角色
    在推进时本来也不需要「继续」。取不到转录按 False（宁可多等一轮，不打断）。
    """
    import glob as _glob
    from datetime import datetime as _dt
    try:
        _d = _TRANSCRIPT_ROOT / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
        files = sorted(_glob.glob(str(_d / "*.jsonl")), key=_os.path.getmtime, reverse=True)
        if not files:
            return False
        with open(files[0], "rb") as fh:
            fh.seek(0, 2)
            fh.seek(max(0, fh.tell() - 65536))
            tail = fh.read().decode("utf-8", "replace")
    except OSError:
        return False
    cut = time.time() - sec
    for line in reversed(tail.splitlines()):
        if '"tool_use"' not in line.replace(" ", ""):
            continue
        try:
            _ts = json.loads(line).get("timestamp") or ""
            t = _dt.fromisoformat(_ts.replace("Z", "+00:00")).timestamp()
        except (ValueError, AttributeError):
            continue
        return t >= cut
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

# 告警冷却落盘：daemon 重启、CLI 每 tick 起新进程都会清空进程内表。实测同一条
# verify traceback 33 分钟内注入 4 次（reviewer f706ce80 的 1114/1482/1561/1606），
# 每次都要角色花一整个回合重读同一段话。
_ALERT_STATE = Path(_os.environ.get("HERMES_RUN_DIR")
                    or Path.home() / ".hermes" / "run") / "alert_cooldowns.json"
_ALERT_MAX_ENTRIES = 200      # 表的上限：超出按时间留最新的，别让文件无限长

# 额度拒绝退避复用同一张表：__rate_ts__ 存最近一次发「继续」的时间，__rate_n__ 存
# attempt 计数。计数行**不受 TTL 裁剪**——它的值是 1、2、3，按时间裁会被当成 1970 年
# 条目整行丢掉（G14：落盘了但永远读回 0，退避每次重启从 120s 首档重来）。
_RATE_TS_TAG = "__rate_ts__"
_RATE_N_TAG = "__rate_n__"


def _load_alert_ts(path: Path, now: float, ttl: float) -> dict:
    """读冷却表；坏文件/读失败当空表——限流失效不等于告警丢失。"""
    try:
        rows = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    out = {}
    for row in rows:
        try:
            role, title, ts = row
            if title == _RATE_N_TAG or now - float(ts) < ttl:   # 过期条目直接丢
                out[(role, title)] = float(ts)
        except (TypeError, ValueError):
            continue
    return out


def _save_alert_ts(path: Path, ts: dict, now: float, ttl: float,
                   override=(), drop=()) -> None:
    """读盘→合并→裁剪→原子写，全程持跨进程文件锁。

    合并而不是整表覆盖：两个进程（workflow-engine / routing_daemon）各持一份内存快照，
    谁后写就把对方刚占的位抹掉 → 同一条告警再投一次（codex 20 维 P1）。
    锁文件独立于数据文件：replace 会换 inode，锁在数据文件上等于没锁。
    临时文件名带 pid：两进程同名 .tmp 互相 replace 会丢更新。
    drop：必须显式删除的键（角色已恢复）。不能靠"写 0 再等 TTL 裁掉"——TTL 大于
    当前时间戳时 0 反而留在盘上，恢复清除就失效了。
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path.with_suffix(".lock"), "a+") as _fh:
            fcntl.flock(_fh, fcntl.LOCK_EX)
            merged = dict(_load_alert_ts(path, now, ttl))
            for key, val in ts.items():
                # override：投递失败要**提前**时间戳（600s 后重试），合并取 max 会把它顶回去
                if key in override or val > merged.get(key, 0.0):
                    merged[key] = val
            for key in drop:
                merged.pop(key, None)
            # 内存表跟着裁剪：长驻 daemon 的 _ts 否则只增不减（codex 20 维 P1）
            ts.clear()
            ts.update({k: v for k, v in merged.items()
                       if k[1] == _RATE_N_TAG or now - v < ttl})
            # 计数行排最前：它的值只有个位数，按值排序会被 200 条上限最先裁掉
            rows = sorted(ts.items(), key=lambda kv: (kv[0][1] == _RATE_N_TAG, kv[1]),
                          reverse=True)[:_ALERT_MAX_ENTRIES]
            tmp = path.with_name(f"{path.name}.{_os.getpid()}.tmp")
            tmp.write_text(json.dumps([[r, t, v] for (r, t), v in rows]))
            tmp.replace(path)
    except OSError as e:      # 落盘失败只退化成进程内冷却，不该影响告警投递
        LOGGER.debug("alert cooldown 落盘失败: %s", e)


# 额度拒绝的退避（_last_rate_recover）是同一类缺陷：也是进程内字典，daemon 一重启
# 就从 120s 首档重来。实测 2h 内「上一轮 API 错误中断（0 产出）」注入 29 次、
# 跨 13 角色 22 个 session（qa 15 分钟 4 次）。用两个 tag 复用上面的读写与文件锁，
# 不再另开一套（tag 定义见 _ALERT_MAX_ENTRIES 处）。


def _load_rate_state(path: Path, now: float, ttl: float) -> dict:
    """role -> (last_ts, attempt)。读失败/坏文件返回空表 = 退避从头开始，不阻断恢复。"""
    rows = _load_alert_ts(path, now, ttl)
    return {role: (val, int(rows.get((role, _RATE_N_TAG), 0.0)))
            for (role, tag), val in rows.items() if tag == _RATE_TS_TAG}


def _save_rate_state(path: Path, states: dict, now: float, ttl: float,
                     delete_roles: tuple = ()) -> None:
    """写退避状态；delete_roles 里的角色写 0 时间戳 → 被裁剪规则直接从盘上抹掉。

    override：内存表在读盘后已同步，这里写的就是权威值；不 override 会被
    「取较大者」的合并规则顶回去，恢复清除（0）永远清不掉。
    """
    flat = {}
    for role, (ts, n) in states.items():
        flat[(role, _RATE_TS_TAG)] = float(ts)
        flat[(role, _RATE_N_TAG)] = float(n)
    drop = tuple((role, tag) for role in delete_roles
                 for tag in (_RATE_TS_TAG, _RATE_N_TAG))
    _save_alert_ts(path, flat, now, ttl, override=tuple(flat), drop=drop)


class RoleLifecycleMixin:
    """WorkflowEngine 的角色管理 mixin。"""
    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    # 告警冷却：同一 (role, title) 30min 内只投一次（见 _notify_role）。
    # 状态放类属性（进程级）而非实例：与 _BACKLOG_ALERT_TS 同口径——限流是进程级
    # 事实，一个进程里多个 engine 实例各存一份等于没限流。
    # 落盘（2026-10-22）：原来只在进程内生效，daemon 重启或 CLI 每 tick 新进程都会清空
    # 内存表 → 实测同一条 verify traceback 33 分钟注入 4 次。现在每次判定前都从
    # $HERMES_RUN_DIR/alert_cooldowns.json（默认 ~/.hermes/run）合并一次，占位后持锁回写。
    _ALERT_TS: dict[tuple, float] = {}
    _ALERT_COOLDOWN_S = 1800
    _ALERT_TTL_S = 3600    # 落盘条目保留 2 倍冷却窗口：够覆盖重启间隔，又不会无限增长
    # 投递失败后的重试间隔（秒）。必须 < _ALERT_COOLDOWN_S：下面写的是
    # now - COOLDOWN + RETRY，RETRY >= COOLDOWN 就写成未来时间戳 → 每 tick 重投。
    _ALERT_RETRY_S = 600
    # 同一 (role, wf, step) 的**相同 prompt** 在窗口内不重发。
    # 实测 24h cross_role_send 24,284 条里 20,499 条（84%）是同 target 同文案重复，
    # 最重一条 product_architect 收 7,290 次、间隔恒 11s（= 每 tick 一次）：每次重发
    # 都要角色花一整个回合重读同一段话，而它早就读过了。
    # 进程级（同 _ALERT_TS）：限流是进程事实。窗口到期仍会重发，丢消息不永久悬空。
    # 窗口取 900s 而非 5min：实测重投节奏是每 tick 一次（间隔 300–310s），300s 的窗口
    # 刚好在下次重投前到期，守卫一次都拦不住（实测 2h 内 198 条里 31 条落在 301–310s）。
    # 同文案重发本来就没有信息增量，宁可少打扰，也不让窗口与 tick 撞线。
    _LAST_SEND: dict[tuple, tuple[int, float, float | None]] = {}
    _SAME_PROMPT_WINDOW_S = 900
    # rc=0 之后多久才要求转录增长（秒）。窗口内先给角色启动时间：
    # 实测提交成功后 1s 内就落 user 轮，但要留余量给慢启/排队。
    _SEND_EVIDENCE_GRACE_S = 120
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
            _pane_err = any(p in _recent for p in self._RATE_LIMIT_PATTERNS)
            # 错误信号 = pane 文案 或 转录最后一条就是错误轮。两者都没有时
            # 必须直接跳过：ccebbcf 把判据换成"错误后有进展"后，健康角色
            # （pane 干净、转录为空）progress=False 会落到下面的发送分支，
            # 每个 tick 给健康角色发一句「继续」（6 个 rate_recovery 用例红）。
            if not _pane_err and not _transcript_last_turn_is_api_error(
                    role, max_age=float("inf")):
                # 无错误信号 = 没有待重发的请求。只在"确认已恢复"时清退避，否则下一次
                # 真 403 会从最大退避档（1800s）起步、恢复要等半小时；错误轮后只落了
                # 纯文本半成品不算恢复（清了会让 429 退避从第 1 档重来）。
                # 转录读不到（新角色/路径缺失/空文件）= 无证据，按已恢复处理。
                if (_transcript_progress_after_error(role)
                        or not _read_transcript_tail(role)):
                    self._last_rate_recover.pop(role, None)
                    _save_rate_state(_ALERT_STATE, self._last_rate_recover, now,
                                    self._ALERT_TTL_S, delete_roles=(role,))
                # 清完直接跳过本轮，不再落到下面的发送分支
                # （健康角色每个 tick 被敲一句「继续」）。
                continue
            # 正在跑工具时不发：C-u 会与工具输出撞车，文本被吞但 tmux rc=0 →
            # 写出「已发继续」假审计（实测 6h 12 次注入中 1 次转录无落地）。
            if _tool_use_within(role):
                continue
            # 额度/出口硬限与瞬时限流分开（见 _QUOTA_HINTS 注释）：前者 1h 探活一次，
            # 后者按原退避曲线。pane 有错误行就先用它，否则读转录最后一条错误。
            # 取**最新**命中行（reversed）：窗口上方残留的旧配额行不该覆盖下方的新 403。
            _why = next((ln for ln in reversed(_recent.splitlines())
                         if any(p in ln for p in self._RATE_LIMIT_PATTERNS)), "")
            if not any(h in _why for h in _QUOTA_HINTS):
                _why = _last_api_error_text(role) or _why
            _quota = (any(h in _why for h in _QUOTA_HINTS)
                      and not any(t in _why for t in _QUOTA_TRANSIENT))
            # 终态错误不进退避曲线：重发不会恢复，只会把同一段 88 字文本一遍遍
            # 叠进输入框（实测 10 份 / 13.1h 零产出）。每角色 6h 一条审计就够，
            # 人工换模型重启会话才是处置。
            if _is_terminal_api_error(_why):
                _rep = getattr(self, "_quota_reported", None)
                if _rep is None:
                    self._quota_reported = _rep = {}
                if role not in _rep:
                    _rep[role] = _seed_report_ts(self._bb, role)
                if now - _rep[role] >= _QUOTA_REPORT_COOLDOWN:
                    self._bb.write("monitor_audit",
                        f"[terminal] {role} 模型已下线，停止重发"
                        f"（{time.strftime('%Y-%m-%d %H:%M')}）",
                        evidence=(_why or output[-300:])[:300], src="workflow_engine")
                    _rep[role] = now
                # 终态不是"暂时失败"：清掉退避记录，别让恢复后第一次限流从第 5 档起步
                self._last_rate_recover.pop(role, None)
                _save_rate_state(_ALERT_STATE, self._last_rate_recover, now,
                                 self._ALERT_TTL_S, delete_roles=(role,))
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
                # 退避状态落盘（与 _ALERT_TS 同一套文件/锁）：daemon 重启后 attempt
                # 从 0 重新爬，退避曲线等于没有 → 实测 2h 内同一句「上一轮 API 错误
                # 中断」注入 29 次、跨 13 个角色。
                # 注意 TTL 与 1h 等待相等：条目到期被裁只让 attempt 归 1、下一次
                # 照常按 wait 放行（不是每 tick 放行），故此处不额外调大 TTL。
                self._last_rate_recover.update(
                    _load_rate_state(_ALERT_STATE, now, self._ALERT_TTL_S))
                last, attempt = self._last_rate_recover.get(role, (0.0, 0))
                wait = (_QUOTA_RETRY_SEC if _quota else
                        self._RATE_LIMIT_BACKOFF_SEC[
                            min(attempt, len(self._RATE_LIMIT_BACKOFF_SEC) - 1)])
                if now - last < wait:
                    continue
                self._last_rate_recover[role] = (now, attempt + 1)
                _save_rate_state(_ALERT_STATE, self._last_rate_recover, now,
                                 self._ALERT_TTL_S)

            LOGGER.warning("额度拒绝: %s 命中 403/429（%s），第 %d 次发「继续」重试",
                           role, "硬限" if _quota else "瞬时", attempt + 1)
            try:
                _rcs = _type_and_submit(tmux_name, _RATE_LIMIT_NUDGE)
            except Exception as e:
                LOGGER.warning("额度恢复发送失败 %s: %s", role, e)
                continue
            # subprocess.run 在 rc≠0 时不抛异常：会话在候选集之后死掉只会得到
            # RC=1（实测 tmux send-keys -t ccs-nonexistent → rc=1），不判 rc 就会
            # 写出「已发继续」假审计。
            # 负数 rc（-9 SIGKILL / -15 SIGTERM）与 None（无法判定）都不是成功：
            # max() 会把 -9 折成 0 → 写出「已发继续」假审计（审查 P2，实测可复现）。
            # 默认取 1：判不了失败时就当失败，宁可多等一轮也不要假审计。
            if any(_rc != 0 for _rc in _rcs):
                LOGGER.warning("额度恢复发送失败 %s: tmux rc=%s", role, _rcs)
                continue
            # 审计与发送分家：发送异常不再吞掉审计，写入失败单独可见。
            # 标题必须带完整日期时刻——bus 的 facts.t 是 UNIQUE 且 ON CONFLICT
            # 保留首次 ts，而 attempt 随退避重置反复从 1 起算：不带日期时新一天
            # 的注入会覆盖昨天那行（实测今日 8 次注入在 facts 里查不到，最后可见
            # 行停在 2026-10-20 23:46；24h 902 次写只落到 91 行 ≈10:1 合并）。
            try:
                # 硬限态的 1h 探活不重复记账：24h 实测 255 条 [rate_limit] 占 bus
                # 68.7%，87.5% 在 600s 内又报同一错——每敲一次写一条只是把日志
                # 搬进 bus。每角色 6h 一条，且带上真正的错误原文（旧写法
                # evidence=output[-300:] 252/255 是 UI 壳，判不了因）。
                _rep = getattr(self, "_quota_reported", None)
                if _rep is None:
                    self._quota_reported = _rep = {}
                if role not in _rep:
                    _rep[role] = _seed_report_ts(self._bb, role)
                if _quota and now - _rep[role] < _QUOTA_REPORT_COOLDOWN:
                    continue
                self._bb.write("monitor_audit",
                    (f"[quota] {role} 额度/出口硬限，改 1h 探活（第 {attempt + 1} 次 "
                     f"{time.strftime('%Y-%m-%d %H:%M')}）" if _quota else
                     f"[rate_limit] {role} 非硬限错误（流中断/瞬时限流）→ 已发继续"
                     f"（第 {attempt + 1} 次 {time.strftime('%Y-%m-%d %H:%M:%S')}）"),
                    evidence=(_why or output[-300:])[:300], src="workflow_engine")
                if _quota:          # 写成功才记账：写失败下轮还能再报
                    _rep[role] = now
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
            # 只认视口**尾部 30 行**：-S -500 连带历史一起抓，而信号词的字面量就在本文件里，
            # 角色 grep/读过 role_lifecycle.py 后输出里的匹配串会被当成溢出 —— 实测
            # coordinator 24h 自触发 34 次，命中在第 417/524 行（自己跑的 grep 结果），
            # 底部 30 行无溢出；真实溢出是最后一轮输出，落在尾窗内。
            _tail = output.splitlines()[-30:]
            # 状态栏形态按**单行**判：本文件第 713 行源码同时含 Context/remaining/compact
            # 三个词，角色 grep 到它就会命中整段字符串拼接的旧判据（2026-10-27 17:24:54
            # coordinator 仍有 1 次自触发）。真横幅是 `Context low (12% remaining) · Run /compact`，
            # 带百分号；源码行没有。
            _sig = next((_p for _l in _tail for _p in _OVERFLOW_PATTERNS if _p in _l), "")
            if not _sig and any("Context" in _l and "remaining" in _l and "%" in _l for _l in _tail):
                _sig = "status-bar"

            # `-S -500` 与 `-S -5000` 返回逐字节相同的视口内容，故溢出报错一旦滚出
            # 视口就再也抓不到。transcript 是落盘事实，不受视口与进程存活影响。
            #

            # 而本函数从未触发过一次 /compact——只有 pane 单路时角色溢出后永久卡死
            # （product-architect 90 次 / consumer 35 次）。CLAUDE.md 早已声明本路存在，
            # 实为缺失实现。
            if not _sig and _transcript_shows_overflow(role):
                _sig = "transcript"
            if not _sig:
                continue

            LOGGER.info("上下文溢出: %s 信号=%s → 发送 /compact", role, _sig)
            try:
                # 先清输入行：溢出时角色常卡在未提交的半截 prompt 上，
                # 直接发 /compact 会被拼到该行尾变成普通文本而非斜杠命令
                # （实测遗留文本 + "/compact" → 不执行，角色继续溢出）。
                _type_and_submit(tmux_name, "/compact")
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
    def _is_agent_alive(role: str, fresh: bool = False) -> bool | None:
        """检查 CCS tmux pane 是否存活。三态：True=活 / False=判死 / None=探测失败。

        ponytail: 用 pane 存活判定而非进程名白名单——claude agent 执行 /goal 时
        pane_current_command 可能是子进程 wrapper（bash/sleep/node），不在
        白名单会误判"无 agent"→ kill-session 杀掉正在工作的角色（观测：pm
        工作时被杀 8 次，2026-08-09）。tmux 会话存在即视为 agent 存活，
        只有 pane 完全消失才判定失活。

        None 与 False 必须区分：tmux 瞬时不可达（实测 578ms 抖动）时返回 False
        会让调用方 kill 掉正在工作的活角色。宁可放过一次真死，不可误杀活角色。
        """
        # fresh=True 绕过快照缓存：kill-session 前的二次确认必须看实时状态，
        # 否则两次读同一份快照，二次确认退化成一个空动作。
        snap = _tmux_snapshot(max_age=0.0 if fresh else 2.0)
        if snap is None:
            return None  # 探测失败 ≠ 判死
        # pane 存在且有 pid → agent 存活；pane 消失/无输出 → 失活
        return bool((snap.get(role) or ("", ""))[0])

    def _ensure_role_alive(self, role: str, deadline: float = 0.0) -> bool:
        """存活探测 + 拉起；正向结果缓存 _ALIVE_CACHE_SEC（实测单次探测 4-5.6s）。

        实测 2026-10-22 03:3x：has-session + _is_agent_alive 两次往返各 2.4s（现已合并为一次快照）。
        _ensure_guard_roles 每 tick 对活跃链的全部 guard/wait 角色各探一次，
        十几个角色就是 40-95s 的纯探测开销，与投递/推进争同一个 tick。
        角色死后最迟 120s 被发现；投递前 _wait_for_role_ready 仍独立复核，不靠这份缓存。
        """
        _now = time.time()
        _cache = getattr(self, "_alive_until", None)
        if _cache is None:
            _cache = self._alive_until = {}
        if _cache.get(role, 0.0) > _now:
            return True
        if self._check_and_start_role(role, deadline=deadline):
            _cache[role] = time.time() + _ALIVE_CACHE_SEC
            return True
        return False

    def _check_and_start_role(self, role: str, deadline: float = 0.0) -> bool:
        """检查角色的 CCS 是否存活（pane 存在即算活），不存活则拉起。

        deadline：调用方（守护巡检）给的绝对时刻。拉起 `ccs start` 原本写死 60s
        超时 + 30×1s 轮询，单角色最坏 95s——巡检只在**角色之间**看预算，于是
        一个角色就能把整个 tick 预算吃光（实测 run_once 卡在 _ensure_guard_roles
        的 sleep(3) 里，全量套件第 29 例 15 分钟 0 前进）。
        """
        # 判据只有一条：pane 是否存在（_is_agent_alive）。原来先跑 has-session 再跑
        # _is_agent_alive，是同一件事付两次往返（各 2.4s，卡顿时各 5s 超时）——19 个
        # 角色 × 每 tick 就是整轮 tick（实测 2026-10-22 12:45 单次 has-session 101.8s，
        # 巡检 480s）。None=探测不可用，不判死也不拉起；False 只意味着没有这个 pane，
        # 没有东西可 kill —— 原来那套「二次确认 + kill-session」因此是空动作（每个死
        # 角色白付 1s sleep + 一次绕过缓存的快照），删掉。
        if self._is_agent_alive(role) is not False:
            return True

        # 预算已耗尽就别拉起：拉起会先盖 300s 冷却戳（下面 _last_restart），
        # 掐断后既不在缓存里、又踩冷却，而巡检游标正好把它排到下一轮首位。
        if deadline and deadline - time.time() <= 0:
            LOGGER.warning("CCS %s: 巡检预算已耗尽，本轮不拉起", role)
            return False
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
        # 预算内拉起：deadline 留给巡检时，子进程超时必须跟着缩，否则"到点就停"
        # 只在角色之间成立，单个 ccs start 仍可独占 60s（tests/test_guard_budget.py）。
        try:
            _st = _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "infinite", "--no-auto-send"],
                capture_output=True, timeout=60 if not deadline else max(5.0, min(60.0, deadline - time.time())), env=_env,
            )
            # 同一条拉起路径的两个入口（daemon/engine tick 走这里）：返回值曾整体
            # 丢弃，于是"角色起不来"只有 tmux 层的假象，没有原因（codex 20 维 P1）。
            if _start_failed(_st):
                _err = _start_error_text(_st.stdout, _st.stderr)
                LOGGER.warning("CCS %s: ccs start 失败 (rc=%s): %s", role, _st.returncode,
                               " ".join(_err.split())[:200])
                # blocker 是资源类启动失败（swap 0MB / 内存不足）唯一的外部产物：
                # 只留 WARNING 时，两条拉起路径里只有 _wait_for_role_ready 那条写 blocker。
                try:
                    self._bb.write("blocker",
                                   f"[workflow] 角色 {role} 启动失败 (rc={_st.returncode})，"
                                   f"工作流无法投递",
                                   evidence=_err, src="workflow_engine")
                except Exception:
                    LOGGER.warning("role-start blocker 写入失败: %s", role)
            for _ in range(30):
                if deadline and time.time() > deadline:
                    LOGGER.warning("CCS %s: 拉起预算耗尽，剩余流程交给下一轮巡检", role)
                    break
                time.sleep(1)
                _invalidate_tmux()   # 刚拉起，让下一次探针拿到新会话
                _s = self._is_agent_alive(role)
                if _s is None:
                    # tmux 卡死：别在 30×1s 里继续空转，交给下一轮巡检
                    LOGGER.warning("CCS %s: 拉起后探测不可用，交给下一轮巡检", role)
                    break
                if _s is False:
                    continue
                time.sleep(3)        # 等 agent 进程启动
                if self._is_agent_alive(role, fresh=True):
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
        except (ValueError, KeyError, TypeError, _engine_mod._sp.TimeoutExpired):
            LOGGER.debug("CCS role alive check failed for %s", role)
        return False

    def _wait_for_role_ready(self, role: str, wf_id: str = "", step_id: str = "",
                              max_wait: int = 90, check_interval: int = 5) -> bool:
        """等待角色就绪：pane 存在（_is_agent_alive）。忙闲由投递层判，不在这里等。

        这是 dispatch 前的关键检查，确保角色真正就绪才发送任务。
        """
        deadline = time.time() + max_wait
        _tag = f"{role}（wf={wf_id or '-'} step={step_id or '-'}）"

        def _nap() -> None:
            # 睡眠不得越过 deadline：否则预算耗尽的最后一次还要白睡满 5s，
            # 多角色顺序调用时把 tick 又堆回 max_wait+check_interval。
            time.sleep(max(0.0, min(check_interval, deadline - time.time())))

        while time.time() < deadline:
            # 探测不可用（tmux server 卡死，实测 2026-10-22 09:18 list-sessions 20s
            # 无响应）≠ 角色死了：必须先于 ccs start 判断，否则整轮先烧
            # ccs start 60s + 10×1s 等待才轮到它。此刻投递走不通，交给下一 tick。
            if (_alive := self._is_agent_alive(role)) is None:
                LOGGER.warning("_wait_for_role_ready %s: tmux 探测不可用，本轮不投递", _tag)
                return False
            # 1. pane 存活（与上面同一次快照探测，不再逐角色付 has-session）
            if not _alive:
                # 拉起角色
                LOGGER.info("_wait_for_role_ready: role %s not alive, starting...", role)
                # 拉起只走 _check_and_start_role 一条路：这里原来内联复制了一份（约 40 行），
                # 但没有 _RESTART_LOCK / 300s 冷却 / monitor_audit，于是 90s 窗口内对同一角色
                # 连发 6 次 ccs start（实测 pg 08:16:30-08:18:08 每 15s 一次，全部因内存不足必败，
                # 每次阻塞 60s），而审计计数看不见这些尝试（24h 记 36 次 = 低估）。
                # 带上本函数的 deadline：不传时拉起走默认 0.0，单个角色可用
                # ccs start 60s + 30 轮轮询把整个 max_wait 吃穿（=tick 预算被单角色吃光，
                # 与 _ensure_guard_roles 同一缺陷；codex 20 维第 3 轮）。
                if not self._check_and_start_role(role, deadline):
                    _nap()
                    continue
                # 拉起函数是 bool 契约：None（探测不可用）与「已拉起」共用 True。
                # 投递侧必须自己复核一次——原来靠下面那条「_probe 为假就等 5s」兜住，
                # 该分支已随重复探针删除，不复核就会把任务发给刚判不存在的 pane
                # （codex 20 维两轮都指到这里）。
                if self._is_agent_alive(role) is not True:
                    _nap()
                    continue

            # 存活即可投递：这里原来还探一次 _is_role_busy，但 busy 与空闲都 return True
            # （排队语义由 _send_to_role 的 pane_cli_busy 判定承担）→ 白付一次探测，
            # tmux 卡顿还会把已判活的投递拖过 max_wait（codex 20 维第 4 轮）。
            LOGGER.info("Role %s is ready (pane alive)", role)
            return True

        LOGGER.warning("_wait_for_role_ready: %s %ds 内未就绪", _tag, max_wait)
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
        # （Barrier 20 线程实测放行 20 次）。先占位 30min，投递失败改占 600s。
        with _ALERT_LOCK:
            # 每次都合并一次盘上最新表：别的进程刚占的位、或本进程重启前写的位都要生效
            _ts.update(_load_alert_ts(_ALERT_STATE, _now, self._ALERT_TTL_S))
            _gap = _now - _ts.get(_key, 0.0)
            # 时间回拨时 _gap 为负，旧写法同样 < 窗口 → 最长静默 1.5h。
            if 0 <= _gap < self._ALERT_COOLDOWN_S:
                LOGGER.debug("notify suppressed (cooldown %ds): %s / %s",
                             self._ALERT_COOLDOWN_S, role, title)
                return
            _ts[_key] = _now
            _save_alert_ts(_ALERT_STATE, _ts, _now, self._ALERT_TTL_S)
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
            LOGGER.warning("notify send 异常（%ds 后重试）%s / %s: %s", self._ALERT_RETRY_S,
                           role, title, _e)
        # 冷却只在**真的叫醒之后**满额保留；投递失败退避 600s，不还回整窗。
        # 还回整窗的旧写法让每个 tick（10s）重投一次，而 API 报错/未登录的角色
        # 不会因为被多敲几次就恢复：实测 maintainer 30h 收 272 条同一告警、196 条
        # 落在错误态，同文案 20s 间隔重复 95 次。bus 记录已在上方落盘，不丢事。
        # WARNING 不能省：抑制路径只有 DEBUG、daemon 级别 INFO，否则失败期零可见信号。
        if not _delivered:
            LOGGER.warning("notify 未送达 %s / %s，%ds 后重试",
                           role, title, self._ALERT_RETRY_S)
            with _ALERT_LOCK:
                _ts[_key] = _now - self._ALERT_COOLDOWN_S + self._ALERT_RETRY_S
                _save_alert_ts(_ALERT_STATE, _ts, _now, self._ALERT_TTL_S, override=(_key,))

    def _sync_task_file(self, role: str, wf_id: str, step_id: str) -> None:
        """把 TASKS.md 重写成**刚投递成功**的那个任务。

        原实现只在发消息之前写，判到文件里是别的任务就只追加一行 [排队] 然后
        照样发 —— 角色手里出现两份互相矛盾的任务书（实测 pm 2026-10-22 05:33：
        TASKS.md 主块是旧任务 + [排队] 行，而 prompt 已经是新任务）。角色按 prompt
        干活，读这份文件的人和 launcher 的教训注入（roles.py:442）却看到旧任务。
        """
        try:
            rows = self._lifecycle.query(
                "SELECT title, description FROM tasks WHERE task_id="
                "(SELECT task_id FROM workflow_instances WHERE instance_id=?)", (wf_id,))
            if not rows:  # 孤儿实例（生产 1103 条里 1 条）：没有任务行就写不出任务书
                LOGGER.warning("TASKS.md 未同步 %s/%s：库里没有任务行", role, wf_id[:12])
                return
            t = rows[0]
            body = (f"# 工作流任务\n\n## 标题\n{t['title'] or '?'}\n\n"
                    f"## 描述\n{t['description'] or t['title'] or '?'}\n\n"
                    f"## 来源\n工作流 {wf_id} / 步骤 {step_id}\n")
            path = _CCS_WORKSPACES / role / "TASKS.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            old = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
            if old == body:
                return  # 同内容不重写：避免每 tick 刷新 mtime
            path.write_text(body, encoding="utf-8")
        # 这一步发生在 ccs send 成功之后：抛出去会让 engine.run_once 的整个
        # for row 循环中断（同一类"单点异常停整轮"）。sqlite3.Error = 并发写 WAL
        # 撞 locked；AttributeError = _lifecycle 懒加载拿不到库（engine.py:410）。
        # WARNING 不能降级：daemon 跑 INFO，DEBUG 等于零信号，而同步失败会让本步
        # 整个周期停在旧任务书（notified 已盖戳，不会重投）。
        except (ValueError, KeyError, TypeError, OSError, AttributeError,
                sqlite3.Error) as _e:
            LOGGER.warning("TASKS.md 同步失败 %s/%s: %s", role, wf_id[:12], _e)

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
        # （该方法的 False 路径只有两条：90s 空转超时、探测不可用提前返回）。
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
        # prompt 已校验完再查 CLI：非法 prompt 必须走 False/blocker，不能被"忙"掩盖。
        # 只看 CLI 忙态，不看 300s 活动窗——刚干完活的空闲角色会被活动窗判忙
        # （bus #395128 的同一根因）。ccs send 等忙 pane 单次 30s，整个 tick 会停。
        if wf_id and not force:
            from liveness import pane_cli_busy
            # force=True 不预检：timeout 重推/dual-node/integrity 提醒都是旁路，
            # 其中 integrity 在发送前就落了 bypass flag，预检返回 busy 会把提醒永久吞掉。
            try:
                _cli_busy = pane_cli_busy(role)
            except Exception as _pe:  # 探针异常不阻断投递（下一 tick 再试）
                LOGGER.warning("CLI 忙预检失败 %s: %s", role, _pe)
                _cli_busy = None
            if _cli_busy is True:
                LOGGER.info("send-to-role %s 跳过: CLI 仍在执行 (wf=%s step=%s)",
                            role, wf_id[:12], step_id)
                return "busy"
        # 相同 prompt 已在窗口内投递过 → 不重发，按排队返回（不盖 notified、不计失败）。
        # 放在占位符校验之后：非法 prompt 仍走 False（快速暴露），不会被"排队"掩盖。
        _fp = hash((role, wf_id, step_id, prompt))
        _prev = RoleLifecycleMixin._LAST_SEND.get((role, wf_id, step_id))
        if _prev and _prev[0] == _fp and time.time() - _prev[1] < self._SAME_PROMPT_WINDOW_S:
            # rc=0 只证明按键送到（见 _type_and_submit 文档），不证明 CLI 提交了：
            # 实测 2026-10-28 13:42:22 投递 s2 后 reviewer pane 有正文但 esc=0，
            # 转录 790s 无新轮，却被记成「已投递」挡住重投（bus #464561）。
            # 判据 = 投递后转录 mtime 是否越过投递前的水位线（转录是唯一能证伪
            # 「提交了」的信号，pane 在 alternate_on 下不可靠）。信号不可得
            # （无转录文件/旧条目无水位）时退回旧行为：宁可少打扰。
            _aged = time.time() - _prev[1]
            _wm = _prev[2]
            _now_mt = _transcript_mtime(role)
            _evidenced = (_wm is not None and _now_mt is not None and _now_mt > _wm)
            if _evidenced or _aged < self._SEND_EVIDENCE_GRACE_S or _wm is None or _now_mt is None:
                LOGGER.info("send-to-role %s 跳过: 同一 prompt 已在 %.0fs 内投递过 (wf=%s step=%s)",
                            role, _aged, wf_id[:12], step_id)
                return "busy"
            LOGGER.warning("send-to-role %s 重投: 上次 rc=0 但转录 %.0fs 未增长，无提交证据 "
                           "(wf=%s step=%s)", role, _aged, wf_id[:12], step_id)
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
            # 不写 bus：这是**预期路径**（我们主动丢弃噪音），不是事故。实测 24h
            # blocker 通道 208 条里 37 条（18%）全是它，而全机没有代码读 cat=blocker
            # —— 唯一效果是把告警通道稀释给读 bus 的角色。引擎日志已留同样信息。
            return False  # 投递未发生：明确语义，避免 engine 将 None 计为失败
        # TASKS.md 只由 _sync_task_file 在**投递成功后**写（见该函数）。
        # 原实现在这里先写一份、判到文件被别的任务占着就只追加 [排队] 再照样发，
        # 于是角色手里出现两份互相矛盾的任务书。
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
        # 投递前的水位线：ccs send 返回 0 后转录是否增长，用来区分「真提交」与
        # 「按键送到但没提交」。必须在 ccs send 之前取，之后取到的已经是新值。
        _wm_before = _transcript_mtime(role)
        try:
            _result = _engine_mod._sp.run(
                ["python3", str(_CCS_CLI), "send", role, prompt,
                 "--from", "workflow_engine"],
                # 30s（原 120s）。run_once 是单线程顺序循环：一次卡住的投递会拖住
                # 整轮，16 个活跃实例时实测 tick 间隔 >10 分钟（2026-10-22 03:00
                # journal 三次 product_architect 超时, 中间 11 分钟无任何日志），
                # 于是所有链都停在原地——单点投递延迟被放大成全局停摆。
                # 杀在半途不会留下叠字：下一次投递第一步就是 C-u 清行
                # （tmux_ops._deliver_once），旧内容先被清掉再写。
                capture_output=True, timeout=30,
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
        # 第三元是投递前转录 mtime（水位线），读取侧据此判断这次是不是真提交了。
        RoleLifecycleMixin._LAST_SEND[(role, wf_id, step_id)] = (_fp, time.time(), _wm_before)
        # TASKS.md 以**真正投递成功的那次**为准：上面那份写在发消息之前，判到
        # 别的任务占着文件就只追加一行 [排队] 照样发 —— 文件说"排队等前一个"、
        # prompt 说"现在做这个"，同一角色手里两份互相矛盾的任务书（实测 pm
        # 2026-10-22 05:33：主块是旧任务 + [排队] 行，prompt 已是新任务）。
        if wf_id:
            self._sync_task_file(role, wf_id, step_id)
        return True  # 投递成功（ccs send 正常退出）
