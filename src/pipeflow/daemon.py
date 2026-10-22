#!/usr/bin/env python3
"""
Workflow Daemon — 驱动工作流引擎，主动推进工作流步骤。

使用 WorkflowEngine 处理 running/pending 工作流：
- pending -> 自动启动 + push prompt
- running -> 检测 exit_condition + 超时升级 + 提醒心跳

用法：
  python3 pipeflow/daemon.py --interval 30

sys.path 由 paths.py 集中管理，本文件不修改 sys.path。
"""

import faulthandler
import fcntl
import json
import logging
import os
import signal
import subprocess
import sys

# 不写新的 .pyc。WHY: 与源文件落在同一秒的 .pyc 会让 Python 的 (mtime, size)
# 失效校验误判命中，加载**过期字节码** —— 实测 WorkflowLifecycleMixin 因此丢失
# _handle_wf_loop 等 10 个方法（AST 显示类完整 12 方法，hasattr 却为 False），
# 引擎静默跑在不同版本上。不写新字节码可消除这个同秒窗口。
# 必须放在其它项目模块导入之前才生效。
sys.dont_write_bytecode = True
import threading
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
LOGGER = logging.getLogger("pipeline.daemon")

# 自愈 sys.path: 从文件位置推导 src/ 路径，支持从任意目录直接执行
_src = Path(__file__).resolve().parent.parent
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from paths import ensure_paths
ensure_paths()

from pipeflow.engine import WorkflowEngine

INTERVAL = 10  # 秒

# tick 卡死取证：超过 _TICK_STACK_DUMP_SEC 自动打印全部线程栈，超过 _TICK_SLOW_SEC 记一条耗时日志。
# 用环境变量可调。负值/非法值 → 禁用（daemon 绝不因配置值自身而无法启动）；0 对 TICK_SLOW_SEC 有效。
def _parse_tick_sec(name, default):
    try:
        _v = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return -1
    return max(_v, -1)


# 源码热加载最小间隔（秒）：实测 2026-10-22 03:46-04:26 的 40 分钟内 execv 8 次
# （多 session 同时改 src/**），每次重执行都丢掉当前 tick 并重跑启动流程。
# 不是禁用热加载（分支上的修复就永远不生效），而是限速：最多 1 次 / 5 分钟。
_RELOAD_MIN_INTERVAL_SEC = 300


def _reload_decision(now_mtime, seen_mtime, pending_mtime, now_ts, last_reload_ts,
                     interval=_RELOAD_MIN_INTERVAL_SEC):
    """返回 (pending_mtime, 是否立即重执行)。

    pending 永远收下新 mtime —— 限速期看到的那次更新不能当"已应用"丢掉，
    否则此后无人再改代码时 daemon 永久跑旧代码（codex P1）。
    """
    pending = max(pending_mtime, now_mtime)
    return pending, pending > seen_mtime and (now_ts - last_reload_ts) >= interval
_TICK_STACK_DUMP_SEC = _parse_tick_sec("TICK_STACK_DUMP_SEC", 180)
_TICK_SLOW_SEC = _parse_tick_sec("TICK_SLOW_SEC", 60)

# ── 饥饿检测：工作流长期空转 → 触发食物链重启生产 ──
# 设计背景：引擎只推进已有工作流，不主动创建；当所有工作流终态且超时未重启，
# 写 bus 触发 scout 生产新情报，让食物链重新转起来。
# 冷却机制（三层，2026-08 事故后确立）：
#   成功冷却（6h）：防止高频骚扰——触发成功后 6h 内不再触发
#   失败短退避（60s）：防止模板缺失时无限重试刷日志
#   失败长退避（24h）：连续失败≥5次后切换，退避期内不再尝试（需人工介入）
# 竞态防护：daemon 饥饿触发与 wf_loop 循环可能同时创建 food_chain_loop，
#   触发前用 has_active_template 检查 DB 是否已有同模板活跃实例（防双写）
_STARVE_ROUNDS = 36          # 连续 36 轮（=6 分钟）无活跃工作流
_STARVE_COOLDOWN = 6 * 3600  # 触发后 6 小时冷却，防止高频骚扰
from paths import BUS_CLIENT, CCS_CLI, STARVE_STATE

_BUS_SCRIPT = BUS_CLIENT
_STARVE_STATE = STARVE_STATE


def _safe_write_state(state: dict) -> None:
    """原子写状态文件（临时文件 + rename），避免并发写半截 JSON。"""
    try:
        _STARVE_STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STARVE_STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(_STARVE_STATE)
    except OSError as e:
        LOGGER.warning("starve 状态写入失败: %s", e)


def _active_count(eng: WorkflowEngine) -> int:
    """当前活跃工作流数（pending/running/step_done_ready）。"""
    return eng.active_workflow_count()


_STARVE_FAIL_BACKOFF = 24 * 3600  # 连续失败 ≥5 次后退避 24 小时
_STARVE_FAIL_RETRY_INTERVAL = 60  # 未达阈值：失败后短退避 60s 重试窗口
_STARVE_FAIL_THRESHOLD = 5        # 连续失败阈值，达到后切换退避策略


# 优雅关闭的硬上限：超过就 os._exit，把「关不掉」变成「最多等这么久」。
# ponytail: 不看事务边界，粗暴退出依赖 SQLite WAL 回滚；要更干净得让 run_once
# 在循环里轮询 shutdown 标志（改动面大得多）。
_SHUTDOWN_GRACE_SEC = 10


def _force_exit_after(sec: float) -> None:
    time.sleep(sec)
    LOGGER.warning("优雅关闭超时 %.0fs，强制退出", sec)
    logging.shutdown()
    os._exit(0)


def _starve_signal(eng: WorkflowEngine) -> None:
    """连续饥饿 → 拉起 scout session + 创建 food_chain_loop 工作流，冷却期内静默。"""
    now = time.time()
    state = {}
    try:
        if _STARVE_STATE.exists():
            state = json.loads(_STARVE_STATE.read_text())
    except (OSError, ValueError):
        pass

    # ── 冷却检查 ──
    # 成功触发冷却: 基于 last_fire（6h 防高频骚扰）
    # 失败退避: 连续失败≥阈值 → 24h 长退避；未达阈值 → 短退避 60s 重试窗口
    #（失败计数必须能递增到阈值——若失败也走 6h 冷却则计数永远停在 1，退避永不生效）
    last_fire = state.get("last_fire", 0)
    last_failure_ts = state.get("last_failure_ts", 0)
    consec_fails = state.get("consecutive_failures", 0)
    if consec_fails >= _STARVE_FAIL_THRESHOLD:
        # 达到阈值 + 最近失败在 24h 内 → 退避（不创建）
        # 失败时间戳过旧（迁移前脏数据/首次触碰）→ 允许创建一次以重置计数
        if last_failure_ts > 0 and now - last_failure_ts < _STARVE_FAIL_BACKOFF:
            return  # 24h 内有失败记录 → 退避
        # 退避期已过 → 重置计数重新尝试。缺此重置则失败计数永久 ≥ 阈值，
        # 每次退避到期都只是再退避一次，饥饿恢复路径永不重试
        # （实测 2026-10-17：consecutive_failures 卡在 5，last_active_ts 停在 8-08）。
        consec_fails = 0
    else:
        if now - last_fire < _STARVE_COOLDOWN:
            return  # 成功冷却期内
        if now - last_failure_ts < _STARVE_FAIL_RETRY_INTERVAL:
            return  # 失败后短退避（重试窗口内静默）

    # 0) 防双路径竞态：wf_loop 可能在 daemon 触发同时创建了新的 food_chain_loop
    # 仅真实 WorkflowEngine（类型含 has_active_template）才检查；mock → 跳过
    if "has_active_template" in type(eng).__dict__:
        try:
            if eng.has_active_template("food_chain_loop"):
                LOGGER.info("饥饿触发跳过: food_chain_loop 已有活跃实例")
                return
        except Exception as e:
            # 竞态检查失败 → 保守跳过（防双实例；失败场景由退避机制兜底）
            LOGGER.warning("饥饿竞态检查异常，保守跳过: %s", e)
            return

    # 1) 确保首步角色（scout）session 已启动
    eng._ensure_role_alive("scout")
    # reviewer 作为审批方需常驻以接收 code_review/architecture 消息（bus #336293 缺口修复）
    eng._ensure_role_alive("reviewer")

    # 2) 创建食物链工作流（确定性触发，不依赖 coordinator 转发）
    try:
        wf_id = eng.start("food_chain_loop", context={
            "source": "starve_detector",
            "reason": "连续6分钟无活跃工作流",
        })
    except Exception as e:
        LOGGER.warning("饥饿创建工作流失败 (连续第 %d 次): %s", consec_fails + 1, e)
        state["consecutive_failures"] = consec_fails + 1
        state["last_failure_ts"] = now
        _safe_write_state(state)
        return
    # 类型守卫：mock/非 str 返回值（测试环境污染）禁止写入 bus 与状态文件
    if not isinstance(wf_id, str) or not wf_id.startswith("wf_"):
        LOGGER.error("饥饿触发返回值异常，丢弃: %r", wf_id)
        state["consecutive_failures"] = consec_fails + 1
        state["last_failure_ts"] = now
        _safe_write_state(state)
        return
    LOGGER.info("饥饿触发: food_chain_loop 已创建 wf=%s", wf_id)

    # 3) 写 bus 供监控审计（非依赖）
    try:
        subprocess.run(
            [sys.executable, str(_BUS_SCRIPT), "write", "scheduler",
             f"starve-restart:食物链已重启 wf={wf_id}",
             "--src", "workflow_engine",
             "--evidence",
             "工作流引擎饥饿检测：连续 6 分钟无活跃工作流，已创建 food_chain_loop"],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass

    state["last_fire"] = now
    state["last_wf"] = wf_id
    # last_active_ts 曾被遗留为 69 天前的陈旧值（无任何写入点），会被读成
    # "引擎长期停摆"。这里显式维护为本次触发时刻。
    state["last_active_ts"] = now
    state.pop("consecutive_failures", None)  # 成功 → 清零失败计数
    state.pop("last_failure_ts", None)
    _safe_write_state(state)


_TMUX_ALARM_AT = 0.0


def _tmux_wedged(timeout: float = 6.0) -> bool:
    """tmux 服务端是否失去响应。健康态实测 2~60ms，卡死态单命令 12~50s。

    WHY 只认超时、不认返回码：会话/目标不存在同样 rc!=0，与卡死无关。
    """
    try:
        subprocess.run(["tmux", "display-message", "-p", "ok"],
                       capture_output=True, timeout=timeout)
        return False
    except subprocess.TimeoutExpired:
        return True
    except (FileNotFoundError, OSError):
        return False  # 没有 tmux 不算卡死，可能是别的 tmux 实例/权限问题


def _tmux_ok(timeout: float = 6.0) -> bool:
    """服务端能应答且 rc==0 = 真的活。超时/无二进制/rc!=0 一律算否。"""
    try:
        return subprocess.run(["tmux", "display-message", "-p", "ok"],
                              capture_output=True, timeout=timeout).returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def _tmux_stuck_utempter(min_age: int = 30) -> list:
    """卡住的 utempter 进程（tmux 借 libutempter 写 /var/run/utmp 的 setuid 助手）。

    tmux 链接 libutempter，新 pane 建会话时它 fork 出 utempter 写 utmp 记录；
    助手卡住时单线程 server 停在 waitpid，整个事件循环停摆——所有 tmux 命令
    排队 12~50s（实测 2026-10-22 03:44~04:05：助手存活 273s 未退出，
    server /proc/<pid>/wchan = do_wait）。

    min_age 是安全阀：正常助手活不过几十毫秒，只杀「活了 30s 还在」的，
    避免误杀并发登录/另一个 tmux server 的健康助手（codex review P2）。
    """
    try:
        r = subprocess.run(["pgrep", "-x", "utempter", "--older", str(min_age)],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return []
    return [int(x) for x in r.stdout.split() if x.isdigit()]


def _tmux_recover() -> bool:
    """最小代价自愈：杀掉卡住的 utempter 再复探。恢复成功返回 True。

    只损失一条 utmp 记账，pane 与会话毫发无损——比重启 server（丢 17 个
    角色会话上下文）代价低两个数量级。

    恢复必须由**正向应答**证明（rc==0）：not _tmux_wedged() 会把「没有 tmux
    二进制/服务端已死（rc!=0）」也算成恢复，从而吞掉该升级的 blocker
    （codex review P1）。
    """
    for _pid in _tmux_stuck_utempter():
        try:
            os.kill(_pid, signal.SIGKILL)
            LOGGER.warning("tmux 服务端阻塞：杀掉卡住的 utempter pid=%d", _pid)
        except OSError:
            pass
    return _tmux_ok()


def _tmux_wedge_alarm() -> None:
    """tick 变慢时探一次 tmux 服务端；卡死先自愈，自愈失败才升级（10 分钟一条）。

    WHY 单独探：tick 变慢不指向 tmux，日志里只有"tick 耗时 399s"看不出原因。
    2026-10-22 实测：has-session 13~15s、list-sessions 18s、单 tick 399s，
    5 条链卡在 step_done_ready 1.5~2.4h，2 条链被僵尸回收误杀。
    """
    global _TMUX_ALARM_AT
    _now = time.time()
    if _now - _TMUX_ALARM_AT < 600:
        return
    _TMUX_ALARM_AT = _now  # 探过就记时，避免慢 tick 期每轮付一次探测超时
    if not _tmux_wedged():
        return
    if _tmux_recover():
        LOGGER.warning("tmux 服务端已自愈（清掉卡住的 utempter），角色投递恢复")
        return
    LOGGER.error("tmux 服务端失去响应：角色投递全部阻塞，工作流全链停摆")
    try:
        subprocess.run(
            [sys.executable, str(_BUS_SCRIPT), "write", "blocker",
             "tmux 服务端卡死：所有角色投递阻塞，工作流全链停摆",
             "--src", "workflow_engine",
             "--evidence",
             "tmux display-message 超时 6s（健康态 2~60ms），角色 pane 读写全堵；"
             "已先试自愈（kill 卡住的 utempter）无效。终极恢复：pkill -9 -f "
             "'tmux: server'，再对每个角色执行 "
             "python3 ~/session-launcher/src/ccs.py start <role> --no-attach"],
            capture_output=True, timeout=10)
    except Exception:
        pass


_LOCK_FILE = Path.home() / ".hermes" / "run" / "workflow-daemon.lock"
_LOCK_FD = None


def _acquire_flock() -> bool:
    """fcntl.flock 互斥锁，防止双进程竞争（不依赖 PID，兼容 WSL2）。"""
    global _LOCK_FD
    _LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = _LOCK_FILE.open("w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fd.write(str(os.getpid()))
        fd.flush()
        _LOCK_FD = fd
        return True
    except (IOError, OSError):
        fd.close()
        old_pid = _LOCK_FILE.read_text().strip() if _LOCK_FILE.exists() else "?"
        LOGGER.warning("已有实例在运行 (lock held by pid=%s)", old_pid)
        return False


def daemon_loop(interval: int):
    """守护进程主循环：驱动 engine.run_once 推进所有工作流。"""
    if not _acquire_flock():
        sys.exit(1)

    eng = WorkflowEngine()
    shutdown = False

    def _handler(s, f):
        nonlocal shutdown
        LOGGER.info("收到信号 %s，开始优雅关闭（最多等 %ds）...", s, _SHUTDOWN_GRACE_SEC)
        shutdown = True
        # 关闭只是置位：正在跑的那次 run_once 得自己走到 while 条件才看得到。
        # 单次 tick 实测 190–317s（_scan_tasks 热点），systemd 会一直停在
        # deactivating —— 2026-10-22 实测两次、最长 >4min，TimeoutStopSec=15 未生效。
        # 到点强制退出，Restart=always 用新代码拉起。SQLite 走 WAL，未提交事务由
        # journal 回滚，不会留半写状态。
        threading.Thread(target=_force_exit_after, args=(_SHUTDOWN_GRACE_SEC,),
                         daemon=True).start()

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    LOGGER.info("Workflow Daemon 启动 PID=%d, 间隔 %ds", os.getpid(), interval)

    idled = 0
    starve_rounds = 0
    # ── 源码热加载守卫 ──
    # 本进程启动后永不重读源文件，分支上的修复对运行中的 daemon 完全不生效。
    # 实测 2026-10-19：判活守卫 _role_producing_recently 已写在工作区，daemon
    # 仍按旧代码误杀正在写报告的 reviewer。判据 = 源码 mtime 晚于本进程启动。
    # 处理：优雅退出，由 systemd Restart=always 用新代码拉起。
    # 连续两次探测到同一新 mtime 才退出 —— 编辑器保存瞬时态不触发重启。
    _src_dir = Path(__file__).resolve().parent

    # 整个 src/ 树：engine 会 import src/task_evidence.py、workflow/*、lifecycle/*、
    # routing/* 等，只 glob pipeflow/*.py 时改这些文件不触发重执行 → 修复写着但
    # 跑的是旧代码（R126 实测 task_evidence.py 改完 20min 仍是旧行为）。
    # 清单启动时取一次，之后只 stat（新增文件要等下次重执行才被跟踪）。
    _src_files = list(_src_dir.parent.rglob("*.py"))

    def _src_mtime() -> float:
        # 清单是启动时快照：文件被删/改名/软链断掉后 stat 会抛 OSError。裸用会把每轮
        # 探测变成必炸（daemon 起不来，热加载彻底失效），所以逐个兜住、缺的当 0。
        # 不用 exists() 先判：那本身也是 stat，且 exists→stat 之间有 TOCTOU 窗口。
        def _mt(_p):
            try:
                return _p.stat().st_mtime
            except OSError:
                return 0.0

        return max((_mt(_p) for _p in _src_files), default=0.0)

    _seen_mtime = _src_mtime()
    try:
        _last_reload = float(os.environ.get("_WF_LAST_RELOAD", "0") or 0)
    except ValueError:
        _last_reload = 0.0
    _pending_mtime = 0.0  # 已看到但还没应用（受 _RELOAD_MIN_INTERVAL_SEC 限速）
    _logged_pending = 0.0  # 只为抑制重复日志：排队期每 tick 都会探到同一个 mtime


    try:
        while not shutdown:
            # 整轮看门狗：覆盖 run_once + 饥饿检测 + 源码扫描，不含 time.sleep。
            # 一次 tick 卡住时进程无日志、无子进程、CPU 近零（实测 2026-10-22 02:45→03:03
            # 单 tick 静默 18 分钟，无 root 拿不到栈）→ 到点让 Python 自己把栈打到 stderr。
            _tick_t0 = time.monotonic()
            if _TICK_STACK_DUMP_SEC > 0:
                faulthandler.dump_traceback_later(_TICK_STACK_DUMP_SEC, repeat=False)
            # 两层 try 是刻意的：内层各自兜异常（一处失败不耽误同轮其它分支），
            # 外层 finally 只管看门狗收尾与整轮计时，不重复处理异常。
            try:
                try:
                    eng.run_once()
                    idled = 0
                except Exception as e:
                    LOGGER.error("run_once 异常: %s", e, exc_info=True)
                    idled += 1
                    if idled % 10 == 0:
                        LOGGER.error("连续失败 %d 轮，最后异常: %s", idled, e)
                # 饥饿检测：连续空转无活跃工作流 → 写 bus 请求生产者重启食物链
                try:
                    if _active_count(eng) > 0:
                        starve_rounds = 0
                    else:
                        starve_rounds += 1
                        if starve_rounds >= _STARVE_ROUNDS:
                            _starve_signal(eng)
                            starve_rounds = 0
                except Exception as e:
                    LOGGER.warning("饥饿检测异常: %s", e)
                # 每轮都探。旧写法按"轮次"计数（6 轮一探），而忙机上 run_once 一轮要
                # 几分钟 → 实测 18:18:43 改 daemon.py、18:26 仍不重启，源码修复不生效。
                try:
                    # 只把"看到的新 mtime"记进 _pending_mtime，冷却期过后**下一轮再判**。
                    # 不能在这里直接推进 _seen_mtime：限速期丢掉的那次更新会永久丢失，
                    # 若此后没人再改代码，daemon 会一直跑旧代码（codex P1）。
                    _now_ts = time.time()
                    _pending_mtime, _do_reload = _reload_decision(
                        _src_mtime(), _seen_mtime, _pending_mtime, _now_ts, _last_reload)
                    if _pending_mtime > _seen_mtime:
                        if not _do_reload:
                            # 同一 mtime 只记一次：限速期内每 tick 都探到它，逐 tick 打印
                            # 实测灌出 ~30 行/5min 的纯噪音，把真正的信号埋掉。
                            if _pending_mtime != _logged_pending:
                                _logged_pending = _pending_mtime
                                LOGGER.info(
                                    "源码已更新，距上次重执行 %.0fs < %ds → 排队等待",
                                    _now_ts - _last_reload, _RELOAD_MIN_INTERVAL_SEC)
                        else:
                            time.sleep(2)  # 等编辑器落盘，半截保存不触发重启
                            if _src_mtime() == _pending_mtime:
                                # 就地重执行，不退出：退出依赖外部拉起（systemd Restart /
                                # 角色自愈脚本），而这两者会互相抢锁——实测手工实例占住锁后
                                # systemd 永远 activating，引擎反而没人管。execv 只换内存镜像，
                                # 锁随新进程重新获取，任何监管方式下都能生效。
                                LOGGER.info("源码已更新 (mtime=%.0f)，就地重执行以加载新代码",
                                            _pending_mtime)
                                os.environ["_WF_LAST_RELOAD"] = str(_now_ts)
                                logging.shutdown()
                                os.execv(sys.executable, [sys.executable] + sys.argv)
                except OSError as _e:
                    LOGGER.debug("源码 mtime 探测失败: %s", _e)
            finally:
                if _TICK_STACK_DUMP_SEC > 0:
                    faulthandler.cancel_dump_traceback_later()
                _tick_dt = time.monotonic() - _tick_t0
                # _TICK_SLOW_SEC 负值 = 禁用（非法/负值统一映射到 -1）；0 仍有效，
                # 强制每次 tick 都记一条（自检测试与排查用）。
                if 0 <= _TICK_SLOW_SEC < _tick_dt:
                    LOGGER.warning("tick 耗时 %.0fs（阈值 %ds）", _tick_dt, _TICK_SLOW_SEC)
                    _tmux_wedge_alarm()
            time.sleep(interval)
    except KeyboardInterrupt:
        LOGGER.info("收到 KeyboardInterrupt，正在关闭...")
    except Exception as e:
        LOGGER.error("Daemon 主循环异常: %s", e, exc_info=True)
    finally:
        LOGGER.info("正在关闭 Workflow Daemon...")
        if _LOCK_FD is not None:
            fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
            _LOCK_FD.close()
            # 不 unlink：删文件会让新实例锁到**新 inode**，与还在退出的旧实例
            # 同时持锁（实测 18:17:25 系统实例 + 18:17:36 手工实例并存 10s 以上）。
            # 留文件不碍事，flock 随进程消失即释放。
        LOGGER.info("Workflow Daemon 已停止")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()

    daemon_loop(args.interval)
