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

import fcntl
import json
import logging
import os
import signal
import subprocess
import sys
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
        LOGGER.info("收到信号 %s，开始优雅关闭...", s)
        shutdown = True

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    LOGGER.info("Workflow Daemon 启动 PID=%d, 间隔 %ds", os.getpid(), interval)

    idled = 0
    starve_rounds = 0
    try:
        while not shutdown:
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
            _LOCK_FILE.unlink(missing_ok=True)
        LOGGER.info("Workflow Daemon 已停止")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Daemon")
    parser.add_argument("--interval", type=int, default=INTERVAL, help="轮询间隔秒数")
    args = parser.parse_args()

    daemon_loop(args.interval)
