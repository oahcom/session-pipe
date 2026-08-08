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
# 引擎只推进已有工作流，不主动创建；当所有工作流终态且超时未重启
# 时，写 bus 触发 scout 生产新情报，让食物链重新转起来。
_STARVE_ROUNDS = 36          # 连续 36 轮（=6 分钟）无活跃工作流
_STARVE_COOLDOWN = 6 * 3600  # 触发后 6 小时冷却，防止高频骚扰
_ACTIVE_STATUSES = ("pending", "running", "step_done_ready")
_BUS_SCRIPT = Path.home() / ".hermes" / "scripts" / "bus_client.py"
_STARVE_STATE = Path.home() / ".hermes" / "run" / "wf_starve.json"
_CCS_PY = Path.home() / "session-launcher" / "src" / "ccs.py"


def _ensure_role_alive(role: str) -> bool:
    """通过 ccs.py start 拉起未运行的角色 session（幂等，已运行则跳过）。"""
    try:
        result = subprocess.run(
            [sys.executable, str(_CCS_PY), "start", role,
             "--no-attach", "--no-auto-send"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            LOGGER.info("_ensure_role_alive: %s 已就绪", role)
            return True
        LOGGER.warning("_ensure_role_alive %s 失败: %s", role, result.stderr[:200])
        return False
    except Exception as e:
        LOGGER.warning("_ensure_role_alive %s 异常: %s", role, e)
        return False


def _active_count(eng: WorkflowEngine) -> int:
    """当前活跃工作流数（pending/running/step_done_ready）。"""
    try:
        rows = eng._lifecycle.query(
            f"SELECT COUNT(*) AS cnt FROM workflow_instances "
            f"WHERE status IN {_ACTIVE_STATUSES!r}"
        )
        return rows[0]["cnt"] if rows else 0
    except Exception as e:
        LOGGER.warning("活跃工作流查询失败: %s", e)
        return 0


def _starve_signal(eng: WorkflowEngine) -> None:
    """连续饥饿 → 拉起 scout session + 创建 food_chain_loop 工作流，冷却期内静默。"""
    now = time.time()
    state = {}
    try:
        if _STARVE_STATE.exists():
            state = json.loads(_STARVE_STATE.read_text())
    except (OSError, ValueError):
        pass

    last_fire = state.get("last_fire", 0)
    if now - last_fire < _STARVE_COOLDOWN:
        return  # 冷却期内不重复骚扰

    # 1) 确保首步角色（scout）session 已启动
    _ensure_role_alive("scout")

    # 2) 创建食物链工作流（确定性触发，不依赖 coordinator 转发）
    try:
        wf_id = eng.start("food_chain_loop", context={
            "source": "starve_detector",
            "reason": "连续6分钟无活跃工作流",
        })
        LOGGER.info("饥饿触发: food_chain_loop 已创建 wf=%s", wf_id)
    except Exception as e:
        LOGGER.warning("饥饿创建工作流失败: %s", e)
        return

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
    _STARVE_STATE.write_text(json.dumps(state))


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
        nonlocal shutdown; shutdown = True

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
    finally:
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
