#!/usr/bin/env python3
"""paths.py — 集中管理路径常量

所有路径从 hermes_bus.config 统一导入，消除重复定义。
"""
import os
import sys
from pathlib import Path

_HOME = Path.home()

# ── 从 hermes_bus.config 统一导入 ──
from hermes_bus.config import (
    BUS_CLIENT, BUS_PROTOCOL,
    SISTER_BUS_CCS_SOCK, SISTER_BUS_FEED_SOCK,
    SISTER_BUS_DKK_SOCK, SISTER_BUS_SSK_SOCK,
    SESSION_LAUNCHER_SRC as _SESSION_LAUNCHER_SRC,
    SESSION_ROLES_ROOT,
    CCS_WORKSPACES,
)

# ── 数据目录 ──
HERMES_STATE = _HOME / ".hermes" / "state"
WORKFLOWS_DB = HERMES_STATE / "workflows.db"
CURSOR_DB = HERMES_STATE / "pipeline_cursor.db"
ROUTING_DB = HERMES_STATE / "routing.db"
ACK_TRACKER_DB = HERMES_STATE / "ack_tracker.db"
COMPOSITE_RUNS_DB = HERMES_STATE / "composite_runs.db"

# ── 模板与配置 ──
HERMES_TEMPLATES = _HOME / ".hermes" / "templates"
HERMES_BACKUPS = _HOME / ".hermes" / "backups"
WORKFLOW_GUIDE = HERMES_TEMPLATES / "WORKFLOW_GUIDE.md"

# ── 项目根目录 ──
HERMES_SCRIPTS = Path(_HOME / ".hermes" / "scripts")
SESSION_LAUNCHER_SRC = Path(_SESSION_LAUNCHER_SRC)
SESSION_PIPELINE_SRC = Path(__file__).resolve().parent

# ── 哨兵目录 ──
CCS_SENTINEL_DIR = Path("/tmp/ccs-sentinels")
LIFECYCLE_SENTINEL_DIR = Path("/tmp/ccs-lifecycle-sentinels")
CODEX_SENTINEL_DIR = Path("/tmp/cdx-sentinels")

# ── 角色目录 ──
SESSION_ROLES_PERSONAS = SESSION_ROLES_ROOT / "personas" / "session-roles"

# ── Hermes 工作流 ──
HERMES_WORKFLOWS = _HOME / ".hermes" / "workflows"
HERMES_WORKFLOW_CHAINS = HERMES_WORKFLOWS / "chains"


def ensure_paths() -> None:
    """统一注册跨项目 sys.path。"""
    _entries = [
        SESSION_PIPELINE_SRC.resolve(),
        SESSION_LAUNCHER_SRC.resolve(),
        Path(HERMES_SCRIPTS).resolve(),
    ]
    for p in reversed(_entries):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)
    # session-pipeline 的 lifecycle 包必须优先于 session-launcher（其 manager.py 已删除）
    # 加父目录（src），这样 import lifecycle 能找到 lifecycle/__init__.py
    _psrc = str(SESSION_PIPELINE_SRC.resolve())
    sys.path.insert(0, _psrc)
    # 如果 launcher 的 lifecycle 已被缓存，清除缓存
    if "lifecycle" in sys.modules:
        del sys.modules["lifecycle"]
