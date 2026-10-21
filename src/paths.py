#!/usr/bin/env python3
"""paths.py — 集中管理路径常量

所有路径从 hermes_bus.config 统一导入，消除重复定义。
"""
import os
import sys
from pathlib import Path

_HOME = Path.home()

# ── 从 hermes_bus.config 统一导入 ──
# hermes_bus 是 ~/.hermes/scripts 下的包，正常靠 miniforge3 的
# `__editable__.hermes_core.pth` 可见。但角色的 pane PATH 有时落到
# /usr/bin/python3(3.12)——那里没有该 .pth，import 直接 ModuleNotFoundError，
# `wf confirm` 委托 pipeline 审批随之失败（实测 #414311）。显式注入
# scripts 目录，使本模块不再依赖解释器安装状态。
_HERMES_SCRIPTS = str(_HOME / ".hermes" / "scripts")
if _HERMES_SCRIPTS not in sys.path:
    sys.path.append(_HERMES_SCRIPTS)
from hermes_bus.config import BUS_CLIENT  # re-exported: p0_exemption.py / workflow/client.py 从本模块导入

__all__ = ["BUS_CLIENT"]
# ── 路径常量：由本项目自行管理，不依赖 bus config ──
_SESSION_LAUNCHER_SRC = Path(_HOME / "session-launcher" / "src")
SESSION_ROLES_ROOT = Path(_HOME / "hermes-session-roles")
# env var 覆盖: 测试隔离用（同 WORKFLOWS_DB）。不隔离时测试写 TASKS.md 会
# 覆盖生产 workspace —— 实测 pytest fixture 的 "test" 任务污染
# scout/engineer 的真实 TASKS.md（2026-10-16 16:22）。
CCS_WORKSPACES = Path(os.environ.get("SESSION_PIPELINE_CCS_WORKSPACES",
                                     _HOME / "ccs-workspaces"))

# ── 数据目录 ──
HERMES_STATE = _HOME / ".hermes" / "state"
# env var 覆盖: 测试隔离用（conftest 设置 SESSION_PIPELINE_WORKFLOWS_DB 指向临时 DB）
WORKFLOWS_DB = Path(os.environ.get("SESSION_PIPELINE_WORKFLOWS_DB", HERMES_STATE / "workflows.db"))
# env var 覆盖: 测试隔离用。task_generator 的指标落盘走这里；不隔离时跑 pytest
# 会把 total=1 的夹具状态写进生产 workflow-metrics.jsonl，与 daemon 的
# total=1042 交替出现，末行去重因此失效（实测 48/60 行是测试产物）。
METRICS_JSONL = Path(os.environ.get("SESSION_PIPELINE_METRICS_JSONL",
                                    HERMES_STATE / "workflow-metrics.jsonl"))
CURSOR_DB = HERMES_STATE / "pipeline_cursor.db"
ROUTING_DB = HERMES_STATE / "routing.db"
ACK_TRACKER_DB = HERMES_STATE / "ack_tracker.db"
STARVE_STATE = Path.home() / ".hermes" / "run" / "wf_starve.json"
# 迭代需求目录（每个需求一个子目录，存 PRD/DESIGN/TASKS 等迭代产物）。
# env var 覆盖: 测试隔离用。不隔离时跑 pytest 会在生产根下建 test_ws_project
# 等目录并写 PRD.md（2026-10-16 实测残留 ws_prj2/ws_prj3/test_ws_project）。
ITERATION_WORKSPACES = Path(os.environ.get(
    "SESSION_PIPELINE_ITERATION_WORKSPACES",
    _HOME / ".hermes" / "workspace"))
# ── 模板与配置 ──
HERMES_TEMPLATES = _HOME / ".hermes" / "templates"
HERMES_BACKUPS = _HOME / ".hermes" / "backups"
WORKFLOW_GUIDE = HERMES_TEMPLATES / "WORKFLOW_GUIDE.md"

# ── 项目根目录 ──
HERMES_SCRIPTS = Path(_HOME / ".hermes" / "scripts")
SESSION_LAUNCHER_SRC = Path(_SESSION_LAUNCHER_SRC)
SESSION_PIPELINE_SRC = Path(__file__).resolve().parent

# ── CCS CLI（统一入口，消除 4 处硬编码） ──
CCS_CLI = SESSION_LAUNCHER_SRC / "ccs.py"

# ── 哨兵目录 ──
# CCS_SENTINEL_DIR 可经环境变量重定向：手工跑 survival tick 等只读验证时，
# 哨兵目录是唯一仍指向生产的口子（HEALTH_DIR / HERMES_BLACKBOARD_DB 均已可重定向），
# 重定向它才能在不动生产的前提下做端到端验证（2026-10-16 实测）。
CCS_SENTINEL_DIR = Path(os.environ.get(
    "CCS_SENTINEL_DIR", str(Path.home() / ".hermes" / "run" / "ccs-sentinels")))
LIFECYCLE_SENTINEL_DIR = Path.home() / ".hermes" / "run" / "ccs-lifecycle-sentinels"
CODEX_SENTINEL_DIR = Path.home() / ".hermes" / "run" / "cdx-sentinels"

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
    # 必须无条件置顶：若仅做 not-in 检查，test 已插 pipeline 时会跳过，launcher 会跑到最前
    _psrc = str(SESSION_PIPELINE_SRC.resolve())
    sys.path.insert(0, _psrc)
    # 清除 launcher 缓存并预加载 pipeline 版本。
    # 若已加载的 lifecycle 本就来自 pipeline（SESSION_PIPELINE_SRC 前缀），跳过重导入，
    # 避免重复 import 副作用；否则才 del 并重导入。
    import importlib
    if "lifecycle" in sys.modules:
        _lf_file = getattr(sys.modules["lifecycle"], "__file__", "") or ""
        if _lf_file.startswith(str(SESSION_PIPELINE_SRC)):
            return
        del sys.modules["lifecycle"]
    try:
        importlib.import_module("lifecycle")
    except Exception as _e:
        print(f"[paths] lifecycle 预加载失败: {_e}")
