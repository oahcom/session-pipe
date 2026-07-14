"""paths.py — 集中管理路径常量（反模式 #10 修复）

所有路径在此定义，各模块从此导入，不再硬编码 Path.home()。
环境变量可覆盖，支持测试和部署环境切换。
"""
import os
from pathlib import Path

_HOME = Path.home()

# ── Hermes 脚本路径 ──
HERMES_SCRIPTS = Path(_HOME / ".hermes" / "scripts")
BUS_CLIENT = HERMES_SCRIPTS / "bus_client.py"
BUS_PROTOCOL = HERMES_SCRIPTS / "bus_protocol.py"

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
SESSION_LAUNCHER_SRC = Path(os.environ.get('SESSION_LAUNCHER_SRC', str(Path.home() / 'session-launcher' / 'src')))
SESSION_PIPELINE_SRC = Path(__file__).resolve().parent
SESSION_ROLES_ROOT = _HOME / "hermes-session-roles"

# ── Sister Bus ──
SISTER_BUS_CCS_SOCK = Path("/tmp/sister_bus_ccs.sock")
SISTER_BUS_FEED_SOCK = Path("/tmp/sister_bus_feed.sock")

# ── 工作空间 ──
CCS_WORKSPACES = _HOME / "ccs-workspaces"

# ── 哨兵目录 ──
CCS_SENTINEL_DIR = Path("/tmp/ccs-sentinels")
LIFECYCLE_SENTINEL_DIR = Path("/tmp/ccs-lifecycle-sentinels")
CODEX_SENTINEL_DIR = Path("/tmp/cdx-sentinels")

# ── 角色目录 ──
SESSION_ROLES_PERSONAS = SESSION_ROLES_ROOT / "personas" / "session-roles"

# ── Hermes 工作流 ──
HERMES_WORKFLOWS = _HOME / ".hermes" / "workflows"
HERMES_WORKFLOW_CHAINS = HERMES_WORKFLOWS / "chains"
