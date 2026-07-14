"""pytest conftest — 测试隔离配置。"""
import os

# 1. 阻止 TTL pruner 在模块导入时自动启动（冲突 bus 连接）
os.environ.setdefault("SESSION_PIPELINE_SKIP_TTL_PRUNER", "1")

# 2. 将 bus 的 Blackboard 默认路径替换为 temp DB（不污染生产 DB）
_HERMES_HOME = os.path.expanduser("~/.hermes")
os.environ.setdefault("BLACKBOARD_DB_PATH",
                       os.path.join(_HERMES_HOME, "sister_bus", "test_blackboard.db"))

# 3. 延迟 import，确保 env var 在 bus_protocol 模块加载前设置
from pathlib import Path
import sqlite3

def ensure_test_db():
    """确保测试 DB 存在（仅表结构，无数据）。"""
    db = os.environ["BLACKBOARD_DB_PATH"]
    if Path(db).exists():
        return
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    # 创建空表结构
    from bus_protocol import Blackboard
    bb = Blackboard(db_path=db)
    # Blackboard 的 __init__ 会执行 _init_schema，这里触发它
    bb.write("_schema_init", "test schema init", src="test")
    bb.mark_consumed(1, "test")
