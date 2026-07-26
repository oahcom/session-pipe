"""pytest conftest — 测试隔离配置。"""
import os

# 1. 阻止 TTL pruner 在模块导入时自动启动（冲突 bus 连接）
os.environ.setdefault("SESSION_PIPELINE_SKIP_TTL_PRUNER", "1")

# 2. 直接覆写 hermes_bus.config.BLACKBOARD_DB（env var 方案无代码读取）
_HERMES_HOME = os.path.expanduser("~/.hermes")
_TEST_DB = os.path.join(_HERMES_HOME, "sister_bus", "test_blackboard.db")
import hermes_bus.config
hermes_bus.config.BLACKBOARD_DB = _TEST_DB

# 3. 确保测试用 DB 存在
from pathlib import Path
if not Path(_TEST_DB).exists():
    Path(_TEST_DB).parent.mkdir(parents=True, exist_ok=True)
    from bus_protocol import Blackboard
    bb = Blackboard(db_path=_TEST_DB)
    bb.write("notice", "test schema init", src="conftest")
    bb.mark_consumed(1, "conftest")
