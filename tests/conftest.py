"""pytest conftest — 测试隔离配置。"""
import os
import sys
import tempfile

from pathlib import Path

# 确保 src/ 在 sys.path 最前面（各测试文件的 sys.path.insert 在模块顶层执行，
# 但 conftest 先于测试模块加载，此时 paths 等 src/ 模块尚不可 import）
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src in sys.path:
    sys.path.remove(_src)
sys.path.insert(0, _src)

# 0. 保存真实 bus_protocol/config_loader 引用（test_reliability_unit 会篡改
#    sys.modules 为 MagicMock，后续模块需恢复真实版本）
_real_bus_protocol = None
_real_config_loader = None
try:
    import bus_protocol as _real_bus_protocol
    import config_loader as _real_config_loader
except ImportError as e:
    import warnings
    warnings.warn(f"conftest: 无法预加载 bus_protocol/config_loader: {e}", stacklevel=1)


def pytest_runtest_setup(item):
    """每个测试用例前恢复真实 bus_protocol/config_loader 引用。

    test_reliability_unit 在模块顶层篡改 sys.modules 为 MagicMock 且不清理，
    会污染后续所有依赖真实 bus 的模块。当前模块正是 reliability_unit 时不恢复
    （它自己依赖替换），其他模块一律恢复真实版本。
    """
    mod = item.module.__name__ if item.module else ""
    if "reliability_unit" in mod:
        return
    if _real_bus_protocol is not None and sys.modules.get("bus_protocol") is not _real_bus_protocol:
        sys.modules["bus_protocol"] = _real_bus_protocol
    if _real_config_loader is not None and sys.modules.get("config_loader") is not _real_config_loader:
        sys.modules["config_loader"] = _real_config_loader


# collectstart 不恢复：它在收集 reliability_unit 后的其他模块时会恢复真实版本，
# 但此时 reliability_unit 的 _isolate_deps fixture 还没运行，
# 导致 workflow_engine 执行时 bus_protocol 是真实版本（fixture 生效前已被恢复）。
# 等 workflow_engine 执行完，_isolate_deps fixture 才运行并恢复 mock，但为时已晚。


# 1. 阻止 TTL pruner 在模块导入时自动启动（冲突 bus 连接）
os.environ.setdefault("SESSION_PIPELINE_SKIP_TTL_PRUNER", "1")

# 2. workflow DB 隔离：src/paths.py 读取该 env var，无参 WorkflowDB()/create_connection()
#    默认路径全部落到临时目录，不触碰生产 ~/.hermes/state/workflows.db
_TEST_STATE_DIR = tempfile.mkdtemp(prefix="pipeline-test-state-")
os.environ["SESSION_PIPELINE_WORKFLOWS_DB"] = os.path.join(_TEST_STATE_DIR, "workflows.db")

# 3. 确保 paths 模块被加载并覆盖 module-level 常量
#    （env var 在 paths.py import 时读取；若 paths 已在 sys.modules 缓存则 monkeypatch 补偿）
import paths as _paths_mod
_paths_mod.WORKFLOWS_DB = Path(os.environ["SESSION_PIPELINE_WORKFLOWS_DB"])

# 3b. CCS_WORKSPACES 隔离：_send_to_role 会写 <workspace>/<role>/TASKS.md。
#     不隔离则每个跑到的用例都写进生产 ~/ccs-workspaces/，实测已污染
#     scout/engineer 的真实 TASKS.md（2026-10-16 16:22，fixture 的 "test"
#     任务覆盖真实任务）。同 WORKFLOWS_DB 走 env + 属性重绑双保险。
_TEST_WS_DIR = os.path.join(_TEST_STATE_DIR, "ccs-workspaces")
os.environ["SESSION_PIPELINE_CCS_WORKSPACES"] = _TEST_WS_DIR
_paths_mod.CCS_WORKSPACES = Path(_TEST_WS_DIR)

# 4. BUS_DB 隔离：重定向 Blackboard 到临时 DB，测试不再写生产 blackboard.db。
#    Blackboard.__init__ 默认用 blackboard_legacy 模块导入时的 BLACKBOARD_DB 快照，
#    因此需 patch 快照值本身（blackboard_legacy.BLACKBOARD_DB + hermes_bus.config.BLACKBOARD_DB）。
_BLACKBOARD_DB_TEST = os.path.join(_TEST_STATE_DIR, "blackboard.db")
try:
    from hermes_bus import blackboard_legacy as _bb_legacy
    _bb_legacy.BLACKBOARD_DB = Path(_BLACKBOARD_DB_TEST)
    from hermes_bus import config as _bb_config
    _bb_config.BLACKBOARD_DB = Path(_BLACKBOARD_DB_TEST)
except Exception as _bbe:
    import warnings
    warnings.warn(f"conftest: bus DB 隔离失败（测试将写生产 bus）: {_bbe}", stacklevel=1)
