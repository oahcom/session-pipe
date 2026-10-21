"""pytest conftest — 测试隔离配置。"""
import os
import signal
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


def pytest_runtest_teardown(item, nextitem):
    """用例结束必拆 SIGALRM 定时器。

    多个用例用 signal.alarm(20) 做自超时，却只在 __main__ 分支装 handler；
    pytest 下 handler 是默认动作（杀进程）。不拆则定时器跨用例残留，等到
    后面某个稍慢的用例（如含真实 sleep 的 _ensure_role_alive）时引爆，
    整个 pytest 进程以 EXIT=142 死掉且不打印任何失败——实测 34% 处。
    """
    if hasattr(signal, "alarm"):
        signal.alarm(0)


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
# 指标文件同样隔离：task_generator 每轮 heal 都 append，不隔离则 pytest 的
# total=1 夹具行会插进 daemon 的 total=1042 序列，生产文件末行去重失效
# （实测一轮全量测试后 60 行里 48 行是测试产物）。
os.environ["SESSION_PIPELINE_METRICS_JSONL"] = os.path.join(_TEST_STATE_DIR, "workflow-metrics.jsonl")

# 3. 确保 paths 模块被加载并覆盖 module-level 常量
#    （env var 在 paths.py import 时读取；若 paths 已在 sys.modules 缓存则 monkeypatch 补偿）
import paths as _paths_mod
_paths_mod.WORKFLOWS_DB = Path(os.environ["SESSION_PIPELINE_WORKFLOWS_DB"])
_paths_mod.METRICS_JSONL = Path(os.environ["SESSION_PIPELINE_METRICS_JSONL"])

# 3b. CCS_WORKSPACES 隔离：_send_to_role 会写 <workspace>/<role>/TASKS.md。
#     不隔离则每个跑到的用例都写进生产 ~/ccs-workspaces/，实测已污染
#     scout/engineer 的真实 TASKS.md（2026-10-16 16:22，fixture 的 "test"
#     任务覆盖真实任务）。同 WORKFLOWS_DB 走 env + 属性重绑双保险。
_TEST_WS_DIR = os.path.join(_TEST_STATE_DIR, "ccs-workspaces")
os.environ["SESSION_PIPELINE_CCS_WORKSPACES"] = _TEST_WS_DIR
_paths_mod.CCS_WORKSPACES = Path(_TEST_WS_DIR)

# 3c. ITERATION_WORKSPACES 隔离：_write_step_prompt 读 <根>/<project_name>/PRD.md
#     生成 {workspace_summary}。不隔离则跑 pytest 会在生产 ~/.hermes/workspace/
#     下建 test_ws_project 等目录并写 PRD.md（2026-10-16 实测残留
#     ws_prj2/ws_prj3/test_ws_project）。
_TEST_ITER_DIR = os.path.join(_TEST_STATE_DIR, "iteration-workspace")
os.environ["SESSION_PIPELINE_ITERATION_WORKSPACES"] = _TEST_ITER_DIR
_paths_mod.ITERATION_WORKSPACES = Path(_TEST_ITER_DIR)

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


# 5. 角色转录隔离：_recover_rate_limited_roles 现在以转录里的 API 错误为主判据
#    （pane 只有视口，实测 24h 1,342 条错误轮里认出 9 次）。不隔离时会读生产
#    转录：某个真实角色刚出错，就让"pane 干净 → 不发"这类断言随机失败。
import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_role_transcripts(tmp_path, monkeypatch):
    from pipeflow import role_lifecycle as _rl
    monkeypatch.setattr(_rl, "_TRANSCRIPT_ROOT", tmp_path)

