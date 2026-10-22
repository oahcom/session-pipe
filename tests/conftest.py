"""pytest conftest — 测试隔离配置。"""
import os
import signal
import subprocess
import sys
import tempfile

from pathlib import Path

# 确保 src/ 在 sys.path 最前面（各测试文件的 sys.path.insert 在模块顶层执行，
# 但 conftest 先于测试模块加载，此时 paths 等 src/ 模块尚不可 import）
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src in sys.path:
    sys.path.remove(_src)
sys.path.insert(0, _src)

# 模块级即建连的路径（routing.RoutingDB / reliability.ACK_TRACKER /
# conversation_monitor 的 forbidden_ops 缓存）必须在任何 src 导入前改到 tmp：
# 实测 2026-10-22 未隔离时 pytest 会建表/写生产 routing.db、ack_tracker.db，
# 并把 1005B/13 角色的 forbidden_ops_cache.json 覆盖成 35B 空表。
_run_dir = tempfile.mkdtemp(prefix="pytest-hermes-run-")
# 直接赋值而非 setdefault：外部已 export 这三个变量时 setdefault 会让隔离静默失效
# （实测 2026-10-22 带着 HERMES_RUN_DIR 跑 pytest，forbidden_ops_cache.json 仍被
# 写到生产 ~/.hermes/run/）。
os.environ["HERMES_RUN_DIR"] = _run_dir
os.environ["ROUTING_DB_PATH"] = os.path.join(_run_dir, "routing.db")
os.environ["SESSION_PIPELINE_ACK_DB"] = os.path.join(_run_dir, "ack_tracker.db")
# CCS_WORKSPACES 的 env 必须在这里设，不能等第 3b 节：step_handler / wf_lifecycle 用
# `from paths import CCS_WORKSPACES` 拷了值（模块级 alias），谁先 import 谁固化。
# 本文件第 44 行就 import pipeflow 做 _PRISTINE 快照 → 晚设等于没设：测试写临时目录、
# 引擎读生产目录（实测 15 个用例假红，2026-10-22）。
_TEST_WS_DIR = os.path.join(_run_dir, "ccs-workspaces")
os.environ["SESSION_PIPELINE_CCS_WORKSPACES"] = _TEST_WS_DIR

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

# 被抓走的进程全局：导入期就抓原值，不能等第一个用例的 setup —— 模块级赋值
# （test_alert_cooldown_persist.py:43 就是）在**收集期**已发生，setup 抓到的就是桩。
_WATCHED_MODULES = ('routing_daemon', 'eval_consistency', 'eval_shared_loader', 'eval_checker')
try:
    from pipeflow import role_lifecycle as _pristine_rl
except Exception:  # 导入失败不阻断收集：还原逻辑对它做 None 判断
    _pristine_rl = None
_PRISTINE = {
    'run': subprocess.run,
    'engine_mod': getattr(_pristine_rl, '_engine_mod', None),
    'mods': {n: sys.modules[n] for n in _WATCHED_MODULES if n in sys.modules},
}



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
    _restore_process_globals()


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
# 哨兵/健康目录同样是生产路径，且各读各的 env（liveness.py:35-36、paths.py:61）：
# 不隔离时用例读的是 ~/.hermes/run/ccs-sentinels 里 12 个真哨兵与 ccs-health。
# 补丁一旦打错模块（见文末 _restore_process_globals），"没有哨兵"的断言会拿到真数据
# 而不是失效，红/绿取决于生产现场。
os.environ["CCS_HEALTH_DIR"] = os.path.join(_TEST_STATE_DIR, "ccs-health")
os.environ["CCS_SENTINEL_DIR"] = os.path.join(_TEST_STATE_DIR, "ccs-sentinels")

# 2b. BUS_DB 的 env 版隔离：contract_updater 等通过 `python3 bus_client.py write`
#     写 bus，子进程只认 HERMES_BLACKBOARD_DB，in-process 属性重绑管不到（实测
#     test_contract_updater_role_map 每次把生产 facts 行 ut 刷成当前时间）。
#     必须在 paths 导入前设，hermes_bus.config 在 import 时就读它。
_BLACKBOARD_DB_TEST = os.path.join(_TEST_STATE_DIR, "blackboard.db")
os.environ["HERMES_BLACKBOARD_DB"] = _BLACKBOARD_DB_TEST

# 3. 确保 paths 模块被加载并覆盖 module-level 常量
#    （env var 在 paths.py import 时读取；若 paths 已在 sys.modules 缓存则 monkeypatch 补偿）
import paths as _paths_mod
_paths_mod.WORKFLOWS_DB = Path(os.environ["SESSION_PIPELINE_WORKFLOWS_DB"])
_paths_mod.METRICS_JSONL = Path(os.environ["SESSION_PIPELINE_METRICS_JSONL"])

# 3d. 哨兵/健康目录：第 111 行的 env 只对**之后才 import** 的模块生效，而本文件第 50 行
#     就已 import pipeflow（连带 liveness/paths），快照早是生产路径。实测（2026-10-22）：
#     liveness.HEALTH_DIR / SENTINEL_DIR、paths.CCS_SENTINEL_DIR 全是 ~/.hermes/run/ccs-*，
#     与 env 里的 /tmp 不一致 —— 按值拷贝这几个常量的模块（drift_detector:17、
#     routing_daemon:31、survival_monitor:18）同样读生产哨兵，"没有哨兵"类断言
#     红/绿取决于机器上有没有角色在跑。故 env 之外必须重绑属性。
#     新模块按值拷贝这几个常量时，也要在此登记。
_ISO_HEALTH = os.path.join(_TEST_STATE_DIR, "ccs-health")
_ISO_SENTINEL = os.path.join(_TEST_STATE_DIR, "ccs-sentinels")
_liveness_mod = sys.modules.get("liveness") or __import__("liveness")
_liveness_mod.HEALTH_DIR = Path(_ISO_HEALTH)
_liveness_mod.SENTINEL_DIR = Path(_ISO_SENTINEL)
_paths_mod.CCS_SENTINEL_DIR = Path(_ISO_SENTINEL)
for _m in ("drift_detector", "routing_daemon"):
    _mod = sys.modules.get(_m)
    if _mod is not None:
        _mod.CCS_SENTINEL_DIR = Path(_ISO_SENTINEL)
_survival = sys.modules.get("routing.survival_monitor")
if _survival is not None:
    _survival.SENTINEL_DIR = Path(_ISO_SENTINEL)

# 3b. CCS_WORKSPACES 隔离：_send_to_role 会写 <workspace>/<role>/TASKS.md。
#     不隔离则每个跑到的用例都写进生产 ~/ccs-workspaces/，实测已污染
#     scout/engineer 的真实 TASKS.md（2026-10-16 16:22，fixture 的 "test"
#     任务覆盖真实任务）。同 WORKFLOWS_DB 走 env + 属性重绑双保险。
_paths_mod.CCS_WORKSPACES = Path(_TEST_WS_DIR)  # env 已在文件顶部设过（见那里的注释）

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
try:
    from hermes_bus import blackboard_legacy as _bb_legacy
    _bb_legacy.BLACKBOARD_DB = Path(_BLACKBOARD_DB_TEST)
    from hermes_bus import config as _bb_config
    _bb_config.BLACKBOARD_DB = Path(_BLACKBOARD_DB_TEST)
except Exception as _bbe:
    # 硬失败，不是 warn：隔离没起来时测试照样绿，会把夹具事实写进生产 bus
    # （2026-10-21 实测 3 条 WF-architect_review 链真实跑了 ~6h）。
    raise RuntimeError(
        f"conftest: bus DB 隔离失败，拒绝继续（否则测试将写生产 bus）: {_bbe}"
    ) from _bbe


# 4b. eval_checker 的转录根：trajectory 测试在模块顶层 `from eval_checker import
#     _TRANSCRIPTS_DIR` 就把值绑死了，fixture 阶段再 patch 已来不及（名字已绑定）。
#     必须在收集测试模块前改常量，否则 trajectory 测试在 ~/.claude/projects 下建
#     -probe-*/-tmp-probe-* 目录（实测一轮 12 个 CREATE+12 个 DELETE），并污染
#     转录统计分母。
_TRANSCRIPTS_ROOT = Path(tempfile.mkdtemp(prefix="pytest-claude-projects-"))
try:
    import eval_checker as _ec
    _ec._TRANSCRIPTS_DIR = _TRANSCRIPTS_ROOT
except Exception as _ece:
    import warnings
    warnings.warn(f"conftest: 转录根隔离失败（测试将写生产 ~/.claude/projects）: {_ece}", stacklevel=1)

# 5. 角色转录隔离：_recover_rate_limited_roles 现在以转录里的 API 错误为主判据
#    （pane 只有视口，实测 24h 1,342 条错误轮里认出 9 次）。不隔离时会读生产
#    转录：某个真实角色刚出错，就让"pane 干净 → 不发"这类断言随机失败。
import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_role_transcripts(tmp_path, monkeypatch):
    from pipeflow import role_lifecycle as _rl
    monkeypatch.setattr(_rl, "_TRANSCRIPT_ROOT", tmp_path)



@pytest.fixture(autouse=True)
def _isolate_probes_and_cooldowns(request, monkeypatch):
    """投递预检与告警冷却默认不跨用例共享。

    pane_cli_busy 走真实 tmux：服务端卡死时单条 capture-pane 实测 29.6s，
    未打桩的用例会把整轮测试拖死；默认返回 None（"探测不可用则放行"），
    需要忙态的用例自己 patch。两个进程级冷却表同理——不清则前一个用例
    写下的 (role,title)/(role,wf,step) 让后一个用例的告警静默。
    """
    import liveness
    from pipeflow.role_lifecycle import RoleLifecycleMixin
    # test_liveness 自己 mock subprocess.run 验底栏判据，打桩会把它测空
    if "test_liveness" not in (request.module.__name__ if request.module else ""):
        monkeypatch.setattr(liveness, "pane_cli_busy", lambda role, timeout=3.0: None)
    # tmux 快照是模块级缓存：不清则上一个用例的 mock 结果泄漏到下一个
    # （表现为"单跑绿、全量红"）。
    import liveness as _lv
    _lv._SNAP_TS, _lv._SNAP = 0.0, None
    RoleLifecycleMixin._LAST_SEND.clear()
    RoleLifecycleMixin._ALERT_TS.clear()
    # 另外三张进程级冷却/去重表此前没清：429 退避、compact 冷却、bus 去重
    # （role_lifecycle.py:266/281/292）。前一个用例写下的 (role) 条目会让后一个
    # 用例的恢复/去重分支静默跳过 —— 表现为"单跑绿、全量红"。
    for _d in (RoleLifecycleMixin._last_rate_recover, RoleLifecycleMixin._last_compact,
               RoleLifecycleMixin._last_bus_write):
        _d.clear()
    # 盘上那份也要清：退避/冷却状态是**跨进程派生数据**，生产路径没错，但用例共用
    # 同一个临时 HERMES_RUN_DIR，前一个用例写下的 (last, attempt) 会被本用例
    # in-loop 的 _load_rate_state 读回来 → "应发 1 次却 0 次"（实测
    # test_rate_recovery 整文件 4 红、逐条跑全绿）。
    try:
        (Path(os.environ["HERMES_RUN_DIR"]) / "alert_cooldowns.json").unlink()
    except OSError:
        pass

@pytest.fixture(autouse=True)
def _no_live_approval_notify(monkeypatch):
    """审批结果通知必须隔离：它起子进程调真实 `ccs send`。

    未隔离时任何 confirm_step(approved=True) 的用例都会把消息投给活体角色：
    2026-10-21 实测 13 条「工作流: wf_pull」通知进了 product_architect
    （测试夹具 id 直接出现在角色上下文里）。回归守卫见 test_no_live_notify.py。
    """
    from lifecycle import manager as _lm
    monkeypatch.setattr(_lm.LifecycleManager, "_notify_result", lambda *a, **k: None)


# 测巡检本身（游标/预算）的模块：它用 `del eng._ensure_guard_roles` 撤**实例**桩，
# 撤不掉 class 级桩，必须整模块豁免。
_REAL_GUARD_MODULES = {"test_tick_budget"}


@pytest.fixture(autouse=True)
def _no_guard_role_start(request, monkeypatch):
    """守护巡检默认不拉起真角色：run_once() 会走 _ensure_guard_roles → _ensure_role_alive
    → `ccs start`（60s 超时 + 30×1s 轮询）。测试里的角色名（test_role）不存在，
    于是一次 run_once 烧掉 90s+：全量套件卡在第 29 例（15 分钟 0 前进，进程 sleep）。
    真跑它的用例在 tests/test_role_alive_cache.py / test_ensure_role_alive_race.py，
    它们直接调 _ensure_role_alive，不经本函数，故不受影响。
    """
    if (request.module.__name__ if request.module else "").split(".")[-1] in _REAL_GUARD_MODULES:
        return
    from pipeflow import engine as _eng
    monkeypatch.setattr(_eng.WorkflowEngine, "_ensure_guard_roles", lambda self, *a, **k: set())

# 7. 进程级全局还原：三个用例原地改写进程全局且不还原，把后续用例带红。
#    - test_no_live_notify.py:38+60：@patch("pipeflow.engine._sp.run") 与
#      monkeypatch.setattr(subprocess, "run") 收尾顺序相反 —— 装饰器先 stop，
#      monkeypatch 后 undo，而 undo 写回的是它保存时的值（那个 MagicMock），
#      真实现永久丢失。实测之后 eval_checker._birthtime（:955）拿到 "claude"
#      → ValueError → fail-open → 预存文件不再判盲改（3 用例红）。
#    - test_alert_cooldown_persist.py:43 把 role_lifecycle._engine_mod 换成
#      SimpleNamespace(_sp.run → rc=7、无 stdout)：之后所有走 tmux 的用例
#      （_ensure_role_alive / _send_to_role / _recover_rate_limited_roles）
#      都拿到 rc=7 或 AttributeError（45 用例红）。
#    - test_daemon_eval_import_cache.py:16-17 弹掉 sys.modules 里的 routing_daemon/
#      eval_consistency 再重导：先收集的测试模块仍绑旧函数对象，而
#      patch("routing_daemon.SENTINEL_DIR") 打在新模块上 → 旧函数读生产哨兵目录
#      → "没有哨兵"用例恒 True（5 用例红）。
# 只还原"被用例借走的"三个句柄，不拦被测行为：三条断言面都不受影响。
# 还原放在**下一个用例的 setup**而不是上一个的 teardown：teardown 里写回会被
# monkeypatch.undo 再覆盖一次（undo 在 fixture 收尾后跑，写回它当初保存的 MagicMock），
# 泄漏照样成立；setup 侧还原与 fixture 收尾顺序无关。
def _pristine():
    return _PRISTINE


def _restore_process_globals() -> None:
    p = _pristine()
    if not p['mods']:
        # 首次还原时才抓模块快照（= 收集期结束、第一个用例开始前）。在 conftest
        # 导入时抓是恒空的：四个模块要等收集期由测试模块 import，实测 {}。
        # 空快照 → 还原循环永不触发 → 它声称修的 sys.modules 泄漏照旧。
        p['mods'] = {n: sys.modules[n] for n in _WATCHED_MODULES if n in sys.modules}
    if subprocess.run is not p['run']:
        subprocess.run = p['run']
    _rl = sys.modules.get('pipeflow.role_lifecycle')
    if (_rl is not None and p['engine_mod'] is not None
            and getattr(_rl, '_engine_mod', None) is not p['engine_mod']):
        _rl._engine_mod = p['engine_mod']
    for _name, _mod in p['mods'].items():
        if sys.modules.get(_name) is not _mod:
            sys.modules[_name] = _mod


# 需要真实 _wait_for_role_ready 的模块：它验的正是「ccs start 失败要可观测」，
# 桩掉就把被测对象测空了。
_REAL_READY_MODULES = {"test_role_start_error_text"}


@pytest.fixture(autouse=True)
def _fast_role_ready(request, monkeypatch):
    """派单前的就绪等待默认桩掉：真实现 = tmux has-session + `ccs start`(60s) + 10×1s 轮询，
    对不存在的 test_role 烧满 90s。`eng.start()` 第一跳就会走它 → 全量套件卡在
    test_chain_convergence（thread 栈：start → _write_step_prompt → _send_to_role →
    _wait_for_role_ready → sleep(5)）。已有 8 个用例各自 MagicMock 过它 → 共性缺口。
    """
    _mod = (request.module.__name__ if request.module else "").split(".")[-1]
    if _mod in _REAL_READY_MODULES:
        return
    from pipeflow import role_lifecycle as _rl
    monkeypatch.setattr(_rl.RoleLifecycleMixin, "_wait_for_role_ready",
                        lambda self, *a, **k: True)

# 直接被测的模块：桩掉就把对象测空了。
_REAL_ENSURE_ALIVE_MODULES = {
    "test_role_alive_cache",
    "test_ensure_role_alive_race",
    "test_pipeflow_error_paths",
}


@pytest.fixture(autouse=True)
def _no_live_role_restart(request, monkeypatch):
    """`_ensure_role_alive` 也会经 on_advance 回调触发：tmux 活着但没有 claude 进程时走
    `sleep(1)` + kill-session + 30×1s 重启循环——test_workcycle_e2e 因此超 20s 被自带
    `signal.alarm(20)` 打断（全量此前直接 EXIT=142，看不到这条红）。默认桩成 True。
    """
    if (request.module.__name__ if request.module else "").split(".")[-1] in _REAL_ENSURE_ALIVE_MODULES:
        return
    from pipeflow import role_lifecycle as _rl
    monkeypatch.setattr(_rl.RoleLifecycleMixin, "_ensure_role_alive",
                        lambda self, role, deadline=0.0: True)


def _alarm_handler(_signum, _frame):
    # 必须抛 BaseException 子类：TimeoutError 是 OSError→Exception 的子类，
    # 会被热路径的 except (TimeoutExpired, OSError) / except Exception 吞掉，
    # 一次性定时器已消耗 → 用例继续挂起，承诺落空。
    raise pytest.fail.Exception("用例超过自带 signal.alarm 预算（非产品缺陷，见 conftest）")


@pytest.fixture(autouse=True)
def _alarm_kills_test_not_process():
    """用例自带 `signal.alarm(N)` 却没装 handler：pytest 下默认动作 = 杀进程。

    实测全量跑到 ~94% 直接 EXIT=142（无 summary），三次复现；`grep -c signal.alarm`
    集中在 test_workcycle_e2e.py（17 处）。装一个抛 TimeoutError 的 handler，
    慢用例就变成**一条红用例**，而不是整轮蒸发——这是「全量永远拿不到结论」的第二个原因。
    """
    if not hasattr(signal, "SIGALRM"):
        yield
        return
    _old = signal.signal(signal.SIGALRM, _alarm_handler)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, _old)

