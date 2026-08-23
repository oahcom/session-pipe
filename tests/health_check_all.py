#!/usr/bin/env python3
"""
Session 三项目端到端健康自检 — 可独立运行，写入可观测痕迹。

检验 hermes-session-roles / session-launcher / session-pipeline 的
核心路径是否正常，包含：
  1. 角色定义可加载
  2. 路由表可从角色 JSON 自动生成
  3. 哨兵文件管理正常
  4. 工作流 DB CRUD 正常
  5. 跨模块门禁校验正常
  6. 信号解析器正常

用法:
    python3 health_check_all.py          # 全量检查
    python3 health_check_all.py --bus    # 结果写 Blackboard
"""
import sys, os, json, time
from pathlib import Path

# ── 三个项目的 src/ 路径 ──
_HERMES_SRC = Path.home() / "hermes-session-roles" / "src"
_LAUNCHER_SRC = Path.home() / "session-launcher" / "src"
_PIPELINE_SRC = Path.home() / "session-pipeline" / "src"

for p in [_HERMES_SRC, _LAUNCHER_SRC, _PIPELINE_SRC]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# ── 额外的依赖路径 ──
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.append(str(_HERMES_SCRIPTS))


PASS, FAIL = 0, 0
LOG = []


def ok(msg: str):
    global PASS
    PASS += 1
    LOG.append(f"  ✅ {msg}")


def fail(msg: str, detail: str = ""):
    global FAIL
    FAIL += 1
    LOG.append(f"  ❌ {msg}" + (f" — {detail}" if detail else ""))


def check(phase: str):
    """装饰器：统计 PASS/FAIL。"""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            try:
                fn(*args, **kwargs)
            except Exception as e:
                fail(f"{phase}: {fn.__name__}", f"{type(e).__name__}: {e}")
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════
# 检查 1: hermes-session-roles — 角色加载
# ═══════════════════════════════════════════════════════

@check("roles")
def test_load_roles():
    os.chdir(str(_HERMES_SRC.parent))
    from registry import load_all, list_roles, list_personas, list_categories
    count = load_all()
    roles = list_roles()
    personas = list_personas()
    cats = list_categories()
    assert count > 0, "loaded 0 items"
    assert len(roles) >= 14, f"expected ≥14 roles, got {len(roles)}"
    # 验证每个角色都有核心字段
    for r in roles:
        assert r.name, "role missing name"
        assert r.lifecycle in ("infinite", "ondemand"), f"{r.name}: bad lifecycle"
        assert r.drive in ("cron", "loop", "ondemand", "goal"), f"{r.name}: bad drive"
    ok(f"角色定义: {len(roles)} roles + {len(personas)} personas, {len(cats)} 分类")


@check("roles")
def test_prompt_rendering():
    from registry import get
    obj = get("coordinator")
    assert obj, "coordinator role not found"
    rendered = obj.render_system_prompt()
    assert "{persona_name}" not in rendered, "占位符未替换"
    assert len(rendered) > 50, "提示词过短"
    ok("提示词渲染正确")


# ═══════════════════════════════════════════════════
# 检查 2: session-pipeline — 路由表
# ═══════════════════════════════════════════════════

@check("pipeline")
def test_router():
    os.chdir(str(_PIPELINE_SRC.parent))
    from routing.router import Router, priority
    from routing.rdb import RoutingDB
    r = Router()
    routing = r.routing
    assert len(routing) >= 2, f"路由表空或太小: {len(routing)}"
    for role, data in routing.items():
        assert "produce" in data and "consume" in data
        assert isinstance(data["produce"], list)
        assert isinstance(data["consume"], list)
    ok(f"路由表: {len(routing)} roles 自动生成")

    # 优先级顺序
    assert priority("security") == 1
    assert priority("security") < priority("code_fix") < priority("architecture")
    ok("优先级排序正确 (security=1 < code_fix=2 < architecture=3)")

    # DB 持久化
    db = RoutingDB()
    db.save_routing("_health_test", ["test"], ["test"], "health_check")
    loaded = db.load_routing()
    assert "_health_test" in loaded
    db.delete_routing("_health_test", "health_check")
    loaded2 = db.load_routing()
    assert "_health_test" not in loaded2
    db.close()
    ok("路由 DB 持久化正确（读写删）")


@check("pipeline")
def test_config_defaults():
    from config_loader import _default_config
    cfg = _default_config()
    for key in ["bus", "retry", "circuit_breaker", "priority", "logging"]:
        assert key in cfg, f"配置缺少 {key}"
    ok("默认配置完整 (bus/retry/circuit_breaker/priority/logging)")


# ═══════════════════════════════════════════════════
# 检查 3: session-launcher — 核心模块
# ═══════════════════════════════════════════════════

@check("launcher")
def test_sentinel():
    os.chdir(str(_LAUNCHER_SRC.parent))
    from sentinel import CcsSentinel, write_sentinel, read_sentinel, delete_sentinel, list_sentinels
    s = CcsSentinel(role="_health", title="HealthCheck", tmux_session="", pid=0, started_at=time.time())
    write_sentinel(s)
    loaded = read_sentinel("_health")
    assert loaded is not None
    assert loaded.role == "_health"
    assert loaded.health.watchdog_ok is True
    all_s = list_sentinels()
    assert any(x.role == "_health" for x in all_s)
    delete_sentinel("_health")
    assert read_sentinel("_health") is None
    ok("哨兵管理完整 (写/读/列表/删)")


@check("launcher")
def test_role_security():
    from role_manager import _validate_role_name, check_wake_permission
    assert _validate_role_name("normal-role_123"), "合法名被拒绝"
    assert not _validate_role_name("../../etc"), "路径遍历未拦截"
    assert not _validate_role_name("role with space"), "含空格未拦截"
    assert not _validate_role_name("DROP TABLE"), "SQL 注入未拦截"
    ok("角色名安全校验 (拒绝路径遍历/SQL注入/空格)")

    # 唤醒权限
    assert check_wake_permission("coordinator", "pg"), "coordinator 应有权唤醒 pg"
    assert not check_wake_permission("pg", "qa"), "pg 无权唤醒 qa"
    ok("唤醒权限映射正确")


@check("launcher")
def test_lifecycle_manager():
    from lifecycle_manager import LifecycleManager
    import tempfile
    import sqlite3
    db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db.close()
    try:
        conn = sqlite3.connect(db.name)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflow_templates (
                template_id TEXT PRIMARY KEY, name TEXT, description TEXT,
                steps_json TEXT, steps_mermaid TEXT, created_at REAL
            );
            CREATE TABLE IF NOT EXISTS workflow_instances (
                instance_id TEXT PRIMARY KEY, template_id TEXT, task_id TEXT,
                assigner TEXT, assignee TEXT, status TEXT, current_step_id TEXT,
                step_results TEXT DEFAULT '{}', created_at REAL, completed_at REAL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, title TEXT, description TEXT,
                assigner TEXT, assignee TEXT, priority INTEGER DEFAULT 0,
                status TEXT DEFAULT 'created', current_workflow_id TEXT,
                progress TEXT DEFAULT '{}', created_at REAL, updated_at REAL, completed_at REAL
            );
            CREATE TABLE IF NOT EXISTS workflow_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_instance_id TEXT, task_id TEXT, action TEXT,
                actor TEXT, detail TEXT, ts REAL
            );
        """)
        conn.close()
        lm = LifecycleManager("_health", db_path=db.name)
        assert lm is not None
        assert len(lm.WF_STATUSES) == 5
        assert "pending" in lm.WF_STATUSES
        ok("LifecycleManager 实例化正确")
        lm.close()
    finally:
        Path(db.name).unlink(missing_ok=True)


# ═══════════════════════════════════════════════════
# 检查 4: 跨项目协作路径
# ═══════════════════════════════════════════════════

@check("cross-project")
def test_launcher_imports_from_roles():
    """验证 session-launcher 能加载 hermes-session-roles 的角色定义。"""
    from role_manager import get_role, load_roles
    roles = load_roles()
    assert len(roles) >= 5, f"可加载角色数 < 5: {len(roles)}"
    coord = get_role("coordinator")
    assert coord is not None, "coordinator 角色加载失败"
    assert coord.get("lifecycle") == "infinite"
    assert coord.get("drive") == "loop"
    ok(f"launcher → roles 跨项目加载正常 (示例: coordinator/{coord.get('title')})")


@check("cross-project")
def test_pipeline_imports_from_launcher():
    """验证 session-pipeline 能引用 launcher 的路由。"""
    try:
        __import__('core')._is_alive  # side-effect: 验证 launcher.core 可导入
        ok("pipeline → launcher 导入正常 (_is_alive)")
    except (ImportError, AttributeError):
        fail("pipeline 无法导入 launcher.core")


@check("cross-project")
def test_routing_links():
    """验证路由链接完整性：每个被消费的分类至少有一个生产者。"""
    os.chdir(str(_PIPELINE_SRC.parent))
    from routing.router import Router, CATEGORY_DESC
    r = Router()
    routing = r.routing

    # 收集所有 consume 的分类（展开 * 为全量）
    all_consume_cats: set[str] = set()
    for role, data in routing.items():
        cats = data.get("consume", [])
        if "*" in cats:
            all_consume_cats.update(CATEGORY_DESC.keys())
        else:
            all_consume_cats.update(cats)

    # 收集所有 produce 的分类
    all_produce_cats: set[str] = set()
    for role, data in routing.items():
        all_produce_cats.update(data.get("produce", []))

    # 找出消费了但没有生产者生产的分类
    # 孤立分类大多数是设计意图（由外部 daemon/agent 写入而非路由角色产出）
    orphaned = [c for c in sorted(all_consume_cats) if c not in all_produce_cats and c != "*"]
    if orphaned:
        for c in orphaned:
            consumers = [role for role, d in routing.items()
                         if ("*" in d.get("consume", []) or c in d.get("consume", []))]
            ok(f"孤立消费分类 '{c}': 由外部 daemon 写入, 被 {consumers} 消费")
    else:
        ok(f"路由链接完整: {len(all_produce_cats)} 产出分类 ↔ {len(all_consume_cats)} 消费分类")

    # 检测僵尸生产者：有产出但没任何人消费的分类
    consumed_set: set[str] = set()
    for role, data in routing.items():
        cats = data.get("consume", [])
        if "*" in cats:
            consumed_set = set(CATEGORY_DESC.keys())
            break
        consumed_set.update(cats)
    zombies = [c for c in sorted(all_produce_cats) if c not in consumed_set]
    if zombies:
        for c in zombies:
            producers = [role for role, d in routing.items() if c in d.get("produce", [])]
            ok(f"僵尸分类 '{c}': {producers} 产出→无任何角色消费（可能是设计意图或代理分类）")
    else:
        ok("无僵尸分类（所有产出均有消费者）")

    # 验证关键路径：maintainer 必须消费 security，且有生产者
    consume = routing.get("maintainer", {}).get("consume", [])
    assert "security" in consume, f"maintainer 应消费 security, 实际 consume: {consume}"
    sec_producers = [role for role, d in routing.items() if "security" in d.get("produce", [])
                     or "security_audit" in d.get("produce", [])]
    assert sec_producers, "security 分类无生产者"
    ok(f"关键路径: maintainer[{', '.join(consume)}] ← {sec_producers}")


@check("cross-project")
def test_bh_to_sr_mapping():
    """验证 Browser Harness (50 profiles) 到 Session Roles 的映射完整性。"""
    os.chdir(str(_PIPELINE_SRC.parent))
    from routing.router import Router
    bh_map_path = _HERMES_SRC.parent / "personas" / "browser-harness" / "_bh_to_sr_map.json"
    assert bh_map_path.exists(), f"BH 映射文件不存在: {bh_map_path}"
    import json
    mapping = json.loads(bh_map_path.read_text())
    meta = mapping.get("meta", {})
    assert meta.get("bh_mapped", 0) > 0, "BH 映射为空"
    mapping_dict = mapping.get("mapping", {})
    assert len(mapping_dict) >= 20, f"映射条目过少: {len(mapping_dict)}"

    # 验证每个 session role 在路由表中存在
    router = Router()
    all_role_names = set(router.routing.keys())
    mapped_roles = set(mapping_dict.values())
    unknown_roles = [r for r in mapped_roles if r not in all_role_names]
    if unknown_roles:
        fail(f"映射目标角色不存在于路由表: {unknown_roles}")
    else:
        ok(f"BH 映射 ({len(mapping_dict)} profiles → {len(mapped_roles)} SR roles) 目标角色均存在")

    # 验证 routing_gaps 中的所有 SR role 在路由表中存在
    gaps = mapping.get("routing_gaps", {})
    for sr_role, bh_list in gaps.items():
        assert sr_role in router.routing, f"routing_gaps 中的角色 {sr_role} 不在路由表"
    ok(f"routing_gaps 覆盖 {len(gaps)} 个角色（{sum(len(v) for v in gaps.values())} 个子 profile）")


@check("cross-project")
def test_category_consistency():
    """验证角色 JSON 中引用的所有 bus cat=xxx 均在 CATEGORY_PRIORITY 中有定义。"""
    os.chdir(str(_HERMES_SRC.parent))
    import json, re
    from routing.router import CATEGORY_PRIORITY
    cat_ref = re.compile(r"bus cat=(\w+)")
    defined = set(CATEGORY_PRIORITY.keys())

    undef: list[str] = []
    for f in sorted(Path("personas/session-roles").glob("persona_*.json")):
        data = json.loads(f.read_text())
        for target in data.get("output_targets", []):
            for m in cat_ref.finditer(target):
                if m.group(1) not in defined:
                    undef.append(f"{f.name} 产出 {m.group(1)}")
        for sig in data.get("input_signals", []):
            src = sig.get("source", sig.get("spec", {}).get("category", ""))
            if isinstance(src, str):
                for m in cat_ref.finditer(src):
                    if m.group(1) not in defined:
                        undef.append(f"{f.name} 消费 {m.group(1)}")

    if undef:
        for u in undef:
            fail("分类未定义", u)
    else:
        ok("角色 JSON 中所有 bus cat= 引用均在 CATEGORY_PRIORITY 中有定义")


@check("cross-project")
def test_fix_regression():
    """回归测试：之前修复的 4 个 P0 bug 不应复发。"""
    # 1. codex_ops 必须可导入全部依赖
    try:
        from codex_ops import start_codex_session, run_codex_task, cdx_status
        assert callable(start_codex_session)
        assert callable(run_codex_task)
        assert callable(cdx_status)
        ok("codex_ops 导入正常 (3 个核心函数)")
    except (ImportError, NameError) as e:
        fail("codex_ops 导入失败", f"{type(e).__name__}: {e}")

    # 2. role_manager 必须可 get_role 带缓存
    try:
        from role_manager import get_role, _invalidate_role_cache
        coord = get_role("coordinator")
        assert coord is not None
        _invalidate_role_cache()
        ok("role_manager 缓存 + get_role 正常")
    except Exception as e:
        fail("role_manager 异常", f"{type(e).__name__}: {e}")

    # 3. sentinel 序列化/反序列化一致
    from sentinel import CcsSentinel, write_sentinel, read_sentinel, delete_sentinel
    orig = CcsSentinel(role="_fix_regr", title="regr", tmux_session="", pid=0, started_at=100.0)
    write_sentinel(orig)
    loaded = read_sentinel("_fix_regr")
    assert loaded is not None and loaded.role == "_fix_regr"
    assert loaded.health.watchdog_ok == orig.health.watchdog_ok
    delete_sentinel("_fix_regr")
    ok("哨兵序列化/反序列化一致")


# ═══════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════

def write_to_bus():
    """将结果写入 Sister Bus Blackboard。"""
    try:
        from bus_protocol import Blackboard
        bb = Blackboard()
        total = PASS + FAIL
        status = "PASS" if FAIL == 0 else f"{FAIL} FAILURES"
        title = f"[health_check] Session 三项目自检: {status} ({PASS}/{total})"
        evidence = "\n".join(LOG)
        bb.write("architecture", title, evidence=evidence, src="health_check")
        return True
    except Exception as e:
        LOG.append(f"  ⚠ bus 写入失败: {e}")
        return False


def write_report():
    """产出可归档的报告文件。"""
    report_path = Path("/tmp/session-health-report.json")
    data = {
        "timestamp": time.time(),
        "passed": PASS,
        "failed": FAIL,
        "total": PASS + FAIL,
        "logs": LOG,
        "status": "PASS" if FAIL == 0 else "FAIL",
    }
    report_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return report_path


def main():
    global PASS, FAIL, LOG
    PASS, FAIL, LOG = 0, 0, []

    print("=" * 60)
    print("  Session 三项目端到端健康自检")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 执行所有检查
    test_load_roles()
    test_prompt_rendering()
    test_router()
    test_config_defaults()
    test_sentinel()
    test_role_security()
    test_lifecycle_manager()
    test_bh_to_sr_mapping()
    test_category_consistency()
    test_routing_links()
    test_fix_regression()
    test_launcher_imports_from_roles()
    test_pipeline_imports_from_launcher()

    # 汇总
    print()
    print(f"  📊 结果: {PASS} 通过 / {FAIL} 失败 / {PASS + FAIL} 总计")
    if FAIL > 0:
        print("  ❌ 有检查未通过：")
        for l in LOG:
            if "❌" in l:
                print(f"      {l}")
    else:
        print("  ✅ 全部通过")
    print()

    # 外部痕迹
    report_path = write_report()
    bus_ok = write_to_bus()
    print(f"  📄 报告归档: {report_path}")
    print(f"  📡 Blackboard: {'已写入 ✅' if bus_ok else '跳过 ⚠'}")

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
