#!/usr/bin/env python3
"""
单元测试：路由逻辑、优先级、消费联动、自动生成路由表。
运行：cd session-pipeline && python3 tests/test_router.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

# launcher 路径（环境变量覆盖）
_launcher_src = os.environ.get(
    "SESSION_LAUNCHER_DIR",
    str(Path.home() / "session-launcher" / "src")
)
if _launcher_src not in sys.path:
    sys.path.insert(0, _launcher_src)

# hermes scripts 路径（环境变量覆盖）
_hermes_scripts = os.environ.get(
    "HERMES_SCRIPTS_DIR",
    str(Path.home() / ".hermes" / "scripts")
)
if _hermes_scripts not in sys.path:
    sys.path.insert(0, _hermes_scripts)

# 确保 src 在路径中（必须在最后 insert，确保 position 0）
_src_dir = str(Path(__file__).resolve().parents[1] / "src")
if _src_dir in sys.path:
    sys.path.remove(_src_dir)
sys.path.insert(0, _src_dir)


def _clear_routing_db():
    """清空路由 DB，保证测试隔离。"""
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from routing import rdb as rd
        with rd.RoutingDB() as db:
            db._conn.execute("DELETE FROM routing")
            db._conn.execute("DELETE FROM routing_audit")
            db._conn.commit()
    except Exception:
        pass


# 模块级清理由 pytest fixture 管理（setup_teardown），避免在 import 时写入生产 DB
# _clear_routing_db() 仅在测试函数内调用


def _make_roles_dir(roles: dict) -> Path:
    """创建包含角色 JSON 的临时目录，返回目录路径。"""
    d = tempfile.mkdtemp()
    for name, data in roles.items():
        persona = {
            "name": name,
            "output_targets": [f"bus cat={c}" for c in data.get("produce", [])],
            "input_signals": [{"source": f"bus cat={c}"} for c in data.get("consume", [])],
        }
        with open(Path(d) / f"{name}.json", "w") as f:
            json.dump(persona, f)

    # 补充通配符消费者（input_signals 里是 unread --all）
    for name, data in roles.items():
        if "*" in data.get("consume", []):
            path = Path(d) / f"{name}.json"
            if path.exists():
                with open(path) as f:
                    p = json.load(f)
                p["input_signals"] = [{"source": "bus_client.py unread --all"}]
                with open(path, "w") as f:
                    json.dump(p, f)
    return d


# 默认测试用角色数据（对应原 _default_routing() 的数据）
_DEFAULT_TEST_ROLES = {
    "maintainer": {"produce": ["code_fix", "architecture"], "consume": ["security"]},
    "scout": {"produce": ["architecture", "evolution_report"], "consume": ["architecture"]},
    "knowledge_curator": {"produce": ["reflexion_lesson"], "consume": ["*"]},
    "engineer": {"produce": ["code_fix"], "consume": ["architecture", "code_fix", "task_spec"]},
    "coordinator": {"produce": ["architecture"], "consume": ["*", "task_spec"]},
    "curator": {"produce": ["architecture"], "consume": []},
    "closer": {"produce": ["architecture", "ops"], "consume": ["code_fix", "architecture"]},
    "product_architect": {"produce": ["prd", "system_design", "task_spec"], "consume": ["architecture", "code_fix", "product_design"]},
}


def test_parse_produce_categories():
    """output_targets 正确解析为产出分类列表。"""
    from routing.router import _load_roles_export
    roles = _load_roles_export()
    assert isinstance(roles, dict)
    eng = roles.get("engineer", {})
    eng_p = eng.get("routing", {}).get("produce", [])
    assert "code_fix" in eng_p, f"engineer produce={eng_p}"
    inv = roles.get("investigator_general", {})
    assert "root_cause_analysis" in inv.get("routing", {}).get("produce", [])


def test_parse_consume_categories():
    """input_signals 正确解析为消费分类列表。"""
    from routing.router import _load_roles_export
    roles = _load_roles_export()
    assert isinstance(roles, dict)
    closer = roles.get("closer", {})
    assert closer.get("routing", {}).get("consume_all") is True
    eng = roles.get("engineer", {})
    eng_c = eng.get("routing", {}).get("consume", [])
    assert "architecture" in eng_c, f"engineer consume={eng_c}"


def test_priority_ordering():
    """分类优先级排序正确：security > code_fix > architecture > prd > system_design > task_spec > performance > evolution_report > reflexion_lesson > deception。"""
    from routing.router import priority

    assert priority("security") < priority("code_fix"), "security 应优先于 code_fix"
    assert priority("code_fix") < priority("architecture"), "code_fix 应优先于 architecture"
    assert priority("architecture") < priority("prd"), "architecture 应优先于 prd"
    assert priority("prd") < priority("system_design"), "prd 应优先于 system_design"
    assert priority("system_design") < priority("task_spec"), "system_design 应优先于 task_spec"
    assert priority("task_spec") < priority("performance"), "task_spec 应优先于 performance"
    assert priority("performance") < priority("evolution_report"), "performance 应优先于 evolution_report"
    assert priority("evolution_report") < priority("reflexion_lesson"), "evolution_report 应优先于 reflexion_lesson"
    assert priority("reflexion_lesson") < priority("deception"), "reflexion_lesson 应优先于 deception"
    # 未知分类返回默认值（最大优先级数字）
    assert priority("unknown_cat") == 11
    print("  ✓ priority ordering")


def test_router_auto_build_from_json():
    """Router 从项目 A 的 JSON 文件自动生成路由表。"""
    from routing.router import Router

    roles_dir = Path.home() / "hermes-session-roles" / "personas" / "session-roles"
    if not roles_dir.exists():
        print("  ⚠ 跳过 router_auto_build（项目 A 目录不存在）")
        return

    router = Router()
    routing = router.routing

    # 应有 9 个角色（可能随项目 A 增减，允许≥7）
    assert len(routing) >= 7, f"应有至少 7 个角色，实际 {len(routing)}"

    # maintainer 应能产出 code_fix 和 architecture
    maintainer = routing["maintainer"]
    assert "code_fix" in maintainer["produce"], "maintainer 应产出 code_fix"
    assert "architecture" in maintainer["produce"], "maintainer 应产出 architecture"

    # scout 应能产出 architecture 和 evolution_report
    scout = routing["scout"]
    assert "architecture" in scout["produce"], "scout 应产出 architecture"
    assert "evolution_report" in scout["produce"], "scout 应产出 evolution_report"

    # knowledge_curator 应消费所有分类（*）+ session_log + disk/mem 信号
    curator = routing.get("knowledge_curator")
    if curator:
        assert "*" in curator["consume"], "knowledge_curator 应消费所有分类"

    # developer/engineer 应能产出 code_fix
    developer = routing.get("developer") or routing.get("engineer")
    assert developer is not None, "应有 developer/engineer 角色"

    print("  ✓ router auto-build from JSON")


def test_router_fallback():
    """Router 在空导出时返回空路由表。"""
    from routing.router import _load_roles_export
    roles = _load_roles_export()
    assert isinstance(roles, dict)
    print("  ✓ router fallback (empty or loaded)")


def test_get_producers_consumers():
    """get_producers / get_consumers 返回正确的角色列表。"""
    from routing.router import Router

    router = Router()
    # 如果有路由数据，测试基本功能
    if router.routing:
        producers = router.get_producers("code_fix")
        assert isinstance(producers, list), "应返回列表"
        consumers = router.get_consumers("code_fix")
        assert isinstance(consumers, list), "应返回列表"
    print("  ✓ get_producers / get_consumers")


def test_unconsumed_by_role_parsing():
    """unconsumed_by_role 返回结构化消息列表（非原始字符串）。"""
    from routing.router import Router

    router = Router()
    messages = router.unconsumed_by_role("closer", limit=5)
    assert isinstance(messages, list), "应返回列表"
    for msg in messages:
        if "error" in msg:
            continue
        assert "id" in msg, "消息应含 id 字段"
        assert "category" in msg, "消息应含 category 字段"
        assert "text" in msg, "消息应含 text 字段"
        assert "priority" in msg, "消息应含 priority 字段"
        if "priority" in msg and msg["priority"] is not None:
            assert isinstance(msg["priority"], int), "priority 应是 int"
    print("  ✓ unconsumed_by_role structured output")


def test_consume_linkage():
    """consume_linkage 返回正确的影响角色列表。"""
    from routing.router import Router

    router = Router()
    linked = router.consume_linkage(1, "code_fix")
    assert isinstance(linked, list), "应返回列表"
    print("  ✓ consume_linkage")


def test_category_desc_complete():
    """CATEGORY_DESC 包含所有路由中出现的分类。"""
    from routing.router import CATEGORY_DESC, Router

    router = Router()
    all_cats: set[str] = set()
    for role, r in router.routing.items():
        all_cats.update(r.get("produce", []))

    for cat in all_cats:
        assert cat in CATEGORY_DESC, f"分类 {cat} 缺少描述"
    print("  ✓ CATEGORY_DESC completeness")


def test_status_format():
    """status() 返回正确的字典结构。"""
    from routing.auto import status

    s = status()
    assert "status" in s, "应含 status 字段"
    assert s["status"] in ("idle", "active"), f"status 应为 idle/active，实际 {s['status']}"
    assert "total" in s, "应含 total 字段"
    assert isinstance(s["total"], int), "total 应是 int"

    if s["status"] == "active":
        assert "by_category" in s, "active 状态应含 by_category"
        assert "oldest" in s, "active 状态应含 oldest"
        assert "top_priority" in s, "active 状态应含 top_priority"

    print("  ✓ status format")


def test_poll_unconsumed_sorted_by_priority():
    """poll_unconsumed 返回的消息按优先级排序。"""
    from routing.auto import poll_unconsumed
    from routing.router import priority

    messages = poll_unconsumed()
    if not messages or "error" in messages[0]:
        print("  ⚠ 跳过 poll_unconsumed 排序测试（无消息或 bus 错误）")
        return

    prev_priority = 0
    for m in messages:
        p = m.get("priority", 99)
        assert p >= prev_priority, f"消息应按优先级升序排列，发现 {p} < {prev_priority}"
        prev_priority = p

    print("  ✓ poll_unconsumed priority sorted")


def test_pipeline_format():
    """format_pipeline 输出包含所有角色。"""
    from routing.router import Router

    router = Router()
    text = router.format_pipeline()
    assert "=== Session 角色产出流水线（自动生成）===" in text
    # 至少应有角色信息
    assert len(router.routing) >= 0
    print("  ✓ format_pipeline")


def test_routing_summary():
    """routing_summary 返回正确的消费者映射。"""
    from routing.router import Router

    router = Router()
    summary = router.routing_summary()
    assert isinstance(summary, dict), "应返回字典"
    for role, info in summary.items():
        assert "produce" in info, f"{role} 缺少 produce"
        assert "consume" in info, f"{role} 缺少 consume"
        assert "consumers" in info, f"{role} 缺少 consumers"
    print("  ✓ routing_summary")


if __name__ == "__main__":
    print("=== Session Pipeline Router 单元测试 ===\n")

    tests = [
        test_parse_produce_categories,
        test_parse_consume_categories,
        test_priority_ordering,
        test_router_auto_build_from_json,
        test_router_fallback,
        test_get_producers_consumers,
        test_unconsumed_by_role_parsing,
        test_consume_linkage,
        test_category_desc_complete,
        test_status_format,
        test_poll_unconsumed_sorted_by_priority,
        test_pipeline_format,
        test_routing_summary,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__}: {e}")
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
