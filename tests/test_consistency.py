"""Tests for routing/consistency.py — 路由一致性校验。"""

from pathlib import Path
import json
import tempfile

_src = Path(__file__).resolve().parent.parent / "src"
import sys
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))


# ── helpers ──

def _write_persona(tmpdir: Path, name: str, output_targets=None, input_signals=None):
    data = {"name": name}
    if output_targets is not None:
        data["output_targets"] = output_targets
    if input_signals is not None:
        data["input_signals"] = input_signals
    (tmpdir / f"persona_{name}.json").write_text(json.dumps(data))


# ── test check_category_consistency ──


def test_category_consistency_all_defined():
    """所有 bus cat= 引用均已定义 → 空列表。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "00_maintainer", output_targets=[
            "输出 bus cat=security 报告",
            "输出 bus cat=code_fix 方案",
        ])
        defined = {"security", "code_fix"}
        undef = check_category_consistency(td_path, defined)
    assert undef == []


def test_category_consistency_undefined_output():
    """产出引用了未定义分类 → 返回未定义条目。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "01_scout", output_targets=[
            "输出 bus cat=nonexistent_cat 结果",
        ])
        defined = {"security", "code_fix"}
        undef = check_category_consistency(td_path, defined)
    assert len(undef) == 1
    assert "nonexistent_cat" in undef[0]


def test_category_consistency_undefined_input():
    """消费引用了未定义分类 → 返回未定义条目。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "02_closer", input_signals=[
            {"type": "bus", "spec": {"category": "unknown_cat"}},
        ])
        defined = {"security"}
        undef = check_category_consistency(td_path, defined)
    assert len(undef) == 1
    assert "unknown_cat" in undef[0]


def test_category_consistency_input_source_field():
    """input_signals 用 source 字段而非 spec.category → 也能匹配。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "03_curator", input_signals=[
            {"type": "bus", "source": "bus cat=undefined_by_source 数据"},
        ])
        defined = {"security"}
        undef = check_category_consistency(td_path, defined)
    assert len(undef) == 1
    assert "undefined_by_source" in undef[0]


def test_category_consistency_header_skip():
    """不含 output_targets/input_signals 的角色不产生错误。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "04_minimal")  # 只有 name
        undef = check_category_consistency(td_path, {"security"})
    assert undef == []


def test_category_consistency_bad_json():
    """损坏的 JSON → 报告无法解析。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        (td_path / "persona_bad.json").write_text("{not json}")
        undef = check_category_consistency(td_path, {"security"})
    assert any("无法解析" in u for u in undef)


def test_category_consistency_not_a_string_source():
    """source/spec.category 不是字符串 → 不报错。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "05_weird", input_signals=[
            {"type": "bus", "source": 42},
            {"type": "bus", "spec": {"category": ["list", "not"]}},
        ])
        undef = check_category_consistency(td_path, {"security"})
    assert undef == []


def test_category_consistency_no_bus_refs():
    """output_targets 没有 bus cat= 引用 → 空列表。"""
    from routing.consistency import check_category_consistency

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        _write_persona(td_path, "06_clean", output_targets=[
            "输出纯文本报告",
        ])
        undef = check_category_consistency(td_path, {"security", "code_fix"})
    assert undef == []


# ── test check_routing_links ──


def test_routing_links_ok():
    """所有生产有消费、消费有生产 → ok=True。"""
    from routing.consistency import check_routing_links

    routing = {
        "engineer": {"produce": ["code_fix"], "consume": ["bug_report"]},
        "scout": {"produce": ["bug_report"], "consume": ["code_fix"]},
    }
    result = check_routing_links(routing, {"security", "code_fix", "bug_report"})
    assert result["ok"] is True
    assert result["orphaned"] == []
    assert result["zombies"] == []


def test_routing_links_orphaned():
    """消费了但没有生产者 → orphaned 非空。"""
    from routing.consistency import check_routing_links

    routing = {
        "engineer": {"produce": ["code_fix"], "consume": ["blocker"]},
    }
    result = check_routing_links(routing, {"code_fix", "blocker"})
    # blocker 被消费但无生产者（engineer 只生产 code_fix）
    assert "blocker" in result["orphaned"]
    assert result["ok"] is False


def test_routing_links_zombies():
    """生产了但没有消费者 → zombies 非空。"""
    from routing.consistency import check_routing_links

    routing = {
        "engineer": {"produce": ["orphan_cat"], "consume": []},
    }
    result = check_routing_links(routing, {"orphan_cat"})
    assert "orphan_cat" in result["zombies"]
    assert result["ok"] is False


def test_routing_links_wildcard_expands():
    """consume: * 展开为全量分类。"""
    from routing.consistency import check_routing_links

    routing = {
        "maintainer": {"produce": ["security"], "consume": ["*"]},
    }
    all_cats = {"security", "architecture", "code_fix"}
    result = check_routing_links(routing, all_cats)
    # maintainer 消费了全部 -> 全部有消费
    # maintainer 生产了 security -> security 有生产
    # architecture 和 code_fix 被 maintainer 消费但无生产者 -> orphaned
    assert "architecture" in result["orphaned"]
    assert result["ok"] is False


def test_routing_links_empty():
    """空路由表 → ok=True（无异常但 trivially 正确）。"""
    from routing.consistency import check_routing_links

    result = check_routing_links({}, {"security"})
    assert result["ok"] is True
    assert result["orphaned"] == []
    assert result["zombies"] == []


# ── test check_bh_mapping ──


def test_bh_mapping_ok(tmp_path):
    """映射完整 → ok=True。"""
    from routing.consistency import check_bh_mapping

    bh_map = tmp_path / "_bh_to_sr_map.json"
    bh_map.write_text(json.dumps({
        "meta": {"bh_mapped": 2},
        "mapping": {"bh_a": "engineer", "bh_b": "scout"},
        "routing_gaps": {},
    }))
    result = check_bh_mapping(bh_map, {"engineer", "scout"})
    assert result["ok"] is True


def test_bh_mapping_file_not_found():
    """文件不存在 → ok=False, error 非空。"""
    from routing.consistency import check_bh_mapping

    result = check_bh_mapping("/tmp/__nonexistent_bh_map.json", {"a"})
    assert result["ok"] is False
    assert result["error"] == "文件不存在"


def test_bh_mapping_bad_json(tmp_path):
    """损坏的 JSON → ok=False。"""
    from routing.consistency import check_bh_mapping

    bh_map = tmp_path / "_bh_to_sr_map.json"
    bh_map.write_text("{bad")
    result = check_bh_mapping(bh_map, {"a"})
    assert result["ok"] is False
    assert result["unknown_roles"] == []


def test_bh_mapping_unknown_roles(tmp_path):
    """映射目标角色不在路由表中 → ok=False, unknown_roles 非空。"""
    from routing.consistency import check_bh_mapping

    bh_map = tmp_path / "_bh_to_sr_map.json"
    bh_map.write_text(json.dumps({
        "meta": {"bh_mapped": 1},
        "mapping": {"bh_a": "ghost_role"},
        "routing_gaps": {},
    }))
    result = check_bh_mapping(bh_map, {"maintainer", "engineer"})
    assert result["ok"] is False
    assert "ghost_role" in result["unknown_roles"]


def test_bh_mapping_missing_from_routing_gaps(tmp_path):
    """routing_gaps 中的角色不在路由表 → ok=False, missing_roles 非空。"""
    from routing.consistency import check_bh_mapping

    bh_map = tmp_path / "_bh_to_sr_map.json"
    bh_map.write_text(json.dumps({
        "meta": {"bh_mapped": 1},
        "mapping": {"bh_a": "engineer"},
        "routing_gaps": {"ghost_role": ["bh_z"]},
    }))
    result = check_bh_mapping(bh_map, {"engineer"})
    assert result["ok"] is False
    assert "ghost_role" in result["missing_roles"]
