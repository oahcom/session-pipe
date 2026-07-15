"""test_router_extend_bh.py — Router._extend_from_bh_config 边界条件测试"""
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from router import Router, _parse_produce_categories


@pytest.fixture
def roles_dir():
    """创建包含一个基本 session-role 的临时角色目录。"""
    with tempfile.TemporaryDirectory() as tmp:
        roles_path = Path(tmp) / "personas" / "session-roles"
        roles_path.mkdir(parents=True)
        # 一个基本角色 JSON
        role = {
            "name": "maintainer",
            "title": "维护者",
            "output_targets": ["bus cat=security 安全告警"],
            "input_signals": [{"type": "bus", "spec": {"category": "security"}}],
        }
        (roles_path / "persona_00_maintainer.json").write_text(json.dumps(role))
        yield Path(tmp)


@pytest.fixture
def router(roles_dir):
    """用临时角色目录创建 Router 实例。"""
    return Router(roles_dir=roles_dir / "personas" / "session-roles")


def test_extend_from_bh_config_no_file(router):
    """BH config 文件不存在时不应报错。"""
    routing = router._build_routing()
    assert "maintainer" in routing
    assert "security" in routing["maintainer"]["produce"]


def test_extend_from_bh_config_corrupted():
    """BH config 文件损坏时不应影响路由表构建。"""
    with tempfile.TemporaryDirectory() as tmp:
        roles_path = Path(tmp) / "personas" / "session-roles"
        roles_path.mkdir(parents=True)
        (roles_path / "persona_00_maintainer.json").write_text(json.dumps({
            "name": "maintainer", "title": "M", "output_targets": [],
            "input_signals": [],
        }))
        # 创建损坏的 BH config
        bh_dir = Path(tmp) / "personas" / "browser-harness"
        bh_dir.mkdir(parents=True)
        (bh_dir / "_bh_route_config.json").write_text("{corrupted json")
        r = Router(roles_dir=roles_path)
        routing = r._build_routing()
        assert "maintainer" in routing  # 不应报错


def test_extend_from_bh_config_empty_profiles():
    """BH config 空 profiles 时不额外扩展路由。"""
    with tempfile.TemporaryDirectory() as tmp:
        roles_path = Path(tmp) / "personas" / "session-roles"
        roles_path.mkdir(parents=True)
        (roles_path / "persona_00_maintainer.json").write_text(json.dumps({
            "name": "maintainer", "title": "M", "output_targets": [],
            "input_signals": [],
        }))
        bh_dir = Path(tmp) / "personas" / "browser-harness"
        bh_dir.mkdir(parents=True)
        (bh_dir / "_bh_route_config.json").write_text(json.dumps({
            "bh_profiles": {},
        }))
        r = Router(roles_dir=roles_path)
        routing = r._build_routing()
        assert "maintainer" in routing


def test_extend_from_bh_config_no_sr_mapping():
    """BH profile 无 sr_mapping 时跳过该 profile。"""
    with tempfile.TemporaryDirectory() as tmp:
        roles_path = Path(tmp) / "personas" / "session-roles"
        roles_path.mkdir(parents=True)
        (roles_path / "persona_00_maintainer.json").write_text(json.dumps({
            "name": "maintainer", "title": "M", "output_targets": [],
            "input_signals": [],
        }))
        bh_dir = Path(tmp) / "personas" / "browser-harness"
        bh_dir.mkdir(parents=True)
        (bh_dir / "_bh_route_config.json").write_text(json.dumps({
            "bh_profiles": {
                "some-profile": {
                    "route": {"produce": ["test_report"]},
                    # 无 sr_mapping
                },
            },
        }))
        r = Router(roles_dir=roles_path)
        routing = r._build_routing()
        assert "maintainer" in routing


def test_parse_produce_categories_empty():
    """空 output_targets 返回空列表。"""
    assert _parse_produce_categories([]) == []


def test_parse_produce_categories_bus_cat():
    """标准 bus cat=xxx 格式。"""
    result = _parse_produce_categories(["bus cat=security 安全告警", "bus cat=code_fix 修复"])
    assert "security" in result
    assert "code_fix" in result
    assert len(result) == 2


def test_parse_produce_categories_ignores_non_bus():
    """非 bus 输出目标（如 git commit）应忽略。"""
    result = _parse_produce_categories(["git commit", "bus cat=security 安全"])
    assert "security" in result
    assert "git" not in result
