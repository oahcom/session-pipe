"""consistency.py — 路由一致性校验。

提供可复用的校验函数，检查：
1. 角色 JSON 中 bus cat 引用 vs CATEGORY_PRIORITY 定义
2. 路由链接完整性（有消费无生产 / 有生产无消费）
3. BH→SR 映射完整性

这些校验从 health_check_all.py 提取为独立模块，使测试可隔离。
"""

from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Optional

_CAT_REF_RE = re.compile(r"bus cat=(\w+)")


def check_category_consistency(
    persona_dir: str | Path,
    defined_categories: set[str],
) -> list[str]:
    """检查所有角色 JSON 的 bus cat= 引用是否在 defined_categories 中。

    返回未定义分类列表（空=全部通过）。
    """
    undef: list[str] = []
    persona_dir = Path(persona_dir)
    for f in sorted(persona_dir.glob("persona_*.json")):
        try:
            data = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            undef.append(f"{f.name}: 无法解析 JSON")
            continue
        for target in data.get("output_targets", []):
            for m in _CAT_REF_RE.finditer(target):
                if m.group(1) not in defined_categories:
                    undef.append(f"{f.name} 产出 {m.group(1)}")
        for sig in data.get("input_signals", []):
            # Check bare spec.category value directly (e.g. "security")
            raw_cat = sig.get("spec", {}).get("category", "")
            if isinstance(raw_cat, str) and raw_cat and raw_cat not in defined_categories:
                undef.append(f"{f.name} 消费 {raw_cat}")
            # Check natural language references (output_targets, source with bus cat=xxx)
            src = sig.get("source", raw_cat)
            if isinstance(src, str):
                for m in _CAT_REF_RE.finditer(src):
                    if m.group(1) not in defined_categories:
                        undef.append(f"{f.name} 消费 {m.group(1)}")
    return undef


def check_routing_links(
    routing: dict[str, dict],
    all_categories: set[str],
) -> dict:
    """检查路由链接完整性。

    routing: {role: {"produce": [...], "consume": [...]}}
    all_categories: 全量分类集合（用于展开 *）

    返回 {"orphaned": [分类], "zombies": [分类], "ok": bool}
    """
    all_consume: set[str] = set()
    for data in routing.values():
        cats = data.get("consume", [])
        if "*" in cats:
            all_consume.update(all_categories)
        else:
            all_consume.update(cats)

    all_produce: set[str] = set()
    for data in routing.values():
        all_produce.update(data.get("produce", []))

    orphaned = [c for c in sorted(all_consume) if c not in all_produce and c != "*"]
    zombies = [c for c in sorted(all_produce) if c not in all_consume]

    return {
        "orphaned": orphaned,
        "zombies": zombies,
        "ok": len(orphaned) == 0 and len(zombies) == 0,
    }


def check_bh_mapping(
    bh_map_path: str | Path,
    routing_roles: set[str],
) -> dict:
    """检查 BH→SR 映射完整性。

    bh_map_path: _bh_to_sr_map.json 路径
    routing_roles: 路由表中的角色名集合

    返回 {"unknown_roles": [...], "missing_roles": [...], "ok": bool, "error": str}
    """
    bh_map_path = Path(bh_map_path)
    if not bh_map_path.exists():
        return {"unknown_roles": [], "missing_roles": [], "ok": False,
                "error": "文件不存在"}

    try:
        mapping = json.loads(bh_map_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return {"unknown_roles": [], "missing_roles": [], "ok": False,
                "error": str(e)}

    mapping_dict = mapping.get("mapping", {})
    mapped_roles = set(mapping_dict.values())

    unknown_roles = [r for r in sorted(mapped_roles) if r not in routing_roles]
    gaps = mapping.get("routing_gaps", {})
    missing_roles = [r for r in gaps if r not in routing_roles]

    return {
        "unknown_roles": unknown_roles,
        "missing_roles": missing_roles,
        "ok": len(unknown_roles) == 0 and len(missing_roles) == 0,
    }
