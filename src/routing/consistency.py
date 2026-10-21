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
            # output_targets 有两种合法格式（与 drift_detector._parse_allowed_categories
            # 一致）：新式 dict {"bus_cat": ..., "label": ...} 与旧式字符串 "bus cat=xxx"。
            # 只处理字符串会对 dict 调 finditer → TypeError，而调用方（routing_daemon
            # 每小时校验）用 except 包住 → 该防御检查自上线起从未成功执行过一次
            # （实测 14 天 15 次异常 / 0 次通过）。
            if isinstance(target, dict):
                cat = target.get("bus_cat")
                if isinstance(cat, str) and cat and cat not in defined_categories:
                    undef.append(f"{f.name} 产出 {cat}")
                continue
            if not isinstance(target, str):
                continue
            for m in _CAT_REF_RE.finditer(target):
                if m.group(1) not in defined_categories:
                    undef.append(f"{f.name} 产出 {m.group(1)}")
        for sig in data.get("input_signals", []):
            # Check bare spec.category value directly (e.g. "security")
            raw_cat = sig.get("spec", {}).get("category", "")
            # "*" 是合法通配符（消费全部分类），不是未定义分类——5 个 persona
            # 用它声明"接收所有信号"，旧实现对它报假阳性。
            if (isinstance(raw_cat, str) and raw_cat and raw_cat != "*"
                    and raw_cat not in defined_categories):
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
    _has_wildcard = False
    for data in routing.values():
        cats = data.get("consume", [])
        if "*" in cats:
            _has_wildcard = True
            all_consume.update(all_categories)
        else:
            all_consume.update(cats)

    all_produce: set[str] = set()
    for data in routing.values():
        all_produce.update(data.get("produce", []))

    # "*" 消费者 = "我处理任何分类"，故无生产者的分类不算孤儿（wildcard 会兜底）。
    # 仅当 all_categories 为空（"*" 展开为空）时才可能误判，此时不强依赖展开。
    orphaned = [c for c in sorted(all_consume)
                if c not in all_produce and c != "*" and not _has_wildcard]
    zombies = [c for c in sorted(all_produce) if c not in all_consume]

    return {
        "orphaned": orphaned,
        "zombies": zombies,
        "ok": len(orphaned) == 0 and len(zombies) == 0,
    }


