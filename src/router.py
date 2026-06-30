#!/usr/bin/env python3
"""
Session Pipeline Router — 角色间消息路由。
基于 Sister Bus (Blackboard) 实现，路由规则从项目 A 角色 JSON 自动生成。

职责：
1. 从角色 JSON 的 output_targets 自动推导 produce/consume 关系
2. 优先级路由：security > code_fix > architecture > 其他
3. 消费联动：consume 时自动递减其他相关角色的待消费计数

ponytail: 若角色数超 50+，Router._routing 应改为 SQLite 持久化，避免每次 import 都解析 JSON。
"""
import json
import re
import sys
from pathlib import Path
from typing import Optional

# ── 路径自动发现 ──
# 加入 hermes scripts 目录（保证 bus_protocol 可导入）
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_HERMES_SCRIPTS))

# 项目 A 角色 JSON 目录
SESSION_ROLES_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"

# ── 分类优先级（数字越小越优先）──
CATEGORY_PRIORITY = {
    "security": 1,
    "code_fix": 2,
    "architecture": 3,
    "performance": 4,
    "evolution_report": 5,
    "reflexion_lesson": 6,
    "deception": 7,
}
_DEFAULT_PRIORITY = 8

# ── 分类描述 ──
CATEGORY_DESC: dict[str, str] = {
    "reflexion_lesson": "经验教训（消费者沉淀）",
    "code_fix": "代码修复（维护者/开发者产出）",
    "architecture": "架构决策/新发现（侦察兵/管理者产出）",
    "evolution_report": "进化轮次报告（侦察兵产出）",
    "security": "安全告警",
    "performance": "性能发现",
    "deception": "欺骗检测",
}


def priority(category: str) -> int:
    """返回分类优先级（数字越小越优先）。"""
    return CATEGORY_PRIORITY.get(category, _DEFAULT_PRIORITY)


def _parse_produce_categories(output_targets: list[str]) -> list[str]:
    """从 output_targets 提取产出分类。

    "bus cat=code_fix 修复方案" → ["code_fix"]
    "git commit" → []（忽略非 bus 目标）
    "bus consume" → []（消费动作，非产出）
    """
    cats: list[str] = []
    for target in output_targets:
        m = re.search(r"bus cat=(\w+)", target)
        if m:
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
    return cats


def _parse_consume_categories(input_signals: list[dict]) -> list[str]:
    """从 input_signals 提取消费分类。

    {"source": "bus cat=security"} → ["security"]
    {"source": "bus_client.py read --cat security"} → ["security"]
    {"source": "bus_client.py unread --all"} → ["*"]（消费所有）
    {"source": "systemctl ..."} → []（非 bus 信号）
    """
    cats: list[str] = []
    for signal in input_signals:
        source = signal.get("source", "")
        # 全量消费标记
        if "unread --all" in source:
            if "*" not in cats:
                cats.append("*")
            continue
        # bus cat=xxx 模式（\w+ 不匹配 *，单独处理）
        if "bus cat=*" in source:
            if "*" not in cats:
                cats.append("*")
            continue
        for m in re.finditer(r"bus cat=(\w+)", source):
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
        # bus_client.py read --cat xxx 模式
        for m in re.finditer(r"--cat (\w+)", source):
            cat = m.group(1)
            if cat not in cats:
                cats.append(cat)
    return cats


class Router:
    """路由表：从角色 JSON 自动构建，支持硬编码回退。"""

    def __init__(self, roles_dir: Path = SESSION_ROLES_DIR):
        self.roles_dir = roles_dir
        self._routing = self._build_routing()

    def _build_routing(self) -> dict:
        """从角色 JSON 文件构建路由表。

        回退：若目录不存在/读取失败，使用硬编码默认值。
        """
        if not self.roles_dir.exists():
            return self._default_routing()

        routing: dict = {}
        for json_file in sorted(self.roles_dir.glob("*.json")):
            try:
                with open(json_file) as f:
                    persona = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            name = persona.get("name")
            if not name:
                continue

            produce = _parse_produce_categories(persona.get("output_targets", []))
            consume = _parse_consume_categories(persona.get("input_signals", []))

            routing[name] = {"produce": produce, "consume": consume}

        return routing if routing else self._default_routing()

    @staticmethod
    def _default_routing() -> dict:
        """硬编码回退路由表（当项目 A JSON 不可读时）。"""
        return {
            "maintainer": {"produce": ["code_fix", "architecture"], "consume": ["security"]},
            "scout": {"produce": ["architecture", "evolution_report"], "consume": ["architecture"]},
            "consumer": {"produce": ["reflexion_lesson"], "consume": ["*"]},
            "developer": {"produce": ["code_fix"], "consume": ["architecture", "code_fix"]},
            "coordinator": {"produce": ["architecture"], "consume": ["*"]},
            "curator": {"produce": ["architecture"], "consume": []},
            "closer": {"produce": ["architecture"], "consume": ["code_fix", "architecture"]},
        }

    @property
    def routing(self) -> dict:
        return self._routing

    def get_producers(self, category: str) -> list[str]:
        """返回某类消息的生产者列表。"""
        return [
            role for role, r in self._routing.items()
            if category in r.get("produce", [])
        ]

    def get_consumers(self, category: str) -> list[str]:
        """返回某类消息的消费者列表。"""
        return [
            role for role, r in self._routing.items()
            if "*" in r.get("consume", []) or category in r.get("consume", [])
        ]

    def get_consumers_prioritized(self, category: str) -> list[str]:
        """返回按优先级排序的消费者列表。

        规则：
        1. 消费该分类的消费者
        2. 按优先级排序：security > code_fix > architecture > 其他
        3. consume="*"（通吃一切）的角色排最后
        """
        consumers = self.get_consumers(category)
        cat_prio = priority(category)
        # 先按分类优先级，通吃角色排后
        consumers.sort(key=lambda r: (
            self._routing.get(r, {}).get("consume", []).count("*"),  # 通吃角色排后
            cat_prio,  # 同类按分类优先级
        ))
        return consumers

    def role_produce_categories(self, role: str) -> list[str]:
        """返回指定角色可产出的分类列表。"""
        return self._routing.get(role, {}).get("produce", [])

    def role_consume_categories(self, role: str) -> list[str]:
        """返回指定角色可消费的分类列表（* 展开为全量分类）。"""
        cats = self._routing.get(role, {}).get("consume", [])
        if "*" in cats:
            return list(CATEGORY_DESC.keys())
        return cats

    def unconsumed_by_role(self, role: str, limit: int = 20) -> list[dict]:
        """返回指定角色应消费的未消费消息。改用 Blackboard 直接 API。"""
        from bus_protocol import Blackboard

        bb = Blackboard()
        cats = self.role_consume_categories(role)
        try:
            facts = bb.unconsumed()
            messages: list[dict] = []
            for f in facts:
                if not cats or f.cat in cats or "*" in cats:
                    messages.append({
                        "id": f.id,
                        "category": f.cat,
                        "text": f.t[:100],
                        "evidence": f.e[:120] if f.e else "",
                        "role": role,
                        "priority": priority(f.cat),
                    })
            # 按优先级排序
            messages.sort(key=lambda m: m["priority"])
            return messages[:limit]
        except Exception as e:
            return [{"error": str(e)}]

    def routing_summary(self) -> dict:
        """返回当前路由拓扑摘要。"""
        result: dict = {}
        for role, r in self._routing.items():
            result[role] = {
                "produce": r.get("produce", []),
                "consume": r.get("consume", []),
                "consumers": [
                    cr for cr in self._routing
                    if ("*" in self._routing[cr].get("consume", [])
                        or any(c in self._routing[cr].get("consume", [])
                               for c in r.get("produce", [])))
                ],
            }
        return result

    def format_pipeline(self) -> str:
        """格式化流水线图（人类可读）。"""
        lines = ["=== Session 角色产出流水线（自动生成）===\n"]
        for role, r in self._routing.items():
            produce = r.get("produce", [])
            consume = r.get("consume", [])
            produce_str = ", ".join(CATEGORY_DESC.get(c, c) for c in produce) if produce else "（无）"
            if "*" in consume:
                consume_str = "所有分类"
            else:
                consume_str = ", ".join(CATEGORY_DESC.get(c, c) for c in consume) if consume else "（无）"
            lines.append(f"  {role}: 产出={produce}  消费={consume}")

        return "\n".join(lines)

    def consume_linkage(self, fact_id: int, category: str) -> list[str]:
        """消费一条消息后，返回其他受影响的消费者列表。

        消费联动逻辑：
        角色 A consume 了分类 X 的消息后，
        其他也消费分类 X 的角色计数自动递减（由 caller 实现标记）。
        """
        consumers = self.get_consumers(category)
        return consumers


# ── 全局单例（懒加载） ──
_router: Optional[Router] = None


def get_router() -> Router:
    """获取（或创建）全局 Router 单例。"""
    global _router
    if _router is None:
        _router = Router()
    return _router


# ── 兼容旧接口（路由函数） ──

def get_producers(category: str) -> list[str]:
    return get_router().get_producers(category)


def get_consumers(category: str) -> list[str]:
    return get_router().get_consumers(category)


def role_produce_categories(role: str) -> list[str]:
    return get_router().role_produce_categories(role)


def role_consume_categories(role: str) -> list[str]:
    return get_router().role_consume_categories(role)


def unconsumed_by_role(role: str, limit: int = 20) -> list[dict]:
    return get_router().unconsumed_by_role(role, limit=limit)


def routing_summary() -> dict:
    return get_router().routing_summary()


def format_pipeline() -> str:
    return get_router().format_pipeline()


def category_priority(cat: str) -> int:
    """返回分类的优先级分数（数字越小越优先）。"""
    return priority(cat)


if __name__ == "__main__":
    r = get_router()
    print(r.format_pipeline())
    print()
    print(json.dumps(r.routing_summary(), ensure_ascii=False, indent=2))
