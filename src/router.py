#!/usr/bin/env python3
"""
Session Pipeline Router — 角色间消息路由。
基于 Sister Bus (bus_protocol.Blackboard) 实现。

角色产出 → 写入 bus → router 更新消费状态 → 下游角色在启动时读取。
"""
import json
from pathlib import Path
from typing import Optional


# 角色路由映射
ROLE_ROUTING = {
    "maintainer": {
        "produce": ["code_fix"],
        "consume": ["security"]
    },
    "scout": {
        "produce": ["architecture", "evolution_report"],
        "consume": []
    },
    "consumer": {
        "produce": ["reflexion_lesson"],
        "consume": ["*"]
    },
    "developer": {
        "produce": ["code_fix"],
        "consume": ["architecture", "code_fix"]
    },
    "coordinator": {
        "produce": ["architecture"],
        "consume": ["*"]
    },
    "curator": {
        "produce": ["architecture"],
        "consume": []
    },
    "closer": {
        "produce": [],
        "consume": ["code_fix", "architecture"]
    }
}

# 分类描述
CATEGORY_DESC = {
    "reflexion_lesson": "经验教训（消费者沉淀）",
    "code_fix": "代码修复（维护者/开发者产出）",
    "architecture": "架构决策/新发现（侦察兵/管理者产出）",
    "evolution_report": "进化轮次报告（侦察兵产出）",
    "security": "安全告警",
    "performance": "性能发现",
    "deception": "欺骗检测"
}


def get_producers(category: str) -> list[str]:
    """返回某类消息的生产者列表。"""
    return [
        role for role, routing in ROLE_ROUTING.items()
        if category in routing.get("produce", [])
    ]


def get_consumers(category: str) -> list[str]:
    """返回某类消息的消费者列表。"""
    return [
        role for role, routing in ROLE_ROUTING.items()
        if "*" in routing.get("consume", []) or category in routing.get("consume", [])
    ]


def role_produce_categories(role: str) -> list[str]:
    """返回指定角色的产出分类。"""
    routing = ROLE_ROUTING.get(role, {})
    return routing.get("produce", [])


def role_consume_categories(role: str) -> list[str]:
    """返回指定角色的消费分类。"""
    routing = ROLE_ROUTING.get(role, {})
    cats = routing.get("consume", [])
    if "*" in cats:
        return list(CATEGORY_DESC.keys())
    return cats


def unconsumed_by_role(role: str, limit: int = 20) -> list[dict]:
    """返回指定角色应消费的未消费消息。"""
    from bus_protocol import Blackboard

    bb = Blackboard()
    cats = role_consume_categories(role)

    # 读所有未消费消息
    try:
        # 尝试从 bus 获取
        import subprocess
        result = subprocess.run(
            ["python3", str(Path.home() / ".hermes/scripts/bus_client.py"), "unread", "--all"],
            capture_output=True, text=True, timeout=10
        )
        output = result.stdout

        # 解析输出提取消息
        messages = []
        for line in output.split("\n"):
            if "[" in line and "]" in line:
                cat = ""
                if "(code_fix)" in line:
                    cat = "code_fix"
                elif "(architecture)" in line:
                    cat = "architecture"
                elif "(reflexion_lesson)" in line:
                    cat = "reflexion_lesson"
                elif "(evolution_report)" in line:
                    cat = "evolution_report"

                if not cats or cat in cats or "*" in cats:
                    messages.append({
                        "raw": line,
                        "category": cat,
                        "role": role
                    })
        return messages[:limit]
    except Exception as e:
        return [{"error": str(e)}]


def routing_summary() -> dict:
    """返回当前路由拓扑。"""
    return {
        role: {
            "produce": routing.get("produce", []),
            "consume": routing.get("consume", []),
            "consumers": [r for r in ROLE_ROUTING
                         if "*" in ROLE_ROUTING[r].get("consume", [])
                         or any(c in ROLE_ROUTING[r].get("consume", [])
                                for c in routing.get("produce", []))]
        }
        for role, routing in ROLE_ROUTING.items()
    }


def format_pipeline() -> str:
    """格式化流水线图。"""
    lines = ["=== Session 角色产出流水线 ===\n"]

    for role, routing in ROLE_ROUTING.items():
        produce = routing.get("produce", [])
        consume = routing.get("consume", [])

        produce_str = ", ".join(CATEGORY_DESC.get(c, c) for c in produce) if produce else "（无）"
        consume_str = ", ".join(CATEGORY_DESC.get(c, c) for c in consume) if consume else "（无）"
        if "*" in consume:
            consume_str = "所有分类"

        lines.append(f"{role} ({routing['produce']} → {routing['consume']})")

    return "\n".join(lines)


if __name__ == "__main__":
    print(format_pipeline())
    print()
    import json
    print(json.dumps(routing_summary(), ensure_ascii=False, indent=2))