#!/usr/bin/env python3
"""
Auto Route — 自动感知 bus 新消息并通知下游角色。
使用 Blackboard 直接 API（不再 subprocess 解析字符串）。
支持优先级路由和消费联动。
"""
import json
import sys
from pathlib import Path

# 加入 hermes scripts 路径
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_HERMES_SCRIPTS))

from router import get_router, CATEGORY_DESC, priority


def poll_unconsumed(category: str | None = None) -> list[dict]:
    """拉取未消费消息，按优先级排序。

    使用 Blackboard.unconsumed() 直接获取，
    不再 subprocess + 字符串解析。
    """
    from bus_protocol import Blackboard

    bb = Blackboard()
    router = get_router()
    try:
        facts = bb.unconsumed()
        messages: list[dict] = []
        for f in facts:
            if category and f.cat != category:
                continue
            messages.append({
                "id": f.id,
                "category": f.cat,
                "text": f.t[:100],
                "evidence": f.e[:120] if f.e else "",
                "priority": priority(f.cat),
                "consumers": router.get_consumers_prioritized(f.cat),
            })
        # 按优先级升序排列（高优先级在前）
        messages.sort(key=lambda m: m["priority"])
        return messages
    except Exception as e:
        return [{"error": str(e)}]


def notify_consumers(messages: list[dict]) -> None:
    """按优先级通知消费者。"""
    consumer_map: dict[str, list[dict]] = {}
    for msg in messages:
        for c in msg.get("consumers", []):
            consumer_map.setdefault(c, []).append(msg)

    for role, msgs in sorted(consumer_map.items()):
        # 该角色的消息按优先级排序
        msgs.sort(key=lambda m: m.get("priority", 99))
        print(f"  {role}: {len(msgs)} 条待消费")
        for m in msgs:
            print(f"    [P{m['priority']}] [{m['category']}] {m['text']}")
            if m.get("evidence"):
                print(f"      → {m['evidence']}")


def status() -> dict:
    """当前管线状态。"""
    messages = poll_unconsumed()
    if not messages or "error" in messages[0]:
        return {"status": "idle", "total": 0}

    by_cat: dict[str, dict] = {}
    for m in messages:
        cat = m.get("category", "unknown")
        by_cat.setdefault(cat, {"count": 0, "priority": priority(cat)})
        by_cat[cat]["count"] += 1

    # 按优先级排序的分类统计
    sorted_cats = dict(
        sorted(by_cat.items(), key=lambda x: x[1]["priority"])
    )

    return {
        "status": "active" if messages else "idle",
        "total": len(messages),
        "by_category": {k: v["count"] for k, v in sorted_cats.items()},
        "oldest": messages[0] if messages else None,
        "top_priority": min((m.get("priority", 99) for m in messages), default=None),
    }


def consume_with_linkage(fact_id: int, category: str, consumer: str = "claude") -> dict:
    """消费一条消息，联动更新其他角色计数。

    消费联动：
    - 标记该消息为已消费
    - 返回其他同样消费该分类的角色列表（供 caller 做后续处理）
    """
    from bus_protocol import Blackboard

    bb = Blackboard()
    router = get_router()

    # 记录消费
    bb.mark_consumed(fact_id, consumer)

    # 消费联动：获取其他受影响的消费者
    linked = router.consume_linkage(fact_id, category)

    return {
        "consumed": fact_id,
        "by": consumer,
        "category": category,
        "linked_consumers": [r for r in linked if r != consumer],
    }


if __name__ == "__main__":
    if "--status" in sys.argv:
        s = status()
        print(json.dumps(s, ensure_ascii=False, indent=2))
    elif "--consume" in sys.argv and len(sys.argv) >= 4:
        # python3 auto_route.py --consume <id> <category> [consumer]
        fid = int(sys.argv[2])
        cat = sys.argv[3]
        c = sys.argv[4] if len(sys.argv) > 4 else "claude"
        result = consume_with_linkage(fid, cat, c)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        msgs = poll_unconsumed()
        if not msgs:
            print("No unconsumed messages.")
        elif "error" in msgs[0]:
            print(f"Error: {msgs[0]['error']}")
        else:
            print(f"Pipeline active: {len(msgs)} messages\n")
            notify_consumers(msgs)
