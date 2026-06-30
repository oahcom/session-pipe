#!/usr/bin/env python3
"""
Auto Route — 自动感知 bus 新消息并通知下游角色。

用于 coordinator 的定期扫描，或 consumer 的推送通知。
"""
import json
import os
import subprocess
from pathlib import Path

from router import get_consumers, CATEGORY_DESC


BUS_CLIENT = Path.home() / ".hermes/scripts/bus_client.py"


def poll_unconsumed(category: str | None = None) -> list[dict]:
    """拉取未消费消息并分类。"""
    try:
        result = subprocess.run(
            ["python3", str(BUS_CLIENT), "unread", "--all"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split("\n")

        messages = []
        for line in lines:
            if not line.strip():
                continue

            # Parse line format: [{id}] ({cat}) {text}
            if "(" in line and ")" in line:
                id_part = line.split("]")[0].strip("[").strip() if "]" in line else ""
                cat_part = line.split("(")[1].split(")")[0] if "(" in line else ""
                text_part = line.split(")")[-1].strip() if ")" in line else line

                if category and cat_part != category:
                    continue

                messages.append({
                    "id": id_part,
                    "category": cat_part,
                    "text": text_part[:100],
                    "consumers": get_consumers(cat_part),
                })
        return messages
    except Exception as e:
        return [{"error": str(e)}]


def notify_consumers(messages: list[dict]) -> None:
    """模拟通知消费者。实际通知通过他们自己的 CronCreate 轮询实现。"""
    # 按消费者分组
    consumer_map = {}
    for msg in messages:
        for c in msg.get("consumers", []):
            consumer_map.setdefault(c, []).append(msg)

    for role, msgs in sorted(consumer_map.items()):
        print(f"  {role}: {len(msgs)} 条待消费")
        for m in msgs:
            print(f"    [{m['category']}] {m['text']}")


def status() -> dict:
    """当前管线状态。"""
    messages = poll_unconsumed()

    if not messages or "error" in messages[0]:
        return {"status": "idle", "total": 0}

    # 按分类统计
    by_cat = {}
    for m in messages:
        cat = m.get("category", "unknown")
        by_cat[cat] = by_cat.get(cat, 0) + 1

    return {
        "status": "active" if messages else "idle",
        "total": len(messages),
        "by_category": by_cat,
        "oldest": messages[0] if messages else None,
    }


if __name__ == "__main__":
    import sys

    if "--status" in sys.argv:
        s = status()
        print(json.dumps(s, ensure_ascii=False, indent=2))
    else:
        msgs = poll_unconsumed()
        if not msgs:
            print("No unconsumed messages.")
        elif "error" in msgs[0]:
            print(f"Error: {msgs[0]['error']}")
        else:
            print(f"Pipeline active: {len(msgs)} messages\n")
            notify_consumers(msgs)