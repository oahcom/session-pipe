"""routing/polling.py — 消息轮询逻辑

从 auto.py 拆出，消除 auto ↔ routes 循环依赖。
"""

import uuid
from typing import Optional

from routing import router as _rt_mod
from reliability import (
    LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT,
    with_retry, DEFAULT_RETRY, get_last_cursor,
)


@with_retry(DEFAULT_RETRY)
def poll_unconsumed(category: str | None = None, consumer: str | None = None,
                    instance_id: str = "", limit: int = 100) -> list[dict]:
    """拉取未消费消息，按优先级排序。

    使用 Blackboard.unconsumed() 直接获取，
    不再 subprocess + 字符串解析。
    集成重试、熔断、心跳、指标。
    使用 config.yaml 中的 max_messages_per_poll（Fix 2）。
    若指定 consumer，跳过 cursor 之前的消息（Fix 6 防重启重复处理）。
    instance_id 区分不同 pipeline 实例的 cursor（Fix 7：多实例隔离）。
    """
    from bus_protocol import Blackboard
    from config_loader import get_config

    bb = Blackboard()
    router = _rt_mod.get_router()
    max_per_poll = get_config().nested_get("bus", "max_messages_per_poll", default=100)
    since_id = get_last_cursor(consumer, "", instance_id) if consumer else 0

    def _do_poll():
        effective_limit = min(limit, max_per_poll) if limit else max_per_poll
        return [f for f in bb.unconsumed() if f.id > since_id][:effective_limit]

    try:
        facts = CIRCUIT_BREAKER.call(_do_poll)
    except Exception as e:
        METRICS.inc("poll_errors_total")
        LOGGER.error(f"poll_unconsumed failed: {e}", extra={"trace_id": str(uuid.uuid4())[:8]})
        return [{"error": str(e)}]

    HEARTBEAT.beat("pipeline")
    messages: list[dict] = []
    for f in facts:
        if category and f.cat != category:
            continue
        messages.append({
            "id": f.id,
            "category": f.cat,
            "text": f.t[:100],
            "evidence": f.e[:120] if f.e else "",
            "priority": _rt_mod.priority(f.cat),
            "consumers": router.get_consumers_prioritized(f.cat),
        })
    messages.sort(key=lambda m: m["priority"])

    METRICS.inc("poll_count")
    METRICS.observe("backlog_size", len(messages))
    return messages


def notify_consumers(messages: list[dict]) -> None:
    """按优先级通知消费者。"""
    consumer_map: dict[str, list[dict]] = {}
    for msg in messages:
        for c in msg.get("consumers", []):
            consumer_map.setdefault(c, []).append(msg)

    for role, msgs in sorted(consumer_map.items()):
        msgs.sort(key=lambda m: m.get("priority", 99))
        print(f"  {role}: {len(msgs)} 条待消费")
        for m in msgs:
            print(f"    [P{m['priority']}] [{m['category']}] {m['text']}")
            if m.get("evidence"):
                print(f"      → {m['evidence']}")
