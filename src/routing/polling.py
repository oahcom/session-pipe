"""routing/polling.py — 消息轮询逻辑

从 auto.py 拆出，消除 auto ↔ routes 循环依赖。
"""

import uuid

from routing import router as _rt_mod
from reliability import (
    LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT,
    with_retry, DEFAULT_RETRY,
)


@with_retry(DEFAULT_RETRY)
def poll_unconsumed(category: str | None = None, consumer: str | None = None,
                    limit: int = 100) -> list[dict]:
    """拉取未消费消息，按优先级排序。

    使用 Blackboard.unconsumed() 直接获取，
    不再 subprocess + 字符串解析。
    集成重试、熔断、心跳、指标。
    使用 config.yaml 中的 max_messages_per_poll（Fix 2）。
    去重靠 bus 的 consume 记录（routes._do_consume_msg 记一笔 pipeline 接管），
    不再用 cursor：旧实现按随机 instance_id 存游标，表长到 21 461 行（2.9MB）
    且每轮写 1 000+ 次都读不回来（R131）。
    """
    from bus_protocol import Blackboard
    from config_loader import get_config

    bb = Blackboard()
    router = _rt_mod.get_router()
    try:
        max_per_poll = get_config().nested_get("bus", "max_messages_per_poll", default=100)
    except Exception as e:
        LOGGER.warning("poll_unconsumed config read failed, using default: %s", e,
                        extra={"trace_id": str(uuid.uuid4())[:8]})
        METRICS.inc("config_errors_total")
        max_per_poll = 100
    def _do_poll():
        effective_limit = min(limit, max_per_poll) if limit else max_per_poll
        # ponytail: passing cat filter would reduce overhead; use _consume_cats if available
        # 关键修复: 必须传 consumer 参数，否则 unconsumed() 返回从未被任何人消费的事实(即 0)
        # limit 下推到 SQL：实测 8,697 行的视图在 Python 侧切片要构造 8,697 个 Fact
        # （54ms/轮，每 60s 一次且 row_factory 逐行建对象）；下推后只取 100 行、1.2ms。
        return bb.unconsumed(consumer=consumer, limit=effective_limit)

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
            "src": getattr(f, "src", ""),
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
