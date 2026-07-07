#!/usr/bin/env python3
"""
Auto Route — 自动感知 bus 新消息并通知下游角色。
使用 Blackboard 直接 API（不再 subprocess 解析字符串）。
支持优先级路由、消费联动、重试、熔断、心跳、指标。
"""
import json
import sys
import time
from pathlib import Path

# 加入 hermes scripts 路径
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_HERMES_SCRIPTS))

from reliability import (
    LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT, DEFAULT_RETRY,
    with_retry, health_check, start_background_services, stop_background_services,
    GRACEFUL_SHUTDOWN, IDEMPOTENT_CONSUME, OPTIMISTIC_CLAIM, ACK_TRACKER
)
from config_loader import get_config
from router import get_router, CATEGORY_DESC, priority


@with_retry(DEFAULT_RETRY)
def poll_unconsumed(category: str | None = None) -> list[dict]:
    """拉取未消费消息，按优先级排序。

    使用 Blackboard.unconsumed() 直接获取，
    不再 subprocess + 字符串解析。
    集成重试、熔断、心跳、指标。
    """
    from bus_protocol import Blackboard

    bb = Blackboard()
    router = get_router()

    # 熔断器调用
    def _do_poll():
        return bb.unconsumed()

    try:
        facts = CIRCUIT_BREAKER.call(_do_poll)
    except Exception as e:
        METRICS.inc("poll_errors_total")
        LOGGER.error(f"poll_unconsumed failed: {e}", extra=LOGGER.handlers[0].formatter.format.__self__.__dict__ if hasattr(LOGGER.handlers[0], "formatter") else {})
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
            "priority": priority(f.cat),
            "consumers": router.get_consumers_prioritized(f.cat),
        })
    # 按优先级升序排列（高优先级在前）
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
    """消费一条消息，自动标记其他应消费该分类的角色为已消费。

    消费联动（P1 修复）：
    - 主消费者 consume（幂等 + 熔断）
    - 其他应消费该分类的角色自动标记 consume（联动更新 rc）
    - 记录 ACK + metrics + 心跳
    """
    from bus_protocol import Blackboard

    bb = Blackboard()
    router = get_router()
    tid = fact_id

    # 获取所有应消费该分类的角色
    all_consumers = router.get_consumers(category)
    # 排除主消费者自己
    linked = [r for r in all_consumers if r != consumer]

    # 熔断器保护消费操作
    def _do_consume():
        bb.mark_consumed(fact_id, consumer)

    try:
        CIRCUIT_BREAKER.call(_do_consume)
        # 联动：自动标记其他角色已消费
        for linked_role in linked:
            try:
                bb.mark_consumed(fact_id, linked_role)
            except Exception:
                METRICS.inc("consume_errors_total", labels={"consumer": linked_role})
    except Exception as e:
        LOGGER.error(f"consume #{tid} failed: {e}", extra={"trace_id": str(tid)})
        METRICS.inc("consume_errors_total", labels={"consumer": consumer})
        return {
            "consumed": fact_id,
            "by": consumer,
            "category": category,
            "error": str(e),
        }

    HEARTBEAT.beat(consumer)
    METRICS.inc("consume_count", labels={"consumer": consumer})

    ACK_TRACKER.record_ack(fact_id, consumer, "consumed", category=category)

    LOGGER.info(
        f"Consumed #{fact_id} [{category}] by {consumer}, auto-linked: {linked}",
        extra={"trace_id": str(fact_id)}
    )

    return {
        "consumed": fact_id,
        "by": consumer,
        "category": category,
        "auto_linked": linked,
    }


def route_all(consumer: str = "pipeline", dry_run: bool = False, parallel: bool = True) -> dict:
    """自动路由消费：为每条未消费消息并行分配给多个消费者。

    P1 改进：
    - 每条消息分配给所有应消费的消费者（不仅是第一个）
    - 使用 ThreadPoolExecutor 并行消费
    - 幂等消费防重复
    - ACK 记录和 metrics

    dry_run=True 时只展示分配方案。
    parallel=True 时使用线程池并行。
    """
    from concurrent.futures import ThreadPoolExecutor
    from bus_protocol import Blackboard

    bb = Blackboard()
    router = get_router()
    messages = poll_unconsumed()
    if not messages or "error" in messages[0]:
        METRICS.inc("route_errors_total")
        return {"routed": 0, "total": 0, "details": [], "dry_run": dry_run, "parallel": parallel}

    with METRICS.timer("route_latency_seconds"):
        details: list[dict] = []
        # 构建分配计划
        assignments: list[dict] = []
        for msg in messages:
            if "error" in msg:
                continue
            consumers = msg.get("consumers", [])
            ordered = (
                [c for c in consumers if "*" not in router._routing.get(c, {}).get("consume", [])]
                + [c for c in consumers if "*" in router._routing.get(c, {}).get("consume", [])]
            )
            if not ordered:
                METRICS.inc("route_orphan_count")
                details.append({"id": msg["id"], "category": msg["category"], "assigned": [], "reason": "no_consumer"})
                continue

            assignments.append({
                "id": msg["id"],
                "category": msg["category"],
                "title": msg["text"][:60],
                "priority": msg["priority"],
                "consumers": ordered,
            })

        if dry_run:
            for a in assignments:
                details.append({
                    "id": a["id"],
                    "category": a["category"],
                    "title": a["title"],
                    "assigned": a["consumers"],
                    "priority": a["priority"],
                })
        else:
            # 并行消费
            def _do_consume_msg(assignment: dict) -> dict:
                fid = assignment["id"]
                cat = assignment["category"]
                results = []
                for role in assignment["consumers"]:
                    try:
                        r = CIRCUIT_BREAKER.call(
                            lambda f=fid, rl=role: IDEMPOTENT_CONSUME.safe_consume(bb, f, rl)
                        )
                        ack_status = r.get("status", "error")
                        results.append({"role": role, "status": ack_status})
                        # ACK 记录
                        ACK_TRACKER.record_ack(fid, role, ack_status, category=cat,
                                               error=r.get("error", "") if ack_status == "error" else "")
                    except Exception as e:
                        LOGGER.error(f"route consume #{fid}->{role} failed: {e}",
                                     extra={"trace_id": str(fid)})
                        METRICS.inc("route_errors_total", labels={"consumer": role})
                        results.append({"role": role, "status": "error", "error": str(e)})
                        ACK_TRACKER.record_ack(fid, role, "error", category=cat, error=str(e))
                HEARTBEAT.beat(cat)
                METRICS.inc("route_assigned_count", labels={"category": cat})
                return {
                    "id": fid,
                    "category": cat,
                    "title": assignment["title"],
                    "assigned": results,
                    "priority": assignment["priority"],
                }

            executor = ThreadPoolExecutor(max_workers=min(8, len(assignments) or 1))
            try:
                details = list(executor.map(_do_consume_msg, assignments))
            finally:
                executor.shutdown(wait=True)

    routed = sum(1 for d in details if d.get("assigned"))
    result = {
        "routed": routed,
        "total": len(messages),
        "dry_run": dry_run,
        "parallel": parallel,
        "details": details,
    }

    HEARTBEAT.beat(consumer)
    LOGGER.info(f"Route-all: {result['routed']}/{result['total']} routed (parallel={parallel})",
                extra={"trace_id": "-"})
    return result


# ── CCS 路由 ──────────────────────────────────────────────


def route_to_ccs(role_name: str, dry_run: bool = False) -> dict:
    """将指定角色应消费的 bus 消息通过 launcher.send_to_ccs() 推送给对应的 CCS。

    role_name: 角色名（如 maintainer、scout）
    dry_run: 仅展示分配方案，不实际发送

    如果 CCS 未启动 -> 发一条警告而不是自动启动（由 cron_worker/daemon 负责启动）。
    返回路由结果字典。
    """
    router = get_router()
    messages = router.unconsumed_by_role(role_name)

    if not messages or (len(messages) == 1 and "error" in messages[0]):
        return {
            "role": role_name,
            "routed": 0,
            "total": 0,
            "dry_run": dry_run,
            "details": [],
        }

    # 延迟导入 launcher（避免启动时循环依赖）
    _launcher_src = "/home/administrator/session-launcher/src"
    if _launcher_src not in sys.path:
        sys.path.insert(0, _launcher_src)
    from launcher import send_to_ccs, is_ccs_running

    # 检查 CCS 是否运行（dry_run 时跳过）
    running = is_ccs_running(role_name) if not dry_run else None

    details: list[dict] = []
    for msg in messages:
        # 压缩 title + evidence 为自然语言
        text = msg.get("text", "")
        evidence = msg.get("evidence", "")
        body = f"{text}\n\n{evidence}" if evidence else text

        if dry_run:
            details.append({
                "id": msg["id"],
                "category": msg["category"],
                "body_preview": body[:120],
                "action": "dry_run",
            })
            continue

        if not running:
            details.append({
                "id": msg["id"],
                "category": msg["category"],
                "action": "skipped",
                "reason": f"CCS {role_name} 未启动",
            })
            continue

        result = send_to_ccs(role_name, body)
        if result.get("success"):
            details.append({
                "id": msg["id"],
                "category": msg["category"],
                "action": "routed",
                "sent_chars": result.get("sent_chars", 0),
            })
        else:
            details.append({
                "id": msg["id"],
                "category": msg["category"],
                "action": "error",
                "error": result.get("error", ""),
            })

    routed = sum(1 for d in details if d.get("action") == "routed")
    out = {
        "role": role_name,
        "routed": routed,
        "total": len(messages),
        "dry_run": dry_run,
        "details": details,
    }
    if not dry_run and not running:
        out["warning"] = f"CCS {role_name} 未启动，消息未推送（由 cron_worker/daemon 负责启动）"

    HEARTBEAT.beat(role_name)
    METRICS.inc("route_to_ccs_count", labels={"role": role_name})
    LOGGER.info(f"Route-to-CCS {role_name}: {routed}/{len(messages)} routed (dry_run={dry_run})",
                extra={"trace_id": "-"})
    return out


def route_all_to_ccs(dry_run: bool = False) -> dict:
    """对所有有未消费消息的角色，并行调用 route_to_ccs()。

    dry_run=True 时只展示分配方案。
    """
    from concurrent.futures import ThreadPoolExecutor

    router = get_router()
    roles = list(router.routing.keys())
    details: list[dict] = []

    with ThreadPoolExecutor(max_workers=min(8, len(roles) or 1)) as executor:
        futures = {executor.submit(route_to_ccs, role, dry_run): role for role in roles}
        for future in futures:
            role = futures[future]
            try:
                result = future.result()
                if result["total"] > 0:
                    details.append(result)
            except Exception as e:
                LOGGER.error(f"route_all_to_ccs role {role} failed: {e}",
                             extra={"trace_id": "-"})
                details.append({
                    "role": role, "routed": 0, "total": -1, "error": str(e),
                })

    result = {
        "routed": sum(d.get("routed", 0) for d in details),
        "total": sum(d.get("total", 0) for d in details),
        "dry_run": dry_run,
        "details": details,
    }

    HEARTBEAT.beat("pipeline")
    LOGGER.info(f"Route-all-to-CCS: {result['routed']}/{result['total']} routed (dry_run={dry_run})",
                extra={"trace_id": "-"})
    return result


if __name__ == "__main__":
    import signal
    has_json = "--json" in sys.argv
    argv = [a for a in sys.argv[1:] if a != "--json"]

    if "--daemon" in argv:
        # 守护模式：启动后台服务 → 持续轮询
        from config_loader import get_config
        cfg = get_config()
        poll_interval = cfg.nested_get("bus", "poll_interval", default=60)
        start_background_services()
        LOGGER.info(f"Daemon mode started, poll interval={poll_interval}s")
        try:
            while not GRACEFUL_SHUTDOWN._shutdown:
                result = route_all(consumer="pipeline")
                if result["routed"] > 0:
                    LOGGER.info(f"Routed {result['routed']} messages", extra={"trace_id": "-"})
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            LOGGER.info("Keyboard interrupt, shutting down...", extra={"trace_id": "-"})
        finally:
            stop_background_services()
            LOGGER.info("Daemon stopped", extra={"trace_id": "-"})
    elif "--status" in argv:
        s = status()
        if has_json:
            print(json.dumps(s, ensure_ascii=False))
        else:
            print(json.dumps(s, ensure_ascii=False, indent=2))
    elif "--health" in argv:
        hc = health_check()
        if has_json:
            print(json.dumps(hc, ensure_ascii=False))
        else:
            print(json.dumps(hc, ensure_ascii=False, indent=2))
    elif "--metrics" in argv:
        print(METRICS.export_prometheus())
    elif "--ack-stats" in argv:
        stats = ACK_TRACKER.ack_stats()
        if has_json:
            print(json.dumps(stats, ensure_ascii=False))
        else:
            print(f"ACKs: {stats['total']} total")
            for s, c in sorted(stats["by_status"].items()):
                print(f"  {s}: {c}")
    elif "--ack-retry" in argv:
        from bus_protocol import Blackboard
        bb = Blackboard()
        retried = ACK_TRACKER.retry_failed(bb)
        print(json.dumps({"retried": retried}, ensure_ascii=False))
    elif "--config" in argv:
        from config_loader import get_config
        cfg = get_config()
        if has_json:
            print(json.dumps(cfg.to_dict(), ensure_ascii=False))
        else:
            print(json.dumps(cfg.to_dict(), ensure_ascii=False, indent=2))
    elif "--route-all" in argv:
        consumer = argv[1] if len(argv) > 1 else "pipeline"
        result = route_all(consumer=consumer, dry_run="--dry-run" in argv)
        if has_json:
            print(json.dumps(result, ensure_ascii=False))
        else:
            for d in result["details"]:
                status_icon = "→" if d.get("assigned") else "✗"
                print(f"  {status_icon} #{d['id']} [{d['category']}] → {d.get('assigned', 'none')}")
            print(f"\n路由: {result['routed']}/{result['total']} 条")
    elif "--consume" in argv and len(argv) >= 3:
        fid = int(argv[1])
        cat = argv[2]
        c = argv[3] if len(argv) > 3 else "claude"
        result = consume_with_linkage(fid, cat, c)
        if has_json:
            print(json.dumps(result, ensure_ascii=False))
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
    elif "--route-to-ccs" in argv:
        if len(argv) < 2:
            print("Usage: --route-to-ccs <role_name> [--dry-run]", file=sys.stderr)
            sys.exit(1)
        role = argv[1]
        dry_run = "--dry-run" in argv
        result = route_to_ccs(role, dry_run=dry_run)
        if has_json:
            print(json.dumps(result, ensure_ascii=False))
        else:
            for d in result["details"]:
                icon = {"routed": "→", "skipped": "⊘", "error": "✗", "dry_run": "□"}.get(d.get("action"), "?")
                print(f"  {icon} #{d['id']} [{d['category']}] {d.get('action', '')} {d.get('reason', '')} {d.get('error', '')}")
            print(f"\n路由: {result['routed']}/{result['total']} 条 (dry_run={dry_run})")
            if result.get("warning"):
                print(f"警告: {result['warning']}")
    elif "--route-all-to-ccs" in argv:
        dry_run = "--dry-run" in argv
        result = route_all_to_ccs(dry_run=dry_run)
        if has_json:
            print(json.dumps(result, ensure_ascii=False))
        else:
            for role_detail in result["details"]:
                print(f"  {role_detail['role']}: {role_detail['routed']}/{role_detail['total']} routed")
                for d in role_detail["details"]:
                    icon = {"routed": "  →", "skipped": "  ⊘", "error": "  ✗", "dry_run": "  □"}.get(d.get("action"), "  ?")
                    print(f"    {icon} #{d['id']} [{d['category']}] {d.get('action', '')} {d.get('reason', '')} {d.get('error', '')}")
            print(f"\n总路由: {result['routed']}/{result['total']} 条 (dry_run={dry_run})")
    else:
        msgs = poll_unconsumed()
        if has_json:
            print(json.dumps({"messages": msgs, "count": len(msgs), "status": "active" if msgs else "idle"}, ensure_ascii=False))
        elif not msgs:
            print("No unconsumed messages.")
        elif "error" in msgs[0]:
            print(f"Error: {msgs[0]['error']}")
        else:
            print(f"Pipeline active: {len(msgs)} messages\n")
            notify_consumers(msgs)
