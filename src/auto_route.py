#!/usr/bin/env python3
"""
Auto Route — 自动感知 bus 新消息并通知下游角色。
使用 Blackboard 直接 API（不再 subprocess 解析字符串）。
支持优先级路由、消费联动、重试、熔断、心跳、指标。
"""
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

# 确保 src 目录在 hermes_scripts 之前（防止 hermes_core 遮蔽本地 config_loader）
_SRC_DIR = str(Path(__file__).resolve().parent)
if _SRC_DIR in sys.path:
    sys.path.remove(_SRC_DIR)
sys.path.insert(0, _SRC_DIR)

# 加入 hermes scripts 路径（环境变量优先）
_HERMES_SCRIPTS = Path(os.environ.get(
    "HERMES_SCRIPTS_DIR",
    str(Path.home() / ".hermes" / "scripts")
))
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(1, str(_HERMES_SCRIPTS))

# 确保 session-launcher/src 路径可用（for sentinel/routine import）

from reliability import (
    LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT,
    with_retry, health_check, start_background_services, stop_background_services,
    reconfigure, reload_config, IDEMPOTENT_CONSUME, OPTIMISTIC_CLAIM, ACK_TRACKER,
    DEFAULT_RETRY, get_last_cursor, set_last_cursor, setup_logging,
)


def _get_retry_policy():
    import reliability as _rel
    return _rel.DEFAULT_RETRY or _rel.RetryPolicy()


def _get_shutdown():
    """Lazy access to GRACEFUL_SHUTDOWN (needs _ensure_initialized first)."""
    import reliability as _rel
    return _rel.GRACEFUL_SHUTDOWN
from config_loader import get_config
from router import get_router, CATEGORY_DESC, priority
from paths import SESSION_LAUNCHER_SRC as _LAUNCHER_SRC
# 延迟导入 launcher.sentinel（避免模块级循环依赖）
_list_sentinels = None
def list_sentinels():
    """读取 CCS 哨兵列表。延迟导入 sentinel（可能不存在于当前 sys.path）。"""
    global _list_sentinels
    if _list_sentinels is None:
        import importlib
        # 确保 session-launcher/src 在 sys.path 中
        _launcher_src = str(_LAUNCHER_SRC)
        if _launcher_src not in sys.path:
            sys.path.insert(0, _launcher_src)
        _sentinel_mod = importlib.import_module("sentinel")
        _list_sentinels = getattr(_sentinel_mod, "list_sentinels", lambda: [])
    return _list_sentinels()


@with_retry(DEFAULT_RETRY)
def poll_unconsumed(category: str | None = None, consumer: str | None = None, instance_id: str = "") -> list[dict]:
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
    router = get_router()
    max_per_poll = get_config().nested_get("bus", "max_messages_per_poll", default=100)
    # 读取持久化 cursor，跳过已处理消息
    since_id = get_last_cursor(consumer, "", instance_id) if consumer else 0

    # 熔断器调用
    def _do_poll():
        return [f for f in bb.unconsumed() if f.id > since_id][:max_per_poll]

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
    if not messages:
        return {"status": "idle", "total": 0}
    if "error" in messages[0]:
        # 区分熔断器错误和其他错误
        return {"status": "error", "total": 0, "error": messages[0].get("error", "unknown")}

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



# ── 路由分发（从 auto_route_routing 导入）──
from auto_route_routing import route_all, route_to_ccs, route_all_to_ccs, dispatch_investigator


# ── CLI 辅助函数 ──────────────────────────────────────────

def _output_json(data, has_json):
    """JSON 或 pretty-print 输出。"""
    if has_json:
        print(json.dumps(data, ensure_ascii=False))
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2))


def _run_daemon():
    """守护模式入口：单实例锁 → 持续轮询。"""
    import signal
    _PID_DIR = Path("/tmp")
    _PID_FILE = _PID_DIR / "session-pipeline-daemon.pid"

    if _PID_FILE.exists():
        try:
            old_pid = int(_PID_FILE.read_text())
            os.kill(old_pid, 0)
            print(f"Daemon already running (PID={old_pid}). Exiting.")
            sys.exit(1)
        except (OSError, ValueError):
            pass
    _PID_FILE.write_text(str(os.getpid()))

    from config_loader import get_config
    cfg = get_config()
    poll_interval = cfg.nested_get("bus", "poll_interval", default=60)
    log_level = getattr(logging, cfg.nested_get("logging", "level", default="INFO"))
    log_json = cfg.nested_get("logging", "json_output", default=True)
    setup_logging(level=log_level, json_output=log_json)

    start_background_services()
    get_router().load_from_db()
    _daemon_instance_id = f"pipeline_{uuid.uuid4().hex[:8]}"
    LOGGER.info(f"Daemon started, poll_interval={poll_interval}s, instance_id={_daemon_instance_id}")
    try:
        while not _get_shutdown()._shutdown:
            reload_config()
            result = route_all(consumer="pipeline", instance_id=_daemon_instance_id)
            if result["routed"] > 0:
                LOGGER.info(f"Routed {result['routed']} messages", extra={"trace_id": "-"})
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        LOGGER.info("Shutting down...")
    finally:
        stop_background_services()
        _PID_FILE.unlink(missing_ok=True)
        LOGGER.info("Daemon stopped")


def _cli_route_all(argv, flags, has_json):
    """--route-all CLI。"""
    consumer = argv[1] if len(argv) > 1 else "pipeline"
    result = route_all(consumer=consumer, dry_run="--dry-run" in flags)
    if has_json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        for d in result["details"]:
            icon = "→" if d.get("assigned") else "✗"
            print(f"  {icon} #{d['id']} [{d['category']}] → {d.get('assigned', 'none')}")
        print(f"\n路由: {result['routed']}/{result['total']} 条")


def _cli_consume(argv, has_json):
    """--consume CLI。"""
    fid = int(argv[1])
    cat = argv[2]
    c = argv[3] if len(argv) > 3 else "claude"
    result = consume_with_linkage(fid, cat, c)
    _output_json(result, has_json)


def _cli_route_to_ccs(argv, dry_run, has_json):
    """--route-to-ccs CLI。"""
    if len(argv) < 2:
        print("Usage: --route-to-ccs <role_name> [--dry-run]", file=sys.stderr)
        sys.exit(1)
    role = argv[1]
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


def _cli_route_all_to_ccs(dry_run, has_json):
    """--route-all-to-ccs CLI。"""
    result = route_all_to_ccs(dry_run=dry_run)
    if has_json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        for rd in result["details"]:
            print(f"  {rd['role']}: {rd['routed']}/{rd['total']} routed")
            for d in rd.get("details", []):
                icon = {"routed": "  →", "skipped": "  ⊘", "error": "  ✗", "dry_run": "  □"}.get(d.get("action"), "  ?")
                print(f"    {icon} #{d['id']} [{d['category']}] {d.get('action', '')} {d.get('reason', '')} {d.get('error', '')}")
        print(f"\n总路由: {result['routed']}/{result['total']} 条 (dry_run={dry_run})")


def _cli_dispatch_investigator(argv, dry_run, has_json):
    """--dispatch-investigator CLI。"""
    cat = argv[1] if len(argv) > 1 else "code_fix"
    result = dispatch_investigator(category=cat, dry_run=dry_run)
    if has_json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        for d in result["details"]:
            icon = {"routed": "→", "dry_run": "□"}.get(d.get("action"), "?")
            print(f"  {icon} #{d['id']} [{d['category']}] → {d.get('assigned_investigator', '?')}")
        print(f"\n分派: {result['dispatched']}/{result['total']} 条")


if __name__ == "__main__":
    import signal
    has_json = "--json" in sys.argv
    argv = [a for a in sys.argv[1:] if a != "--json"]
    flags = set(argv)
    _daemon_flag = "--daemon" in flags
    _health_flag = "--health" in flags
    _status_flag = "--status" in flags
    _metrics_flag = "--metrics" in flags
    _ack_stats_flag = "--ack-stats" in flags
    _ack_retry_flag = "--ack-retry" in flags
    _config_flag = "--config" in flags
    _route_all_flag = "--route-all" in flags
    _consume_flag = "--consume" in flags
    _routetoccs_flag = "--route-to-ccs" in flags
    _routetoccsall_flag = "--route-all-to-ccs" in flags
    _dispatch_flag = "--dispatch-investigator" in flags
    _dry_run = "--dry-run" in flags

    # ── dispatch ──
    if _daemon_flag:
        _run_daemon()
    elif _status_flag:
        _output_json(status(), has_json)
    elif _health_flag:
        _output_json(health_check(), has_json)
    elif _metrics_flag:
        print(METRICS.export_prometheus())
    elif _ack_stats_flag:
        stats = ACK_TRACKER.ack_stats()
        if has_json:
            print(json.dumps(stats, ensure_ascii=False))
        else:
            print(f"ACKs: {stats['total']} total")
            for s, c in sorted(stats["by_status"].items()):
                print(f"  {s}: {c}")
    elif _ack_retry_flag:
        from bus_protocol import Blackboard
        bb = Blackboard()
        retried = ACK_TRACKER.retry_failed(bb)
        print(json.dumps({"retried": retried}, ensure_ascii=False))
    elif _config_flag:
        from config_loader import get_config
        _output_json(get_config().to_dict(), has_json)
    elif _route_all_flag:
        _cli_route_all(argv, flags, has_json)
    elif _consume_flag and len(argv) >= 3:
        _cli_consume(argv, has_json)
    elif _routetoccs_flag:
        _cli_route_to_ccs(argv, _dry_run, has_json)
    elif _routetoccsall_flag:
        _cli_route_all_to_ccs(_dry_run, has_json)
    elif _dispatch_flag:
        _cli_dispatch_investigator(argv, _dry_run, has_json)
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
