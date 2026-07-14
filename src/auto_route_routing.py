#!/usr/bin/env python3
"""auto_route_routing.py — 路由分发逻辑（从 auto_route.py 提取）"""

import json
import os
import sys
import time
import uuid
from pathlib import Path

# Path setup
_HERMES_SCRIPTS = Path(os.environ.get("HERMES_SCRIPTS_DIR", str(Path.home() / ".hermes" / "scripts")))
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(1, str(_HERMES_SCRIPTS))

from config_loader import get_config
from router import get_router, CATEGORY_DESC, priority
from composite_runner import CompositeRunner

from reliability import LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT, GRACEFUL_SHUTDOWN
from reliability import with_retry, DEFAULT_RETRY, get_last_cursor, set_last_cursor
from reliability import IDEMPOTENT_CONSUME, ACK_TRACKER



def route_all(consumer: str = "pipeline", dry_run: bool = False, parallel: bool = True, instance_id: str = "") -> dict:
    """自动路由消费：为每条未消费消息并行分配给多个消费者。

    P1 改进：
    - 每条消息分配给所有应消费的消费者（不仅是第一个）
    - 使用 ThreadPoolExecutor 并行消费
    - 幂等消费防重复
    - ACK 记录和 metrics
    - Fix 6：poll_unconsumed 用 cursor 过滤已处理消息，route_all 结束更新 cursor
    - Fix 7：instance_id 隔离多 pipeline 实例的 cursor

    dry_run=True 时只展示分配方案。
    parallel=True 时使用线程池并行。
    instance_id 为空时自动生成，确保每个 route_all 调用周期有独立 cursor。
    """
    from concurrent.futures import ThreadPoolExecutor
    from bus_protocol import Blackboard

    if not instance_id:
        instance_id = f"pipeline_{uuid.uuid4().hex[:8]}"

    # 延迟导入规避循环依赖（poll_unconsumed 定义在 auto_route.py）
    from auto_route import poll_unconsumed as _poll

    bb = Blackboard()
    router = get_router()
    messages = _poll(consumer=consumer, instance_id=instance_id)
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

            # Fix 6：更新持久化 cursor，重启后不再重复处理已消费的消息
            if details:
                max_id = max(d.get("id", 0) for d in assignments)
                if max_id:
                    set_last_cursor(consumer, "", max_id, instance_id)

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
    from paths import SESSION_LAUNCHER_SRC as _LAUNCHER_SRC
    _launcher_src = str(_LAUNCHER_SRC)
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


def _match_investigator(evidence: str) -> str:
    """根据证据文本匹配 investigator 角色。"""
    if not evidence:
        return "investigator_general"
    low = evidence.lower()
    # 优先匹配 Python 专用关键字
    for kw in _INVESTIGATOR_PYTHON_KEYWORDS:
        if kw in low:
            return "investigator_python"
    # 其次匹配跨栈/复杂关键字
    for kw in _INVESTIGATOR_SENIOR_KEYWORDS:
        if kw in low:
            return "investigator_senior"
    return "investigator_general"


def dispatch_investigator(category: str = "code_fix", dry_run: bool = False) -> dict:
    """自动分派需排查的消息给对应 investigator。

    category: 要检查的 bus 分类（默认 code_fix，也可 architecture）
    dry_run: 仅展示分配方案，不实际路由
    """
    from bus_protocol import Blackboard
    router = get_router()

    bb = Blackboard()
    facts = bb.read(cat=category, limit=50)

    results = []
    for f in facts:
        # 只处理 filter=needs_investigation / needs_triage 的消息
        if "needs_investigation" not in (f.e or "").lower() and "needs_triage" not in (f.e or "").lower():
            continue

        role = _match_investigator(f.e or f.t)
        if dry_run:
            results.append({
                "id": f.id,
                "category": f.cat,
                "text": f.t[:80],
                "evidence_preview": (f.e or "")[:120],
                "assigned_investigator": role,
                "action": "dry_run",
            })
        else:
            # 写 notice 通知对应 investigator
            bb.write("notice",
                f"@{role}: 需要刑侦排查. context: {f.t[:100]}",
                src="auto_route_investigator")
            results.append({
                "id": f.id,
                "category": f.cat,
                "text": f.t[:80],
                "assigned_investigator": role,
                "action": "routed",
            })

    return {
        "dispatched": len([r for r in results if r.get("action") == "routed"]),
        "total": len(results),
        "dry_run": dry_run,
        "details": results,
    }