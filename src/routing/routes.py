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

from routing import router as _rt_mod
from paths import CCS_CLI as _CCS_CLI
from workflow.client import WorkflowClient

from reliability import LOGGER, METRICS, CIRCUIT_BREAKER, HEARTBEAT, set_last_cursor
from reliability import IDEMPOTENT_CONSUME, ACK_TRACKER
from output_validator import validate_output

# 系统内通知分类：这些是路由信号而非任务，创建 workflow 会引发级联风暴
# _SYSTEM_CATEGORIES: 跳过 workflow 创建的分类（路由信号）
# _NON_TASK_CATEGORIES: 跳过路由分发的分类（纯通知/心跳，无任务语义）
_SYSTEM_CATEGORIES = frozenset({"architecture", "notice", "ccs_health", "blocker", "task_spec",
                                       "feedback", "code_review", "verification"})
_NON_TASK_CATEGORIES = frozenset({"notice", "verification+notice", "heartbeat", "metrics", "ccs_send_fallback"})
SYSTEM_CATEGORIES = _NON_TASK_CATEGORIES  # 向后兼容别名

_CONTENT_WORDS = ("修复", "实现", "设计", "调研", "分析", "测试", "审查", "部署", "监控",
                  "开发", "优化", "重构", "排查", "处理", "检查", "升级", "清理", "创建",
                  "fix", "implement", "design", "research", "analy", "test", "review",
                  "deploy", "monitor", "build", "optimi", "refactor", "investigate")

def _has_content_words(title: str) -> bool:
    """标题是否含内容性动词（防 'ok'/'hi'/'x' 这类纯占位标题生成无意义 workflow）。"""
    t = title.lower()
    return any(w in t for w in _CONTENT_WORDS)



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
        instance_id = f"pipeline_{uuid.uuid4().hex[:8]}_{os.getpid()}_{int(time.time())}"

    # 延迟导入规避循环依赖（poll_unconsumed 定义在 auto_route.py）
    from routing.auto import poll_unconsumed as _poll

    bb = Blackboard()
    router = _rt_mod.get_router()
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
                [c for c in consumers if "*" not in router.routing.get(c, {}).get("consume", [])]
                + [c for c in consumers if "*" in router.routing.get(c, {}).get("consume", [])]
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
                        # 消费成功后创建对应角色的工作流（自动推荐模板）
                        # -- 跳过系统内通知类分类，防止级联风暴 --
                        # architecture / notice / ccs_health 是路由信号而非任务，
                        # 为它们创建 workflow 会引发 workflow-engine → blocker 循环。
                        if ack_status == "consumed" and cat not in _SYSTEM_CATEGORIES:
                            if cat in SYSTEM_CATEGORIES:
                                LOGGER.debug("Skipping workflow creation for system category %s", cat)
                            else:
                                _t = assignment.get("title", "")
                                if len(_t) < 8 or not _has_content_words(_t):
                                    LOGGER.debug("Skipping workflow create: title too short or 无内容性 (%d chars) role=%s", len(_t), role)
                                else:
                                    try:
                                        _wc = WorkflowClient(role)
                                        _wc.create_task_v2(_t[:80], assignee=role,
                                                           initiator_role="", bus_category=cat)
                                        _wc.close()
                                    except Exception as _e:
                                        LOGGER.warning("route_all workflow create failed for %s: %s", role, _e)
                        try:
                            set_last_cursor(consumer, "", fid, instance_id)
                        except Exception as _sc_e:
                            LOGGER.debug("set_last_cursor failed for %s fid=%s: %s", consumer, fid, _sc_e)
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


def route_to_ccs(role_name: str, dry_run: bool = False) -> dict: # noqa: C901
    """将指定角色应消费的 bus 消息通过 ccs.py send 推送给对应的 CCS。

    dry_run=True 时只展示分配方案。
    """
    from bus_protocol import Blackboard as _Blackboard
    from routing.auto import poll_unconsumed as _poll

    category_limit = 10

    # 消费该角色未消费的消息
    messages = _poll(consumer=role_name, limit=category_limit)
    if not messages or "error" in messages[0]:
        return {"role": role_name, "routed": 0, "total": 0, "dry_run": dry_run, "details": []}

    # 子进程调 ccs.py 发消息，规避 routing 包名冲突
    _launcher_env = {**os.environ, "PYTHONPATH": str(_CCS_CLI.parent)}

    def _send(role, msg):
        import subprocess as _sp, json as _json
        r = _sp.run([sys.executable, str(_CCS_CLI), "send", "--from", "pipeline", role, msg],
                     capture_output=True, text=True, timeout=15, env=_launcher_env)
        err = ""
        if r.returncode != 0:
            try: err = _json.loads(r.stdout).get("error", r.stderr[:100])
            except Exception: err = r.stderr[:100] or "exit=" + str(r.returncode)
        return {"success": r.returncode == 0, "sent_chars": len(msg), "error": err}

    def _is_running(role):
        import subprocess as _sp
        r = _sp.run([sys.executable, str(_CCS_CLI), "status-role", role],
                     capture_output=True, text=True, timeout=10, env=_launcher_env)
        return "未运行" not in r.stdout

    running = _is_running(role_name) if not dry_run else None
    details = []

    # 忙检测：查角色 DB 是否有活跃工作流（与 _is_role_busy 信号2 同数据源）。
    # 有活跃工作流 → 本轮跳过推送，消息保留在 bus 下轮再试（不消费不丢失）。
    # 角色在 workflow 中工作时收到 bus 消息会打断当前任务（观测：role_drift 18 条逐条
    # 推给 maintainer，AI 处理完每条前又被下一条打断）。
    # DB 查询失败 → 降级为无忙检测（返回 None，与旧行为一致），不阻断路由。
    wf_busy = None
    if running:
        try:
            _wf_probe = WorkflowClient(role_name)
            wf_busy = _wf_probe.check_task() is not None
            _wf_probe.close()
        except Exception:
            wf_busy = None  # DB 不可用 → 不检测，避免误杀路由
    body_facts: list[dict] = []

    for msg in messages:
        if "error" in msg:
            continue
        body = f"[{msg['category']}] {msg['text']}"
        if msg.get("evidence"):
            body += f"\n证据: {msg['evidence']}"
        body_facts.append({"id": msg["id"], "category": msg["category"],
                           "text": body[:500], "consumers": msg.get("consumers", [])})

        if dry_run:
            details.append({"id": msg["id"], "category": msg["category"],
                            "action": "dry_run", "body_preview": body[:100]})
            continue

        if not running:
            details.append({"id": msg["id"], "category": msg["category"],
                            "action": "skipped", "reason": f"CCS {role_name} 未启动"})
            continue

        # 忙检测：角色有活跃工作流 → 跳过推送（消息留 bus 下轮再试）
        if wf_busy:
            details.append({"id": msg["id"], "category": msg["category"],
                            "action": "deferred", "reason": f"CCS {role_name} 工作流进行中"})
            continue

        # Output validation: validate structured JSON content before routing
        try:
            _content_dict = json.loads(msg.get("text", ""))
            _v = validate_output(role_name, msg.get("category", ""), _content_dict)
            if not _v.get("valid") and _v.get("severity") == "error":
                _bb = _Blackboard()
                _bb.write("notice",
                          f"@{role_name} output validation failed: missing fields {_v['missing']}",
                          evidence=f"msg_id={msg['id']} category={msg['category']}")
                details.append({"id": msg["id"], "category": msg["category"],
                                "action": "rejected", "reason": f"validation_error: missing {_v['missing']}"})
                continue
            if not _v.get("valid") and _v.get("severity") == "warning":
                body += "\n(validated: false)"
        except (json.JSONDecodeError, TypeError):
            pass  # must-silent: non-JSON content — skip validation

        result = _send(role_name, body)
        if result.get("success"):
            details.append({"id": msg["id"], "category": msg["category"],
                            "action": "routed", "sent_chars": result.get("sent_chars", 0)})
            # 创建对应角色的工作流实例（按 bus 分类推荐模板）
            _cat = msg.get("category", "")
            if _cat in _NON_TASK_CATEGORIES:
                LOGGER.debug("Skipping workflow creation for system category %s", _cat)
            else:
                try:
                    _raw = msg.get("text", "")
                    # 从消息文本提取有意义的标题：取第一段或前 60 字符
                    _title = _raw.split("\n")[0] if _raw else ""
                    _title = _title[:80]
                    if len(_title) < 4:
                        LOGGER.debug("Skipping workflow create: title too short (%d chars) role=%s", len(_title), role_name)
                    else:
                        _wf = WorkflowClient(role_name)
                        _wf.create_task_v2(_title, assignee=role_name, initiator_role="",
                                           bus_category=_cat)
                        _wf.close()
                except Exception as _e:
                    LOGGER.warning("route_to_ccs workflow create failed for %s: %s", role_name, _e)
        else:
            details.append({"id": msg["id"], "category": msg["category"],
                            "action": "failed", "error": result.get("error", "unknown")})

    result = {"role": role_name, "routed": sum(1 for d in details if d["action"] == "routed"),
              "total": len(messages), "dry_run": dry_run, "details": details}
    METRICS.inc("route_to_ccs_count", labels={"role": role_name})
    return result

def route_all_to_ccs(dry_run: bool = False) -> dict:
    """对所有有未消费消息的角色，并行调用 route_to_ccs()。

    dry_run=True 时只展示分配方案。
    """
    from concurrent.futures import ThreadPoolExecutor

    router = _rt_mod.get_router()
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


_INVESTIGATOR_PYTHON_KEYWORDS = ["python", "traceback", "importerror", "typeerror",
    "valueerror", "keyerror", "attributeerror", "indentationerror", "modulenotfounderror",
    "zerodivisionerror", "filenotfounderror", "jsondecode", "unicodedecode",
    "sqlite3", "sqlalchemy", "django", "flask", "fastapi", "pytest", "unittest",
    ".py", "requirements.txt", "pip install", "poetry", "pipenv", "setup.py",
    "pyproject.toml", "site-packages", "egg-info", "__pycache__", ".pyc"]

_INVESTIGATOR_SENIOR_KEYWORDS = ["crash", "segfault", "oom", "deadlock", "race",
    "memory leak", "file descriptor leak", "socket hang", "connection refused",
    "timeout", "corruption", "data loss", "inconsistency", "infinite loop",
    "stack overflow", "concurrent", "mutex", "semaphore", "thread safe",
    "distributed", "cascade", "thundering herd", "split brain",
    "replication lag", "quorum", "consensus", "raft", "paxos",
    "kernel", "systemd", "cgroup", "namespace", "overlayfs", "cni",
    "iptables", "ebpf", "tcpdump", "tcp", "udp", "dns", "tls", "ssl",
    "webrtc", "sip", "rtp", "h264", "h265", "opus", "ffmpeg", "gstreamer",
    "cross-platform", "cross-architecture", "endian", "alignment", "mmap",
    "dma", "interrupt", "firmware", "driver", "kernel module"]


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
    bb = Blackboard()
    # codex CL-1 修复: read 不过滤已消费 → 改用 unconsumed，mark_consumed 才有闭环意义
    facts = bb.unconsumed(cat=category, consumer="auto_route_investigator")[:50]

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
            # codex D20 修复: 分派后 mark_consumed，防止下轮重复分派同一批
            # （dispatch 周期无 cursor 推进，不消耗则永久重复）
            # P1-C 修复: mark_consumed 失败时重试一次并写 blocker，防止消息丢失
            try:
                bb.mark_consumed(f.id, consumer="auto_route_investigator")
            except Exception as _mc:
                LOGGER.warning("dispatch mark_consumed 失败(首次) fid=%s: %s", f.id, _mc)
                try:
                    bb.mark_consumed(f.id, consumer="auto_route_investigator")
                except Exception as _mc2:
                    LOGGER.error("dispatch mark_consumed 失败(重试) fid=%s: %s, 消息可能丢失", f.id, _mc2)
                    # blocker 写入本身必须防崩：DB 已故障时 write 也会抛，
                    # 未保护会中断整个 dispatch 循环，丢失剩余消息（codex R3 P1）
                    try:
                        bb.write("blocker",
                            f"mark_consumed 失败: fid={f.id} 消息可能重复分派或丢失",
                            evidence=str(_mc2)[:200], src="auto_route_investigator")
                    except Exception:
                        LOGGER.error("blocker 写入也失败 fid=%s（DB 故障）", f.id)
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