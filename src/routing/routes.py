#!/usr/bin/env python3
"""auto_route_routing.py — 路由分发逻辑（从 auto_route.py 提取）"""

import json
import os
import re
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
# 产出类分类：承载的是**已完成产出**的回报（PRD/设计/测试报告/部署结果），
# 不是待办请求。缺它们时完成汇报被 mint 成新工作流 —— 实测 product_architect
# 的 prd/system_design 两条完成信号各生成一个 echo pipeline（wf_6a9ad42282 /
# wf_3366899dfe，2026-10-19）。
# 判定依据是 bus_canonical.json 的 description：这些分类描述为「XX 文档/报告/结果」，
# 即角色产出的落盘通知；「问题/发现/建议」类（bug_report/design_issue/optimization）
# 不在此列——它们需要后续动作。
_OUTPUT_CATEGORIES = frozenset({
    "prd", "system_design", "documentation", "test_report", "deployment_report",
    "test_plan", "deployment_plan", "root_cause_analysis", "vuln_report",
    "tech_decision", "sprint_report", "evolution_report", "changelog",
    "reflexion_lesson", "skill_audit", "report", "knowledge_distill", "memory_store",
})

_SYSTEM_CATEGORIES = frozenset({"architecture", "notice", "ccs_health", "blocker", "task_spec",
                                       "feedback", "code_review", "verification",
                                       # scheduler: 定时器心跳，206 条中 196 条同文（95% 重复），
                                       # 建 workflow 只会产出同标题空任务。session_log: 角色
                                       # 自述汇报，非工作请求。二者仍正常路由给角色，仅不建任务。
                                       # monitor_audit: 引擎自身的审计输出（如“决策: 轮次追踪超时”），
                                       # 建 workflow 会形成引擎写审计→建 wf→引擎又写审计的自环
                                       # （实测 2026-10-17：标题内秒数变化逃过唯一索引，每 5 分钟重建一批）。
                                       #
                                       # code_fix（2026-10-17 补）：它承载的是**已完成事件**的回报
                                       # （"[deploy] wf_x 产出已部署"、"CODE_FIX: wf_x/sN ... 已落地并验证"），
                                       # 不是待办请求。缺它导致 24h 内 99 条 [deploy] 回报被 mint 成新任务
                                       # 派给 maintainer（maintainer→maintainer 自喂环 80 实例）。
                                       #
                                       # security_audit（2026-10-19 补）：同 code_fix 的形态，承载的是
                                       # **已完成事件**的回报（"security_audit_scan/s2-complete —
                                       # 漏洞分析完成"）。“分析”在内容词表里 → 被判成待办请求。
                                       # 实测 24h 内 39 条此类回报被 mint 成任务，均因 client.py
                                       # 的 Gate 顺序缺陷落库即孤儿（task_aff8dbb4 等）。
                                       "scheduler", "session_log", "monitor_audit", "code_fix", "security_audit"}) | _OUTPUT_CATEGORIES
_NON_TASK_CATEGORIES = frozenset({"notice", "verification+notice", "heartbeat", "metrics", "ccs_send_fallback"})
SYSTEM_CATEGORIES = _NON_TASK_CATEGORIES  # 向后兼容别名

_CONTENT_WORDS = ("修复", "实现", "设计", "调研", "分析", "测试", "审查", "部署", "监控",
                  "开发", "优化", "重构", "排查", "处理", "检查", "升级", "清理", "创建",
                  "fix", "implement", "design", "research", "analy", "test", "review",
                  "deploy", "monitor", "build", "optimi", "refactor", "investigate")

# 模块头注释里已声明的「问题/发现/建议」类：内容本身即待办，不靠动词判定。
_CONTENT_SIGNAL_CATEGORIES = frozenset({"bug_report", "design_issue", "optimization"})

def _has_content_words(title: str, category: str = "") -> bool:
    """标题是否含内容性动词（防 'ok'/'hi'/'x' 这类纯占位标题生成无意义 workflow）。

    内容信号分类（bug_report/design_issue/optimization）豁免：它们的标题本来就是
    「问题描述」，很少带「修复/排查」等动词。实测 26 条 qa 缺陷报告（如
    #423275 "state_dir 仍取自计划"、#422911 "引擎链串味"）因无动词被此门
    静默丢弃，与 _is_role_report_title 是同一个丢弃面，需一并修。
    """
    if category in _CONTENT_SIGNAL_CATEGORIES:
        return True
    t = title.lower()
    return any(w in t for w in _CONTENT_WORDS)


# 只匹配"机器标签"形态：纯小写字母/数字/下划线/连字符（engineer、ccs-monitor、
# self-reflect、test_role_interact_0d76d4）。**不收中文**——`[紧急] 修复X`、
# `[重要] ...` 是合法任务标签，收了会把它们静默丢弃（无 bus 痕迹、无法重试）。
# 中文自述标签（[修复]/[闭环缺口]）靠 client.py 的跨角色去重兜底。
_ROLE_REPORT_RE = re.compile(r"^\[([a-z0-9_\-]+)\]")



def _is_role_report_title(title: str, category: str = "", src: str = "") -> bool:
    """[标签] 开头的 title 是否为产出汇报（拦截建 workflow）。

    WHY: 角色完成后写 bus 的汇报（`[engineer] s1-complete`、`[optimizer] 本轮...`）
    若不拦，会被 fan-out 成 7~9 个角色各自的新 workflow（实测 92/92 全中）。

    判据：标签复用总线分类名（`[bug_report] cat=bug_report`）的是**内容型待办**，
    必须放行——否则真缺陷报告被静默吞掉、无人接手（#421072/#421074）；
    标签与分类不一致的（`[step_output] cat=workflow`、`[optimizer] cat=optimization`）
    是汇报/审计。引擎自写的一律拦，防引擎写审计→建任务→再写审计的自喂环。
    """
    m = _ROLE_REPORT_RE.match(title or "")
    if not m:
        return False
    if src == "workflow_engine":
        return True
    return m.group(1) != category


# 对「已存在工作流」的事件汇报：wf_4f3a... / s3-complete / xxx_complete。
# 这类标题描述的是**已发生的事**，不是要让谁去做什么。不拦则每个消费者角色
# 各自建一条完整链——7 天 909 个实例中 372 个此类，265 条直接作废
# （WL-01 106 / consumer_processing 51 / maintainer_health_monitor 47）。
# 不按 cat 豁免：`[bug_report] wf_x/s4 审批密钥不可达` 也是汇报，原消息仍
# 正常路由给角色，只不 mint 新链。
# `/s2-complete` 必须带斜杠：不带斜杠时 "请新增 s3-complete 状态的审计脚本"
# 这类**真实需求**会被误杀（实测 1 条）。带斜杠的 154/364 条全是汇报，
# 误判方向不对称——漏判只多建一条链，误杀则丢掉用户的活。
_WF_ECHO_RE = re.compile(
    r"wf_(?:sub_|rw_)?[0-9a-f]{5,}|/\s*s\d+\s*[-_]?\s*complete", re.I)


def _is_wf_echo_title(title: str) -> bool:
    return bool(_WF_ECHO_RE.search(title or ""))


def _is_self_echo(msg: dict, role: str) -> bool:
    """该消息由 role 自己写 —— 不派回给 role。

    WHY: 回执/完成汇报是"我已处理"的状态，不是任务请求。bus 的 rc 字段已
    原生追踪消费（consume 时 rc+1），无需再写消息。实测 2026-10-17 01 时段：
    consumer 回执被自身反复消费，30 分钟 bus 写 1789 条，正反馈放大。
    对应 RFC 3834：自动回执不应再触发回执。
    """
    src = msg.get("src", "")
    return bool(src) and src == role


def _is_noop_report(title: str, evidence: str = "") -> bool:
    """no-op 健康报告判据（DESIGN §7 S3）：措辞命中 → 不 mint 工作流。

    maintainer 服务健康时产出 `无职责内修复动作...` 报告，被路由 fan-out 成
    新 WL-01 实例（要求为"什么都不用做"写技术方案）。判据委托给
    cron_scheduler._is_noop_report，措辞表单一来源，避免两处漂移。
    真修复报告携带变更 delta 措辞 → 不命中 → 正常 mint。
    """
    try:
        from cron_scheduler import _is_noop_report as _f
        return _f(title, evidence)
    except Exception as _e:
        LOGGER.debug("noop 判据不可用（放行）: %s", _e)
        return False



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
        import re as _re
        for msg in messages:
            if "error" in msg:
                continue
            consumers = msg.get("consumers", [])
            # @role 定向路由：@target 的消息只给 target，其他角色跳过
            _tags = _re.findall(r"@([a-zA-Z_][a-zA-Z0-9_-]*)", msg.get("text", ""))
            if _tags:
                consumers = [c for c in consumers if c in _tags]
            # 回声抑制：消息由某角色自己写 → 不派回给该角色（RFC 3834）。
            # 回执/完成汇报是状态而非任务，派回自己只会自激放大。
            ordered = (
                [c for c in consumers if "*" not in router.routing.get(c, {}).get("consume", [])
                 and not _is_self_echo(msg, c)]
                + [c for c in consumers if "*" in router.routing.get(c, {}).get("consume", [])
                   and not _is_self_echo(msg, c)]
            )
            if not ordered:
                METRICS.inc("route_orphan_count")
                details.append({"id": msg["id"], "category": msg["category"], "assigned": [], "reason": "no_consumer"})
                continue

            assignments.append({
                "id": msg["id"],
                "category": msg["category"],
                # 与 route_to_ccs 同口径（首行 + 80 上限）：两条路径对同一条
                # bus 消息必须产出相同 title，否则 client.create_task_v2 的
                # 按 title 去重会漏掉跨路径重复（60 vs 80 截断不等）
                "title": (msg["text"] or "").split("\n")[0][:80],
                # 正文作 description 落库：原实现只传 title，角色收到的任务描述恒为空
                # （实测 WL-full_dev_pipeline 47/48、WL-01 194/194 为空）→ prompt 只有一行
                # 标题，角色无法知道需求原意。截断 2000 防撑爆 prompt。
                "body": (msg["text"] or "")[:2000],
                "priority": msg["priority"],
                "src": msg.get("src", ""),
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
                                if len(_t) < 8 or not _has_content_words(_t, cat):
                                    LOGGER.debug("Skipping workflow create: title too short or 无内容性 (%d chars) role=%s", len(_t), role)
                                elif _is_role_report_title(_t, cat, assignment.get("src", "")):
                                    # [角色名] 开头的 title 是"某角色的产出汇报"，不是"给某角色的任务"。
                                    # 实测 72h 内 92/92 个 [角色名] 开头实例都是 fan-out（同 title 多角色），
                                    # 100% 是噪音。不创建 workflow。
                                    # INFO 而非 DEBUG：拦截会改变投递行为，需在 journalctl 可见，
                                    # 否则若某个合法任务被误判，排查时无迹可寻。
                                    LOGGER.info("Skipping workflow create: role report title (%s) role=%s", _t[:40], role)
                                elif _is_wf_echo_title(_t):
                                    # 事件汇报而非新需求：原消息已正常路由给角色，
                                    # 再 mint 新链只会派生同标题空任务（7 天 372 条）。
                                    LOGGER.info("Skipping workflow create: wf echo title (%s) role=%s", _t[:40], role)
                                elif _is_noop_report(_t, assignment.get("body", "")):
                                    # DESIGN §7 S3：no-op 健康报告是状态而非任务请求，
                                    # mint 出"为无需动作写方案"的空转实例。INFO 可见。
                                    # 判据用 body：assignment 从未带 evidence 键，
                                    # 原来取 evidence 恒为 "" → 只能靠标题命中，永远晚半拍。
                                    LOGGER.info("Skipping workflow create: no-op report (%s) role=%s", _t[:40], role)
                                else:
                                    try:
                                        _wc = WorkflowClient(role)
                                        _wc.create_task_v2(_t[:80], assignee=role,
                                                           initiator_role="system", bus_category=cat,
                                                           description=assignment.get("body", ""))
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
    raw_msgs = _poll(consumer=role_name, limit=category_limit * 3)
    # 定向过滤：@target_role 的消息只给 target role，其他角色跳过不消费
    import re as _re
    # 容错：get_router() 在测试中被 mock 成 MagicMock 时 routing 是 MagicMock，
    # .get(...) 返回 MagicMock 而非 list，in/not in 判断行为不可靠 → 显式校验类型，
    # 非 list 时退化为"不过滤"，保持旧行为（与未加过滤前一致）。
    try:
        _router_d = _rt_mod.get_router().routing
        # "登记了但订阅为空" ≠ "没配置"：optimizer / ccs-monitor 在 routing.db 里
        # consume=[] 是有意的（它们不该收未定向的 bus 流），旧写法把空表当无配置
        # → 每条未定向消息都投给它们（实测 pipeline→optimizer 2175 条/天、
        # →ccs-monitor 1757 条/天，全是 monitor_audit/verification 这类审计流）。
        _configured = isinstance(_router_d, dict) and role_name in _router_d
        _my_cats = _router_d.get(role_name, {}).get("consume", []) if _configured else []
        if not isinstance(_my_cats, list):
            _my_cats = []
    except Exception:
        _configured = False
        _my_cats = []
    def _is_for_me(msg):
        # 回声抑制：role 自己写的消息不推回给它自己（与 route_all 同语义）。
        if _is_self_echo(msg, role_name):
            return False
        text = msg.get("text", "")
        tags = _re.findall(r"@([a-zA-Z_][a-zA-Z0-9_-]*)", text)
        directed = role_name in tags
        # 定向消息绕过分类过滤：有人明确 @ 你，比你的默认订阅列表优先级高。
        # 实测 24h 内 62 条 @engineer 有 26 条（42%）因分类不在 consume 列表被丢，
        # 其中 notice×13、blocker×4；两条独立的角色复核各自查到同一处。
        # 这条过滤本意是防止「blocker 被广播给全部 32 个角色」，定向消息不会广播。
        #
        # 唯一豁免 task_spec：它是「派发记录」而非「找人办事」，三个写入者
        # （launcher client:229 / pipeline client:247 / role_lifecycle:958）写完都
        # 自己 ccs send 正文，路由再投一遍就是双投。实测 product_architect 存量
        # 528 条 @它 的 task_spec 中位年龄 100h、最老 111h，全是引擎早已投过的重放。
        if directed and msg.get("category") == "task_spec":
            return False
        if (not directed and "*" not in _my_cats
                and msg.get("category") not in _my_cats
                and (_configured or _my_cats)):
            return False
        return not tags or directed
    messages = [m for m in raw_msgs if _is_for_me(m)]
    if not messages or "error" in messages[0]:
        return {"role": role_name, "routed": 0, "total": 0, "dry_run": dry_run, "details": []}

    # 子进程调 ccs.py 发消息，规避 routing 包名冲突
    _launcher_env = {**os.environ, "PYTHONPATH": str(_CCS_CLI.parent)}

    def _send(role, msg):
        import subprocess as _sp, json as _json
        r = _sp.run([sys.executable, str(_CCS_CLI), "send", "--from", "pipeline", role, msg],
                     capture_output=True, text=True, timeout=15, env=_launcher_env)
        err = ""
        reason = ""
        if r.returncode != 0:
            try:
                _out = _json.loads(r.stdout)
                err = _out.get("error", r.stderr[:100])
                reason = _out.get("reason", "")  # busy 需上传给调用方区分排队/故障
            except Exception:
                err = r.stderr[:100] or "exit=" + str(r.returncode)
        return {"success": r.returncode == 0, "sent_chars": len(msg),
                "error": err, "reason": reason}

    def _is_running(role):
        import subprocess as _sp, json as _json
        r = _sp.run([sys.executable, str(_CCS_CLI), "status-role", role],
                     capture_output=True, text=True, timeout=10, env=_launcher_env)
        # status-role 输出 JSON("alive": false)，不再含字面串"未运行"；
        # 原字符串匹配恒为 True → 离线角色被误判存活 → 每周期写 fallback 通知死循环。
        try:
            return bool(_json.loads(r.stdout).get("alive", False))
        except (_json.JSONDecodeError, ValueError):
            return "未运行" not in r.stdout  # 兼容旧输出格式

    running = _is_running(role_name) if not dry_run else None
    details = []

    # 忙检测：查角色 DB 是否有活跃工作流（与 _is_role_busy 信号2 同数据源）。
    # 有活跃工作流 → 本轮跳过推送，消息保留在 bus 下轮再试（不消费不丢失）。
    # 角色在 workflow 中工作时收到 bus 消息会打断当前任务（观测：role_drift 18 条逐条
    # 推给 maintainer，AI 处理完每条前又被下一条打断）。
    # DB 查询失败 → 降级为无忙检测（返回 None，与旧行为一致），不阻断路由。
    wf_busy = None
    if running:
        _wf_probe = None
        try:
            _wf_probe = WorkflowClient(role_name)
            wf_busy = _wf_probe.check_task() is not None
        except Exception:
            wf_busy = None  # DB 不可用 → 不检测，避免误杀路由
        finally:
            if _wf_probe is not None:
                try:
                    _wf_probe.close()
                except Exception:
                    pass  # 关闭失败不掩盖主逻辑
    body_facts: list[dict] = []

    for msg in messages:
        if "error" in msg:
            continue
        body = msg.get("text") or ""
        _tag = f"[{msg['category']}]"
        # 幂等：总线 fact 的 text 多数已自带 [category] 前缀，再拼一次会变成
        # 「[test_report] [test_report] …」，角色读到的任务本身就是脏的。
        if not body.startswith(_tag):
            body = f"{_tag} {body}"
        _ev = (msg.get("evidence") or "").strip()
        # 同理：feed/bus 写入方的 evidence 常已带「证据: 」，再拼一次会出现
        # 「证据: 证据: …」（实测 consumer pane）。
        if _ev and not _ev.startswith("证据:"):
            body += f"\n证据: {_ev}"
        elif _ev:
            body += f"\n{_ev}"
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
            # 投递成功即标记已消费，防下轮重投同一批。
            # route_all 走 cursor 推进，route_to_ccs 无 cursor，不标记则永久重复：
            # 实测 15 条 P0 blocker 因 rc=0 被无限重投（每 5 分钟一轮）。
            try:
                _Blackboard().mark_consumed(msg["id"], consumer=role_name)
            except Exception as _mc:
                LOGGER.warning("route_to_ccs mark_consumed 失败 fid=%s role=%s: %s",
                               msg["id"], role_name, _mc)
            # 创建对应角色的工作流实例（按 bus 分类推荐模板）
            _cat = msg.get("category", "")
            # 此处判定"是否建 workflow"，route_all 用 _SYSTEM_CATEGORIES。
            # 原用 _NON_TASK_CATEGORIES（其职责是跳过路由分发）会漏掉
            # architecture/ccs_health/verification 等 7 个纯信号分类；
            # 但该集合独有 heartbeat/metrics/ccs_send_fallback 也须继续跳过，故取并集。
            if _cat in (_SYSTEM_CATEGORIES | _NON_TASK_CATEGORIES):
                LOGGER.debug("Skipping workflow creation for system category %s", _cat)
            else:
                try:
                    _raw = msg.get("text", "")
                    # 从消息文本提取有意义的标题：取第一段或前 60 字符
                    _title = _raw.split("\n")[0] if _raw else ""
                    _title = _title[:80]
                    # 阈值与 route_all 对齐（len<8 或 无内容词）: 同消息两路径判定一致
                    if len(_title) < 8 or not _has_content_words(_title, _cat):
                        LOGGER.debug("Skipping workflow create: title too short or 无内容性 (%d chars) role=%s", len(_title), role_name)
                    elif _is_role_report_title(_title, _cat, msg.get("src", "")):
                        # 与 route_all 对齐：[角色名] 开头是产出汇报，不是派发任务。
                        # 缺此判定时同一份完成汇报被 fan-out 成 7~9 个角色各自的新 workflow。
                        LOGGER.debug("Skipping workflow create: role report title (%s) role=%s", _title[:40], role_name)
                    elif _is_wf_echo_title(_title):
                        # 与 route_all 对齐：事件汇报不 mint 新链。
                        LOGGER.debug("Skipping workflow create: wf echo title (%s) role=%s", _title[:40], role_name)
                    elif _is_noop_report(_title, _raw):
                        # 与 route_all 对齐（DESIGN §7 S3）：no-op 健康报告不 mint。
                        LOGGER.debug("Skipping workflow create: no-op report (%s) role=%s", _title[:40], role_name)
                    else:
                        _wf = WorkflowClient(role_name)
                        _wf.create_task_v2(_title, assignee=role_name,
                                           initiator_role="system", bus_category=_cat,
                                           description=_raw[:2000])
                        _wf.close()
                except Exception as _e:
                    LOGGER.warning("route_to_ccs workflow create failed for %s: %s", role_name, _e)
        else:
            details.append({"id": msg["id"], "category": msg["category"],
                            # busy 不是投递失败：消息未发送、也未标记消费，
                            # 下轮重推同一 id。分开记录才能在监控里区分排队与故障。
                            "action": "deferred" if result.get("reason") == "busy" else "failed",
                            "error": result.get("error", "unknown")})

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
