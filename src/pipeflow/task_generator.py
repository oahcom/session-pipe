#!/usr/bin/env python3
"""TaskGeneratorMixin — 任务扫描、异常检测、动态任务生成、角色可分配检测。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import time
import uuid
import logging
import hashlib
import threading
import re  # 修复: _scan_tasks 用 re.match 但漏 import, 导致每 tick NameError 使任务生成静默死亡
from pathlib import Path
from types import SimpleNamespace

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效

LOGGER = logging.getLogger("workflow.engine")


def _event_key(title: str, assignee: str, template_id: str) -> str:
    """事件去重键（DESIGN §2.1）：同一 标题|角色|模板 24h 内只 mint 一个任务实例。

    与既有 5-min 精确标题去重（workflow/client.py create_task_v2）互补：
    标题常内嵌 wf 实例号，精确匹配恒不命中，hash 键把语义同一的事件收敛。
    """
    return hashlib.sha256(f"{title}|{assignee}|{template_id}".encode()).hexdigest()[:16]


# per-role running 上限（DESIGN §2.2 backpressure 水位线）。
MAX_RUNNING_PER_ROLE = 3

# 测试 harness 模板名前缀。**不用 `.*_test`**：它会命中 qa_test_* 等生产模板
# （实测 qa_test_execute/qa_test_plan 被误判为 harness → 超时即静默 cancel，
# 且被 full_dev_pipeline/architect_review/food_chain_loop 作为 subflow 引用）。
# 新增测试模板请以 test_ 或 drill 开头，或加入本元组。
_TEST_HARNESS_PREFIXES = ("test_", "exit_schema", "role_interact", "drill")

# engine 注册 JSON 模板时用 "WL-" + name 生成 workflow_id（engine.py:316），于是同一个
# harness 模板在 DB/内存里有两个 id：`test_x` 与 `WL-test_x`。只堵不带前缀的那个等于
# 没堵：WL-test_exit_schema 逃过本判据 → 自动选择把它排在真实模板之前 → 路由 daemon
# 把真任务的标题套上测试模板 → 产出回执又是新的 code_fix 消息 → 自激循环，无外部
# 输入也能永转（2026-10-17 实测：177 个实例、159 条 bus fact、12 个角色被派）。
_WORKFLOW_ID_PREFIXES = ("WL-", "WF-")


def _strip_wf_prefix(template_id: str) -> str:
    """剥离 engine 生成 id 时的 WL-/WF- 前缀（str.removeprefix 不接受元组）。"""
    for p in _WORKFLOW_ID_PREFIXES:
        if template_id.startswith(p):
            return template_id[len(p):]
    return template_id


def _is_test_harness_template(template_id: str | None) -> bool:
    """该模板是否为测试 harness（不参与真实生产派发/超时取消）。"""
    if not template_id:
        return False
    return _strip_wf_prefix(template_id).startswith(_TEST_HARNESS_PREFIXES)


# 子链驳回判据。子链末步驳回不拦（wf_lifecycle.py:302「末步仍不拦」是有意设计：
# 无 next_step 可退回、也无返工承接方，硬拦即静默卡死），于是子链仍报 completed
# → 本文件回流处把父步标 completed → 驳回信号在父链彻底丢失。
# 实测 3 例（wf_sub_9e3d0b017f29 / wf_sub_9b5329785ab9 / wf_sub_fda79b387821）：
# 子链 s1 文案为 REJECTED / CHANGES_REQUESTED，父链 wf_5412acef35 照常推进到 s5/s6。
# 词表与 wf_lifecycle.py:312、lifecycle/manager.py 的门禁保持三方一致。
_REJECT_KEYWORDS = ("changes_requested", "rejected", "需修复", "回退",
                   "未通过", "not_approved", "驳回")


def _subflow_verdict_rejected(sr: dict) -> bool:
    """子链 step_results 的 exit_messages 是否含驳回文案（即其 completed 掩盖了驳回）。"""
    _txt = " ".join(
        str(m.get("text", ""))
        for _sd in sr.values() if isinstance(_sd, dict)
        for m in (_sd.get("exit_messages") or []) if isinstance(m, dict)
    ).lower()
    return any(k in _txt for k in _REJECT_KEYWORDS)



class TaskGeneratorMixin:
    """WorkflowEngine 的任务生成 mixin。"""

    _TASK_GEN_COOLDOWN: dict[str, float] = {}  # role → last gen ts
    # check-then-act 竞态防护：cooldown 检查与写入之间加锁（GIL 下结构不损坏，仅语义竞态）
    _TASK_GEN_LOCK = threading.Lock()
    # 积压告警冷却（DESIGN §2.6 S4）：同一角色积压告警 30min 内只发一次。
    # 无冷却时每个 run_once tick 对同一积压角色重发同一条 → curator 实测 3 天 106 条
    # 重复告警（8 角色各 4-19 条），把 architecture 类淹没成噪音。只告警一次不丢信息量。
    _BACKLOG_ALERT_TS: dict[str, float] = {}
    _BACKLOG_ALERT_COOLDOWN_S = 1800

    def _parent_contract_errs(self, conn, wf_id: str, step_id: str) -> list:
        """父步（handoff 容器）自身 exit_schema 尚未满足的项。

        子链完成不代父步履行契约：子链模板与父模板是两个独立文件，父步要的
        产物子链未必产出。校验失败返回空表（守卫自身不得成为新的卡死源）。
        """
        try:
            _row = conn.execute(
                "SELECT template_id, created_at, effective_roles, step_results "
                "FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
            if not _row:
                return []
            _wf = (self._workflows.get(_row["template_id"])
                   or self._workflows.get(f"WF-{_row['template_id']}"))
            _st = next((s for s in (_wf.steps if _wf else []) if s.id == step_id), None)
            if not _st or not _st.exit_schema:
                return []
            # effective_roles 一并带上：等价角色分流后产出落在实际执行者目录
            _run = SimpleNamespace(
                id=wf_id, created_at=_row["created_at"],
                effective_roles=_row["effective_roles"],
                step_results=json.loads(_row["step_results"] or "{}"))
            ok, errs = self._validate_exit_schema(_st, _run)
            return [] if ok else errs
        except Exception as _e:
            LOGGER.debug("parent contract check failed %s/%s: %s", wf_id, step_id, _e)
            return []

    def _scan_tasks(self):
        lm = self._lifecycle
        try:
            lm.ping()
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("lifecycle ping failed during _scan_tasks")
        deferred: list = []
        try:
            with lm._conn_tx() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT t.task_id, t.status FROM tasks t "
                    "JOIN workflow_instances wi ON t.task_id = wi.task_id "
                    "WHERE t.status NOT IN ('completed', 'failed', 'cancelled', 'step_done_ready')"
                ).fetchall()
                for row in rows:
                    task_id = row["task_id"]
                    inst_rows = conn.execute(
                        "SELECT status FROM workflow_instances WHERE task_id=?",
                        (task_id,)
                    ).fetchall()
                    statuses = [dict(r)["status"] for r in inst_rows]
                    if not statuses:
                        continue
                    if all(s in ("completed", "failed", "cancelled") for s in statuses):
                        ts = "completed" if all(s == "completed" for s in statuses) else "failed"
                        conn.execute(
                            "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
                            (ts, time.time(), task_id))
                        conn.commit()
                # 子工作流完成 → 推进父工作流
                # 子链完成 → 推进父链。这是唯一的回流通道，它此前用
                # `template_id IN (SELECT template_id FROM workflow_templates)` 做过滤，
                # 而两侧键空间不一致：子链实例写的是**裸名**（tgt_step.subflow_template，
                # 见 wf_lifecycle._subflow_takeover），而 workflow_templates 里注册的是
                # **WF- 前缀名**（engine 启动 upsert 时 `tid = 'WF-' + tid`）。
                # 裸名不在子查询结果里 → 6/7 条子链被静默排除 → 子链建得出、也跑得动，
                # 但父链永远收不到完成信号（实测 food_chain_loop s2 是稳定死点，
                # 历史 12 个实例 s2 从未突破，唯一能做到 s7 的是旧模板版本）。
                # 修法：两侧都剥掉 WF- 前缀再比，兼容两种命名，不改写入端（存量实例
                # 已经是裸名，改消费端可同时覆盖存量与新增）。
                for _sub_row in conn.execute(
                    "SELECT wi.instance_id, wi.current_step_id, wi.step_results FROM workflow_instances wi "
                    "WHERE wi.status='running' AND REPLACE(wi.template_id,'WF-','') IN "
                    "(SELECT REPLACE(template_id,'WF-','') FROM workflow_templates)"
                ).fetchall():
                    _sr = json.loads(_sub_row["step_results"] or "{}")
                    for _step_id, _sdata in _sr.items():
                        if not isinstance(_sdata, dict):
                            continue  # 简写值（如 "done"）没有 subflow_id
                        _sf = _sdata.get("subflow_id", "")
                        if _sf:
                            # 只处理当前步骤的 subflow：历史步骤的 subflow 已完成会重复
                            # 触发 _advance_production_wf，而 advance 又为带 subflow_template
                            # 的下一步创建新 subflow → 每 tick 新建 → 垃圾任务堆积
                            if _step_id != _sub_row["current_step_id"]:
                                continue
                            _sub_status = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                            # 子链跑完 ≠ 父步交付契约达成：父步自己的 exit_schema 才是这一
                            # 阶段的交付物清单，而子链模板未必产出其中每一项。实测
                            # wf_26f108c663e4/s2（architect_review 要 REVIEW.md，子链
                            # architect_full_design 从不产出该文件）子链全绿、REVIEW.md 从未
                            # 落盘，父步照样 completed。全历史 161 个「有 schema 且已推进」的
                            # 父步中 23 个契约未达成 —— 全部静默。
                            if _sub_status and _sub_status['status'] in ('completed', 'step_done_ready'):
                                # 幂等：本步已记过账就不再重复写 bus（每 tick 重扫同一子链）
                                if not _sdata.get("contract_unmet"):
                                    _cerrs = self._parent_contract_errs(conn, _sub_row["instance_id"], _step_id)
                                    if _cerrs:
                                        # 与 _subflow_verdict_rejected 同一处置：不改完成语义
                                        # （硬拦会制造静默卡死），只记账 + 写 bus 让缺失可观测。
                                        _sdata["contract_unmet"] = _cerrs
                                        _sdata["contract_unmet_at"] = time.time()
                                        try:
                                            self._bb.write(
                                                "blocker",
                                                f"[workflow] {_sub_row['instance_id'][:12]}/{_step_id} "
                                                f"子链完成但父步交付契约未达成",
                                                src="workflow_engine",
                                                evidence=(f"子链 {_sf[:12]} status={_sub_status['status']}，"
                                                          f"但父步 exit_schema 未满足: {'; '.join(_cerrs[:5])}。"
                                                          f"父链仍按 completed 推进，该步产出实际缺失，"
                                                          f"请核查父模板 schema 与子链模板产出是否对齐。"))
                                            LOGGER.warning("父步契约未达成 %s/%s: %s",
                                                           _sub_row["instance_id"][:12], _step_id, _cerrs[:3])
                                        except Exception as _cwe:
                                            LOGGER.debug("contract-unmet blocker 写入失败: %s", _cwe)
                                _sdata["status"] = "completed"
                                _sdata["completed_at"] = time.time()
                                conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                    (json.dumps(_sr, ensure_ascii=False), _sub_row["instance_id"]))
                                conn.commit()
                                # 子链完成但其末步文案是驳回 → 父步「完成」是假象。
                                # 不改完成语义（wf_lifecycle.py:302 有意不拦末步，硬拦会
                                # 制造静默卡死），只记账 + 写 bus 让缺失可被外部观测，
                                # 与同文件 _forced 子链超时的做法一致。
                                if _subflow_verdict_rejected(_sr):
                                    try:
                                        self._bb.write(
                                            "blocker",
                                            f"[workflow] {_sub_row['instance_id'][:12]}/{_step_id} "
                                            f"子链 {_sf[:12]} 末步驳回，父步完成不可信",
                                            src="workflow_engine",
                                            evidence=f"子链 {_sf} status={_sub_status['status']} "
                                                     f"但 exit_messages 含驳回文案，父链仍按 completed 推进。"
                                                     f"该步产出未通过审查，请核查是否需要返工。")
                                        LOGGER.warning(
                                            "子链 %s 末步驳回但报 completed → 父 %s/%s 完成不可信",
                                            _sf[:12], _sub_row["instance_id"][:12], _step_id)
                                    except Exception as _bwe:
                                        LOGGER.debug("subflow-verdict blocker 写入失败: %s", _bwe)
                                # 推进父工作流（advance 内部自带锁内 DB + 锁外 subprocess）
                                _tmpl = conn.execute("SELECT template_id FROM workflow_instances WHERE instance_id=?", (_sub_row["instance_id"],)).fetchone()
                                if _tmpl:
                                    _wf = self._workflows.get(_tmpl["template_id"])
                                    if _wf:
                                        deferred.append(
                                            lambda wfid=_sub_row["instance_id"], tpl=_tmpl["template_id"], sid=_step_id, sr=_sr:
                                                self._advance_production_wf(wfid, lm, tpl, sid, sr)
                                        )
                            elif _sub_status and _sub_status['status'] in ('failed', 'cancelled'):
                                # 子链终态（failed/cancelled）→ 父步无出口，只能等 exit
                                # 超时被 auto-cancel（实测 wf_59263/s5：子链 cancelled 后
                                # 父步 notified 悬空 40min）。重置父步状态让它重走投递。
                                # 按 subflow_id 去重：同一子链只重置一次，否则每 tick 重写。
                                # 前 2 次连 subflow_id 一起清 → _subflow_takeover 重建子链（首选，
                                # 实测 writer_document 因此走通并让 wf_59263 推进到 s6）。
                                # 之后保留 subflow_id：_subflow_takeover 见子链不在活跃白名单
                                # 即返回 False 交还父步自投（wf_lifecycle.py 终态分支）。
                                # **不可**置父步 delivery_blocked 收口：_recover_delivery_blocked
                                # 只扫行级 status=delivery_blocked 的实例，而父链行级是
                                # running，看不见步骤级标记 → 父步永久悬空（实测 wf_22b96/s3）。
                                if _sdata.get("subflow_failed_id") != _sf:
                                    _sdata["subflow_failed_id"] = _sf
                                    _sdata["subflow_failed_at"] = time.time()
                                    _sdata["subflow_failed_status"] = _sub_status["status"]
                                    # 命中 _subflow_takeover 三步：前 2 次清 subflow_id 重建子链；
                                    # 2 次用完父步自投一次（子链重建失败说明该角色/链路持续异常，
                                    # 再建只是同型空转），之后保留 subflow_id 并交人工——
                                    # 无限自投只会让同一失败模式每 tick 重演（实测 wf_22b96/s3
                                    # reviewer 子链连建 5 条全部 delivery_blocked）。
                                    _n = int(_sdata.get("subflow_retry", 0) or 0) + 1
                                    _sdata["subflow_retry"] = _n
                                    if _n <= 2:
                                        _sdata.pop("subflow_id", None)
                                    _sdata.pop("notified_at", None)
                                    _sdata.pop("poll_since", None)
                                    _sdata["status"] = ""
                                    if _n > 3:
                                        # 保留 status="" 但不重建：engine 的派发分支会看到
                                        # subflow_id 仍指向终态子链 → _subflow_takeover 返回
                                        # False → 父步自投；自投失败才落到 delivery_blocked 收口。
                                        _sdata["subflow_escalated"] = True
                                        self._bb.write(
                                            "blocker",
                                            f"[workflow] {_sub_row['instance_id'][:12]}/{_step_id} "
                                            f"子链 {_n} 次失败，停止自动重建",
                                            src="workflow_engine",
                                            evidence=(f"子链 {_sf} status={_sub_status['status']}；"
                                                      f"同一父步已重建 {_n - 1} 次。需人工排查"
                                                      f"该角色/模板链路，自动重建已停止。"))
                                    conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                        (json.dumps(_sr, ensure_ascii=False), _sub_row["instance_id"]))
                                    conn.commit()
                                    _note = (f"已重置重投(第 {_n} 次)" if _n <= 2
                                             else "保留 subflow_id，交还父步自投")
                                    try:
                                        self._bb.write(
                                            "blocker",
                                            f"[workflow] {_sub_row['instance_id'][:12]}/{_step_id} "
                                            f"子链 {_sf[:12]} {_sub_status['status']}，{_note}",
                                            src="workflow_engine",
                                            evidence=f"子链 {_sf} status={_sub_status['status']}。父步 {_step_id} {_note}。")
                                        LOGGER.warning(
                                            "子链 %s %s → 父 %s/%s %s",
                                            _sf[:12], _sub_status["status"],
                                            _sub_row["instance_id"][:12], _step_id, _note)
                                    except Exception as _bwe:
                                        LOGGER.debug("subflow-failed blocker 写入失败: %s", _bwe)

                conn.commit()
                self._check_anomalies(conn, deferred)
        except Exception as _e:
            LOGGER.exception("_scan_tasks 异常")
            try:
                self._bb.write("code_fix", "pipeflow: _scan_tasks 异常",
                               evidence=str(_e), src="pipeflow")
            except (ValueError, KeyError, TypeError):
                LOGGER.debug("bus write fail in _scan_tasks error handler")
        # ── 锁外阶段：subprocess/IO 不再持锁，busy_timeout=5s 不再冲突 ──
        for _fn in deferred:
            try:
                _fn()
            except Exception as _d:
                LOGGER.debug("deferred advance failed: %s", _d)
        # ponytail: 动态任务生成——每轮 run_once 检查一次每个角色是否需要新任务
        self._generate_tasks_from_state()
        # ponytail: 任务质量反馈闭环——每轮 run_once 评估最近完成的任务并写入 bus
        try:
            from task_evidence import evaluate_and_feedback
            _feed = evaluate_and_feedback()
            if _feed.get("evaluated", 0):
                LOGGER.info("task-evidence: evaluated %d completed tasks", _feed["evaluated"])
        except Exception as _e:
            LOGGER.debug("task_evidence feedback failed: %s", _e)

    _HEAL_COOLDOWN_SEC = 600

    def _heal_send(self, role: str, prompt: str, instance_id: str, step_id: str) -> None:
        """锁外自愈重投：投递成功才盖戳 last_heal（锁外独立事务）。

        投递失败（并发守卫/变量校验拦截）时返回 False，不盖戳 → 下轮重试，
        避免把"未投递却标记已自愈"掩盖死锁（bus #728c6e4307c1 同类根因）。

        冷却：同一 step 每 _HEAL_COOLDOWN_SEC 最多重投一次。原先 last_heal 只写
        不读（全仓无读取点），_check_anomalies 每 tick（10s）都重走本路径 →
        同一 prompt 气温每 11s 重复注入角色 pane（实测近 24h 916 条同前缀投递）。
        与 role_lifecycle 的 _KICK_COOLDOWN_SEC 同范式。
        """
        try:
            _row0 = self._lifecycle.query_one(
                "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                (instance_id,))
            _sr0 = json.loads((_row0["step_results"] if _row0 else None) or "{}")
            _last_heal = (_sr0.get(step_id) or {}).get("last_heal", 0)
            if _last_heal and time.time() - _last_heal < self._HEAL_COOLDOWN_SEC:
                return
        except Exception as _e:
            LOGGER.debug("heal cooldown read failed: %s", _e)
        self._ensure_role_alive(role)
        # 用 `is not False` 而非 bool()："busy"（角色正忙）不应视为投递失败去重试 ——
        # 角色已在工作，heal 无意义；只对真失败（False）返回不盖戳以待下轮。
        _ok = self._send_to_role(role, prompt, wf_id=instance_id, step_id=step_id) is not False
        if not _ok:
            return
        try:
            with self._lifecycle._conn_tx() as _c:
                _row = _c.execute(
                    "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                    (instance_id,)).fetchone()
                _sr = json.loads(_row["step_results"] or "{}") if _row else {}
                _sr.setdefault(step_id, {})["last_heal"] = time.time()
                _c.execute(
                    "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                    (json.dumps(_sr, ensure_ascii=False), instance_id))
        except Exception as _e:
            LOGGER.debug("heal stamp failed: %s", _e)

    def _backlog_alert_allowed(self, role: str) -> bool:
        """积压告警冷却闸（DESIGN §2.6 S4）：同角色 30min 内只放行一次。

        无冷却时每 run_once tick 对同一积压角色重发同条告警 → curator 实测
        3 天 106 条。去重键取 assignee：消息文本含实时计数、每 tick 都变，
        文本维度的 hash 恒不命中（这正是原告警无节流的原因）。
        """
        _now = time.time()
        if _now - self._BACKLOG_ALERT_TS.get(role, 0) < self._BACKLOG_ALERT_COOLDOWN_S:
            return False
        self._BACKLOG_ALERT_TS[role] = _now
        return True

    def _check_anomalies(self, conn=None, deferred=None):
        """Detect workflows stuck across multiple steps; auto-heal.
        Also monitors overall completion rate and backlog health.

        conn: 由调用方传入的持锁事务连接（_scan_tasks 持锁期间调用）；
        None 时自取（独立调用场景，如 CLI tick）。
        deferred: 锁外执行的 subprocess 收集列表（_notify_role/_send_to_role 不持锁）；
        None 时独立执行（锁内收集，锁外执行）。"""
        if conn is None:
            lm2 = self._lifecycle
            _deferred = []
            with lm2._conn_tx() as _c:
                self._check_anomalies(_c, _deferred)
            for _fn in _deferred:
                try:
                    _fn()
                except Exception as _d:
                    LOGGER.debug("deferred anomaly action failed: %s", _d)
            return
        _deferred = deferred if deferred is not None else []
        try:
            # 全局健康仪表板
            _total = conn.execute("SELECT COUNT(*) as c FROM workflow_instances").fetchone()
            _completed = conn.execute("SELECT COUNT(*) as c FROM workflow_instances WHERE status='completed'").fetchone()
            _running = conn.execute("SELECT COUNT(*) as c FROM workflow_instances WHERE status='running'").fetchone()
            total = _total["c"] if _total else 0
            if total > 20:
                completed = _completed["c"] or 0
                rate = round(completed / total * 100, 1)
                if rate < 20:
                    self._bb.write("architecture",
                        f"[workflow_engine] 完成率 {rate}% ({completed}/{total})，低于 20%，需人工审视",
                        src="workflow_engine")
                for _row in conn.execute(
                    "SELECT assignee, COUNT(*) as c FROM workflow_instances "
                    "WHERE status IN ('pending','running') GROUP BY assignee"
                ).fetchall():
                    if _row["c"] > 10:
                        # S4 冷却（DESIGN §2.6）：同角色积压告警 30min 只发一次。
                        # 去重键用 assignee（不是消息文本）：文本含实时计数，每 tick 都变，
                        # 文本 hash 恒不命中 → 冷却失效。只按角色节流即可。
                        if not self._backlog_alert_allowed(_row["assignee"]):
                            continue
                        self._bb.write("architecture",
                            f"[workflow_engine] 角色 {_row['assignee']} 积压 {_row['c']} 个运行中任务，超过阈值 10",
                            src="workflow_engine")
            running = conn.execute(
                "SELECT instance_id, template_id, current_step_id, step_results, created_at, assignee "
                "FROM workflow_instances WHERE status='running'"
            ).fetchall()
            for row in running:
                inst = dict(row)
                results = json.loads(inst.get("step_results") or "{}")
                wf = self._workflows.get(inst["template_id"])
                _wf_steps = (wf.steps if wf else [])
                timed_out_steps = 0
                _escalated = False
                for _sid, _sr in results.items():
                    if not isinstance(_sr, dict):
                        continue
                    if _sr.get("escalated"):
                        _escalated = True
                    _tc = _sr.get("timeout_count", 0)
                    if _tc == 0:
                        continue
                    _sf = next((s for s in _wf_steps if s.id == _sid), None)
                    _mr = _sf.max_retries if _sf else 0
                    if _tc >= _mr + 1:
                        timed_out_steps += 1
                # ZOMBIE 模板（test/retry_test/prompt_test 等）超时不 escalate → 过滤 95% stress-test blocker 噪音
                _is_zombie = inst.get("template_id") in _engine_mod.ZOMBIE_TEMPLATES
                # 测试 harness 模板（test_*/exit_schema 等验收仅 grep 关键词）在生产环境
                # 反复运行 → 派发 phantom 角色 → ondemand 回收 → 无限重推 → bus 噪音循环。
                # 与 ZOMBIE 同一处理：超时不重推，直接 cancel，让活着的角色注意力回到真实产出。
                _is_test_harness = _is_test_harness_template(inst.get("template_id"))
                if timed_out_steps >= 2 and not _escalated and not _is_zombie and not _is_test_harness:
                    _deferred.append(lambda i=inst:
                        self._notify_role("maintainer",
                            f"工作流多步骤超时: {i['instance_id'][:12]}",
                            f"{timed_out_steps} 个步骤已耗尽重试次数，当前步骤={i.get('current_step_id','?')} 角色={i.get('assignee','?')}"))
                if timed_out_steps >= 3 and not _is_zombie and not _is_test_harness:
                    _deferred.append(lambda i=inst:
                        self._notify_role("maintainer",
                            f"工作流多步骤超限自愈: {i['instance_id'][:12]}",
                            f"{timed_out_steps} 个步骤全部超限，正在强制重推，检查是否需人工介入。"))
                    _role = inst.get("assignee", "")
                    _sid = inst.get("current_step_id", "")
                    if _sid and _role and wf:
                        _sf = next((s for s in wf.steps if s.id == _sid), None)
                        if _sf:
                            # 必须走 _fill_prompt_vars：直接投 prompt_template 会把
                            # {title}/{description} 原样发出，被 _send_to_role 的
                            # _UNMATCHED_VAR_RE 守卫拦下 → 自愈链路整体空转（_heal_send
                            # 返回前就 return、不盖 last_heal，下轮重来）。与
                            # step_handler:380、role_lifecycle:214、wf_lifecycle:417 一致。
                            _hf_name = inst.get("template_id") or ""
                            _hf_title = inst.get("title") or _hf_name
                            _hf_idx = next((n for n, s in enumerate(wf.steps)
                                            if s.id == _sid), -1)
                            _hf_prev = wf.steps[_hf_idx - 1].id if _hf_idx > 0 else ""
                            _prompt = self._fill_prompt_vars(
                                _sf.prompt_template, _sf, _hf_name, _hf_title,
                                inst.get("description") or "", results, _hf_prev,
                                wf_id=inst["instance_id"])
                            # 锁外执行：投递成功才盖戳 last_heal（投递失败不下轮重试，不误标已自愈）
                            _deferred.append(lambda role=_role, p=_prompt, i=inst, s=_sid:
                                self._heal_send(role, p, i["instance_id"], s))
                # 测试 harness 实例超时达阈值 → 直接 cancel，不再无限重推喂噪音。
                # 这是堵死 phantom 回环的关键：保留 running 会让下一轮继续 timeout→重推→升级，
                # 把测试噪音广播给所有活着的角色。
                if _is_test_harness and timed_out_steps >= 3:
                    try:
                        conn.execute(
                            "UPDATE workflow_instances SET status='cancelled', completed_at=? "
                            "WHERE instance_id=?",
                            (time.time(), inst["instance_id"]))
                        LOGGER.info("test-harness instance cancelled (no re-push): %s",
                                    inst["instance_id"][:12])
                    except Exception as _e:
                        LOGGER.debug("cancel test-harness instance failed: %s", _e)
                conn.commit()
            # ── 指标持久化（JSONL）──
            try:
                _st_rows = conn.execute("SELECT status, COUNT(*) as c FROM workflow_instances GROUP BY status").fetchall()
                _metrics = {
                    "ts": time.time(),
                    "total": total,
                    "completed": (_completed["c"] if _completed else 0),
                    "rate": round((_completed["c"] or 0) / max(total, 1) * 100, 1),
                    "running": len(running) if running else _running["c"] if _running else 0,
                    "by_status": {r["status"]: r["c"] for r in _st_rows},
                }
                _mf = Path.home() / ".hermes" / "state" / "workflow-metrics.jsonl"
                _mf.parent.mkdir(parents=True, exist_ok=True)
                with _mf.open("a") as f:
                    f.write(json.dumps(_metrics, ensure_ascii=False) + "\n")
            except (ValueError, KeyError, TypeError):
                pass
        except Exception as _e:
            LOGGER.error("heal_stalled failed: %s", _e)

    def _generate_tasks_from_state(self):
        """从工作流模板为空闲角色自动生成任务。

        每轮 run_once 末尾运行，检测当前 DB 中无 running/pending 任务的角色，
        从加载的模板中选择最匹配的生成一个 task_spec 写入 bus。

        # 防重复机制
        - 每个角色每 600s 最多生成 1 个任务
        - 已有 running/pending workflow 的角色跳过
        - 生成标题描述性（≥12字符），非占位符
        """
        getattr(self, '_TASK_GEN_COOLDOWN', {})
        _now = time.time()
        _COOLDOWN_S = 120  # 2min，平衡速度与稳定性

        # 1) 哪些角色已有 running/pending 任务
        try:
            _busy_roles = set()
            for _r in self._lifecycle.query(
                "SELECT DISTINCT assignee FROM workflow_instances "
                "WHERE status IN ('running','pending','step_done_ready')"
            ):
                _busy_roles.add(_r['assignee'])
        except Exception:
            return

        # 2) 遍历模板，为每个有模板但空闲的角色生成任务
        _tasks_added = 0
        for _wf_name, _wf in self._workflows.items():
            if not _wf.steps or not _wf.allowed_executors:
                continue
            if _wf.is_subflow:
                continue
            # 测试 harness 模板（test_*/exit_schema 等验收仅 grep 关键词）禁止自动派发：
            # 反复生成 → 喂饱 production 角色做无意义的测试任务 → 真实需求被淹没。
            # 与 _check_anomalies 共用同一判据函数，避免测试噪音回环。
            if _is_test_harness_template(_wf_name):
                continue
            # 取第一个 executor 作为目标角色
            _role = _wf.allowed_executors[0]
            if _role in _busy_roles:
                continue
            if not self._can_assign_role(_role):
                continue
            # 跳过空模板描述（task_spec needs_implementation → 理解 → 编码 是占位符）
            if not _wf.description or len(_wf.description.strip()) < 20 or "needs_implementation" in (_wf.description or ""):
                continue
            # 价值门槛：自动生成必须有真实需求信号，防止模板空转。
            # 无 bus 需求信号（task_spec/blocker/需求类消息）时跳过自动生成。
            try:
                _demand = self._bb.read(cat="task_spec", limit=10)
                _demand_text = ""
                for _f in _demand:
                    if _f.src not in ("workflow_engine", "survival_monitor"):
                        # closer 周期性 backlog 扫描是模板循环产物，不是真实需求
                        if "backlog_scan" not in (_f.t or ""):
                            _demand_text += (_f.t or "") + " " + (_f.e or "") + " "
                # 需求信号必须与目标角色相关：标题或内容含角色名/其职责关键词才算
                _role_hits = (
                    f"@{_role}" in _demand_text
                    or f"assignee={_role}" in _demand_text
                    or _role in _demand_text
                )
                _has_demand = bool(_demand_text) and _role_hits and _demand_text.strip() != ""
                if not _has_demand:
                    continue
            except Exception:
                pass
            # cooldown 检测（加锁消除 check-then-act 竞态：两个线程可能同时通过检查）
            with self._TASK_GEN_LOCK:
                _last = self._TASK_GEN_COOLDOWN.get(_role, 0)
                if _now - _last < _COOLDOWN_S:
                    continue
            # 同模板同角色：有活跃实例(running/pending) → 跳过（防重复创建）
            # 24h 内已完成/取消过 → 跳过（防循环派发同模板任务）
            try:
                _existing = self._lifecycle.query(
                    "SELECT COUNT(*) as c FROM workflow_instances "
                    "WHERE template_id=? AND assignee=? "
                    "AND (status IN ('running','pending') OR created_at > ?)",
                    (_wf_name, _role, _now - 86400)
                )[0]["c"]
                if _existing > 0:
                    continue
            except Exception:
                pass
            # 3) 组装有描述性的任务标题（必须在去重检查**之前**：
            # 去重要按 title 比对，先读后写会 UnboundLocalError，
            # 且被下面的 except 吞掉 → 去重静默失效）
            _title = f"[auto] {_wf.title or _wf_name}: {_wf.description[:40]}" if _wf.description else f"[auto] 执行 {_wf_name} 工作流"
            if len(_title) < 12:
                continue  # 跳过标题过短
            # S1 事件去重（DESIGN §2.1）：同标题同角色 24h 内已 mint 过 task → 跳过。
            # 上一段 per-template 守卫挡不住同模板下不同标题的事件洪水（每次超时
            # 重推/路由消费都带新标题），title 维度的 24h 窗口补齐该缺口。
            try:
                _dup = self._lifecycle.query(
                    "SELECT COUNT(*) as c FROM tasks "
                    "WHERE title=? AND assignee=? AND created_at > ?",
                    (_title[:80], _role, _now - 86400)
                )[0]["c"]
                if _dup > 0:
                    continue
            except Exception as _e:
                # 不再静默：去重失效 = 事件洪水无闸门，必须在日志可见
                LOGGER.warning("auto-task title 去重检查失败（降级为不去重）: %s", _e)
            # 原 _prompt 构造块已删除：只赋值从不使用，且内含 /goal 死路径 (task_generator.py:434-443)

            # 4) 直接启动工作流——绕过路由 daemon 直接创建 task + workflow_instance
            # _can_assign_role 已检测 tmux 会话存活
            try:
                _task_id = f"task_auto_{uuid.uuid4().hex[:8]}"
                _now_ts = time.time()
                # 写入 task 记录
                try:
                    self._lifecycle.execute(
                        "INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee, status, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, 'open', ?, ?)",
                        (_task_id, _title[:80], _wf.description[:200] if _wf.description else "",
                         "workflow_engine", _role, _now_ts, _now_ts))
                except Exception:
                    pass  # 表可能不存在
                # 启动工作流实例（创建 + 推 prompt 到角色）
                _rid = self.start(_wf_name, context={"task_id": _task_id, "title": _title})
                # 修正 assignee — start() 内部用 workflow_engine role，需覆盖为真实执行者
                try:
                    self._lifecycle.execute(
                        "UPDATE workflow_instances SET assignee=? WHERE instance_id=?",
                        (_role, _rid))
                except Exception:
                    pass
                # 更新 task → workflow 关联
                try:
                    self._lifecycle.execute(
                        "UPDATE workflow_instances SET task_id=? WHERE instance_id=?",
                        (_task_id, _rid))
                except Exception:
                    pass
                LOGGER.info("auto-task: %s → %s (wf=%s, task=%s)", _role, _title[:60], _rid[:16], _task_id)
                with self._TASK_GEN_LOCK:
                    self._TASK_GEN_COOLDOWN[_role] = _now
                _tasks_added += 1
                if _tasks_added >= 5:
                    break  # 每轮最多创建 5 个任务，加速产出
            except Exception as _e:
                LOGGER.debug("auto-task start failed for %s: %s", _role, _e)

        if _tasks_added:
            LOGGER.info("auto-task: 本轮生成 %d 个新任务", _tasks_added)

    def _can_assign_role(self, role: str) -> bool:
        """检查角色是否可分配任务：L1+L2 忙闲检测（委托 liveness 统合模块）。

        L1: tmux 会话存活（session 不存在 → 不可分配）
        L2: 正在思考（liveness L1 thinking 信号 → busy，跳过）
        L3: running 实例 >= MAX_RUNNING_PER_ROLE → backpressure（DESIGN §2.2）
        idel/stale/unknown → 放行
        """
        _skip = {"coordinator", "pipeline", "claude", "workflow_engine", ""}
        if role in _skip:
            return False
        # L3: per-role running 上限（防洪水第二层）：角色已有 >= MAX_RUNNING_PER_ROLE
        # 个 running/pending 实例 → 拒绝新派发，等回收自然降到水位线以下。
        # 常量化而非配置：改大=放开洪水闸门，改小=饿死慢角色；3 与 DESIGN 验收 ⑤ 一致。
        try:
            _run = self._lifecycle.query(
                "SELECT COUNT(*) as c FROM workflow_instances "
                "WHERE assignee=? AND status IN ('running','pending')",
                (role,)
            )[0]["c"]
            if _run >= MAX_RUNNING_PER_ROLE:
                LOGGER.debug("_can_assign_role backpressure: %s running=%d >= %d",
                             role, _run, MAX_RUNNING_PER_ROLE)
                return False
        except Exception as _e:
            LOGGER.debug("_can_assign_role running-count query failed: %s", _e)
        # L1: tmux 存活（与原实现一致：异常/session 不在 → 不可分配）
        from liveness import _tmux_has_session, role_liveness
        if _tmux_has_session(role, timeout=5) is not True:
            return False
        # L2: thinking 信号（health 文件 survival_l2_thinking）。health 文件缺失时
        # （available=False）降级回 pane 活跃检查，保留原语义兜底——
        # 否则自动任务生成从"60s 活跃拦截"退化为"无条件放行"（review P1-2 修复）。
        res = role_liveness(role, level=1)
        if res["busy"]:
            return False
        if res.get("signals", {}).get(1, {}).get("available"):
            return True  # health 信号可用且不忙
        # 降级：health 文件缺失 → pane 最近有无输出（原 _can_assign_role L2 语义）。
        # 原实现直接跑 `tmux list-panes -F #{pane_activity}`，该格式在 tmux 3.4
        # 恒为空串（实测），故条件恒不成立 → 返回 False → 角色永久不可分配任务。
        # 改调 liveness.tmux_pane_active()：同一信号源，#{window_activity} 已实测有效。
        from liveness import tmux_pane_active
        _active = tmux_pane_active(role, timeout=3)
        if _active is None:
            return True  # 探测不可用 → 不阻断（与 liveness 的 available=False 放行一致）
        return not _active  # 近期有输出 = 在干活 → 不可分配
