#!/usr/bin/env python3
"""TaskGeneratorMixin — 任务扫描、异常检测、动态任务生成、角色可分配检测。

从 engine.py 拆出，方法签名和 self 访问完全不变。
"""

import json
import time
import uuid
import logging
import threading
from pathlib import Path

from pipeflow import engine as _engine_mod  # _engine_mod._sp — 保证 patch("pipeflow.engine._sp") 生效

LOGGER = logging.getLogger("workflow.engine")


class TaskGeneratorMixin:
    """WorkflowEngine 的任务生成 mixin。"""

    _TASK_GEN_COOLDOWN: dict[str, float] = {}  # role → last gen ts
    # check-then-act 竞态防护：cooldown 检查与写入之间加锁（GIL 下结构不损坏，仅语义竞态）
    _TASK_GEN_LOCK = threading.Lock()

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
                for _sub_row in conn.execute(
                    "SELECT wi.instance_id, wi.current_step_id, wi.step_results FROM workflow_instances wi "
                    "WHERE wi.status='running' AND wi.template_id IN (SELECT template_id FROM workflow_templates)"
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
                            if _sub_status and _sub_status['status'] in ('completed', 'step_done_ready'):
                                _sdata["status"] = "completed"
                                _sdata["completed_at"] = time.time()
                                conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                    (json.dumps(_sr, ensure_ascii=False), _sub_row["instance_id"]))
                                conn.commit()
                                # 推进父工作流（advance 内部自带锁内 DB + 锁外 subprocess）
                                _tmpl = conn.execute("SELECT template_id FROM workflow_instances WHERE instance_id=?", (_sub_row["instance_id"],)).fetchone()
                                if _tmpl:
                                    _wf = self._workflows.get(_tmpl["template_id"])
                                    if _wf:
                                        deferred.append(
                                            lambda wfid=_sub_row["instance_id"], tpl=_tmpl["template_id"], sid=_step_id, sr=_sr:
                                                self._advance_production_wf(wfid, lm, tpl, sid, sr)
                                        )

                conn.commit()
                self._check_anomalies(conn, deferred)
        except Exception as _e:
            LOGGER.exception("_scan_tasks 异常")
            try:
                self._bb.write("code_fix", f"pipeflow: _scan_tasks 异常",
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
                if timed_out_steps >= 2 and not _escalated:
                    _deferred.append(lambda i=inst:
                        self._notify_role("maintainer",
                            f"工作流多步骤超时: {i['instance_id'][:12]}",
                            f"{timed_out_steps} 个步骤已耗尽重试次数，当前步骤={i.get('current_step_id','?')} 角色={i.get('assignee','?')}"))
                if timed_out_steps >= 3:
                    _deferred.append(lambda i=inst:
                        self._notify_role("maintainer",
                            f"工作流多步骤超限自愈: {i['instance_id'][:12]}",
                            f"{timed_out_steps} 个步骤全部超限，正在强制重推，检查是否需人工介入。"))
                    _role = inst.get("assignee", "")
                    _sid = inst.get("current_step_id", "")
                    if _sid and _role and wf:
                        _sf = next((s for s in wf.steps if s.id == _sid), None)
                        if _sf:
                            _prompt = _sf.prompt_template
                            _deferred.append(lambda role=_role, p=_prompt, i=inst, s=_sid:
                                (self._ensure_role_alive(role),
                                 self._send_to_role(role, p, wf_id=i["instance_id"], step_id=s)))
                    if _sid and _sid in results:
                        conn.execute("BEGIN IMMEDIATE")
                        try:
                            results[_sid]["last_heal"] = time.time()
                            conn.execute(
                                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                (json.dumps(results, ensure_ascii=False), inst["instance_id"]))
                            conn.commit()
                        except Exception:
                            conn.rollback()
                            raise
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
        _lock = getattr(self, '_TASK_GEN_COOLDOWN', {})
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

            # 3) 组装有描述性的任务标题
            _title = f"[auto] {_wf.title or _wf_name}: {_wf.description[:40]}" if _wf.description else f"[auto] 执行 {_wf_name} 工作流"
            if len(_title) < 12:
                continue  # 跳过标题过短
            _prompt = (
                f"/goal\n\n"
                f"## 任务\n执行工作流模板 {_wf_name}\n\n"
                f"## 描述\n{_wf.description}\n\n"
                f"## 步骤\n"
            )
            for _s in _wf.steps:
                _prompt += f"  {_s.id}: {_s.title}\n"
            if _wf.quality_standards:
                _prompt += f"\n## 质量标准\n{_wf.quality_standards}\n"

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
        """检查角色是否可分配任务：L1+L2+L3 忙闲检测。

        L1: tmux 会话存活
        L2: 正在思考（pane 活跃 < 300s）→ busy，跳过
        L3: 最近有产出（bus 最近 10min 有该角色产出）→ busy，跳过
        idel/stale/unknown → 放行
        """
        _skip = {"coordinator", "pipeline", "claude", "workflow_engine", ""}
        if role in _skip:
            return False
        # L1: tmux 存活
        try:
            _r = _engine_mod._sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                         capture_output=True, timeout=5)
            if _r.returncode != 0:
                return False
        except Exception:
            return False
        # L2: pane 活动
        try:
            _pr = _engine_mod._sp.run(["tmux", "list-panes", "-t", f"ccs-{role}",
                           "-F", "#{pane_activity}"],
                          capture_output=True, text=True, timeout=3)
            if _pr.returncode == 0 and _pr.stdout.strip():
                _val = _pr.stdout.strip().split("\n")[0]
                if _val and _val != "0":
                    if time.time() - float(_val) < 60:
                        return False  # 1分钟内活跃 → 真在忙，跳过
        except Exception:
            pass
        # ponytail: L3 bus 产出检查移除——L1+L2 足够判断忙闲，L3 太慢导致空角色无法分配
        return True
