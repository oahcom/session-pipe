#!/usr/bin/env python3
"""
lifecycle_manager.py — 生命周期管理器

管理工作流状态流转：pending→running→step_done_ready→completed。
5 种步骤类型（handoff/review/single/gate/notify）各有不同完成策略。
并发安全：使用 threading.RLock + SQLite WAL 模式 + BEGIN IMMEDIATE。
"""

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import List, Optional

from paths import WORKFLOWS_DB as DB_PATH  # ponytail: direct import, switch to relative when moved to subpackage



# ── 资源治理约束（agent-contracts 模式）──
_RESOURCE_BUDGETS: dict[str, dict] = {}
_DEFAULT_BUDGET = {"max_sessions": 3, "max_memory_mb": 2000, "max_calls_per_hour": 60, "cooldown_sec": 10}

def set_role_budget(role: str, budget: dict) -> None:
    merged = dict(_DEFAULT_BUDGET); merged.update(budget)
    _RESOURCE_BUDGETS[role] = merged

def get_role_budget(role: str) -> dict:
    return _RESOURCE_BUDGETS.get(role, dict(_DEFAULT_BUDGET))

def check_resource_constraints(role: str) -> dict:
    import subprocess
    budget = get_role_budget(role); violations = []; usage = {}
    try:
        r = subprocess.run(["tmux","ls"], capture_output=True, text=True, timeout=5)
        usage["sessions"] = r.stdout.count(f"ccs-{role}")
        if usage["sessions"] > budget["max_sessions"]:
            violations.append(f"sessions {usage['sessions']}/{budget['max_sessions']}")
    except Exception: usage["sessions"] = -1
    try:
        mem = subprocess.run(["free","-m"], capture_output=True, text=True, timeout=5)
        for line in mem.stdout.split("\n"):
            if line.startswith("Mem:"):
                usage["memory_mb"] = int(line.split()[3])
                if usage["memory_mb"] < budget["max_memory_mb"]:
                    violations.append(f"mem {usage['memory_mb']} < {budget['max_memory_mb']}MB")
    except Exception: usage["memory_mb"] = -1
    return {"ok": len(violations)==0, "violations": violations, "usage": usage}

class LifecycleManager:
    """生命周期管理器：状态机 + 步骤执行 + 推进逻辑。"""

    WF_STATUSES = {"pending", "running", "completed", "failed", "cancelled"}
    STEP_STATUSES = {"pending", "running", "step_done_ready", "completed", "failed"}
    STEP_TYPES = {"handoff", "review", "single", "gate", "notify"}

    def __init__(self, role: str, db_path: str = None):
        self.role = role
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()

    # ── 状态查询 ─────────────────────────────────

    def upsert_template(self, template_id: str, name: str,
                         description: str, steps: list[dict],
                         trigger_scene: list[str] = None,
                         allowed_initiators: list[str] = None,
                         allowed_executors: list[str] = None,
                         max_duration_hours: int = 24,
                         quality_standards: str = "") -> None:
        """写入或更新工作流模板定义（含角色适配元数据）。"""
        self._conn.execute("""
            INSERT OR REPLACE INTO workflow_templates
            (template_id, name, description, steps_json, created_at, is_active,
             trigger_scene, allowed_initiators, allowed_executors,
             max_duration_hours, quality_standards)
            VALUES (?, ?, ?, ?, ?, 1,
                    ?, ?, ?,
                    ?, ?)
        """, (template_id, name, description,
              json.dumps(steps, ensure_ascii=False), time.time(),
              json.dumps(trigger_scene or [], ensure_ascii=False),
              json.dumps(allowed_initiators or [], ensure_ascii=False),
              json.dumps(allowed_executors or [], ensure_ascii=False),
              max_duration_hours, quality_standards))
        self._conn.commit()

    def get_wf(self, wf_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE instance_id=?", (wf_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_step(self, wf_id: str, step_id: str) -> Optional[dict]:
        wf = self.get_wf(wf_id)
        if not wf:
            return None
        template = self._get_template(wf.get("template_id"))
        if not template:
            return None
        for step in template.get("steps", []):
            if step.get("step_id") == step_id:
                return step
        return None

    def _get_template(self, template_id: Optional[str]) -> Optional[dict]:
        if not template_id:
            return None
        row = self._conn.execute(
            "SELECT * FROM workflow_templates WHERE template_id=?", (template_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        steps = d.get("steps_json")
        if isinstance(steps, str):
            try:
                d["steps"] = json.loads(steps)
            except (json.JSONDecodeError, TypeError):
                d["steps"] = []
        return d

    def _log(self, wf_id: str, task_id: str = None,
             action: str = "", detail: str = ""):
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) VALUES (?, ?, ?, ?, ?, ?)",
            (wf_id, task_id, action, self.role, detail, time.time())
        )
        self._conn.commit()

    # ── 核心操作（并发安全） ─────────────────────

    def start_wf(self, wf_id: str, current_step_id: str = "s1",
                  template_id: str = "", context: dict = None) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                ctx_json = json.dumps(context or {}, ensure_ascii=False)
                cur = self._conn.execute(
                    "UPDATE workflow_instances SET status='running', "
                    "current_step_id=?, context=? WHERE instance_id=? AND status='pending'",
                    (current_step_id, ctx_json, wf_id)
                )
                if cur.rowcount == 0:
                    # engine 直接启动的工作流（无前置 task），INSERT 完整行
                    task_id_val = "wf_" + wf_id[-8:]
                    self._conn.execute("""
                        INSERT OR IGNORE INTO workflow_instances
                        (instance_id, task_id, assigner, assignee, status, current_step_id,
                         template_id, created_at, context)
                        VALUES (?, ?, 'workflow_engine', 'workflow_engine', 'running',
                                ?, ?, ?, ?)
                    """, (wf_id, task_id_val, current_step_id,
                          template_id or None, time.time(), ctx_json))
                self._conn.commit()
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                self._log(wf_id, task_id, "wf_started",
                          detail=f"workflow started by {self.role}, step={current_step_id}")
                return True
            except Exception:
                self._conn.rollback()
                raise

    def complete_step(self, wf_id: str, step_id: str) -> str:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if wf.get("current_step_id") != step_id:
                    raise ValueError(
                        f"步骤不匹配: 当前步骤={wf['current_step_id']}, 传入={step_id}")

                results = self._parse_results(wf)
                cur_status = results.get(step_id, {}).get("status")
                if cur_status in ("step_done_ready", "completed"):
                    self._conn.commit()
                    return "already_completed"

                step = self.get_step(wf_id, step_id)
                if not step:
                    raise ValueError(f"步骤定义不存在: {step_id}")
                step_type = step.get("type", "single")
                task_id = wf.get("task_id")

                handler = {
                    "single": self._complete_single_unsafe,
                    "handoff": self._complete_handoff_unsafe,
                    "review": self._complete_review_unsafe,
                    "gate": self._complete_gate_unsafe,
                    "notify": self._complete_notify_unsafe,
                    "subflow": self._complete_subflow_unsafe,
                }
                fn = handler.get(step_type)
                if not fn:
                    raise ValueError(f"未知步骤类型: {step_type}")
                result = fn(wf_id, step_id, step, task_id, results)
                self._conn.commit()
                return result
            except Exception:
                self._conn.rollback()
                raise

    def _get_assigner_for_step(self, wf: dict, step: dict, step_id: str) -> str:
        """返回应确认此步骤的角色（分配者链）。

        handoff: 第1步→发起者, 第N步→第N-1步的target_role
        review: target_role
        single/gate/notify: 无需确认, 返回空
        """
        st = step.get("type", "")
        if st == "review":
            return step.get("target_role", "")
        if st in ("handoff", "subflow"):
            template = self._get_template(wf.get("template_id"))
            if not template:
                return ""
            steps = template.get("steps", [])
            for i, s in enumerate(steps):
                if s.get("step_id") == step_id:
                    if i == 0:
                        return wf.get("assigner", "")
                    return steps[i - 1].get("target_role", "")
        return ""

    def get_approval_token(self, wf_id: str, step_id: str) -> Optional[str]:
        """获取步骤的当前审批密钥（测试辅助）。"""
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return None
        results = self._parse_results(wf)
        return results.get(step_id, {}).get("approval_token")

    def confirm_step(self, wf_id: str, step_id: str, token: str = "",
                     approved: bool = True, reason: str = "") -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")

                results = self._parse_results(wf)
                current = results.get(step_id, {})
                if current.get("status") != "step_done_ready":
                    raise ValueError(
                        f"步骤 {step_id} 状态为 {current.get('status','?')}，非 step_done_ready")

                # ── 审批密钥验证 ──
                stored_token = current.get("approval_token")
                if stored_token:
                    if not token:
                        raise PermissionError("approval token required but not provided")
                    if current.get("approval_consumed_at"):
                        raise PermissionError("token already used")
                    if time.time() > current.get("approval_token_ttl", 0):
                        raise PermissionError("token expired")
                    if token != stored_token:
                        raise PermissionError("invalid token")
                    current["approval_consumed_at"] = time.time()
                    current["approval_token"] = None
                else:
                    step = self.get_step(wf_id, step_id)
                    if step:
                        assigner = self._get_assigner_for_step(wf, step, step_id)
                        if assigner and self.role != assigner:
                            raise PermissionError(
                                f"only assigner can confirm: {self.role} != {assigner}")

                # ── subflow 步骤: 确认前检查子工作流是否完成 ──
                step_def = self.get_step(wf_id, step_id)
                if step_def and step_def.get("type") == "subflow":
                    sub_wf_id = current.get("sub_wf_id") or current.get("subflow_id", "")
                    if sub_wf_id:
                        child = self._get_wf_unsafe(sub_wf_id)
                        if not child:
                            raise ValueError(f"子工作流 {sub_wf_id} 不存在")
                        if child["status"] == "failed":
                            raise ValueError(
                                f"子工作流 {sub_wf_id} 已失败，无法审批父步骤")
                        if child["status"] != "completed":
                            raise ValueError(
                                f"子工作流 {sub_wf_id} 未完成 (status={child['status']})")

                completed_by = current.get("completed_by", "")
                task_id = wf.get("task_id")

                if approved:
                    current["status"] = "completed"
                    current["confirmed_at"] = time.time()
                    current["confirmed_by"] = self.role
                    results[step_id] = current
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._advance_unsafe(wf_id, task_id)
                    self._log_unsafe(wf_id, task_id, "step_confirmed",
                                     detail=f"{step_id} confirmed by {self.role}")
                    self._conn.commit()
                    self._notify_result(completed_by, wf_id, step_id, True, reason)
                    return True
                else:
                    current["status"] = "rejected"
                    current["rejected_at"] = time.time()
                    current["rejected_by"] = self.role
                    current["rejection_reason"] = reason
                    results[step_id] = current
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._log_unsafe(wf_id, task_id, "step_rejected",
                                     detail=f"{step_id} rejected by {self.role}: {reason}")
                    self._conn.commit()
                    self._notify_result(completed_by, wf_id, step_id, False, reason)
                    return False
            except Exception:
                self._conn.rollback()
                raise

    def _notify_result(self, executor: str, wf_id: str, step_id: str,
                       approved: bool, reason: str):
        """通知原角色审批结果"""
        import logging as _log
        logger = _log.getLogger("lifecycle.notify")
        if not executor:
            return
        try:
            from workflow.client import get_ccs_cli as _get_ccs_cli
            import subprocess as _sp
            status = "✅ 通过" if approved else "❌ 拒绝"
            msg = (
                f"[workflow] 审批结果通知\n\n"
                f"工作流: {wf_id}\n"
                f"步骤: {step_id}\n"
                f"审批人: {self.role}\n"
                f"结果: {status}\n"
            )
            if reason:
                msg += f"原因: {reason}\n"
            if not approved:
                msg += "\n请根据拒绝原因修改后重新提交。"
            _ccs_cli = str(_get_ccs_cli()) if not isinstance(_get_ccs_cli(), str) else _get_ccs_cli()
            result = _sp.run(
                ["python3", _ccs_cli, "send", "coordinator", executor, msg,
                 "--from", self.role],
                capture_output=True, timeout=15)
            if result.returncode != 0:
                logger.warning("notify result failed for %s: %s", executor,
                               result.stderr.decode()[:200])
        except Exception as e:
            logger.warning("notify result error: %s", e)

    def fail_step(self, wf_id: str, step_id: str,
                  reason: str = "", allow_retry: bool = False) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if wf.get("current_step_id") != step_id:
                    raise ValueError(
                        f"步骤不匹配: 当前={wf['current_step_id']}, 传入={step_id}")

                results = self._parse_results(wf)
                task_id = wf.get("task_id")

                if allow_retry:
                    results[step_id] = {"status": "failed", "failed_at": time.time(),
                                        "failed_by": self.role, "reason": reason}
                    self._conn.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), wf_id)
                    )
                    self._log_unsafe(wf_id, task_id, "step_failed", detail=f"{step_id}: {reason}")
                    self._conn.commit()
                    return True
                else:
                    self._conn.execute(
                        "UPDATE workflow_instances SET status='failed', completed_at=? "
                        "WHERE instance_id=?", (time.time(), wf_id)
                    )
                    # 级联取消子工作流
                    self._cascade_cancel_unsafe(wf_id, reason)
                    self._sync_task_unsafe(task_id)
                    self._log_unsafe(wf_id, task_id, "wf_failed", detail=f"step {step_id}: {reason}")
                    self._conn.commit()
                    return False
            except Exception:
                self._conn.rollback()
                raise

    def rollback_step(self, wf_id: str, step_id: str) -> bool:
        """回滚已完成步骤到 running。

        仅分配者或 coordinator 可调用。
        将指定步骤状态重置为 running，后续步骤状态重置为 pending。
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                wf = self._get_wf_unsafe(wf_id)
                if not wf:
                    raise ValueError(f"workflow 不存在: {wf_id}")
                if self.role not in (wf.get("assigner", ""), "coordinator"):
                    raise PermissionError(
                        f"only assigner/coordinator can rollback: {self.role}")
                step_def = self.get_step(wf_id, step_id)
                if not step_def:
                    raise ValueError(f"步骤不存在: {step_id}")
                results = self._parse_results(wf)
                if results.get(step_id, {}).get("status") not in ("completed", "step_done_ready"):
                    raise ValueError(f"步骤 {step_id} 未完成，不可回滚")
                results[step_id] = {"status": "running", "rolled_back": True,
                                    "rolled_back_at": time.time(), "rolled_back_by": self.role}
                # 删除后续所有步骤状态
                steps = self._get_all_steps_unsafe(wf.get("template_id"))
                found = False
                for s in steps:
                    if s.get("step_id") == step_id:
                        found = True
                    elif found:
                        results.pop(s.get("step_id"), None)
                self._conn.execute(
                    "UPDATE workflow_instances SET current_step_id=?, step_results=?, status='running' "
                    "WHERE instance_id=?", (step_id, json.dumps(results, ensure_ascii=False), wf_id)
                )
                self._log_unsafe(wf.get("task_id"), wf.get("task_id"), "step_rolled_back",
                                 detail=f"{step_id} rolled back by {self.role}")
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def close_wf(self, wf_id: str, status: str = "completed"):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE workflow_instances SET status=?, completed_at=? "
                    "WHERE instance_id=?", (status, time.time(), wf_id)
                )
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                self._sync_task_unsafe(task_id)
                self._log_unsafe(wf_id, task_id, "wf_closed", detail=f"workflow {status}")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # ── 子工作流级联操作 ───────────────────────────

    def _cascade_cancel_unsafe(self, wf_id: str, reason: str = ""):
        """级联取消所有子工作流（迭代实现，避免递归栈溢出）。"""
        _stack = [wf_id]
        while _stack:
            _current = _stack.pop()
            _children = self._conn.execute(
                "SELECT instance_id, task_id FROM workflow_instances "
                "WHERE parent_wf_id=? AND status NOT IN ('completed', 'cancelled', 'failed')",
                (_current,)
            ).fetchall()
            for _child in _children:
                _cid = _child["instance_id"]
                _ctid = _child["task_id"]
                self._conn.execute(
                    "UPDATE workflow_instances SET status='cancelled', completed_at=? "
                    "WHERE instance_id=?", (time.time(), _cid))
                self._conn.execute(
                    "UPDATE tasks SET status='cancelled' WHERE task_id=?", (_ctid,))
                self._log_unsafe(_cid, _ctid, "wf_cancelled",
                                 detail=f"cascade from parent {_current}: {reason}")
                _stack.append(_cid)

    def _count_pending_subflows(self, wf_id: str, step_id: str) -> int:
        """统计指定步骤创建的子工作流中未完成的个数。"""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM workflow_instances "
            "WHERE parent_wf_id=? AND subflow_source_step_id=? AND status NOT IN ('completed')",
            (wf_id, step_id)
        ).fetchone()
        return row[0] if row else 0

    # ── 内部 unsafe 方法（在已开启的事务中调用） ──

    def _get_wf_unsafe(self, wf_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM workflow_instances WHERE instance_id=?", (wf_id,)
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _parse_results(wf: dict) -> dict:
        if wf.get("step_results"):
            try:
                return json.loads(wf["step_results"])
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    def _log_unsafe(self, wf_id: str, task_id: str = None,
                    action: str = "", detail: str = ""):
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) VALUES (?, ?, ?, ?, ?, ?)",
            (wf_id, task_id, action, self.role, detail, time.time())
        )

    def _set_step_result_unsafe(self, wf_id: str, step_id: str,
                                 status: str, results: dict):
        results[step_id] = {"status": status, "completed_at": time.time(),
                            "completed_by": self.role}
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(results, ensure_ascii=False), wf_id)
        )

    # ── 步骤类型处理 ────────────────────────────

    def _complete_single_unsafe(self, wf_id, step_id, step, task_id, results):
        self._set_step_result_unsafe(wf_id, step_id, "completed", results)
        self._advance_unsafe(wf_id, task_id)
        self._log_unsafe(wf_id, task_id, "step_completed",
                         detail=f"single auto-advance: {step_id}")
        return "completed_and_advanced"

    def _complete_handoff_unsafe(self, wf_id, step_id, step, task_id, results):
        self._set_step_result_unsafe(wf_id, step_id, "step_done_ready", results)
        self._log_unsafe(wf_id, task_id, "step_done_ready",
                         detail=f"handoff waiting confirm: {step_id}")
        self._issue_approval_token(wf_id, step_id, step, task_id, results)
        return "step_done_ready"

    def _complete_review_unsafe(self, wf_id, step_id, step, task_id, results):
        self._set_step_result_unsafe(wf_id, step_id, "step_done_ready", results)
        self._log_unsafe(wf_id, task_id, "step_done_ready",
                         detail=f"review waiting approval: {step_id}")
        self._issue_approval_token(wf_id, step_id, step, task_id, results)
        return "step_done_ready"

    def _issue_approval_token(self, wf_id, step_id, step, task_id, results):
        """生成一次性审批密钥，存入 step_results，ccs send-safe 给 assigner。"""
        import secrets as _secrets
        import logging as _log
        logger = _log.getLogger("lifecycle.approval")

        token = _secrets.token_urlsafe(32)
        ttl = 3600  # 1 小时

        # 存储到 step_results
        wf = self._get_wf_unsafe(wf_id)
        step_results = self._parse_results(wf)
        step_results.setdefault(step_id, {}).update({
            "approval_token": token,
            "approval_token_ttl": time.time() + ttl,
            "approval_consumed_at": None,
        })
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(step_results, ensure_ascii=False), wf_id))

        # 确定审批人
        assigner = self._get_assigner_for_step(wf, step, step_id)
        if not assigner:
            return  # 无审批人，跳过

        # 构建审批提示词
        approval_prompt = step.get("approval_prompt", "")
        if not approval_prompt:
            step_title = step.get('title', '')
            completed_by = results.get(step_id, {}).get('completed_by', 'unknown')
            step_prompt = (step.get('prompt_template', '') or '')[:800]
            approval_prompt = (
                f"## 审批请求: 步骤 {step_id} - {step_title}\n\n"
                f"### 基本信息\n"
                f"- 工作流: {wf_id}\n"
                f"- 步骤: {step_id} - {step_title}\n"
                f"- 执行人: {completed_by}\n"
                f"- 审批密钥: {token}（有效期 1 小时）\n\n"
                f"### 审查要点\n"
                f"1. 完整性 - 是否完成步骤要求的所有产出？\n"
                f"2. 正确性 - 产出物是否符合质量标准？\n"
                f"3. 可追溯性 - 产出物路径和来源是否可验证？\n"
                f"4. 风险 - 是否存在未提及的风险或副作用？\n\n"
                f"### 步骤原始要求\n"
                f"{step_prompt}\n"
            )

        # ccs send-safe 给 assigner
        try:
            from workflow.client import get_ccs_cli as _get_ccs_cli
            import subprocess as _sp
            msg = (
                f"[workflow] 步骤审批请求\n\n"
                f"工作流: {wf_id}\n"
                f"步骤: {step_id} — {step.get('title', '')}\n"
                f"密钥: {token}\n"
                f"有效期: 1小时\n\n"
                f"审批提示:\n{approval_prompt}"
            )
            _ccs_cli = str(_get_ccs_cli()) if not isinstance(_get_ccs_cli(), str) else _get_ccs_cli()
            result = _sp.run(
                ["python3", _ccs_cli, "send", self.role, assigner, msg,
                 "--from", self.role],
                capture_output=True, timeout=15)
            if result.returncode != 0:
                logger.warning("ccs send failed for %s->%s: %s", self.role, assigner,
                               result.stderr.decode()[:200])
        except Exception as e:
            logger.warning("approval token CCS send error: %s", e)

    def _complete_subflow_unsafe(self, wf_id, step_id, step, task_id, results):
        """创建子工作流 + 启动，返回 step_done_ready（等 confirm 时检查子状态）。"""
        sub_tpl_id = step.get("subflow_template_id", "")
        sub_assignee = step.get("subflow_assignee", "")
        if not sub_tpl_id or not sub_assignee:
            self._set_step_result_unsafe(wf_id, step_id, "failed", results)
            self._log_unsafe(wf_id, task_id, "subflow_failed",
                             detail=f"missing subflow_template_id or subflow_assignee")
            return "failed"

        # 构建子任务描述：父 prompt + 步骤上下文
        step_prompt = step.get("prompt_template", "")
        parent_wf = self._get_wf_unsafe(wf_id)
        parent_task_id = parent_wf.get("task_id", "") if parent_wf else ""
        sub_title = f"[subflow] {step.get('title', sub_tpl_id)}"
        sub_desc = f"Parent: {wf_id} / {task_id}\nStep: {step_id}\nTemplate: {sub_tpl_id}\nPrompt:\n{step_prompt}"

        # 创建子 workflow_instance（通过 raw SQL，避免触发 Gate 递归）
        sub_wf_id = f"wf_{int(time.time()*1000) % 100000000}"
        child_instance_id = f"task_{__import__('uuid').uuid4().hex[:8]}"
        now = time.time()
        self._conn.execute("""
            INSERT INTO tasks (task_id, title, description, assigner, assignee,
                               status, created_at, updated_at, parent_task_id)
            VALUES (?, ?, ?, ?, ?, 'created', ?, ?, ?)
        """, (child_instance_id, sub_title, sub_desc, self.role, sub_assignee,
              now, now, parent_task_id))
        self._conn.execute("""
            INSERT INTO workflow_instances
                (instance_id, template_id, task_id, assigner, assignee,
                 status, current_step_id, created_at,
                 parent_wf_id, subflow_source_step_id)
            VALUES (?, ?, ?, ?, ?, 'pending', 's1', ?, ?, ?)
        """, (sub_wf_id, sub_tpl_id, child_instance_id, self.role, sub_assignee,
              now, wf_id, step_id))

        # 启动子工作流
        self._conn.execute(
            "UPDATE workflow_instances SET status='running' WHERE instance_id=?",
            (sub_wf_id,))
        self._conn.commit()

        # 记录到 step_results
        results[step_id] = {
            "status": "step_done_ready",
            "sub_wf_id": sub_wf_id,
            "sub_task_id": child_instance_id,
            "sub_template_id": sub_tpl_id,
            "completed_at": now,
            "completed_by": self.role,
        }
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(results, ensure_ascii=False), wf_id))
        self._log_unsafe(wf_id, task_id, "subflow_created",
                         detail=f"→ {sub_wf_id} ({sub_tpl_id})")
        self._issue_approval_token(wf_id, step_id, step, task_id, results)
        return "step_done_ready"

    def _complete_gate_unsafe(self, wf_id, step_id, step, task_id, results):
        check = step.get("completion_check", {})
        passed, msg = self._check_gate_condition(check)
        if passed:
            self._set_step_result_unsafe(wf_id, step_id, "completed", results)
            self._advance_unsafe(wf_id, task_id)
            self._log_unsafe(wf_id, task_id, "gate_passed",
                             detail=f"gate passed: {step_id}: {msg}")
            return "completed_and_advanced"
        else:
            self._set_step_result_unsafe(wf_id, step_id, "blocked", results)
            self._log_unsafe(wf_id, task_id, "gate_blocked",
                             detail=f"gate blocked: {step_id}: {msg}")
            return "gate_blocked"

    def _complete_notify_unsafe(self, wf_id, step_id, step, task_id, results):
        target = step.get("target_role", "")
        self._set_step_result_unsafe(wf_id, step_id, "completed", results)
        self._advance_unsafe(wf_id, task_id)
        self._log_unsafe(wf_id, task_id, "notify_sent",
                         detail=f"notify sent to {target}: {step_id}")
        return "completed_and_advanced"

    def _check_gate_condition(self, check: dict) -> tuple:
        if not check:
            return (True, "no conditions")
        output_exists = check.get("output_exists", [])
        if output_exists:
            missing = [p for p in output_exists if not Path(p).exists()]
            if missing:
                return (False, f"output not found: {', '.join(missing)}")
        return (True, "conditions satisfied")

    def _advance_unsafe(self, wf_id: str, task_id: Optional[str]) -> bool:
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return False
        steps = self._get_all_steps_unsafe(wf.get("template_id"))
        current = wf.get("current_step_id", "")
        idx = -1
        for i, s in enumerate(steps):
            if s.get("step_id") == current:
                idx = i
                break
        if idx == -1 or idx + 1 >= len(steps):
            self._conn.execute(
                "UPDATE workflow_instances SET status='completed', completed_at=? "
                "WHERE instance_id=?", (time.time(), wf_id)
            )
            self._sync_task_unsafe(task_id)
            return False
        next_step = steps[idx + 1]
        self._conn.execute(
            "UPDATE workflow_instances SET current_step_id=? WHERE instance_id=?",
            (next_step["step_id"], wf_id)
        )
        return True

    def _sync_task_unsafe(self, task_id: Optional[str]):
        if not task_id:
            return
        from workflow.sync import sync_task_status
        sync_task_status(self._conn, task_id)

    def _get_all_steps_unsafe(self, template_id: Optional[str]) -> list:
        t = self._get_template(template_id)
        return t.get("steps", []) if t else []

    def check_gate_timeouts(self) -> list[dict]:
        timeouts = []
        templates = self._conn.execute("SELECT * FROM workflow_templates").fetchall()
        for tpl_row in templates:
            tpl = dict(tpl_row)
            if not tpl.get("steps_json"):
                continue
            try:
                steps = json.loads(tpl["steps_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            for step in steps:
                if step.get("type") != "gate":
                    continue
                timeout_hours = step.get("estimated_hours", 24)
                escalation_role = step.get("target_role", "coordinator")
                rows = self._conn.execute(
                    "SELECT * FROM workflow_instances "
                    "WHERE template_id=? AND status='running' AND current_step_id=?",
                    (tpl["template_id"], step["step_id"])
                ).fetchall()
                for row in rows:
                    inst = dict(row)
                    elapsed = (time.time() - inst["created_at"]) / 3600
                    if elapsed > timeout_hours:
                        timeouts.append({
                            "wf_id": inst["instance_id"],
                            "step_id": step["step_id"],
                            "elapsed_hours": round(elapsed, 1),
                            "timeout_hours": timeout_hours,
                        })
        return timeouts

    def get_assigned_workflows(self, role: str) -> List[dict]:
        """获取分配给指定角色的所有活跃工作流"""
        rows = self._conn.execute(
            """SELECT wi.*, wt.name as template_name, wt.steps_json
               FROM workflow_instances wi
               JOIN workflow_templates wt ON wi.template_id = wt.template_id
               WHERE wi.status = 'running'
               ORDER BY wi.created_at DESC""",
        ).fetchall()
        result = []
        for r in rows:
            inst = dict(r)
            steps = json.loads(inst.get("steps_json", "[]"))
            current = inst.get("current_step_id", "")
            for s in steps:
                if s.get("target_role") == role and s.get("step_id") == current:
                    result.append(inst)
                    break
        return result

    def get_workflow_context(self, wf_id: str) -> dict:
        """获取工作流完整上下文"""
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return {}
        steps = self._get_all_steps_unsafe(wf.get("template_id"))
        current_step = wf.get("current_step_id", "")
        results = self._parse_results(wf)
        context = {
            "wf_id": wf_id,
            "template_name": wf.get("template_id"),
            "current_step": current_step,
            "status": wf.get("status"),
            "assigner": wf.get("assigner"),
            "steps": [],
        }
        for step in steps:
            step_result = results.get(step["step_id"], {})
            context["steps"].append({
                "step_id": step["step_id"],
                "title": step["title"],
                "type": step["type"],
                "target_role": step.get("target_role"),
                "status": step_result.get("status", "pending"),
                "approval_prompt": step.get("approval_prompt", ""),
            })
        return context

    def get_workflow_progress(self, wf_id: str) -> dict:
        """获取工作流实时进度"""
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return {}
        steps = self._get_all_steps_unsafe(wf.get("template_id"))
        current_step = wf.get("current_step_id", "")
        results = self._parse_results(wf)
        progress = {
            "wf_id": wf_id,
            "status": wf.get("status"),
            "current_step": current_step,
            "steps": [],
        }
        for i, step in enumerate(steps):
            step_result = results.get(step["step_id"], {})
            progress["steps"].append({
                "index": i + 1,
                "step_id": step["step_id"],
                "title": step["title"],
                "target_role": step.get("target_role"),
                "status": step_result.get("status", "pending"),
                "is_current": step["step_id"] == current_step,
                "handoff_status": step_result.get("handoff_status", "N/A"),
            })
        return progress

    def acknowledge_handoff(self, wf_id: str, step_id: str, role: str) -> bool:
        """被交接方确认收到任务"""
        wf = self._get_wf_unsafe(wf_id)
        if not wf:
            return False
        steps = self._get_all_steps_unsafe(wf.get("template_id"))
        current = wf.get("current_step_id", "")
        if current != step_id:
            return False
        idx = next((i for i, s in enumerate(steps) if s.get("step_id") == step_id), -1)
        if idx == -1 or idx + 1 >= len(steps):
            return False
        next_step = steps[idx + 1]
        if next_step.get("target_role") != role:
            return False
        results = self._parse_results(wf)
        step_result = results.get(step_id, {})
        step_result["handoff_status"] = "acknowledged"
        step_result["acknowledged_at"] = time.time()
        step_result["acknowledged_by"] = role
        results[step_id] = step_result
        self._conn.execute(
            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
            (json.dumps(results, ensure_ascii=False), wf_id))
        self._conn.commit()
        return True

    def _ensure_schema(self):
        """初始化数据库 schema（如不存在）。"""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflow_templates (
                template_id TEXT PRIMARY KEY, name TEXT, description TEXT, steps_json TEXT,
                created_at REAL, is_active INTEGER DEFAULT 1,
                trigger_scene TEXT, allowed_initiators TEXT, allowed_executors TEXT,
                max_duration_hours INTEGER DEFAULT 24, quality_standards TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS workflow_instances (
                instance_id TEXT PRIMARY KEY, template_id TEXT, task_id TEXT,
                assigner TEXT, assignee TEXT, status TEXT DEFAULT 'pending',
                current_step_id TEXT DEFAULT 's1', step_results TEXT, created_at REAL,
                completed_at REAL, parent_wf_id TEXT, subflow_source_step_id TEXT
            );
            CREATE TABLE IF NOT EXISTS workflow_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_instance_id TEXT, task_id TEXT,
                action TEXT, actor TEXT, detail TEXT, ts REAL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, title TEXT, description TEXT,
                assigner TEXT, assignee TEXT, priority INTEGER DEFAULT 0, status TEXT,
                created_at REAL, updated_at REAL, parent_task_id TEXT
            );
        """)

    def close(self):
        self._conn.close()

    # ── Engine 集成 ─────────────────────────────

    def get_run(self, wf_id: str) -> Optional[dict]:
        """返回工作流运行的轻量快照，供 engine 查询状态。"""
        wf = self.get_wf(wf_id)
        if not wf:
            return None
        template = self._get_template(wf.get("template_id"))
        steps = template.get("steps", []) if template else []
        results = self._parse_results(wf)
        current_step = wf.get("current_step_id", "")
        return {
            "id": wf_id,
            "status": wf.get("status", ""),
            "current_step": current_step,
            "steps": steps,
            "step_results": results,
            "created": wf.get("created_at"),
        }

    # ── 升级与重分配 ────────────────────────────

    def escalate_step(self, wf_id: str, step_id: str,
                      reason: str = "") -> dict:
        """升级步骤：标记 escalated + 写日志 + 返回 coordinator 角色名。"""
        with self._lock:
            results = self._parse_results(self._get_wf_unsafe(wf_id) or {})
            step_result = results.setdefault(step_id, {})
            step_result["escalated"] = True
            step_result["escalated_at"] = time.time()
            step_result["escalated_by"] = self.role
            if reason:
                step_result["escalation_reason"] = reason
            self._conn.execute(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(results, ensure_ascii=False), wf_id))
            self._conn.commit()
            task_id = (self._get_wf_unsafe(wf_id) or {}).get("task_id")
            self._log(wf_id, task_id, "step_escalated",
                      detail=f"step {step_id}: {reason}")
            return {"wf_id": wf_id, "step_id": step_id, "role": "coordinator"}

    def reassign_step(self, wf_id: str, step_id: str, new_role: str) -> bool:
        """重分配步骤到新角色：改 step_results 中的 target_role。"""
        with self._lock:
            results = self._parse_results(self._get_wf_unsafe(wf_id) or {})
            step_result = results.setdefault(step_id, {})
            step_result["reassigned_to"] = new_role
            step_result["reassigned_at"] = time.time()
            step_result["reassigned_by"] = self.role
            self._conn.execute(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(results, ensure_ascii=False), wf_id))
            self._conn.commit()
            task_id = (self._get_wf_unsafe(wf_id) or {}).get("task_id")
            self._log(wf_id, task_id, "step_reassigned",
                      detail=f"{step_id} → {new_role}")
            return True
