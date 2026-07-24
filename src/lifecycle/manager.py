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

    # ── 状态查询 ─────────────────────────────────

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

    def start_wf(self, wf_id: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    "UPDATE workflow_instances SET status='running' "
                    "WHERE instance_id=? AND status='pending'", (wf_id,)
                )
                self._conn.commit()
                if cur.rowcount == 0:
                    return False
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                self._log(wf_id, task_id, "wf_started",
                          detail=f"workflow started by {self.role}")
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
        if st == "handoff":
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
            from workflow.client import CCS_CLI
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
            result = _sp.run(
                ["python3", str(CCS_CLI), "send", "coordinator", executor, msg,
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

    def close_wf(self, wf_id: str):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE workflow_instances SET status='completed', completed_at=? "
                    "WHERE instance_id=?", (time.time(), wf_id)
                )
                wf = self.get_wf(wf_id)
                task_id = wf.get("task_id") if wf else None
                self._sync_task_unsafe(task_id)
                self._log_unsafe(wf_id, task_id, "wf_closed", detail="workflow completed")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

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
            approval_prompt = (
                f"步骤 {step_id}（{step.get('title', '')}）已完成，请审查质量并确认。\n"
                f"完成者: {results.get(step_id, {}).get('completed_by', '未知')}\n"
                f"用密钥审批: python3 src/ccs.py send {self.role} {assigner} "
                f'"confirm {wf_id} {step_id} {token}"'
            )

        # ccs send-safe 给 assigner
        try:
            from workflow.client import CCS_CLI
            import subprocess as _sp
            msg = (
                f"[workflow] 步骤审批请求\n\n"
                f"工作流: {wf_id}\n"
                f"步骤: {step_id} — {step.get('title', '')}\n"
                f"密钥: {token}\n"
                f"有效期: 1小时\n\n"
                f"审批提示:\n{approval_prompt}"
            )
            result = _sp.run(
                ["python3", str(CCS_CLI), "send", self.role, assigner, msg,
                 "--from", self.role],
                capture_output=True, timeout=15)
            if result.returncode != 0:
                logger.warning("ccs send failed for %s->%s: %s", self.role, assigner,
                               result.stderr.decode()[:200])
        except Exception as e:
            logger.warning("approval token CCS send error: %s", e)

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
        rows = self._conn.execute(
            "SELECT status FROM workflow_instances WHERE task_id=?", (task_id,)
        ).fetchall()
        statuses = [dict(r)["status"] for r in rows]
        if not statuses:
            return
        if all(s == "completed" for s in statuses):
            ts = "completed"
        elif any(s == "failed" for s in statuses):
            ts = "failed"
        elif any(s in ("running", "pending", "step_done_ready") for s in statuses):
            ts = "in_progress"
        else:
            ts = "completed"
        self._conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
            (ts, time.time(), task_id)
        )

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

    def close(self):
        self._conn.close()
