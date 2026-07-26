#!/usr/bin/env python3
"""
workflow_gate.py — 模板门禁系统

在 create_task 时校验调用者角色权限和模板有效性。
独立文件，依赖 template_registry 获取模板定义。
"""

import json
import sqlite3
import time
from pathlib import Path
from typing import Optional

from paths import WORKFLOWS_DB as DB_PATH, SESSION_ROLES_PERSONAS
ROLE_REGISTRY: list[str] = []


def _load_role_registry() -> list[str]:
    """从 persona JSON 加载有效角色列表。"""
    if ROLE_REGISTRY:
        return ROLE_REGISTRY
    persona_dir = SESSION_ROLES_PERSONAS
    if not persona_dir.exists():
        return []
    roles = set()
    for f in sorted(persona_dir.glob("persona_*.json")):
        try:
            with open(f) as fh:
                d = json.load(fh)
            roles.add(d.get("assignee", d.get("name", "")))
        except (json.JSONDecodeError, OSError):
            continue
    roles.discard("")
    ROLE_REGISTRY.clear()
    ROLE_REGISTRY.extend(sorted(roles))
    return ROLE_REGISTRY


class Gate:
    """模板门禁：校验调用者权限和模板有效性。"""

    def __init__(self, db_path: str = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row

    # ── 模板校验 ─────────────────────────────────

    def is_template_active(self, template_id: str) -> bool:
        """检查模板是否存在且已激活。"""
        try:
            row = self._conn.execute(
                "SELECT is_active FROM workflow_templates WHERE template_id=?",
                (template_id,)
            ).fetchone()
            if not row:
                return False
            val = row["is_active"]
        except sqlite3.OperationalError:
            # schema 无 is_active 列 → 降级为检查 exists
            return self.is_template_exists(template_id)
        # SQLite 可能返回 TEXT('1') 或 INTEGER(1)
        return val in (1, "1", True)

    def is_template_exists(self, template_id: str) -> bool:
        """检查模板是否存在。"""
        row = self._conn.execute(
            "SELECT 1 FROM workflow_templates WHERE template_id=?",
            (template_id,)
        ).fetchone()
        return row is not None

    def get_template(self, template_id: str) -> Optional[dict]:
        """获取模板完整定义。"""
        from template_registry import TemplateRegistry
        reg = TemplateRegistry(str(self.db_path))
        try:
            return reg.get(template_id)
        finally:
            reg.close()

    # ── 角色校验 ─────────────────────────────────

    def check_can_initiate(self, role: str, template_id: str) -> bool:
        """检查角色是否允许发起此模板的工作流。"""
        try:
            row = self._conn.execute(
                "SELECT allowed_initiators FROM workflow_templates WHERE template_id=?",
                (template_id,)
            ).fetchone()
        except sqlite3.OperationalError:
            # schema 无 allowed_initiators 列 → 跳过权限校验
            return True
        if not row or not row["allowed_initiators"]:
            # no allowed_initiators set → backward compatible, allow all
            return True
        try:
            initiators = json.loads(row["allowed_initiators"])
        except (json.JSONDecodeError, TypeError):
            initiators = []
        return role in initiators

    def check_can_execute(self, role: str, template_id: str) -> bool:
        """检查角色是否为模板的允许执行者。"""
        try:
            row = self._conn.execute(
                "SELECT allowed_executors FROM workflow_templates WHERE template_id=?",
                (template_id,)
            ).fetchone()
        except sqlite3.OperationalError:
            # schema 无 allowed_executors 列 → 跳过权限校验
            return True
        if not row or not row["allowed_executors"]:
            # no allowed_executors set → backward compatible, allow all
            return True
        try:
            executors = json.loads(row["allowed_executors"])
        except (json.JSONDecodeError, TypeError):
            executors = []
        return role in executors

    def is_valid_role(self, role: str) -> bool:
        """检查角色名是否存在。"""
        return role in _load_role_registry()

    # ── 完整校验链 ───────────────────────────────

    def validate_create_task(self, template_id: str,
                             initiator_role: str,
                             assignee: str) -> None:
        """完整的 create_task 门禁校验链。

        通过 → 返回 None
        失败 → 抛出 ValueError 或 PermissionError
        """
        # 1. template_id 必填
        if not template_id:
            raise ValueError("template_id is required")

        # 2. template_id 存在
        if not self.is_template_exists(template_id):
            raise ValueError(f"template_id not found: {template_id}")

        # 3. template_id 已激活
        if not self.is_template_active(template_id):
            raise ValueError(f"template is inactive: {template_id}")

        # 4. 发起角色存在（pipeline/system 跳过——路由 daemon 自动创建）
        if initiator_role not in ("pipeline", "system"):
            if not self.is_valid_role(initiator_role):
                raise PermissionError(
                    f"role not found in role registry: {initiator_role}")

            # 5. 发起角色在 allowed_initiators 中
            if not self.check_can_initiate(initiator_role, template_id):
                raise PermissionError(
                    f"not allowed to initiate: {initiator_role}")

        # 6. assignee 在 allowed_executors 中
        if not self.check_can_execute(assignee, template_id):
            raise ValueError(f"not a valid executor: {assignee}")

    # ── P0 豁免 ──────────────────────────────────

    def record_p0_audit(self, role: str, task_id: str,
                        reason: str, conn: sqlite3.Connection = None):
        """记录 P0 豁免审计日志。"""
        target = conn or self._conn
        target.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (None, task_id, "p0_exemption", role,
             json.dumps({"reason": reason}, ensure_ascii=False), time.time())
        )
        target.commit()

    # ── 分配者链 ──────────────────────────────────

    def route_task(self, task_id: str, from_role: str, to_role: str) -> None:
        """Route task through the chain (3-level: coordinator→dispatcher→executor)."""
        self._conn.execute(
            "INSERT INTO workflow_logs (workflow_instance_id, task_id, "
            "action, actor, detail, ts) VALUES (?, ?, ?, ?, ?, ?)",
            (None, task_id, "routed", from_role,
             json.dumps({"chain": f"{from_role}→{to_role}"}, ensure_ascii=False), time.time())
        )
        self._conn.commit()

    def close(self):
        self._conn.close()
