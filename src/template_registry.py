#!/usr/bin/env python3
"""
template_registry.py — 模板注册中心

10 字段 JSON Schema 验证，register/get/list/activate/deactivate/validate。
独立文件，无外部依赖（仅标准库 + 已安装的 sqlite3）。
"""

import json
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional

from paths import WORKFLOWS_DB as DB_PATH

# ── 有效角色列表（从 persona JSON 动态加载） ──────
_ROLE_REGISTRY: list[str] = []


def _load_role_registry() -> list[str]:
    if _ROLE_REGISTRY:
        return _ROLE_REGISTRY
    persona_dir = Path.home() / "hermes-session-roles" / "personas" / "session-roles"
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
    _ROLE_REGISTRY.clear()
    _ROLE_REGISTRY.extend(sorted(roles))
    return _ROLE_REGISTRY


def get_role_registry() -> list[str]:
    return _load_role_registry()


# ── 10 字段 JSON Schema ──────────────────────────

TEMPLATE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": [
        "workflow_id", "name", "description", "trigger_scene",
        "allowed_initiators", "allowed_executors", "steps",
        "max_duration_hours", "quality_standards"
    ],
    "properties": {
        "workflow_id": {"type": "string", "pattern": "^WL-\\d{2}$"},
        "name": {"type": "string", "minLength": 2},
        "description": {"type": "string", "minLength": 10},
        "trigger_scene": {
            "type": "array",
            "items": {"type": "string", "minLength": 5},
            "minItems": 1
        },
        "allowed_initiators": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1
        },
        "allowed_executors": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1
        },
        "steps": {
            "type": "array",
            "minItems": 1,
            "items": {"$ref": "#/definitions/Step"}
        },
        "max_duration_hours": {"type": "number", "minimum": 1},
        "quality_standards": {"type": "string", "minLength": 10}
    },
    "definitions": {
        "Step": {
            "type": "object",
            "required": ["step_id", "title", "type", "prompt_template"],
            "properties": {
                "step_id": {"type": "string", "pattern": "^s\\d+$"},
                "title": {"type": "string", "minLength": 2},
                "type": {
                    "type": "string",
                    "enum": ["handoff", "review", "single", "gate", "notify"]
                },
                "target_role": {"type": "string"},
                "prompt_template": {"type": "string", "minLength": 30},  # 禁止缩减提示词长度
                "completion_check": {
                    "type": "object",
                    "properties": {
                        "output_exists": {
                            "type": "array", "items": {"type": "string"}
                        },
                        "contains_keyword": {
                            "type": "array", "items": {"type": "string"}
                        },
                        "review_required": {"type": "boolean"}
                    }
                },
                "failure_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 2
                },
                # AI不需要工时描述，禁止填写此字段
                # "estimated_hours": 禁止使用
            }
        }
    }
}


# ── 简易 JSON Schema 校验器（无需第三方库） ──────

class ValidationReport:
    """校验结果报告。"""

    def __init__(self):
        self.passed = True
        self.errors: list[str] = []

    def add_error(self, msg: str):
        self.passed = False
        self.errors.append(msg)

    def to_dict(self) -> dict:
        return {"passed": self.passed, "errors": self.errors}

    def __repr__(self) -> str:
        return f"ValidationReport(passed={self.passed}, errors={self.errors})"


def _validate_schema(template: dict) -> ValidationReport:
    """对模板做 10 字段 JSON Schema 校验。"""
    report = ValidationReport()

    # 检查必填字段
    for field in TEMPLATE_SCHEMA["required"]:
        if field not in template:
            report.add_error(f"缺少必填字段: {field}")

    if not report.passed:
        return report

    # workflow_id 格式
    wid = template.get("workflow_id", "")
    if not re.match(r"^WL-\d{2}$", wid):
        report.add_error(f"workflow_id 格式无效: {wid}，需为 WL-XX 格式")

    # name
    if not isinstance(template.get("name"), str) or len(template["name"]) < 2:
        report.add_error(f"name 需≥2字符")

    # description
    if not isinstance(template.get("description"), str) or len(template["description"]) < 10:
        report.add_error(f"description 需≥10字符")

    # trigger_scene
    ts = template.get("trigger_scene", [])
    if not isinstance(ts, list) or len(ts) < 1:
        report.add_error("trigger_scene 需为非空数组")
    else:
        for i, s in enumerate(ts):
            if not isinstance(s, str) or len(s) < 5:
                report.add_error(f"trigger_scene[{i}] 需≥5字符")

    # allowed_initiators
    ai = template.get("allowed_initiators", [])
    if not isinstance(ai, list) or len(ai) < 1:
        report.add_error("allowed_initiators 需为非空数组")

    # allowed_executors
    ae = template.get("allowed_executors", [])
    if not isinstance(ae, list) or len(ae) < 1:
        report.add_error("allowed_executors 需为非空数组")

    # steps
    steps = template.get("steps", [])
    if not isinstance(steps, list) or len(steps) < 1:
        report.add_error("steps 需为非空数组")
    else:
        for i, step in enumerate(steps):
            _validate_step(step, i, report)

    # max_duration_hours
    mdh = template.get("max_duration_hours")
    if not isinstance(mdh, (int, float)) or mdh < 1:
        report.add_error("max_duration_hours 需为≥1 的数字")

    # quality_standards
    if not isinstance(template.get("quality_standards"), str) or len(template["quality_standards"]) < 10:
        report.add_error("quality_standards 需≥10字符")

    return report


def _validate_step(step: dict, index: int, report: ValidationReport):
    """校验单个步骤。"""
    # 必填字段
    for field in ["step_id", "title", "type", "prompt_template"]:
        if field not in step:
            report.add_error(f"steps[{index}] 缺少必填字段: {field}")

    if "step_id" in step:
        sid = step["step_id"]
        if not isinstance(sid, str) or not re.match(r"^s\d+$", sid):
            report.add_error(f"steps[{index}].step_id 格式无效: {sid}，需为 s+数字")

    if "type" in step:
        valid_types = {"handoff", "review", "single", "gate", "notify"}
        if step["type"] not in valid_types:
            report.add_error(f"steps[{index}].type 无效: {step['type']}，需为 {valid_types}")

    # prompt_template 禁止缩减，必须保持完整详细
    if "prompt_template" in step:
        pt = step["prompt_template"]
        if isinstance(pt, str):
            # 三段式检查：做什么、怎么做、验收标准
            sections = ["做什么", "怎么做", "验收标准"]
            for sec in sections:
                if sec not in pt:
                    report.add_error(f"steps[{index}].prompt_template 缺少'{sec}'段")

    # failure_patterns 至少 2 项
    fp = step.get("failure_patterns", [])
    if not isinstance(fp, list) or len(fp) < 2:
        report.add_error(f"steps[{index}].failure_patterns 需≥2 项")

    # AI不需要工时描述，禁止填写 estimated_hours
    if "estimated_hours" in step:
        report.add_error(f"steps[{index}].estimated_hours 禁止使用（AI不需要工时描述）")


def _check_role_existence(template: dict) -> ValidationReport:
    """检查模板中引用的角色是否存在。"""
    report = ValidationReport()
    roles = set(_load_role_registry())

    if not roles:
        return report  # 角色库未加载时跳过

    for field in ("allowed_initiators", "allowed_executors"):
        for role in template.get(field, []):
            if role not in roles:
                report.add_error(f"角色不存在: '{role}'（{field} 中引用）")

    steps = template.get("steps", [])
    if not isinstance(steps, list):
        report.add_error("steps 字段应为数组类型")
        return report
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            report.add_error(f"steps[{i}] 应为对象类型")
            continue
        target = step.get("target_role", "")
        if target and target not in roles:
            report.add_error(f"角色不存在: '{target}'（steps[{i}].target_role）")

    return report


# ── 模板注册中心 ──────────────────────────────────

class TemplateRegistry:
    """模板注册中心：管理 10 字段模板的注册、查询、激活、校验。"""

    def __init__(self, db_path: str = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._conn = sqlite3.connect(str(self.db_path), timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()

    def _ensure_schema(self):
        """确保模板相关的表和索引存在（含迁移）。"""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflow_templates (
                template_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                steps_json TEXT NOT NULL,
                steps_mermaid TEXT,
                created_at REAL NOT NULL
            );
        """)
        # 迁移：添加新列（V2 schema 扩展）
        _migrate_cols = [
            ("is_active", "INTEGER NOT NULL DEFAULT 1"),
            ("trigger_scene", "TEXT"),
            ("allowed_initiators", "TEXT"),
            ("allowed_executors", "TEXT"),
            ("max_duration_hours", "REAL"),
            ("quality_standards", "TEXT"),
        ]
        existing = {r[1] for r in self._conn.execute(
            "PRAGMA table_info(workflow_templates)").fetchall()}
        for col_name, col_def in _migrate_cols:
            if col_name not in existing:
                self._conn.execute(
                    f"ALTER TABLE workflow_templates ADD COLUMN {col_name} {col_def}")
        self._conn.commit()

    # ── 寄存器 ────────────────────────────────────

    def register(self, template: dict) -> str:
        """注册一个新模板。

        校验通过后写入 DB，返回 template_id。
        校验不通过抛出 ValueError，错误信息指向缺失字段。
        """
        # 合并校验
        schema_report = _validate_schema(template)
        role_report = _check_role_existence(template)

        full_report = ValidationReport()
        full_report.errors = schema_report.errors + role_report.errors
        full_report.passed = len(full_report.errors) == 0

        if not full_report.passed:
            raise ValueError(f"模板校验失败: {'; '.join(full_report.errors)}")

        template_id = template.get("workflow_id", f"WL-{uuid.uuid4().hex[:4].upper()}")

        # 检查重复
        existing = self._conn.execute(
            "SELECT 1 FROM workflow_templates WHERE template_id=?", (template_id,)
        ).fetchone()
        if existing:
            raise ValueError(f"模板 '{template_id}' 已存在")

        now = time.time()

        self._conn.execute("""
            INSERT OR REPLACE INTO workflow_templates
                (template_id, name, description, steps_json,
                 trigger_scene, allowed_initiators, allowed_executors,
                 max_duration_hours, quality_standards, created_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            template_id,
            template["name"],
            template["description"],
            json.dumps(template["steps"], ensure_ascii=False),
            json.dumps(template.get("trigger_scene", []), ensure_ascii=False),
            json.dumps(template.get("allowed_initiators", []), ensure_ascii=False),
            json.dumps(template.get("allowed_executors", []), ensure_ascii=False),
            template.get("max_duration_hours", 24),
            template.get("quality_standards", ""),
            now,
        ))
        self._conn.commit()
        return template_id

    # ── 查询 ──────────────────────────────────────

    def get(self, template_id: str) -> Optional[dict]:
        """返回模板完整定义。不存在返回 None。"""
        row = self._conn.execute(
            "SELECT * FROM workflow_templates WHERE template_id=?", (template_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_template(row)

    def list(self, active_only: bool = False) -> list[dict]:
        """列出模板。active_only=True 只返回已激活的。"""
        query = "SELECT * FROM workflow_templates"
        params = []
        if active_only:
            query += " WHERE is_active=1"
        query += " ORDER BY template_id"
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_template(r) for r in rows]

    # ── 激活/停用 ─────────────────────────────────

    def activate(self, template_id: str) -> bool:
        """激活模板。不存在返回 False。"""
        cur = self._conn.execute(
            "UPDATE workflow_templates SET is_active=1 WHERE template_id=?",
            (template_id,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    def deactivate(self, template_id: str) -> bool:
        """停用模板。不存在返回 False。"""
        cur = self._conn.execute(
            "UPDATE workflow_templates SET is_active=0 WHERE template_id=?",
            (template_id,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    # ── 纯校验（不入库） ───────────────────────────

    def validate(self, template: dict) -> dict:
        """校验模板，返回 ValidationReport 字典。不入库。"""
        schema_report = _validate_schema(template)
        role_report = _check_role_existence(template)

        full_report = ValidationReport()
        full_report.errors = schema_report.errors + role_report.errors
        full_report.passed = len(full_report.errors) == 0

        return full_report.to_dict()

    def get_allowed_roles(self, template_id: str) -> Optional[dict]:
        """返回模板的允许角色矩阵。"""
        t = self.get(template_id)
        if not t:
            return None
        return {
            "allowed_initiators": t.get("allowed_initiators", []),
            "allowed_executors": t.get("allowed_executors", []),
        }

    # ── 内部辅助 ─────────────────────────────────

    @staticmethod
    def _row_to_template(row: sqlite3.Row) -> dict:
        d = dict(row)
        # 接口兼容：template_id 和 workflow_id 同时可用
        if "template_id" in d and "workflow_id" not in d:
            d["workflow_id"] = d["template_id"]
        if "workflow_id" in d and "template_id" not in d:
            d["template_id"] = d["workflow_id"]
        # 解析 JSON 字段
        for field in ["steps_json", "trigger_scene",
                       "allowed_initiators", "allowed_executors"]:
            val = d.get(field)
            if isinstance(val, str):
                try:
                    d[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
        # 别名
        if "steps_json" in d:
            d["steps"] = d.pop("steps_json")
        # SQLite 布尔值转 Python bool
        if "is_active" in d:
            d["is_active"] = bool(d["is_active"])
        return d

    def close(self):
        self._conn.close()


# ── CLI 使用 ──────────────────────────────────────

def _cli_register(args: list[str]):
    """从 JSON 文件注册模板。"""
    if not args:
        print("用法: python3 template_registry.py register <template.json>")
        return
    path = Path(args[0])
    if not path.exists():
        print(f"文件不存在: {path}")
        return
    with open(path) as fh:
        template = json.load(fh)
    reg = TemplateRegistry()
    try:
        tid = reg.register(template)
        print(f"注册成功: {tid}")
    except ValueError as e:
        print(f"注册失败: {e}")
    finally:
        reg.close()


def _cli_list(args: list[str]):
    reg = TemplateRegistry()
    templates = reg.list()
    if not templates:
        print("无模板")
    else:
        for t in templates:
            status = "活跃" if t.get("is_active") else "停用"
            print(f"  {t['template_id']} — {t['name']} [{status}]")
    reg.close()


def _cli_validate(args: list[str]):
    if not args:
        print("用法: python3 template_registry.py validate <template.json>")
        return
    path = Path(args[0])
    if not path.exists():
        print(f"文件不存在: {path}")
        return
    with open(path) as fh:
        template = json.load(fh)
    reg = TemplateRegistry()
    report = reg.validate(template)
    if report["passed"]:
        print("校验通过 ✅")
    else:
        print(f"校验失败 ({len(report['errors'])} 个错误):")
        for e in report["errors"]:
            print(f"  ❌ {e}")
    reg.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"用法: {sys.argv[0]} <register|list|validate> [args...]")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "register":
        _cli_register(sys.argv[2:])
    elif cmd == "list":
        _cli_list(sys.argv[2:])
    elif cmd == "validate":
        _cli_validate(sys.argv[2:])
    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)
