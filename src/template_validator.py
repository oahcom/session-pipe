"""template_validator.py — 模板验证逻辑（从 template_registry.py 提取）。"""

from dataclasses import dataclass
from pathlib import Path

from role_manager import load_roles, get_role

# ── ValidationReport ──
@dataclass
class ValidationReport:
    """模板验证报告。"""
    is_valid: bool
    errors: list[str]
    warnings: list[str]


# ── Schema 验证 ──
# ── 10 字段 Schema ──
TEMPLATE_SCHEMA_REQUIRED = [
    "workflow_id", "name", "description", "trigger_scene",
    "allowed_initiators", "allowed_executors", "steps",
    "max_duration_hours", "quality_standards"
]


def _validate_schema(tpl: dict) -> ValidationReport:
    """验证模板的顶层 Schema。"""
    errors = []
    warnings = []

    # 必需字段（10 字段 Schema）
    for f in TEMPLATE_SCHEMA_REQUIRED:
        if f not in tpl:
            errors.append(f"缺少必需字段: {f}")

    # 类型检查
    if "steps" in tpl and not isinstance(tpl["steps"], list):
        errors.append("steps 必须是列表")

    if "initiator_roles" in tpl and not isinstance(tpl.get("initiator_roles"), list):
        errors.append("initiator_roles 必须是列表")

    if "assigner_roles" in tpl and not isinstance(tpl.get("assigner_roles"), list):
        errors.append("assigner_roles 必须是列表")

    # 空检查
    if "name" in tpl and not tpl["name"]:
        errors.append("name 不能为空")

    return ValidationReport(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def _validate_step(step: dict, step_index: int, all_step_ids: set) -> ValidationReport:
    """验证单个步骤。"""
    errors = []
    warnings = []
    prefix = f"步骤 {step_index + 1}"

    # 兼容 id 和 step_id 两种键名
    step_id = step.get("id") or step.get("step_id")
    if not step_id:
        errors.append(f"{prefix}: 缺少 id")
    elif step_id in all_step_ids:
        errors.append(f"{prefix}: 重复的 id: {step_id}")
    else:
        all_step_ids.add(step_id)

    if not step.get("type"):
        errors.append(f"{prefix}: 缺少 type")
    elif step["type"] not in {"handoff", "review", "single", "gate", "notify"}:
        errors.append(f"{prefix}: 未知类型: {step['type']}")

    if not step.get("target_role"):
        errors.append(f"{prefix}: 缺少 target_role")
    elif not _role_exists(step["target_role"]):
        errors.append(f"{prefix}: 目标角色不存在: {step['target_role']}")

    # type 特定验证
    stype = step.get("type")
    if stype == "gate" and not step.get("conditions"):
        warnings.append(f"{prefix}: gate 类型建议配置 conditions")
    if stype == "handoff" and not step.get("confirm_needed"):
        warnings.append(f"{prefix}: handoff 类型建议设置 confirm_needed=true")

    # prompt_template 三段式验证
    prompt = step.get("prompt_template", "")
    if prompt:
        required_sections = ["做什么", "怎么做", "验收标准"]
        missing_sections = [s for s in required_sections if s not in prompt]
        if missing_sections:
            errors.append(f"{prefix}: prompt 缺少部分: {', '.join(missing_sections)}")

    return ValidationReport(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def _role_exists(role_name: str) -> bool:
    """检查角色是否在注册表中。"""
    roles = load_roles()
    return any(r.get("name") == role_name for r in roles)


def _check_role_existence(tpl: dict) -> ValidationReport:
    """检查模板引用的所有角色是否存在。"""
    errors = []
    warnings = []

    # 检查 initiator_roles / allowed_initiators
    for key in ("initiator_roles", "allowed_initiators"):
        for r in tpl.get(key, []):
            if not _role_exists(r):
                errors.append(f"{key} 引用了不存在的角色: {r}")

    # 检查 assigner_roles / allowed_executors
    for key in ("assigner_roles", "allowed_executors"):
        for r in tpl.get(key, []):
            if not _role_exists(r):
                errors.append(f"{key} 引用了不存在的角色: {r}")

    # 检查步骤中的 target_role
    for i, step in enumerate(tpl.get("steps", [])):
        tr = step.get("target_role")
        if tr and not _role_exists(tr):
            errors.append(f"步骤 {i + 1} (id={step.get('id', '?')}) 引用了不存在的角色: {tr}")

    return ValidationReport(is_valid=len(errors) == 0, errors=errors, warnings=warnings)



def run_validation(tpl: dict) -> list[dict]:
    """运行完整模板验证，返回 5 步验证结果列表。
    
    Args:
        tpl: 模板字典
        
    Returns:
        List of 5 dicts with: step (int), name (str), passed (bool), errors (list)
    """
    results = []
    all_step_ids = set()
    
    # Step 1: Schema 验证
    schema_report = _validate_schema(tpl)
    results.append({
        "step": 1,
        "name": "schema",
        "passed": schema_report.is_valid,
        "errors": schema_report.errors,
    })
    
    # Step 2: 角色存在性
    role_report = _check_role_existence(tpl)
    results.append({
        "step": 2,
        "name": "role_existence",
        "passed": role_report.is_valid,
        "errors": role_report.errors,
    })
    
    # Step 3: 步骤内部验证
    step_errors = []
    for i, step in enumerate(tpl.get("steps", [])):
        step_report = _validate_step(step, i, all_step_ids)
        step_errors.extend(step_report.errors)
    results.append({
        "step": 3,
        "name": "step_validation",
        "passed": len(step_errors) == 0,
        "errors": step_errors,
    })
    
    # Step 4: completion_check 验证
    cc_errors = []
    for i, step in enumerate(tpl.get("steps", [])):
        if step.get("type") in {"review", "gate"}:
            if not step.get("completion_check"):
                cc_errors.append(f"步骤 {i+1} (id={step.get('id','?')}): {step['type']} 类型缺少 completion_check")
    results.append({
        "step": 4,
        "name": "completion_check",
        "passed": len(cc_errors) == 0,
        "errors": cc_errors,
    })
    
    # Step 5: 一致性检查
    consistency_errors = []
    step_types = {"handoff", "review", "single", "gate", "notify"}
    for i, step in enumerate(tpl.get("steps", [])):
        st = step.get("type")
        if st and st not in step_types:
            consistency_errors.append(f"步骤 {i+1}: 未知类型 '{st}'")
    seen_ids = set()
    for i, step in enumerate(tpl.get("steps", [])):
        sid = step.get("id")
        if sid:
            if sid in seen_ids:
                consistency_errors.append(f"重复的 step_id: {sid}")
            seen_ids.add(sid)
    results.append({
        "step": 5,
        "name": "consistency",
        "passed": len(consistency_errors) == 0,
        "errors": consistency_errors,
    })
    
    return results
