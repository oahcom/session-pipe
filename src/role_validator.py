#!/usr/bin/env python3
"""
role_validator.py — 角色名校验装饰器

提取 is_valid_role 逻辑为可复用的装饰器。
"""

from functools import wraps
from typing import Callable, Optional

from paths import SESSION_ROLES_PERSONAS


_ROLE_REGISTRY: list[str] = []


def _load_role_registry() -> list[str]:
    """从 persona JSON 加载有效角色列表。"""
    global _ROLE_REGISTRY
    if _ROLE_REGISTRY:
        return _ROLE_REGISTRY
    persona_dir = SESSION_ROLES_PERSONAS
    if not persona_dir.exists():
        return []
    roles = set()
    for f in sorted(persona_dir.glob("persona_*.json")):
        try:
            with open(f) as fh:
                import json
                d = json.load(fh)
            roles.add(d.get("assignee", d.get("name", "")))
        except (json.JSONDecodeError, OSError):
            continue
    roles.discard("")
    _ROLE_REGISTRY.clear()
    _ROLE_REGISTRY.extend(sorted(roles))
    return _ROLE_REGISTRY


def is_valid_role(role: str) -> bool:
    """检查角色名是否存在。"""
    return role in _load_role_registry()


def validate_role(
    arg_name: str = "role",
    *,
    allow_system: bool = True,
    on_invalid: Optional[Callable[[str], None]] = None,
) -> Callable:
    """
    装饰器：校验函数的 role 参数是否为有效角色名。

    Parameters
    ----------
    arg_name:
        要校验的参数名（默认 "role"），支持位置参数或关键字参数。
    allow_system:
        是否允许 "pipeline" / "system" 等内置角色跳过校验（默认 True）。
    on_invalid:
        可选回调，校验失败时调用（如记录审计日志），再抛出异常。

    Raises
    ------
    PermissionError
        如果角色不在注册表中且不是被允许的系统角色。
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            import inspect
            sig = inspect.signature(fn)
            bound = sig.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            role_val = bound.arguments.get(arg_name)
            if role_val is None:
                raise TypeError(f"missing required argument: {arg_name}")

            system_roles = {"pipeline", "system"} if allow_system else set()
            if role_val not in system_roles and not is_valid_role(role_val):
                if on_invalid:
                    on_invalid(role_val)
                raise PermissionError(f"role not found in role registry: {role_val}")

            return fn(*args, **kwargs)
        return wrapper
    return decorator


# ── 便捷别名 ────────────────────────────────────
validate_role_name = validate_role  # 兼容旧叫法
