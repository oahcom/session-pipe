#!/usr/bin/env python3
"""eval 共享加载器 — 统一角色+eval_criteria 加载与解析，消除双 schema 漂移。

这是 eval_checker 与 eval_consistency 的单一真相源。

解析（_extract_cmd）与校验（_check_cmd）都委托给 eval_checker —— 延迟导入，
因为 eval_checker 反向依赖本模块的 _load_roles。此前这里各写了一份副本：
白名单通过模块级 `from eval_checker import` 获取，导入顺序不同会落到 51 项的
fallback 静态副本（真表 29 项），且静态校验把引号内的 '|' 与 for 首词误判为
不兼容 —— 同一批 persona 在静态检查报 10 项、运行时只拒 3 项。
"""

import json
from pathlib import Path

# personas 目录（与 eval_checker 一致）
PERSONAS_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"


def _extract_cmd(criterion: str) -> str | None:
    """从 eval_criterion 提取验证命令；委托 eval_checker 的同名实现。"""
    from eval_checker import _extract_cmd as _ec_extract
    extracted = _ec_extract(criterion)
    return extracted[0] if extracted else None


def _check_cmd(cmd: str) -> list[str]:
    """检查命令能否被运行时执行。返回不兼容原因列表（空=兼容）。

    与 eval_checker._run 共用同一个 _preflight，静态预检不再与运行时判定分叉。
    """
    from eval_checker import _preflight
    _, rejected = _preflight(cmd)
    return [rejected["stderr"]] if rejected else []


def _load_roles(personas_dir: Path | None = None) -> list[dict]:
    """加载所有角色 JSON，返回统一格式：[{name, eval_criteria: [criterion...], issues: [...]}]。

    单一真相源：eval_checker 和 eval_consistency 均从此导入。
    """
    if personas_dir is None:
        personas_dir = PERSONAS_DIR

    if not personas_dir.is_dir():
        return []

    roles = []
    for f in sorted(personas_dir.glob("persona_*.json")):
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        criteria = d.get("eval_criteria", [])
        role_issues = []
        for c in criteria:
            cmd = _extract_cmd(c)
            if cmd is None:
                continue
            issues = _check_cmd(cmd)
            if issues:
                role_issues.append({"criterion": c[:120], "issues": issues})
        roles.append({"name": d.get("name", f.stem), "eval_criteria": criteria, "issues": role_issues})
    return roles


# 兼容别名
_load_role_criteria = _load_roles

if __name__ == "__main__":
    # 简易自检
    roles = _load_roles()
    print(f"加载角色数: {len(roles)}")
    for r in roles:
        if r["issues"]:
            print(f"  ❌ {r['name']}: {len(r['issues'])} 不兼容项")
        else:
            print(f"  ✅ {r['name']}: {len(r['eval_criteria'])} criteria")
