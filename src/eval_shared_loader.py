#!/usr/bin/env python3
"""eval 共享加载器 — 统一角色+eval_criteria 加载与解析，消除双 schema 漂移。

这是 eval_checker 与 eval_consistency 的单一真相源。
"""

import json
import re
import sys
from pathlib import Path

# personas 目录（与 eval_checker 一致）
PERSONAS_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"

# 白名单（源自 eval_checker._ALLOWED_CMDS，单一真相源）
# 注意：此处必须与 eval_checker._ALLOWED_CMDS 保持同步，建议直接从 eval_checker 导入
try:
    from eval_checker import _ALLOWED_CMDS as ALLOWED_CMDS
except ImportError:
    # 兜底：若 eval_checker 未安装（如单元测试），提供静态副本
    ALLOWED_CMDS = frozenset({
        "python3", "python", "bash", "sh", "ls", "cat", "grep", "find",
        "git", "pytest", "python3", "python", "make", "cargo", "go",
        "npm", "pip", "ruff", "black", "mypy", "pyright", "py_compile",
        "jq", "yq", "awk", "sed", "sort", "uniq", "wc", "head", "tail",
        "diff", "patch", "curl", "wget", "openssl", "ssh", "scp", "rsync",
        "systemctl", "journalctl", "tmux", "ps", "pkill", "kill", "pgrep",
        "stat", "md5sum", "sha256sum", "file", "which", "type", "command",
    })


def _extract_cmd(criterion: str) -> str | None:
    """从 eval_criterion 提取验证命令（单一真相源，供 eval_checker/eval_consistency 共用）。

    逻辑：去除 "验证:" 前缀，剥离 bash -c 外层，清理尾部噪音。
    """
    if "验证:" not in criterion:
        return None
    _, _, part = criterion.partition("验证:")
    part = part.strip()
    # 截断到输出/预期标记之前
    for marker in ("输出结果", "输出 ", "全部返回 "):
        idx = part.rfind(marker)
        if idx >= 0 and part[:idx].count("'") % 2 == 0:
            part = part[:idx].strip()
            break
    # 去掉中文混入
    part = re.sub(r'[一-鿿　-〿＀-￯]+[<\d]*', '', part).strip()
    # 剥离 bash -c 外层（支持单引号/双引号包裹）
    m = re.match(r"^(?:bash|sh)\s+-c\s+(['\"])(.+?)\1\s*$", part, re.DOTALL)
    if m:
        part = m.group(2).strip()
    # 清理尾部
    part = re.sub(r'\s+[<>!]=?\s*\d+(?:\.\d+)?\s*', ' ', part).strip()
    part = re.sub(r';$', '', part).strip()
    part = re.sub(r'^\s*\|\s*', '', part)
    return part or None


def _check_cmd(cmd: str) -> list[str]:
    """检查命令是否与白名单兼容。返回不兼容原因列表（空=兼容）。

    单一真相源，供 eval_checker/eval_consistency 共用。
    """
    issues = []
    first_word = cmd.lstrip().split(maxsplit=1)[0] if cmd else ""
    _is_python_c = first_word == "python3" and "-c" in cmd
    _is_composite = first_word in ("bash", "for", "while", "if") or cmd.startswith(("for ", "while ", "if "))
    if not _is_python_c and not _is_composite:
        if ";" in cmd or "`" in cmd or "$(" in cmd:
            issues.append(f"含禁止符号（分号/命令替换）: {cmd[:60]}")
    segments = [s.strip() for s in cmd.split("|") if s.strip()]
    if not _is_python_c and not _is_composite:
        for seg in segments:
            first = seg.split(maxsplit=1)[0] if seg else ""
            if first not in ALLOWED_CMDS:
                issues.append(f"管道段命令不在白名单: {first!r} ← {seg[:50]}")
    if not segments:
        return issues
    first_word = segments[0].split(maxsplit=1)[0] if segments[0] else ""
    if first_word not in ALLOWED_CMDS:
        issues.append(f"首命令不在白名单: {first_word!r}")
    return issues


def _load_roles(personas_dir: Path | None = None) -> list[dict]:
    """加载所有角色 JSON，返回统一格式：[{name, eval_criteria: [criterion...], issues: [...]}]。

    单一真相源：eval_checker 和 eval_consistency 均从此导入。
    """
    if personas_dir is None:
        personas_dir = Path.home() / "hermes-session-roles" / "personas" / "session-roles"

    if not personas_dir.is_dir():
        return []

    roles = []
    for f in sorted(personas_dir.glob("persona_*.json")):
        try:
            d = json.loads(f.read_text())
        except Exception as e:
            continue
        name = d.get("name", f.stem)
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