#!/usr/bin/env python3
"""eval_consistency — eval_criteria 与 eval_checker 白名单一致性校验。

对比 personas/*.json eval_criteria 中的验证命令与 eval_checker._ALLOWED_CMDS，
输出不兼容项（将导致 exit=-3 的命令）。
用法: python3 cli/eval_consistency.py [--json] [--personas DIR]
"""
import argparse
import json
import re
import sys
from pathlib import Path

# ── 从 eval_checker.py 硬编码同步（避免跨模块导入依赖）─
# ponytail: 与 eval_checker._ALLOWED_CMDS 保持一致；若白名单变更需同步更新
_ALLOWED_CMDS = frozenset({
    "systemctl", "journalctl", "ps", "df", "free", "grep",
    "curl", "git", "wc", "find",
    "cat", "head", "tail", "ls", "stat", "echo", "test",
    "sort", "uniq", "awk", "sed", "diff", "which", "command",
    "python3", "cut",
    "pytest",  # 只读测试运行（eval_criteria 验证用）
    "sqlite3",  # 只读数据库查询
    "bash",  # 复合命令外壳（内层已由白名单二次校验保护）
})

_DEFAULT_PERSONAS = (Path.home() / "hermes-session-roles"
                     / "personas" / "session-roles")


def _extract_cmd(criterion: str) -> str | None:
    """从 eval_criterion 提取验证命令（简化版，复刻 eval_checker._extract_cmd 核心逻辑）。"""
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
    """检查命令是否与白名单兼容。返回不兼容原因列表（空=兼容）。"""
    issues = []
    first_word = cmd.lstrip().split(maxsplit=1)[0] if cmd else ""
    # python3 -c '...' 与复合命令（for/while/if/bash -c）的分号/管道是语法，
    # 非 shell 拼接，豁免（与 eval_checker 一致）
    _is_python_c = first_word == "python3" and "-c" in cmd
    _is_composite = first_word in ("bash", "for", "while", "if") or cmd.startswith(("for ", "while ", "if "))
    if not _is_python_c and not _is_composite:
        if ";" in cmd or "`" in cmd or "$(" in cmd:
            issues.append(f"含禁止符号（分号/命令替换）: {cmd[:60]}")
    segments = [s.strip() for s in cmd.split("|") if s.strip()]
    if not _is_python_c and not _is_composite:
        for seg in segments:
            first = seg.split(maxsplit=1)[0] if seg else ""
            if first not in _ALLOWED_CMDS:
                issues.append(f"管道段命令不在白名单: {first!r} ← {seg[:50]}")
    if not segments:
        return issues
    first_word = segments[0].split(maxsplit=1)[0] if segments[0] else ""
    if first_word not in _ALLOWED_CMDS and not _is_composite:
        issues.append(f"首命令不在白名单: {first_word!r}")
    return issues


def _load_role_criteria(personas_dir: Path) -> list[dict]:
    """加载所有 persona 的 eval_criteria，返回 [{name, criteria, issues}]。"""
    results = []
    for f in sorted(personas_dir.glob("persona_*.json")):
        try:
            d = json.loads(f.read_text())
        except Exception:
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
        results.append({"name": name, "criteria_count": len(criteria),
                        "incompatible": role_issues})
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="eval_criteria 与白名单一致性校验")
    p.add_argument("--json", action="store_true")
    p.add_argument("--personas", default=str(_DEFAULT_PERSONAS))
    args = p.parse_args(argv)

    personas_dir = Path(args.personas)
    if not personas_dir.is_dir():
        sys.stderr.write(f"personas 目录不存在: {personas_dir}\n")
        return 1

    roles = _load_role_criteria(personas_dir)
    bad = [r for r in roles if r["incompatible"]]

    if args.json:
        print(json.dumps({"roles": len(roles), "incompatible_roles": len(bad),
                          "details": bad}, ensure_ascii=False, indent=2))
        return 1 if bad else 0

    total_incompatible = sum(len(r["incompatible"]) for r in bad)
    print(f"角色总数: {len(roles)} | 不兼容角色: {len(bad)} | 不兼容项: {total_incompatible}")
    for r in bad:
        print(f"\n  {r['name']} ({len(r['incompatible'])} 项):")
        for item in r["incompatible"]:
            print(f"    ❌ {item['criterion'][:80]}")
            for issue in item["issues"]:
                print(f"       → {issue}")
    if not bad:
        print("  ✅ 所有 eval_criteria 与白名单兼容")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
