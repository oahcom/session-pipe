#!/usr/bin/env python3
"""eval_consistency — eval_criteria 可执行性预检。

对每种 persona 的每条 eval_criteria 走与运行时相同的白名单/沙箱校验，
输出会被 eval_checker 拒绝的条目（运行时记 skipped，角色永远拿不到该判据的反馈）。
用法: python3 src/eval_consistency.py [--json] [--personas DIR]
"""
import argparse
import json
import sys
from pathlib import Path

from eval_shared_loader import _load_roles, _check_cmd, _extract_cmd

_DEFAULT_PERSONAS = (Path.home() / "hermes-session-roles"
                     / "personas" / "session-roles")


def _load_role_criteria(personas_dir: Path) -> list[dict]:
    """加载所有 persona 的 eval_criteria，返回 [{name, criteria, issues}]。"""
    results = []
    roles = _load_roles(personas_dir)
    for r in roles:
        role_issues = []
        for c in r["eval_criteria"]:
            cmd = _extract_cmd(c)
            if cmd is None:
                continue
            issues = _check_cmd(cmd)
            if issues:
                role_issues.append({"criterion": c[:120], "issues": issues})
        results.append({"name": r["name"], "criteria_count": len(r["eval_criteria"]),
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
