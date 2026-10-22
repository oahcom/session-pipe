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


def _window() -> int:
    """运行期窗口大小（每轮只跑前 N 条判据）。

    延迟从 eval_checker 取而不是模块级导入：daemon 热重载只刷新 eval_checker，
    共享模块缓存陈旧时模块级导入会 ImportError，预检与热重载一起静默退化
    （实测 2026-10-22 09:56-10:27 每 10 分钟一条 WARNING）。
    """
    from eval_checker import CRITERIA_WINDOW
    return CRITERIA_WINDOW

_DEFAULT_PERSONAS = (Path.home() / "hermes-session-roles"
                     / "personas" / "session-roles")


def _load_role_criteria(personas_dir: Path) -> list[dict]:
    """加载所有 persona 的 eval_criteria，返回 [{name, criteria, issues}]。

    只统计运行时真正会跑的前 _window() 条：窗口外的判据永远不执行，
    把它们算进 exec_count 就是假绿（knowledge_curator 5 条判据里唯一可执行的
    在第 4 条，旧口径判它非盲区、实际零判据执行）。
    """
    results = []
    roles = _load_roles(personas_dir)
    for r in roles:
        role_issues = []
        role_no_cmd = []
        window = r["eval_criteria"][:_window()]
        for c in window:
            cmd = _extract_cmd(c)
            if cmd is None:
                # 抽不出可执行命令 = 运行时恒 skipped（角色永远拿不到这条判据的
                # 反馈）。旧实现直接 continue 不计数，预检只报 1 条，而实测
                # 52 条判据里 37 条是这种形态（71%）→ 唯一的预检通道看不见主体。
                # 单独成列、不并入 incompatible：它恒为真，并进去等于 rc 恒 1，
                # daemon 每 600s 一条 WARNING（日志风暴）且真不兼容被淹没。
                role_no_cmd.append(c[:120])
                continue
            issues = _check_cmd(cmd)
            if issues:
                role_issues.append({"criterion": c[:120], "issues": issues})
        results.append({"name": r["name"], "criteria_count": len(r["eval_criteria"]),
                        "window": _window(),
                        "exec_count": len(window) - len(role_no_cmd) - len(role_issues),
                        "no_cmd": role_no_cmd, "incompatible": role_issues})
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

    _no_cmd = sum(len(r["no_cmd"]) for r in roles)
    # 零可执行判据的角色要按名字列出来：只给总数时，盲区角色与「N 条判据跳过」
    # 混在一个数字里，没人知道是哪些角色。
    blind = [r["name"] for r in roles if r["exec_count"] == 0]
    exec_total = sum(r["exec_count"] for r in roles)
    if args.json:
        # details 必须排在摘要字段前：daemon 的 WARNING 分支只打前 400 字节，
        # 摘要字段在前会把不兼容明细挤出截断点（codex 20 维 P2）。
        print(json.dumps({"roles": len(roles), "incompatible_roles": len(bad),
                          "details": bad,
                          "no_cmd_total": _no_cmd, "exec_total": exec_total,
                          "blind_roles": blind}, ensure_ascii=False, indent=2))
        return 1 if bad else 0

    total_incompatible = sum(len(r["incompatible"]) for r in bad)
    print(f"角色总数: {len(roles)} | 不兼容角色: {len(bad)} | 不兼容项: {total_incompatible}"
          f" | 无验证命令(运行时恒跳过): {_no_cmd} 条 | 可执行判据: {exec_total} 条")
    if blind:
        print(f"  ⚠️ 无可执行判据的角色 {len(blind)} 个（运行时只能靠通用检查）: "
              + ", ".join(blind))
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
