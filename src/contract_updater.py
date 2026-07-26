#!/usr/bin/env python3
"""contract_updater.py — 从 eval_checker 历史记录分析并建议改进。

Usage:
    python3 contract_updater.py --from-file <历史记录 JSON 文件>
    cat <history.json> | python3 contract_updater.py
"""

import json, logging, os, subprocess, sys
from pathlib import Path

BUS_SCRIPT = Path(os.environ.get(
    "BUS_CLIENT",
    os.path.expanduser("~/.hermes/scripts/bus_client.py"),
))
LOGGER = logging.getLogger("session-pipeline.contract_updater")
PERSONAS_DIR = Path(os.environ.get(
    "SESSION_ROLES_DIR",
    "/home/administrator/hermes-session-roles/personas/session-roles",
))


def _apply_suggestion(suggestion: dict) -> bool:
    """应用一条改进建议到角色 JSON。

    目前支持 type=add_verification:
      - 读取角色 JSON
      - 在 eval_criteria 中按文本匹配条目
      - 若条目是纯自然语言字符串且 suggestion 提供了 verification_command:
        替换为 {"text":, "verifiable":, "expected":}
      - 写回文件
    返回 True 表示成功应用。
    """
    role = suggestion.get("role")
    criterion_text = suggestion.get("criterion")
    stype = suggestion.get("type")
    if stype != "add_verification" or not role or not criterion_text:
        return False

    # 找到对应的角色 JSON 文件
    target: dict | None = None
    target_path: Path | None = None
    for f in PERSONAS_DIR.glob("persona_*.json"):
        try:
            d = json.loads(f.read_text())
            if d.get("name") == role:
                target, target_path = d, f
                break
        except Exception:
            continue
    if target is None:
        return False

    # 按文本匹配 eval_criteria 条目
    criteria = target.get("eval_criteria", [])
    idx = None
    for i, entry in enumerate(criteria):
        if isinstance(entry, str) and criterion_text in entry:
            idx = i
            break
        if isinstance(entry, dict) and criterion_text in entry.get("text", ""):
            idx = i
            break
    if idx is None:
        return False

    entry = criteria[idx]
    if not isinstance(entry, str):
        return False  # 已结构化，跳过

    verification_cmd = suggestion.get("verification_command")
    if not verification_cmd:
        return False

    criteria[idx] = {
        "text": entry,
        "verifiable": verification_cmd,
        "expected": suggestion.get("expected", ""),
    }

    try:
        target_path.write_text(json.dumps(target, ensure_ascii=False, indent=2) + "\n")
        return True
    except Exception:
        return False


def apply_all_suggestions(suggestions: list[dict]) -> dict:
    """批量应用建议。返回 {applied, failed, details}。"""
    applied = 0
    failed = 0
    details: list[dict] = []
    for s in suggestions:
        role = s.get("role", "unknown")
        try:
            ok = _apply_suggestion(s)
            if ok:
                applied += 1
            else:
                failed += 1
            details.append({"role": role, "success": ok, "error": "" if ok else "apply returned False"})
        except Exception as e:
            failed += 1
            details.append({"role": role, "success": False, "error": str(e)})
    return {"applied": applied, "failed": failed, "details": details}


def _has_output_schema(role_name: str) -> bool:
    """检查角色 JSON 是否定义了 output_schema。"""
    for f in PERSONAS_DIR.glob("persona_*.json"):
        try:
            d = json.loads(f.read_text())
            if d.get("name") == role_name:
                return bool(d.get("output_schema"))
        except Exception:
            continue
    return False


def suggest_improvements(check_history: list[dict]) -> list[dict]:
    """分析 eval_checker 历史记录，生成改进建议。

    规则:
    - 某角色连续 3 次以上被 skipped（无可执行 criterion）→ 建议加验证命令
    - 某角色连续 fail > 3 → 建议调整阈值或检查服务
    - 某角色 output_schema 不存在 → 建议加 output_schema
    """
    suggestions: list[dict] = []
    # 按角色分组
    by_role: dict[str, list[dict]] = {}
    for entry in check_history:
        role = entry.get("role", "unknown")
        by_role.setdefault(role, []).append(entry)

    for role, entries in by_role.items():
        # 规则 1: 连续 3+ 次 skipped
        if all(e.get("skipped", 0) > 0 and e.get("checked", 1) == 0 for e in entries[-3:]):
            suggestions.append({
                "role": role,
                "criterion": "eval_criteria",
                "suggestion": f"角色 {role} 连续 {len(entries[-3:])} 次被跳过（无可执行 criterion），建议添加含可执行验证命令的 eval_criterion",
                "type": "add_verification_cmd",
            })

        # 规则 2: 连续 fail > 3
        recent = [e for e in entries[-5:] if e.get("failed", 0) > 0]
        if len(recent) >= 3 and all(e.get("failed", 0) > (e.get("checked", 1) or 1) * 0.5 for e in recent):
            suggestions.append({
                "role": role,
                "criterion": "eval_criteria",
                "suggestion": f"角色 {role} 最近 {len(recent)} 次检查失败率 > 50%，建议调整验证阈值或检查对应服务状态",
                "type": "adjust_threshold_or_service",
            })

        # 规则 3: output_schema 不存在
        if not _has_output_schema(role):
            suggestions.append({
                "role": role,
                "criterion": "output_schema",
                "suggestion": f"角色 {role} 未定义 output_schema，建议添加以规范输出格式",
                "type": "add_output_schema",
            })

    return suggestions


def write_suggestions(suggestions: list[dict]) -> int:
    """将建议写入 bus cat=architecture，带 tag contract_improvement。

    返回写入数量。
    """
    count = 0
    for s in suggestions:
        text = f"[contract_updater] {s['type']}: {s['suggestion'][:80]}"
        evidence = json.dumps(s, ensure_ascii=False)
        try:
            subprocess.run(
                [sys.executable, str(BUS_SCRIPT), "write", "architecture", text,
                 "--evidence", evidence, "--src", "contract_updater",
                 "--tags", "contract_improvement"],
                capture_output=True, timeout=15,
            )
            count += 1
        except Exception as e:
            LOGGER.warning("bus write fail: %s", e)
    return count


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    import argparse
    parser = argparse.ArgumentParser(description="从 eval_checker 历史记录分析并建议改进")
    parser.add_argument("--from-file", type=str, default=None,
                        help="从 JSON 文件读取检查历史，默认从 stdin 读取")
    parser.add_argument("--no-write", action="store_true",
                        help="仅输出建议到 stdout，不写入 bus")
    parser.add_argument("--apply", action="store_true",
                        help="从 stdin 读取 JSON 格式的 suggestion 列表并执行应用")
    args = parser.parse_args()

    if args.apply:
        suggestions = json.load(sys.stdin)
        if not isinstance(suggestions, list):
            LOGGER.error("--apply 输入应为 suggestion 列表 (list[dict])")
            return 1
        result = apply_all_suggestions(suggestions)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.from_file:
        with open(args.from_file) as f:
            history = json.load(f)
    else:
        history = json.load(sys.stdin)

    if not isinstance(history, list):
        LOGGER.error("输入应为 check_history 列表 (list[dict])")
        return 1

    suggestions = suggest_improvements(history)
    print(json.dumps(suggestions, ensure_ascii=False, indent=2))

    if suggestions and not args.no_write:
        n = write_suggestions(suggestions)
        LOGGER.info("已写入 %d 条建议到 bus", n)
    elif not suggestions:
        LOGGER.info("无改进建议")

    return 0


if __name__ == "__main__":
    sys.exit(main())
