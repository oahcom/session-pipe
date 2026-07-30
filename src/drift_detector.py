#!/usr/bin/env python3
"""drift_detector.py — 检测角色输出偏离其 output_targets 范围"""

import importlib.util
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

# 直接加载 pipeline 的 paths.py，不依赖 sys.path 顺序
_pipeline_paths = importlib.util.spec_from_file_location(
    "pipeline_paths", str(Path(__file__).resolve().parent / "paths.py"))
_pipeline_paths_mod = importlib.util.module_from_spec(_pipeline_paths)
_pipeline_paths.loader.exec_module(_pipeline_paths_mod)
CCS_SENTINEL_DIR = _pipeline_paths_mod.CCS_SENTINEL_DIR
SESSION_ROLES_PERSONAS = _pipeline_paths_mod.SESSION_ROLES_PERSONAS

from bus_protocol import Blackboard

log = logging.getLogger("drift-detector")

# 系统分类，所有角色始终允许
_ALWAYS_ALLOWED = {"notice", "architecture"}


def _load_role_config(role: str) -> Optional[dict]:
    """从 session-roles 目录加载角色配置（包含 output_targets）"""
    persona_dir = SESSION_ROLES_PERSONAS
    if not persona_dir.is_dir():
        log.warning("persona dir not found: %s", persona_dir)
        return None
    for f in sorted(persona_dir.glob("persona_*.json")):
        try:
            data = json.loads(f.read_bytes())
            if data.get("name") == role:
                return data
        except (json.JSONDecodeError, OSError):
            continue
    return None


def _parse_allowed_categories(output_targets: list) -> set:
    """从 output_targets 列表解析允许的 category 集合"""
    cats = set()
    for entry in output_targets:
        m = re.search(r"cat=(\S+)", entry)
        if m:
            cats.add(m.group(1))
    return cats | _ALWAYS_ALLOWED


def detect_drift(role: str, limit: int = 50) -> List[dict]:
    """检测指定角色的 output 漂移"""
    config = _load_role_config(role)
    if config is None:
        log.warning("role %r not found in persona dir", role)
        return []

    allowed = _parse_allowed_categories(config.get("output_targets", []))
    bb = Blackboard()
    facts = bb.read(src=role, limit=limit)
    drift = []
    for f in facts:
        if f.cat not in allowed:
            drift.append({
                "category": f.cat,
                "text_preview": f.t[:120],
                "timestamp": f.ts,
            })
    return drift


def detect_all_drift() -> Dict[str, list]:
    """对所有有活哨兵的角色执行漂移检测"""
    if not CCS_SENTINEL_DIR.is_dir():
        log.warning("sentinel dir not found: %s", CCS_SENTINEL_DIR)
        return {}
    results = {}
    for sentinel_file in sorted(CCS_SENTINEL_DIR.glob("*.json")):
        try:
            sentinel = json.loads(sentinel_file.read_bytes())
        except (json.JSONDecodeError, OSError):
            continue
        role = sentinel.get("role")
        if not role:
            continue
        drift = detect_drift(role)
        if drift:
            results[role] = drift
    return results


def report_drift(results: Dict[str, list]) -> int:
    """将漂移结果写入 bus，返回有漂移的角色数"""
    if not results:
        return 0
    bb = Blackboard()
    for role, entries in results.items():
        cats = ", ".join(e["category"] for e in entries)
        bb.write(
            "architecture",
            f"[role_drift] {role} has {len(entries)} drift entries: {cats}",
            evidence=json.dumps(entries, ensure_ascii=False, default=str),
            tags="role_drift",
            src="drift_detector",
        )
    return len(results)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="检测角色输出漂移")
    parser.add_argument("role", nargs="?", help="指定角色名，不指定则检测所有活跃角色")
    parser.add_argument("--limit", type=int, default=50, help="每个角色检查的消息数")
    parser.add_argument("--report", action="store_true", help="将漂移结果写入 bus")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.role:
        results = {args.role: detect_drift(args.role, args.limit)}
    else:
        results = detect_all_drift()

    if not results or all(len(v) == 0 for v in results.values()):
        print("no drift detected")
        return

    for role, entries in results.items():
        if not entries:
            continue
        print(f"[{role}] {len(entries)} drift entries:")
        for e in entries:
            print(f"  cat={e['category']} {e['text_preview'][:80]}")

    if args.report:
        n = report_drift(results)
        print(f"reported {n} roles with drift to bus")


if __name__ == "__main__":
    main()
