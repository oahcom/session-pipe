#!/usr/bin/env python3
"""drift_detector.py — 检测角色输出偏离其 output_targets 范围"""

import importlib.util
import json
import logging
import re
import time
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
# R4 drift P0 修复: monitor_audit 是 tracker/watchdog 审计产物，不应被漂移检测误报
# （漂移分析发现 maintainer 36 条 + archivist 20 条误报根因）
# workflow 是引擎内部信道（审批/步骤推送经 ccs.py send 回写 src 归因于角色，cat=workflow），
# 属角色消费而非产出，所有角色都收，故放行（bus[301357] engineer 误报根因）。
# 引擎回写分类：routing_daemon/task_generator 经 ccs.py send 把任务/步骤推送
# 以 src=角色 归因写回（cat=task_spec/scheduler 等），属输入信道而非角色产出。
# L82 注释已声明 task_spec 为输入信道，但原 _ALWAYS_ALLOWED 只放了 workflow，
# 导致 pm/knowledge_curator 等收到任务分配时被误报"产出越界"（PM 确认误报）。
# 这里补全引擎回写分类，与 workflow 同列。
_ENGINE_WRITEBACK_CATS = {"workflow", "task_spec", "scheduler", "monitor_wf_health"}
# reflexion_lesson: prompts/base.md 的「违规后果」条款要求**所有角色**
# 「首次违规 → 退回原角色 + 写 reflexion_lesson」，README 亦将其列为
# consumer/archivist 的 output_targets。属被系统性要求的合法产出，但不在
# persona 的 output_targets 里 → 4 角色 + watchdog 误报「产出越界」，
# 且形成自指循环：写教训 → 被判漂移 → 再写教训。engineer #370527 取证确认。
_ALWAYS_ALLOWED = ({"notice", "architecture", "monitor_audit", "reflexion_lesson"}
                   | _ENGINE_WRITEBACK_CATS)


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


def _parse_allowed_categories(targets: list) -> set:
    """从 output_targets/input_signals 列表解析允许的 category 集合。

    兼容三种条目格式：
      - 旧式字符串: "bus cat=architecture 新发现"
      - 新式 dict:   {"bus_cat": "architecture", "label": ...}
      - input_signals dict: {"spec": {"category": "task_spec"}, ...}  # 角色消费的分类也算合法
    """
    cats = set()
    for entry in targets:
        if isinstance(entry, dict):
            bc = entry.get("bus_cat")
            if isinstance(bc, str):
                cats.add(bc)
            else:
                # input_signals 用 spec.category 声明角色消费的分类
                spec = entry.get("spec")
                if isinstance(spec, dict) and isinstance(spec.get("category"), str):
                    cats.add(spec["category"])
            continue
        m = re.search(r"cat=(\S+)", entry)
        if m:
            cats.add(m.group(1))
    return cats | _ALWAYS_ALLOWED


# 漂移判定的时间窗：只看最近 N 小时的角色产出。
# 原实现只看「最近 50 条」无时间窗 —— 引擎停摆 35 天期间积压的历史消息占满窗口
# （实测 scout 最近 50 条里 36 条属 08-23~08-25），一条 53 天前的 design_issue
# 持续每小时被判为漂移，直到被新消息挤出窗口。漂移是"当前产出是否越界"，
# 历史越界早已处理完毕，不应长期挂账。
_DRIFT_WINDOW_HOURS = 24


def detect_drift(role: str, limit: int = 50) -> List[dict]:
    """检测指定角色的 output 漂移（仅最近 _DRIFT_WINDOW_HOURS 小时的产出）"""
    config = _load_role_config(role)
    if config is None:
        log.warning("role %r not found in persona dir", role)
        return []

    allowed = _parse_allowed_categories(config.get("output_targets", []))
    # 角色消费的分类（input_signals.spec.category）也放行：审批/步骤推送经 ccs.py send
    # 回写时 src 归因于角色，category 为 workflow/task_spec 等——那是输入信道，非产出漂移。
    allowed |= _parse_allowed_categories(config.get("input_signals", []))
    bb = Blackboard()
    facts = bb.read(src=role, limit=limit)
    _cutoff = time.time() - _DRIFT_WINDOW_HOURS * 3600
    drift = []
    for f in facts:
        # 排除测试场景消息（test_role_interact_* 是测试工作流产物，合法但非生产）
        if f.t.startswith("[test_role_interact_"):
            continue
        try:
            if float(f.ts) < _cutoff:
                continue
        except (TypeError, ValueError):
            # ts 非数值（测试 mock / 脏数据）→ 不据其判窗口，保留原判定行为
            pass
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


# 每轮检测写一条全量快照（固定标题 → bus 的 ON CONFLICT(t) 保证全库仅一行，
# 恒被最新一轮覆写）。消费方据此判断「该角色本轮是否仍在漂移」。
#
# 为何需要：原实现只写「有漂移」的角色，不写「已恢复」的角色 —— 角色一旦停止
# 漂移，其旧告警行无人清理，长期留在 bus 最近 N 条内被反复注入 pane
# （实测 scout/pm/engineer 于 17:26 修复后，其 17:15 的告警仍持续注入）。
_DRIFT_SUMMARY_TITLE = "[role_drift_summary]"


def report_drift(results: Dict[str, list]) -> int:
    """将漂移结果写入 bus，返回有漂移的角色数"""
    bb = Blackboard()
    drifted = [r for r, e in (results or {}).items() if e]
    for role in drifted:
        cats = ", ".join(e["category"] for e in results[role])
        bb.write(
            "architecture",
            f"[role_drift] {role} has {len(results[role])} drift entries: {cats}",
            evidence=json.dumps(results[role], ensure_ascii=False, default=str),
            tags="role_drift",
            src="drift_detector",
        )
    # 全量快照：标题固定 → 恒覆写同一行，无累积噪音
    bb.write(
        "architecture",
        _DRIFT_SUMMARY_TITLE,
        evidence=json.dumps({"drifted": drifted}, ensure_ascii=False),
        tags="role_drift",
        src="drift_detector",
    )
    return len(drifted)


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
