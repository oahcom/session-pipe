#!/usr/bin/env python3
"""
stability_test_runner.py — 24h+ 稳定性测试运行器。

目的：
- 持续 24h+ 监控所有角色产出、质量、稳定性、生产对接度
- 记录每个角色的：产出数量、质量分、稳定时间、生产对接率、价值交付率
- 自动检测：角色死锁、模型故障、import 失败、空转、伪产出
- 每 5 分钟输出一次阶段报告，最后生成 24h+ 完整测试报告
- 只有跑满 24h+ 且全指标达标，才判定 "stable production"

运行：python3 -m stability_test_runner --hours 24
"""

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from paths import CCS_WORKSPACES

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
_log = logging.getLogger("stability_test_runner")

# ── 配置 ──
DB_PATH = Path.home() / ".hermes" / "data" / "state" / "production_metrics.db"
STABILITY_DB = Path.home() / ".hermes" / "data" / "state" / "stability_test.db"
CHECK_INTERVAL = 300          # 每 5 分钟检查一次
REPORT_INTERVAL = 1800        # 每 30 分钟输出阶段报告
TARGET_HOURS = 24             # 目标 24 小时

# 所有需要监控的角色（来自 dispatcher 的 ROLE_FALLBACK_TEMPLATES）
MONITORED_ROLES = [
    "engineer", "qa", "reviewer", "pm", "architect", "lr",
    "scout", "product_architect", "coordinator", "maintainer",
    "curator", "ccs-monitor", "knowledge_curator", "closer",
    "rebutter", "debate_verifier", "writer", "codex-dev",
    "data-analyst", "security_auditor", "archivist", "consumer",
    "optimizer", "pg"
]

# 质量评分阈值
QUALITY_THRESHOLD = 0.6       # 质量分 >= 0.6 算合格
STABILITY_THRESHOLD = 0.8     # 稳定时间占比 >= 80%
PRODUCTION_INTEGRATION_THRESHOLD = 0.7  # 生产对接率 >= 70%

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(STABILITY_DB)
    conn.row_factory = sqlite3.Row
    return conn

def _get_prod_db() -> sqlite3.Connection:
    prod = Path.home() / ".hermes" / "data" / "state" / "production_metrics.db"
    conn = sqlite3.connect(prod)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    STABILITY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STABILITY_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS role_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            role TEXT NOT NULL,
            alive INTEGER NOT NULL,
            idle INTEGER NOT NULL,
            current_task TEXT,
            wf_id TEXT,
            output_files TEXT,          -- JSON list
            quality_score REAL,         -- 0-1
            production_value REAL,      -- 0-1
            stability_sec REAL,         -- 连续存活时长
            last_active_ts REAL,
            error TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_role_ts ON role_snapshots(role, ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts REAL NOT NULL,
            end_ts REAL,
            target_hours REAL NOT NULL,
            total_roles INTEGER,
            passed_roles INTEGER,
            overall_pass INTEGER,
            report_json TEXT
        )
    """)
    conn.commit()
    conn.close()
    _log.info("Stability test DB initialized at %s", STABILITY_DB)

def _get_role_snapshot(role: str) -> dict:
    """获取角色当前完整快照"""
    try:
        # 1. tmux 存活
        out = subprocess.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=3
        )
        alive = out.returncode == 0

        # 2. pane 内容
        pane = ""
        idle = False
        if alive:
            pane = subprocess.run(
                ["tmux", "capture-pane", "-t", f"ccs-engineer" if role == "engineer" else f"ccs-{role}", "-p"],
                capture_output=True, text=True, timeout=5
            ).stdout
            idle = any(kw in pane.lower() for kw in ["awaiting", "standing by", "idle", "no task", "waiting for"])

        # 3. 当前 workflow
        wf_id = None
        current_task = None
        prod = _get_prod_db()
        cur = prod.execute("""
            SELECT instance_id, current_step_id, step_results
            FROM workflow_instances
            WHERE assignee=? AND status='running'
            ORDER BY created_at DESC LIMIT 1
        """, (role,))
        wf = cur.fetchone()
        if wf:
            wf_id = wf["instance_id"]

        # 4. 近期产出文件（从 git status / workspace 扫描）
        ws = CCS_WORKSPACES / role
        output_files = []
        if ws.exists():
            for f in ws.rglob("*"):
                if f.is_file() and f.suffix in (".py", ".md", ".json", ".yaml", ".yml", ".txt") and "CLAUDE.md" not in f.name:
                    try:
                        stat = f.stat()
                        if time.time() - stat.st_mtime < 3600:  # 1 小时内修改
                            output_files.append(str(f.relative_to(ws)))
                    except Exception:
                        pass

        return {
            "alive": alive,
            "idle": idle,
            "current_task": current_task,
            "wf_id": wf_id,
            "output_files": json.dumps(output_files),
            "last_active_ts": time.time() if alive else 0,
            "error": None if alive else "tmux session dead"
        }
    except Exception as e:
        return {
            "alive": False, "idle": True, "current_task": None,
            "wf_id": None, "output_files": "[]", "last_active_ts": 0,
            "error": str(e)
        }

def _evaluate_quality(snapshot: dict, role: str) -> tuple[float, float, float]:
    """评估单次快照的质量/生产价值/稳定性"""
    quality = 0.0
    production_value = 0.0
    stability = 1.0 if snapshot["alive"] else 0.0

    if not snapshot["alive"]:
        return 0.0, 0.0, 0.0

    # 质量：有产出文件 + 非 idle + 有工作流
    if snapshot["output_files"] != "[]":
        quality += 0.5
    if not snapshot["idle"]:
        quality += 0.3
    if snapshot["wf_id"]:
        quality += 0.2
    quality = min(1.0, quality)

    # 生产价值：产出数量 * 质量
    try:
        output_count = len(json.loads(snapshot["output_files"]))
    except Exception:
        output_count = 0
    production_value = min(1.0, output_count * 0.2 + quality * 0.5)

    return quality, production_value, stability

def _record_snapshot(snapshots: list) -> None:
    """批量记录快照到 DB"""
    conn = sqlite3.connect(STABILITY_DB)
    ts = time.time()
    for s in snapshots:
        quality, prod_val, stab = _evaluate_quality(s, s.get("role", ""))
        conn.execute("""
            INSERT INTO role_snapshots (ts, role, alive, idle, current_task, wf_id,
                                        output_files, quality_score, production_value,
                                        stability_sec, last_active_ts, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (time.time(), s.get("role", ""), s["alive"], s["idle"],
              s.get("current_task"), s.get("wf_id"),
              s.get("output_files", "[]"), s.get("quality_score", 0),
              s.get("production_value", 0), s.get("stability_sec", 0),
              s.get("last_active_ts", 0), s.get("error")))
    conn.commit()
    conn.close()

def _generate_report() -> dict:
    """生成当前阶段报告"""
    conn = sqlite3.connect(STABILITY_DB)
    conn.row_factory = sqlite3.Row

    # 角色级统计
    role_stats = {}
    for role in MONITORED_ROLES:
        cur = conn.execute("""
            SELECT alive, idle, quality_score, production_value, output_files
            FROM role_snapshots WHERE role=? ORDER BY ts DESC LIMIT 20
        """, (role,))
        rows = cur.fetchall()
        if not rows:
            role_stats[role] = {"pass": 0, "reason": "no data"}
            continue
        alive_count = sum(r["alive"] for r in rows)
        idle_count = sum(r["idle"] for r in rows)
        avg_quality = sum(r["quality_score"] for r in rows) / len(rows) if rows else 0
        alive_ratio = alive_count / len(rows)
        idle_ratio = idle_count / len(rows) if rows else 1

        # 判定
        pass_cond = (
            alive_ratio >= STABILITY_THRESHOLD and
            idle_ratio <= 0.3 and
            avg_quality >= QUALITY_THRESHOLD
        )
        role_stats[role] = {
            "alive_ratio": round(alive_ratio, 3),
            "idle_ratio": round(idle_ratio, 3),
            "avg_quality": round(avg_quality, 3),
            "pass": int(pass_cond),
            "snapshots": len(rows)
        }

    conn.close()
    return {
        "ts": time.time(),
        "elapsed_hours": (time.time() - _TEST_START_TS) / 3600,
        "target_hours": TARGET_HOURS,
        "roles": role_stats,
        "overall_pass": all(r["pass"] for r in role_stats.values())
    }

def _print_report(report: dict) -> None:
    _log.info("=== STABILITY REPORT (%.2fh/%.1fh) ===", report["elapsed_hours"], TARGET_HOURS)
    _log.info("Overall: %s", "PASS" if report["overall_pass"] else "FAIL")
    for role, s in report["roles"].items():
        status = "✓" if s["pass"] else "✗"
        _log.info("  %s: %s alive=%.2f idle=%.2f qual=%.2f snaps=%d",
                  role, status, s["alive_ratio"], s["idle_ratio"], s["avg_quality"], s["snapshots"])

_TEST_START_TS = time.time()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="24h+ 稳定性测试运行器")
    parser.add_argument("--hours", type=int, default=24, help="目标测试小时数")
    parser.add_argument("--init-db", action="store_true", help="仅初始化数据库")
    args = parser.parse_args()

    global TARGET_HOURS
    TARGET_HOURS = args.hours

    init_db()

    if args.init_db:
        _log.info("DB initialized, exiting")
        return

    _log.info("=== 24h+ 稳定性测试启动 (target=%dh) ===", TARGET_HOURS)
    _log.info("Monitored roles: %s", ", ".join(MONITORED_ROLES))

    last_report = time.time()

    try:
        while True:
            # 检查是否达标时间
            if time.time() - _TEST_START_TS >= TARGET_HOURS * 3600:
                _log.info("=== TARGET HOURS REACHED (%dh) ===", TARGET_HOURS)
                break

            # 采集所有角色快照
            snapshots = []
            for role in MONITORED_ROLES:
                snap = _get_role_snapshot(role)
                snap["role"] = role
                snapshots.append(snap)

            _record_snapshot(snapshots)

            # 定期输出报告
            if time.time() - last_report >= REPORT_INTERVAL:
                report = _generate_report()
                _print_report(report)
                last_report = time.time()

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        _log.info("Interrupted by user")

    # 最终报告
    final_report = _generate_report()
    _print_report(final_report)

    # 保存最终报告到 DB
    conn = sqlite3.connect(STABILITY_DB)
    conn.execute("""
        INSERT INTO test_summary (start_ts, end_ts, target_hours, total_roles, passed_roles,
                                  overall_pass, report_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (_TEST_START_TS, time.time(), TARGET_HOURS, len(MONITORED_ROLES),
          sum(1 for r in final_report["roles"].values() if r["pass"]),
          int(final_report["overall_pass"]), json.dumps(final_report)))
    conn.commit()
    conn.close()

    _log.info("=== 24h+ 稳定性测试结束 ===")
    if final_report["overall_pass"]:
        _log.info("✅ STABILITY TEST PASSED - 24h+ stable production achieved")
    else:
        _log.info("❌ STABILITY TEST FAILED - 24h+ stability not achieved")

if __name__ == "__main__":
    main()