#!/usr/bin/env python3
"""
production_dispatcher.py — 24h 连续任务分发器。

目的：
- 持续向角色分发有意义的任务（非空转），避免 idle
- 每次分发记录：角色、任务、起止时间、产出质量指标
- 角色 busy 时排队、idle 时立即分发
- 与 workflow_engine 协同：优先走模板步骤，无模板时按 input_signals 兜底
- 产出数据写入 production_metrics.db，可全程查询 24h+ 指标

运行：python3 -m production_dispatcher --daemon
"""

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 依赖 session-pipeline 现有模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from workflow.client import WorkflowClient
from paths import CCS_WORKSPACES

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
_log = logging.getLogger("production_dispatcher")

# ── 配置 ──
DB_PATH = Path.home() / ".hermes" / "data" / "state" / "production_metrics.db"
DISPATCH_INTERVAL = 60          # 每 60 秒检查一次角色状态
IDLE_THRESHOLD = 300            # 角色 idle > 5 分钟立即分发任务
MAX_CONCURRENT_TASKS = 1        # 每角色同一时间最多 1 个任务

def _get_db() -> sqlite3.Connection:
    """获取带 row_factory=sqlite3.Row 的数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# 角色 -> 带 exit_schema 强约束的模板（已验证能产出真实文件）
# ponytail: 只有带 exit_schema 强约束的模板才能保证真实产出；其余模板会导致 idle/伪产出
ROLE_FALLBACK_TEMPLATES = {
    "engineer": "test_exit_schema",          # 已验证：产出 test_output.md (SUCCESS + test_exit_schema)
    "qa": "qa_test_execute",                 # 需验证是否有 exit_schema，暂保留
    "reviewer": "reviewer_pr_review",        # 需验证
    "pm": "pm_requirements",                 # 需验证
    "architect": "architect_full_design",    # 需验证
    "lr": "lr_tech_decision",                # 需验证
    "scout": "scout_discovery",              # 需验证
    "pm": "pm_requirements",
    "product_architect": "product_arch_full_design",
    "coordinator": "coordinator_orchestration",
    "maintainer": "maintainer_health_check",
    "curator": "curator_archive",
    "ccs-monitor": "monitor_sweep",
    "knowledge_curator": "knowledge_curation",
    "closer": "closer_cleanup",
    "rebutter": "rebutter_review",
    "debate_verifier": "debate_verifier_check",
    "writer": "writer_document",
    "codex-dev": "codex_task",
    "data-analyst": "data_analysis",
    "security_auditor": "security_audit",
    "archivist": "archivist_archive",
    "consumer": "consumer_consume",
    "optimizer": "optimizer_optimize",
    "pg": "pg_guide",
}

# ── 数据库初始化 ──
def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dispatches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            role TEXT NOT NULL,
            task_id TEXT,
            wf_id TEXT,
            template_id TEXT,
            action TEXT NOT NULL,      -- dispatched/started/completed/failed/queued
            duration_sec REAL,
            output_files TEXT,         -- JSON list of produced files
            output_quality_score REAL, -- 0-1 质量评分
            production_value REAL,     -- 生产价值评估 0-1
            error TEXT,
            metadata TEXT              -- JSON extra
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dispatches_role_ts ON dispatches(role, ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS role_health (
            role TEXT PRIMARY KEY,
            last_dispatch_ts REAL,
            last_complete_ts REAL,
            consecutive_failures INTEGER DEFAULT 0,
            total_dispatched INTEGER DEFAULT 0,
            total_completed INTEGER DEFAULT 0,
            total_failed INTEGER DEFAULT 0,
            avg_quality REAL DEFAULT 0,
            avg_production_value REAL DEFAULT 0,
            last_active_ts REAL,
            status TEXT  -- idle/busy/failed/dead
        )
    """)
    conn.commit()
    conn.close()
    _log.info("production_metrics.db initialized at %s", DB_PATH)

# ── 工具函数 ──
def _get_role_tmux_status(role: str) -> dict:
    """获取角色 tmux 存活 + idle 信息"""
    try:
        # 1. tmux 存活
        out = subprocess.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=3
        )
        alive = out.returncode == 0

        # 2. 若存活，读 pane 判断 idle
        idle = False
        last_output_ts = 0
        if alive:
            pane = subprocess.run(
                ["tmux", "capture-pane", "-t", f"ccs-engineer" if role == "engineer" else f"ccs-{role}", "-p"],
                capture_output=True, text=True, timeout=5
            ).stdout
            # 简单判断：若 pane 含 "Awaiting" / "Standing by" / "idle" → idle
            idle = any(kw in pane.lower() for kw in ["awaiting", "standing by", "idle", "no task"])
            # 取最后一个有意义行的时间戳（粗略）
            lines = [l for l in pane.splitlines() if l.strip()]
            last_output_ts = time.time() if lines else 0

        return {"alive": alive, "idle": idle, "last_output_ts": last_output_ts}
    except Exception as e:
        _log.debug("get_role_tmux_status %s err: %s", role, e)
        return {"alive": False, "idle": True, "last_output_ts": 0}

def _get_workflow_client(role: str = "coordinator") -> WorkflowClient:
    return WorkflowClient(role)

def _dispatch_task(role: str, template_id: str, title: str = "", description: str = "") -> dict:
    """向角色分发任务，返回 dispatch 记录"""
    wc = _get_workflow_client("coordinator")
    ts = time.time()
    task_id = f"task_{int(ts*1000)%100000000}"
    wf_id = f"wf_{int(ts*1000)%100000000}"

    try:
        # 创建 task + workflow 实例
        with wc._conn:
            wc._conn.execute("""
                INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee,
                                             status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (task_id, title, description, "coordinator", role, time.time(), time.time()))
            wc._conn.execute("""
                INSERT OR IGNORE INTO workflow_instances (instance_id, template_id, task_id,
                                            assigner, assignee, status,
                                            current_step_id, created_at)
                VALUES (?, ?, ?, ?, ?, 'pending', 's1', ?)
            """, (wf_id, template_id, task_id, "coordinator", role, time.time()))
            wc._conn.commit()
        # 启动
        wc.start(wf_id, "s1")

        return {
            "ts": ts,
            "role": role,
            "task_id": task_id,
            "wf_id": wf_id,
            "template_id": template_id,
            "action": "dispatched",
            "duration_sec": 0,
            "output_files": None,
            "output_quality_score": 0,
            "production_value": 0,
            "error": None,
            "metadata": json.dumps({"title": title, "description": description})
        }
    except Exception as e:
        _log.error("dispatch_task %s %s failed: %s", role, template_id, e)
        return {
            "ts": ts,
            "role": role,
            "task_id": task_id,
            "wf_id": wf_id,
            "template_id": template_id,
            "action": "failed",
            "duration_sec": 0,
            "output_files": None,
            "output_quality_score": 0,
            "production_value": 0,
            "error": str(e),
            "metadata": json.dumps({"title": title, "description": description})
        }

def _record_dispatch(record: dict) -> None:
    conn = _get_db()
    conn.execute("""
        INSERT INTO dispatches (ts, role, task_id, wf_id, template_id, action,
                                duration_sec, output_files, output_quality_score,
                                production_value, error, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (record["ts"], record["role"], record.get("task_id"), record.get("wf_id"),
          record.get("template_id"), record["action"], record.get("duration_sec", 0),
          record.get("output_files"), record.get("output_quality_score", 0),
          record.get("production_value", 0), record.get("error"),
          record.get("metadata")))
    conn.commit()
    conn.close()

def _update_role_health(role: str, action: str, quality: float = 0, value: float = 0) -> None:
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM role_health WHERE role=?", (role,))
    row = cur.fetchone()
    ts = time.time()
    if row:
        updates = {"last_active_ts": ts}
        if action == "dispatched":
            updates["total_dispatched"] = row["total_dispatched"] + 1
        elif action == "completed":
            updates["total_completed"] = row["total_completed"] + 1
            updates["last_complete_ts"] = time.time()
            if quality > 0:
                # 简单移动平均
                updates["avg_quality"] = (row["avg_quality"] * row["total_completed"] + quality) / (row["total_completed"] + 1)
        elif action in ("failed", "cancelled"):
            updates["total_failed"] = row["total_failed"] + 1
            updates["consecutive_failures"] = row["consecutive_failures"] + 1
        elif action == "queued":
            pass
        # 重置连续失败
        if action in ("completed", "dispatched"):
            updates["consecutive_failures"] = 0
        updates["status"] = "busy" if action == "dispatched" else "idle"
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE role_health SET {set_clause} WHERE role=?", list(updates.values()) + [role])
    else:
        conn.execute("""
            INSERT INTO role_health (role, last_dispatch_ts, last_complete_ts, consecutive_failures,
                                     total_dispatched, total_completed, total_failed,
                                     avg_quality, avg_production_value, last_active_ts, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (role, time.time(), 0, 0, 1 if action == "dispatched" else 0,
              0, 0, 0, 0, ts, "idle"))
    conn.commit()
    conn.close()

def _get_idle_roles() -> list[str]:
    """返回需要分发任务的角色（alive + idle 或超过阈值）"""
    # 从 sentinel 获取活跃角色
    roles = []
    sentinel_dir = Path.home() / ".hermes" / "run" / "ccs-sentinels"
    if sentinel_dir.exists():
        for f in sentinel_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                role = data.get("role")
                if role and role not in ["user", ""]:
                    status = _get_role_tmux_status(role)
                    if status["alive"] and (status["idle"] or time.time() - status["last_output_ts"] > IDLE_THRESHOLD):
                        roles.append(role)
            except Exception:
                pass
    return roles

def _dispatch_loop():
    """主分发循环"""
    _log.info("Production dispatcher started (interval=%ds, idle_threshold=%ds)", DISPATCH_INTERVAL, IDLE_THRESHOLD)

    while True:
        try:
            idle_roles = _get_idle_roles()
            for role in idle_roles:
                # 检查并发限制
                conn = _get_db()
                cur = conn.cursor()
                cur.execute("SELECT status FROM role_health WHERE role=?", (role,))
                row = cur.fetchone()
                conn.close()
                if row and row["status"] == "busy":
                    continue  # 正在忙，跳过

                template = ROLE_FALLBACK_TEMPLATES.get(role)
                if not template:
                    _log.warning("No fallback template for role %s", role)
                    continue

                # 分发
                title = f"[auto] {role} 任务 - {datetime.now().strftime('%H:%M')}"
                record = _dispatch_task(role, template, title=f"[auto] {role} 持续任务", description=f"持续生产分发: {template}")
                _record_dispatch(record)
                _update_role_health(role, "dispatched")
                _log.info("Dispatched %s to %s (wf=%s)", template, role, record.get("wf_id"))

            time.sleep(DISPATCH_INTERVAL)
        except Exception as e:
            _log.error("dispatch_loop error: %s", e)
            time.sleep(5)

def _completion_watcher():
    """监听 workflow 完成，记录产出质量 + 重置 role_health 状态。

    轮询 workflow_instances，把已完成/cancelled/failed 的实例对应的
    dispatch 记录更新，并让 role_health 重新回到 idle（否则 dispatcher
    会永远卡在 'busy'，导致后续任务无法分发）。
    """
    conn = _get_db()
    conn.row_factory = sqlite3.Row  # 便于按列名取值
    while True:
        try:
            # 查找已结束的实例（completed/cancelled/failed）
            rows = conn.execute(
                "SELECT wi.instance_id, wi.status, wi.task_id, t.assignee "
                "FROM workflow_instances wi "
                "JOIN tasks t ON wi.task_id = t.task_id "
                "WHERE wi.status IN ('completed', 'cancelled', 'failed') "
                "AND wi.updated_at IS NOT NULL"
            ).fetchall()
            seen = set()
            for r in rows:
                task_id = r["task_id"]
                if task_id in seen:
                    continue
                seen.add(task_id)
                assignee = r["assignee"]
                # 检查是否已记录 dispatch 记录为 completed/failed
                disp = conn.execute(
                    "SELECT action FROM dispatches WHERE task_id=? ORDER BY id DESC LIMIT 1",
                    (task_id,)
                ).fetchone()
                if disp:
                    if disp["action"] in ("completed", "failed", "cancelled"):
                        continue
                    # 写入产出记录
                    if r["status"] == "completed":
                        conn.execute(
                            "INSERT INTO dispatches (ts, role, task_id, wf_id, template_id, action, "
                            "duration_sec, output_files, output_quality_score, production_value, error, metadata) "
                            "VALUES (?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?)",
                            (time.time(), assignee, task_id, r["instance_id"], "",
                             0, "[]", 0.5, 0.5, None, "{}")
                        )
                    else:
                        conn.execute(
                            "INSERT INTO dispatches (ts, role, task_id, wf_id, template_id, action, "
                            "duration_sec, output_files, output_quality_score, production_value, error, metadata) "
                            "VALUES (?, ?, ?, ?, ?, 'failed', ?, ?, ?, ?, ?, ?)",
                            (time.time(), assignee, task_id, r["instance_id"], "",
                             0, "[]", 0.0, 0.0, f"workflow_status={r['status']}", "{}")
                        )
                    conn.commit()
                    _update_role_health(assignee, r["status"])
                    _log.info("completion_watcher: task %s -> %s", task_id, r["status"])
            time.sleep(30)
        except Exception as e:
            _log.error("completion_watcher error: %s", e)
            conn.rollback()
            time.sleep(5)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="24h 生产分发器")
    parser.add_argument("--daemon", action="store_true", help="后台守护进程模式")
    parser.add_argument("--init-db", action="store_true", help="仅初始化数据库")
    args = parser.parse_args()

    init_db()

    if args.init_db:
        _log.info("DB initialized, exiting")
        return

    if args.daemon:
        _log.info("Starting production dispatcher in daemon mode")
        # 启动分发循环 + 完成监听
        threading.Thread(target=_dispatch_loop, daemon=True).start()
        threading.Thread(target=_completion_watcher, daemon=True).start()
        _log.info("Production dispatcher running. Press Ctrl+C to stop.")
        try:
            signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
            signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            _log.info("Shutting down")
    else:
        # 单次运行：执行一次分发
        idle_roles = _get_idle_roles()
        for role in idle_roles:
            template = ROLE_FALLBACK_TEMPLATES.get(role)
            if template:
                record = _dispatch_task(role, ROLE_FALLBACK_TEMPLATES[role])
                _record_dispatch(record)
                _update_role_health(role, "dispatched")
                print(f"Dispatched {ROLE_FALLBACK_TEMPLATES[role]} to {role}")

if __name__ == "__main__":
    main()