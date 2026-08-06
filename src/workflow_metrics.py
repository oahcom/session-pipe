#!/usr/bin/env python3
"""workflow_metrics.py — CCS 工作流效率指标分析。

用法:
  python3 workflow_metrics.py              # 当前快照
  python3 workflow_metrics.py --trend 10   # 最近 10 条历史记录
  python3 workflow_metrics.py --by-role    # 按角色统计
"""
import sqlite3, json, sys, time
from pathlib import Path

DB = Path.home() / ".hermes" / "state" / "workflows.db"
METRICS = Path.home() / ".hermes" / "state" / "workflow-metrics.jsonl"


def snapshot():
    db = sqlite3.connect(str(DB))
    db.row_factory = sqlite3.Row
    now = time.time()
    total = db.execute("SELECT COUNT(*) as c FROM workflow_instances").fetchone()["c"]
    by_status = {
        r["status"]: r["c"]
        for r in db.execute("SELECT status, COUNT(*) as c FROM workflow_instances GROUP BY status").fetchall()
    }
    completed = by_status.get("completed", 0)
    running = by_status.get("running", 0)
    cancelled = by_status.get("cancelled", 0)

    print("=== 当前快照 ===")
    print(f"总计: {total} | 完成: {completed} | 运行中: {running} | 取消: {cancelled}")
    print(f"完成率: {completed/max(total,1)*100:.1f}%")
    print()

    # 超时统计
    rows = db.execute(
        "SELECT step_results FROM workflow_instances WHERE status='running'"
    ).fetchall()
    tc_dist = {}
    for r in rows:
        try:
            sr = json.loads(r["step_results"] or "{}")
        except Exception: continue
        for sdata in sr.values():
            if isinstance(sdata, dict):
                tc = sdata.get("timeout_count", 0)
                tc_dist[tc] = tc_dist.get(tc, 0) + 1
    if tc_dist:
        print("=== 运行中步骤的 timeout_count 分布 ===")
        for tc in sorted(tc_dist):
            print(f"  timeout_count={tc}: {tc_dist[tc]} 步")
        print()


def trend(n=10):
    if not METRICS.exists():
        print("无历史指标，等待 daemon 首次运行")
        return
    lines = METRICS.read_text().strip().split("\n")[-n:]
    print(f"=== 最近 {len(lines)} 条指标 ===")
    print(f"{'时间':<20} {'总数':>5} {'完成':>5} {'运行':>5} {'完成率':>7}")
    print("-" * 48)
    for line in lines:
        try:
            m = json.loads(line)
            t = time.strftime("%m-%d %H:%M", time.localtime(m["ts"]))
            print(f"{t:<20} {m['total']:>5} {m['completed']:>5} {m.get('running','?'):>5} {m['rate']:>6.1f}%")
        except Exception: continue


def by_role():
    db = sqlite3.connect(str(DB))
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT assignee, status, COUNT(*) as c FROM workflow_instances "
        "GROUP BY assignee, status ORDER BY assignee"
    ).fetchall()
    print("=== 按角色统计 ===")
    by_role = {}
    for r in rows:
        role = r["assignee"] or "未分配"
        if role not in by_role:
            by_role[role] = {}
        by_role[role][r["status"]] = r["c"]
    for role in sorted(by_role):
        s = by_role[role]
        total = sum(s.values())
        done = s.get("completed", 0)
        print(f"  {role:<15} 总计={total:>3} 完成={done:>3} 取消={s.get('cancelled',0):>3} "
              f"运行={s.get('running',0):>3} 完成率={done/max(total,1)*100:.0f}%")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--trend" in args:
        n = 10
        idx = args.index("--trend")
        if idx + 1 < len(args):
            n = int(args[idx + 1])
        trend(n)
    elif "--by-role" in args:
        by_role()
    else:
        snapshot()
