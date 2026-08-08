#!/usr/bin/env python3
"""wf_audit.py — 工作流产出审计查询 CLI。

查询"哪个工作流/步骤产出了什么、改了什么代码"：
  python3 wf_audit.py --latest          # 最近 10 条产出审计
  python3 wf_audit.py --wf <wf_id>      # 查指定工作流全部审计
  python3 wf_audit.py --role <role>     # 查某角色全部审计
  python3 wf_audit.py --since 2h        # 最近 2 小时
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB = Path.home() / ".hermes" / "state" / "workflows.db"
BUS = Path.home() / ".hermes" / "sister_bus" / "blackboard.db"


def _main():
    ap = argparse.ArgumentParser(description="工作流产出审计查询")
    ap.add_argument("--wf", help="工作流实例 ID（前缀匹配）")
    ap.add_argument("--role", help="角色名")
    ap.add_argument("--since", default="", help="时间窗（如 2h）")
    args = ap.parse_args()

    sql, params = "", []
    if args.wf:
        sql += "AND e LIKE ?"
        params.append(f"%{args.wf}%")
    if args.role:
        sql += "AND e LIKE ?"
        params.append(f'%"role": "{args.role}"%')
    if args.since:
        h = int(args.since.rstrip("h"))
        sql += "AND ts > ?"
        params.append(__import__("time").time() - h * 3600)

    rows = _facts_where(sql, params)
    _render(rows)


def _facts_where(extra, params):
    conn = sqlite3.connect(BUS)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, t, e, ts FROM facts WHERE t LIKE '%step_output%' "
        f"{extra} ORDER BY ts DESC LIMIT 20", params).fetchall()
    conn.close()
    return rows


def _render(rows):
    if not rows:
        print("（无产出审计记录）")
        return
    for r in rows:
        try:
            d = json.loads(r["e"]) if r["e"] else {}
        except json.JSONDecodeError:
            d = {}
        ts = __import__("datetime").datetime.fromtimestamp(r["ts"]).strftime("%m-%d %H:%M")
        print(f"\n■ {ts} step={d.get('step','?')} role={d.get('role','?')} {d.get('title','')}")
        for o in d.get("outputs", [])[:3]:
            print(f"  📄 {o['f']} ({o['size']}B) {o.get('preview','')[:50]}")
        for g in d.get("git_changed", [])[:5]:
            print(f"  🔧 git: {g}")
        for m in d.get("exit_messages", [])[:2]:
            print(f"  📝 complete: {m[:70]}")


if __name__ == "__main__":
    _main()