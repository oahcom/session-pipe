#!/usr/bin/env python3
"""wf_audit.py — 工作流产出审计查询 CLI。

查询"哪个工作流/步骤产出了什么、改了什么代码"：
  python3 wf_audit.py --latest          # 最近 10 条产出审计
  python3 wf_audit.py --wf <wf_id>      # 查指定工作流（含子工作流聚合）
  python3 wf_audit.py --role <role>     # 查某角色全部审计
  python3 wf_audit.py --since 2h        # 最近 2 小时

历史兼容：无 step_output 消息的历史工作流，从 step_results.exit_messages
+ workspace 现存文件实时算产出（无法恢复 git 改动——当时没记录）。
"""
import argparse
import json
import sqlite3
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from paths import CCS_WORKSPACES

BUS = Path.home() / ".hermes" / "sister_bus" / "blackboard.db"
WF_DB = Path.home() / ".hermes" / "state" / "workflows.db"

_SYSTEM_FILES = ("TASKS.md", "CLAUDE.md", "base.md", ".claude/", "settings.json", ".pytest_cache/", "__pycache__/", ".pyc")


def _bus():
    conn = sqlite3.connect(BUS)
    conn.row_factory = sqlite3.Row
    return conn


def _wf():
    conn = sqlite3.connect(WF_DB)
    conn.row_factory = sqlite3.Row
    return conn


def _is_system(name: str) -> bool:
    return any(s in name for s in _SYSTEM_FILES)


def preview_head(f: Path, n: int = 80) -> str:
    """只读前 n 字节，避免大文件读入内存。"""
    try:
        with f.open("r", encoding="utf-8", errors="ignore") as fh:
            return fh.read(n).replace("\n", " ")
    except Exception:
        return ""


def _scan_outputs(role: str, window_start: float = 0) -> list:
    """扫描角色 workspace 产出文件（可选时间窗过滤）。"""
    ws = CCS_WORKSPACES / role
    if not ws.exists():
        return []
    out = []
    try:
        for f in (f for f in ws.rglob("*") if ".git" not in f.parts):
            st = f.stat()
            if not f.is_file() or st.st_size <= 0:
                continue
            mtime = st.st_mtime
            if window_start and mtime < window_start:
                continue
            name = str(f.relative_to(CCS_WORKSPACES))
            if name.startswith("ccs-workspaces/"):
                name = name[len("ccs-workspaces/"):]
            if _is_system(name):
                continue
            out.append({
                "f": name, "size": st.st_size, "mtime": mtime,
                "preview": preview_head(f),
            })
    except Exception:
        pass
    out.sort(key=lambda x: x["mtime"], reverse=True)
    return out[:3]


def _now_step_outputs(wf_id: str) -> list:
    """查指定工作流的 step_output bus 消息（含子工作流 parent_wf 聚合）。"""
    conn = _bus()
    rows = conn.execute(
        "SELECT e FROM facts WHERE t LIKE '%step_output%' "
        "AND (t LIKE ? OR e LIKE ?) ORDER BY ts DESC",
        (f'%{wf_id[:12]}%', f'%"parent_wf": "{wf_id[:12]}%')).fetchall()
    conn.close()
    return [json.loads(r["e"]) for r in rows if r["e"]]


def _history_replay(wf_id: str) -> list:
    """历史兼容：对无 step_output 的工作流，从 step_results + workspace 现算。"""
    conn = _wf()
    row = conn.execute(
        "SELECT template_id, step_results, status FROM workflow_instances "
        "WHERE instance_id=?", (wf_id,)).fetchone()
    if not row:
        conn.close()
        return []
    results = json.loads(row["step_results"] or "{}")
    tpl = conn.execute("SELECT steps_json FROM workflow_templates WHERE template_id=?",
                       (row["template_id"],)).fetchone()
    conn.close()
    steps = json.loads(tpl["steps_json"]) if tpl and tpl["steps_json"] else []
    out = []
    for sdata in (steps or []):
        sid = sdata.get("step_id") or sdata.get("id", "")
        sr = results.get(sid, {})
        if not isinstance(sr, dict) or not sr.get("exit_messages"):
            continue
        role = sdata.get("target_role", "")
        ts = sr.get("ts", 0) or 0
        msgs = [m.get("text", "")[:100] for m in sr.get("exit_messages", [])
                if isinstance(m, dict)]
        out.append({
            "step": sid, "title": sdata.get("title", ""), "role": role,
            "outputs": _scan_outputs(role, ts - 2 * 3600) if role else [],
            "exit_messages": msgs[:3],
            "from_history": True,
        })
    return out


def _render(entries):
    if not entries:
        print("（无产出审计记录）")
        return
    for d in entries:
        ts_line = " [历史回放]" if d.get("from_history") else ""
        print(f"\n■ step={d.get('step','?')} role={d.get('role','?')}{ts_line} {d.get('title','')}")
        if d.get("parent_wf"):
            print(f"  主链: {d['parent_wf']}")
        for o in d.get("outputs", [])[:3]:
            _preview = o.get("preview", "")[:50]
            print(f"  📄 {o['f']} ({o.get('size','?')}B) | {_preview}")
        for g in d.get("git_changed", [])[:5]:
            print(f"  🔧 git: {g}")
        for gs in d.get("git_diffstat", [])[:5]:
            print(f"  📊 diff: {gs}")
        for m in d.get("exit_messages", [])[:2]:
            print(f"  📝 complete: {m[:70]}")


def _main():
    ap = argparse.ArgumentParser(description="工作流产出审计查询")
    ap.add_argument("--wf", help="工作流实例 ID（含子工作流聚合）")
    ap.add_argument("--role", help="角色名")
    ap.add_argument("--since", default="", help="时间窗（如 2h）")
    ap.add_argument("--latest", action="store_true", help="最近 10 条")
    args = ap.parse_args()

    if args.wf:
        entries = _now_step_outputs(args.wf)
        if not entries:
            entries = _history_replay(args.wf)
        _render(entries)
        return

    conn = _bus()
    try:
        sql = "SELECT e, ts FROM facts WHERE t LIKE '%step_output%'"
        params = []
        if args.role:
            sql += " AND e LIKE ?"
            params.append(f'%"role": "{args.role}"%')
        if args.since:
            h = int(args.since.rstrip("h"))
            sql += " AND ts > ?"
            params.append(time.time() - h * 3600)
        if not args.role and not args.since:
            args.latest = True
        sql += " ORDER BY ts DESC LIMIT 10" if args.latest else " ORDER BY ts DESC"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    entries = [json.loads(r["e"]) for r in rows if r["e"]]
    _render(entries)


if __name__ == "__main__":
    _main()