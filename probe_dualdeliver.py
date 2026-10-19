#!/usr/bin/env python3
"""探针：子链接管后父步是否仍被投递（双投递）+ 回流完整性。只读。"""
import json
import sqlite3

DB = "/home/administrator/.hermes/state/workflows.db"
con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

rows = con.execute(
    "SELECT instance_id,template_id,status,current_step_id,step_results,parent_wf_id "
    "FROM workflow_instances WHERE step_results LIKE '%subflow_id%' ORDER BY created_at DESC"
).fetchall()
print(f"带 subflow_id 的实例数: {len(rows)}")

both, clean = [], []
for r in rows:
    try:
        sr = json.loads(r["step_results"] or "{}")
    except Exception:
        continue
    if not isinstance(sr, dict):
        continue
    for sid, d in sr.items():
        if not isinstance(d, dict) or "subflow_id" not in d:
            continue
        st = d.get("status")
        rec = (r["instance_id"], (r["template_id"] or "")[:22], sid, st,
               d.get("delivery_fail_count"), d.get("queued"))
        if st in ("notified", "running") or d.get("queued"):
            both.append(rec)
        else:
            clean.append(rec)

print(f"\n=== A. 子链已接管，但父步仍被投递/排队（双投递）: {len(both)} ===")
for x in both[:30]:
    print("  ", x)
print(f"\n=== B. 子链接管后父步无投递痕迹（正常）: {len(clean)} ===")
for x in clean[:10]:
    print("  ", x)

print("\n=== C. 子链回流完整性 ===")
subs = con.execute(
    "SELECT instance_id,parent_wf_id,subflow_source_step_id,status,completed_at,template_id "
    "FROM workflow_instances WHERE COALESCE(parent_wf_id,'')<>'' AND completed_at IS NOT NULL "
    "ORDER BY created_at DESC LIMIT 60"
).fetchall()
ok = miss = 0
for s in subs:
    pr = con.execute(
        "SELECT step_results,status FROM workflow_instances WHERE instance_id=?",
        (s["parent_wf_id"],),
    ).fetchone()
    if not pr:
        miss += 1
        continue
    try:
        psr = json.loads(pr["step_results"] or "{}")
    except Exception:
        miss += 1
        continue
    sd = psr.get(s["subflow_source_step_id"]) if isinstance(psr, dict) else None
    pc = sd.get("completed_at") if isinstance(sd, dict) else None
    if pc and s["completed_at"]:
        ok += 1
    else:
        miss += 1
        print(f"  未回流: 子链 {s['instance_id']}({s['status']}) -> 父 {s['parent_wf_id']}/{s['subflow_source_step_id']} 父状态={pr['status']}")
print(f"  回流成功 {ok} / 未回流 {miss}")

print("\n=== D. 当前 delivery_blocked 行级实例 ===")
n = con.execute("SELECT COUNT(*) FROM workflow_instances WHERE status='delivery_blocked'").fetchone()[0]
print("  count =", n)
for r in con.execute(
    "SELECT instance_id,template_id,assignee,parent_wf_id FROM workflow_instances "
    "WHERE status='delivery_blocked' LIMIT 10"
):
    print("  ", tuple(r))
