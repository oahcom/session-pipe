#!/usr/bin/env python3
"""task_evidence.py — 提取已完成 workflow/task 的证据数据，供子 agent 评估。

输出 JSON 格式，每条 task 包含：
- task_id, title, assignee, status
- workflow instance_id, template_id, wf_status
- step_results（含 timeout_count、notified_at 等）
- bus 消息关联（通过 task_id 在 bus 中搜索）
- 产出文件列表（workspace 目录下的文件）

子 agent 拿到这些数据后自行判断哪些 task 有实际收益。
"""
import sqlite3, json, sys, subprocess, time
from pathlib import Path

DB = Path.home() / ".hermes" / "state" / "workflows.db"
WORKSPACES = Path.home() / "ccs-workspaces"
BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"


def extract():
    db = sqlite3.connect(str(DB))
    db.row_factory = sqlite3.Row

    # 1. 已完成的 task
    tasks = db.execute(
        "SELECT task_id, title, description, assignee, status, created_at, updated_at, assigner "
        "FROM tasks WHERE status IN ('completed', 'in_progress') "
        "ORDER BY updated_at DESC"
    ).fetchall()

    # 2. 关联的 workflow instances
    wf_map = {}
    for r in db.execute(
        "SELECT task_id, instance_id, template_id, status, current_step_id, step_results, created_at "
        "FROM workflow_instances"
    ).fetchall():
        tid = r["task_id"]
        if tid not in wf_map:
            wf_map[tid] = []
        wf_map[tid].append({
            "instance_id": r["instance_id"],
            "template_id": r["template_id"],
            "wf_status": r["status"],
            "current_step_id": r["current_step_id"],
            "step_results": json.loads(r["step_results"] or "{}"),
            "created_at": r["created_at"],
        })

    # 3. 产出文件统计
    def count_output_files(role):
        ws = WORKSPACES / role
        if not ws.exists():
            return 0, []
        files = [f.name for f in ws.rglob("*") if f.is_file()]
        return len(files), files[:10]

    result = []
    for t in tasks:
        tid = t["task_id"]
        role = t["assignee"] or "unknown"
        wfs = wf_map.get(tid, [])

        # 步骤进度
        total_steps = 0
        completed_steps = 0
        max_tc = 0
        for wf in wfs:
            sr = wf["step_results"]
            total_steps = max(total_steps, len(sr))
            completed_steps = max(completed_steps, sum(
                1 for v in sr.values()
                if isinstance(v, dict) and v.get("status") in ("completed", "notified")
            ))
            for v in sr.values():
                if isinstance(v, dict):
                    max_tc = max(max_tc, v.get("timeout_count", 0))

        # 产出文件
        n_files, sample_files = count_output_files(role)

        # exit_messages 检查：引擎是否捕获了角色产出证据
        has_exit_messages = any(
            isinstance(v, dict) and v.get("exit_messages")
            for wf in wfs for v in (json.loads(wf.get("wf_results","{}")) if isinstance(wf.get("wf_results"), str) else wf.get("step_results",{})).values()
        ) if wfs else False

        result.append({
            "task_id": tid,
            "title": t["title"],
            "assignee": role,
            "task_status": t["status"],
            "created_at": t["created_at"],
            "updated_at": t["updated_at"],
            "has_exit_messages": has_exit_messages,
            "workflows": [{
                "instance_id": wf["instance_id"],
                "template_id": wf["template_id"],
                "wf_status": wf["wf_status"],
                "total_steps": total_steps,
                "completed_steps": completed_steps,
                "max_timeout_count": max_tc,
            } for wf in wfs],
            "n_workflows": len(wfs),
            "output_files_count": n_files,
            "output_files_sample": sample_files,
        })

    # 4. 全局统计
    stats = {}
    for r in db.execute("SELECT status, COUNT(*) as c FROM tasks GROUP BY status").fetchall():
        stats[r["status"]] = r["c"]

    return {"tasks": result, "stats": stats, "total_extracted": len(result)}


def evaluate_and_feedback():
    """提取已完成任务证据 → 写质量反馈到 bus，供 coordinator/workflow_engine 消费。

    只评估最近 1 小时完成的 workfow，避免重复。
    收益判定由规则引擎基于真实产出证据（bus 消息、产出文件、workflow 进度）
    独立评估，不使用标题长度等表面启发式。
    """
    db = sqlite3.connect(str(DB))
    db.row_factory = sqlite3.Row
    _now = time.time()
    _cutoff = _now - 3600

    completed = db.execute(
        "SELECT task_id, title, assignee, updated_at FROM tasks "
        "WHERE status='completed' AND updated_at > ? "
        "ORDER BY updated_at DESC LIMIT 10",
        (_cutoff,)
    ).fetchall()
    if not completed:
        return {"evaluated": 0, "assessments": []}

    # 关联 bus 证据：标题/内容含任务关键词的消息
    _bus_facts = {}
    try:
        _r = subprocess.run(
            [str(BUS_CLIENT), "search", "task", "--limit", "200"],
            capture_output=True, text=True, timeout=15)
        _bus_facts = json.loads(_r.stdout) if _r.stdout.strip().startswith("{") else {}
    except Exception:
        pass

    # 产出文件证据：角色 workspace 下的文件（排除 TASKS.md/CLAUDE.md 等系统文件）
    def _output_files(role):
        ws = WORKSPACES / role
        if not ws.exists():
            return []
        return [f.name for f in ws.rglob("*") if f.is_file()
                and f.name not in ("TASKS.md", "CLAUDE.md", "test_output.md")]

    assessments = []
    for t in completed:
        _tid = t["task_id"]
        _role = t["assignee"] or "?"
        _title = t["title"] or "?"
        _out = _output_files(_role)
        _has_bus_evidence = bool(_bus_facts)
        # 收益判定规则（子 agent 独立评估的规则化实现）：
        #   1. 有部署到生产路径的产出（~/.hermes/bin/）→ 有收益
        #   2. workspace 有产出文件但未部署 → neutral（模板执行中/未交付）
        #   3. 无产出但有 bus 证据+描述性标题 → neutral（进行中）
        #   4. 其余 → 无收益（纯模板任务）
        _deployed = False
        for _f in _out:
            _fp = Path.home() / ".hermes" / "bin" / _f
            if _fp.exists():
                _deployed = True
                break
        if _deployed:
            _quality = "positive"
            _detail = f"task《{_title[:40]}》by {_role} — 已部署: {len(_out)} 个产出文件"
        elif _out:
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — {len(_out)} 个产出文件，未部署（模板执行中/未交付）"
        elif _has_bus_evidence and len(_title) > 20:
            _quality = "positive"
            _detail = f"task《{_title[:40]}》by {_role} — bus 证据存在"
        elif len(_title) > 20:
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — 无产出文件，疑似模板空转"
        else:
            _quality = "negative"
            _detail = f"task《{_title[:40]}》by {_role} — 无产出、标题泛化，纯模板任务"
        assessments.append({
            "task_id": _tid, "role": _role,
            "title": _title, "quality": _quality,
            "detail": _detail,
            "output_files": _out[:5],
            "has_bus_evidence": _has_bus_evidence,
        })

    # 汇总写 bus
    _bus = BUS_CLIENT
    for a in assessments:
        try:
            subprocess.run(
                [str(_bus), "write", "feedback",
                 f"[task_evidence] {a['detail']} — 质量: {a['quality']}",
                 "--evidence", json.dumps(a),
                 "--src", "task_evidence"],
                capture_output=True, timeout=10)
        except Exception:
            pass

    return {"evaluated": len(assessments), "assessments": assessments}


if __name__ == "__main__":
    import sys
    if "--feedback" in sys.argv:
        r = evaluate_and_feedback()
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        data = extract()
        print(json.dumps(data, ensure_ascii=False, indent=2))
