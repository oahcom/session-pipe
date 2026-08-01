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
import sqlite3, json, sys, subprocess, time, os
from pathlib import Path

DB = Path.home() / ".hermes" / "state" / "workflows.db"
WORKSPACES = Path.home() / "ccs-workspaces"
BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"

# 系统文件：不是角色产出，不应计入证据
SYSTEM_FILES = {"TASKS.md", "CLAUDE.md", "test_output.md", "base.md"}

# 产出收益判定不依赖部署路径：文档类产出留在 workspace 即为交付


def _output_files(role, t_created=None, t_updated=None):
    """角色 workspace 下的真实产出文件（排除系统文件、目录、空文件）。

    t_created/t_updated: 任务时间窗口（epoch float），只统计窗口内新增/修改的文件，
    避免同一角色多个任务共用 workspace 导致文件被重复计数。
    """
    ws = WORKSPACES / role
    if not ws.exists():
        return []
    files = []
    for f in ws.rglob("*"):
        if not f.is_file() or f.name in SYSTEM_FILES:
            continue
        try:
            st = f.stat()
            # 排除空文件与 <1KB 的疑似模板残留
            if st.st_size < 1024:
                continue
            # 时间窗口过滤：文件 mtime 必须在任务创建与更新之间
            # 30min 缓冲覆盖角色创建文件的时差，同时避免跨任务文件重叠
            if t_created and t_updated:
                if not (t_created - 1800 <= st.st_mtime <= t_updated + 1800):
                    continue
        except OSError:
            continue
        files.append(f)
    return files


def _output_summary(files):
    """产出摘要：文件数 + 总大小 + 样例。"""
    if not files:
        return 0, 0, []
    total = sum(f.stat().st_size for f in files)
    names = sorted(f.name for f in files)[:5]
    return len(files), total, names


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
        "SELECT task_id, title, assignee, created_at, updated_at FROM tasks "
        "WHERE status='completed' AND updated_at > ? "
        "ORDER BY updated_at DESC LIMIT 10",
        (_cutoff,)
    ).fetchall()
    if not completed:
        return {"evaluated": 0, "assessments": []}

    # 每任务的独立 bus 证据：按 task_id 搜索（不再用全局 'task' 搜索）
    def _bus_evidence(tid):
        try:
            _r = subprocess.run(
                [str(BUS_CLIENT), "search", tid, "--limit", "20"],
                capture_output=True, text=True, timeout=15)
            if not _r.stdout.strip().startswith("{"):
                return False
            _data = json.loads(_r.stdout)
            return bool(_data.get("results") or _data.get("facts") or _data.get("items"))
        except Exception:
            return False

    # 预查: 每个任务关联的 workflow_instances 中 exit_messages 写入情况
    _wf_exit = {}
    _task_ids = [t["task_id"] for t in completed]
    if _task_ids:
        _placeholders = ",".join("?" for _ in _task_ids)
        for wf in db.execute(
            f"SELECT task_id, step_results FROM workflow_instances WHERE task_id IN ({_placeholders})",
            _task_ids
        ).fetchall():
            _sr = json.loads(wf["step_results"] or "{}")
            _has_exit = any(
                isinstance(v, dict) and v.get("exit_messages")
                for v in _sr.values()
            )
            if _has_exit:
                _wf_exit[wf["task_id"]] = True

    assessments = []
    for t in completed:
        _tid = t["task_id"]
        _role = t["assignee"] or "?"
        _title = t["title"] or "?"
        # 时间窗口：任务创建前后 1h ~ 更新后 1h，避免跨任务文件重复计数
        _files = _output_files(_role, t["created_at"], t["updated_at"])
        _n, _size, _names = _output_summary(_files)
        _bus = _bus_evidence(_tid)
        _exit = _wf_exit.get(_tid, False)

        # 收益判定规则（子 agent 独立评估的规则化实现）：
        #   1. 有 exit_messages 证据（角色实际产出被引擎记录）→ 有收益
        #   2. 有真实产出文件（≥1KB，≥3 个或 ≥2KB）→ 有实质产出
        #   3. 有产出文件但量小 → 疑似模板残留
        #   4. 无产出但有 bus 证据 → 进行中
        #   5. 无产出、无证据 → 纯模板任务（无收益）
        if _exit:
            _quality = "positive"
            _detail = f"task《{_title[:40]}》by {_role} — exit_messages 证据存在，有实际产出"
        elif _n >= 3 or (_n >= 1 and _size > 2048):
            _quality = "positive"
            _detail = f"task《{_title[:40]}》by {_role} — {_n} 个产出文件（{_size//1024}KB），有实质产出"
        elif _n >= 1:
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — {_n} 个产出文件（{_size//1024}KB），疑似模板残留"
        elif _bus:
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — 无产出文件但有 bus 证据，进行中"
        else:
            _quality = "negative"
            _detail = f"task《{_title[:40]}》by {_role} — 无产出、无证据，纯模板任务"
        assessments.append({
            "task_id": _tid, "role": _role,
            "title": _title, "quality": _quality,
            "detail": _detail,
            "output_files": _names,
            "output_count": _n,
            "output_size": _size,
            "has_bus_evidence": _bus,
            "has_exit_messages": _exit,
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
        files = [f.name for f in ws.rglob("*") if f.is_file() and f.name not in SYSTEM_FILES]
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


if __name__ == "__main__":
    import sys
    if "--feedback" in sys.argv:
        r = evaluate_and_feedback()
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        data = extract()
        print(json.dumps(data, ensure_ascii=False, indent=2))
