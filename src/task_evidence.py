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
import sqlite3, json, subprocess, time
from pathlib import Path

DB = Path.home() / ".hermes" / "state" / "workflows.db"
WORKSPACES = Path.home() / "ccs-workspaces"
BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"

# 系统文件：不是角色产出，不应计入证据
# 前缀匹配覆盖 CLAUDE.md.1/.2 等轮转备份副本（子 agent 审计发现 98% 指标虚增来自此）
SYSTEM_FILES = ("TASKS.md", "CLAUDE.md", "test_output.md", "base.md")
SYSTEM_PREFIXES = ("CLAUDE.md.", "TASKS.md.")


def _is_system_file(name: str) -> bool:
    return name in SYSTEM_FILES or any(name.startswith(p) for p in SYSTEM_PREFIXES)

# 评估结果幂等守卫：task_id → 上轮评估签名。
# WHY: 本函数由 daemon 每 10s 调用，而 bus 写入是 ON CONFLICT(t) DO UPDATE（刷新 ts），
# 重复写会把已沉底的旧 fact 顶回 unread 列表，实测单条 fact 被写 1091 次。
# 已知边界：签名存进程内存，daemon 重启后首轮会对窗口内任务重写一次
# （实测 24h 内非计划重启 0 次，代价是可接受的几次重复写）。
# 若将来重启频率上升，改用 workflow_logs 表存签名（action='evidence_assessed'）。
_LAST_ASSESSMENT: dict[str, str] = {}

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
        if not f.is_file() or _is_system_file(f.name):
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


def _append_role_feedback(role: str, title: str, task_id: str) -> bool:
    """把 negative 评分回写到 role TASKS.md「历史教训」节。

    WHY: 评分信号收集在 bus 却不回写 role prompt —— 角色无法从模板任务教训学习
   （GEPA rounds memory 要求评分回写 persona/prompt）。补全闭环。
    仅记录 negative，positive 不回写（防噪）。幂等：同 task_id 不重复。
    CLAUDE.md 红线：绝不回写 CLAUDE.md（inject 管理），仅 TASKS.md。
    """
    try:
        p = WORKSPACES / role / "TASKS.md"
        if not p.exists():
            return False
        txt = p.read_text(encoding="utf-8", errors="ignore")
        # 幂等：匹配 `({task_id})` 边界，避免 task_123 被 task_1234 子串误杀
        if f"({task_id})" in txt:
            return False
        line = f"\n- [negative] 任务《{title}》({task_id}) → 评分回写：无产出/无证据/模板残留。参考 bus feedback + code_fix 自省。"
        with open(p, "a", encoding="utf-8") as f:
            f.write(line)
        return True
    except Exception:
        return False  # 回写失败不影响主流程


def evaluate_and_feedback():
    """提取已完成任务证据 → 写质量反馈到 bus，供 coordinator/workflow_engine 消费。

    只评估最近 1 小时完成的 workfow，避免重复。
    收益判定由规则引擎基于真实产出证据（bus 消息、产出文件、workflow 进度）
    独立评估，不使用标题长度等表面启发式。

    连接必须关闭：本函数由引擎 daemon 每 tick 调用（task_generator.py:156），
    原实现裸 connect 从不 close —— 实测每轮泄漏 1 个 fd，与 Blackboard 同类，
    9 分钟累积后随引擎进程一并崩溃（2026-10-17）。
    """
    db = sqlite3.connect(str(DB))
    try:
        return _evaluate_with(db)
    finally:
        db.close()


def _evaluate_with(db):
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

    # 预查: 每个任务关联的 workflow_instances 中 exit_messages 或 summary 写入情况
    _wf_evidence = {}
    _task_ids = [t["task_id"] for t in completed]
    if _task_ids:
        _placeholders = ",".join("?" for _ in _task_ids)
        for wf in db.execute(
            f"SELECT task_id, step_results FROM workflow_instances WHERE task_id IN ({_placeholders})",
            _task_ids
        ).fetchall():
            _sr = json.loads(wf["step_results"] or "{}")
            # exit_messages 是修复后新增的字段（角色实际 bus 产出）
            _has_exit = any(
                isinstance(v, dict) and v.get("exit_messages")
                for v in _sr.values()
            )
            # summary 是引擎生成的角色完成摘要（历史实例的主要证据源）
            _has_summary = bool(_sr.get("summary"))
            if _has_exit:
                _wf_evidence[wf["task_id"]] = "exit_messages"
            elif _has_summary:
                _wf_evidence[wf["task_id"]] = "summary"

    assessments = []
    for t in completed:
        _tid = t["task_id"]
        _role = t["assignee"] or "?"
        _title = t["title"] or "?"
        # 时间窗口：任务创建前后 1h ~ 更新后 1h，避免跨任务文件重复计数
        _files = _output_files(_role, t["created_at"], t["updated_at"])
        _n, _size, _names = _output_summary(_files)
        _bus = _bus_evidence(_tid)
        _evidence = _wf_evidence.get(_tid, "")

        # 收益判定规则（子 agent 独立评估的规则化实现）：
        #   1. 有 exit_messages 证据（角色实际产出被引擎记录）→ 有收益
        #   2. 有 summary 证据（引擎生成的角色完成摘要）→ 有收益
        #   3. 有真实产出文件（≥1KB，≥3 个或 ≥2KB）→ 有实质产出
        #   4. 有产出文件但量小 → 疑似模板残留
        #   5. 无产出但有 bus 证据 → 进行中
        #   6. 无产出、无证据 → 纯模板任务（无收益）
        # 存在性 ≠ 质量：exit_messages/summary 仅证明"角色回了话/引擎记了摘要"，
        # 不证明"做对了"。按 CLAUDE.md 红线，不可用文件存在替代人读判断 → 仅判 neutral。
        # positive 必须建在真实外部产物（≥1KB 非系统产出文件）上（见下方 _n 分支）。
        if _evidence == "exit_messages":
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — exit_messages 存在（回复≠产出），待真实产物确认"
        elif _evidence == "summary":
            _quality = "neutral"
            _detail = f"task《{_title[:40]}》by {_role} — summary 存在（摘要≠产出），待真实产物确认"
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
            "has_exit_messages": _evidence == "exit_messages",
            "has_summary": _evidence == "summary",
            "evidence_type": _evidence,
        })

    # 汇总写 bus
    _bus = BUS_CLIENT
    for a in assessments:
        # 评估签名未变则跳过 feedback 写入：任务完成后 1h 内每 tick 重评估结果完全相同，
        # 重复写不产生新信息，只把旧 fact 的 ts 刷新、重新顶进 unread。
        # 注意：只跳过 bus feedback，不跳过下方的 negative 复盘分支——
        # 复盘与 TASKS.md 回写自带幂等（_append_role_feedback 按 task_id 去重），
        # 被 continue 连带跳过会让角色的学习信号永久丢失。
        _sig = f"{a['quality']}|{a['output_count']}|{a['output_size']}|{a['evidence_type']}"
        _already_reported = _LAST_ASSESSMENT.get(a["task_id"]) == _sig
        # 签名只在写入**成功后**落账（codex P1-1）。先记后写会让瞬时失败
        # （bus 锁冲突/超时）在 1h 窗口内永不重试——原实现每 tick 重写恰好
        # 有自愈性，守卫不能把自愈性一并消掉。
        if not _already_reported:
            try:
                _r = subprocess.run(
                    [str(_bus), "write", "feedback",
                     f"[task_evidence] {a['detail']} — 质量: {a['quality']}",
                     "--evidence", json.dumps(a),
                     "--src", "task_evidence"],
                    capture_output=True, timeout=10)
                if _r.returncode == 0:
                    _LAST_ASSESSMENT[a["task_id"]] = _sig
            except Exception:
                pass
        # R2-2 self-reflection（Reflexion 洞察 3）: 无产出任务写 code_fix 根因总结，
        # 供后续 session 消费避免同类错误重复（失败后语言化复盘 > 纯重试）
        if a["quality"] == "negative":
            try:
                _reflect = {
                    "task_id": a["task_id"], "role": a["role"],
                    "title": a["title"][:60],
                    "root_cause": "无产出文件、无 bus 证据、无 exit_messages — 任务未真正执行或角色未响应",
                    "action_items": [
                        "检查角色是否存活/被拉起（survival_monitor 已接自动恢复）",
                        "检查任务模板是否被角色实际消费（route_all 是否创建 workflow）",
                        "确认角色 eval_criteria 是否阻止了执行（eval_consistency 预检）",
                    ],
                    "quality": "negative",
                }
                # self-reflect 与 feedback 各写一条，但共享同一签名。
                # 必须用**本轮开始时**的 _already_reported 判断，不能用
                # _LAST_ASSESSMENT 现查 —— 上面的 feedback 分支刚把自己落账，
                # 现查会恒为 True 而永远跳过 self-reflect（实测回归）。
                if not _already_reported:
                    _r2 = subprocess.run(
                        [str(_bus), "write", "code_fix",
                         f"[self-reflect] 任务《{a['title'][:40]}》无产出 — 需根因排查",
                         "--evidence", json.dumps(_reflect, ensure_ascii=False),
                         "--src", "task_evidence"],
                        capture_output=True, timeout=10)
                    # 失败则注销签名，让下一轮两条一起重试
                    if _r2.returncode != 0:
                        _LAST_ASSESSMENT.pop(a["task_id"], None)
                # TASKS.md 回写无条件执行：它自身按 task_id 幂等，
                # 且上一轮可能因文件缺失失败，早退会丢掉学习信号。
                # 回写评分信号到 role TASKS.md（闭环补全 —— 评分→prompt学习）
                # WHY: evaluate_and_feedback 原仅写 bus，role 无法从 negative 教训学习。
                # CLAUDE.md 禁止回写 CLAUDE.md，仅 TASKS.md 足矣。
                _append_role_feedback(a["role"], a["title"][:60], a["task_id"])
            except Exception:
                pass

    # 修剪只按 1h 窗口，不按本轮 LIMIT 结果集：窗口内实测 13 个 completed 任务
    # 竞争 LIMIT 10 槽位，排序翻转时被挤出的任务签名会被删，下一轮回到窗口
    # 又重写一遍。实测同一 fact 每 tick 重写一发（写放大 88.6×，R42）。
    # 按窗口修剪后：落出 TOP10 的任务保留签名，回来时命中 _already_reported。
    _window_ids = {r[0] for r in db.execute(
        "SELECT task_id FROM tasks WHERE status='completed' AND updated_at > ?",
        (_cutoff,)).fetchall()}
    for _k in [k for k in _LAST_ASSESSMENT if k not in _window_ids]:
        del _LAST_ASSESSMENT[_k]

    return {"evaluated": len(assessments), "assessments": assessments}


def extract():
    db = sqlite3.connect(str(DB))
    try:
        return _extract_with(db)
    finally:
        db.close()


def _extract_with(db):
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
        files = [f.name for f in ws.rglob("*") if f.is_file() and not _is_system_file(f.name)]
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
