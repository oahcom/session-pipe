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
import os, sqlite3, json, subprocess, sys, time
from pathlib import Path

# env 覆盖: 与 paths.py 同源的两个变量。不隔离时 engine.run_once 会往生产库
# INSERT workflow_logs 并往真实 ccs-workspaces/<role>/TASKS.md 追加负反馈
# （2026-10-22 审计实测：test_workcycle_e2e 一轮即新增 1 行 + 1 条 negative）。
DB = Path(os.environ.get("SESSION_PIPELINE_WORKFLOWS_DB") or
          Path.home() / ".hermes" / "state" / "workflows.db") 
WORKSPACES = Path(os.environ.get("SESSION_PIPELINE_CCS_WORKSPACES") or
                  Path.home() / "ccs-workspaces")
BUS_CLIENT = Path.home() / ".hermes" / "scripts" / "bus_client.py"

# 系统文件：不是角色产出，不应计入证据
# 前缀匹配覆盖 CLAUDE.md.1/.2 等轮转备份副本（子 agent 审计发现 98% 指标虚增来自此）
SYSTEM_FILES = ("TASKS.md", "CLAUDE.md", "test_output.md", "base.md")
SYSTEM_PREFIXES = ("CLAUDE.md.", "TASKS.md.")
# 非产出目录：缓存/依赖/版本库里的文件不是角色产出。
# 用黑名单而非"排除所有点目录"：.github/workflows 这类是合法交付（codex 第 4 轮 nit）。
NON_OUTPUT_DIRS = ("__pycache__", "node_modules", "venv", ".git", ".pytest_cache",
                   ".mypy_cache", ".ruff_cache", ".cache", ".venv", ".tox")


def _is_system_file(name: str) -> bool:
    return name in SYSTEM_FILES or any(name.startswith(p) for p in SYSTEM_PREFIXES)

# 评估结果幂等守卫：task_id → 上轮评估签名。
# WHY: 本函数由 daemon 每 10s 调用，而 bus 写入是 ON CONFLICT(t) DO UPDATE（刷新 ts），
# 重复写会把已沉底的旧 fact 顶回 unread 列表，实测单条 fact 被写 1091 次。
# 签名原先只存进程内存，前提是「24h 内非计划重启 0 次」——这条前提已被推翻：
# daemon 会在任意 src 文件 mtime 变化时就地 re-exec（daemon.py:229），而三个 agent
# 同时在改这个仓库，等于每次改动都清空签名。实测同一 task 的 [task_evidence] 每
# ~330s 重发一次、每次投给 5 个观察角色（ccs-monitor/consumer/knowledge_curator/
# optimizer/pm），6h 133 行。故按本文件预留的方案落库（action='evidence_assessed'）。
_LAST_ASSESSMENT: dict[str, str] = {}
# 评语文案版本：进签名用。改 detail 文案时 +1，让历史 fact 在回填时能重写（codex 第 3 轮 P2）
_DETAIL_VERSION = 4
_SEEN_LOADED = False


def _load_assessed(db) -> None:
    """从 workflow_logs 恢复签名，跨 os.execv 生效（只做一次）。"""
    global _SEEN_LOADED
    if _SEEN_LOADED:
        return
    _SEEN_LOADED = True
    try:
        for _r in db.execute("SELECT task_id, detail FROM workflow_logs "
                             "WHERE action='evidence_assessed' ORDER BY id DESC LIMIT 4000"):
            if _r["task_id"] and _r["task_id"] not in _LAST_ASSESSMENT:
                _LAST_ASSESSMENT[_r["task_id"]] = _r["detail"]
    except Exception:
        pass


def _record_assessed(db, task_id, sig) -> bool:
    """签名落库（跨 os.execv 存活）。失败不能静默：内存已置位，库侧没有这条
    → 下次 re-exec 会当没评估过、把同一 fact 再写一遍（写放大复发）。"""
    try:
        db.execute("INSERT INTO workflow_logs(task_id, action, actor, detail, ts) "
                   "VALUES(?,?,?,?,?)",
                   (task_id, "evidence_assessed", "task_evidence", sig, time.time()))
        db.commit()
        return True
    except Exception as _e:
        print(f"[task_evidence] 签名落库失败 task={task_id}（下次重启会重写）: {_e}",
              file=sys.stderr)
        return False

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
        # 目录级排除：缓存/依赖/版本库里的文件不是产出。实测 engineer 窗口内只有
        # .coverage(53KB) 与 .pytest_cache/.../nodeids(20KB)，旧口径直接判 positive。
        # 点文件（.coverage/.DS_Store 等）从来不是交付物；点**目录**只按黑名单排除，
        # 以便 .github/workflows/ci.yml 这类合法交付仍计入（codex 第 5 轮 blocker）
        if f.name.startswith(".") or any(p in NON_OUTPUT_DIRS
                                         for p in f.relative_to(ws).parts):
            continue
        try:
            st = f.stat()
            # 排除空文件与 <1KB 的疑似模板残留
            if st.st_size < 1024:
                continue
            # 时间窗口：只认任务**尾部**产出。旧口径 [created-30min, updated+30min]
            # 覆盖整段任务时长，长链/并发链会把同一份产出重复算给多个 task_id
            # （实测 qa 同窗命中 test_cases.md 与 test_cases-wf_sub_*.md 两份）。
            if t_created and t_updated:
                lo = max(t_created, t_updated - 3600) - 300
                if not (lo <= st.st_mtime <= t_updated + 300):
                    continue
        except OSError:
            continue
        files.append(f)
    return files


def _output_summary(files):
    """产出摘要：文件数 + 总大小 + 样例。"""
    if not files:
        return 0, 0, []
    # stat 可能因并发删除抛错：一个文件失败不该让整轮评估炸掉（反馈全丢）
    total = 0
    for f in files:
        try:
            total += f.stat().st_size
        except OSError:
            continue
    names = sorted(f.name for f in files)[:5]
    return len(files), total, names


def _classify_outputs(n: int, size: int, evidence: str, bus_evidence: bool) -> str:
    """任务收益 → 质量档。顺序即判据：真实产物 > 量小产出/回话/bus > 纯模板。

    WHY: 旧实现把 exit_messages 判在最前，而几乎每条完成的链都有该字段 ——
    唯一据实产物判 positive 的规则只在无该字段时才可达（实际几乎不可达）。实测最近 120 条完成任务里，
    85 条（71%）有 ≥3 个 ≥1KB 产出（如 pm 19 文件/84KB）却被判「回复≠产出」，
    反过来教角色「发消息比写产物重要」。
    """
    if n >= 3 or (n >= 1 and size > 2048):
        return "positive"
    if n >= 1 or evidence or bus_evidence:
        return "neutral"
    return "negative"


def evaluate_and_feedback(hours: float = 1):
    """提取已完成任务证据 → 写质量反馈到 bus，供 coordinator/workflow_engine 消费。

    默认只评估最近 1 小时完成的 workfow，避免重复。hours 放大窗口用于回填：
    判据修正后，历史任务需要按新判据重算一次，否则旧的假中性 fact 会一直留着。
    收益判定由规则引擎基于真实产出证据（bus 消息、产出文件、workflow 进度）
    独立评估，不使用标题长度等表面启发式。

    连接必须关闭：本函数由引擎 daemon 每 tick 调用（task_generator.py:156），
    原实现裸 connect 从不 close —— 实测每轮泄漏 1 个 fd，与 Blackboard 同类，
    9 分钟累积后随引擎进程一并崩溃（2026-10-17）。
    """
    db = sqlite3.connect(str(DB))
    try:
        return _evaluate_with(db, hours=hours)
    finally:
        db.close()


def _evaluate_with(db, hours: float = 1):
    db.row_factory = sqlite3.Row
    _load_assessed(db)
    _now = time.time()
    _cutoff = _now - hours * 3600

    completed = db.execute(
        "SELECT task_id, title, assignee, created_at, updated_at FROM tasks "
        "WHERE status='completed' AND updated_at > ? "
        "ORDER BY updated_at DESC LIMIT ?",
        # 回填不设上限：LIMIT 500 会让 7 天窗口里更旧的 246 条永远轮不到（codex 第 3 轮 P2）
        (_cutoff, 10 if hours <= 1 else -1)
    ).fetchall()
    if not completed:
        return {"evaluated": 0, "assessments": []}

    # 每任务的独立 bus 证据：按 task_id 搜索（不再用全局 'task' 搜索）
    def _bus_evidence(tid, role):
        """角色自己写的 bus 证据。

        两个坑（实测 2026-10-23）：
        1. 不带 --json 时 bus_client 输出的是人读的 "🔍 N results"，json.loads 必失败，
           该判据自上线起恒为 False（"有价值的逻辑从没生效"）。
        2. 本模块自己的 [task_evidence] fact 也带 task_id，下一 tick 会被当成"角色说过话"
           → negative 翻成 neutral，自己给自己作证。故只认 src == 该角色 的 fact。
        """
        try:
            _r = subprocess.run(
                # limit 100 而不是 20：自评模块对同一 task 会写 2~3 条 fact，
                # 窗口太小时角色自己的消息可能被挤出去（limit 在 src 过滤之前生效）。
                [str(BUS_CLIENT), "search", tid, "--limit", "100", "--json"],
                capture_output=True, text=True, timeout=15)
            _data = json.loads(_r.stdout)
            _facts = _data.get("facts") or _data.get("results") or _data.get("items") or []
            # 只有"角色自己说的"才算回话：自评模块/引擎/巡检/其他角色都按 task_id 写 fact，
            # 算进来等于让别人替角色作证（negative → neutral）。
            # 全段落都在 try 内：bus_client 输出异常形状（list/None/非 dict 元素）必须降级为
            # "无证据"，不能抛出去把整轮评估打断。
            return any(isinstance(f, dict) and (f.get("src") or "") == role
                       for f in _facts)
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
        _bus = _bus_evidence(_tid, _role)
        _evidence = _wf_evidence.get(_tid, "")

        _quality = _classify_outputs(_n, _size, _evidence, _bus)
        # 措辞按 evidence 取值：exit_messages 是"回复"，summary 是"摘要"，不能混称
        _ev_label = {"exit_messages": "回复≠产出", "summary": "摘要≠产出"}.get(_evidence, "无产出")
        if _quality == "positive":
            _detail = (f"task《{_title[:40]}》by {_role} — {_n} 个产出文件（{_size//1024}KB），有实质产出"
                       + (f"（另有 {_evidence}）" if _evidence else ""))
        elif _evidence:
            _detail = f"task《{_title[:40]}》by {_role} — 仅 {_evidence}，无合格产出文件（{_ev_label}）"
        elif _n >= 1:
            _detail = f"task《{_title[:40]}》by {_role} — {_n} 个产出文件（{_size//1024}KB），疑似模板残留"
        elif _bus:
            _detail = f"task《{_title[:40]}》by {_role} — 无产出文件但有 bus 证据，进行中"
        else:
            _detail = f"task《{_title[:40]}》by {_role} — 无产出、无证据，纯模板任务"
        assessments.append({
            "task_id": _tid, "role": _role,
            "title": _title, "quality": _quality,
            "detail": _detail,
            "output_files": _names,
            "output_count": _n,
            "output_size": _size,
            # 角色到底有没有"说过话"：无 exit_messages 且无 bus 证据 = 引擎空推进，
            # 那不是角色的错（实测 7 天 1120 个 completed 步骤里 349 个无任何回话，
            # 其中 engineer/maintainer 的产出在代码仓库、不在 workspace，按目录判必然 0 文件）
            "role_spoke": bool(_evidence or _bus),
            "has_bus_evidence": _bus,
            "has_exit_messages": _evidence == "exit_messages",
            "has_summary": _evidence == "summary",
            "evidence_type": _evidence,
        })

    # 汇总写 bus：一轮评估里所有写入**都成功**才落账签名（codex P1-1）。
    # 否则下轮命中同一签名直接 continue，失败的那条永不重试（混合成败：feedback
    # 成功会先落账，blocker 失败就被吞掉）。
    _bus = BUS_CLIENT

    def _bus_write(*args) -> bool:
        try:
            return subprocess.run(
                [str(_bus), "write", *args], capture_output=True, timeout=10
            ).returncode == 0
        except Exception:
            return False

    for a in assessments:
        # 评估签名未变则跳过：任务完成后 1h 内每 tick 重评估结果完全相同，
        # 重复写不产生新信息，只把旧 fact 的 ts 刷新、重新顶进 unread。
        # 签名只取"结论"维度：output_count/output_size 每 tick 都在抖（角色还在写），
        # 带上它们等于每分钟一个新签名 -> 实测 83min 内同一任务写 18 条同文案 fact
        # （engineer s4 / PA s2 / pm s1 / reviewer 共 53 条），每个角色都得读一遍自证。
        # 文案版本也进签名：措辞变了要重写，否则 --hours 回填对"质量不变、detail 变"的历史 fact 无效
        _sig = f"{a['quality']}|{a['evidence_type']}|{_DETAIL_VERSION}"
        if _LAST_ASSESSMENT.get(a["task_id"]) == _sig:
            continue
        # bus 按标题 UPSERT。标题必须**只由结论维度组成**：带 detail（含文件数、措辞）
        # 就会每 tick 插新行（实测 6h 内 90 条里 75 条重复），而且 _DETAIL_VERSION 升级后
        # 旧文案对应的 fact 永远覆盖不掉 —— "回填重写历史 fact" 就不成立（第三方审查 2026-10-26）。
        # detail 全文进 evidence（JSON），消费方取最近一条即可。
        _ok = _bus_write(
            "feedback",
            f"[task_evidence] {a['task_id']} {a['role']} — 质量: {a['quality']}",
            "--evidence", json.dumps(a, ensure_ascii=False),
            "--src", "task_evidence")
        # negative ⟺ 0 产出文件且无回话（n>=1 已在 _classify_outputs 判 neutral），
        # 所以这里的 negative 必然是引擎空推进：步骤 completed 但角色一句话没说、
        # workspace 里也没有文件。不写 TASKS.md（那是拿引擎的空推进罚角色，R161），
        # 改发 blocker 让运维看引擎侧。
        if a["quality"] == "negative":
            _ok = _bus_write(
                "blocker",
                f"[task_evidence] {a['task_id']} completed 但角色零回话零产出",
                "--evidence", json.dumps({
                    "task_id": a["task_id"], "role": a["role"],
                    "title": a["title"][:60],
                    "detail": a["detail"],
                    "why": "exit_messages/summary/bus 全空 -> 步骤疑似被引擎空推进；"
                           "该角色产出若在代码仓库，按 workspace 目录判也必然 0 文件",
                }, ensure_ascii=False),
                "--src", "task_evidence") and _ok
        if _ok:
            _LAST_ASSESSMENT[a["task_id"]] = _sig
            _record_assessed(db, a["task_id"], _sig)
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

    # 3. 产出文件统计（与评分同一取样口径**含任务时间窗**，否则导出与评分互相矛盾）
    def count_output_files(role, t_created=None, t_updated=None):
        files = _output_files(role, t_created, t_updated)
        return len(files), [f.name for f in files[:10]]

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
        n_files, sample_files = count_output_files(role, t["created_at"], t["updated_at"])

        # exit_messages 检查：引擎是否捕获了角色产出证据。
        # 旧写法里套了 wf.get("wf_results") 分支 —— 该键从未存在（wf_map 只放 step_results，
        # 且已 json.loads 成 dict），整段是解析不到的兜底（第三方审查 2026-10-26）。
        has_exit_messages = any(
            isinstance(v, dict) and v.get("exit_messages")
            for wf in wfs for v in wf["step_results"].values()
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
    if "--feedback" in sys.argv:
        # --hours N：回填窗口（默认 1h = 正常 tick 口径）
        _h = 1.0
        if "--hours" in sys.argv:
            try:
                _h = max(1.0, float(sys.argv[sys.argv.index("--hours") + 1]))
            except (IndexError, ValueError):
                print("--hours 需要数字，回落到 1h", file=sys.stderr)
        r = evaluate_and_feedback(hours=_h)
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        data = extract()
        print(json.dumps(data, ensure_ascii=False, indent=2))
