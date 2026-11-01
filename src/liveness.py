#!/usr/bin/env python3
"""liveness.py — 角色存活/工作检测统一模块。

统合 4 类信号源（tmux / health 文件 / DB workflow_instances / bus 产出）
为 5 个级别（L0-L4），默认最高级别 L4（步骤级最严格）。

设计：
- 纯函数模块，零状态零类，不导入 engine/mixin，无循环依赖。
- L1/L2 读 health 文件（survival_monitor 每 120s 写入的合成结果），不重跑检测。
- L3/L4 读 DB（workflow_instances / workflow_templates），实时。
- 信号不可用（无 DB/无 health 文件/无 tmux）时降级：该信号返回 available=False，
  调用方按放行处理（busy=False），不阻断业务。

关键参数说明：
- pane_fallback: 仅 health 门控的 stale/dead/l2_false 分支用——tmux pane 近期
  （PANE_ACTIVE_WINDOW 秒内）有**输出活动**时视为忙（兜底）。注意判据是
  #{window_activity} 时间戳而非 #{pane_active} 焦点：焦点在单 pane 会话下恒为 1，
  曾使该兜底恒成立、空闲角色被判忙 → 任务排队 → 饿死 → 回收（见 tmux_pane_active）。
- workflows: 可选内存 WorkflowDef 缓存（dict[str, WorkflowDef]）。_is_role_busy
  委托时传入（与原 self._workflows 语义一致）；已提供但模板不在其中 → 跳过该实例
  （engine 未加载的模板应由清理路径回收，不由忙检测拦截）；未提供 → 回退读 DB
  workflow_templates.steps_json（独立调用场景）。
- 无扫描上限：实测 173 条 running 实例 + 模板查询合计 0.7ms，截断判据（>40）会在积压期把**所有角色**判为不忙 → 引擎继续投递 → 积压自我放大（2026-10-17 实测 82~173 个 running 实例常驻）。
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path

LOGGER = logging.getLogger("liveness")

HEALTH_DIR = Path(os.environ.get("CCS_HEALTH_DIR", str(Path.home() / ".hermes" / "run" / "ccs-health")))
SENTINEL_DIR = Path(os.environ.get("CCS_SENTINEL_DIR", str(Path.home() / ".hermes" / "run" / "ccs-sentinels")))

# pane 活跃窗口：与 survival_monitor._l2_check 的 thinking 判据同量级（300s），
# 保证"两处对同一 pane 是否在动"的结论一致。
PANE_ACTIVE_WINDOW = float(os.environ.get("CCS_PANE_ACTIVE_WINDOW", "300"))

# 活跃步骤状态：角色正被该步骤占用。step_done_ready 不在其中——它表示
# 「产出已落盘，等审批」，角色本身已空闲。曾把它算作忙，导致一个等审批的
# review 步骤锁死整个角色 90 分钟（TTL+grace），同角色其它工作流排队 7~21 次
# 全部饿死（实测 wf_sub_60e96bbdef28/s4 → product_architect 被锁，
# wf_3f71a2a78f76 停滞 235 分钟）。
_ACTIVE_STEP_STATUS = ("notified", "running")

# notified 步骤的"仍算占用"窗口。超过该窗口且 timeout_count<3，说明角色从未
# 响应过这次投递（403/卡在 login 提示等），此时继续判 busy 会让同角色的
# 其他工作流全部饿死——两条互不相干的链各卡一个 notified 步骤时，双方互为
# "忙"，谁都不会被重推（实测 wf_sub_4e63/wf_sub_b548 互锁 110 分钟，
# kick-stalled 每 10 分钟触发却因 busy 直接 continue）。
# running 不受此限：它是角色主动写的执行中状态，代表真在干活。
_STALE_NOTIFIED_SEC = float(os.environ.get("CCS_STALE_NOTIFIED_SEC", "900"))


# ── tmux 快照：一次 list-panes -a 拿全部会话 ─────────────────────────────
# WHY：每个 tmux 子进程 0.1~5s（实测 10 次采样 2 次 5s 超时，2026-10-22 08:33
# 三个探针对同一角色全部返回 None），而一个 tick 要探 19 个角色 × 3 个探针
# ≈ 60 次调用，每次都独立付一次超时 → 巡检 480s、tick 563s、主循环只剩 1 条
# 实例。快照把 N 次调用压成 1 次。
# ponytail：快照只新鲜 2s；角色被拉起/杀掉后必须 invalidate_tmux_snapshot()，
# 否则刚起来的角色会被旧快照判成死（kill-session 是破坏性动作）。
_SNAP_TS = 0.0
_SNAP: dict | None = None      # {role: (pane_pid, window_activity)}


def invalidate_tmux_snapshot() -> None:
    """角色被拉起/杀掉后调用：旧快照里没有新会话，会把刚起来的角色判死。"""
    global _SNAP_TS, _SNAP
    _SNAP_TS, _SNAP = 0.0, None


def tmux_snapshot(max_age: float = 2.0) -> dict | None:
    """一次 tmux list-panes -a 拿全部 ccs-* 会话的 (pane_pid, window_activity)。

    返回 None = 探测不可用（tmux 卡死/不可达），调用方按"未知"处理。
    """
    global _SNAP_TS, _SNAP
    _now = time.time()
    # 失败也缓存：tmux server 卡死时（实测 2026-10-22 09:20 list-sessions 20s 无响应）
    # 每次调用都会付满 5s 超时，而一个 tick 要探十几处 → tick 变成 180s+。
    # 缓存 None 把"每次 5s"压成"每 max_age 5s"。
    if _SNAP_TS and _now - _SNAP_TS < max_age:
        return _SNAP
    try:
        r = subprocess.run(
            ["tmux", "list-panes", "-a", "-F",
             "#{session_name}\t#{pane_pid}\t#{window_activity}"],
            capture_output=True, text=True, timeout=5)
        if r.returncode != 0:
            raise OSError(r.returncode, "list-panes")
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        _SNAP_TS, _SNAP = _now, None
        return None
    _d = {}
    for _line in r.stdout.splitlines():
        _p = _line.split("\t")
        if len(_p) >= 3 and _p[0].startswith("ccs-"):
            _d.setdefault(_p[0][4:], (_p[1].strip(), _p[2].strip()))
    _SNAP_TS, _SNAP = _now, _d
    return _d


def tmux_server_cgroup() -> str | None:
    """tmux server 所在 cgroup。无 server → ""；有 server 但读不到 → None（判不了）。

    tmux 调用失败按「没有 server」处理：ccs stop 走的是同一个 socket 上的
    kill-session，那条路不通时 stop 本身也是空操作，不存在误杀房东。
    """
    try:
        r = subprocess.run(["tmux", "display-message", "-p", "#{pid}"],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return ""
    pid = r.stdout.strip()
    if r.returncode != 0 or not pid.isdigit():
        return ""
    try:
        return Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[-1]
    except OSError:
        return None


def unit_control_group(role: str) -> str | None:
    """unit 的 control group。inactive → ""（没有 cgroup 可连坐）；问不出来 → None。"""
    try:
        r = subprocess.run(["systemctl", "--user", "show", "-p", "ControlGroup", "--value",
                            f"ccs@{role}.service"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def role_hosts_tmux_server(role: str) -> bool:
    """该角色的 unit 是不是 tmux server 的「房东」。

    tmux server 是进程间单例，cgroup 由创建它的 unit 决定且此后不变；pane 里的
    claude/bash 全部继承它。停掉房东 = KillMode=control-group 清空整个 cgroup =
    所有角色一起断（2026-11-01 12:14 实测 9 个 ccs@* unit 连坐重启 66 秒）。
    判不了按房东处理：误杀成本是全员中断，漏杀只是 idle 角色多活 30 分钟。

    两条杀 session 的路径（routing_daemon 的 G3 stop、survival_monitor 的
    ondemand idle 回收）共用这一份判据，别再各写一份。
    """
    server_cg, role_cg = tmux_server_cgroup(), unit_control_group(role)
    if server_cg is None or role_cg is None:
        return True
    return bool(server_cg) and server_cg == role_cg


def _tmux_has_session(role: str, timeout: float = 3.0) -> bool | None:
    """L0: tmux 会话存活。返回 None = 无数据（tmux 不可用），调用方按放行处理。"""
    snap = tmux_snapshot()
    if snap is None:
        return None
    return role in snap


def _read_health(role: str, health_dir: Path = HEALTH_DIR) -> dict | None:
    """读 health 文件。返回 None = 无文件或损坏。"""
    try:
        hp = health_dir / f"{role}.json"
        if not hp.exists():
            return None
        return json.loads(hp.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _s0_alive(role: str, health_dir: Path = HEALTH_DIR, **opts) -> dict:
    """L0: 进程存活。tmux has-session + 哨兵文件区分 ORPHAN/UNKNOWN/DEAD。"""
    alive = _tmux_has_session(role)
    sentinel = SENTINEL_DIR / f"{role}.json"
    has_sentinel = sentinel.exists()
    if alive is True and has_sentinel:
        status = "alive"
    elif alive is True and not has_sentinel:
        status = "unknown"
    elif alive is False and has_sentinel:
        status = "orphan"
    else:
        status = "dead"
    return {"available": alive is not None, "alive": bool(alive),
            "status": status, "has_sentinel": has_sentinel}


def _s1_thinking(role: str, health_dir: Path = HEALTH_DIR, **opts) -> dict:
    """L1: 思考中。读 health 文件 survival_l2_thinking（survival_monitor 每 120s 合成）。"""
    h = _read_health(role, health_dir)
    if h is None:
        return {"available": False, "thinking": None}
    return {"available": "survival_l2_thinking" in h, "thinking": h.get("survival_l2_thinking")}


def _s2_producing(role: str, health_dir: Path = HEALTH_DIR, **opts) -> dict:
    """L2: 最近有产出。读 health 文件 survival_l3_producing（bus 最近 10min）。"""
    h = _read_health(role, health_dir)
    if h is None:
        return {"available": False, "producing": None}
    return {"available": "survival_l3_producing" in h, "producing": h.get("survival_l3_producing")}


def _db_fetch(db_conn, sql: str, params: tuple = ()) -> list:
    """兼容 LifecycleManager（有 .query）和原生 sqlite3.Connection（有 .execute）。"""
    if hasattr(db_conn, "query"):
        return db_conn.query(sql, params)
    return db_conn.execute(sql, params).fetchall()


def _row_get(row, key: str, idx: int):
    """兼容 dict 行（测试/旧代码用键访问）和 tuple/sqlite3.Row 行（索引访问）。"""
    if isinstance(row, dict):
        return row.get(key)
    return row[idx] if len(row) > idx else None


def effective_role(inst: dict, step_id: str, fallback: str = "") -> str:
    """取步骤的实际执行角色：实例的 effective_roles 覆盖优先，否则回退模板值。

    背景：engineer 与 pg 是等价开发角色（persona 描述均为"按方案编码、自测、
    提 PR"），但主链模板写死 engineer → 实例全压 engineer、pg 闲置。分流决定
    必须**随实例持久化**才能在全部消费点一致生效：exit_messages 按角色过滤、
    busy 门控、exit_schema 校验的 workspace、codex review 的 cwd 等 20+ 处。
    在投递层单点替换已被三方审查证明会在下游制造更严重的不一致（"pg 写的产出
    永不被认可 → 步骤必然超时"）。

    inst 可以是 dict（DB 行）或任何带 effective_roles/step_results 的对象。
    """
    if inst is None:
        return fallback
    if isinstance(inst, dict):
        _er = inst.get("effective_roles")
    elif hasattr(inst, "keys"):  # sqlite3.Row：支持按键访问
        try:
            _er = inst["effective_roles"]
        except (IndexError, KeyError):
            _er = None
    else:
        _er = getattr(inst, "effective_roles", None)
    if isinstance(_er, str):
        import json as _json
        try:
            _er = _json.loads(_er or "{}")
        except (ValueError, TypeError):
            _er = {}
    if isinstance(_er, dict) and _er.get(step_id):
        return _er[step_id]
    return fallback


def _s3_assigned(role: str, db_conn=None, **opts) -> dict:
    """L3: 有活跃工作流记录。DB pending/running（同 WorkflowClient.check_task）。"""
    if db_conn is None:
        return {"available": False, "assigned": None}
    try:
        rows = _db_fetch(db_conn,
            "SELECT instance_id FROM workflow_instances "
            "WHERE assignee=? AND status IN ('pending','running') LIMIT 1",
            (role,))
        return {"available": True, "assigned": len(rows) > 0}
    except Exception:
        return {"available": False, "assigned": None, "error": "db_query_failed"}


def _load_steps_json(template_id: str, db_conn) -> list | None:
    """从 workflow_templates.steps_json 读步骤定义（L4 用，脱离 engine 的 _workflows 缓存）。

    格式兼容：DB 中 steps_json 实际存储为 list（直接步骤数组），但也兼容
    dict({"steps": [...]}) 格式。返回 [{"id": ..., "target_role": ...}, ...] 或 None。
    """
    try:
        rows = _db_fetch(db_conn,
            "SELECT steps_json FROM workflow_templates WHERE template_id=?", (template_id,))
        if not rows:
            # 同仓两条命名并存：实例行写裸名（engineer_feature），注册表存
            # WF- 前缀（WF-engineer_feature）。查不到即整块跳过 → 忙检测对
            # 这些实例永久静默失效（实测 wf_sub_4e63/wf_sub_b548 的
            # engineer_feature 在 running 中却从未被 pg 判忙）。
            # 与 wf_lifecycle._subflow_takeover 同款回退。
            _alt = (template_id[3:] if template_id.startswith("WF-")
                    else f"WF-{template_id}")
            rows = _db_fetch(db_conn,
                "SELECT steps_json FROM workflow_templates WHERE template_id=?",
                (_alt,))
        if not rows:
            return None
        raw = rows[0][0]
        if not raw:
            return None
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("steps", [])
        return None
    except Exception:
        return None


def _s4_stepping(role: str, db_conn=None, exclude_wf_id: str = "", workflows=None, **opts) -> dict:
    """L4: 步骤级最严格。running 实例 → 模板步骤 → target_role 匹配 → step_results 状态 + 豁免。

    与 _is_role_busy 信号2 完全一致：
    - 跳过 exclude_wf_id 自身
    - 跳过 wf_sub_ 子链（仅当 parent_wf_id == exclude_wf_id，codex 三维度审查 P2 修复）
    - step_results[step].status in _ACTIVE_STEP_STATUS（notified/running；
      step_done_ready 等审批时角色已空闲，见常量注释）
    - timeout_count<3 才判忙
    """
    if db_conn is None:
        return {"available": False, "busy": False}
    try:
        # effective_roles 用 COALESCE 兜底：旧测试库/旧 schema 无此列时，
        # 查询整体失败会让 _s4_stepping 返回 unavailable → 忙检测静默失效。
        try:
            rows = _db_fetch(db_conn,
                "SELECT instance_id, template_id, step_results, assigner, parent_wf_id, "
                "effective_roles "
                "FROM workflow_instances WHERE status='running'")
        except Exception:
            rows = _db_fetch(db_conn,
                "SELECT instance_id, template_id, step_results, assigner, parent_wf_id "
                "FROM workflow_instances WHERE status='running'")
        # exclude 自身的父链 id：下面要豁免「父链占用同角色」——子链投递时
        # 父链的同角色步骤正卡在 notified，原单向判据看不见反向，子链被判忙。
        _exclude_parent = ""
        if exclude_wf_id:
            try:
                _er = _db_fetch(db_conn,
                    "SELECT parent_wf_id FROM workflow_instances WHERE instance_id=?",
                    (exclude_wf_id,))
                _exclude_parent = (_row_get(_er[0], "parent_wf_id", 0) if _er else "") or ""
            except Exception as _e:
                _exclude_parent = ""
                # 静默降级会丢掉反向豁免，表现为「偶发判忙」——难归因。
                LOGGER.warning("_s4_stepping: 查 %s 的 parent_wf_id 失败，"
                               "反向豁免失效（保守判忙）: %s", exclude_wf_id[:12], _e)
        for r in rows:
            inst_id = _row_get(r, "instance_id", 0)
            if exclude_wf_id and inst_id == exclude_wf_id:
                continue
            # 父子链豁免必须**双向**（原实现只有单向：子链 parent == exclude）：
            #   (a) 本实例是 exclude 的子链 → 父链投递时不被自己的子链堵
            #   (b) 本实例是 exclude 的父链 → 子链投递时不被父链堵。实测子链
            #       wf_sub_f3621 投递 pm/s1 时，父链 wf_e50bb 的 s1 同角色卡在
            #       notified → 判忙 → 子链投不出 → delivery_blocked（日志
            #       「send-to-role pm 跳过: 有其他 workflow 在进行中」每 30s 重复）。
            #       父子同角色的链（WF-product_delivery s1 是 pm，其子链
            #       pm_requirements 首步也是 pm）必然命中 → 子链机制天然失效。
            _parent = _row_get(r, "parent_wf_id", 4)
            if str(inst_id or "").startswith("wf_sub_") and _row_get(r, "assigner", 3) == "workflow_engine" \
                    and (exclude_wf_id and _parent == exclude_wf_id):
                continue
            if _exclude_parent and inst_id == _exclude_parent:
                continue
            template_id = _row_get(r, "template_id", 1)
            # workflows 参数：优先用内存 WorkflowDef 缓存（_is_role_busy 委托时传入）。
            # 当 workflows 已提供但模板不在其中时，保持旧 _is_role_busy 语义（跳过，
            # 不查 DB）——engine 未加载的模板说明模板不存在或 schema 校验失败，
            # 其实例应由清理路径回收，不应由忙检测拦截（2026-08-12 review P1 修复）。
            if workflows is not None:
                wf_def = workflows.get(template_id)
                if wf_def is None:
                    # 模板不在内存缓存时不跳过（否则漏判 busy）——回退 DB 加载 steps，
                    # 否则角色真在干活的 running 实例被漏判 → 上层 delivery_blocked 早关真任务。
                    steps = _load_steps_json(template_id, db_conn)
                else:
                    # wf_def 可能是 dict（TemplateRegistry.get 返回 dict）或对象（engine WorkflowDef）
                    _raw_steps = wf_def.get("steps") if isinstance(wf_def, dict) else getattr(wf_def, "steps", [])
                    steps = []
                    for s in (_raw_steps or []):
                        if isinstance(s, dict):
                            steps.append({"step_id": s.get("step_id") or s.get("id"), "target_role": s.get("target_role")})
                        else:  # 对象：WorkflowDef step（.id/.target_role）
                            steps.append({"step_id": getattr(s, "id", None), "target_role": getattr(s, "target_role", None)})
            else:
                steps = _load_steps_json(template_id, db_conn)
            if not steps:
                continue
            step_results = {}
            try:
                step_results = json.loads(_row_get(r, "step_results", 2) or "{}") or {}
            except (json.JSONDecodeError, TypeError):
                step_results = {}
            if not isinstance(step_results, dict):
                continue
            for _s in steps:
                _sid = _s.get("step_id") or _s.get("id") or ""
                # 开发角色池分流后执行者是 pg，但模板 target_role 仍是 engineer。
                # 只比对模板值 → 已改投 pg 的实例仍被算作"engineer 忙"，
                # engineer 被两个自己根本不执行的子链永久堵死，父链 s4 停滞
                # （实测 wf_sub_4e63/wf_sub_b548 effective_roles={"s1":"pg"}，
                # 03:25 起每 tick 打 liveness: skip engineer busy）。
                # effective_role() 与 engine 投递侧解析同一真相源。
                # 行类型三态（dict / sqlite3.Row / 裸 tuple）统一按列名取值，
                # 不把行对象直接交给 effective_role——裸 tuple 无 .keys()，
                # 会让覆盖读不到、静默退回模板值（本修复自身的第一个 bug）。
                _er_row = {"effective_roles": _row_get(r, "effective_roles", 5)}
                if effective_role(_er_row, _sid, fallback=_s.get("target_role")) != role:
                    continue
                _sdata = step_results.get(_sid, {})
                if not isinstance(_sdata, dict):
                    continue
                if _sdata.get("status") in _ACTIVE_STEP_STATUS:
                    # 陈旧的 notified（投递后长期零响应）不构成"占用"：否则两条
                    # 互不相干的链各卡一个 notified 步骤时互为"忙"，双双饿死。
                    # 只豁免 notified；running 是角色主动写的，仍是真占用。
                    if _sdata.get("status") == "notified":
                        _notified_at = _sdata.get("notified_at") or _sdata.get("ts") or 0
                        if _notified_at and (time.time() - _notified_at) >= _STALE_NOTIFIED_SEC:
                            LOGGER.info("liveness: %s 步 %s/%s notified 已 %.0fs 无响应 → 不计占用",
                                        role, inst_id[:8], _s.get("step_id") or _s.get("id"),
                                        time.time() - _notified_at)
                            continue
                    if (_sdata.get("timeout_count") or 0) < 3:
                        LOGGER.info("liveness: skip %s busy with %s/%s (step=%s)",
                                    role, inst_id[:8], _s.get("step_id") or _s.get("id"), _sdata.get("status"))
                        return {"available": True, "busy": True,
                            "wf_id": inst_id, "step_id": _s.get("step_id") or _s.get("id") or "?"}
        return {"available": True, "busy": False}
    except Exception:
        return {"available": False, "busy": False, "error": "db_query_failed"}


def _health_gate(role: str, health_dir: Path, pane_fallback: bool) -> tuple[bool | None, dict | None]:
    """复用 _is_role_busy 信号1 的健康门控。

    返回 (busy_decision, health)：
    - busy_decision=True/False：门控直接给出结论（健康文件主导）
    - busy_decision=None：需要继续 L3/L4 DB 检查

    pane_fallback 路径用 tmux_pane_active（pane 近期有无输出，非 has-session），
    与原 _is_role_busy 的 self._tmux_pane_alive 兜底意图一致。
    """
    h = _read_health(role, health_dir)
    if h is None:
        return None, None
    overall = h.get("survival_overall", "unknown")
    if overall in ("orphan", "unknown"):
        # 不短路: orphan 可能因 tmux 瞬时故障误判, DB 若有活跃步骤仍应判 busy
        # （防新任务推送打断工作中角色）。unknown 同理交给 L3/L4 DB 定结论。
        LOGGER.info("liveness: health=%s → 交 L3/L4 DB 判定", overall)
        return None, h
    if overall in ("stale", "dead"):
        if pane_fallback and tmux_pane_active(role) is True:
            LOGGER.info("liveness: health=%s 但 tmux pane 活跃 → 视为 busy", overall)
            return True, h
        return False, h
    if h.get("survival_l2_thinking") is False:
        if pane_fallback and tmux_pane_active(role) is True:
            LOGGER.info("liveness: l2_thinking=false 但 tmux pane 活跃 → 视为 busy")
            return True, h
        return False, h
    return None, h


def role_liveness(role: str, level: int = 4, exclude_wf_id: str = "",
                  db_conn=None, health_dir: Path = HEALTH_DIR, pane_fallback: bool = True,
                  workflows=None, **opts) -> dict:
    """统一角色存活/忙检测。level: 最高检测级别（默认 4 = 步骤级最严格）。

    workflows: 可选内存 WorkflowDef 缓存（dict[str, WorkflowDef]），L4 步骤级扫描
    优先用它（与原 _is_role_busy 的 self._workflows 语义一致），否则读 DB steps_json。

    返回: {"busy": bool, "level_used": int, "signals": {level→dict}, "reason": str}
    """
    level = max(0, min(int(level), 4))
    signals: dict[int, dict] = {}
    busy = False
    reason = ""

    # 健康门控（L1/L2 前置，语义与 _is_role_busy 信号1 一致）
    if level >= 1:
        gate, h = _health_gate(role, health_dir, pane_fallback)
        if gate is not None:
            signals[1] = {"available": True, "gate": gate, "overall": (h or {}).get("survival_overall")}
            signals[2] = {"available": True, "gate": gate, "producing": (h or {}).get("survival_l3_producing")}
            # 健康门控判 idle 时，若 L4 DB 步骤扫描有 running 实例（含 think 状态），
            # 不能 short-circuit 回 busy=False——否则真在干活的角色被漏判 idle，
            # step_handler 误走 delivery_blocked。让 L4 有判责。
            if gate is False and level >= 4 and db_conn is not None:
                s4 = _s4_stepping(role, db_conn, exclude_wf_id, workflows=workflows)
                signals[4] = s4
                if s4.get("available") and s4.get("busy") is True:
                    return {"busy": True, "level_used": 4, "signals": signals,
                            "reason": f"stepping={s4.get('wf_id')}/{s4.get('step_id')} (health_gate=idle but L4 stepping)"}
            if gate is not False:
                return {"busy": gate, "level_used": level, "signals": signals,
                        "reason": f"health_gate={gate}"}

    # L0: tmux 存活（不直接判忙，仅信息；launcher 侧可独立用）
    if level >= 0:
        s0 = _s0_alive(role, health_dir)
        signals[0] = s0
        if not s0.get("alive") and s0.get("available"):
            # 角色 session 不在：非"工作忙"，交给上层存活逻辑；busy=False
            busy = busy or False
            reason = f"session={s0.get('status')}"

    # L1: 思考中（仅 level<3 时直接判忙；level>=3 时由 L3/L4 DB 判定）
    if level >= 1:
        s1 = _s1_thinking(role, health_dir)
        signals[1] = s1
        if level < 3 and s1.get("available") and s1.get("thinking") is True:
            busy = True
            reason = f"thinking={s1.get('thinking')}"

    # L2: 产出中（仅 level<3 时直接判忙；level>=3 时由 L3/L4 DB 判定）
    if level >= 2:
        s2 = _s2_producing(role, health_dir)
        signals[2] = s2
        if level < 3 and s2.get("available") and s2.get("producing") is True and not busy:
            busy = True
            reason = f"producing={s2.get('producing')}"

    # L3: 有活跃工作流（仅 level=3；level=4 由 L4 步骤级判定，跳过此简单查询
    # 避免 MagicMock 等测试环境的 mock 数据被所有查询共用导致误判）
    if level == 3:
        s3 = _s3_assigned(role, db_conn)
        signals[3] = s3
        if s3.get("available") and s3.get("assigned") is True and not busy:
            busy = True
            reason = f"assigned={s3.get('assigned')}"

    # L4: 步骤级最严格（含豁免）
    if level >= 4:
        s4 = _s4_stepping(role, db_conn, exclude_wf_id, workflows=workflows)
        signals[4] = s4
        if s4.get("available") and s4.get("busy") is True and not busy:
            busy = True
            reason = f"stepping={s4.get('wf_id')}/{s4.get('step_id')}"

    # level_used: 实际参与判定的最高级别（排除 signal 缺失的级别）。
    # 注意 L0 从不判忙（仅信息），故从参与判定的级别中排除；未被使用的高级
    # 信号（如 level=4 时 L1/L2 仅记录不判忙）不计数。
    _used = [lvl for lvl, sig in signals.items() if lvl >= 1 and sig.get("available") and lvl <= level]
    level_used = max(_used, default=0)
    return {"busy": busy, "level_used": level_used, "signals": signals, "reason": reason}


def tmux_pane_active(role: str, timeout: float = 3.0,
                     window: float = PANE_ACTIVE_WINDOW) -> bool | None:
    """pane 在最近 window 秒内是否有输出活动（#{window_activity} 时间戳，非焦点标志）。

    原实现读 #{pane_active}（焦点 0/1）—— 单 pane 会话下恒为 1，等于恒返回 True，
    使 stale/dead 兜底永远成立：**空闲被翻译成忙** → 任务排队 → 饿死 → 回收。
    2026-10-17 实测全链停摆（running 实例 82~173 常驻、角色被反复判忙、投递全排队），
    见 bus #395128/#395235/#395251。

    #{window_activity} 是 pane 最后一次输出的 unix 时间戳，实测随输出递增，
    与 survival_monitor._l2_check 同源（该处 2026-10-16 已从 pane_activity 修正）。

    超窗后还要看"CLI 是否仍在运行"：codex review 这类长工具调用期间 pane 零输出
    （实测 reviewer 静默 8.5~11.7min），只按时间戳会把正在干活判成死亡 →
    auto-cancel 杀掉工作流（实测 wf_sub_8de33/s1、wf_sub_fb975/s1）。
    Claude Code 忙态底栏带 `esc to interrupt`，空闲态不带——这是与"已死"
    区分的唯一可靠视口信号。
    返回值：True=近期有活动或 CLI 忙；False=tmux 可达、静默超窗且 CLI 空闲；
    None=探测不可用（不判忙）。
    """
    snap = tmux_snapshot()
    if snap is None:
        return None  # 探测不可用 ≠ 不活跃
    if role not in snap:
        return False  # 会话不存在（与原 list-panes 非零返回同义）
    raw = snap[role][1]
    if not raw or raw == "0":
        return None  # 无数据 ≠ 不活跃，交调用方按未知处理
    try:
        if (time.time() - float(raw)) < window:
            return True
    except (TypeError, ValueError):
        return None
    # 仅在耗时阶段付这次额外 capture-pane（正常路径不增加 tmux 调用）
    return pane_cli_busy(role, timeout)


_CLI_BUSY_MARKERS = ("esc to interrupt", "esc to cancel", "ctrl+b to run in background")
# 429/403 客户端重试退避：CLI 自己在数秒后重试，期间 pane 无输出、转录也不写，
# 两个判活信号同时为静默 → 被条件4 当僵尸回收（实测 maintainer 410s 空档，
# wf_rw_3df85d1ed26e 15:16:47 被杀，而角色 42 分钟后写出 complete）。
# 判据与 role_lifecycle._recover_rate_limited_roles 同源（Retrying + attempt）：
# attempt 用尽后该行消失，回收路径自动恢复，不会造出永不回收的实例。
_CLI_RETRY_MARKERS = ("retrying in", "attempt")


_PANE_TAIL_LINES = 6   # 底栏状态行上方的 tip/输入框最多 3~5 行


def _pane_tail(role: str, timeout: float = 3.0) -> str | None:
    """pane 底部若干行（小写）。只看底部：角色正文里讨论 "esc to interrupt"、贴日志里的
    "Retrying … attempt" 时，全文匹配会把该角色所有投递永久判忙。None=探测失败。"""
    try:
        r = subprocess.run(
            ["tmux", "capture-pane", "-p", "-t", f"ccs-{role}"],
            capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return None
        lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
        return "\n".join(lines[-_PANE_TAIL_LINES:]).lower()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def pane_cli_busy(role: str, timeout: float = 3.0) -> bool | None:
    """判断 CLI 是否仍在执行（底栏 esc/retrying 标记）。调用方可无条件调用。

    返回 None 表示探测失败——调用方按"不可用不判死"处理。
    """
    tail = _pane_tail(role, timeout)
    if tail is None:
        return None
    if any(m in tail for m in _CLI_BUSY_MARKERS):
        return True
    # 退避中的 CLI 不显示 esc 提示，只挂 "Retrying in 4m 27s · attempt 19/30"：
    # 仅凭 _CLI_BUSY_MARKERS 会把"正在重试"读成"已死"（#433589 后果 A）。
    return all(m in tail for m in _CLI_RETRY_MARKERS)


def pane_cli_retrying(role: str, timeout: float = 3.0) -> bool | None:
    """CLI 是否卡在传输层/配额退避（底栏 `Retrying in … · attempt n/m`）。

    与 pane_cli_busy 分开：busy=正在执行且会自己结束；retrying=在等上游回来，
    期间没有工具调用、转录也不写。步骤超时路径据此给宽限而不是记一次超时
    （实测 2026-10-27 11:28 food_chain_loop/s0：engineer 卡在
    `Connection refused … Retrying in 0s · attempt 10/15` 两小时 → 4 次超时回收）。
    """
    tail = _pane_tail(role, timeout)
    if tail is None:
        return None
    return all(m in tail for m in _CLI_RETRY_MARKERS)
