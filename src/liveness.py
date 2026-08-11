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

# L4 步骤级扫描上限：>40 个 running 实例截断（只读信号，截断=不判忙，避免 CPU 放大）
_STEP_SCAN_LIMIT = 40
# 活跃步骤状态：与 _is_role_busy 信号2 一致
_ACTIVE_STEP_STATUS = ("notified", "running", "step_done_ready")


def _tmux_has_session(role: str, timeout: float = 3.0) -> bool | None:
    """L0: tmux 会话存活。返回 None = 无数据（tmux 不可用），调用方按放行处理。"""
    try:
        r = subprocess.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=timeout)
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


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
        return {"available": False, "assigned": None}


def _load_steps_json(template_id: str, db_conn) -> list | None:
    """从 workflow_templates.steps_json 读步骤定义（L4 用，脱离 engine 的 _workflows 缓存）。"""
    try:
        rows = _db_fetch(db_conn,
            "SELECT steps_json FROM workflow_templates WHERE template_id=?", (template_id,))
        if not rows:
            return None
        raw = rows[0][0]
        if not raw:
            return None
        data = json.loads(raw)
        return data.get("steps", []) if isinstance(data, dict) else None
    except (Exception, json.JSONDecodeError):
        return None


def _s4_stepping(role: str, db_conn=None, exclude_wf_id: str = "", workflows=None, **opts) -> dict:
    """L4: 步骤级最严格。running 实例 → 模板步骤 → target_role 匹配 → step_results 状态 + 豁免。

    与 _is_role_busy 信号2 完全一致：
    - 跳过 exclude_wf_id 自身
    - 跳过 wf_sub_ 子链（仅当 parent_wf_id == exclude_wf_id，codex 三维度审查 P2 修复）
    - step_results[step].status in ('notified','running','step_done_ready')
    - queued>=6 且同一工作流 → 豁免
    - timeout_count<3 才判忙
    """
    if db_conn is None:
        return {"available": False, "busy": False}
    try:
        rows = _db_fetch(db_conn,
            "SELECT instance_id, template_id, step_results, assigner, parent_wf_id "
            "FROM workflow_instances WHERE status='running'")
        if len(rows) > _STEP_SCAN_LIMIT:
            LOGGER.warning("liveness L4: %d running 实例超上限 %d，截断不判忙",
                           len(rows), _STEP_SCAN_LIMIT)
            return {"available": True, "busy": False, "truncated": True}
        for r in rows:
            inst_id = _row_get(r, "instance_id", 0)
            if exclude_wf_id and inst_id == exclude_wf_id:
                continue
            if str(inst_id or "").startswith("wf_sub_") and _row_get(r, "assigner", 3) == "workflow_engine" \
                    and (exclude_wf_id and _row_get(r, "parent_wf_id", 4) == exclude_wf_id):
                continue
            template_id = _row_get(r, "template_id", 1)
            # workflows 参数：优先用内存 WorkflowDef 缓存（_is_role_busy 委托时传入）。
            # 当 workflows 已提供但模板不在其中时，保持旧 _is_role_busy 语义（跳过，
            # 不查 DB）——engine 未加载的模板说明模板不存在或 schema 校验失败，
            # 其实例应由清理路径回收，不应由忙检测拦截（2026-08-12 review P1 修复）。
            if workflows is not None:
                wf_def = workflows.get(template_id)
                if wf_def is None:
                    continue
                steps = [{"id": s.id, "target_role": s.target_role} for s in getattr(wf_def, "steps", [])]
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
                if _s.get("target_role") != role:
                    continue
                _sdata = step_results.get(_s.get("id", ""), {})
                if not isinstance(_sdata, dict):
                    continue
                if _sdata.get("status") in _ACTIVE_STEP_STATUS:
                    _q = _sdata.get("queued", 0)
                    if _q >= 6 and exclude_wf_id and inst_id == exclude_wf_id:
                        LOGGER.info("liveness: %s queued=%d 同一工作流 → 豁免 busy", role, _q)
                        continue
                    if _sdata.get("timeout_count", 0) < 3:
                        LOGGER.info("liveness: skip %s busy with %s/%s (step=%s)",
                                    role, inst_id[:8], _s.get("id"), _sdata.get("status"))
                        return {"available": True, "busy": True,
                                "wf_id": inst_id, "step_id": _s.get("id", "?")}
        return {"available": True, "busy": False}
    except Exception:
        return {"available": False, "busy": False}


def _health_gate(role: str, health_dir: Path, pane_fallback: bool) -> tuple[bool | None, dict | None]:
    """复用 _is_role_busy 信号1 的健康门控。

    返回 (busy_decision, health)：
    - busy_decision=True/False：门控直接给出结论（健康文件主导）
    - busy_decision=None：需要继续 L3/L4 DB 检查

    pane_fallback 路径用 tmux_pane_active（#{pane_active} 焦点检查，非 has-session），
    与原 _is_role_busy 的 self._tmux_pane_alive 语义一致。
    """
    h = _read_health(role, health_dir)
    if h is None:
        return None, None
    overall = h.get("survival_overall", "unknown")
    if overall in ("orphan", "unknown"):
        LOGGER.info("liveness: health=%s → 角色不可用，不视为 busy", overall)
        return False, h
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

    # 降级：信号均不可用 → level_used 降到实际可用的最高级别
    level_used = max([lvl for lvl, sig in signals.items() if sig.get("available")], default=0)
    return {"busy": busy, "level_used": level_used, "signals": signals, "reason": reason}


def tmux_pane_active(role: str, timeout: float = 3.0) -> bool | None:
    """tmux pane_active（0/1 焦点）二值判断。保留原 _tmux_pane_alive 语义。

    tmux 3.4 的 pane_activity 恒为空导致 survival L2 失效，这里用 pane_active 兜底。
    """
    try:
        r = subprocess.run(["tmux", "list-panes", "-t", f"ccs-{role}", "-F", "#{pane_active}"],
                           capture_output=True, text=True, timeout=timeout)
        if r.returncode == 0 and "1" in r.stdout:
            return True
        return False
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
