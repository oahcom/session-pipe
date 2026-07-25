"""
pipeflow/anomaly.py — 工作流异常检测与自愈

从 WorkflowEngine 中抽离，职责：
  - 检测多步骤超时
  - 自动通知 / 升级 / 重推
"""

import json
import logging
import time
from typing import Any

LOGGER = logging.getLogger(__name__)


def check_anomalies(lifecycle, workflows: dict[str, Any], bb) -> None:
    """Detect workflows stuck across multiple steps; auto-heal."""
    try:
        conn = lifecycle._conn
        running = conn.execute(
            "SELECT instance_id, template_id, current_step_id, step_results, created_at, assignee "
            "FROM workflow_instances WHERE status='running'"
        ).fetchall()
        for row in running:
            inst = dict(row)
            results = json.loads(inst.get("step_results") or "{}")
            wf = workflows.get(inst["template_id"])
            _wf_steps = (wf.steps if wf else [])
            timed_out_steps = 0
            _escalated = False
            for _sid, _sr in results.items():
                if not isinstance(_sr, dict):
                    continue
                if _sr.get("escalated"):
                    _escalated = True
                _tc = _sr.get("timeout_count", 0)
                if _tc == 0:
                    continue
                _sf = next((s for s in _wf_steps if s.id == _sid), None)
                _mr = _sf.max_retries if _sf else 0
                if _tc >= _mr + 1:
                    timed_out_steps += 1
            if timed_out_steps >= 2 and not _escalated:
                bb.write("blocker",
                    f"[anomaly] {inst['instance_id']} has {timed_out_steps} steps exhausted retries",
                    src="workflow_engine")
            if timed_out_steps >= 3:
                bb.write("blocker",
                    f"[anomaly] {inst['instance_id']} exhausted {timed_out_steps} steps, healing",
                    src="workflow_engine")
                _role = inst.get("assignee", "")
                _sid = inst.get("current_step_id", "")
                if _role:
                    _ensure_role_alive_impl(lifecycle, _role)
                if _sid and _role and wf:
                    _sf = next((s for s in wf.steps if s.id == _sid), None)
                    if _sf:
                        _prompt = _sf.prompt_template
                        _send_to_role_impl(lifecycle, bb, _role, _prompt, wf_id=inst["instance_id"], step_id=_sid)
                if _sid and _sid in results:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        results[_sid]["last_heal"] = time.time()
                        conn.execute(
                            "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(results, ensure_ascii=False), inst["instance_id"]))
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
        conn.commit()
    except Exception:
        LOGGER.exception("check_anomalies failed")


def _ensure_role_alive_impl(lm, role: str) -> bool:
    """检查角色的 CCS 是否存活，不存活则拉起。"""
    import subprocess as _sp
    from pathlib import Path
    alive = _sp.run(
        ["tmux", "has-session", "-t", f"ccs-{role}"],
        capture_output=True, timeout=5,
    ).returncode == 0
    if alive:
        _sp.run(
            ["tmux", "send-keys", "-t", f"ccs-{role}", "/loop", "Enter"],
            capture_output=True, timeout=3,
        )
        return True
    _project_root = Path(__file__).resolve().parent.parent.parent.parent
    try:
        _sp.run(
            ["python3", str(_project_root / "session-launcher" / "src" / "ccs.py"),
             "start", role, "--no-attach", "--drive", "loop"],
            capture_output=True, timeout=30,
        )
        for _ in range(15):
            import time as _t
            _t.sleep(1)
            if _sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                       capture_output=True, timeout=5).returncode == 0:
                return True
    except Exception:
        LOGGER.exception("_ensure_role_alive_impl failed")
    return False


def _send_to_role_impl(lm, bb, role: str, prompt: str, wf_id: str = "", step_id: str = ""):
    """发送消息给角色 CCS。"""
    import subprocess as _sp
    from pathlib import Path
    _ensure_role_alive_impl(lm, role)
    prompt = "/goal " + prompt
    title = f"needs_implementation @{role} 工作流任务: {wf_id}/{step_id}" if wf_id else f"@{role} 工作流任务"
    bb.write("task_spec", title, evidence=prompt, src="workflow_engine")
    _ccs_cli = Path(__file__).resolve().parent.parent.parent.parent / "session-launcher" / "src" / "ccs.py"
    try:
        _sp.run(
            ["python3", str(_ccs_cli), "send", "workflow_engine", role, prompt, "--from", "workflow_engine"],
            capture_output=True, timeout=30,
        )
    except Exception:
        LOGGER.exception("_send_to_role_impl: ccs send failed")
        try:
            from routing.partner import PartnerClient
            PartnerClient("workflow_engine").force_send(role, prompt, auto_wake=True)
        except Exception as _e:
            LOGGER.warning("_send_to_role_impl fallback 失败: %s", _e)
