#!/usr/bin/env python3
"""
Workflow Engine --- 数据驱动的对话工作流执行引擎。

通过 Sister Bus 与 CCS 会话交互，执行 JSON 定义的工作流。
工作流来自 ~/.hermes/workflows/*.json，运行状态持久化到 runs/*.json，
同时扫描 SQLite 中的 production 工作流实例并自动推进。
"""

import json
import time
import uuid
import logging
import shlex as _shlex
import subprocess as _sp
from dataclasses import dataclass
from pathlib import Path
LOGGER = logging.getLogger("workflow.engine")
from typing import Any, Optional

from paths import ensure_paths
ensure_paths()
from paths import HERMES_WORKFLOWS as _WORKFLOWS_DIR
from paths import CCS_CLI as _CCS_CLI

from bus_protocol import Blackboard

_POLL_SLEEP = 5
_TIMEOUT_GRACE = 10
_ESCALATED_AFTER = 2
_FAIL_AFTER = 3
_REMINDER_INTERVAL = 120


@dataclass
class Step:
    id: str
    title: str
    target_role: str
    prompt_template: str
    exit_condition: dict
    type: str = "single"  # handoff/review/single/gate/notify
    completion_check: dict = None
    max_retries: int = 0
    condition: str = ""
    rollback_to: str = ""
    verify: str = ""
    failure_patterns: list[str] = None
    subflow_template: str = ""  # 该步骤对应的子工作流模板名


@dataclass
class WorkflowDef:
    name: str
    title: str
    description: str
    steps: list[Step]
    workflow_id: str = ""
    trigger_scene: list[str] = None
    allowed_initiators: list[str] = None
    allowed_executors: list[str] = None
    max_duration_hours: int = 24
    quality_standards: str = ""
    loop: Optional[dict] = None


@dataclass
class WorkflowRun:
    id: str
    workflow_name: str
    context: dict
    current_step: str = ""
    step_retries: dict[str, int] = None
    status: str = "running"
    created_at: float = None
    updated_at: float = None
    step_results: dict[str, Any] = None

    def __post_init__(self):
        self.step_retries = self.step_retries or {}
        self.step_results = self.step_results or {}
        self.created_at = self.created_at or time.time()
        self.updated_at = self.updated_at or time.time()


class WorkflowEngine:
    def __init__(self, workflows_dir: Path = _WORKFLOWS_DIR):
        self.workflows_dir = Path(workflows_dir).expanduser()
        self.runs_dir = self.workflows_dir / "runs"
        self._bb = Blackboard()
        self._workflows: dict[str, WorkflowDef] = {}
        self._lm = None
        self._load_workflows()

    @property
    def _lifecycle(self):
        if self._lm is None:
            from lifecycle.manager import LifecycleManager
            self._lm = LifecycleManager("workflow_engine")
        return self._lm

    def _load_workflows(self):
        if not self.workflows_dir.exists():
            return
        for f in sorted(self.workflows_dir.glob("*.json")):
            if f.parent == self.runs_dir:
                continue
            try:
                data = json.loads(f.read_text())
                steps = [Step(**{k: v for k, v in s.items() if k in Step.__dataclass_fields__}) for s in data.get("steps", [])]
                # 模板质量自动校验
                try:
                    from template_registry import _validate_meta as _vmeta
                    _vr = _vmeta(data)
                    if not _vr.passed:
                        print(f"  [wf] 警告: {f.name} 质量检查不通过:")
                        for _e in _vr.errors[:4]:
                            print(f"        - {_e}")
                except Exception:
                    LOGGER.debug("workflow meta validation failed for %s", f.name)
                self._workflows[data["name"]] = WorkflowDef(
                    name=data["name"], title=data.get("title", ""),
                    description=data.get("description", ""), steps=steps,
                    workflow_id=data.get("workflow_id", "WL-" + data["name"]),
                    trigger_scene=data.get("trigger_scene") or [],
                    allowed_initiators=data.get("allowed_initiators") or [],
                    allowed_executors=data.get("allowed_executors") or [],
                    max_duration_hours=data.get("max_duration_hours", 24),
                    quality_standards=data.get("quality_standards", ""),
                    loop=data.get("loop"),
                )
            except Exception as e:
                print(f"  [wf] 加载 {f.name} 失败: {e}")
        # 也从 SQLite 加载模板（仅默认目录时）
        if self.workflows_dir != _WORKFLOWS_DIR:
            return
        try:
            lm = self._lifecycle
            rows = lm.query(
                "SELECT template_id, name, description, steps_json FROM workflow_templates"
            )
            for row in rows:
                t = dict(row)
                tid = t["template_id"]
                if tid in self._workflows:
                    continue
                raw = json.loads(t.get("steps_json") or "[]")
                if not raw or not isinstance(raw, list) or not isinstance(raw[0], dict):
                    continue
                steps = []
                for s in raw:
                    s_clean = {}
                    for k, v in s.items():
                        if k == "step_id":
                            s_clean["id"] = v
                        elif k in Step.__dataclass_fields__:
                            s_clean[k] = v
                    if "exit_condition" not in s_clean:
                        s_clean["exit_condition"] = {}
                    if "target_role" not in s_clean:
                        continue
                    try:
                        steps.append(Step(**s_clean))
                    except Exception:
                        continue
                if not steps:
                    continue
                self._workflows[tid] = WorkflowDef(
                    name=tid, title=t.get("name", tid),
                    description=t.get("description", ""), steps=steps,
                    trigger_scene=json.loads(t.get("trigger_scene") or "[]") if isinstance(t.get("trigger_scene"), str) else (t.get("trigger_scene") or []),
                    allowed_initiators=json.loads(t.get("allowed_initiators") or "[]") if isinstance(t.get("allowed_initiators"), str) else (t.get("allowed_initiators") or []),
                    allowed_executors=json.loads(t.get("allowed_executors") or "[]") if isinstance(t.get("allowed_executors"), str) else (t.get("allowed_executors") or []),
                    max_duration_hours=t.get("max_duration_hours", 24),
                    quality_standards=t.get("quality_standards", ""),
                )
        except Exception as e:
            print(f"  [wf] SQLite 模板加载失败: {e}")

    def list_workflows(self) -> list[str]:
        return sorted(self._workflows.keys())

    def start(self, name: str, context: dict = None) -> str:
        wf = self._workflows.get(name)
        if wf:
            if not wf.steps:
                raise ValueError(f"工作流 {name} 无步骤")
            run_id = f"wf_{uuid.uuid4().hex[:12]}"
            run = WorkflowRun(
                id=run_id,
                workflow_name=name, context=context or {},
                current_step=wf.steps[0].id,
            )
            self._upsert_template(name, wf)
            try:
                self._lifecycle.start_wf(run.id, current_step_id=wf.steps[0].id,
                                          template_id=name, context=context)
            except Exception as e:
                print(f"  [wf] LM start_wf 失败: {e}", flush=True)
            self._write_step_prompt(run, wf.steps[0])
            return run.id

        raise ValueError(f"未知工作流: {name}")

    def _upsert_template(self, name: str, wf: WorkflowDef):
        try:
            steps = [
                {"step_id": s.id, "title": s.title, "type": s.type,
                 "target_role": s.target_role,
                 "prompt_template": s.prompt_template,
                 "max_retries": s.max_retries,
                 "verify": s.verify,
                 "subflow_template": s.subflow_template}
                for s in wf.steps
            ]
            self._lifecycle.upsert_template(
                name, wf.title, wf.description, steps,
                trigger_scene=wf.trigger_scene,
                allowed_initiators=wf.allowed_initiators,
                allowed_executors=wf.allowed_executors,
                max_duration_hours=wf.max_duration_hours,
                quality_standards=wf.quality_standards,
            )
        except Exception as e:
            print(f"  [wf] upsert template 失败: {e}", flush=True)

    def status(self, wid: str) -> dict:
        try:
            rows = self._lifecycle.query(
                "SELECT instance_id, template_id, status, current_step_id, step_results, created_at "
                "FROM workflow_instances WHERE instance_id=?", (wid,)
            )
            row = rows[0] if rows else None
        except Exception:
            row = None
        if not row:
            return {"error": "不存在"}
        return {
            "id": row["instance_id"], "workflow": row["template_id"],
            "status": row["status"], "current_step": row["current_step_id"],
            "retries": {}, "results": json.loads(row["step_results"] or "{}"),
        }

    def cancel(self, wid: str) -> bool:
        try:
            rows = self._lifecycle.query(
                "SELECT status FROM workflow_instances WHERE instance_id=?", (wid,)
            )
            row = rows[0] if rows else None
        except Exception:
            row = None
        if not row or row["status"] in ("completed", "cancelled", "failed"):
            return False
        self._lifecycle.close_wf(wid, status="cancelled")
        return True

    def tick(self) -> int:
        self.run_once()
        return 0

    def run_once(self):
        # 确保工作流涉及的角色存活
        for _wf_row in self._lifecycle.query("SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"):
            self._ensure_role_alive(_wf_row["assignee"])

        # JSON 工作流路径已删除
        # SQLite production 工作流 — 复用 _lifecycle 连接避免泄漏
        try:
            lm = self._lifecycle
            lm.ping()
        except Exception:
            LOGGER.debug("lifecycle ping failed during run_once")
        try:
            lm = self._lifecycle
            rows = lm.query(
                "SELECT instance_id, template_id, current_step_id, step_results, created_at, status "
                "FROM workflow_instances WHERE status IN ('pending','running')"
            )
            for row in rows:
                inst = dict(row)
                if inst.get("status") == "pending":
                    lm.execute("UPDATE workflow_instances SET status='running' WHERE instance_id=?", (inst["instance_id"],))
                inst = dict(row)
                wf_name = inst.get("template_id") or ""
                wf = self._workflows.get(wf_name)
                if not wf:
                    continue
                step_id = inst.get("current_step_id", "")
                step = next((s for s in wf.steps if s.id == step_id), None)
                if not step:
                    continue
                results = json.loads(inst.get("step_results") or "{}")
                step_status = results.get(step_id, {}).get("status", "")

                # 步骤尚未开始（首次检测到）→ 发初始提示词给角色
                if not step_status:
                    task_rows = lm.query(
                        "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                        (inst["instance_id"],)
                    )
                    task = task_rows[0] if task_rows else None
                    task_title = task["title"] if task else wf_name
                    task_desc = task["description"] if task else ""
                    prompt = step.prompt_template
                    prompt = prompt.replace("{title}", task_title)
                    prompt = prompt.replace("{description}", task_desc)
                    prompt = prompt.replace("{assignee}", step.target_role)
                    prompt = prompt.replace("{topic}", task_title)
                    self._send_to_role(step.target_role, prompt, wf_id=inst["instance_id"], step_id=step_id)
                    # 标记已通知
                    results[step_id] = {"status": "notified", "notified_at": time.time()}
                    lm.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                    )
                    step_status = "notified"  # 更新状态，让后续 exit_condition/超时检查能执行

                # 步骤已完成 → 自动推进到下一步
                # step_done_ready/completed 由角色或外部系统标记，无需额外的 completion_check
                if step_status in ("step_done_ready", "completed"):
                    self._advance_production_wf(inst["instance_id"], lm, wf_name, step_id, results)
                    continue

                # 所有 notified/running 步骤都进入 _tick() 处理超时/催办/升级
                # exit_condition 可选：有 bus_category 时检查 bus 消息匹配退出条件
                # 无 bus_category 时退化为纯超时驱动
                if step_status in ("notified", "running"):
                    _created = inst.get("created_at") or time.time()
                    run = WorkflowRun(
                        id=inst["instance_id"], workflow_name=wf_name,
                        context={}, current_step=step_id,
                        status="running", step_results=results,
                        created_at=_created, updated_at=_created,
                    )
                    self._tick(run, step)
        except Exception as _sqle:
            import logging as _lg
            _lg.getLogger("engine.sqlite_block").warning("生产工作流扫描异常: %s", _sqle)


        self._scan_tasks()

    def _advance_production_wf(self, wf_id: str, lm, wf_name: str, step_id: str,
                                results: dict = None):
        """推进生产工作流到下一步，并通知目标角色。"""
        try:
            conn = lm._conn  # ponytail: 事务内批量操作，下一轮重构时统一用 execute_raw
            wf = self._workflows.get(wf_name)
            if not wf:
                return
            steps = wf.steps
            idx = next((i for i, s in enumerate(steps) if s.id == step_id), -1)
            if idx < 0:
                return
            # 最后一步 → 检查所有子工作流是否全部完成
            if idx + 1 >= len(steps):
                _all_subs_done = True
                for _sid, _sdata in (results or {}).items():
                    _sf = _sdata.get("subflow_id", "")
                    if _sf:
                        _st = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                        if not _st or _st["status"] != "completed":
                            _all_subs_done = False
                            break
                if _all_subs_done:
                    conn.execute("UPDATE workflow_instances SET status='completed' WHERE instance_id=?", (wf_id,))
                    conn.commit()
                    self._bb.write("workflow",
                        f"[workflow] {wf_name} 完成: {len(steps)} 步",
                        src="workflow_engine")
                return
            # 推进到下一步
            next_step = steps[idx + 1]
            conn.execute("UPDATE workflow_instances SET current_step_id=? WHERE instance_id=? AND current_step_id=?",
                        (next_step.id, wf_id, step_id))
            new_results = dict(results or {})
            new_results[next_step.id] = {"status": "pending", "assigned_at": time.time()}
            conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(new_results, ensure_ascii=False), wf_id))
            conn.commit()
            # 通知目标角色
            task = conn.execute(
                "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                (wf_id,)
            ).fetchone()
            task_title = task["title"] if task else wf_name
            task_desc = task["description"] if task else ""
            prompt = next_step.prompt_template
            prompt = prompt.replace("{title}", task_title)
            prompt = prompt.replace("{description}", task_desc)
            prompt = prompt.replace("{assignee}", next_step.target_role)
            prompt = prompt.replace("{topic}", task_title)
            self._send_to_role(next_step.target_role, prompt,
                               wf_id=wf_id, step_id=next_step.id)
            # 如果步骤有子工作流模板 → 自动创建子工作流
            if next_step.subflow_template:
                try:
                    _sub_wf = self._workflows.get(next_step.subflow_template)
                    if _sub_wf and _sub_wf.steps and _sub_wf.steps[0].target_role == next_step.target_role:
                        _prev_done = ""
                        if idx >= 0:
                            _prev_done = results.get(step_id, {}).get("completed_by", "")
                        if not _prev_done:
                            _prev_wf = conn.execute("SELECT assigner FROM workflow_instances WHERE instance_id=?", (wf_id,)).fetchone()
                            _prev_done = _prev_wf["assigner"] if _prev_wf else "pm"
                        _task_id = f"task_sub_{uuid.uuid4().hex[:8]}"
                        _wf_id = f"wf_sub_{uuid.uuid4().hex[:12]}"
                        conn.execute("INSERT OR IGNORE INTO tasks (task_id,title,description,assigner,assignee,status,created_at,updated_at,template_id) VALUES (?,?,?,?,?,'in_progress',?,?,?)",
                            (_task_id, f"{wf_name}/{next_step.id}: {next_step.title}", task_desc, _prev_done, next_step.target_role, time.time(), time.time(), next_step.subflow_template))
                        conn.execute("INSERT OR IGNORE INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,status,current_step_id,step_results,created_at) VALUES (?,?,?,?,?,'pending','s1',?,?)",
                            (_wf_id, next_step.subflow_template, _task_id, _prev_done, next_step.target_role, json.dumps({}), time.time()))
                        conn.commit()
                        new_results[next_step.id]["subflow_id"] = _wf_id
                        conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                            (json.dumps(new_results, ensure_ascii=False), wf_id))
                        conn.commit()
                except Exception as _e:
                    LOGGER.error("subflow creation failed for %s: %s", wf_id, _e)
        except Exception as e:
            # 异常时通知目标角色排查修复
            try:
                _target_role = wf_name if 'wf_name' in dir() else step_id
            except Exception:
                _target_role = ""
            import logging as _lg
            _lg.getLogger("engine.advance").warning("推进生产工作流失败 %s: %s", wf_id, e)
    
    def _tick(self, run: WorkflowRun, step: Step):
        # ponytail: skip completed/cancelled runs — prevents stale step-escalation noise
        if run.status in ("completed", "cancelled"):
            return
        ec = step.exit_condition
        cat = ec.get("bus_category", "")
        src_filter = ec.get("source_contains", "")
        text_filter = ec.get("text_contains", "")
        timeout = ec.get("timeout_minutes", 30) * 60
        max_retries = step.max_retries

        last_ts = run.step_results.get(step.id, {}).get("poll_since", run.created_at)

        if self._check_exit(cat, src_filter, text_filter, created_after=last_ts):
            if step.verify:
                ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
                vcmd = step.verify
                for k, val in ctx.items():
                    vcmd = vcmd.replace(f"{{{k}}}", str(val))
                # ponytail: verify 使用 shlex.split 防注入；若需 pipes/redirects 改为白名单模式
                ver = _sp.run(_shlex.split(vcmd), capture_output=True, timeout=30)
                if ver.returncode != 0:
                    err = ver.stderr.decode()[:200] or "verify failed"
                    self._bb.write("blocker",
                        f"[workflow] {run.workflow_name} {step.id} 验证不通过: {err}",
                        src="workflow_engine")
                    return

            try:
                self._lifecycle.complete_step(run.id, step.id)
            except Exception as e:
                print(f"  [wf] LM complete_step 失败: {e}", flush=True)
                # ponytail: 角色已自行推进步骤 → 推进 poll_since 避免下一轮重复匹配同一条 exit 消息
                run.step_results[step.id] = {
                    **run.step_results.get(step.id, {}),
                    "poll_since": time.time()
                }
                self._sync_step_results(run.id, run.step_results)

            # lifecycle.manager 已写入 step_results，此处不再重复写入
            return

        run.step_results[step.id] = {
            **run.step_results.get(step.id, {}),
            "poll_since": time.time()
        }
        self._sync_step_results(run.id, run.step_results)

        elapsed = time.time() - (run.step_results.get(step.id, {}).get("ts", run.created_at))
        if elapsed < timeout:
            last_reminder = run.step_results.get(step.id, {}).get("last_reminder", 0)
            if last_reminder > 0 and time.time() - last_reminder > _REMINDER_INTERVAL:
                remaining = int((timeout - elapsed) / 60)
                hint = f"⏰ {step.id}（{step.title}）运行中，剩余约 {remaining} 分钟"
                self._send_to_role(step.target_role, hint)
                run.step_results[step.id]["last_reminder"] = time.time()
                self._sync_step_results(run.id, run.step_results)
            return

        # 从 step_results 读持久化的超时计数
        timeout_count = run.step_results.get(step.id, {}).get("timeout_count", 0) + 1
        run.step_results[step.id] = {
            **run.step_results.get(step.id, {}),
            "timeout_count": timeout_count,
        }

        # 每次超时重发提示词给目标角色
        hint = f"⏱️ {step.id} 已超时 ({timeout_count}回)，请尽快完成"
        self._write_step_prompt(run, step, extra_prompt=hint)

        # 每 3 次通知 coordinator
        if timeout_count % 3 == 0:
            warning = f"[workflow] {run.workflow_name} {step.id} 持续超时 ({timeout_count}×)，业务侧重点关注"
            self._bb.write("blocker", warning, src="workflow_engine")
            try:
                self._lifecycle.escalate_step(run.id, step.id, reason=warning)
            except Exception as _e:
                    LOGGER.exception("silenced exception")
            self._send_to_role("coordinator", warning)

        # 同步到 SQLite
        self._sync_step_results(run.id, run.step_results)
        try:
            lm = self._lifecycle
            lm.execute(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(run.step_results, ensure_ascii=False), run.id)
            )
        except Exception:
            LOGGER.debug("step_results SQLite sync (secondary) failed for %s", run.id)

    def _sync_step_results(self, wf_id: str, step_results: dict):
        try:
            lm = self._lifecycle
            lm.begin()
            lm.execute_raw(
                "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps(step_results, ensure_ascii=False), wf_id)
            )
            lm.commit()
        except Exception as _e:
            LOGGER.exception("_sync_step_results 写入失败")
            try:
                lm.rollback()
            except Exception as _e2:
                LOGGER.exception("_sync_step_results rollback 也失败")

    def _check_session_alive(self, role: str, since: float) -> bool:
        try:
            session_dir = Path.home() / ".claude" / "projects" \
                / f"-home-administrator-ccs-workspaces/{role}"
            if not session_dir.exists():
                return False
            latest = max(session_dir.glob("*.jsonl"),
                         key=lambda f: f.stat().st_mtime) if list(session_dir.glob("*.jsonl")) else None
            if not latest:
                return False
            age = time.time() - latest.stat().st_mtime
            return age < 300
        except Exception:
            return False

    def _check_exit(self, cat: str, src_filter: str, text_filter: str, created_after: float = None) -> bool:
        facts = self._bb.read(cat=cat, limit=50) if cat else self._bb.read(limit=50)
        for f in facts:
            if src_filter and f.src != src_filter:
                continue
            if text_filter and text_filter not in f.t:
                continue
            if created_after and f.ts < created_after:
                continue
            return True
        return False

    def _eval_cond(self, expr: str, run: WorkflowRun) -> bool:
        try:
            import re
            m = re.search(r"s(\d+)\.status\s*==\s*'([^']+)'", expr)
            if m:
                step_num = int(m.group(1))
                expected_status = m.group(2)
                step_key = f"s{step_num}"
                actual = run.step_results.get(step_key, {}).get("status", "")
                return actual == expected_status
            return False
        except Exception:
            return False

    def _ensure_role_alive(self, role: str) -> bool:
        """检查角色的 CCS 是否存活（tmux 会话），不存活则拉起。"""
        alive = _sp.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=5,
        ).returncode == 0
        if alive:
            return True

        # 拉起 CCS（ondemand 模式，处理完消息自动退出）
        try:
            _sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "ondemand"],
                capture_output=True, timeout=30,
            )
            for _ in range(15):
                time.sleep(1)
                if _sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=5).returncode == 0:
                    return True
        except Exception:
            LOGGER.debug("CCS role alive check failed for %s", role)
        return False

    def _send_to_role(self, role: str, prompt: str,
                       wf_id: str = "", step_id: str = ""):
        """确保角色 CCS 存活，写完整 task_spec 到 bus，再 ccs send 推送全文。"""
        self._ensure_role_alive(role)
        prompt = "/goal " + prompt
        # 工作习惯提示：仅独立消息显示（非工作流步骤）
        if not wf_id:
            prompt = "/goal\n\n## 工作习惯\n用 `wf create <title> --assignee <role>` 创建工作流（自动匹配模板），或 `wf suggest <title>` 预览推荐。\n\n" + prompt.removeprefix("/goal ")
        # task_spec 携带完整提示词（角色 loop 读 bus 拿到全部上下文）
        title = f"needs_implementation @{role} 工作流任务: {wf_id}/{step_id}" if wf_id else f"@{role} 工作流任务"
        self._bb.write("task_spec", title, evidence=prompt, src="workflow_engine")
        # ccs send 推送全文（参数顺序：ccs.py send <target_role> <message> --from <source>）
        try:
            _sp.run(
                ["python3", str(_CCS_CLI), "send", role, prompt,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=30,
            )
        except Exception:
            try:
                ccs_cli = Path.home() / "session-launcher" / "src" / "ccs.py"
                subprocess.run(
                    ["python3", str(ccs_cli), "send", role, prompt[:2000]],
                    capture_output=True, timeout=10,
                )
            except Exception as _e:
                    LOGGER.exception("silenced exception")

    def _write_step_prompt(self, run: WorkflowRun, step: Step, extra_prompt: str = ""):
        ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
        if "{workspace_summary}" in step.prompt_template:
            ws_dir = Path.home() / ".hermes" / "workspace" / ctx.get("project_name", "")
            ctx["workspace_summary"] = self._collect_workspace_summary(ws_dir)
        # 从 SQLite 补 task 上下文
        if "{title}" in step.prompt_template or "{description}" in step.prompt_template:
            try:
                lm = self._lifecycle
                task_rows = lm.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (run.id,)
                )
                task = task_rows[0] if task_rows else None
                if task:
                    ctx["title"] = task["title"]
                    ctx["description"] = task["description"]
                    ctx["assignee"] = step.target_role
            except Exception as _e:
                    LOGGER.exception("silenced exception")
        prompt = step.prompt_template + "\n" + extra_prompt if extra_prompt else step.prompt_template
        for k, v in ctx.items():
            prompt = prompt.replace(f"{{{k}}}", str(v))
        self._send_to_role(step.target_role, prompt)

    def _collect_workspace_summary(self, ws_dir: Path) -> str:
        if not ws_dir.exists():
            return "workspace 不存在"
        parts = []
        for fname in ["PRD.md", "DESIGN.md", "TASKS.json", "INTAKE.md"]:
            fpath = ws_dir / fname
            if fpath.exists():
                content = fpath.read_text()
                parts.append(f"[{fname}] {content[:200]}...")
        return "\n".join(parts) if parts else "workspace 为空"

    def _check_anomalies(self):
        """Detect workflows stuck across multiple steps; auto-heal."""
        try:
            conn = self._lifecycle._conn  # ponytail: 同上一轮重构时统一
            running = conn.execute(
                "SELECT instance_id, template_id, current_step_id, step_results, created_at, assignee "
                "FROM workflow_instances WHERE status='running'"
            ).fetchall()
            for row in running:
                inst = dict(row)
                results = json.loads(inst.get("step_results") or "{}")
                wf = self._workflows.get(inst["template_id"])
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
                    self._bb.write("blocker",
                        f"[anomaly] {inst['instance_id']} has {timed_out_steps} steps exhausted retries",
                        src="workflow_engine")
                if timed_out_steps >= 3:
                    self._bb.write("blocker",
                        f"[anomaly] {inst['instance_id']} exhausted {timed_out_steps} steps, healing",
                        src="workflow_engine")
                    _role = inst.get("assignee", "")
                    _sid = inst.get("current_step_id", "")
                    if _role:
                        self._ensure_role_alive(_role)
                    if _sid and _role and wf:
                        _sf = next((s for s in wf.steps if s.id == _sid), None)
                        if _sf:
                            _prompt = _sf.prompt_template
                            self._send_to_role(_role, _prompt, wf_id=inst["instance_id"], step_id=_sid)
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
        except Exception as _e:
            LOGGER.error("heal_stalled failed for %s: %s", inst.get("instance_id",""), _e)


    def _scan_tasks(self):
        try:
            lm = self._lifecycle
            lm.ping()
        except Exception:
            LOGGER.debug("lifecycle ping failed during _scan_tasks")
        try:
            lm = self._lifecycle
            conn = lm._conn  # ponytail: 事务内批量操作，下一轮重构时统一用 execute_raw
            rows = conn.execute(
                "SELECT DISTINCT t.task_id, t.status FROM tasks t "
                "JOIN workflow_instances wi ON t.task_id = wi.task_id "
                "WHERE t.status NOT IN ('completed', 'failed', 'cancelled')"
            ).fetchall()
            for row in rows:
                task_id = row["task_id"]
                inst_rows = conn.execute(
                    "SELECT status FROM workflow_instances WHERE task_id=?",
                    (task_id,)
                ).fetchall()
                statuses = [dict(r)["status"] for r in inst_rows]
                if not statuses:
                    continue
                if all(s in ("completed", "failed", "cancelled") for s in statuses):
                    ts = "completed" if all(s == "completed" for s in statuses) else "failed"
                    conn.execute(
                        "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
                        (ts, time.time(), task_id))
                    conn.commit()
            # 子工作流完成 → 推进父工作流
            for _sub_row in conn.execute(
                "SELECT wi.instance_id, wi.step_results FROM workflow_instances wi "
                "WHERE wi.status='running' AND wi.template_id IN (SELECT template_id FROM workflow_templates)"
            ).fetchall():
                _sr = json.loads(_sub_row["step_results"] or "{}")
                for _step_id, _sdata in _sr.items():
                    _sf = _sdata.get("subflow_id", "")
                    if _sf:
                        _sub_status = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                        if _sub_status and _sub_status["status"] == "completed":
                            _sdata["status"] = "completed"
                            _sdata["completed_at"] = time.time()
                            conn.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                                (json.dumps(_sr, ensure_ascii=False), _sub_row["instance_id"]))
                            conn.commit()
                            # 推进父工作流
                            _tmpl = conn.execute("SELECT template_id FROM workflow_instances WHERE instance_id=?", (_sub_row["instance_id"],)).fetchone()
                            if _tmpl:
                                _wf = self._workflows.get(_tmpl["template_id"])
                                if _wf:
                                    self._advance_production_wf(_sub_row["instance_id"], lm, _tmpl["template_id"], _step_id, _sr)

            conn.commit()
            self._check_anomalies()
        except Exception as _e:
            LOGGER.exception("_scan_tasks 异常")


def main():
    import argparse, sys, os, signal
    p = argparse.ArgumentParser(description="Workflow Engine")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("start")
    s.add_argument("name")
    s.add_argument("--context", default="{}")

    sub.add_parser("list")

    s2 = sub.add_parser("status")
    s2.add_argument("workflow_id")

    s3 = sub.add_parser("cancel")
    s3.add_argument("workflow_id")

    sub.add_parser("tick")

    daemon = sub.add_parser("daemon")
    daemon.add_argument("--interval", type=int, default=10, help="轮询间隔秒数")

    args = p.parse_args()
    eng = WorkflowEngine()

    _PID_FILE = Path("/tmp") / "workflow-engine-daemon.pid"

    def _daemon_loop(interval: int):
        if _PID_FILE.exists():
            try:
                old = int(_PID_FILE.read_text())
                os.kill(old, 0)
                print(f"Daemon PID={old} 已在运行")
                sys.exit(1)
            except (OSError, ValueError):
                LOGGER.debug("PID file stale or missing")
        _PID_FILE.write_text(str(os.getpid()))
        print(f"Workflow Engine daemon PID={os.getpid()}, interval={interval}s")

        shutdown = False
        def _handler(s, f):
            nonlocal shutdown; shutdown = True
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

        try:
            while not shutdown:
                eng.run_once()
                time.sleep(interval)
        finally:
            _PID_FILE.unlink(missing_ok=True)
            print("Daemon stopped")

    if args.cmd == "list":
        for wf in eng.list_workflows():
            print(wf)
    elif args.cmd == "start":
        ctx = json.loads(args.context)
        print(eng.start(args.name, ctx))
    elif args.cmd == "status":
        print(json.dumps(eng.status(args.workflow_id), ensure_ascii=False))
    elif args.cmd == "cancel":
        ok = eng.cancel(args.workflow_id)
        print("OK" if ok else "NOT FOUND")
    elif args.cmd == "tick":
        eng.run_once()
        print("Tick done")
    elif args.cmd == "daemon":
        _daemon_loop(args.interval)


if __name__ == "__main__":
    main()
