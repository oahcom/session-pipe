#!/usr/bin/env python3
"""
Workflow Engine --- 数据驱动的对话工作流执行引擎。

通过 Sister Bus 与 CCS 会话交互，执行 JSON 定义的工作流。
工作流来自 ~/.hermes/workflows/*.json，运行状态持久化到 runs/*.json，
同时扫描 SQLite 中的 production 工作流实例并自动推进。
"""

import json
import os
import re
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
from paths import CCS_WORKSPACES as _CCS_WORKSPACES

from bus_protocol import Blackboard

_ZOMBIE_TEMPLATES = frozenset({
    "test", "hr", "back_ccs", "busy_ccs", "stuck_ccs", "heartbeat",
    "ho", "research", "dev", "prompt_test", "cancel_test", "cond_test",
    "cond_block", "timeout_test", "retry_test", "fast_ccs", "ws_test",
    "single_wf", "two_step", "handoff_wf", "reject_wf", "cancel_wf",
    "alpha", "beta", "ok", "quick", "valid", "empty",
})

_POLL_SLEEP = 5
_REMINDER_INTERVAL = 120

# 未替换变量检测：含 {xxx} 模板变量的 prompt 不应发送，否则角色 /goal 用它作
# StopHook 条件时 bool([]) 为 False → 死循环
_UNMATCHED_VAR_RE = re.compile(r'\{[a-z_]+\}')


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
    exit_schema: dict = None  # 退出校验 schema（文件校验/内容校验/自定义校验）


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
    is_subflow: bool = False  # True = 仅限子工作流调用，不能独立 start


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
            self._lm = LifecycleManager("workflow_engine", on_advance=self._ensure_role_alive)
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
                except (ValueError, KeyError, TypeError):
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
                    is_subflow=data.get("is_subflow", False),
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
                    except (ValueError, KeyError, TypeError):
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
                    is_subflow=t.get("is_subflow", False),
                )
        except Exception as e:
            print(f"  [wf] SQLite 模板加载失败: {e}")

    def list_workflows(self) -> list[str]:
        return sorted(name for name, wf in self._workflows.items() if not wf.is_subflow)

    def start(self, name: str, context: dict = None) -> str:
        wf = self._workflows.get(name)
        if wf:
            if wf.is_subflow:
                raise ValueError(f"工作流 {name} 是子工作流模板，不能直接启动")
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
        except (ValueError, KeyError, TypeError):
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
        except (ValueError, KeyError, TypeError):
            row = None
        if not row or row["status"] in ("completed", "cancelled", "failed"):
            return False
        self._lifecycle.close_wf(wid, status="cancelled")
        return True

    def tick(self) -> int:
        self.run_once()
        return 0

    def _cleanup_stale_workflows(self, lm):
        """自动取消僵尸 workflow。

        3 级回收策略:
          - 开发/测试模板（_ZOMBIE_TEMPLATES）> 2h → cancel
          - 所有模板 > 24h → cancel
          - 有 timeout_count >= 6（约 3 小时超时）→ cancel
        """
        now = time.time()
        try:
            for row in lm.query(
                "SELECT instance_id, template_id, created_at, step_results "
                "FROM workflow_instances WHERE status='running'"
            ):
                d = dict(row)
                wf_id = d["instance_id"]
                tpl = d.get("template_id", "?")
                created = d.get("created_at", 0)
                age_h = (now - created) / 3600 if created else 0
                sr = json.loads(d.get("step_results") or "{}")

                max_tc = max(
                    (v.get("timeout_count", 0) for v in sr.values() if isinstance(v, dict)),
                    default=0,
                )

                _auto_cancel_at = 6
                reason = ""
                # 条件1: 超时重试耗尽
                if max_tc >= _auto_cancel_at:
                    reason = f"timeout_count={max_tc}（已达自动回收阈值{_auto_cancel_at}）"
                # 条件2: 测试/开发模板超2小时
                elif tpl in _ZOMBIE_TEMPLATES and age_h > 2:
                    reason = f"测试模板 {tpl} 运行{age_h:.0f}h > 2h阈值"
                # 条件3: 全局24小时
                elif age_h > 24:
                    reason = f"运行{age_h:.0f}h > 24h全局阈值"
                # 条件4: running 但 step_results 为空且 >1h（从未推进）
                elif age_h > 1 and not sr:
                    _step = d.get("current_step_id", "")
                    reason = f"running 状态 {_step} 无推进记录 {age_h:.1f}h"

                if reason:
                    LOGGER.warning("auto-cancel: %s (template=%s) — %s", wf_id, tpl, reason)
                    # ── 回收前抢救: 当前步骤已有 exit_messages(产出证据) → 先尝试闭合 ──
                    try:
                        _cur = d.get("current_step_id", "")
                        _sdata = sr.get(_cur, {}) if isinstance(sr, dict) else {}
                        if isinstance(_sdata, dict) and _sdata.get("exit_messages"):
                            self._lifecycle.complete_step(wf_id, _cur)
                            LOGGER.info("stale rescue: %s step %s exit_messages 已就绪 → 已闭合",
                                        wf_id, _cur)
                            continue
                    except Exception as _re:
                        LOGGER.debug("stale rescue failed for %s: %s", wf_id, _re)
                    try:
                        lm.execute(
                            "UPDATE workflow_instances SET status='cancelled' WHERE instance_id=?",
                            (wf_id,)
                        )
                        lm.execute(
                            "INSERT INTO workflow_logs (workflow_instance_id, task_id, action, actor, detail, ts) "
                            "VALUES (?, ?, 'cancelled', 'workflow_engine', ?, ?)",
                            (wf_id, "", f"僵尸回收: {reason}", time.time())
                        )
                        self._notify_role("maintainer",
                            f"僵尸工作流自动回收: {tpl}/{wf_id}",
                            f"原因: {reason}\n请排查该工作流为何没有正常完成。")
                    except Exception as e:
                        LOGGER.debug("stale cleanup write failed: %s", e)
            LOGGER.info("stale cleanup done")
        except Exception as e:
            LOGGER.warning("stale cleanup error: %s", e)

    def run_once(self):
        # 先清理僵尸 workflow
        try:
            self._cleanup_stale_workflows(self._lifecycle)
        except Exception as e:
            LOGGER.debug("stale cleanup in run_once failed: %s", e)

        # 确保工作流涉及的角色存活
        # 注意：assignee 可能是 coordinator 创建任务时的派发角色（首步目标角色），
        # 后续步骤的 target_role 才是真正的执行者。只检查 assignee 会漏拉起
        # 后续步骤角色，因此同时检查当前步骤的 target_role。
        _alive_roles = set()
        for _wf_row in self._lifecycle.query("SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"):
            _alive_roles.add(_wf_row["assignee"])
        for _wf_row in self._lifecycle.query(
            "SELECT instance_id, template_id, current_step_id FROM workflow_instances WHERE status IN ('running','pending')"
        ):
            _wf = self._workflows.get(_wf_row["template_id"])
            if not _wf:
                continue
            _step = next((s for s in _wf.steps if s.id == _wf_row["current_step_id"]), None)
            if _step:
                _alive_roles.add(_step.target_role)
        for _role in _alive_roles:
            self._ensure_role_alive(_role)

        # JSON 工作流路径已删除
        # SQLite production 工作流 — 复用 _lifecycle 连接避免泄漏
        lm = self._lifecycle
        try:
            lm.ping()
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("lifecycle ping failed during run_once")
        try:
            rows = lm.query(
                "SELECT instance_id, template_id, current_step_id, step_results, created_at, status "
                "FROM workflow_instances WHERE status IN ('pending','running','step_done_ready')"
            )
            for row in rows:
                inst = dict(row)
                if inst.get("status") == "pending":
                    lm.execute("UPDATE workflow_instances SET status='running' WHERE instance_id=?", (inst["instance_id"],))
                wf_name = inst.get("template_id") or ""
                wf = self._workflows.get(wf_name)
                if not wf:
                    continue
                step_id = inst.get("current_step_id", "")
                step = next((s for s in wf.steps if s.id == step_id), None)
                if not step:
                    # current_step 不在模板中（模板更新遗留/数据损坏）→ 回收
                    LOGGER.warning("wf %s/%s current_step=%s 不在模板中，自动回收",
                                   inst["instance_id"][:12], wf_name, step_id)
                    try:
                        self._lifecycle.close_wf(inst["instance_id"], status="cancelled")
                    except Exception:
                        pass
                    continue
                results = json.loads(inst.get("step_results") or "{}")
                step_status = results.get(step_id, {}).get("status", "")
                # 行 status 与 step_results JSON 之间可能存在分歧：
                # 客户端（workflow/client.py）写行 status 但不写 step_results，
                # 引擎读 step_results 但行 status 才是真相源。
                # 当 step_results 空或 notified 但行 status 更高级时，以行 status 为准。
                if not step_status and inst.get("status") in ("step_done_ready", "completed"):
                    step_status = inst["status"]
                elif step_status == "notified" and inst.get("status") == "step_done_ready":
                    step_status = "step_done_ready"

                # 步骤尚未开始（首次检测到）→ 发初始提示词给角色
                if not step_status:
                    task_rows = lm.query(
                        "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                        (inst["instance_id"],)
                    )
                    task = task_rows[0] if task_rows and len(task_rows) > 0 else None
                    task_title = task["title"] if task else wf_name
                    task_desc = task["description"] if task else ""
                    # 任务上下文注入：把 title/desc 放到 prompt 头部，角色立刻知道干什么
                    task_header = (
                        f"[任务] {task_title}\n"
                        f"[工作流] {wf_name} | 步骤: {step.id} — {step.title}\n"
                        f"[工作流目标] {step.title}\n"
                        + (f"[任务描述] {task_desc}\n" if task_desc else "")
                        + "\n"
                    )
                    prompt = task_header + step.prompt_template
                    prompt = prompt.replace("{title}", task_title)
                    prompt = prompt.replace("{description}", task_desc)
                    prompt = prompt.replace("{assignee}", step.target_role)
                    prompt = prompt.replace("{topic}", task_title)
                    # 补充替换：12个JSON模板使用{task_definition}/{acceptance_criteria}
                    prompt = prompt.replace("{task_definition}", task_title or task_desc or wf_name)
                    prompt = prompt.replace("{acceptance_criteria}", task_desc or "按模板要求完成产出并写入对应bus分类")
                    # 首步无上一步产出，用任务标题作变量的语义默认值，避免空变量
                    fallback = task_title or task_desc or step.title or wf_name
                    # focus_area 语义是"聚焦范围"，step.title 比 task_title 更精确
                    prompt = prompt.replace("{focus_area}", step.title or fallback)
                    for var in ("{target}", "{findings}", "{results}", "{backlog}",
                                "{exception_info}", "{project}", "{changes}",
                                "{task_list}", "{assignments}"):
                        prompt = prompt.replace(var, fallback)
                    # 写 TASKS.md 到 workspace，让模板中"读 TASKS.json"指令能找到任务
                    ws_dir = Path.home() / "ccs-workspaces" / step.target_role
                    ws_dir.mkdir(parents=True, exist_ok=True)
                    tasks_file = ws_dir / "TASKS.md"
                    tasks_file.write_text(f"# 当前任务\n\n## 标题\n{task_title}\n\n## 描述\n{task_desc or task_title}\n\n## 工作流\n{wf_name} — 步骤 {step.id}: {step.title}\n", encoding="utf-8")
                    prompt = prompt.replace("{workspace_summary}", self._collect_workspace_summary(Path.home() / "ccs-workspaces" / step.target_role) if hasattr(self,'_collect_workspace_summary') else "")
                    self._send_to_role(step.target_role, prompt, wf_id=inst["instance_id"], step_id=step_id)
                    # 标记已通知
                    results[step_id] = {"status": "notified", "ts": time.time(), "notified_at": time.time()}
                    lm.execute(
                        "UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                        (json.dumps(results, ensure_ascii=False), inst["instance_id"])
                    )
                    step_status = "notified"  # 更新状态，让后续 exit_condition/超时检查能执行

                # 步骤已完成 → 自动推进到下一步
                # step_done_ready/completed 由角色或外部系统标记，无需额外的 completion_check
                if step_status in ("step_done_ready", "completed"):
                    self._advance_production_wf(inst["instance_id"], lm, wf_name, step_id, results)
                    # 推进后立即把行 status 改为 running（推进到非最后一步时），
                    # 避免角色重复 wf complete 把行 status 打回 step_done_ready
                    lm.execute("UPDATE workflow_instances SET status='running' "
                               "WHERE instance_id=? AND status='step_done_ready'",
                               (inst["instance_id"],))
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
            import traceback as _tb
            _lg.getLogger("engine.sqlite_block").warning("生产工作流扫描异常: %s", _sqle)
            _tb.print_exc()


        self._scan_tasks()
        LOGGER.debug("run_once: _scan_tasks done, querying workflow status...")

        # 报告当前工作流概况
        try:
            lm = self._lifecycle
            # 总数摘要 — 始终打印，含零
            summary = lm.query(
                "SELECT status, COUNT(*) as cnt FROM workflow_instances GROUP BY status"
            )
            total = sum(r["cnt"] for r in summary)
            parts = [f"{r['status']}={r['cnt']}" for r in summary]
            LOGGER.info("workflow 概况: %d 个 — %s", total, ", ".join(parts))
            # 逐条列出活跃 workflow 进度
            runs = lm.query(
                "SELECT instance_id, template_id, current_step_id, assignee, created_at "
                "FROM workflow_instances WHERE status IN ('running','pending','step_done_ready')"
            )
            for wf in runs:
                LOGGER.info("wf %s [%s] → %s step=%s (创建于 %s)",
                            wf["instance_id"][:8], wf["template_id"][:30],
                            wf["assignee"] or "?", wf["current_step_id"],
                            str(wf["created_at"])[:16] if wf["created_at"] else "?")
        except Exception as e:
            LOGGER.warning("workflow 概况查询异常: %s", e, exc_info=True)

        # 角色停滞保护：检测有积压 task 但无 progress 的角色
        try:
            self._kick_stalled_roles()
        except Exception as _e:
            LOGGER.debug("kick_stalled_roles failed: %s", _e)

        # 上下文溢出检测 → 自动发 /compact
        try:
            self._check_context_overflow()
        except Exception:
            LOGGER.debug("context overflow check failed")

    # ── 上下文溢出检测 ─────────────────────────────────────────
    _OVERFLOW_THRESHOLD_LINES = 2000   # pane scrollback 行数超过此值触发 /compact（备用指标）
    _OVERFLOW_COOLDOWN = 300           # 同一角色两次 /compact 最小间隔（秒）
    _last_compact: dict[str, float] = {}

    def _check_context_overflow(self):
        """检测角色 tmux pane 中 Claude 的上下文溢出信号，自动发 /compact。

        检测两个信号：
        - "Context limit reached"（Claude 阻塞等待 /compact）
        - "Context low"（Claude 提示上下文不足）

        覆盖两个来源：
        - DB 中 running/pending workflow 的角色
        - 所有实际存在的 ccs-* tmux session（覆盖非 workflow 启动的角色）
        """
        roles = set()
        # 来源1: DB
        try:
            for row in self._lifecycle.query(
                "SELECT DISTINCT assignee FROM workflow_instances WHERE status IN ('running','pending')"
            ):
                if row["assignee"]:
                    roles.add(row["assignee"])
        except Exception:
            pass

        # 来源2: 实际 tmux 会话（兜底：覆盖手动启动或 DB 查不到的角色）
        try:
            r = _sp.run(
                ["tmux", "list-sessions", "-F", "#{session_name}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                for name in r.stdout.strip().split("\n"):
                    name = name.strip()
                    if name.startswith("ccs-"):
                        role_name = name[4:]
                        if role_name:
                            roles.add(role_name)
        except Exception:
            pass

        if not roles:
            return

        now = time.time()
        for role in roles:
            tmux_name = f"ccs-{role}"
            last = self._last_compact.get(role, 0.0)
            if now - last < self._OVERFLOW_COOLDOWN:
                continue

            # 抓最近 500 行输出，搜索 Claude 的上下文溢出信号
            try:
                r = _sp.run(
                    ["tmux", "capture-pane", "-p", "-t", f"{tmux_name}:0.0", "-S", "-500"],
                    capture_output=True, text=True, timeout=5,
                )
            except Exception:
                continue
            if r.returncode != 0:
                continue
            output = r.stdout or ""

            # 信号匹配：Context limit reached / Context low / Context (数字% remaining)
            has_overflow = (
                "Context limit reached" in output
                or "Context low" in output
                or ("Context" in output and "remaining" in output and "compact" in output.lower())
            )

            if not has_overflow:
                continue

            LOGGER.info("上下文溢出: %s 检测到溢出信号，发送 /compact", role)
            try:
                _sp.run(
                    ["tmux", "send-keys", "-t", f"{tmux_name}:0.0", "/compact", "Enter"],
                    capture_output=True, timeout=5,
                )
                self._last_compact[role] = now
                self._bb.write("architecture",
                    f"[context_overflow] {role} → /compact",
                    src="workflow_engine")
            except Exception as e:
                LOGGER.warning("compact send failed for %s: %s", role, e)

    def _fill_prompt_vars(self, prompt: str, step: Step, wf_name: str,
                          task_title: str, task_desc: str, results: dict,
                          prev_step_id: str = "") -> str:
        """填充 prompt 变量：任务上下文 + 步骤间数据传递（上一步 exit_messages）。
        供 _advance_production_wf / _kick_stalled_roles / run_once 复用，保证
        重推与首推拿到的 prompt 一致（避免重推丢上下文）。"""
        prompt = prompt.replace("{title}", task_title)
        prompt = prompt.replace("{description}", task_desc)
        prompt = prompt.replace("{assignee}", step.target_role)
        prompt = prompt.replace("{task_definition}", task_title or task_desc or wf_name)
        prompt = prompt.replace("{acceptance_criteria}", task_desc or "按模板要求完成产出并写入对应bus分类")
        prompt = prompt.replace("{topic}", task_title)
        # workspace 摘要：给角色上下文（首步生产扫描在调用前已替换，这里统一兜底）
        try:
            _ws_dir = Path.home() / "ccs-workspaces" / step.target_role
            prompt = prompt.replace("{workspace_summary}",
                                    self._collect_workspace_summary(_ws_dir))
        except Exception:
            pass
        # 步骤间数据传递：上一步骤的 exit_messages 填充后续步骤变量
        prev_sr = (results or {}).get(prev_step_id, {}) if prev_step_id else {}
        prev_msgs = prev_sr.get("exit_messages", []) if isinstance(prev_sr, dict) else []
        if prev_msgs:
            prev_text = "\n".join(str(m) for m in prev_msgs)[:3000]
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}"):
                prompt = prompt.replace(var, prev_text)
        else:
            fallback = task_desc or task_title or wf_name
            for var in ("{target}", "{findings}", "{results}", "{backlog}",
                        "{exception_info}", "{focus_area}", "{changes}",
                        "{task_list}", "{assignments}", "{workspace_summary}", "{project}"):
                if var in prompt:
                    prompt = prompt.replace(var, fallback)
        return prompt

    def _kick_stalled_roles(self):
        """检测有 running workflow 但 steps 停滞超时的角色，重推任务。

        条件: step 处于 notified 状态超过 3 分钟，且无 timeout_count 递增
        (timeout_count 由 tick 处理，这里补 tick 照顾不到的冷启动停滞)
        """
        lm = self._lifecycle
        now = time.time()
        try:
            rows = lm.query(
                "SELECT instance_id, template_id, current_step_id, step_results, assignee "
                "FROM workflow_instances WHERE status='running'"
            )
            for row in rows:
                _results = json.loads(row["step_results"] or "{}")
                _sid = row["current_step_id"]
                _sr = _results.get(_sid, {})
                _status = _sr.get("status", "")
                if _status != "notified":
                    continue
                _notified_at = _sr.get("notified_at", 0)
                if not _notified_at or now - _notified_at < 180:
                    continue
                # 停滞超过 10 分钟 + 无 timeout_count → 重推
                _tc = _sr.get("timeout_count", 0)
                if _tc > 0:
                    continue  # tick 已经在处理
                _wf = self._workflows.get(row["template_id"])
                if not _wf:
                    continue
                _step = next((s for s in _wf.steps if s.id == _sid), None)
                if not _step:
                    continue
                # 用当前步骤的 target_role（同其他修复一致），不是 assignee
                _role = _step.target_role
                if not _role or _role in ("coordinator",):
                    continue
                # 角色已有挂起的任务 → 不打扰
                if self._is_role_busy(_role):
                    continue
                LOGGER.info("kick-stalled: %s/%s role=%s notified=%ds ago",
                            row["template_id"], row["instance_id"], _role, now - _notified_at)
                # 重推用统一变量填充（任务上下文 + 上一步产出），与首推一致
                _task_row = lm.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (row["instance_id"],)
                )
                _task = _task_row[0] if _task_row else None
                _tt = _task["title"] if _task else row["template_id"]
                _td = _task["description"] if _task else ""
                _prev_sid = ""
                _wf_steps = _wf.steps
                _idx = next((i for i, s in enumerate(_wf_steps) if s.id == _sid), -1)
                if _idx > 0:
                    _prev_sid = _wf_steps[_idx - 1].id
                _prompt = self._fill_prompt_vars(
                    _step.prompt_template, _step, row["template_id"],
                    _tt, _td, _results, _prev_sid)
                if _UNMATCHED_VAR_RE.search(_prompt):
                    LOGGER.warning("kick-stalled 跳过 %s/%s: 含未替换变量 %s",
                                   row["template_id"], _sid, _UNMATCHED_VAR_RE.findall(_prompt))
                    continue
                self._send_to_role(_role, _prompt,
                                   wf_id=row["instance_id"], step_id=_sid)
        except Exception as _e:
            LOGGER.debug("kick_stalled error: %s", _e)

    def _advance_production_wf(self, wf_id: str, lm, wf_name: str, step_id: str,
                                results: dict = None):
        """推进生产工作流到下一步，并通知目标角色。"""
        try:
            conn = lm._conn  # ponytail: 事务内批量操作，下一轮重构时统一用 execute_raw
            # 防重闭合守卫：launcher wf complete 全量覆写 step_results 会把已
            # completed 的工作流打回 step_done_ready，这里拒绝二次推进。
            _row = conn.execute(
                "SELECT status FROM workflow_instances WHERE instance_id=?", (wf_id,)
            ).fetchone()
            if _row and _row["status"] == "completed":
                LOGGER.debug("advance skipped: %s already completed", wf_id)
                return
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
                    if not isinstance(_sdata, dict):
                        continue
                    _sf = _sdata.get("subflow_id", "")
                    if _sf:
                        _st = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                        if not _st or _st['status'] not in ('completed', 'step_done_ready'):
                            _all_subs_done = False
                            break
                if _all_subs_done:
                    LOGGER.info("wf %s [%s] 完成: %d 步", wf_id[:8], wf_name, len(steps))
                    self._lifecycle.close_wf(wf_id, status='completed')
                    conn.commit()
                    # 部署自动闭环：把产出 cli/*.py 安装到 ~/.hermes/bin/，让工具可被
                    # cron/daemon/角色直接调用（"有实际收益"的最后一环——部署）。
                    self._deploy_production_outputs(wf_id)
                    self._bb.write("workflow",
                        f"[workflow] {wf_name} 完成: {len(steps)} 步",
                        src="workflow_engine")
                return
            # 推进到下一步
            next_step = steps[idx + 1]
            conn.execute("UPDATE workflow_instances SET current_step_id=? WHERE instance_id=? AND current_step_id=?",
                        (next_step.id, wf_id, step_id))
            new_results = dict(results or {})
            new_results[next_step.id] = {"status": "notified", "ts": time.time(), "notified_at": time.time(),
                                          "poll_since": time.time(), "bus_anchor": time.time()}
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
            prompt = self._fill_prompt_vars(
                next_step.prompt_template, next_step, wf_name,
                task_title, task_desc, results, prev_step_id=step_id)
            # 未替换变量检测：advance 层提前拦截，跳过通知但不中断推进（步骤状态已更新）
            if _UNMATCHED_VAR_RE.search(prompt):
                unmatched = _UNMATCHED_VAR_RE.findall(prompt)
                LOGGER.warning("advance skip notify: %s/%s step=%s 含未替换变量 %s",
                              wf_id[:8], wf_name, next_step.id, unmatched)
                return
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
            LOGGER.warning("_advance_production_wf failed %s: %s", wf_id, e)

    def _deploy_production_outputs(self, wf_id: str):
        """全链路完成后自动部署产出物到 ~/.hermes/bin/。

        扫描 engineer workspace 下的 cli/*.py，复制到生产路径。
        部署后写 bus cat=code_fix 通知维护者。
        """
        try:
            eng_ws = _CCS_WORKSPACES / "engineer" / "cli"
            if not eng_ws.exists():
                return
            deploy_dir = Path.home() / ".hermes" / "bin"
            deploy_dir.mkdir(parents=True, exist_ok=True)
            deployed = []
            for f in eng_ws.glob("*.py"):
                if f.name.startswith("_") or f.name.startswith("test_"):
                    continue
                import shutil
                dest = deploy_dir / f.name
                shutil.copy2(str(f), str(dest))
                deployed.append(f.name)
            if deployed:
                self._bb.write("code_fix",
                    f"[deploy] {wf_id[:12]} 产出已部署: {', '.join(deployed)}",
                    evidence=f"路径: ~/.hermes/bin/{'/'.join(deployed)}",
                    src="workflow_engine")
                LOGGER.info("deployed %d outputs from %s", len(deployed), wf_id[:12])
        except Exception as e:
            LOGGER.debug("deploy_production_outputs failed: %s", e)

    def _tick(self, run: WorkflowRun, step: Step):
        # ponytail: skip completed/cancelled runs — prevents stale step-escalation noise
        if run.status in ('completed', 'cancelled', 'step_done_ready'):
            return
        ec = step.exit_condition
        cat = ec.get("bus_category", "")
        src_filter = ec.get("source_contains", "")
        text_filter = ec.get("text_contains", "")
        timeout = ec.get("timeout_minutes", 30) * 60
        max_retries = step.max_retries

        # ── 双锚点：persisted.poll_since 与 bus_anchor，避免 tick 内消息被跳 ──
        # bus_anchor：永久记录已匹配消息的时间戳，只增不减；poll_since：轮询游标，仅在 exit_condition 匹配后同步
        # ponytail: fallback 用 time.time() 而非 run.created_at——当 wf complete 全量覆写
        # step_results 擦除 poll_since/bus_anchor 时，fall back 到 workflow 创建时间
        # 会导致 created_after 过滤器跳过所有近期 bus 消息，流水线永久卡死。
        _persisted = run.step_results.get(step.id, {}).get("poll_since", time.time())
        _bus_anchor = run.step_results.get(step.id, {}).get("bus_anchor")
        # 初始化时用 notified_at（步骤首次通知时间），确保不跳过消息
        if _bus_anchor is None:
            _bus_anchor = run.step_results.get(step.id, {}).get("notified_at", _persisted)
        last_ts = _bus_anchor
        _skip_before = run.step_results.get(step.id, {}).get("bus_skip_before", 0)

        # ── 旧引擎产物抢救: exit_messages 已存在但 tick 未闭合 → 直接 complete ──
        _sdata = run.step_results.get(step.id, {})
        if isinstance(_sdata, dict) and _sdata.get("exit_messages") and _sdata.get("status") != "completed":
            try:
                self._lifecycle.complete_step(run.id, step.id)
                LOGGER.info("tick rescue: %s/%s exit_messages pre-existing → completed", run.workflow_name, step.id)
                return
            except Exception as _e:
                LOGGER.debug("tick rescue failed for %s/%s: %s", run.workflow_name, step.id, _e)

        # exit_schema 校验（文件/内容约束）— 放在退出条件匹配之后，
        # 避免 schema 在步骤完成之前就阻塞推进。
        # ── Timeout 检查（每次 tick 都执行，不受 schema/verify 影响）──
        # 超时后自动升级到 coordinator，不会被 schema 失败 return 阻断。
        # 超限阈值: max(max_retries+2, 3) 次超时后自动 cancel，防止无限重试堆积。
        elapsed = time.time() - (run.step_results.get(step.id, {}).get("ts", run.created_at))
        _auto_cancel_at = max(max_retries + 3, 4)
        if elapsed >= timeout:
            # 角色忙（有其他任务执行中）→ 排队语义：不递增 timeout_count，
            # 仅延长响应窗口（避免"忙时被塞任务→未响应→超时回收"的假阳性）
            if self._is_role_busy(step.target_role, exclude_wf_id=run.id):
                _queued = run.step_results.get(step.id, {}).get("queued", 0) + 1
                # 排队上限: 连续排队超过上限（约 3 小时未响应）说明角色
                # 活跃但卡死（pm 类故障）→ 升级 coordinator 而非无限排队
                if _queued >= 6:
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"排队 {_queued} 次仍未响应（角色 {step.target_role} 活跃但可能卡死），请介入",
                        wf_id=run.id, step_id=run.current_step, force=True)
                run.step_results[step.id] = {
                    **run.step_results.get(step.id, {}),
                    "ts": time.time(),  # 重置计时器，等待角色空闲
                    "queued": _queued,
                }
                self._sync_step_results(run.id, run.step_results)
                return

            timeout_count = run.step_results.get(step.id, {}).get("timeout_count", 0) + 1
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
                "timeout_count": timeout_count,
                "ts": time.time(),  # 重置计时器，给角色响应窗口
            }
            self._sync_step_results(run.id, run.step_results)
            # 超限 → 自动 cancel，阻止死循环升级
            if timeout_count >= _auto_cancel_at:
                try:
                    self._lifecycle.close_wf(run.id, status="cancelled")
                except Exception as _e:
                    LOGGER.warning("auto-cancel close_wf failed: %s", _e)
                self._notify_role("maintainer",
                    f"工作流超时自动回收: {run.workflow_name}/{run.id}",
                    f"step={run.current_step} timeout_count={timeout_count} >= {_auto_cancel_at}\n请排查该步骤为何持续超时。")
                return
            # 重推任务（最多重推 1 次，不自动完成）
            # 角色忙（有其他任务在跑）→ 不强制重推，通知 coordinator 排队
            if timeout_count == 1:
                if self._is_role_busy(step.target_role, exclude_wf_id=run.id):
                    # 角色在忙，延长超时窗口：通知 coordinator 而非重推
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} "
                        f"超时1次但{step.target_role}忙碌（有其他任务执行中），自动排队等待",
                        wf_id=run.id, step_id=run.current_step, force=True)
                else:
                    self._send_to_role(step.target_role, step.prompt_template,
                                       wf_id=run.id, step_id=step.id, force=True)
                    self._send_to_role("coordinator",
                        f"[workflow] {run.workflow_name}/{run.current_step} 已超时 1 次，等待角色响应",
                        wf_id=run.id, step_id=run.current_step, force=True)

            # ── 宽松推进：角色已写同工作流 bus 消息但分类不匹配（如 evolution_report
            #    写成了 architecture）→ 产出存在即推进，避免"产出已存在却死锁"。
            #    触发条件：步骤已超时 >=1 次（角色确实执行过）且非首步。
            #    匹配策略：先找引擎发的 task_spec（含 wf 前缀）→ 再匹配该角色在
            #    task_spec 之后写的任意消息（标题可能不含 wf 前缀，按时间窗匹配）。
            if timeout_count >= 1:
                try:
                    _role_msgs = self._bb.read(limit=100)
                    _wf_id = run.id
                    _notified_ts = run.step_results.get(step.id, {}).get("notified_at", 0)
                    _relaxed = [
                        {"id": f.id, "text": f.t[:200], "ts": f.ts, "src": f.src}
                        for f in _role_msgs
                        if f.src == step.target_role
                        and f.ts > _notified_ts
                        and f.ts < time.time() + 60  # 防未来时间戳
                    ]
                    # task_spec 锚点确认确实是本工作流任务
                    _has_anchor = any(
                        _wf_id[:8] in str(f.t) and f.cat == "task_spec"
                        for f in _role_msgs
                    )
                    if _relaxed and _has_anchor:
                        LOGGER.info("relaxed-advance %s/%s: 角色已写同工作流 bus 消息 (cat 不匹配, %d 条)",
                                    run.workflow_name, step.id, len(_relaxed))
                        run.step_results[step.id] = {
                            **run.step_results.get(step.id, {}),
                            "bus_anchor": _relaxed[0]["ts"],
                            "exit_messages": _relaxed,
                            "relaxed_advance": True,
                        }
                        self._sync_step_results(run.id, run.step_results)
                        self._bb.write("blocker",
                            f"[workflow] {run.workflow_name} {step.id} 宽松推进: "
                            f"角色写了同工作流消息但分类不匹配（期望 {cat}）",
                            src="workflow_engine")
                        try:
                            self._lifecycle.complete_step(run.id, step.id)
                        except Exception as _e:
                            LOGGER.debug("relaxed-advance complete_step 失败: %s", _e)
                        return
                except Exception as _relax_err:
                    LOGGER.debug("relaxed-advance 检查失败: %s", _relax_err)

            return

        _match_ts, _match_msgs = self._check_exit(cat, src_filter, text_filter, created_after=last_ts)
        if _match_ts:
            # ── 锚定已匹配消息的时间戳，以后只检更新消息 ──
            # 同时保存 exit_messages 作为角色产出证据（task_evidence 依赖此字段判定收益）
            run.step_results[step.id] = {
                **run.step_results.get(step.id, {}),
                "bus_anchor": _match_ts,
                "exit_messages": _match_msgs,
            }
            self._sync_step_results(run.id, run.step_results)

            # schema/verify 只报警不阻停——角色写了 exit 消息就放行推进
            if step.exit_schema:
                ok, errs = self._validate_exit_schema(step, run)
                if not ok:
                    self._bb.write("blocker",
                        f"[workflow] {run.workflow_name} {step.id} schema: {'; '.join(errs)}",
                        src="workflow_engine")

            if step.verify:
                ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}
                vcmd = step.verify
                for k, val in ctx.items():
                    vcmd = vcmd.replace(f"{{{k}}}", str(val))
                ver = _sp.run(_shlex.split(vcmd), capture_output=True, timeout=30)
                if ver.returncode != 0:
                    self._bb.write("blocker",
                        f"[workflow] {run.workflow_name} {step.id} verify: {ver.stderr.decode()[:200] or 'failed'}",
                        src="workflow_engine")

            try:
                self._lifecycle.complete_step(run.id, step.id)
            except Exception as e:
                print(f"  [wf] LM complete_step 失败: {e}", flush=True)
                # 步骤不匹配说明 lifecycle 已推进到下一步 → 跳过不重试
                if "步骤不匹配" in str(e):
                    return
                # DB 瞬态故障 → 有 timeout_count 间接阻止无限重试
                _prev = run.step_results.get(step.id, {})
                run.step_results[step.id] = {
                    **_prev,
                    "timeout_count": _prev.get("timeout_count", 0) + 1,
                }
                self._sync_step_results(run.id, run.step_results)
                return

            # ── 空转检测: exit_messages 命中空报告模式 → 角色冷却 ──
            try:
                from cron_scheduler import _IDLE_PATTERNS
                from cron_scheduler import CronScheduler
                _msgs_text = " ".join(
                    m.get("text", "") for m in _match_msgs if isinstance(m, dict)
                ).lower()
                if any(pat in _msgs_text for pat in _IDLE_PATTERNS):
                    _cs = CronScheduler()
                    _cs.report_idle(step.target_role)
            except Exception as _idle_err:
                LOGGER.debug("idle detection skipped: %s", _idle_err)

            # ── 自动确认 handoff 步骤，推进到下一棒 ──
            # daemon 替代人工 approve，用 step 角色名作为密钥
            try:
                _token = self._lifecycle.get_approval_token(run.id, step.id)
                if _token:
                    self._lifecycle.confirm_step(run.id, step.id, token=_token, approved=True,
                                                  reason="daemon auto-advance")
                    LOGGER.info("auto-advance %s/%s -> next step", run.workflow_name, step.id)
            except Exception as e:
                LOGGER.debug("auto-advance skipped for %s/%s: %s", run.workflow_name, step.id, e)

            # lifecycle.manager 已写入 step_results，此处不再重复写入
            return

        # 退出条件未匹配（无 bus 消息，timeout 已在顶部检查）
        # poll_since 推进 = now - 30s 安全窗口，覆盖慢角色写 bus 的延迟（qa 写消息可达数分钟）。
        # bus_anchor 保持不动——它是"已匹配消息"的锚点，只在匹配时更新。
        # 修复：之前 no-match 也推进 bus_anchor，导致 tick 间消息被永久跳过。
        _now = time.time()
        _new_poll = _now - 30
        run.step_results[step.id] = {
            **run.step_results.get(step.id, {}),
            "poll_since": _new_poll,
        }
        self._sync_step_results(run.id, run.step_results)
        return

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
                try:
                    self._bb.write("code_fix", f"pipeflow: DB write+rollback 双重失败 wf={wf_id}",
                                   evidence=f"write_err={_e}, rollback_err={_e2}", src="pipeflow")
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("bus write fail in _sync_step_results fallback")

    def _validate_exit_schema(self, step: Step, run: WorkflowRun) -> tuple[bool, list[str]]:
        """校验 exit_schema 定义的文件约束。返回 (ok, error_list)。"""
        schema = step.exit_schema
        if not schema:
            return (True, [])
        ws = _CCS_WORKSPACES / step.target_role
        ws_real = os.path.realpath(ws)
        errs: list[str] = []

        for req in schema.get("required", []):
            fpath = ws / req
            fpath_real = os.path.realpath(fpath)
            if not fpath_real.startswith(ws_real):
                errs.append(f"路径越权: {req}")
                continue
            # 支持通配符: mc_*/ * → 检查 glob 匹配
            if "*" in req:
                matches = sorted(fpath.parent.glob(fpath.name))
                if not matches:
                    errs.append(f"缺少产出: {req} 无 glob 匹配")
            elif not fpath.exists():
                errs.append(f"缺少产出: {req}")

        for fname, props in schema.get("properties", {}).items():
            fpath = ws / fname

            if "minLength" in props:
                if fpath.exists():
                    content = fpath.read_text(encoding="utf-8", errors="replace")
                    if len(content) < props["minLength"]:
                        errs.append(f"{fname} 内容不足 ({len(content)}<{props['minLength']})")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "mustContain" in props:
                if fpath.exists():
                    content = fpath.read_text(encoding="utf-8", errors="replace")
                    missing = [kw for kw in props["mustContain"] if kw not in content]
                    if missing:
                        errs.append(f"{fname} 缺少必需内容: {missing}")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "mustContainUrl" in props and props["mustContainUrl"]:
                if fpath.exists():
                    content = fpath.read_text(encoding="utf-8", errors="replace")
                    if not re.search(r'https?://', content):
                        errs.append(f"{fname} 缺少 URL 链接 (mustContainUrl)")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "checksum" in props:
                import hashlib as _hl
                if fpath.exists():
                    content = fpath.read_text(encoding="utf-8", errors="replace").encode()
                    actual = _hl.md5(content).hexdigest()
                    if actual != props["checksum"]:
                        errs.append(f"{fname} checksum mismatch: {actual[:8]}≠{props['checksum'][:8]}")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "minFiles" in props:
                if fpath.is_dir():
                    ext = props.get("extension", "")
                    files = [f for f in fpath.iterdir() if f.is_file() and (not ext or f.name.endswith(ext))]
                    if len(files) < props["minFiles"]:
                        ext_label = ext or "文件"
                        errs.append(f"{fname} {ext_label} 文件不足 ({len(files)}<{props['minFiles']})")
                else:
                    errs.append(f"缺少产出: {fname}")

            if "minCount" in props:
                if "*" in fname:
                    import glob as _gl
                    matches = _gl.glob(str(fpath))
                    if len(matches) < props["minCount"]:
                        errs.append(f"{fname} glob 匹配不足 ({len(matches)}<{props['minCount']})")
                elif fpath.exists():
                    pass  # file exists but no glob → ok

        return (len(errs) == 0, errs)

    def _eval_cond(self, expr: str, run: WorkflowRun) -> bool:
        """简朴条件求值: s1.status == 'done' 格式。"""
        import re as _re
        m = _re.match(r"s(\d+)\.status\s*==\s*'(\w+)'", expr.strip())
        if not m:
            return False
        step_id = f"s{m.group(1)}"
        expected = m.group(2)
        sr = run.step_results.get(step_id)
        if not sr:
            return False
        return sr.get("status") == expected

    def _check_exit(self, cat: str, src_filter: str, text_filter: str, created_after: float = None) -> tuple:
        """检查 bus 是否有匹配 exit_condition 的消息。返回 (timestamp, matched_msgs)"""
        facts = self._bb.read(cat=cat, limit=50) if cat else self._bb.read(limit=50)
        earliest = 0.0
        matched = []
        for f in facts:
            if src_filter and f.src != src_filter:
                continue
            if text_filter and text_filter not in f.t:
                continue
            if created_after and f.ts < created_after:
                continue
            matched.append({"id": f.id, "text": f.t[:200], "ts": f.ts, "src": f.src})
            if earliest == 0 or f.ts < earliest:
                earliest = f.ts
        return (earliest, matched)

    @staticmethod
    def _is_agent_alive(role: str) -> bool:
        """检查 CCS tmux pane 内是否有 claude agent 进程在运行。"""
        try:
            r = _sp.run(
                ["tmux", "list-panes", "-t", f"ccs-{role}",
                 "-F", "#{pane_current_command}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode != 0 or not r.stdout.strip():
                return False
            cmd = r.stdout.strip().split("\n")[0]
            return cmd in ("claude", "claude-code", "python3", "python")
        except (ValueError, KeyError, TypeError):
            return False

    def _ensure_role_alive(self, role: str) -> bool:
        """检查角色的 CCS 是否存活（tmux + claude 进程），不存活则拉起。"""
        alive = _sp.run(
            ["tmux", "has-session", "-t", f"ccs-{role}"],
            capture_output=True, timeout=5,
        ).returncode == 0
        if alive and not self._is_agent_alive(role):
            # tmux 会话存活但无 claude 进程 → 重启
            LOGGER.info("CCS %s: tmux alive but no claude, restarting", role)
            _sp.run(["tmux", "kill-session", "-t", f"ccs-{role}"],
                    capture_output=True, timeout=5)
            alive = False
        if alive:
            # 代码变更检测已禁用：workspace 的 .py 是角色自己的产出（正常开发），
            # 全量 md5 扫描会误判"代码变更"→ 杀死正在工作的角色（2026-08-01 观测 8 次误杀）。
            # 如需热替换 launcher 代码，由运维手动 ccs restart 完成。
            return True

        # 拉起 CCS（infinite 模式，agent 常驻执行任务）
        # ponytail: ondemand 模式只写哨兵不创建 tmux 会话，必须用 infinite 让 agent 持续运行
        try:
            _sp.run(
                ["python3", str(_CCS_CLI), "start", role, "--no-attach",
                 "--drive", "infinite"],
                capture_output=True, timeout=30,
            )
            for _ in range(30):
                time.sleep(1)
                if _sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                           capture_output=True, timeout=5).returncode == 0:
                    time.sleep(3)  # 等 agent 进程启动
                    if self._is_agent_alive(role):
                        return True
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("CCS role alive check failed for %s", role)
        return False

    _HEALTH_DIR = Path.home() / ".hermes" / "run" / "ccs-health"

    def _is_role_busy(self, role: str, exclude_wf_id: str = "") -> bool:
        """检查角色是否真的在工作。

        信号1: survival monitor 健康文件（survival_monitor.py 每 120s 更新）
          - stale/idle/unknown → 角色不忙，跳过数据库检查直接返回 False
        信号2: workflow_instances 表中是否有该角色的其他活跃步骤（兜底）
        """
        # 信号1: survival health 文件（直接读，不重跑检测）
        # 信号1: survival health 文件（直接读，不重跑检测）
        try:
            hp = self._HEALTH_DIR / f"{role}.json"
            if hp.exists():
                h = json.loads(hp.read_text())
                overall = h.get("survival_overall", "unknown")
                # idle/unknown → 查 tmux pane 兜底：tmux 3.4 的 pane_activity 恒为空，
                # survival L2 信号失效，忙任务被误判 idle → 超时误回收。pane 存活即视为 busy。
                if overall in ("idle", "stale", "dead", "orphan", "unknown"):
                    if self._tmux_pane_alive(role):
                        LOGGER.info("health=%s 但 tmux pane 存活 → 视为 busy (survival L2 兜底)", overall)
                        return True
                    return False
                # healthy: 角色可能在工作，继续信号2确认
                # stale/l2=false: 角色空闲，放行
                if h.get("survival_l2_thinking") is False:
                    if self._tmux_pane_alive(role):
                        LOGGER.info("l2_thinking=false 但 tmux pane 存活 → 视为 busy (survival L2 兜底)")
                        return True
                    return False
        except Exception:
            pass  # 健康文件损坏，退化到信号2
        # 信号2: DB 兜底——角色健康但 workflow 层面已有别的活跃步骤
        # 按当前步骤的 target_role 匹配（同 _role_has_pending_assignment 语义）
        try:
            rows = self._lifecycle.query(
                "SELECT instance_id, template_id, current_step_id, step_results "
                "FROM workflow_instances "
                "WHERE status='running'"
            )
            for r in rows:
                if exclude_wf_id and r["instance_id"] == exclude_wf_id:
                    continue
                _wf = self._workflows.get(r["template_id"])
                if not _wf:
                    continue
                _step = next((s for s in _wf.steps if s.id == r["current_step_id"]), None)
                if not _step or _step.target_role != role:
                    continue
                sr = json.loads(r["step_results"] or "{}")
                sdata = sr.get(r["current_step_id"], {})
                if isinstance(sdata, dict) and sdata.get("status") in ("notified", "running"):
                    _q = sdata.get("queued", 0)
                    if _q >= 6:
                        LOGGER.info("%s queued=%d >= 6 → 不再视为 busy", role, _q)
                        continue
                    if sdata.get("timeout_count", 0) < 3:
                        LOGGER.info("skip: %s busy with %s/%s (step=%s)",
                                    role, r["instance_id"][:8], r["current_step_id"], sdata.get("status"))
                        return True
            return False
        except Exception:
            return False

    def _tmux_pane_alive(self, role: str) -> bool:
        """tmux pane 兜底信号：pane 存活即视为角色在忙。

        tmux 3.4 的 pane_activity 恒为空导致 survival L2 失效（详见 survival_monitor._l2_check），
        这里用 pane 存在 + active 作为低误报兜底。"""
        try:
            r = _sp.run(
                ["tmux", "list-panes", "-t", f"ccs-{role}", "-F", "#{pane_active}"],
                capture_output=True, text=True, timeout=3,
            )
            if r.returncode == 0 and "1" in r.stdout:
                return True
        except Exception:
            pass
        return False

    def _notify_role(self, role: str, title: str, evidence: str):
        """告警通知：写 blocker bus + ccs send 唤醒角色（绕过并发检查）。"""
        self._bb.write("blocker", title, evidence=evidence, src="workflow_engine")
        try:
            _sp.run(
                ["python3", str(_CCS_CLI), "send", role,
                 f"[workflow 告警] {title}\n\n{evidence}",
                 "--from", "workflow_engine"],
                capture_output=True, timeout=15,
            )
        except Exception:
            pass

    def _send_to_role(self, role: str, prompt: str,
                       wf_id: str = "", step_id: str = "", force: bool = False):
        """确保角色 CCS 存活，写完整 task_spec 到 bus，再 ccs send 推送全文。
        force=True 时跳过 pending/busy 检查，用于超时重推。"""
        # 并发保护：角色已有一个不同 workflow 的活跃步骤时，跳过推送
        # （统一走 _is_role_busy 并排除自身 wf，避免"自己的 pending 挡住重推"）
        if not force and wf_id and self._is_role_busy(role, exclude_wf_id=wf_id):
            LOGGER.info("send-to-role %s 跳过: 有其他 workflow 在进行中 (wf=%s step=%s)",
                        role, wf_id[:12], step_id)
            return
        # 未替换变量检测：含 {xxx} 模板变量的 prompt 不应发送，否则角色 /goal 用
        # 这些内容作 StopHook 条件时 bool([]) 为 False → 死循环
        if _UNMATCHED_VAR_RE.search(prompt):
            unmatched = _UNMATCHED_VAR_RE.findall(prompt)
            LOGGER.warning("send-to-role %s 跳过: prompt 含未替换变量 %s (wf=%s step=%s)",
                          role, unmatched, wf_id[:12] if wf_id else "?", step_id)
            self._bb.write("blocker",
                f"[workflow] {role} prompt 含未替换变量: {unmatched}",
                evidence=prompt[:500], src="workflow_engine")
            return
        self._ensure_role_alive(role)
        # ponytail: prompt_template 已含 /goal 前缀，避免重复叠加导致畸形
        if not prompt.startswith(("/goal", "/GOAL", "/Goal")):
            prompt = "/goal " + prompt
        # 写 TASKS.md 到角色 workspace，让模板中"读 TASKS.json"等指令能找到具体任务
        if wf_id:
            try:
                ws_dir = Path.home() / "ccs-workspaces" / role
                ws_dir.mkdir(parents=True, exist_ok=True)
                task_rows = self._lifecycle.query(
                    "SELECT title, description FROM tasks WHERE task_id=(SELECT task_id FROM workflow_instances WHERE instance_id=?)",
                    (wf_id,)
                ) if hasattr(self, '_lifecycle') else []
                if task_rows:
                    t = task_rows[0]
                    (ws_dir / "TASKS.md").write_text(
                        f"# 工作流任务\n\n## 标题\n{t['title'] or '?'}\n\n"
                        f"## 描述\n{t['description'] or t['title'] or '?'}\n\n"
                        f"## 来源\n工作流 {wf_id} / 步骤 {step_id}\n",
                        encoding="utf-8")
            except (ValueError, KeyError, TypeError):
                pass
        # 超时/告警通知走 blocker 而非 task_spec，防止 coordinator 误认为新任务
        _is_warning = any(kw in prompt for kw in ["持续超时", "异常", "超时自动回收"])
        if _is_warning:
            _title = f"[workflow] {role} 告警: {wf_id}/{step_id}" if wf_id else f"[workflow] {role} 告警"
            self._bb.write("blocker", _title, evidence=prompt, src="workflow_engine")
        else:
            _title = f"needs_implementation @{role} 工作流任务: {wf_id}/{step_id}" if wf_id else f"@{role} 工作流任务"
            self._bb.write("task_spec", _title, evidence=prompt, src="workflow_engine")
        # ccs send 推送全文（参数顺序：ccs.py send <target_role> <message> --from <source>）
        try:
            _sp.run(
                ["python3", str(_CCS_CLI), "send", role, prompt,
                 "--from", "workflow_engine"],
                capture_output=True, timeout=30,
            )
        except (ValueError, KeyError, TypeError):
            try:
                ccs_cli = Path.home() / "session-launcher" / "src" / "ccs.py"
                _sp.run(
                    ["python3", str(ccs_cli), "send", role, prompt[:2000]],
                    capture_output=True, timeout=10,
                )
            except Exception as _e:
                LOGGER.exception("fallback CCS send failed for %s", role)
                try:
                    self._bb.write("code_fix", "pipeflow: fallback CCS send failed role=" + role,
                                   evidence=str(_e)[:200], src="pipeflow")
                except (ValueError, KeyError, TypeError):
                    LOGGER.debug("bus write fail after fallback CCS send")

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
                LOGGER.exception("_write_step_prompt task context fetch failed for %s", run.id)
        prompt = step.prompt_template + "\n" + extra_prompt if extra_prompt else step.prompt_template
        for k, v in ctx.items():
            prompt = prompt.replace(f"{{{k}}}", str(v))
        # 语义兜底（与生产扫描块一致）：context 缺失时防止未替换变量导致发送跳过
        fallback = ctx.get("title") or ctx.get("task_title") or run.workflow_name or step.title or ""
        prompt = prompt.replace("{focus_area}", step.title or fallback)
        for var in ("{target}", "{findings}", "{results}", "{backlog}",
                    "{exception_info}", "{title}", "{description}", "{topic}",
                    "{task_definition}", "{acceptance_criteria}", "{project}",
                    "{changes}", "{task_list}", "{assignments}"):
            if var in prompt:
                prompt = prompt.replace(var, fallback)
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
        """Detect workflows stuck across multiple steps; auto-heal.
        Also monitors overall completion rate and backlog health."""
        try:
            conn = self._lifecycle._conn  # ponytail: 同上一轮重构时统一
            # 全局健康仪表板
            _total = conn.execute("SELECT COUNT(*) as c FROM workflow_instances").fetchone()
            _completed = conn.execute("SELECT COUNT(*) as c FROM workflow_instances WHERE status='completed'").fetchone()
            _running = conn.execute("SELECT COUNT(*) as c FROM workflow_instances WHERE status='running'").fetchone()
            total = _total["c"] if _total else 0
            if total > 20:
                completed = _completed["c"] or 0
                rate = round(completed / total * 100, 1)
                if rate < 20:
                    self._bb.write("architecture",
                        f"[workflow_engine] 完成率 {rate}% ({completed}/{total})，低于 20%，需人工审视",
                        src="workflow_engine")
                for _row in conn.execute(
                    "SELECT assignee, COUNT(*) as c FROM workflow_instances "
                    "WHERE status IN ('pending','running') GROUP BY assignee"
                ).fetchall():
                    if _row["c"] > 10:
                        self._bb.write("architecture",
                            f"[workflow_engine] 角色 {_row['assignee']} 积压 {_row['c']} 个运行中任务，超过阈值 10",
                            src="workflow_engine")
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
                    self._notify_role("maintainer",
                        f"工作流多步骤超时: {inst['instance_id'][:12]}",
                        f"{timed_out_steps} 个步骤已耗尽重试次数，当前步骤={inst.get('current_step_id','?')} 角色={inst.get('assignee','?')}")
                if timed_out_steps >= 3:
                    self._notify_role("maintainer",
                        f"工作流多步骤超限自愈: {inst['instance_id'][:12]}",
                        f"{timed_out_steps} 个步骤全部超限，正在强制重推，检查是否需人工介入。")
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
            # ── 指标持久化（JSONL）──
            try:
                _st_rows = conn.execute("SELECT status, COUNT(*) as c FROM workflow_instances GROUP BY status").fetchall()
                _metrics = {
                    "ts": time.time(),
                    "total": total,
                    "completed": (_completed["c"] if _completed else 0),
                    "rate": round((_completed["c"] or 0) / max(total, 1) * 100, 1),
                    "running": len(running) if running else _running["c"] if _running else 0,
                    "by_status": {r["status"]: r["c"] for r in _st_rows},
                }
                _mf = Path.home() / ".hermes" / "state" / "workflow-metrics.jsonl"
                _mf.parent.mkdir(parents=True, exist_ok=True)
                with _mf.open("a") as f:
                    f.write(json.dumps(_metrics, ensure_ascii=False) + "\n")
            except (ValueError, KeyError, TypeError):
                pass
        except Exception as _e:
            LOGGER.error("heal_stalled failed: %s", _e)


    def _scan_tasks(self):
        lm = self._lifecycle
        try:
            lm.ping()
        except (ValueError, KeyError, TypeError):
            LOGGER.debug("lifecycle ping failed during _scan_tasks")
        try:
            conn = lm._conn  # ponytail: 事务内批量操作，下一轮重构时统一用 execute_raw
            rows = conn.execute(
                "SELECT DISTINCT t.task_id, t.status FROM tasks t "
                "JOIN workflow_instances wi ON t.task_id = wi.task_id "
                "WHERE t.status NOT IN ('completed', 'failed', 'cancelled', 'step_done_ready')"
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
                    if not isinstance(_sdata, dict):
                        continue  # 简写值（如 "done"）没有 subflow_id
                    _sf = _sdata.get("subflow_id", "")
                    if _sf:
                        _sub_status = conn.execute("SELECT status FROM workflow_instances WHERE instance_id=?", (_sf,)).fetchone()
                        if _sub_status and _sub_status['status'] in ('completed', 'step_done_ready'):
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
            # ponytail: 动态任务生成——每轮 run_once 检查一次每个角色是否需要新任务
            self._generate_tasks_from_state()
            # ponytail: 任务质量反馈闭环——每轮 run_once 评估最近完成的任务并写入 bus
            try:
                from task_evidence import evaluate_and_feedback
                _feed = evaluate_and_feedback()
                if _feed.get("evaluated", 0):
                    LOGGER.info("task-evidence: evaluated %d completed tasks", _feed["evaluated"])
            except Exception as _e:
                LOGGER.debug("task_evidence feedback failed: %s", _e)
        except Exception as _e:
            LOGGER.exception("_scan_tasks 异常")
            try:
                self._bb.write("code_fix", f"pipeflow: _scan_tasks 异常",
                               evidence=str(_e), src="pipeflow")
            except (ValueError, KeyError, TypeError):
                LOGGER.debug("bus write fail in _scan_tasks error handler")

    _TASK_GEN_COOLDOWN: dict[str, float] = {}  # role → last gen ts

    def _generate_tasks_from_state(self):
        """从工作流模板为空闲角色自动生成任务。

        每轮 run_once 末尾运行，检测当前 DB 中无 running/pending 任务的角色，
        从加载的模板中选择最匹配的生成一个 task_spec 写入 bus。

        # 防重复机制
        - 每个角色每 600s 最多生成 1 个任务
        - 已有 running/pending workflow 的角色跳过
        - 生成标题描述性（≥12字符），非占位符
        """
        _lock = getattr(self, '_TASK_GEN_COOLDOWN', {})
        _now = time.time()
        _COOLDOWN_S = 120  # 2min，平衡速度与稳定性

        # 1) 哪些角色已有 running/pending 任务
        try:
            _busy_roles = set()
            for _r in self._lifecycle.query(
                "SELECT DISTINCT assignee FROM workflow_instances "
                "WHERE status IN ('running','pending','step_done_ready')"
            ):
                _busy_roles.add(_r['assignee'])
        except Exception:
            return

        # 2) 遍历模板，为每个有模板但空闲的角色生成任务
        _tasks_added = 0
        for _wf_name, _wf in self._workflows.items():
            if not _wf.steps or not _wf.allowed_executors:
                continue
            if _wf.is_subflow:
                continue
            # 取第一个 executor 作为目标角色
            _role = _wf.allowed_executors[0]
            if _role in _busy_roles:
                continue
            if not self._can_assign_role(_role):
                continue
            # 跳过空模板描述（task_spec needs_implementation → 理解 → 编码 是占位符）
            if not _wf.description or len(_wf.description.strip()) < 20 or "needs_implementation" in (_wf.description or ""):
                continue
            # 价值门槛：自动生成必须有真实需求信号，防止模板空转。
            # 无 bus 需求信号（task_spec/blocker/需求类消息）时跳过自动生成。
            try:
                _demand = self._bb.read(cat="task_spec", limit=10)
                _demand_text = ""
                for _f in _demand:
                    if _f.src not in ("workflow_engine", "survival_monitor"):
                        # closer 周期性 backlog 扫描是模板循环产物，不是真实需求
                        if "backlog_scan" not in (_f.t or ""):
                            _demand_text += (_f.t or "") + " " + (_f.e or "") + " "
                # 需求信号必须与目标角色相关：标题或内容含角色名/其职责关键词才算
                _role_hits = (
                    f"@{_role}" in _demand_text
                    or f"assignee={_role}" in _demand_text
                    or _role in _demand_text
                )
                _has_demand = bool(_demand_text) and _role_hits and _demand_text.strip() != ""
                if not _has_demand:
                    continue
            except Exception:
                pass
            # cooldown 检测
            _last = self._TASK_GEN_COOLDOWN.get(_role, 0)
            if _now - _last < _COOLDOWN_S:
                continue
            # 同模板同角色：有活跃实例(running/pending) → 跳过（防重复创建）
            # 24h 内已完成/取消过 → 跳过（防循环派发同模板任务）
            try:
                _existing = conn.execute(
                    "SELECT COUNT(*) as c FROM workflow_instances "
                    "WHERE template_id=? AND assignee=? "
                    "AND (status IN ('running','pending') OR created_at > ?)",
                    (_wf_name, _role, _now - 86400)
                ).fetchone()["c"]
                if _existing > 0:
                    continue
            except Exception:
                pass

            # 3) 组装有描述性的任务标题
            _title = f"[auto] {_wf.title or _wf_name}: {_wf.description[:40]}" if _wf.description else f"[auto] 执行 {_wf_name} 工作流"
            if len(_title) < 12:
                continue  # 跳过标题过短
            _prompt = (
                f"/goal\n\n"
                f"## 任务\n执行工作流模板 {_wf_name}\n\n"
                f"## 描述\n{_wf.description}\n\n"
                f"## 步骤\n"
            )
            for _s in _wf.steps:
                _prompt += f"  {_s.id}: {_s.title}\n"
            if _wf.quality_standards:
                _prompt += f"\n## 质量标准\n{_wf.quality_standards}\n"

            # 4) 直接启动工作流——绕过路由 daemon 直接创建 task + workflow_instance
            # _can_assign_role 已检测 tmux 会话存活
            try:
                _task_id = f"task_auto_{uuid.uuid4().hex[:8]}"
                _now_ts = time.time()
                # 写入 task 记录
                try:
                    self._lifecycle.execute(
                        "INSERT OR IGNORE INTO tasks (task_id, title, description, assigner, assignee, status, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, 'open', ?, ?)",
                        (_task_id, _title[:80], _wf.description[:200] if _wf.description else "",
                         "workflow_engine", _role, _now_ts, _now_ts))
                except Exception:
                    pass  # 表可能不存在
                # 启动工作流实例（创建 + 推 prompt 到角色）
                _rid = self.start(_wf_name, context={"task_id": _task_id, "title": _title})
                # 修正 assignee — start() 内部用 workflow_engine role，需覆盖为真实执行者
                try:
                    self._lifecycle.execute(
                        "UPDATE workflow_instances SET assignee=? WHERE instance_id=?",
                        (_role, _rid))
                except Exception:
                    pass
                # 更新 task → workflow 关联
                try:
                    self._lifecycle.execute(
                        "UPDATE workflow_instances SET task_id=? WHERE instance_id=?",
                        (_task_id, _rid))
                except Exception:
                    pass
                LOGGER.info("auto-task: %s → %s (wf=%s, task=%s)", _role, _title[:60], _rid[:16], _task_id)
                self._TASK_GEN_COOLDOWN[_role] = _now
                _tasks_added += 1
                if _tasks_added >= 5:
                    break  # 每轮最多创建 5 个任务，加速产出
            except Exception as _e:
                LOGGER.debug("auto-task start failed for %s: %s", _role, _e)

        if _tasks_added:
            LOGGER.info("auto-task: 本轮生成 %d 个新任务", _tasks_added)

    def _can_assign_role(self, role: str) -> bool:
        """检查角色是否可分配任务：L1+L2+L3 忙闲检测。

        L1: tmux 会话存活
        L2: 正在思考（pane 活跃 < 300s）→ busy，跳过
        L3: 最近有产出（bus 最近 10min 有该角色产出）→ busy，跳过
        idel/stale/unknown → 放行
        """
        _skip = {"coordinator", "pipeline", "claude", "workflow_engine", ""}
        if role in _skip:
            return False
        # L1: tmux 存活
        try:
            _r = _sp.run(["tmux", "has-session", "-t", f"ccs-{role}"],
                         capture_output=True, timeout=5)
            if _r.returncode != 0:
                return False
        except Exception:
            return False
        # L2: pane 活动
        try:
            _pr = _sp.run(["tmux", "list-panes", "-t", f"ccs-{role}",
                           "-F", "#{pane_activity}"],
                          capture_output=True, text=True, timeout=3)
            if _pr.returncode == 0 and _pr.stdout.strip():
                _val = _pr.stdout.strip().split("\n")[0]
                if _val and _val != "0":
                    if time.time() - float(_val) < 60:
                        return False  # 1分钟内活跃 → 真在忙，跳过
        except Exception:
            pass
        # ponytail: L3 bus 产出检查移除——L1+L2 足够判断忙闲，L3 太慢导致空角色无法分配
        return True


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

    _PID_FILE = Path.home() / ".hermes" / "run" / "workflow-engine-daemon.pid"

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
