#!/usr/bin/env python3
"""
Workflow Engine --- 数据驱动的对话工作流执行引擎。

通过 Sister Bus 与 CCS 会话交互，执行 JSON 定义的工作流。
工作流来自 ~/.hermes/workflows/*.json，运行状态持久化到 runs/*.json，
同时扫描 SQLite 中的 production 工作流实例并自动推进。
"""

import json
import re
import time
import uuid
import logging
import subprocess as _sp

from pathlib import Path
LOGGER = logging.getLogger("workflow.engine")
from typing import Any, Optional

from paths import ensure_paths
ensure_paths()
from paths import HERMES_WORKFLOWS as _WORKFLOWS_DIR

from bus_protocol import Blackboard

from dataclasses import dataclass

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


# ── 混入模块：方法签名和 self 访问完全不变 ─────────────────────────
# 注意：混入导入必须放在 dataclass 定义之后，mixin 顶层依赖这些类。
from pipeflow.step_handler import StepHandlerMixin
from pipeflow.role_lifecycle import RoleLifecycleMixin
from pipeflow.task_generator import TaskGeneratorMixin
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin


class WorkflowEngine(WorkflowLifecycleMixin, TaskGeneratorMixin, RoleLifecycleMixin, StepHandlerMixin):
    """工作流引擎主类。各 mixin 提供具体职责，此类保留编排与 CLI。"""

    # _sp: 子进程引用，供 mixin 方法通过 self._sp 访问（测试 patch("pipeflow.engine._sp") 可替换）
    _sp = _sp

    def __init__(self, workflows_dir: Path = _WORKFLOWS_DIR, db_path: str = None):
        self.workflows_dir = Path(workflows_dir).expanduser()
        self.runs_dir = self.workflows_dir / "runs"
        # 非默认目录（测试/沙箱）→ 使用目录内独立 DB，避免污染生产 workflows.db
        # ponytail: 测试仍共享生产 bus；若要完全隔离需注入 Blackboard
        self._db_path = db_path or (
            str(self.workflows_dir / "test_workflows.db")
            if self.workflows_dir != Path(_WORKFLOWS_DIR)
            else None
        )
        self._bb = Blackboard()
        self._workflows: dict[str, WorkflowDef] = {}
        self._lm = None
        self._load_workflows()

    @property
    def _lifecycle(self):
        if self._lm is None:
            from lifecycle.manager import LifecycleManager
            self._lm = LifecycleManager("workflow_engine", db_path=self._db_path,
                                        on_advance=self._ensure_role_alive)
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
                        LOGGER.warning("[wf] 警告: %s 质量检查不通过:", f.name)
                        for _e in _vr.errors[:4]:
                            LOGGER.warning("        - %s", _e)
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
                    LOGGER.warning("[wf] 加载 %s 失败: %s", f.name, e)
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
                # JSON 定义优先：DB 中已有同名模板（简化版/旧版）→ 跳过，防止
                # upsert 的简化 steps（无 exit_condition）覆盖完整 JSON 定义
                # （bus #167084 根因：去掉此检查后 s4 exit_condition 变空 → 工作流卡死）
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
            LOGGER.warning("[wf] SQLite 模板加载失败: %s", e)

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
                LOGGER.error("[wf] LM start_wf 失败: %s", e)
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
            LOGGER.error("[wf] upsert template 失败: %s", e)

    def status(self, wid: str) -> dict:
        try:
            rows = self._lifecycle.query(
                "SELECT instance_id, template_id, status, current_step_id, step_results, created_at "
                "FROM workflow_instances WHERE instance_id=?", (wid,)
            )
            row = rows[0] if rows else None
        except (ValueError, KeyError, TypeError, IndexError):
            row = None
        if not row:
            return {"error": "不存在"}
        try:
            d = dict(row)
        except Exception:
            return {"error": "行数据异常"}
        try:
            results = json.loads(d.get("step_results") or "{}")
        except (ValueError, TypeError):
            results = {}
        return {
            "id": d.get("instance_id", wid), "workflow": d.get("template_id", ""),
            "status": d.get("status", "unknown"), "current_step": d.get("current_step_id", ""),
            "retries": {}, "results": results,
        }

    def cancel(self, wid: str) -> bool:
        try:
            rows = self._lifecycle.query(
                "SELECT status, created_at FROM workflow_instances WHERE instance_id=?",
                (wid,),
            )
            row = rows[0] if rows else None
        except (ValueError, KeyError, TypeError, IndexError):
            row = None
        if not row or row["status"] in ("completed", "cancelled", "failed"):
            return False
        # 最小存活时间保护：创建不足 5 分钟的工作流禁止 cancel。
        # 防止"通知角色后 32 秒即被取消"的误杀（bus #158145：s1 响应窗口仅 32s）。
        # ponytail: 5 分钟是保守值，正式角色多数 5 分钟内会响应；超时取消不受此限制（走 _tick 路径）。
        _created = row["created_at"] or 0
        if time.time() - _created < 300:
            LOGGER.warning("cancel rejected: %s 创建不足 5 分钟 (age=%.0fs)，拒绝取消", wid, time.time() - _created)
            return False
        self._lifecycle.close_wf(wid, status="cancelled")
        return True

    def tick(self) -> int:
        self.run_once()
        return 0

    def run_once(self):
        # 每次 tick 重载模板（支持热加载，DB 修改即时生效）
        self._load_workflows()

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
