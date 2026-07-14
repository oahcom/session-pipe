#!/usr/bin/env python3
"""
CompositeRunner — 跨角色复合工作流引擎。

将 atomic workflow 编排为多步骤流水线：
- subflow：原子工作流→创建 Task + Workflow Instance → 写 bus notice
- parallel：并行执行多个 subflow
- choice：条件分支

依赖：
  composite_models.py (CompositeRun, CompositeRunDB)
  workflow_client.py (WorkflowClient)
  bus_protocol.py (Blackboard)
"""

from __future__ import annotations
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

# ── 路径 ──
_PIPELINE_SRC = Path.home() / "session-pipeline" / "src"
_LAUNCHER_SRC = Path.home() / "session-launcher" / "src"
for p in [_PIPELINE_SRC, _LAUNCHER_SRC]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from bus_protocol import Blackboard
from composite_models import CompositeRun, CompositeRunDB

_CHAINS_DIR = Path.home() / ".hermes" / "workflows" / "chains"


class CompositeRunner:
    def __init__(self, chains_dir: Path = _CHAINS_DIR):
        self.chains_dir = Path(chains_dir).expanduser()
        self.chains_dir.mkdir(parents=True, exist_ok=True)
        self._db = CompositeRunDB()
        self._bb = Blackboard()
        self._templates: dict[str, dict] = {}
        self._load_templates()

    def _load_templates(self):
        self._templates.clear()
        if not self.chains_dir.exists():
            return
        for f in sorted(self.chains_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                if data.get("type") == "composite":
                    self._templates[data["name"]] = data
            except Exception:
                pass

    def list_chains(self) -> list[str]:
        return sorted(self._templates.keys())

    # ── API ──────────────────────────────────────────────────────

    def start(self, name: str, context: dict = None) -> str:
        """启动复合工作流。创建 Run → 派发第一步。"""
        template = self._templates.get(name)
        if not template:
            raise ValueError(f"未知复合工作流: {name}. "
                             f"可用: {list(self._templates.keys())}")

        run = CompositeRun(
            run_id=f"chain_{uuid.uuid4().hex[:12]}",
            name=name, context=context or {},
            current_step_id=template["steps"][0]["id"],
        )
        # 所有步骤初始化为 pending
        for s in template["steps"]:
            run.step_statuses[s["id"]] = "pending"

        self._db.save(run)
        # 派发第一步
        self._dispatch(run, template["steps"][0])
        run.step_statuses[run.current_step_id] = "running"
        self._db.save(run)
        return run.run_id

    def status(self, run_id: str) -> dict:
        run = self._db.load(run_id)
        if not run:
            return {"error": "不存在"}
        return {
            "id": run.run_id, "name": run.name,
            "status": run.status, "current_step": run.current_step_id,
            "step_statuses": run.step_statuses,
            "sub_runs": run.sub_runs,
            "errors": run.errors,
        }

    def cancel(self, run_id: str, reason: str = "") -> bool:
        run = self._db.load(run_id)
        if not run or run.status in ("completed", "cancelled"):
            return False
        run.status = "cancelled"
        if reason:
            run.errors.append(reason)
        self._db.save(run)
        return True

    def list_runs(self, status: str = None) -> list[dict]:
        runs = self._db.list_runs(status=status or "")
        return [{"id": r.run_id, "name": r.name, "status": r.status,
                 "current_step": r.current_step_id} for r in runs]

    # ── 核心推进 ─────────────────────────────────────────────────

    def tick(self) -> int:
        """推进所有 running 的 composite run。返回推进的步数。"""
        advanced = 0
        for run in self._db.list_runs(status="running"):
            try:
                if self._tick_one(run):
                    advanced += 1
            except Exception as e:
                run.errors.append(f"tick error: {e}")
                self._db.save(run)
        return advanced

    def _tick_one(self, run: CompositeRun) -> bool:
        """推进单个 run 的当前步骤。返回 True 表示有推进。"""
        template = self._templates.get(run.name)
        if not template:
            return False

        current = next(
            (s for s in template["steps"] if s["id"] == run.current_step_id),
            None
        )
        if not current:
            return False

        # choice 步骤直接推进（条件在 dispatch 时已决定分支）
        if current.get("step_type") == "choice":
            self._advance(run, template)
            return True

        # subflow / parallel 检查 exit_condition
        if self._check_exit(run, current):
            self._advance(run, template)
            return True
        return False

    def _check_exit(self, run: CompositeRun, step: dict) -> bool:
        """检查当前步骤的 exit_condition。"""
        ec = step.get("exit_condition", {})
        if not ec:
            return True  # 无 exit_condition → 自动完成
        cat = ec.get("bus_category", "")
        src = ec.get("source_contains", "")
        text = ec.get("text_contains", "")

        facts = self._bb.read(cat=cat) if cat else self._bb.read()
        for f in facts:
            if src and (f.src or "") != src:
                continue
            if text and f.t and text not in f.t:
                continue
            return True
        return False

    def _advance(self, run: CompositeRun, template: dict):
        """完成当前步骤 → 解析下一步 → dispatch。"""
        run.step_statuses[run.current_step_id] = "completed"

        next_steps = self._resolve_next(template, run)
        if not next_steps:
            run.status = "completed"
            self._db.save(run)
            elapsed = run.updated_at - run.created_at
            self._bb.write("reflexion_lesson",
                f"[composite] {run.name} 完成: "
                f"{len(template['steps'])} 步, 耗时 {elapsed:.0f}s",
                src="composite_runner")
            return

        for ns in next_steps:
            run.step_statuses[ns["id"]] = "running"
            self._dispatch(run, ns)

        run.current_step_id = next_steps[0]["id"]
        self._db.save(run)

    def _resolve_next(self, template: dict, run: CompositeRun) -> list[dict]:
        """找出可推进的下一步（支持并行）。"""
        completed = {s for s, st in run.step_statuses.items() if st == "completed"}
        running = {s for s, st in run.step_statuses.items() if st == "running"}
        all_steps = template["steps"]

        next_steps = []
        for s in all_steps:
            sid = s["id"]
            if sid in completed or sid in running:
                continue
            deps = set(s.get("depends_on", []))
            # 检查依赖是否全部完成
            if deps and not deps.issubset(completed):
                continue
            next_steps.append(s)

        return next_steps

    def _dispatch(self, run: CompositeRun, step: dict):
        """派发一个步骤（subflow / parallel）。"""
        step_type = step.get("step_type", "subflow")

        if step_type == "subflow":
            self._dispatch_subflow(run, step)
        elif step_type == "parallel":
            for sub in step.get("subflows", []):
                self._dispatch_subflow(run, {**sub, "step_type": "subflow"})
        elif step_type == "choice":
            self._dispatch_choice(run, step)

    def _dispatch_subflow(self, run: CompositeRun, step: dict):
        """创建 Task + Workflow Instance → 写 notice 通知角色。"""
        subflow = step.get("subflow", "")
        role = step.get("role", "")
        title = step.get("title", subflow)
        step_id = step.get("id", run.current_step_id)

        try:
            from workflow_client import WorkflowClient
            with WorkflowClient("coordinator") as wf:
                task_id = wf.create_task(
                    f"[{run.name}] {title}",
                    f"来自复合工作流 {run.name}, 上下文: {json.dumps(run.context)}",
                    assignee=role,
                )
                wf_id = wf.create(role, title, task_id=task_id)

                # 记录子工作流 ID
                run.sub_runs.setdefault(step_id, []).append(wf_id)
                self._db.save(run)

                # 写 notice 通知角色
                self._bb.write("notice",
                    f"@{role}: {title}. context: {json.dumps(run.context)}",
                    src="composite_runner")
        except Exception as e:
            run.errors.append(f"dispatch {step_id}: {e}")
            self._db.save(run)

    def _dispatch_choice(self, run: CompositeRun, step: dict):
        """根据条件分支派发。"""
        branches = step.get("branches", {})
        cond = step.get("condition", "")
        if cond:
            # 简单评估：检查 bus 是否有 security_audit（hotfix 分支）
            if "critical" in cond.lower() or "security" in cond.lower():
                facts = self._bb.read(cat="security_audit", limit=1)
                branch_key = "critical" if facts else "low"
            else:
                branch_key = list(branches.keys())[0]
        else:
            branch_key = list(branches.keys())[0]

        chosen = branches.get(branch_key, list(branches.values())[0])
        self._dispatch_subflow(run, {**chosen, "step_type": "subflow"})
