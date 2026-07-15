#!/usr/bin/env python3
"""
Workflow Engine — 数据驱动的对话工作流执行引擎。

通过 Sister Bus 与 CCS 会话交互，执行 JSON 定义的工作流。
工作流来自 ~/.hermes/workflows/*.json，运行状态持久化到 runs/*.json。

用法：
  python3 src/workflow_engine.py start <name> --context '{"key":"val"}'
  python3 src/workflow_engine.py status <workflow_id>
  python3 src/workflow_engine.py cancel <workflow_id>
  python3 src/workflow_engine.py run     # 以守护模式轮询
"""

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

# ── 路径 ───────────────────────────────────────────────────────────
from paths import ensure_paths
ensure_paths()
from paths import HERMES_WORKFLOWS as _WORKFLOWS_DIR

from bus_protocol import Blackboard

_RUNS_DIR = _WORKFLOWS_DIR / "runs"
_POLL_SLEEP = 5
_TIMEOUT_GRACE = 10


@dataclass
class Step:
    id: str
    title: str
    target_role: str
    prompt_template: str
    exit_condition: dict
    max_retries: int = 0
    condition: str = ""
    rollback_to: str = ""


@dataclass
class WorkflowDef:
    name: str
    title: str
    description: str
    steps: list[Step]


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
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self._bb = Blackboard()
        self._workflows: dict[str, WorkflowDef] = {}
        self._composite_runner = None
        self._load_workflows()

    @property
    def composite_runner(self):
        if self._composite_runner is None:
            from composite_runner import CompositeRunner
            self._composite_runner = CompositeRunner()
        return self._composite_runner

    def _load_workflows(self):
        if not self.workflows_dir.exists():
            return
        for f in sorted(self.workflows_dir.glob("*.json")):
            if f.parent == self.runs_dir:
                continue
            try:
                data = json.loads(f.read_text())
                steps = [Step(**s) for s in data.get("steps", [])]
                self._workflows[data["name"]] = WorkflowDef(
                    name=data["name"], title=data.get("title", ""),
                    description=data.get("description", ""), steps=steps,
                )
            except Exception as e:
                print(f"  [wf] 加载 {f.name} 失败: {e}")

    def list_workflows(self) -> list[str]:
        return sorted(self._workflows.keys())

    # ── API ───────────────────────────────────────────────────────

    def start(self, name: str, context: dict = None) -> str:
        # 尝试作为 atomic workflow 启动
        wf = self._workflows.get(name)
        if wf:
            if not wf.steps:
                raise ValueError(f"工作流 {name} 无步骤")
            run = WorkflowRun(
                id=f"wf_{uuid.uuid4().hex[:8]}",
                workflow_name=name, context=context or {},
                current_step=wf.steps[0].id,
            )
            self._save_run(run)
            self._write_step_prompt(run, wf.steps[0])
            self._save_run(run)
            return run.id

        # 尝试作为 composite workflow 启动
        if name in self.composite_runner.list_chains():
            return self.composite_runner.start(name, context)

        raise ValueError(f"未知工作流: {name}")

    def status(self, wid: str) -> dict:
        run = self._load_run(wid)
        if not run:
            return {"error": "不存在"}
        return {
            "id": run.id, "workflow": run.workflow_name,
            "status": run.status, "current_step": run.current_step,
            "retries": run.step_retries, "results": run.step_results,
        }

    def cancel(self, wid: str) -> bool:
        run = self._load_run(wid)
        if not run or run.status in ("completed", "cancelled"):
            return False
        run.status = "cancelled"
        self._save_run(run)
        return True

    # ── daemon loop ───────────────────────────────────────────────

    def tick(self) -> int:
        """显式推进 composite workflows。返回推进的步数。"""
        return self.composite_runner.tick()

    def run_once(self):
        """执行一轮：处理所有 running 状态的 atomic + composite work run。"""
        # Atomic workflows
        for run_file in self.runs_dir.glob("*.json"):
            try:
                run = self._load_run_data(run_file.read_text())
                if not run or run.status != "running":
                    continue
                wf = self._workflows.get(run.workflow_name)
                if not wf:
                    continue
                current = next((s for s in wf.steps if s.id == run.current_step), None)
                if not current:
                    continue
                self._tick(run, current)
            except Exception:
                pass
        # Composite workflows
        self.composite_runner.tick()

    def _tick(self, run: WorkflowRun, step: Step):
        """检查当前步骤的 exit_condition。"""
        ec = step.exit_condition
        cat = ec.get("bus_category", "")
        src_filter = ec.get("source_contains", "")
        text_filter = ec.get("text_contains", "")
        timeout = ec.get("timeout_minutes", 30) * 60
        max_retries = step.max_retries

        # 检查匹配
        if self._check_exit(cat, src_filter, text_filter):
            # 完成当前步骤
            run.step_results[step.id] = {"status": "done", "ts": time.time()}
            self._advance(run)
            return

        # 检查超时
        elapsed = time.time() - (run.step_results.get(step.id, {}).get("ts", run.created_at))
        if elapsed > timeout + _TIMEOUT_GRACE:
            attempt = run.step_retries.get(step.id, 0) + 1
            if attempt <= max_retries:
                run.step_retries[step.id] = attempt
                self._write_step_prompt(run, step)
            else:
                run.status = "failed"
                self._bb.write("blocker",
                    f"[workflow] {run.workflow_name} 步骤 {step.id} 失败(超过最大重试)",
                    src="workflow_engine")
            self._save_run(run)

    def _check_exit(self, cat: str, src_filter: str, text_filter: str) -> bool:
        """检查 bus 中是否有匹配 exit_condition 的消息（不消费）。"""
        # 使用 read() 而非 unconsumed()，避免消费消息导致目标角色读不到
        facts = self._bb.read(cat=cat, limit=50) if cat else self._bb.read(limit=50)
        for f in facts:
            if src_filter and f.src != src_filter:
                continue
            if text_filter and text_filter not in f.t:
                continue
            return True
        return False

    def _advance(self, run: WorkflowRun):
        """推进到下一步或完成。"""
        wf = self._workflows.get(run.workflow_name)
        if not wf:
            return
        idx = next((i for i, s in enumerate(wf.steps) if s.id == run.current_step), -1)
        if idx < 0 or idx + 1 >= len(wf.steps):
            run.status = "completed"
            self._save_run(run)
            # 完成后写 workflow 记录（不写 reflexion_lesson）
            elapsed = run.updated_at - run.created_at
            self._bb.write("workflow",
                f"[workflow] {run.workflow_name} 完成: {len(wf.steps)} 步, 耗时 {elapsed:.0f}s",
                src="workflow_engine")
            return
        next_step = wf.steps[idx + 1]
        if next_step.condition and not self._eval_cond(next_step.condition, run):
            return
        run.current_step = next_step.id
        self._write_step_prompt(run, next_step)
        self._save_run(run)

    def _eval_cond(self, expr: str, run: WorkflowRun) -> bool:
        """安全评估步骤条件：仅支持 s{id}.status == '状态' 格式。
        不 eval，仅做模式匹配。"""
        try:
            import re
            # 匹配 s<数字>.status == '<状态>' 模式
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

    def _write_step_prompt(self, run: WorkflowRun, step: Step):
        """发送 step prompt 到 bus。"""
        ctx = {**run.context, "workflow_id": run.id, "step_id": step.id}

        # 收集 workspace_summary
        if "{workspace_summary}" in step.prompt_template:
            ws_dir = Path.home() / ".hermes" / "workspace" / ctx.get("project_name", "")
            ctx["workspace_summary"] = self._collect_workspace_summary(ws_dir)

        prompt = step.prompt_template
        for k, v in ctx.items():
            prompt = prompt.replace(f"{{{k}}}", str(v))
        self._bb.write("workflow", prompt, src="workflow_engine")

    def _collect_workspace_summary(self, ws_dir: Path) -> str:
        """收集 workspace 下的关键文件摘要。"""
        if not ws_dir.exists():
            return "workspace 不存在"
        parts = []
        for fname in ["PRD.md", "DESIGN.md", "TASKS.json", "INTAKE.md"]:
            fpath = ws_dir / fname
            if fpath.exists():
                content = fpath.read_text()
                parts.append(f"[{fname}] {content[:200]}...")
        return "\n".join(parts) if parts else "workspace 为空"

    def _save_run(self, run: WorkflowRun):
        """原子性保存工作流运行状态。"""
        run.updated_at = time.time()
        data = json.dumps({
            "id": run.id, "workflow_name": run.workflow_name,
            "context": run.context, "current_step": run.current_step,
            "step_retries": run.step_retries, "status": run.status,
            "created_at": run.created_at, "updated_at": run.updated_at,
            "step_results": run.step_results,
        }, ensure_ascii=False, indent=2)
        tmp_path = self.runs_dir / f"{run.id}.tmp"
        final_path = self.runs_dir / f"{run.id}.json"
        tmp_path.write_text(data)
        tmp_path.rename(final_path)

    def _load_run(self, wid: str) -> Optional[WorkflowRun]:
        p = self.runs_dir / f"{wid}.json"
        return self._load_run_data(p.read_text()) if p.exists() else None

    def _load_run_data(self, text: str) -> Optional[WorkflowRun]:
        try:
            d = json.loads(text)
            return WorkflowRun(**d)
        except Exception:
            return None


# ── CLI ───────────────────────────────────────────────────────────

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
        # 单实例锁
        if _PID_FILE.exists():
            try:
                old = int(_PID_FILE.read_text())
                os.kill(old, 0)
                print(f"Daemon PID={old} 已在运行")
                sys.exit(1)
            except (OSError, ValueError):
                pass
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

# ── DAG 步骤支持（Hatchet 模式）──
# 允许工作流步骤之间存在 DAG 依赖关系
# 步骤可以并行执行，前置条件满足后自动触发

_DAG_RUNNING: dict[str, set] = {}  # run_id -> set of completed steps

def dag_step_done(run_id: str, step_id: str) -> list[str]:
    """标记 DAG 步骤完成，返回可解锁的下游步骤列表。"""
    if run_id not in _DAG_RUNNING:
        _DAG_RUNNING[run_id] = set()
    _DAG_RUNNING[run_id].add(step_id)
    return list(_DAG_RUNNING[run_id])

def dag_ready_steps(run_id: str, dag: dict[str, list[str]]) -> list[str]:
    """返回 DAG 中当前可执行的步骤（所有前置依赖已完成）。"""
    completed = _DAG_RUNNING.get(run_id, set())
    ready = []
    for step, deps in dag.items():
        if step in completed:
            continue
        if all(d in completed for d in deps):
            ready.append(step)
    return ready

def dag_reset(run_id: str) -> None:
    """重置 DAG 状态。"""
    _DAG_RUNNING.pop(run_id, None)


