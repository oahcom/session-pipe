#!/usr/bin/env python3
"""
SET 基础设施 — 测试工具库 (Test Helpers)

本文件由 SET (Software Engineer in Test) 维护。
TE (Test Engineer) 使用本模块提供的工具编写测试场景。

职责边界：
  SET → 维护本文件 / CI 配置 / 测试框架
  TE → 使用本文件写测试场景 / 执行探索测试 / 报告 bug
"""

__test__ = False  # 工具库，非测试文件
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

# ── 路径工具 ────────────────────────────────────────────────────

PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
LAUNCHER_SRC = str(Path.home() / "session-launcher" / "src")
HERMES_SCRIPTS = str(Path.home() / ".hermes" / "scripts")

def ensure_imports():
    """确保所有项目路径可导入 (SET: 维护此函数, TE: 无需关心)"""
    for p in [PIPELINE_SRC, LAUNCHER_SRC, HERMES_SCRIPTS]:
        if p not in sys.path:
            sys.path.insert(0, p)


# ── 临时数据库 (SET: 维护, TE: 使用) ────────────────────────────

def tmp_db(module: str = "workflow_db") -> tuple:
    """
    创建临时 SQLite 数据库。

    TE 用法:
        path, db = tmp_db()
        tid = db.create_task("测试任务")
        assert db.get_task(tid) is not None
        db.close()
        os.unlink(path)
    """
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    if module == "workflow_db":
        from pipeflow.db import WorkflowDB
        return f.name, WorkflowDB(f.name)
    raise ValueError(f"未知模块: {module}")


def tmp_wf_dir() -> Path:
    """创建临时工作流定义目录 (含 runs/ 子目录)。"""
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True, exist_ok=True)
    return d


def write_wf_def(wf_dir: Path, name: str, steps: list):
    """写入工作流定义 JSON。"""
    (wf_dir / f"{name}.json").write_text(json.dumps({
        "name": name, "title": name, "description": "", "steps": steps,
    }, ensure_ascii=False))


# ── Bus 消息隔离 (SET: 维护, TE: 使用) ─────────────────────────

def unique_tag() -> str:
    """生成唯一前缀，避免测试消息污染。"""
    return f"TEST_{uuid.uuid4().hex[:8]}"


def write_test_fact(tag: str, cat: str = "notice") -> int:
    """
    写一条测试消息到 Blackboard (使用唯一标签避免与其他测试冲突)。
    返回 fact_id。
    """
    ensure_imports()
    from bus_protocol import Blackboard
    bb = Blackboard()
    fid = bb.write(cat, f"{tag} 测试消息", src="test_engineer")
    return fid


def find_test_facts(tag: str, cat: str = None) -> list:
    """查找所有带有指定标签的测试消息。"""
    ensure_imports()
    from bus_protocol import Blackboard
    bb = Blackboard()
    facts = bb.read(cat=cat) if cat else bb.read()
    return [f for f in facts if tag in f.t]


# ── 测试执行结果记录 (TE 用) ─────────────────────────────────────

class TestSession:
    """
    记录一次测试执行的完整上下文。

    TE 用法:
        session = TestSession("回归测试 v2.1", tester="张三")
        session.record("创建任务", True, "任务 ID: task_abc")
        session.record("状态同步", False, "预期 completed, 实际 in_progress")
        session.summary()
    """

    def __init__(self, title: str, tester: str = "TE"):
        self.title = title
        self.tester = tester
        self.ts = time.time()
        self.results: list[dict] = []
        self.bugs: list[dict] = []

    def record(self, scenario: str, passed: bool, detail: str = ""):
        self.results.append({
            "scenario": scenario, "passed": passed,
            "ts": time.time(), "detail": detail,
        })

    def record_bug(self, title: str, severity: str, steps: str,
                   expected: str, actual: str, root_cause: str = ""):
        self.bugs.append({
            "title": title, "severity": severity, "steps": steps,
            "expected": expected, "actual": actual,
            "root_cause": root_cause, "ts": time.time(),
        })

    def summary(self) -> dict:
        passed = sum(1 for r in self.results if r["passed"])
        failed = len(self.results) - passed
        return {
            "title": self.title, "tester": self.tester,
            "total": len(self.results), "passed": passed,
            "failed": failed, "bugs": self.bugs,
        }


# ── 场景库注册 (SET) ────────────────────────────────────────────

class ScenarioRegistry:
    """
    注册可执行的测试场景。SET 维护此注册表，TE 注册场景。

    TE 用法:
        @register("用户创建后状态为 created")
        def test_user_created():
            ...
    """

    def __init__(self):
        self._scenarios: dict[str, callable] = {}

    def register(self, name: str):
        def decorator(fn):
            self._scenarios[name] = fn
            return fn
        return decorator

    def run(self, name: str) -> bool:
        fn = self._scenarios.get(name)
        if not fn:
            raise KeyError(f"未知场景: {name}")
        fn()
        return True

    def list_scenarios(self) -> list[str]:
        return sorted(self._scenarios.keys())

    def run_all(self) -> dict[str, bool]:
        results = {}
        for name, fn in self._scenarios.items():
            try:
                fn()
                results[name] = True
            except Exception as e:
                results[name] = False
        return results


# SET 单例
scenarios = ScenarioRegistry()


# ── 入口 ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("SET 测试工具库 v1")
    print("  本文件由 SET 维护，TE 通过 import 使用")
    print(f"  可用场景: {scenarios.list_scenarios()}")
