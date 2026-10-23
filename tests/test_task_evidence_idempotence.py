#!/usr/bin/env python3
"""task_evidence 幂等守卫表征测试。

背景：bus 写入是 ON CONFLICT(t) DO UPDATE（刷新 ts），重复写会把已沉底的
旧 fact 顶回 unread 列表。daemon 每 10s 调 evaluate_and_feedback，任务完成后
1h 内被重评估 ~360 次，实测单条 fact 被写 1091 次（bus 审计 2026-10-19）。

测试完全封闭：DB 用内存库、workspace 用临时目录、bus 写入用假 subprocess。
"""
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import task_evidence as te  # noqa: E402


class _FakeRun:
    """假 subprocess.run：记录 bus 写入，按标题子串决定成败。"""

    def __init__(self):
        self.writes = []
        self.returncode = 0
        self.fail_substrings = []

    def __call__(self, cmd, **kwargs):
        if len(cmd) > 3 and cmd[1] == "write":
            self.writes.append(cmd[3])

        class _R:
            pass

        r = _R()
        bad = self.returncode or any(s in cmd[3] for s in self.fail_substrings)
        r.returncode = 1 if bad else 0
        r.stdout = ""
        return r


@pytest.fixture
def env(monkeypatch, tmp_path):
    """隔离环境：假 subprocess + 临时 workspace 根 + 清空签名。"""
    fake = _FakeRun()
    monkeypatch.setattr(te.subprocess, "run", fake)
    monkeypatch.setattr(te, "WORKSPACES", tmp_path / "ccs-workspaces")
    te._LAST_ASSESSMENT.clear()
    return {"writes": fake.writes, "fake": fake, "root": tmp_path}


def _run_once(env, role="pm"):
    """驱动一轮 _evaluate_with（空 workspace → quality=negative）。"""
    ws = env["root"] / "ccs-workspaces" / role
    ws.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE tasks (task_id TEXT, title TEXT, assignee TEXT, "
        "status TEXT, created_at REAL, updated_at REAL)"
    )
    db.execute("CREATE TABLE workflow_instances (task_id TEXT, step_results TEXT)")
    db.execute(
        "INSERT INTO tasks VALUES ('t1','测试任务',?,'completed',?,?)",
        (role, time.time() - 60, time.time() - 30),
    )
    try:
        te._evaluate_with(db)
    finally:
        db.close()


def test_negative_task_writes_once_then_skips(env):
    """negative 任务首次写 feedback + blocker 共 2 条，同签名重评估全跳过。"""
    _run_once(env)
    first = len(env["writes"])
    _run_once(env)
    second = len(env["writes"]) - first
    assert first == 2, f"首次应写 feedback + blocker，实际 {first}"
    assert second == 0, f"同签名重评估应跳过，实际 {second}"


def test_changed_signature_rewrites(env):
    """评估结果变化时应重新写入。"""
    _run_once(env)
    before = len(env["writes"])
    te._LAST_ASSESSMENT["t1"] = "stale_signature"
    _run_once(env)
    assert len(env["writes"]) - before == 2, "签名变化应重写 feedback + blocker"


def test_failed_write_is_retried(env):
    """写入失败时签名不落账，下一轮重试（codex P1-1 回归）。

    先记后写会让 bus 锁冲突等瞬时失败在 1h 窗口内永不重试。
    """
    _run_once(env)
    assert env["writes"], "首轮应有写入"

    te._LAST_ASSESSMENT.clear()
    env["fake"].returncode = 1
    before = len(env["writes"])
    _run_once(env)
    assert len(env["writes"]) > before, "失败后应重试写入"
    assert te._LAST_ASSESSMENT.get("t1") is None, "失败不得落账，否则永不重试"

    env["fake"].returncode = 0
    _run_once(env)
    assert te._LAST_ASSESSMENT.get("t1") is not None, "成功后应落账"


def test_blocker_failure_is_retried(env):
    """feedback 成功、blocker 失败时不得落账，下轮 blocker 必须重试（混合成败）。"""
    env["fake"].fail_substrings = ["零回话零产出"]
    _run_once(env)
    assert te._LAST_ASSESSMENT.get("t1") is None, "部分失败不得落账"

    env["fake"].fail_substrings = []
    before = len(env["writes"])
    _run_once(env)
    assert any("零回话零产出" in w for w in env["writes"][before:]), \
        "下轮应重试 blocker"
    assert te._LAST_ASSESSMENT.get("t1") is not None, "全部成功后才落账"


def test_silent_completion_reports_blocker_not_tasks_md(env):
    """静默完成（角色零回话、零产出）不罚角色：发 blocker，不写 TASKS.md。

    旧契约是"negative 必写 TASKS.md"，但静默完成的 negative 等于让引擎的空推进
    去惩罚角色 —— 实测 7 天 1120 个 completed 步骤里 349 个（31.2%）角色一句话没说，
    且 engineer/maintainer 的产出在代码仓库，按 workspace 目录判结构性恒为 0 文件。
    """
    _run_once(env)
    assert any("零回话零产出" in w for w in env["writes"]), env["writes"]
    assert not any("[self-reflect]" in w for w in env["writes"]), env["writes"]
    assert not (env["root"] / "ccs-workspaces" / "pm" / "TASKS.md").exists()
