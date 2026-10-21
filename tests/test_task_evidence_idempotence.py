#!/usr/bin/env python3
"""task_evidence 幂等守卫表征测试。

背景：bus 写入是 ON CONFLICT(t) DO UPDATE（刷新 ts），重复写会把已沉底的
旧 fact 顶回 unread 列表。daemon 每 10s 调 evaluate_and_feedback，任务完成后
1h 内被重评估 ~360 次，实测单条 fact 被写 1091 次（bus 审计 2026-10-19）。

测试完全封闭：DB 用内存库、workspace 用临时目录、bus 写入用假 subprocess。
"""
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import task_evidence as te  # noqa: E402


class _FakeRun:
    """假 subprocess.run：记录 bus 写入，可控 returncode。"""

    def __init__(self):
        self.writes = []
        self.returncode = 0

    def __call__(self, cmd, **kwargs):
        if len(cmd) > 3 and cmd[1] == "write":
            self.writes.append(cmd[3])

        class _R:
            pass

        r = _R()
        r.returncode = self.returncode
        r.stdout = ""
        return r


@pytest.fixture
def env(monkeypatch, tmp_path):
    """隔离环境：假 subprocess + 临时 workspace 根 + 清空签名。"""
    fake = _FakeRun()
    monkeypatch.setattr(te.subprocess, "run", fake)
    calls = []
    monkeypatch.setattr(
        te, "_append_role_feedback", lambda *a, **kw: calls.append(a) or True
    )
    monkeypatch.setattr(te, "WORKSPACES", tmp_path / "ccs-workspaces")
    te._LAST_ASSESSMENT.clear()
    return {"writes": fake.writes, "calls": calls, "fake": fake, "root": tmp_path}


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
    """negative 任务首次写 feedback + self-reflect 共 2 条，同签名重评估全跳过。"""
    _run_once(env)
    first = len(env["writes"])
    _run_once(env)
    second = len(env["writes"]) - first
    assert first == 2, f"首次应写 feedback + self-reflect，实际 {first}"
    assert second == 0, f"同签名重评估应跳过，实际 {second}"


def test_changed_signature_rewrites(env):
    """评估结果变化时应重新写入。"""
    _run_once(env)
    before = len(env["writes"])
    te._LAST_ASSESSMENT["t1"] = "stale_signature"
    _run_once(env)
    assert len(env["writes"]) - before == 2, "签名变化应重写 feedback + self-reflect"


def test_failed_write_is_retried(env):
    """写入失败时签名不落账，下一轮重试（codex P1-1 回归）。

    先记后写会让 bus 锁冲突等瞬时失败在 1h 窗口内永不重试。
    """
    _run_once(env)
    assert env["writes"], "首轮应有写入"

    # 模拟写入失败：清空签名（等价于从未成功落账）后重跑
    te._LAST_ASSESSMENT.clear()
    env["fake"].returncode = 1
    before = len(env["writes"])
    _run_once(env)
    assert len(env["writes"]) > before, "失败后应重试写入"
    assert te._LAST_ASSESSMENT.get("t1") is None, "失败不得落账，否则永不重试"

    # 恢复成功后应正常落账并停止重试
    env["fake"].returncode = 0
    _run_once(env)
    assert te._LAST_ASSESSMENT.get("t1") is not None, "成功后应落账"


def test_tasks_md_feedback_not_skipped(env):
    """签名未变时 TASKS.md 回写仍须每轮执行（该函数自身按 task_id 幂等）。

    回归：早期实现用 `continue` 跳过整个循环体，连带跳过了 negative 复盘分支。
    """
    _run_once(env)
    _run_once(env)
    assert len(env["calls"]) == 2, (
        f"TASKS.md 回写每轮都须执行，实际 {len(env['calls'])} 次"
    )
