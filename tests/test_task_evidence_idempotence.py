#!/usr/bin/env python3
"""task_evidence 幂等守卫表征测试。

背景：bus 写入是 ON CONFLICT(t) DO UPDATE（刷新 ts），重复写会把已沉底的
旧 fact 顶回 unread 列表。daemon 每 10s 调 evaluate_and_feedback，任务完成后
1h 内被重评估 ~360 次，实测单条 fact 被写 1091 次（bus 审计 2026-10-19）。

测试完全封闭：DB 用内存库、workspace 用临时目录、bus 写入用假 subprocess。
"""
import json
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
        self.search_facts = []

    def __call__(self, cmd, **kwargs):
        if len(cmd) > 3 and cmd[1] == "write":
            self.writes.append(cmd[3])

        class _R:
            pass

        r = _R()
        if len(cmd) > 2 and cmd[1] == "search":
            # 只认带 --json 的调用：不带时 bus_client 输出人读文本，解析必然失败。
            # 这样删掉 --json 会立刻红灯，而不是留在假绿里。
            if "--json" in cmd:
                r.stdout = json.dumps({"facts": self.search_facts})
                r.returncode = 0
            else:
                r.stdout = "No results."
                r.returncode = 0
            return r
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


def _run_once(env, role="pm", stored_sig=None):
    """驱动一轮 _evaluate_with（空 workspace → quality=negative）。

    stored_sig 会写进 workflow_logs 并让 _load_assessed 重新播种，用来测"重启后按
    库里的签名判断"这条真实路径（只改内存测不到 os.execv 之后的行为）。
    """
    ws = env["root"] / "ccs-workspaces" / role
    ws.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE tasks (task_id TEXT, title TEXT, assignee TEXT, "
        "status TEXT, created_at REAL, updated_at REAL)"
    )
    db.execute("CREATE TABLE workflow_instances (task_id TEXT, step_results TEXT)")
    # id 自增列必须在：_load_assessed 用 ORDER BY id DESC 取最近记录
    db.execute("CREATE TABLE workflow_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, "
               "task_id TEXT, action TEXT, actor TEXT, detail TEXT, ts REAL)")
    if stored_sig:
        db.execute("INSERT INTO workflow_logs(task_id, action, actor, detail, ts) "
                   "VALUES ('t1','evidence_assessed','task_evidence',?,?)",
                   (stored_sig, time.time()))
        te._LAST_ASSESSMENT.clear()
        te._SEEN_LOADED = False
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
    first = list(env["writes"])
    _run_once(env)
    second = len(env["writes"]) - len(first)
    assert len(first) == 2, f"首次应写 feedback + blocker，实际 {first}"
    assert any("质量: negative" in w for w in first), first
    assert any("零回话零产出" in w for w in first), first
    assert second == 0, f"同签名重评估应跳过，实际多写 {second} 条"


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
    ws = env["root"] / "ccs-workspaces" / "pm"
    ws.mkdir(parents=True, exist_ok=True)
    seeded = "# 工作流任务\n\n## 标题\n旧任务\n"
    (ws / "TASKS.md").write_text(seeded, encoding="utf-8")
    _run_once(env)
    assert any("零回话零产出" in w for w in env["writes"]), env["writes"]
    assert not any("[self-reflect]" in w for w in env["writes"]), env["writes"]
    assert (ws / "TASKS.md").read_text(encoding="utf-8") == seeded, \
        "静默完成不得往 TASKS.md 写教训"


def test_role_bus_fact_counts_as_spoken(env):
    """角色自己在 bus 写的 fact（src == 角色名）算回话 → neutral，不发 blocker。"""
    env["fake"].search_facts = [{"src": "pm", "t": "pm 的进度"}]
    _run_once(env)
    assert not any("零回话零产出" in w for w in env["writes"]), env["writes"]


def test_own_assessment_fact_is_not_evidence(env):
    """本模块自评 fact 不能被当成"角色说过话"，否则 negative 自己翻成 neutral。"""
    env["fake"].search_facts = [{"src": "task_evidence", "t": "[task_evidence] x 质量: negative"}]
    _run_once(env)
    assert any("零回话零产出" in w for w in env["writes"]), env["writes"]


def test_other_role_fact_is_not_evidence(env):
    """别的角色/巡检替该任务写的 fact 不算回话（工程上常这样串味）。"""
    env["fake"].search_facts = [{"src": "coordinator", "t": "P0 超时 violation: task_x"}]
    _run_once(env)
    assert any("零回话零产出" in w for w in env["writes"]), env["writes"]


def test_old_signature_version_is_reassessed(env):
    """库里存的旧版本签名（重启后 _load_assessed 播种）不能挡住重评估。"""
    _run_once(env, stored_sig="negative||3")
    assert env["writes"], "旧版本签名应被当作未评估"
    assert te._LAST_ASSESSMENT["t1"].endswith("|4"), te._LAST_ASSESSMENT


def test_same_version_signature_from_db_is_skipped(env):
    """库里的当前版本签名要生效：重启后不该把同一 fact 再写一遍（写放大）。"""
    _run_once(env)
    sig = te._LAST_ASSESSMENT["t1"]
    before = len(env["writes"])
    _run_once(env, stored_sig=sig)
    assert len(env["writes"]) == before, "同签名重启后应跳过"


def test_feedback_failure_but_blocker_ok_is_retried(env):
    """feedback 失败、blocker 成功：整体不得落账，下轮 feedback 必须重写。"""
    env["fake"].fail_substrings = ["质量: negative"]
    _run_once(env)
    assert te._LAST_ASSESSMENT.get("t1") is None, "部分失败不得落账"
    env["fake"].fail_substrings = []
    before = len(env["writes"])
    _run_once(env)
    assert any("质量: negative" in w for w in env["writes"][before:]), \
        "下轮应重写 feedback"


def test_feedback_title_is_stable(env):
    """标题只含 task_id/角色/质量：带文件数或措辞会让 bus 的 UPSERT 变成插新行。"""
    _run_once(env)
    title = next(w for w in env["writes"] if "质量:" in w)
    assert "t1" in title and "个产出文件" not in title, title
