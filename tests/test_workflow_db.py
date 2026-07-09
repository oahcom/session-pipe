#!/usr/bin/env python3
"""
WorkflowDB 测试 — 三层数据库 (Template→Instance→Task) 全功能覆盖。

运行：cd /home/administrator/session-pipeline && python3 tests/test_workflow_db.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

from workflow_db import WorkflowDB


def _count_tables(db):
    rows = db._conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return [r["name"] for r in rows]


def _count_rows(db, table):
    return db._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── Schema ───────────────────────────────────────────────────────

def test_schema_creates_all_tables():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tables = _count_tables(db)
    db.close()
    os.unlink(tmppath)
    expected = {"workflow_templates", "workflow_instances", "tasks", "workflow_logs"}
    assert expected.issubset(tables), f"缺少表: {expected - set(tables)}"


def test_context_manager():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    with WorkflowDB(tmppath) as db:
        tid = db.create_task("ctx test")
        assert db.get_task(tid) is not None
    # close 后不应异常
    os.unlink(tmppath)


# ── Template CRUD ────────────────────────────────────────────────

def test_create_and_get_template():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1", "title": "调研", "target_role": "scout"}]}
    tid = db.create_template("test_tmpl", "测试模板", steps)
    t = db.get_template(tid)
    assert t is not None
    assert t["name"] == "test_tmpl"
    assert json.loads(t["steps_json"]) == steps
    db.close()
    os.unlink(tmppath)


def test_find_template_by_name():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": []}
    db.create_template("alpha", "A", steps)
    db.create_template("beta", "B", steps)
    assert db.find_template("alpha") is not None
    assert db.find_template("gamma") is None
    db.close()
    os.unlink(tmppath)


def test_list_templates_ordered():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": []}
    db.create_template("z", "Z", steps)
    db.create_template("a", "A", steps)
    names = [t["name"] for t in db.list_templates()]
    assert names == sorted(names), f"未按 name 排序: {names}"
    db.close()
    os.unlink(tmppath)


def test_delete_template():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": []}
    tid = db.create_template("del_me", "X", steps)
    assert db.get_template(tid) is not None
    db.delete_template(tid)
    assert db.get_template(tid) is None
    db.close()
    os.unlink(tmppath)


# ── Task CRUD ────────────────────────────────────────────────────

def test_create_task_defaults():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("默认任务测试")
    task = db.get_task(tid)
    assert task is not None
    assert task["title"] == "默认任务测试"
    assert task["status"] == "created"
    assert task["assigner"] == "system"
    assert task["priority"] == 0
    assert json.loads(task["tags"]) == []
    assert json.loads(task["context"]) == {}
    db.close()
    os.unlink(tmppath)


def test_create_task_with_all_fields():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("完整任务", description="desc", assigner="alice",
                          assignee="bob", priority=2, tags=["urgent", "P0"],
                          context={"env": "prod"})
    task = db.get_task(tid)
    assert task["assigner"] == "alice"
    assert task["assignee"] == "bob"
    assert task["priority"] == 2
    assert json.loads(task["tags"]) == ["urgent", "P0"]
    assert json.loads(task["context"]) == {"env": "prod"}
    db.close()
    os.unlink(tmppath)


def test_get_task_nonexistent():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    assert db.get_task("not_a_real_task") is None
    db.close()
    os.unlink(tmppath)


def test_list_tasks_filters():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    t1 = db.create_task("A", assigner="alice", assignee="bob")
    t2 = db.create_task("B", assigner="alice", assignee="carol")
    t3 = db.create_task("C", assigner="eve", assignee="bob")
    assert len(db.list_tasks(assignee="bob")) == 2
    assert len(db.list_tasks(assigner="alice")) == 2
    assert len(db.list_tasks(status="created")) == 3
    assert len(db.list_tasks(status="completed")) == 0
    db.close()
    os.unlink(tmppath)


def test_update_task():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("原标题")
    assert db.update_task(tid, title="新标题", priority=5)
    task = db.get_task(tid)
    assert task["title"] == "新标题"
    assert task["priority"] == 5
    db.close()
    os.unlink(tmppath)


def test_update_task_allowed_fields_only():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("A")
    assert db.update_task(tid, invalid_field="x") == False
    db.close()
    os.unlink(tmppath)


def test_update_task_context_serializes():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("A")
    db.update_task(tid, context={"k": "v"})
    raw = db._conn.execute("SELECT context FROM tasks WHERE task_id=?", (tid,)).fetchone()[0]
    assert json.loads(raw) == {"k": "v"}
    db.close()
    os.unlink(tmppath)


def test_delete_task_normal():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("能删的", assigner="alice", assignee="bob")
    # bob(assignee) 可以删
    assert db.delete_task(tid, actor="bob")
    assert db.get_task(tid) is None
    db.close()
    os.unlink(tmppath)


def test_delete_task_assigner_cannot_delete():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    tid = db.create_task("不能删", assigner="alice", assignee="bob")
    # assigner=alice 不能删（非 system）
    assert db.delete_task(tid, actor="alice") == False
    assert db.get_task(tid) is not None
    db.close()
    os.unlink(tmppath)


def test_delete_task_nonexistent():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    assert db.delete_task("ghost", actor="system") == False
    db.close()
    os.unlink(tmppath)


# ── Workflow Instance CRUD ───────────────────────────────────────

def test_create_workflow_from_template():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1", "title": "调研"} for _ in range(2)]}
    tmpl_id = db.create_template("dev", "开发流程", steps)
    task_id = db.create_task("开发任务", assigner="alice", assignee="bob")
    wf_id = db.create_workflow(task_id, "dev", "alice", "bob")
    assert wf_id is not None
    wf = db.get_workflow(wf_id)
    assert wf is not None
    assert wf["status"] == "pending"
    assert wf["current_step_id"] == "s1"  # first step
    # Task 状态自动更新为 in_progress
    task = db.get_task(task_id)
    assert task["status"] == "in_progress"
    assert task["current_workflow_id"] == wf_id
    db.close()
    os.unlink(tmppath)


def test_create_workflow_unknown_template():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    task_id = db.create_task("X", assigner="a", assignee="b")
    assert db.create_workflow(task_id, "non_existent", "a", "b") is None
    db.close()
    os.unlink(tmppath)


def test_get_workflow_nonexistent():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    assert db.get_workflow("ghost") is None
    db.close()
    os.unlink(tmppath)


def test_list_workflows_for_task():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t1", "T1", steps)
    db.create_template("t2", "T2", steps)
    task_id = db.create_task("多工作流", assigner="a", assignee="b")
    w1 = db.create_workflow(task_id, "t1", "a", "b")
    w2 = db.create_workflow(task_id, "t2", "a", "b")
    wfs = db.list_workflows_for_task(task_id)
    assert len(wfs) == 2
    assert {w["instance_id"] for w in wfs} == {w1, w2}
    db.close()
    os.unlink(tmppath)


def test_list_workflows_filter():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    t1 = db.create_task("A", assigner="a", assignee="alice")
    t2 = db.create_task("B", assigner="a", assignee="bob")
    db.create_workflow(t1, "t", "a", "alice")
    db.create_workflow(t2, "t", "a", "bob")
    assert len(db.list_workflows(assignee="alice")) == 1
    assert len(db.list_workflows(assignee="bob")) == 1
    assert len(db.list_workflows(assignee="nobody")) == 0
    db.close()
    os.unlink(tmppath)


def test_update_workflow_advance_status():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}, {"id": "s2"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    wf_id = db.create_workflow(task_id, "t", "a", "b")
    db.update_workflow(wf_id, status="running", current_step_id="s2")
    wf = db.get_workflow(wf_id)
    assert wf["status"] == "running"
    assert wf["current_step_id"] == "s2"
    db.close()
    os.unlink(tmppath)


def test_update_workflow_tasks_sync_on_complete():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    wf_id = db.create_workflow(task_id, "t", "a", "b")
    # 完成工作流
    db.update_workflow(wf_id, status="completed", completed_at=time.time())
    task = db.get_task(task_id)
    assert task["status"] == "completed", f"应同步为 completed: {task['status']}"
    db.close()
    os.unlink(tmppath)


def test_update_workflow_failed_syncs_to_failed():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    wf_id = db.create_workflow(task_id, "t", "a", "b")
    db.update_workflow(wf_id, status="failed")
    task = db.get_task(task_id)
    assert task["status"] == "failed"
    db.close()
    os.unlink(tmppath)


def test_update_workflow_disallowed_fields():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    wf_id = db.create_workflow(task_id, "t", "a", "b")
    assert db.update_workflow(wf_id, title="nope") == False
    db.close()
    os.unlink(tmppath)


def test_delete_workflow():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="alice", assignee="bob")
    wf_id = db.create_workflow(task_id, "t", "alice", "bob")
    # assignee 可以删
    assert db.delete_workflow(wf_id, actor="bob")
    assert db.get_workflow(wf_id) is None
    db.close()
    os.unlink(tmppath)


def test_delete_workflow_assigner_cannot():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="alice", assignee="bob")
    wf_id = db.create_workflow(task_id, "t", "alice", "bob")
    assert db.delete_workflow(wf_id, actor="alice") == False
    assert db.get_workflow(wf_id) is not None
    db.close()
    os.unlink(tmppath)


def test_delete_workflow_nonexistent():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    assert db.delete_workflow("ghost", actor="system") == False
    db.close()
    os.unlink(tmppath)


# ── Chain Workflows ──────────────────────────────────────────────

def test_chain_workflows():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("alpha", "A", steps)
    db.create_template("beta", "B", steps)
    db.create_template("gamma", "C", steps)
    task_id = db.create_task("链式任务", assigner="a", assignee="b")
    ids = db.chain_workflows(task_id, ["alpha", "beta", "gamma"], "a", "b")
    assert len(ids) == 3
    wfs = db.list_workflows_for_task(task_id)
    assert len(wfs) == 3
    db.close()
    os.unlink(tmppath)


def test_chain_workflows_unknown_template_skipped():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("exists", "E", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    ids = db.chain_workflows(task_id, ["exists", "ghost"], "a", "b")
    assert len(ids) == 1  # ghost 被跳过
    db.close()
    os.unlink(tmppath)


# ── Progress Computation ─────────────────────────────────────────

def test_compute_progress_basic():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    prog = db.get_task(task_id)["progress"]
    assert prog["workflow_count"] == 0
    assert prog["completed"] == 0
    # 创建 2 个工作流
    w1 = db.create_workflow(task_id, "t", "a", "b")
    w2 = db.create_workflow(task_id, "t", "a", "b")
    db.update_workflow(w1, status="completed", completed_at=time.time())
    prog = db.get_task(task_id)["progress"]
    assert prog["workflow_count"] == 2
    assert prog["completed"] == 1
    assert prog["percent"] == 50
    db.close()
    os.unlink(tmppath)


# ── Logs ─────────────────────────────────────────────────────────

def test_logs_written_on_operations():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    task_id = db.create_task("日志测试", assigner="alice")
    logs = db.get_logs(task_id=task_id)
    assert len(logs) >= 1
    assert logs[0]["action"] == "created"
    assert logs[0]["actor"] == "alice"
    # 更新应有日志
    db.update_task(task_id, title="改标题")
    logs = db.get_logs(task_id=task_id)
    assert any(l["action"] == "updated" for l in logs)
    db.close()
    os.unlink(tmppath)


def test_get_logs_filter():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    t1 = db.create_task("A", assigner="a")
    t2 = db.create_task("B", assigner="b")
    w1 = db.create_workflow(t1, "t", "a", "c")
    w2 = db.create_workflow(t2, "t", "b", "d")
    assert len(db.get_logs(task_id=t1)) >= 1
    assert len(db.get_logs(workflow_instance_id=w1)) >= 1
    assert len(db.get_logs(task_id="ghost")) == 0
    db.close()
    os.unlink(tmppath)


# ── Integration: 多工作流同步 ────────────────────────────────────

def test_multiple_workflows_one_complete_one_pending():
    """多工作流场景：其中一个完成时 task 仍在 in_progress。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("多工作流任务", assigner="alice", assignee="bob")
    w1 = db.create_workflow(task_id, "t", "alice", "bob")
    w2 = db.create_workflow(task_id, "t", "alice", "bob")
    db.update_workflow(w1, status="completed", completed_at=time.time())
    task = db.get_task(task_id)
    # 还有 w2 在 pending → in_progress
    assert task["status"] == "in_progress", f"应 in_progress: {task['status']}"
    # 完成全部
    db.update_workflow(w2, status="completed", completed_at=time.time())
    task = db.get_task(task_id)
    assert task["status"] == "completed"
    db.close()
    os.unlink(tmppath)


def test_workflow_context_persisted():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    steps = {"steps": [{"id": "s1"}]}
    db.create_template("t", "T", steps)
    task_id = db.create_task("X", assigner="a", assignee="b")
    ctx = {"repo": "myorg/myrepo", "branch": "main", "env": "prod"}
    wf_id = db.create_workflow(task_id, "t", "a", "b", context=ctx)
    wf = db.get_workflow(wf_id)
    assert json.loads(wf["context"]) == ctx
    db.close()
    os.unlink(tmppath)


# ── WAL mode ─────────────────────────────────────────────────────

def test_wal_mode_enabled():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmppath = f.name
    db = WorkflowDB(tmppath)
    mode = db._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal", f"应为 wal: {mode}"
    db.close()
    os.unlink(tmppath)


# ── Main ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== WorkflowDB 测试 ===\n")

    tests = [
        ("Schema 创建所有表", test_schema_creates_all_tables),
        ("Context Manager", test_context_manager),
        ("创建/获取模板", test_create_and_get_template),
        ("按名称查找模板", test_find_template_by_name),
        ("模板列表排序", test_list_templates_ordered),
        ("删除模板", test_delete_template),
        ("创建任务默认值", test_create_task_defaults),
        ("创建任务全字段", test_create_task_with_all_fields),
        ("获取不存在任务", test_get_task_nonexistent),
        ("任务列表筛选", test_list_tasks_filters),
        ("更新任务", test_update_task),
        ("更新仅允许字段", test_update_task_allowed_fields_only),
        ("更新任务 context 序列化", test_update_task_context_serializes),
        ("删除任务正常", test_delete_task_normal),
        ("删除任务下达者不能删", test_delete_task_assigner_cannot_delete),
        ("删除不存在任务", test_delete_task_nonexistent),
        ("从模板创建工作流", test_create_workflow_from_template),
        ("从未知模板创建返回 None", test_create_workflow_unknown_template),
        ("获取不存在工作流", test_get_workflow_nonexistent),
        ("按任务列出工作流", test_list_workflows_for_task),
        ("筛选工作流列表", test_list_workflows_filter),
        ("更新工作流状态", test_update_workflow_advance_status),
        ("工作流完成同步 Task", test_update_workflow_tasks_sync_on_complete),
        ("工作流失败同步 Task", test_update_workflow_failed_syncs_to_failed),
        ("更新工作流不允许字段", test_update_workflow_disallowed_fields),
        ("删除工作流", test_delete_workflow),
        ("删除工作流下达者不能删", test_delete_workflow_assigner_cannot),
        ("删除不存在工作流", test_delete_workflow_nonexistent),
        ("工作流链式创建", test_chain_workflows),
        ("链式创建跳过未知模板", test_chain_workflows_unknown_template_skipped),
        ("进度计算", test_compute_progress_basic),
        ("操作日志写入", test_logs_written_on_operations),
        ("日志筛选", test_get_logs_filter),
        ("多工作流一个完成一个待定", test_multiple_workflows_one_complete_one_pending),
        ("工作流 context 持久化", test_workflow_context_persisted),
        ("WAL 模式启用", test_wal_mode_enabled),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
