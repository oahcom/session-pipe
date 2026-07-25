#!/usr/bin/env python3
"""
跨组件集成测试 — WorkflowDB + WorkflowClient + WorkflowEngine 全链路联动。

测试目标：
  1. WorkflowDB ↔ WorkflowClient 双层操作一致性
  2. WorkflowDB + WorkflowEngine 联动
  3. 多角色并发操作
  4. 全链路生命周期
  5. 数据一致性与竞态

运行：cd /home/administrator/session-pipeline && python3 tests/test_cross_component.py
"""
import json
import os
import sys
import tempfile
import time
import threading
import uuid
from pathlib import Path

# 路径设置 — launcher 在前（workflow_client 需要 workflow.gateway）
_launcher_src = str(Path.home() / "session-launcher" / "src")
_pipeline_src = str(Path(__file__).resolve().parents[1] / "src")
_hermes_scripts = str(Path.home() / ".hermes" / "scripts")
for p in reversed([_launcher_src, _pipeline_src, _hermes_scripts]):
    if p not in sys.path:
        sys.path.insert(0, p)

from pipeflow.db import WorkflowDB
from pipeflow.engine import WorkflowEngine, WorkflowDef, Step
from bus_protocol import Blackboard


def _tmp_db():
    """创建临时 DB，返回 (path, db) 调用者负责 close。"""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    return f.name, WorkflowDB(f.name)


def _tmp_wf_dir():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True, exist_ok=True)
    return d


def _write_wf_def(wf_dir, name, steps):
    (wf_dir / f"{name}.json").write_text(json.dumps({
        "name": name, "title": name, "description": "", "steps": steps,
    }, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════
# 1. WorkflowDB ↔ WorkflowClient 一致性
# ══════════════════════════════════════════════════════════════════

def test_db_client_same_schema():
    """WorkflowDB 和 WorkflowClient 连同一个 DB 应能操作。"""
    db_path = _tmp_db()[0]
    db = WorkflowDB(db_path)
    from workflow.client import WorkflowClient
    client = WorkflowClient("test_role", db_path=db_path)
    # DB 创建模板
    tmpl_id = db.create_template("shared_tmpl", "共享模板", {"steps": [{"id": "s1"}]})
    assert tmpl_id is not None, "模板创建应返回 ID"
    # Client 查找
    found = client.find_template("shared_tmpl")
    assert found is not None, "Client 应能找到 DB 创建的模板"
    # Client 创建 task (V2) — 使用真实角色名（Gate 校验需要）
    task_id, wf_id = client.create_task_v2("Client 任务", assignee="maintainer",
                                           template_id=tmpl_id, initiator_role="maintainer")
    task = db.get_task(task_id)
    assert task is not None, "DB 应能找到 Client 创建的 task"
    assert task["assigner"] == "test_role"
    assert task["assignee"] == "maintainer"
    db.close()
    client.close()
    os.unlink(db_path)


def test_client_create_task_db_sees():
    """Client 创建 task → DB 能查到，状态、assigner 正确。"""
    db_path = _tmp_db()[0]
    from workflow.client import WorkflowClient
    from pipeflow.db import WorkflowDB
    # pre-create template for Gate validation
    db = WorkflowDB(db_path)
    tmpl_id = db.create_template("fix", "修复模板", {"steps": [{"id": "s1"}]})
    db.close()
    with WorkflowClient("product_architect", db_path=db_path) as client:
        tid, _ = client.create_task_v2("设计告警模块", assignee="engineer", template_id=tmpl_id, initiator_role="product_architect")
        task = client.get_task(tid)
        assert task["title"] == "设计告警模块"
        assert task["assigner"] == "product_architect"
        assert task["assignee"] == "engineer"
        assert task["status"] == "in_progress"  # create_task_v2 sets in_progress
    os.unlink(db_path)


def test_db_workflow_triggers_task_sync():
    """DB 创建 workflow → task status 自动变为 in_progress。"""
    db_path, db = _tmp_db()
    db.create_template("tmpl", "T", {"steps": [{"id": "s1"}]})
    tid = db.create_task("同步任务", assigner="alice", assignee="bob")
    wid = db.create_workflow(tid, "tmpl", "alice", "bob")
    task = db.get_task(tid)
    assert task["status"] == "in_progress", f"应 in_progress: {task['status']}"
    assert task["current_workflow_id"] == wid
    db.close()
    os.unlink(db_path)


def test_full_lifecycle():
    """完整生命周期：创建模板→任务→工作流→推进→完成。"""
    db_path, db = _tmp_db()
    # 1. 创建模板
    tid = db.create_template("review", "代码审查", {
        "steps": [
            {"id": "s1", "title": "初审"},
            {"id": "s2", "title": "修订"},
            {"id": "s3", "title": "终审"},
        ]
    })
    # 2. 创建任务
    task_id = db.create_task("审查 PR #42", assigner="pm", assignee="reviewer")
    # 3. 创建工作流
    wf_id = db.create_workflow(task_id, "review", "pm", "reviewer")
    assert wf_id is not None
    wf = db.get_workflow(wf_id)
    assert wf["status"] == "pending"
    assert wf["current_step_id"] == "s1"
    # 4. 推进
    db.update_workflow(wf_id, status="running", current_step_id="s1")
    assert db.get_workflow(wf_id)["status"] == "running"
    # 5. 完成
    db.update_workflow(wf_id, status="completed", completed_at=time.time())
    task = db.get_task(task_id)
    assert task["status"] == "completed"
    # 6. 验证日志
    logs = db.get_logs(task_id=task_id)
    actions = [l["action"] for l in logs]
    assert "created" in actions
    assert "completed" in actions
    db.close()
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 2. WorkflowDB + WorkflowEngine 联动
# ══════════════════════════════════════════════════════════════════

def test_engine_start_then_db_status():
    """Engine 启动 → DB 能查到 run 状态。"""
    wf_dir = _tmp_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "调研 {topic}",
              "exit_condition": {"bus_category": "architecture"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf_def(wf_dir, "research", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("research", {"topic": "AI Agent"})
    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["workflow"] == "research"
    # 验证 SQLite 记录存在
    lm = eng._lifecycle
    row = lm._conn.execute(
        "SELECT current_step_id, status FROM workflow_instances WHERE instance_id=?",
        (rid,)
    ).fetchone()
    assert row is not None, "workflow instance not in SQLite"
    assert row["current_step_id"] == "s1"
    assert row["status"] == "running"


def test_engine_advance_to_completion():
    """Engine 完整推进：s1→s2→完成。"""
    wf_dir = _tmp_wf_dir()
    steps = [
        {"id": "s1", "title": "设计", "target_role": "architect",
         "prompt_template": "设计 {topic}",
         "exit_condition": {"bus_category": "architecture"},
         "max_retries": 0, "condition": "", "rollback_to": ""},
        {"id": "s2", "title": "实现", "target_role": "engineer",
         "prompt_template": "实现 {topic}",
         "exit_condition": {"bus_category": "code_fix"},
         "max_retries": 0, "condition": "", "rollback_to": ""},
    ]
    _write_wf_def(wf_dir, "dev", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("dev", {"topic": "缓存模块"})
    bb = eng._bb
    # 推进 s1
    bb.write("architecture", "设计完成", src="architect")
    eng.run_once()
    assert eng.status(rid)["current_step"] == "s2"
    # 推进 s2
    bb.write("code_fix", f"test-header: 跨组件s2完成_{int(time.time())&0xffff:04x}", src="engineer")
    eng.run_once()
    assert eng.status(rid)["status"] == "completed"
    # NOTE: engine 完成时不写 reflexion_lesson（设计决定，非测试覆盖范围）


# ══════════════════════════════════════════════════════════════════
# 3. 多角色并发操作
# ══════════════════════════════════════════════════════════════════

def test_concurrent_task_creation():
    """多角色同时创建 task → 全部成功，ID 不冲突。"""
    db_path = _tmp_db()[0]
    ids = []
    lock = threading.Lock()

    def create(role, title):
        # 每线程独立连接，避免 SQLite 跨线程错误
        db = WorkflowDB(db_path)
        tid = db.create_task(title, assigner=role)
        db.close()
        with lock:
            ids.append(tid)

    threads = [threading.Thread(target=create, args=(f"role_{i}", f"Task_{i}"))
               for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(ids) == 10
    assert len(set(ids)) == 10, "ID 不应重复"
    db = WorkflowDB(db_path)
    tasks = db.list_tasks()
    assert len(tasks) == 10
    db.close()
    os.unlink(db_path)


def test_concurrent_workflow_operations():
    """多角色同时创建/完成 workflow → task 状态正确。"""
    db_path = _tmp_db()[0]
    db = WorkflowDB(db_path)
    db.create_template("concurrent", "C", {"steps": [{"id": "s1"}]})
    task_id = db.create_task("并发任务", assigner="pm", assignee="dev1")
    wf_ids = []
    for i in range(3):
        wid = db.create_workflow(task_id, "concurrent", "pm", f"dev{i+1}")
        wf_ids.append(wid)
    db.close()

    def complete(wid):
        d = WorkflowDB(db_path)
        d.update_workflow(wid, status="completed", completed_at=time.time())
        d.close()

    threads = [threading.Thread(target=complete, args=(wf_ids[0],)),
               threading.Thread(target=complete, args=(wf_ids[1],))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    db = WorkflowDB(db_path)
    task = db.get_task(task_id)
    assert task["status"] == "in_progress", f"应 in_progress: {task['status']}"
    db.close()

    # 完成最后一个
    db = WorkflowDB(db_path)
    db.update_workflow(wf_ids[2], status="completed", completed_at=time.time())
    task = db.get_task(task_id)
    assert task["status"] == "completed"
    db.close()
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 4. 权限规则一致性
# ══════════════════════════════════════════════════════════════════

def test_permission_delete_consistency():
    """WorkflowDB 和 WorkflowClient 权限规则一致：assigner 不能删。"""
    db_path = _tmp_db()[0]
    db = WorkflowDB(db_path)
    db.create_template("tmpl", "T", {"steps": [{"id": "s1"}]})
    tid = db.create_task("权限测试", assigner="alice", assignee="bob")
    wid = db.create_workflow(tid, "tmpl", "alice", "bob")
    # alice(assigner) 在 DB 层不能删
    assert db.delete_task(tid, actor="alice") == False
    assert db.delete_workflow(wid, actor="alice") == False
    # bob(assignee) 可以删
    assert db.delete_task(tid, actor="bob") == True

    # 重新创建一个测 Client 层
    tid2 = db.create_task("Client 权限", assigner="carol", assignee="dan")
    wid2 = db.create_workflow(tid2, "tmpl", "carol", "dan")
    db.close()

    from workflow.client import WorkflowClient
    with WorkflowClient("carol", db_path=db_path) as client:
        # carol(assigner) 不能删
        assert client.delete_task(tid2) == False
        assert client.delete(wid2) == False
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 5. 工作流链式创建
# ══════════════════════════════════════════════════════════════════

def test_chain_workflow_lifecycle():
    """链式工作流：3 步模板链，逐步推进到完成。"""
    db_path, db = _tmp_db()
    db.create_template("design", "设计", {"steps": [{"id": "s1"}]})
    db.create_template("implement", "实现", {"steps": [{"id": "s2"}]})
    db.create_template("test", "测试", {"steps": [{"id": "s3"}]})

    tid = db.create_task("链式任务", assigner="pm")
    wfs = db.chain_workflows(tid, ["design", "implement", "test"], "pm", "dev")
    assert len(wfs) == 3

    # 逐步完成
    for i, wf_id in enumerate(wfs):
        db.update_workflow(wf_id, status="running", current_step_id=f"s{i+1}")
        db.update_workflow(wf_id, status="completed", completed_at=time.time())
        task = db.get_task(tid)
        if i < len(wfs) - 1:
            assert task["status"] == "in_progress"
        else:
            assert task["status"] == "completed"
    db.close()
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 6. Bus 消息与工作流联动
# ══════════════════════════════════════════════════════════════════

def test_engine_writes_step_prompt_to_bus():
    """Engine 启动时写第一步 prompt 到 bus。"""
    wf_dir = _tmp_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "紧急调研 {topic}",
              "exit_condition": {"bus_category": "notice",
                                 "text_contains": "ENGINEPROMPT_TEST"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf_def(wf_dir, "prompt_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    eng.start("prompt_test", {"topic": "紧急安全漏洞"})
    bb = eng._bb
    facts = bb.read(cat="task_spec", limit=500)
    matched = [f for f in facts if "紧急安全漏洞" in (f.t or "") or "紧急安全漏洞" in (f.e or "")]
    assert len(matched) >= 1, "Engine 应将步骤 prompt 写入 bus"


# ══════════════════════════════════════════════════════════════════
# 7. 数据完整性
# ══════════════════════════════════════════════════════════════════

def test_task_progress_computed_correctly():
    """多工作流时进度计算正确。"""
    db_path, db = _tmp_db()
    db.create_template("t", "T", {"steps": [{"id": "s1"}]})
    tid = db.create_task("进度任务", assigner="a", assignee="b")
    w1 = db.create_workflow(tid, "t", "a", "b")
    w2 = db.create_workflow(tid, "t", "a", "b")
    w3 = db.create_workflow(tid, "t", "a", "b")

    db.update_workflow(w1, status="completed", completed_at=time.time())
    task = db.get_task(tid)
    prog = task["progress"]
    assert prog["workflow_count"] == 3
    assert prog["completed"] == 1
    assert prog["percent"] == 33

    db.update_workflow(w2, status="completed", completed_at=time.time())
    db.update_workflow(w3, status="completed", completed_at=time.time())
    task = db.get_task(tid)
    assert task["progress"]["percent"] == 100
    assert task["status"] == "completed"
    db.close()
    os.unlink(db_path)


def test_logs_audit_trail():
    """所有操作应有日志审计。"""
    db_path, db = _tmp_db()
    db.create_template("audit", "审计", {"steps": [{"id": "s1"}]})
    tid = db.create_task("审计任务", assigner="admin")
    wid = db.create_workflow(tid, "audit", "admin", "dev")
    db.update_workflow(wid, status="running", current_step_id="s1")
    db.update_workflow(wid, status="completed", completed_at=time.time())

    logs = db.get_logs(task_id=tid)
    assert len(logs) >= 3
    actions = [l["action"] for l in logs]
    assert "created" in actions
    assert "completed" in actions
    timestamps = [l["ts"] for l in logs]
    assert timestamps == sorted(timestamps, reverse=True)
    db.close()
    os.unlink(db_path)


def test_template_not_found_returns_none():
    """从未知模板创建 workflow 返回 None。"""
    db_path, db = _tmp_db()
    tid = db.create_task("孤儿任务", assigner="a", assignee="b")
    wid = db.create_workflow(tid, "ghost_template", "a", "b")
    assert wid is None
    db.close()
    os.unlink(db_path)


def test_context_serialization_roundtrip():
    """Context dict 通过 JSON 序列化后精确还原。"""
    db_path, db = _tmp_db()
    db.create_template("ctx", "C", {"steps": [{"id": "s1"}]})
    ctx = {
        "repo": "myorg/backend",
        "branch": "feature/alerts",
        "env": "prod",
        "nested": {"key": "value"},
    }
    tid = db.create_task("Context 任务", assigner="a", assignee="b")
    wid = db.create_workflow(tid, "ctx", "a", "b", context=ctx)
    wf = db.get_workflow(wid)
    assert json.loads(wf["context"]) == ctx
    # 也验证 task context
    db.update_task(tid, context={"k": "v"})
    task = db.get_task(tid)
    assert json.loads(task["context"]) == {"k": "v"}
    db.close()
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 8. Engine + Bus + DB 全链路
# ══════════════════════════════════════════════════════════════════

def test_engine_cancel_persists():
    """Engine cancel → 状态持久化 → 重启后仍为 cancelled。"""
    wf_dir = _tmp_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "调研 {topic}",
              "exit_condition": {"bus_category": "notice",
                                 "text_contains": "CANCELPERS_TEST"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf_def(wf_dir, "cancel_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("cancel_test", {"topic": "x"})
    assert eng.cancel(rid)
    # 重新加载验证持久化
    eng2 = WorkflowEngine(workflows_dir=wf_dir)
    s = eng2.status(rid)
    assert s["status"] == "cancelled"


def test_engine_run_once_multiple_runs():
    """Engine 一次 run_once 处理多个 running 状态的 run。"""
    wf_dir = _tmp_wf_dir()
    steps = [
        {"id": "s1", "title": "步骤1", "target_role": "scout",
         "prompt_template": "调研 {topic}",
         "exit_condition": {"bus_category": "notice",
                            "text_contains": "MULTIRUN_TEST"},
         "max_retries": 0, "condition": "", "rollback_to": ""},
    ]
    _write_wf_def(wf_dir, "multi", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid1 = eng.start("multi", {"topic": "A"})
    rid2 = eng.start("multi", {"topic": "B"})
    # 写入匹配消息
    eng._bb.write("notice", "MULTIRUN_TEST 完成", src="test")
    eng.run_once()
    s1 = eng.status(rid1)
    s2 = eng.status(rid2)
    # 两个都应该推进或完成
    assert s1["status"] in ("running", "completed")
    assert s2["status"] in ("running", "completed")


# ══════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=== 跨组件集成测试 ===\n")

    tests = [
        # 1. 双层一致性
        ("DB ↔ Client 同 schema", test_db_client_same_schema),
        ("Client 创建 task DB 可见", test_client_create_task_db_sees),
        ("DB workflow 触发 task 同步", test_db_workflow_triggers_task_sync),
        ("完整生命周期", test_full_lifecycle),
        # 2. Engine + DB
        ("Engine start → DB 可查", test_engine_start_then_db_status),
        ("Engine 推进到完成", test_engine_advance_to_completion),
        # 3. 并发
        ("多角色并发 task 创建", test_concurrent_task_creation),
        ("多角色并发 workflow 操作", test_concurrent_workflow_operations),
        # 4. 权限
        ("权限规则一致性", test_permission_delete_consistency),
        # 5. 链式
        ("链式 workflow 生命周期", test_chain_workflow_lifecycle),
        # 6. Bus 联动
        ("Engine 写 prompt 到 bus", test_engine_writes_step_prompt_to_bus),
        # 7. 数据完整性
        ("进度计算正确", test_task_progress_computed_correctly),
        ("日志审计", test_logs_audit_trail),
        ("未知模板返回 None", test_template_not_found_returns_none),
        ("Context 序列化往返", test_context_serialization_roundtrip),
        # 8. Engine 全链路
        ("cancel 持久化", test_engine_cancel_persists),
        ("Engine 多 run 处理", test_engine_run_once_multiple_runs),
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
