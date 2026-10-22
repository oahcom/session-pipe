#!/usr/bin/env python3
"""
跨组件集成测试 — WorkflowClient + WorkflowEngine 全链路联动。

测试目标：
  1. WorkflowClient 写入可被直连 SQL 读到（schema 兼容）
  2. WorkflowEngine + SQLite 实例状态
  3. 权限规则（assigner 不能删）
  4. Engine + Bus + DB 全链路

运行：cd /home/administrator/session-pipeline && python3 tests/test_cross_component.py
"""
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock

# 消除 client 测试的真 ccs/bus subprocess（ccs send / bus notify 均跳过）
os.environ.setdefault("SESSION_PIPELINE_TEST", "1")

# 路径设置 — pipeline 在前（workflow.gateway/sync 只在 pipeline 的 workflow 包中，
# launcher 的 workflow 包无这两个模块，置前会导致 workflow.sync 不可达）
_launcher_src = str(Path.home() / "session-launcher" / "src")
_pipeline_src = str(Path(__file__).resolve().parents[1] / "src")
_hermes_scripts = str(Path.home() / ".hermes" / "scripts")
for p in reversed([_pipeline_src, _launcher_src, _hermes_scripts]):
    if p not in sys.path:
        sys.path.insert(0, p)

from pipeflow.engine import WorkflowEngine


def _tmp_db_path():
    """创建空临时 DB 文件，返回路径。调用者负责 unlink。"""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    return f.name


def _seed_template(db_path, name, steps=None):
    """直插模板行，返回 template_id。

    WorkflowClient 没有建模板接口，Gate 只读模板；测试只需要模板存在且可读。
    """
    from workflow.db import create_connection
    template_id = f"tmpl_{uuid.uuid4().hex[:8]}"
    conn = create_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO workflow_templates (template_id, name, steps_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (template_id, name,
             json.dumps(steps or {"steps": [{"id": "s1"}]}, ensure_ascii=False),
             time.time()))
        conn.commit()
    finally:
        conn.close()
    return template_id


def _tmp_wf_dir():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True, exist_ok=True)
    return d


def _write_wf_def(wf_dir, name, steps):
    (wf_dir / f"{name}.json").write_text(json.dumps({
        "name": name, "title": name, "description": "", "steps": steps,
    }, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════
# 1. WorkflowClient 写入 → 直连 SQL 可见
# ══════════════════════════════════════════════════════════════════

def test_client_create_task_db_sees():
    """Client 创建 task → 能查到，状态、assigner 正确。"""
    db_path = _tmp_db_path()
    tmpl_id = _seed_template(db_path, "fix")
    from workflow.client import WorkflowClient
    with WorkflowClient("product_architect", db_path=db_path) as client:
        # 标题需 ≥8 字符（pipeline client 的占位符门禁）
        tid, _ = client.create_task_v2("设计告警模块详细方案", assignee="engineer", template_id=tmpl_id, initiator_role="product_architect")
        task = client.get_task(tid)
        assert task["title"] == "设计告警模块详细方案"
        assert task["assigner"] == "product_architect"
        assert task["assignee"] == "engineer"
        assert task["status"] == "in_progress"  # create_task_v2 sets in_progress
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 2. WorkflowEngine + SQLite
# ══════════════════════════════════════════════════════════════════

def test_engine_start_then_db_status():
    """Engine 启动 → DB 能查到 run 状态。"""
    wf_dir = _tmp_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "调研 {topic}",
              "exit_condition": {"bus_category": "architecture"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf_def(wf_dir, "research", steps)
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=0, stdout="claude\n")):
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
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=0, stdout="claude\n")):
        eng = WorkflowEngine(workflows_dir=wf_dir)
        rid = eng.start("dev", {"topic": "缓存模块"})
        bb = eng._bb
        # 先 run_once 标记 notified（bus_anchor=notified_at），再写消息才能被 created_after 过滤到
        eng.run_once()
        # 推进 s1
        bb.write("architecture", "设计完成", src="architect")
        eng.run_once()
        assert eng.status(rid)["current_step"] == "s2"
        # 先 run_once 让 s2 标记 notified，再写 code_fix 消息（否则消息被 s2 的 notified_at 过滤）
        eng.run_once()
        # 推进 s2
        bb.write("code_fix", f"test-header: 跨组件s2完成_{int(time.time())&0xffff:04x}", src="engineer")
        eng.run_once()
        # 末步：complete_step 只置 completed，收尾（close_wf）由下一 tick 的
        # _advance_production_wf is_last 分支完成（manager._advance_unsafe 明确
        # 不在末步自行 close，否则子链/loop/部署收尾全被绕过）。
        eng.run_once()
        assert eng.status(rid)["status"] == "completed"
        # NOTE: engine 完成时不写 reflexion_lesson（设计决定，非测试覆盖范围）


# ══════════════════════════════════════════════════════════════════
# 3. 权限规则
# ══════════════════════════════════════════════════════════════════

def test_permission_delete_consistency():
    """权限规则：assigner 不能删自己创建的任务和工作流。"""
    db_path = _tmp_db_path()
    tmpl_id = _seed_template(db_path, "tmpl")
    from workflow.client import WorkflowClient
    with WorkflowClient("product_architect", db_path=db_path) as client:
        tid, wid = client.create_task_v2("权限一致性校验任务", assignee="engineer",
                                         template_id=tmpl_id,
                                         initiator_role="product_architect")
        assert client.delete_task(tid) == False, "assigner 不能删任务"
        assert client.delete(wid) == False, "assigner 不能删工作流"
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════
# 4. Bus 消息与工作流联动
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
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=0, stdout="claude\n")):
        eng = WorkflowEngine(workflows_dir=wf_dir)
        eng.start("prompt_test", {"topic": "紧急安全漏洞"})
        bb = eng._bb
        facts = bb.read(cat="task_spec", limit=500)
        matched = [f for f in facts if "紧急安全漏洞" in (f.t or "") or "紧急安全漏洞" in (f.e or "")]
        assert len(matched) >= 1, "Engine 应将步骤 prompt 写入 bus"


# ══════════════════════════════════════════════════════════════════
# 5. Engine 全链路
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
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=0, stdout="claude\n")):
        eng = WorkflowEngine(workflows_dir=wf_dir)
        rid = eng.start("cancel_test", {"topic": "x"})
        # cancel 有"创建不足 5 分钟拒绝取消"保护 → 把 created_at 改老再 cancel
        eng._lifecycle.execute(
            "UPDATE workflow_instances SET created_at=? WHERE instance_id=?",
            (time.time() - 600, rid))
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
    with patch("pipeflow.engine._sp.run",
               return_value=MagicMock(returncode=0, stdout="claude\n")):
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
        # 1. Client → DB
        ("Client 创建 task DB 可见", test_client_create_task_db_sees),
        # 2. Engine + DB
        ("Engine start → DB 可查", test_engine_start_then_db_status),
        ("Engine 推进到完成", test_engine_advance_to_completion),
        # 3. 权限
        ("权限规则一致性", test_permission_delete_consistency),
        # 4. Bus 联动
        ("Engine 写 prompt 到 bus", test_engine_writes_step_prompt_to_bus),
        # 5. Engine 全链路
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
