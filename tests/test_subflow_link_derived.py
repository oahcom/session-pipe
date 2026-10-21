#!/usr/bin/env python3
"""子链链接必须能从 DB 派生，不能只靠 step_results.subflow_id。

事故（2026-10-22）：40 条子链里 20 条父步已无 subflow_id 指针（重置/返工路径会 pop 掉），
子链跑完也没人消费 → 父步停在 ''/step_done_ready 直到 4h 全局 cancel。
活着的一例：wf_85faf589be90/s3 的子链 wf_sub_b40d7745be7c 02:04 就 completed，父步一直没动；
同型 wf_175751a92033/s2（bus #438223 记的「exit_msgs 有、subflow_id 无」）。

子链行上的 (parent_wf_id, subflow_source_step_id) 两列在建链事务里写入、从不删，
扫描端补一条派生路径即可自愈存量实例。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_subflow_link_derived.py -q
"""
import contextlib
import json
import sqlite3
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.task_generator import TaskGeneratorMixin  # noqa: E402
from workflow.db import create_connection  # noqa: E402


class _FakeLifecycle:
    def __init__(self, db):
        self.db = db

    def ping(self):
        return True

    @contextlib.contextmanager
    def _conn_tx(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


class _Gen(TaskGeneratorMixin):
    """只跑扫描路径：_workflows 空 → 契约复算返回空表；advance 记账不真派单。"""

    def __init__(self, db):
        self._lifecycle = _FakeLifecycle(db)
        self._workflows = {}
        self._bb = types.SimpleNamespace(write=lambda *a, **k: None)
        self.advanced = []

    def _advance_production_wf(self, *a, **k):
        self.advanced.append(a)


def _db(tmp_path):
    db = str(tmp_path / "wf.db")
    conn = create_connection(db)
    conn.execute("INSERT INTO workflow_templates (template_id,name,steps_json,created_at)"
                 " VALUES ('test_chain','test_chain','{}',0)")
    conn.commit()
    conn.close()
    return db


def _insert(db, instance_id, template_id, status, current_step, step_results,
            parent=None, src_step=None, created=None):
    conn = sqlite3.connect(db)
    conn.execute("INSERT OR IGNORE INTO tasks (task_id,title,assigner,status,created_at,updated_at)"
                 " VALUES (?,?,?,?,?,?)", (f"t_{instance_id}", "t", "system", "completed", 0, 0))
    conn.execute(
        "INSERT INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,"
        "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (instance_id, template_id, f"t_{instance_id}", "system", "pm", status, current_step,
         json.dumps(step_results), created or 0, parent, src_step))
    conn.commit()
    conn.close()


def _step_status(db, instance_id, step_id):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT step_results FROM workflow_instances WHERE instance_id=?",
                       (instance_id,)).fetchone()
    conn.close()
    return json.loads(row["step_results"]).get(step_id, {}).get("status")


def test_lost_pointer_is_derived_from_child_rows(tmp_path):
    """父步没有 subflow_id，但子链行指向它且已完成 → 仍要消费子链完成。"""
    db = _db(tmp_path)
    _insert(db, "wf_parent", "test_chain", "running", "s1", {"s1": {"queued": 3}})
    _insert(db, "wf_sub_child", "child_tpl", "completed", "c1",
            {"c1": {"status": "completed"}}, parent="wf_parent", src_step="s1")
    gen = _Gen(db)
    gen._scan_tasks()
    assert _step_status(db, "wf_parent", "s1") == "completed", "父步应被推进"
    # advance 走 deferred 队列（本 stub 的 _workflows 为空，不入队），故只断言落库状态。


def test_running_child_is_not_derived(tmp_path):
    """子链还在跑 → 派生不能把父步推进（只有终态好才补链）。"""
    db = _db(tmp_path)
    _insert(db, "wf_parent", "test_chain", "running", "s1", {"s1": {"queued": 3}})
    _insert(db, "wf_sub_child", "child_tpl", "running", "c1",
            {"c1": {"status": "notified"}}, parent="wf_parent", src_step="s1")
    gen = _Gen(db)
    gen._scan_tasks()
    assert _step_status(db, "wf_parent", "s1") != "completed", "子链未完成时不得推进父步"

def test_stale_completed_child_does_not_win_over_running_one(tmp_path):
    """同一步有多条子链：旧的 completed、新的还在跑 → 只能认最新那条，不得推进。"""
    db = _db(tmp_path)
    _insert(db, "wf_parent", "test_chain", "running", "s1", {"s1": {"queued": 3}})
    _insert(db, "wf_sub_old", "child_tpl", "completed", "c1",
            {"c1": {"status": "completed"}}, parent="wf_parent", src_step="s1", created=10)
    _insert(db, "wf_sub_new", "child_tpl", "running", "c1",
            {"c1": {"status": "notified"}}, parent="wf_parent", src_step="s1", created=20)
    gen = _Gen(db)
    gen._scan_tasks()
    assert _step_status(db, "wf_parent", "s1") != "completed", "新子链还在跑，不得用旧链的完成信号推进"

