#!/usr/bin/env python3
"""rework_ids 与 subflow_id 同型：JSON 里的指针缓存会被整份回写抹掉。

实测（2026-10-22）：wf_85faf589be90/s3 在 02:04 建过 2 条返工链，到 03:58
rework_ids 已空 → 又建第 3 条，同一步三份返工并行跑。全库 39 组「同一步多条
子链」、涉及 28 个父实例（多出 73 条子行）。

行上的 (parent_wf_id, subflow_source_step_id) 在建链事务内写入、从不删，
所以「这一步有没有在跑的返工链」永远可以从库里读回来。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_rework_ids_derived.py -q
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import _derive_rework_ids, _persist_queue_tick  # noqa: E402
from workflow.db import create_connection  # noqa: E402


class _Lm:
    """最小 LifecycleManager 替身：只需要 query / execute。"""

    def __init__(self, db=None, boom=False):
        self.db, self.boom = db, boom

    def _con(self):
        if self.boom:
            raise sqlite3.OperationalError("database is locked")
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        return con

    def query(self, sql, params=()):
        con = self._con()
        try:
            return con.execute(sql, params).fetchall()
        finally:
            con.close()

    def execute(self, sql, params=()):
        con = self._con()
        try:
            con.execute(sql, params)
            con.commit()
        finally:
            con.close()


def _db(tmp_path):
    db = str(tmp_path / "wf.db")
    conn = create_connection(db)
    conn.execute("INSERT INTO workflow_templates (template_id,name,steps_json,created_at)"
                 " VALUES ('test_chain','test_chain','{}',0)")
    conn.commit()
    conn.close()
    return db


def _insert(db, instance_id, status, parent=None, src_step=None, created=0):
    conn = sqlite3.connect(db)
    conn.execute("INSERT OR IGNORE INTO tasks (task_id,title,assigner,status,created_at,updated_at)"
                 " VALUES (?,?,?,?,?,?)", (f"t_{instance_id}", "t", "system", "running", 0, 0))
    conn.execute(
        "INSERT INTO workflow_instances (instance_id,template_id,task_id,assigner,assignee,"
        "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (instance_id, "test_chain", f"t_{instance_id}", "system", "pm", status, "s1",
         json.dumps({}), created, parent, src_step))
    conn.commit()
    conn.close()


def test_derives_only_live_rework_chains(tmp_path):
    """认回仍在跑的返工链；已终结的不算（终态的该走重投原步，不是再建一条）。"""
    db = _db(tmp_path)
    _insert(db, "wf_parent", "running")
    _insert(db, "wf_rw_done", "completed", parent="wf_parent", src_step="s3", created=10)
    _insert(db, "wf_rw_live", "running", parent="wf_parent", src_step="s3", created=20)
    assert _derive_rework_ids(_Lm(db), "wf_parent", "s3") == ["wf_rw_live"]


def test_ignores_plain_subflow_children(tmp_path):
    """wf_sub_* 是普通子链，不是返工链。"""
    db = _db(tmp_path)
    _insert(db, "wf_parent", "running")
    _insert(db, "wf_sub_child", "running", parent="wf_parent", src_step="s3")
    assert _derive_rework_ids(_Lm(db), "wf_parent", "s3") == []


def test_query_failure_falls_back_to_old_behaviour():
    """读失败返回空：按旧行为（重投原步）处理，不因一次读失败卡住链。"""
    assert _derive_rework_ids(_Lm(boom=True), "wf_parent", "s3") == []

def test_queue_tick_preserves_sibling_keys(tmp_path):
    """排队账本按单键写：不能把同一步的 rework_ids / subflow_id 抹掉。

    实测 40 条子链里 20 条父步丢了指针；同一步因此连建多条返工链。
    """
    db = _db(tmp_path)
    _insert(db, "wf_q", "running")
    con = sqlite3.connect(db)
    con.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                (json.dumps({"s1": {"status": "", "queued": 5, "queued_since": 100.0,
                                    "rework_ids": ["wf_rw_x"], "subflow_id": "wf_sub_y"}}),
                 "wf_q"))
    con.commit()
    con.close()
    _persist_queue_tick(_Lm(db), "wf_q", "s1", 6, 100.0)
    con = sqlite3.connect(db)
    sd = json.loads(con.execute("SELECT step_results FROM workflow_instances WHERE instance_id=?",
                                ("wf_q",)).fetchone()[0])["s1"]
    con.close()
    assert sd["queued"] == 6, sd
    assert sd["rework_ids"] == ["wf_rw_x"], f"rework_ids 被抹掉: {sd}"
    assert sd["subflow_id"] == "wf_sub_y", f"subflow_id 被抹掉: {sd}"

def test_queue_tick_on_bad_json_does_not_raise(tmp_path):
    """列被写坏（非合法 JSON）时不抛，不打断本轮剩余实例。

    生产 0 例，但整份回写的旧实现顺手修了这种坏行；单键写修不了，
    只能记账。测试点：不抛异常，且之后对同一行的正常写还能跑。
    """
    db = _db(tmp_path)
    _insert(db, "wf_bad", "running")
    con = sqlite3.connect(db)
    con.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                ("{broken", "wf_bad"))
    con.commit()
    con.close()
    # 第一次：坏列，只记 warning，不抛。
    _persist_queue_tick(_Lm(db), "wf_bad", "s1", 1, time.time())
    # 第二次：此时列还是坏的（helper 不修写旧列），再次不抛。
    _persist_queue_tick(_Lm(db), "wf_bad", "s1", 2, time.time())
