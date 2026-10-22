#!/usr/bin/env python3
"""R138 第 1 步：准入只数"在跑"，排队超时必须让父步失败（同一契约）。

现场（需求.md「补充取证（第 17 轮）」）：architect_full_design 子链 q=273、
reviewer q=234 各自困在投递队列；门控把"排队"当"在跑"算进并发额度 →
新子链一律 "subflow skip"、老子链排不进 → 队列冻结。

只放开额度不改超时处置会放大堆积（父步下一 tick 重建一条新子链排同一个队），
所以两条必须同一次提交。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_r138_gate_and_queue_fail.py -q
"""
import json
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import _fail_parent_step  # noqa: E402
from pipeflow.task_generator import MAX_RUNNING_PER_ROLE  # noqa: E402
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402

_SCHEMA = """
CREATE TABLE tasks(task_id TEXT PRIMARY KEY, title TEXT, description TEXT, assigner TEXT,
                   assignee TEXT, status TEXT, created_at REAL, updated_at REAL, template_id TEXT);
CREATE TABLE workflow_instances(instance_id TEXT PRIMARY KEY, template_id TEXT, task_id TEXT,
                   assigner TEXT, assignee TEXT, status TEXT, current_step_id TEXT,
                   step_results TEXT, created_at REAL, parent_wf_id TEXT,
                   subflow_source_step_id TEXT);
"""


def _db(children, parent=None):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    for i, sd in enumerate(children):
        conn.execute(
            "INSERT INTO workflow_instances(instance_id,template_id,task_id,assigner,assignee,"
            "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (f"wf_sub_c{i}", "architect_full_design", "t", "pm", "product_architect",
             "running", "s1", json.dumps(sd), time.time(), "wf_other_parent", "s1"))
    if parent:
        conn.execute(
            "INSERT INTO workflow_instances(instance_id,template_id,task_id,assigner,assignee,"
            "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
            (parent["instance_id"], "WL-parent", "t_p", "pm", "pm",
             parent.get("status", "running"), parent["current_step_id"],
             json.dumps(parent.get("step_results", {})), time.time()))
    conn.commit()
    return conn


class _Tx:
    def __init__(self, conn):
        self._c = conn

    def __enter__(self):
        return self._c

    def __exit__(self, *a):
        return False


class _LM:
    """真实 sqlite 上的最小 lifecycle 代理（_fail_parent_step / _subflow_takeover 用）。"""

    def __init__(self, conn):
        self._conn = conn
        self.fail_step = MagicMock(return_value=False)
        self.close_wf = MagicMock()

    def _conn_tx(self):
        return _Tx(self._conn)

    def query_one(self, sql, params=()):
        return self._conn.execute(sql, params).fetchone()

    def query(self, sql, params=()):
        return self._conn.execute(sql, params).fetchall()

    def execute(self, sql, params=()):
        self._conn.execute(sql, params)
        self._conn.commit()


class _Host(WorkflowLifecycleMixin):
    def __init__(self, lm):
        self._lifecycle = lm
        self._bb = MagicMock()
        self._workflows = {}
        step = MagicMock()
        step.target_role = "product_architect"
        wf = MagicMock()
        wf.steps = [step]
        self._workflows["architect_full_design"] = wf

    def _is_role_busy(self, *a, **kw):
        return False


def _step():
    s = MagicMock()
    s.id = "s1"
    s.target_role = "product_architect"
    s.subflow_template = "architect_full_design"
    s.title = "需求分析"
    s.exit_condition = {}
    s.max_retries = 0
    return s


def test_queued_subflows_do_not_consume_quota():
    """排队中的子链（step 未认领）不占并发额度 —— 否则队列只加深不消化。"""
    queued = {"s1": {"queued": 234, "queued_since": time.time() - 3600, "ts": time.time()}}
    conn = _db([queued] * MAX_RUNNING_PER_ROLE)
    h = _Host(_LM(conn))
    out = h._subflow_takeover(h._lifecycle, "wf_parent", "WL-x", _step(), -1, {}, "s0")
    assert out is True, (
        f"{MAX_RUNNING_PER_ROLE} 条排队子链把额度占满，新子链被 skip —— 队列冻结")


def test_claimed_subflows_consume_quota():
    """已认领（已投递）的子链仍受上限约束，不能无限堆在一个角色上。"""
    claimed = {"s1": {"status": "notified", "notified_at": time.time()}}
    conn = _db([claimed] * MAX_RUNNING_PER_ROLE)
    h = _Host(_LM(conn))
    out = h._subflow_takeover(h._lifecycle, "wf_parent", "WL-x", _step(), -1, {}, "s0")
    assert not out, "已认领子链达上限仍创建新子链"


def _child_db():
    conn = _db([])
    conn.execute(
        "INSERT INTO workflow_instances(instance_id,template_id,task_id,assigner,assignee,"
        "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
        "VALUES ('wf_sub_dead','architect_full_design','t','pm','product_architect',"
        "'running','s1','{}',?,'wf_p','s2')", (time.time(),))
    conn.commit()
    return conn


def test_queue_timeout_fails_parent_step():
    """子链排队超时 → 父步 status=failed + fail_step(父链) + blocker（含证据）。"""
    conn = _child_db()
    conn.execute(
        "INSERT INTO workflow_instances(instance_id,template_id,task_id,assigner,assignee,"
        "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
        "VALUES ('wf_p','WL-p','t_p','pm','product_architect','running','s2',?,?,NULL,NULL)",
        (json.dumps({"s2": {"queued": 44, "queued_since": time.time() - 7200}}), time.time()))
    conn.commit()
    lm = _LM(conn)
    bb = MagicMock()

    out = _fail_parent_step(lm, bb, "wf_sub_dead", "子链排队 120min 未认领")

    assert out == "wf_p"
    sr = json.loads(conn.execute(
        "SELECT step_results FROM workflow_instances WHERE instance_id='wf_p'").fetchone()[0])
    assert sr["s2"]["status"] == "failed", "父步没有落 failed，父链仍在空等"
    assert sr["s2"]["child_failed_at"] > 0
    assert lm.fail_step.call_args[0][:2] == ("wf_p", "s2"), lm.fail_step.call_args
    assert "排队" in lm.fail_step.call_args[1]["reason"]
    bb.write.assert_called_once()
    assert bb.write.call_args[0][0] == "blocker"
    assert "wf_sub_dead" in bb.write.call_args[1]["evidence"]


def test_stale_parent_signal_is_ignored():
    """父步已推进到下一步：过时的失败信号不得砍整条父链。"""
    conn = _child_db()
    conn.execute(
        "INSERT INTO workflow_instances(instance_id,template_id,task_id,assigner,assignee,"
        "status,current_step_id,step_results,created_at,parent_wf_id,subflow_source_step_id) "
        "VALUES ('wf_p','WL-p','t_p','pm','qa','running','s3','{}',?,NULL,NULL)",
        (time.time(),))
    conn.commit()
    lm = _LM(conn)
    bb = MagicMock()

    _fail_parent_step(lm, bb, "wf_sub_dead", "子链排队超时")

    assert not lm.fail_step.called, "父步已推进，仍被旧子链失败信号砍掉"
    assert not bb.write.called


def test_tick_queue_timeout_also_fails_parent():
    """step_handler 的排队 auto-cancel 同样要判父步（两条排队路径同源）。"""
    from pipeflow.engine import Step, WorkflowRun
    from pipeflow.step_handler import StepHandlerMixin

    class _Eng(StepHandlerMixin):
        def __init__(self):
            self._lifecycle = MagicMock()
            self._lifecycle.query_one.return_value = None
            self._bb = MagicMock()
            self._send_to_role = MagicMock(return_value=True)
            self._sync_step_results = MagicMock()
            self._validate_exit_schema = MagicMock(return_value=(True, []))
            self._has_live_subflow = MagicMock(return_value=False)
            self._workflows = {}

        def _eff_role(self, run, step):
            return step.target_role

        def _is_role_busy(self, role, exclude_wf_id=None):
            return True

        def _check_exit(self, *a, **kw):
            return (None, [])

    run = WorkflowRun(id="wf_sub_q", workflow_name="tpl", context={},
                      current_step="s1", status="running", created_at=time.time())
    run.step_results = {"s1": {"status": "", "queued": 30,
                               "queued_since": time.time() - 1900, "ts": time.time() - 1900}}
    step = Step(id="s1", title="t", target_role="engineer", prompt_template="t",
                exit_condition={"bus_category": "notice", "timeout_minutes": 0.001})

    eng = _Eng()
    with patch("pipeflow.step_handler.get_conversation_signal", return_value=None), \
         patch("pipeflow.step_handler._role_producing_recently", return_value=False), \
         patch("pipeflow.step_handler._fail_parent_step") as fail_parent:
        eng._tick(run, step)

    assert fail_parent.called, "排队 auto-cancel 只回收了子链，父步无人处置"
    assert fail_parent.call_args[0][2] == "wf_sub_q"
    assert "排队" in fail_parent.call_args[1]["reason"]
