"""建链实例的 current_step_id 必须取模板首步，不能硬编码 s1。

根因：launcher/pipeline 两份 workflow/client.py 都把 current_step_id 写成
's1'，而 engine 的 start 路径用 wf.steps[0].id。首步为 s0 的模板
（food_chain_loop / WL-food_chain_loop）走客户端建链就跳过 s0——
实测 45 条该模板实例 23 条无 s0 键。
"""
import json

import pytest


def _seed(conn, template_id, first_step):
    steps = [{"step_id": first_step, "title": "t", "type": "handoff",
              "target_role": "engineer", "prompt_template": "p", "max_retries": 0,
              "verify": "", "subflow_template": "", "dual_node": False,
              "exit_condition": {}, "exit_schema": None, "guard": {},
              "failure_patterns": []}]
    conn.execute(
        "INSERT INTO workflow_templates (template_id, name, description, steps_json,"
        " created_at, is_active, allowed_initiators, allowed_executors)"
        " VALUES (?, ?, '', ?, 0, '1', '', ?)",
        (template_id, template_id, json.dumps(steps), '["engineer"]'))
    conn.execute(
        "INSERT INTO tasks (task_id, title, description, assigner, assignee, status,"
        " created_at, updated_at) VALUES ('task_x', 't', '', 'engineer', 'engineer',"
        " 'created', 0, 0)")
    conn.commit()


@pytest.fixture()
def wf(tmp_path):
    from workflow.client import WorkflowClient
    from workflow.db import create_connection
    db = str(tmp_path / "wf.db")
    conn = create_connection(db)
    _seed(conn, "tpl_s0", "s0")
    conn.close()
    client = WorkflowClient("engineer", db_path=db)
    yield client
    client.close()


def test_template_with_s0_first_step_starts_at_s0(wf):
    wf_id = wf._create_workflow_instance("task_x", "tpl_s0", "engineer", "engineer")
    row = wf._conn.execute(
        "SELECT current_step_id FROM workflow_instances WHERE instance_id=?",
        (wf_id,)).fetchone()
    assert row["current_step_id"] == "s0", "首步 s0 被跳过"


def test_missing_template_falls_back_to_s1(wf):
    assert wf._first_step_id("no_such_template") == "s1"
