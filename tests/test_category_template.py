#!/usr/bin/env python3
"""分类合同层测试 — cat 决定模板，未命中回退 recommend()。

事故形态（bus #439027）：qa 的 bug_report 事实按标题 2-gram 打分选模板，
落进首步为 scout 的情报链，跑满 7 步后末步价值审计失败。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_category_template.py -q
"""
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

os.environ["SESSION_PIPELINE_SKIP_TTL_PRUNER"] = "1"
os.environ["SESSION_PIPELINE_TEST"] = "1"

from workflow import client as client_mod
from workflow.client import CATEGORY_TEMPLATE, WorkflowClient
from workflow.db import create_connection

_TPL_SQL = """
INSERT OR REPLACE INTO workflow_templates
    (template_id, name, description, steps_json, trigger_scene,
     allowed_initiators, allowed_executors, max_duration_hours,
     quality_standards, is_active, created_at)
VALUES (?, ?, 'desc', ?, '["编码实现","功能开发","缺陷修复"]',
        '["coordinator","pm","engineer"]', '["engineer"]', 24,
        'test standard with enough chars here', ?, 1234567890)
"""


def _make_client(templates):
    """建临时库 + 模板，返回 (client, db_path)。templates: [(id, is_active)]"""
    db_path = f"/tmp/test_cat_tpl_{os.getpid()}_{id(templates)}.db"
    conn = create_connection(db_path)
    for tid, active in templates:
        conn.execute(_TPL_SQL, (tid, tid, "[]", active))
    conn.commit()
    conn.close()
    return WorkflowClient("coordinator", db_path=db_path), db_path


def test_bug_report_uses_category_template_not_recommend():
    """AC3: 映射命中 → 不调用 recommend()，模板取映射值。"""
    client, db = _make_client([("WF-engineer_feature", 1), ("WL-full_dev_pipeline", 1)])
    try:
        with patch("template_registry.TemplateRegistry.recommend") as rec:
            tid, wid = client.create_task_v2(
                "[bug_report] 引擎三缺陷", assignee="engineer",
                initiator_role="system", bus_category="bug_report")
        assert not rec.called, "命中 cat 时不得再走打分推荐"
        row = client._conn.execute(
            "SELECT template_id FROM workflow_instances WHERE task_id=?", (tid,)).fetchone()
        assert row[0] == CATEGORY_TEMPLATE["bug_report"], f"应为映射模板, got {row[0]}"
        assert wid.startswith("wf_")
    finally:
        client.close()
        os.unlink(db)


def test_unmapped_category_still_recommends():
    """AC4: 未覆盖分类行为不变 — 仍走 recommend() 并取首候选。"""
    client, db = _make_client([("WF-engineer_feature", 1), ("WL-full_dev_pipeline", 1)])
    try:
        with patch("template_registry.TemplateRegistry.recommend",
                   return_value=[{"template_id": "WF-engineer_feature"}]) as rec:
            tid, _ = client.create_task_v2(
                "编码实现", assignee="engineer",
                initiator_role="system", bus_category="code_fix")
        assert rec.called, "未命中映射必须回退 recommend()"
        row = client._conn.execute(
            "SELECT template_id FROM workflow_instances WHERE task_id=?", (tid,)).fetchone()
        assert row[0] == "WF-engineer_feature"
    finally:
        client.close()
        os.unlink(db)


def test_stale_mapping_warns_and_falls_back():
    """AC5: 映射指向不存在/停用的模板 → 不抛异常，warning，回退 recommend()。"""
    client, db = _make_client([("WF-engineer_feature", 1)])
    try:
        with patch.dict(CATEGORY_TEMPLATE, {"bug_report": "WF-does_not_exist"}), \
                patch("template_registry.TemplateRegistry.recommend",
                      return_value=[{"template_id": "WF-engineer_feature"}]) as rec, \
                patch.object(client_mod.LOGGER, "warning") as warn:
            tid, _ = client.create_task_v2(
                "[bug_report] 引擎三缺陷", assignee="engineer",
                initiator_role="system", bus_category="bug_report")
        assert rec.called, "映射失效必须回退 recommend()"
        assert warn.called, "映射失效必须告警"
        row = client._conn.execute(
            "SELECT template_id FROM workflow_instances WHERE task_id=?", (tid,)).fetchone()
        assert row[0] == "WF-engineer_feature"
    finally:
        client.close()
        os.unlink(db)


def test_inactive_template_in_mapping_falls_back():
    """停用的模板与不存在同等对待，不得被映射强推。"""
    client, db = _make_client([("WF-engineer_feature", 0), ("WL-full_dev_pipeline", 1)])
    try:
        with patch("template_registry.TemplateRegistry.recommend",
                   return_value=[{"template_id": "WL-full_dev_pipeline"}]) as rec:
            tid, _ = client.create_task_v2(
                "[bug_report] 引擎三缺陷", assignee="engineer",
                initiator_role="system", bus_category="bug_report")
        assert rec.called, "模板停用时必须回退 recommend()"
        row = client._conn.execute(
            "SELECT template_id FROM workflow_instances WHERE task_id=?", (tid,)).fetchone()
        assert row[0] == "WL-full_dev_pipeline"
    finally:
        client.close()
        os.unlink(db)


if __name__ == "__main__":
    test_bug_report_uses_category_template_not_recommend()
    test_unmapped_category_still_recommends()
    test_stale_mapping_warns_and_falls_back()
    test_inactive_template_in_mapping_falls_back()
    print("OK")
