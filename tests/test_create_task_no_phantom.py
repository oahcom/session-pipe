#!/usr/bin/env python3
"""INSERT OR IGNORE 被忽略时，create_task 必须失败且不广播假成功。

事故形态：总线留下 `创建任务: X / task_id=task_53f3b875` 的事实，但全机任何
DB 都没有该 task，调用方以为需求已入链。OR IGNORE 把约束冲突吞成 0 行，
旧代码不校验 rowcount 就 notify。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_create_task_no_phantom.py -q
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def _client(rowcount, is_test=True):
    from workflow.client import WorkflowClient

    c = WorkflowClient.__new__(WorkflowClient)
    c.role = "pm"
    c._TASK_COLUMNS_CACHE = {}
    c._conn = MagicMock()
    c._conn.execute.return_value = MagicMock(rowcount=rowcount)
    c._get_task_columns = lambda: {"template_id"}
    c._log = MagicMock()
    c.notify = MagicMock()
    return c


def test_ignored_insert_raises_and_does_not_notify():
    """rowcount=0（插入被忽略）→ 抛异常，且不写总线事实。"""
    import os
    os.environ["SESSION_PIPELINE_TEST"] = "1"
    c = _client(rowcount=0)
    try:
        c._create_task_impl("给 pm 做一个高质量落地页", "", "product_architect", 0,
                            "WL-full_dev_pipeline")
    except RuntimeError as e:
        assert "未写入" in str(e), f"异常信息应说明未写入: {e}"
    else:
        raise AssertionError("插入被忽略时必须抛异常")
    assert not c.notify.called, "不得广播未落库的任务"


def test_successful_insert_notifies():
    """rowcount=1 → 正常返回 task_id 并写总线事实。"""
    import os
    os.environ["SESSION_PIPELINE_TEST"] = "1"
    c = _client(rowcount=1)
    tid = c._create_task_impl("真实任务", "", "engineer", 0, "WL-full_dev_pipeline")
    assert tid.startswith("task_"), f"应返回 task_id, got {tid!r}"
    assert c._log.called, "落库成功必须写 created 审计"


if __name__ == "__main__":
    test_ignored_insert_raises_and_does_not_notify()
    test_successful_insert_notifies()
    print("OK")
