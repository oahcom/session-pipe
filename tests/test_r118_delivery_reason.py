"""投递失败的原因必须在 blocker 标题里可分辨（R118 M11）。

现场：7 天 103 条 blocker 标题同为「投递失败已回退 pending（角色忙或变量未替换）」，
两种根因（排队 vs sender 拒绝）混写，事后无法归因，也就无法决定该调排队预算
还是查未替换变量。
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402


def _host():
    h = WorkflowLifecycleMixin.__new__(WorkflowLifecycleMixin)
    h._bb = MagicMock()
    return h


def test_busy_reason_is_distinguishable():
    h = _host()
    assert "角色忙" in h._DELIVERY_WHY["busy"]
    assert "变量未替换" in h._DELIVERY_WHY["refused"]
    assert h._DELIVERY_WHY["busy"] != h._DELIVERY_WHY["refused"]


def test_recorded_title_carries_reason():
    """走真实记账路径：构造一行 notified 的 step，命中 busy 后标题必须带原因。"""
    h = _host()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = {
        "step_results": '{"s1": {"status": "notified", "notified_at": 1}}'}
    lm = MagicMock()
    lm._conn_tx.return_value.__enter__.return_value = conn
    h._mark_step_delivery_failed(lm, "wf_x", "s1", why="busy")
    title = h._bb.write.call_args[0][1]
    assert "角色忙" in title, title
    assert "或变量未替换" not in title, title
