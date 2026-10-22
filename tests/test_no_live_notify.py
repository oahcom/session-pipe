#!/usr/bin/env python3
"""测试不得把审批结果通知投给活体角色（回归守卫）。

2026-10-21 实测：13 条「[workflow] 审批结果通知 / 工作流: wf_pull / 步骤: s2」
落进 product_architect 的 pane。wf_pull 是 wd-quality 的测试夹具 id，
说明 confirm_step → _notify_result → 子进程 `ccs send` 这条出口没被隔离
（那个用例自己的注释写着"累计 435 行、4 个角色收到"）。

判据：confirm 期间没有任何子进程参数含「审批结果通知」。
反向验证：删掉 conftest 的 _no_live_approval_notify，本用例必须变红。
Run: cd ~/session-pipeline && python3 -m pytest tests/test_no_live_notify.py -q
"""
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine


def _step(sid, **ov):
    base = {
        "id": sid, "title": sid, "target_role": "test_role",
        "prompt_template": f"do {sid}",
        "exit_condition": {"bus_category": "notice", "text_contains": f"T_{sid}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "review",
    }
    base.update(ov)
    return base


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_confirm_step_sends_no_live_notification(_mock_engine_run, monkeypatch):
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir()
    (d / "notify_iso.json").write_text(json.dumps(
        {"name": "notify_iso", "title": "t", "description": "",
         "steps": [_step("s1"), _step("s2", type="single")]}, ensure_ascii=False))
    eng = WorkflowEngine(d)
    rid = eng.start("notify_iso", {})
    lm = eng._lifecycle
    sr = {"s1": {"status": "step_done_ready", "ts": time.time(),
                 "notified_at": time.time() - 600, "completed_by": "product_architect"}}
    lm.execute("UPDATE workflow_instances SET status='step_done_ready', step_results=? "
               "WHERE instance_id=?", (json.dumps(sr, ensure_ascii=False), rid))

    seen = []

    def spy(args, *a, **k):
        seen.append(args)
        return subprocess.CompletedProcess(args, 0, b"", b"")

    monkeypatch.setattr(subprocess, "run", spy)
    lm.confirm_step(rid, "s1", approved=True, reason="ok")

    leaked = [s for s in seen if "审批结果通知" in str(s)]
    assert not leaked, f"测试向活体角色投递了审批通知: {leaked[:1]}"
