"""审批超时兜底 confirm 后，同 tick 的旧快照不得把 completed 覆写回 step_done_ready。

现场（生产 wf_rw_46e42f725e74/s1，2026-10-19）：
  19:33:36 engine.py approval_timeout_fallback 调 confirm_step → 落库 completed
  19:33:58 同一 tick 落到 step_done_ready 分支 → _persist_step_done 用 762 行读的
           旧快照整份 UPDATE step_results → completed 被打回 step_done_ready、
           approval_token 复活，而 current_step_id 已推进 s2
  → 父链 _rework_exit 判链仍在跑（wait），永久卡死 7h

判据：tick 后 DB 里 s1.status 必须是 completed，且 approval_token 必须已被消费。
"""
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.expanduser("~/session-pipeline/src"))
sys.path.insert(0, os.path.expanduser("~/session-pipeline"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from unittest.mock import patch, MagicMock

from test_workcycle_e2e import _make_env, _write_wf, _step, _cleanup, _TEST_TIMEOUT


@patch("subprocess.run", return_value=MagicMock(returncode=0, stdout="claude\n", stderr=b""))
def test_expired_token_fallback_keeps_completed(mock_run):
    from pipeflow.engine import WorkflowEngine
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    try:
        _write_wf(d, "gate_af", [
            _step("s1", type="review"),
            _step("s2"),
        ])
        eng = WorkflowEngine(d)
        rid = eng.start("gate_af", {})
        eng.run_once()

        # 种子：s1 已产出等审批，token 早过 TTL+30min grace（构造 allow_expired 分支）
        _s1 = {
            "status": "step_done_ready",
            "approval_token": "tok_stale_overwrite",
            "approval_token_ttl": time.time() - 3600,
        }
        lm = eng._lifecycle
        lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                   (json.dumps({"s1": _s1}, ensure_ascii=False), rid))

        eng.run_once()

        s1 = eng.status(rid)["results"].get("s1", {})
        assert s1.get("status") == "completed", (
            f"审批兜底后 s1 被旧快照覆写，实际 status={s1.get('status')!r}，"
            f"approval_token={bool(s1.get('approval_token'))}")
        assert not s1.get("approval_token"), "confirm 已消费的 token 不得复活"
    finally:
        # alarm 不拆会在后续用例中途触发 SIGALRM 杀死整个 pytest 进程（实测 EXIT=142）
        signal.alarm(0)
        _cleanup(d)
