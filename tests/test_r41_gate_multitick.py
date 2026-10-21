"""R41 返工门禁回归守卫。

背景：门禁判据从「只查 review/gate + 窄词表」放宽为「任意 type + 宽词表」，
为防止放宽过头（误拦正常完成）或放宽不足（驳回被吞），固定两个边界：

- **有分类**（真实 review 产出）→ 驳回必须拦下推进
- **无分类**（handoff/notify 产出不写【系统问题】/【业务问题】）→ 放行，
  否则既不返工也不推进 = 静默卡死
"""
import sys, os, time, signal
sys.path.insert(0, os.path.expanduser("~/session-pipeline/src"))
sys.path.insert(0, os.path.expanduser("~/session-pipeline"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from unittest.mock import patch, MagicMock
from test_workcycle_e2e import _make_env, _write_wf, _step, _cleanup, _TEST_TIMEOUT

_CLS = ("DONE_S1 CHANGES_REQUESTED\n"
        "【业务问题】\n"
        "- P1 | 返回值错误 | chain.py:68")


class TestGateClassification:
    """_has_issue_classification 的判定边界。"""

    @staticmethod
    def _fn():
        from pipeflow.wf_lifecycle import WorkflowLifecycleMixin
        return WorkflowLifecycleMixin._has_issue_classification

    def test_classified_message_detected(self):
        assert self._fn()(_CLS) is True

    def test_system_issue_only_detected(self):
        assert self._fn()(
            "【系统问题】\n- P1 | exit_condition 与 prompt 不一致 | s2 子链") is True

    def test_bare_rejection_not_classified(self):
        """无分类标记 → 放行（fail-open），避免非 review 步骤卡死。"""
        assert self._fn()("DONE_S1 CHANGES_REQUESTED: 3 P1 需修复") is False

    def test_empty(self):
        assert self._fn()("") is False


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_classified_rejection_blocks_across_ticks(mock_run):
    """有分类的驳回：多轮 tick 都不得推进（防单拍漏放）。"""
    from pipeflow.engine import WorkflowEngine
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "gate_r41", [
        _step("s1", type="review",
              exit_condition={"bus_category": "notice", "text_contains": "DONE_S1",
                              "timeout_minutes": 30}),
        _step("s2", type="single"),
    ])
    eng = WorkflowEngine(d)
    rid = eng.start("gate_r41", {})
    eng.run_once()
    eng._bb.write("notice", _CLS, src="test_role")
    try:
        for _ in range(5):
            time.sleep(0.05)
            eng.run_once()
            s = eng.status(rid)
            assert s["current_step"] == "s1", (
                f"有分类的驳回被放行到 {s['current_step']}")
    finally:
        signal.alarm(0)
        _cleanup(d)
