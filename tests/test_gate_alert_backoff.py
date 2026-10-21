#!/usr/bin/env python3
"""闸门告警退避：同一条失败不得每 5 分钟无限重发（角色会反复取证）。"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow.step_handler import StepHandlerMixin


class _H(StepHandlerMixin):
    pass


def test_gate_alert_backoff_then_stop():
    h = _H()
    h._bb = MagicMock()
    now = [1000.0]
    calls = []

    def _send():
        calls.append(1)

    with patch("pipeflow.step_handler.time.time", side_effect=lambda: now[0]):
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 1
        now[0] += 299
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 1, "5 分钟内不得重发"
        now[0] += 2
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 2
        now[0] += 601
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 3
        now[0] += 1201
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 4
        now[0] += 2401
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 5
        now[0] += 4801
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 5, "第 6 次应停发"
        assert h._bb.write.call_count == 1, "停发时应升级一次"
        now[0] += 10000
        h._gate_alert("wf/s4/verify", "missing REVIEW.md", _send)
        assert len(calls) == 5, "停发后不得复活"

        # 新原因 = 新信息，立即播报
        h._gate_alert("wf/s4/verify", "verify traceback", _send)
        assert len(calls) == 6


if __name__ == "__main__":
    test_gate_alert_backoff_then_stop()
    print("PASS: gate alert backoff")
