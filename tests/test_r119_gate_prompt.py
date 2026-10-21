#!/usr/bin/env python3
"""闸门失败必须直接提示角色 — 只写 bus 时角色不读，返工链挂到超时。

生产证据（2026-10-21）：wf_rw_6f51b0ff030e/s1 18:55:16 写 bus 声明返工完成，
闸门判缺产出 → 只写 blocker 到 bus → 角色没再收到任何提示 → 20:12 仍 notified；
wf_rw_50eb6ec9d6fd 同样挂 3h20m（timeout_count=3），父链停在 s4 rejected。

运行: cd /home/administrator/session-pipeline && python3 tests/test_r119_gate_prompt.py
"""
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from pipeflow.step_handler import StepHandlerMixin  # noqa: E402


def _fake():
    fake = SimpleNamespace()
    fake._bb = MagicMock()
    fake._send_to_role = MagicMock(return_value=True)
    fake._eff_role = MagicMock(return_value="pg")
    return fake


def _run():
    return SimpleNamespace(id="wf_rw_test", workflow_name="engineer_feature", step_results={})


def _step():
    return SimpleNamespace(id="s1", target_role="engineer")


def test_writes_bus_and_prompts_role():
    fake = _fake()
    StepHandlerMixin._gate_prompt(fake, _run(), _step(), "缺产出: IMPL_REPORT-wf_rw_test.md")
    assert fake._bb.write.call_count == 1, "bus 仍要留痕"
    args, kwargs = fake._send_to_role.call_args
    assert args[0] == "pg", f"应提示实际执行者 pg，实际 {args[0]}"
    assert "IMPL_REPORT-wf_rw_test.md" in args[1], "提示必须带缺失文件名"
    assert kwargs.get("force") is True, "闸门提示要越过忙检查"
    print("✅ 闸门失败同时写 bus + 直接提示角色 ✓")


def test_send_failure_does_not_raise():
    fake = _fake()
    fake._send_to_role.side_effect = RuntimeError("ccs send failed")
    StepHandlerMixin._gate_prompt(fake, _run(), _step(), "x")
    assert fake._bb.write.call_count == 1, "提示失败不能吞掉 bus 留痕"
    print("✅ 提示失败不阻断闸门 ✓")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
