#!/usr/bin/env python3
"""自动建链门禁 — 首步委派子链的重模板不给需求就不派。

根因（2026-10-21 生产实测）：自动建链只能拿模板简介当需求，
产品交付全流程 / full_dev_pipeline 这类模板第一步就把活交给 pm/architect
子链去拆需求 → 拆无可拆 → 子链自判 REJECTED「无可排需求」→ 返工空转。
实测产品交付全流程 自动建链 4 条：0 completed / 3 cancelled / 1 循环重试。

运行: cd /home/administrator/session-pipeline && python3 tests/test_r118_auto_mint_gate.py
"""
import sys
from pathlib import Path
from types import SimpleNamespace

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pipeflow.task_generator as tg  # noqa: E402


def _wf(*subflows):
    return SimpleNamespace(steps=[SimpleNamespace(subflow_template=s) for s in subflows])


def test_s1_subflow_requires_brief():
    assert tg._requires_user_brief(_wf("pm_requirements", "")) is True, "首步委派子链 → 必须拦"


def test_s1_plain_step_passes():
    assert tg._requires_user_brief(_wf("", "pm_requirements")) is False, "子链在后续步 → 放行"


def test_empty_steps_passes():
    assert tg._requires_user_brief(SimpleNamespace(steps=[])) is False, "无步骤模板不能被误拦"


def test_gate_is_wired():
    src = Path(tg.__file__).read_text(encoding="utf-8")
    assert "_requires_user_brief(_wf)" in src, "门禁函数必须挂在自动建链循环里"


if __name__ == "__main__":
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print(f"PASS {_name}")
