#!/usr/bin/env python3
"""_record_step_output 产出审计功能测试"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow import wf_lifecycle


class FakeBB:
    def __init__(self):
        self.entries = []
    def write(self, cat, title, **kw):
        self.entries.append((cat, title, kw))


class FakeStep:
    def __init__(self, sid="s1", role="scout", title="情报生产"):
        self.id = sid
        self.target_role = role
        self.title = title


def _make_instance():
    mixin = object.__new__(wf_lifecycle.WorkflowLifecycleMixin)
    mixin._bb = FakeBB()
    mixin._lifecycle = MagicMock()
    mixin._workflows = {}
    return mixin


# ── 1. 有产出文件时记录到 bus ──
def test_records_output_files():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        ws = home / "ccs-workspaces" / "scout"
        ws.mkdir(parents=True)
        (ws / "SCOUT_REPORT.md").write_text("# 报告\n发现 3 个工具")
        mixin = _make_instance()
        step = FakeStep("s1", "scout", "情报生产")
        sdata = {"status": "step_done_ready", "exit_messages": [{"text": "s1-complete 报告"}]}
        with monkeypatch_home(home):
            mixin._record_step_output("wf_test123", step, sdata)
        assert len(mixin._bb.entries) == 1
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert len(d["outputs"]) > 0
        assert any("SCOUT_REPORT" in o["f"] for o in d["outputs"])


# ── 2. exit_messages 摘要被记录 ──
def test_records_exit_messages_summary():
    mixin = _make_instance()
    step = FakeStep("s2", "pm", "需求审视")
    sdata = {"exit_messages": [{"text": "s2-complete 需求已审视"}]}
    mixin._record_step_output("wf_xxx", step, sdata)
    d = json.loads(mixin._bb.entries[0][2]["evidence"])
    assert "exit_messages" in d
    assert "s2-complete" in d["exit_messages"][0]


# ── 3. 角色无 workspace 时不崩溃 ──
def test_no_workspace_no_crash():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        (home / "ccs-workspaces").mkdir(parents=True)
        mixin = _make_instance()
        step = FakeStep("s1", "nonexistent_role", "测试")
        with monkeypatch_home(home):
            mixin._record_step_output("wf_noop", step, {"exit_messages": []})
        assert len(mixin._bb.entries) == 0  # 无产出，不写 bus


# ── 4. step_data 为空时不写 bus ──
def test_empty_step_data_no_write():
    mixin = _make_instance()
    mixin._record_step_output("wf_x", FakeStep("s1", "scout"), {})
    assert len(mixin._bb.entries) == 0


# ── 5. 混合产出文件 + exit_messages 都被记录 ──
def test_mixed_output_and_messages():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        ws = home / "ccs-workspaces" / "engineer"
        ws.mkdir(parents=True)
        (ws / "main.py").write_text("print('hello')")
        mixin = _make_instance()
        step = FakeStep("s4", "engineer", "编码实现")
        sdata = {"exit_messages": [{"text": "[s4-complete] 实现完成"}]}
        with monkeypatch_home(home):
            mixin._record_step_output("wf_eng", step, sdata)
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert any("main.py" in o["f"] for o in d["outputs"])
        assert "[s4-complete]" in d["exit_messages"][0]


import contextlib

@contextlib.contextmanager
def monkeypatch_home(home: Path):
    """临时将 pathlib.Path.home() 指向 tmp 目录。"""
    _orig = Path.home
    Path.home = staticmethod(lambda: home)
    try:
        yield
    finally:
        Path.home = _orig


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {passed}/{passed+failed} tests passed")
    sys.exit(1 if failed else 0)
