#!/usr/bin/env python3
"""_record_step_output 产出审计功能测试"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def _make():
    mixin = object.__new__(wf_lifecycle.WorkflowLifecycleMixin)
    mixin._bb = FakeBB()
    mixin._lifecycle = MagicMock()
    mixin._workflows = {}
    return mixin


import contextlib
@contextlib.contextmanager
def monkeypatch_home(home: Path):
    _orig = Path.home
    _orig_ccs = wf_lifecycle._CCS_WORKSPACES
    Path.home = staticmethod(lambda: home)
    wf_lifecycle._CCS_WORKSPACES = home / "ccs-workspaces"
    try:
        yield
    finally:
        Path.home = _orig
        wf_lifecycle._CCS_WORKSPACES = _orig_ccs


def _mock_git(stdout=""):
    mock_sp = MagicMock()
    mock_sp.run.return_value = MagicMock(returncode=0, stdout=stdout)
    return patch.object(wf_lifecycle, "_engine_mod", MagicMock(_sp=mock_sp))


def _setup_workspace(home, role, files):
    """helper: 创建角色 workspace 并写入指定文件。"""
    ws = home / "ccs-workspaces" / role
    ws.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (ws / name).write_text(content)
    return ws


# ── 1. 有产出文件时记录到 bus ──
def test_records_output_files():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        _setup_workspace(home, "scout", {"SCOUT_REPORT.md": "# 报告\n发现 3 个工具"})
        mixin = _make()
        step = FakeStep("s1", "scout", "情报生产")
        sdata = {"exit_messages": [{"text": "s1-complete 报告"}]}
        with monkeypatch_home(home), _mock_git():
            mixin._record_step_output("wf_test123", step, sdata)
        assert len(mixin._bb.entries) == 1
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert any("SCOUT_REPORT" in o["f"] for o in d["outputs"])
        assert "s1-complete" in d["exit_messages"][0]


# ── 2. exit_messages 摘要被记录 ──
def test_records_exit_messages_summary():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        _setup_workspace(home, "pm", {"INTAKE.md": "# 需求"})
        mixin = _make()
        step = FakeStep("s2", "pm", "需求审视")
        sdata = {"exit_messages": [{"text": "s2-complete 需求已审视"}]}
        with monkeypatch_home(home), _mock_git():
            mixin._record_step_output("wf_xxx", step, sdata)
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert "s2-complete" in d["exit_messages"][0]


# ── 3. 角色无 workspace 时不崩溃 ──
def test_no_workspace_no_crash():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        (home / "ccs-workspaces").mkdir()
        mixin = _make()
        with monkeypatch_home(home), _mock_git():
            mixin._record_step_output("wf_noop", FakeStep("s1", "nonexistent_role"), {"exit_messages": []})
        assert len(mixin._bb.entries) == 0


# ── 4. step_data 为空时不写 bus ──
def test_empty_step_data_no_write():
    mixin = _make()
    with _mock_git():
        mixin._record_step_output("wf_x", FakeStep("s1", "scout"), {})
    assert len(mixin._bb.entries) == 0


# ── 5. 混合产出文件 + exit_messages 都被记录 ──
def test_mixed_output_and_messages():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        _setup_workspace(home, "engineer", {"main.py": "print('hello')"})
        mixin = _make()
        step = FakeStep("s4", "engineer", "编码实现")
        sdata = {"exit_messages": [{"text": "[s4-complete] 实现完成"}]}
        with monkeypatch_home(home), _mock_git():
            mixin._record_step_output("wf_eng", step, sdata)
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert any("main.py" in o["f"] for o in d["outputs"])
        assert "[s4-complete]" in d["exit_messages"][0]


# ── 6. git 有改动时记录 git_changed ──
def test_git_changed_files_recorded():
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        _setup_workspace(home, "scout", {"note.md": "hi"})
        mixin = _make()
        step = FakeStep("s1", "scout", "情报生产")
        git_out = "M  new_file.py\n?  untracked.txt\n"
        with monkeypatch_home(home), _mock_git(git_out):
            mixin._record_step_output("wf_git", step, {"status": "step_done_ready"})
        d = json.loads(mixin._bb.entries[0][2]["evidence"])
        assert "new_file.py" in d["git_changed"]
        assert "untracked.txt" in d["git_changed"]


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
