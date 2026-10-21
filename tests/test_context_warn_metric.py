"""context_warn 度量与冷却（ccs-monitor #429090）。

证伪的三点，每条一条回归：
  1. pane 的 `↓ Nk tokens` 不是上下文占用（按轮归零；实测 archivist pane 4.7k
     vs 真实 163k、maintainer pane 203k vs 真实 161k）→ 改用 transcript 的
     last assistant usage。
  2. 旧实现只在 pane 有 spinner 行时才进阈值分支 → IDLE 的大上下文角色
     （archivist/devops/reviewer/codex-dev/pm）全成盲区，从未告警。
  3. 冷却在进程内 dict → daemon 重启即清零，实测同角色两次告警间隔 84s。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_context_warn_metric.py -q
"""
import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipeflow import role_lifecycle as rl  # noqa: E402
from pipeflow.engine import WorkflowEngine, _WORKFLOWS_DIR  # noqa: E402


def _transcript(root: Path, role: str, usage: dict) -> Path:
    """在注入的 transcript 根下造一个角色会话，末条为带 usage 的 assistant。"""
    d = root / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "0000-test.jsonl"
    lines = [
        json.dumps({"type": "user", "message": {"content": "hi"}}),
        json.dumps({"type": "assistant", "message": {
            "usage": {"input_tokens": 1, "cache_read_input_tokens": 0,
                      "cache_creation_input_tokens": 0}}}),
        json.dumps({"type": "assistant", "message": {"usage": usage}}),
        json.dumps({"type": "user", "message": {"content": "tail"}}),
    ]
    f.write_text("\n".join(lines) + "\n")
    return f


def _engine(tmp_path, monkeypatch):
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    eng._lm.query.return_value = []
    eng._last_compact = {}
    monkeypatch.setattr(rl, "_TRANSCRIPT_ROOT", tmp_path)
    monkeypatch.setattr(eng, "_CONTEXT_WARN_STATE", tmp_path / "context_warn.json")
    eng._bb = MagicMock()
    return eng


def _run(eng, pane_text="", sessions="ccs-archivist"):
    def fake(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout=sessions + "\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=pane_text)
        return MagicMock(returncode=0, stdout="", stderr="")
    with patch("pipeflow.engine._sp.run", side_effect=fake):
        eng._check_context_overflow()


def test_transcript_usage_is_the_metric(tmp_path, monkeypatch):
    """真实占用 163k、pane 显示 4.7k → 必须按 163k 告警（旧实现按 pane 判，漏报）。"""
    _transcript(tmp_path, "archivist", {"input_tokens": 12,
                                        "cache_read_input_tokens": 160000,
                                        "cache_creation_input_tokens": 3000})
    eng = _engine(tmp_path, monkeypatch)
    _run(eng, pane_text="Orchestrating… (1m 4s · ↓ 4.7k tokens)")
    assert eng._bb.write.called, "真实 163k 未告警 —— 仍在用 pane 计数？"
    body = eng._bb.write.call_args[0][1]
    assert "163k" in body, f"告警值应来自 transcript: {body}"
    assert "@archivist" in body, f"标题缺 @role，unread 不会命中: {body}"
    assert eng._bb.write.call_args[0][0] == "notice", "应写 notice 而非 blocker（阻塞类会全角色广播）"


def test_idle_role_without_spinner_still_warns(tmp_path, monkeypatch):
    """pane 无 `↓ k tokens` 行（IDLE）→ 仍要按 transcript 判，这是旧实现的盲区。"""
    _transcript(tmp_path, "devops", {"input_tokens": 5,
                                     "cache_read_input_tokens": 162000,
                                     "cache_creation_input_tokens": 0})
    eng = _engine(tmp_path, monkeypatch)
    _run(eng, pane_text="", sessions="ccs-devops")
    assert eng._bb.write.called, "IDLE 大上下文角色不得被跳过"


def test_pane_inflated_count_does_not_warn(tmp_path, monkeypatch):
    """pane 203k 但真实 81k → 不得告警（旧实现会误报）。"""
    _transcript(tmp_path, "maintainer", {"input_tokens": 1000,
                                         "cache_read_input_tokens": 80000,
                                         "cache_creation_input_tokens": 0})
    eng = _engine(tmp_path, monkeypatch)
    _run(eng, pane_text="↓ 203.0k tokens", sessions="ccs-maintainer")
    assert not eng._bb.write.called, "pane 虚高触发了假告警"


def test_cooldown_survives_restart(tmp_path, monkeypatch):
    """冷却落盘：新实例（模拟 daemon 重启）不得立刻重发。"""
    _transcript(tmp_path, "curator", {"input_tokens": 0,
                                      "cache_read_input_tokens": 164000,
                                      "cache_creation_input_tokens": 0})
    eng1 = _engine(tmp_path, monkeypatch)
    _run(eng1, sessions="ccs-curator")
    assert eng1._bb.write.called
    eng2 = _engine(tmp_path, monkeypatch)   # 全新实例 = 重启，内存状态清零
    _run(eng2, sessions="ccs-curator")
    assert not eng2._bb.write.called, "重启后冷却被清零，同一角色立即重复告警"


def test_no_transcript_silently_skips(tmp_path, monkeypatch):
    """取不到 transcript → 静默跳过，不得因缺文件抛穿整轮巡检。"""
    eng = _engine(tmp_path, monkeypatch)
    _run(eng, sessions="ccs-qa")
    assert not eng._bb.write.called


def test_usage_parser_tolerates_broken_lines(tmp_path):
    """坏行/非 assistant 行不得中断解析。"""
    d = tmp_path / "-home-administrator-ccs-workspaces-pg"
    d.mkdir(parents=True)
    f = d / "a.jsonl"
    f.write_text(
        '{"broken"\n'
        + json.dumps({"type": "user", "message": {"usage": {"input_tokens": 999}}}) + "\n"
        + json.dumps({"type": "assistant", "message": {"usage": {
            "input_tokens": 2, "cache_read_input_tokens": 120000,
            "cache_creation_input_tokens": 0}}}) + "\n")
    assert rl._last_assistant_usage_tokens(str(f)) == 120002
