"""R123 回归：角色拉起失败时，blocker 证据必须是真因，不是 stderr 的 INFO 噪声。"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from pipeflow.role_lifecycle import RoleLifecycleMixin, _start_error_text, _start_failed

INFO = b'2026-10-21 17:53:59,277 [INFO] core: xieshu settings.json mcp_servers=[]\n'
ERR = b'{"success": false, "error": "swap only 102MB"}'


def test_prefers_protocol_json_over_stderr_noise():
    """实测 #434228/#434257/#434307：证据全是 INFO 行，真因在 stdout。"""
    text = _start_error_text(ERR, INFO)
    assert '"success": false' in text and "swap" in text
    assert "INFO" not in text


def test_falls_back_to_stderr_when_stdout_has_no_protocol():
    assert "Traceback" in _start_error_text(b"", b"Traceback: boom")
    assert _start_error_text(None, None) == ""


def test_success_json_does_not_outrank_stderr():
    """成功协议不是失败证据：rc≠0 时选它会把真因藏起来（codex 20 维 P1）。"""
    assert "Traceback" in _start_error_text(b'{"success": true}', b"Traceback: boom")


def test_str_output_is_normalized():
    """子进程 mock/text=True 会给 str：旧 helper 在 'b"..." in str' 处 TypeError，
    被外层 except 吞成"无 blocker"（本轮实测 test_ensure_role_alive_dead_started 失败）。"""
    assert '"success": false' in _start_error_text('{"success": false, "error": "x"}', "INFO")
    assert _start_failed(SimpleNamespace(returncode=0, stdout='{"success": false}')) is True
    assert _start_failed(SimpleNamespace(returncode=0, stdout='{"success": true}')) is False


class _SP:
    """tmux 一律失败；ccs start 返回给定结果。"""

    def __init__(self, start):
        self.start = start
        self.calls = []

    def run(self, cmd, **_kw):
        self.calls.append(cmd)
        if cmd[0] == "tmux":
            return SimpleNamespace(returncode=1, stdout=b"", stderr=b"")
        return self.start


class _Engine(RoleLifecycleMixin):
    def __init__(self, bb):
        self._bb = bb

    def _is_agent_alive(self, role):
        return False

    def _is_role_busy(self, role, exclude_wf_id="", pane_fallback=False):
        return False


def test_call_site_writes_blocker_with_stdout_error(monkeypatch):
    """只测 helper 会漏掉调用点回归：这里断言 blocker 收到的证据。"""
    import pipeflow.engine as engine

    start = SimpleNamespace(returncode=1, stdout=ERR, stderr=INFO)
    monkeypatch.setattr(engine, "_sp", _SP(start))
    monkeypatch.setattr("time.sleep", lambda *_: None)
    bb = MagicMock()

    assert _Engine(bb)._wait_for_role_ready("qa", max_wait=1) is False

    args, kw = bb.write.call_args
    assert args[0] == "blocker" and "rc=1" in args[1]
    assert kw["evidence"].startswith('{"success": false')
