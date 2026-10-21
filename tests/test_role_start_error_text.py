"""R123 回归：角色拉起失败时，blocker 证据必须是真因，不是 stderr 的 INFO 噪声。"""
from pipeflow.role_lifecycle import _start_error_text

INFO = b'2026-10-21 17:53:59,277 [INFO] core: \xe5\x86\x99\xe5\x85\xa5 settings.json mcp_servers=[]\n'
ERR = b'{"success": false, "error": "swap \xe4\xbb\x85\xe5\x89\xa9 102MB"}'


def test_prefers_protocol_json_over_stderr_noise():
    """实测 #434228/#434257/#434307：证据全是 INFO 行，真因在 stdout。"""
    text = _start_error_text(ERR, INFO)
    assert '"success": false' in text and "swap" in text
    assert "INFO" not in text


def test_falls_back_to_stderr_when_stdout_has_no_protocol():
    assert "Traceback" in _start_error_text(b"", b"Traceback: boom")
    assert _start_error_text(None, None) == ""
