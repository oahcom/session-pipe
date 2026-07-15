#!/usr/bin/env python3
"""
WorkflowDaemon 测试 — 守护进程 socket 推送、轮询、异常处理。

运行：cd /home/administrator/session-pipeline && python3 tests/test_workflow_daemon.py
"""
import json
import os
import socket
import sys
import tempfile
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

import workflow_daemon as daemon


# ── connect_feed ────────────────────────────────────────────────

@patch("socket.socket")
def test_connect_feed_sends_subscribe(mock_socket_cls):
    mock_sock = MagicMock()
    mock_socket_cls.return_value = mock_sock
    s = daemon.connect_feed()
    mock_sock.connect.assert_called_once_with("/tmp/sister_bus_feed.sock")
    mock_sock.sendall.assert_called_once_with(b'{"cmd":"SUBSCRIBE","agent":"feed"}\n')
    assert s is mock_sock


@patch("socket.socket")
def test_connect_feed_sets_timeout(mock_socket_cls):
    mock_sock = MagicMock()
    mock_socket_cls.return_value = mock_sock
    daemon.connect_feed()
    mock_sock.settimeout.assert_called_once_with(5)


# ── push_prompt_to_ccs ──────────────────────────────────────────

@patch("pipeflow.daemon.connect_feed")
def test_push_prompt_sends_publish(mock_connect):
    mock_sock = MagicMock()
    mock_connect.return_value = mock_sock
    ok = daemon.push_prompt_to_ccs("scout", "请调研 topic X")
    assert ok
    sent = mock_sock.sendall.call_args[0][0]
    payload = json.loads(sent.decode())
    assert payload["cmd"] == "PUBLISH"
    assert payload["to"] == "scout"
    assert payload["msg"]["event"] == "workflow_prompt"
    assert payload["msg"]["prompt"] == "请调研 topic X"
    mock_sock.close.assert_called_once()


@patch("pipeflow.daemon.connect_feed")
def test_push_prompt_handles_connection_error(mock_connect):
    mock_connect.side_effect = ConnectionRefusedError("socket not available")
    ok = daemon.push_prompt_to_ccs("dev", "test")
    assert not ok


@patch("pipeflow.daemon.connect_feed")
def test_push_prompt_handles_timeout(mock_connect):
    mock_connect.side_effect = socket.timeout("timeout")
    ok = daemon.push_prompt_to_ccs("dev", "test")
    assert not ok


# ── check_and_push — need mock WorkflowDB ───────────────────────

@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_with_pending(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db

    wf = {
        "instance_id": "wf_abc123",
        "assignee": "scout",
        "template_id": "tmpl_xyz",
    }
    mock_db.list_workflows.return_value = [wf]

    template = {
        "steps_json": json.dumps({
            "steps": [{"id": "s1", "prompt_template": "调研 {topic}"}]
        })
    }
    mock_db.get_template.return_value = template
    wf_with_context = {**wf, "context": '{"topic": "AI"}', "template_id": "tmpl_xyz"}
    mock_db.list_workflows.return_value = [wf_with_context]

    daemon.check_and_push()

    mock_push.assert_called_once()
    mock_db.update_workflow.assert_called_once_with(
        "wf_abc123", status="running", current_step_id="s1"
    )
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_skips_empty(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = []
    daemon.check_and_push()
    mock_push.assert_not_called()
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_no_template_id(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = [{
        "instance_id": "wf_1", "assignee": "dev",
        "template_id": None, "context": "{}"
    }]
    daemon.check_and_push()
    mock_push.assert_not_called()
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_missing_template(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = [{
        "instance_id": "wf_1", "assignee": "dev",
        "template_id": "tmpl_ghost", "context": "{}"
    }]
    mock_db.get_template.return_value = None
    daemon.check_and_push()
    mock_push.assert_not_called()
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_empty_steps(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = [{
        "instance_id": "wf_1", "assignee": "dev",
        "template_id": "tmpl_1", "context": "{}"
    }]
    mock_db.get_template.return_value = {
        "steps_json": json.dumps({"steps": []})
    }
    daemon.check_and_push()
    mock_push.assert_not_called()
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_replaces_context_vars(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = [{
        "instance_id": "wf_ctx",
        "assignee": "dev",
        "template_id": "tmpl_1",
        "context": '{"repo": "myorg/myrepo", "branch": "main"}'
    }]
    mock_db.get_template.return_value = {
        "steps_json": json.dumps({
            "steps": [{"id": "s1", "prompt_template": "Clone {repo} branch {branch}"}]
        })
    }
    daemon.check_and_push()
    prompt = mock_push.call_args[0][1]
    assert "myorg/myrepo" in prompt
    assert "main" in prompt
    mock_db.close.assert_called_once()


@patch("pipeflow.daemon.WorkflowDB")
@patch("pipeflow.daemon.push_prompt_to_ccs")
def test_check_and_push_push_failure_does_not_update(mock_push, mock_db_cls):
    mock_db = MagicMock()
    mock_db_cls.return_value = mock_db
    mock_db.list_workflows.return_value = [{
        "instance_id": "wf_err",
        "assignee": "dev",
        "template_id": "tmpl_1",
        "context": "{}"
    }]
    mock_db.get_template.return_value = {
        "steps_json": json.dumps({
            "steps": [{"id": "s1", "prompt_template": "hello"}]
        })
    }
    mock_push.return_value = False
    daemon.check_and_push()
    # push 失败 → 不应更新状态
    mock_db.update_workflow.assert_not_called()
    mock_db.close.assert_called_once()


# ── daemon_loop (smoke test) ────────────────────────────────────

@patch("pipeflow.daemon.check_and_push")
def test_daemon_loop_interrupt(mock_check):
    """daemon_loop 收到异常后继续运行。"""
    call_count = 0
    def side_effect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("测试异常")
    mock_check.side_effect = side_effect

    t = threading.Thread(target=daemon.daemon_loop, args=(0.05,), daemon=True)
    t.start()
    time.sleep(0.15)
    # 至少调用了 2 次（第1次抛异常，之后继续）
    assert mock_check.call_count >= 2, f"异常后应继续: {mock_check.call_count}"


# ── CLI 参数 ────────────────────────────────────────────────────

def test_cli_defaults():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=int, default=30)
    args = p.parse_args([])
    assert args.interval == 30
    args = p.parse_args(["--interval", "10"])
    assert args.interval == 10


# ── Main ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== WorkflowDaemon 测试 ===\n")

    tests = [
        ("connect_feed 发送 SUBSCRIBE", test_connect_feed_sends_subscribe),
        ("connect_feed 设置 timeout", test_connect_feed_sets_timeout),
        ("push 发送 PUBLISH", test_push_prompt_sends_publish),
        ("push 处理连接拒绝", test_push_prompt_handles_connection_error),
        ("push 处理超时", test_push_prompt_handles_timeout),
        ("check_and_push 有挂起工作流", test_check_and_push_with_pending),
        ("check_and_push 空列表跳过", test_check_and_push_skips_empty),
        ("check_and_push 无模板 ID 跳过", test_check_and_push_no_template_id),
        ("check_and_push 模板不存在跳过", test_check_and_push_missing_template),
        ("check_and_push 空步骤跳过", test_check_and_push_empty_steps),
        ("check_and_push context 变量替换", test_check_and_push_replaces_context_vars),
        ("check_and_push push 失败不更新", test_check_and_push_push_failure_does_not_update),
        ("daemon_loop 异常继续", test_daemon_loop_interrupt),
        ("CLI 默认参数", test_cli_defaults),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
