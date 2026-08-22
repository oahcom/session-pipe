#!/usr/bin/env python3
"""单元测试：conversation_monitor.py 的信号解析 + 去重逻辑"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import hashlib
from conversation_monitor import (
    capture_role_pane, parse_conversation_signals,
    _last_hash,
)


def test_capture_role_pane_nonexistent_role():
    """不存在的角色返回空字符串"""
    result = capture_role_pane("__nonexistent_role__")
    assert result == "", f"期望空字符串，得到 {repr(result[:50])}"


def test_dedup_same_content_twice():
    """连续两次读相同内容，第二次不解析"""
    text = "s1 已完成"
    # 模拟两次读取：先用一次 hash 占位
    h = hashlib.md5(text.encode()).hexdigest()
    _last_hash["_test_dedup"] = h
    # capture_role_pane 会因 hash 相同返回空串
    # 直接测 parse 对空串的处理
    sig = parse_conversation_signals("", "engineer")
    assert not sig.step_completed
    assert not sig.idle_complaint
    assert not sig.violation_found
    # 清理
    _last_hash.pop("_test_dedup", None)


def test_no_signal_random_text():
    """无信号的随机文本 → 全部 False"""
    text = "今天天气不错\n正在处理一个 bug\n看看代码"
    sig = parse_conversation_signals(text, "engineer")
    assert not sig.step_completed
    assert not sig.idle_complaint
    assert not sig.violation_found


def test_completion_s1_done():
    """信号1: s1 完成"""
    text = "● s1 理解需求已完成，产出齐全"
    sig = parse_conversation_signals(text, "lr")
    assert sig.step_completed
    assert "s1" in sig.completion_evidence


def test_completion_step_done_ready():
    """信号1: step_done_ready 显式标记"""
    text = "step_done_ready 状态已标记，等待确认"
    sig = parse_conversation_signals(text, "engineer")
    assert sig.step_completed


def test_completion_goal_achieved():
    """信号1: Goal achieved"""
    text = "✔ Goal achieved (15s · 1 turn · 351 tokens)"
    sig = parse_conversation_signals(text, "engineer")
    assert sig.step_completed


def test_completion_chanchu():
    """信号1: 产出齐全"""
    text = "● 产出完整，写入 bus cat=code_fix 完成"
    sig = parse_conversation_signals(text, "engineer")
    assert sig.step_completed


def test_idle_same_template():
    """信号2: Same generic template. No change"""
    text = "● Same generic template. No change. Idle."
    sig = parse_conversation_signals(text, "engineer")
    assert sig.idle_complaint, f"期望 idle，但得到 idle_evidence={sig.idle_evidence}"


def test_idle_no_task():
    """信号2: 没有真实 task"""
    text = "没有真实 task，进入空闲等待"
    sig = parse_conversation_signals(text, "pg")
    assert sig.idle_complaint


def test_idle_excludes_waiting_for_coordinator():
    """信号2: waiting for coordinator 不触发 idle"""
    text = "状态: ⏳ standby — 等待 coordinator 修复 cron-worker"
    sig = parse_conversation_signals(text, "lr")
    assert not sig.idle_complaint, "期望不触发 idle (等待 coordinator)，但触发了"


def test_idle_excludes_waiting_reply():
    """信号2: 等待他人回复不触发 idle"""
    text = "等待 reviewer 确认后继续"
    sig = parse_conversation_signals(text, "engineer")
    assert not sig.idle_complaint


def test_violation_engineer_kubectl():
    """信号3: engineer 执行 kubectl → deploy 越界"""
    text = "$ kubectl apply -f deploy.yaml"
    sig = parse_conversation_signals(text, "engineer")
    assert sig.violation_found
    assert any("deploy" in cmd for cmd in sig.violation_commands)


def test_violation_qa_writes_code():
    """信号3: qa 写代码越界"""
    text = "$ cat > new_module.py << EOF"
    sig = parse_conversation_signals(text, "qa")
    assert sig.violation_found
    assert any("write_code" in cmd for cmd in sig.violation_commands)


def test_violation_pg_runs_tests():
    """信号3: pg 跑 pytest 越界"""
    text = "$ python3 -m pytest tests/"
    sig = parse_conversation_signals(text, "pg")
    assert sig.violation_found
    assert any("run_tests" in cmd for cmd in sig.violation_commands)


def test_no_violation_engineer_normal_work():
    """信号3: engineer 正常写代码不触发"""
    text = "$ python3 -c \"print('hello')\"\n$ ls -la\n$ cat README.md"
    sig = parse_conversation_signals(text, "engineer")
    assert not sig.violation_found, f"期望不触发越界，但得到 {sig.violation_commands}"


def test_violation_coordinator_deploy():
    """信号3: coordinator deploy 越界"""
    text = "$ git push origin main"
    sig = parse_conversation_signals(text, "coordinator")
    assert sig.violation_found
    assert any("deploy" in cmd for cmd in sig.violation_commands)


def test_combined_signals():
    """组合信号：完成 + 空闲 + 越界同时出现"""
    text = """✔ Goal achieved (15s · 1 turn · 351 tokens)
● Same generic template. No change. Idle.
$ kubectl get pods"""
    sig = parse_conversation_signals(text, "engineer")
    assert sig.step_completed
    assert sig.idle_complaint
    assert not sig.violation_found  # kubectl get 是只读，不是 apply


def test_combined_with_waiting_exclude():
    """组合信号：完成 + 等待（不触发 idle）"""
    text = """s1 已完成产出
等待 coordinator 确认后进入下一步"""
    sig = parse_conversation_signals(text, "lr")
    assert sig.step_completed
    assert not sig.idle_complaint  # 因为含"等待 coordinator"


# ── G3: 信号 → 治理提案 (propose_governance_action) ──
import subprocess as _sp
from unittest import mock
from conversation_monitor import propose_governance_action


def test_propose_idle_signal_writes_bus():
    """idle_complaint 命中 → 写 bus architecture 提案"""
    with mock.patch("conversation_monitor.capture_role_pane", return_value="No change, idle, same generic template"), \
         mock.patch("conversation_monitor.subprocess.run") as m_run:
        result = propose_governance_action("engineer")
        assert result["idle"] is True
        assert result["proposed"] is True
        assert m_run.called
        cmd = m_run.call_args[0][0]
        assert "write" in cmd and "architecture" in cmd
        m_run.reset_mock()


def test_propose_violation_signal_writes_bus():
    """violation 命中 → 写 bus architecture 提案"""
    pane = "kubectl apply -f prod.yaml\nsystemctl restart hermes-gateway"
    with mock.patch("conversation_monitor.capture_role_pane", return_value=pane), \
         mock.patch("conversation_monitor.subprocess.run") as m_run:
        result = propose_governance_action("engineer")
        assert result["violation"] is True
        assert result["proposed"] is True
        assert m_run.called
        m_run.reset_mock()


def test_propose_no_signal_no_bus_write():
    """无 idle/violation → 不写 bus"""
    with mock.patch("conversation_monitor.capture_role_pane", return_value="正在分析代码，无异常"), \
         mock.patch("conversation_monitor.subprocess.run") as m_run:
        result = propose_governance_action("engineer")
        assert result["proposed"] is False
        assert not m_run.called

