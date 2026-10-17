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
    text = "  ⎿  $ kubectl apply -f deploy.yaml"
    sig = parse_conversation_signals(text, "engineer")
    assert sig.violation_found
    assert any("deploy" in cmd for cmd in sig.violation_commands)


def test_violation_qa_writes_code():
    """信号3: qa 写代码越界"""
    text = "  ⎿  $ cat > new_module.py << EOF"
    sig = parse_conversation_signals(text, "qa")
    assert sig.violation_found
    assert any("write_code" in cmd for cmd in sig.violation_commands)


def test_violation_pg_runs_tests():
    """信号3: pg 跑 pytest 越界"""
    text = "  ⎿  $ python3 -m pytest tests/"
    sig = parse_conversation_signals(text, "pg")
    assert sig.violation_found
    assert any("run_tests" in cmd for cmd in sig.violation_commands)


# ── 误报回归（2026-10-16）──
# 判据原先对**整段 pane 文本**跑正则，把人话当命令：角色写调研报告、引用验证
# 命令、读含关键词的日志，都被判越界。实测 5 样本中 3 个纯文本讨论全误判。
# 放大效应：误报 → 引擎反复注入停止指令/cancel_step → 步骤不推进 → 2.4h 后
# 当僵尸回收（wf_6ff71a38016c 的 s1 累计 violation_count=26，而其 transcript
# 里 2911 条 Bash 命令全是只读，真实越界 0 次）。

def test_discussing_pytest_in_prose_not_violation():
    """正文讨论 pytest 不判越界——scout 调研报告的正常内容。"""
    text = "本次调研发现该项目的 pytest 覆盖率已达 87%，建议关注"
    sig = parse_conversation_signals(text, "scout")
    assert not sig.violation_found, f"正文讨论被误判: {sig.violation_commands}"

def test_citing_command_in_prose_not_violation():
    """正文引用验证命令不判越界——写报告时的正常内容。"""
    text = "验证命令: systemctl is-active hermes-gateway"
    sig = parse_conversation_signals(text, "scout")
    assert not sig.violation_found, f"命令引用被误判: {sig.violation_commands}"

def test_reading_log_mentioning_systemctl_not_violation():
    """读含 systemctl 字样的日志不判越界。"""
    text = "日志显示 systemctl status 反复报 failed，根因是 unit 文件路径错误"
    sig = parse_conversation_signals(text, "scout")
    assert not sig.violation_found, f"读日志被误判: {sig.violation_commands}"

def test_executed_command_with_echo_marker_still_flagged():
    """正向控制：带 ⎿ 回显的已执行命令**仍须**判越界。

    防止判据被改过头成"永不报警"的静默器——只放行叙述文字而放过真实执行。
    """
    text = "  ⎿  ⏵ $ systemctl restart hermes-gateway"
    sig = parse_conversation_signals(text, "scout")
    assert sig.violation_found, "已执行命令应判越界，实际被放过"

def test_readonly_systemctl_query_not_violation():
    """只读 systemctl 查询（排障常用）不判 edit_config 越界。

    实测 2026-10-17：engineer 执行 `systemctl status user@1000` 排障被误报
    「越界: edit_config」，引擎连发停止指令卡住工作流 s1。
    """
    for cmd in ("systemctl status user@1000.service --no-pager",
                "systemctl is-active user@1000.service",
                "systemctl cat hermes-dbus-watchdog.service",
                "systemctl show user@1000 -p TimeoutStopUSec",
                "systemctl list-timers --all"):
        sig = parse_conversation_signals(f"  ⎿  $ {cmd}\n", "engineer")
        assert not sig.violation_found, f"只读查询被误判: {cmd} -> {sig.violation_commands}"

def test_readonly_and_lifecycle_systemctl_not_edit_config():
    """只读查询与生命周期动作（start/stop）不判 edit_config。

    实测 bus #397969/#398702：`systemctl start user@1000`（maintainer 恢复动作）
    与 `systemctl status`（排障）均被误报，引擎连发停止指令卡住工作流。
    """
    for cmd in ("systemctl start user@1000.service",
                "sudo systemctl stop user@1000.service",
                "systemctl restart hermes-gateway"):
        sig = parse_conversation_signals(f"  ⎿  $ {cmd}\n", "engineer")
        assert not any("edit_config" in c for c in sig.violation_commands), \
            f"误判 edit_config: {cmd} -> {sig.violation_commands}"

def test_config_writing_systemctl_still_flagged():
    """正向控制：写配置类 systemctl 子命令仍须判 edit_config 越界（防矫枉过正）。"""
    sig = parse_conversation_signals("  ⎿  $ systemctl disable hermes-gateway.service\n", "engineer")
    assert sig.violation_found, "写配置子命令应判越界，实际被放过"
    assert any("edit_config" in c for c in sig.violation_commands), sig.violation_commands

def test_flag_prefixed_config_write_still_flagged():
    """P1-1 回归: 动词前带 flag 的写配置仍须判越界。

    `systemctl --user <verb>` 是本生态主导写法（~/.hermes/scripts/ 内 80 处）。
    收窄正则若要求动词紧跟 systemctl，则 flag 前缀写配置全部漏判（reviewer
    dev_implement/s2 CONDITIONAL_APPROVED P1-1）。
    """
    for cmd in ("systemctl --user disable x.service",
                "systemctl --user enable x.timer",
                "sudo systemctl --user mask x",
                "systemctl --now disable x"):
        sig = parse_conversation_signals(f"  ⎿  $ {cmd}\n", "engineer")
        assert sig.violation_found, f"flag 前缀写配置漏判: {cmd}"
        assert any("edit_config" in c for c in sig.violation_commands), sig.violation_commands

def test_flag_prefixed_readonly_still_not_flagged():
    """反向控制: flag 前缀的只读查询仍不判越界（防 flag 通配过头）。"""
    for cmd in ("systemctl --user status x.service",
                "systemctl --user is-active x",
                "systemctl --user show x -p TimeoutStopUSec"):
        sig = parse_conversation_signals(f"  ⎿  $ {cmd}\n", "engineer")
        assert not sig.violation_found, f"flag 前缀只读查询被误判: {cmd} -> {sig.violation_commands}"


def test_no_violation_engineer_normal_work():
    """信号3: engineer 正常写代码不触发"""
    text = "$ python3 -c \"print('hello')\"\n$ ls -la\n$ cat README.md"
    sig = parse_conversation_signals(text, "engineer")
    assert not sig.violation_found, f"期望不触发越界，但得到 {sig.violation_commands}"


def test_violation_coordinator_deploy():
    """信号3: coordinator deploy 越界"""
    text = "  ⎿  $ git push origin main"
    sig = parse_conversation_signals(text, "coordinator")
    assert sig.violation_found
    assert any("deploy" in cmd for cmd in sig.violation_commands)


def test_combined_signals():
    """组合信号：完成 + 空闲 + 越界同时出现"""
    text = """✔ Goal achieved (15s · 1 turn · 351 tokens)
● Same generic template. No change. Idle.
  ⎿  $ kubectl get pods"""
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
    pane = "  ⎿  $ kubectl apply -f prod.yaml\n  ⎿  $ systemctl restart hermes-gateway"
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


# ── G3: scan_and_propose — daemon 接入点的单例扫描 ──
from conversation_monitor import scan_and_propose


def test_scan_and_propose_counts_proposals():
    """scan_and_propose 対指定角色列表掃描並返回提案數"""
    idle_text = "No change, idle"
    normal_text = "s1 已完成，等待确认"
    with mock.patch("conversation_monitor.capture_role_pane",
                    side_effect=[idle_text, normal_text]), \
         mock.patch("conversation_monitor.subprocess.run") as m_run:
        out = scan_and_propose(["engineer", "maintainer"])
        assert len(out) == 1, f"期望 1 个提案，得到 {len(out)}"
        assert out[0]["role"] == "engineer"
        assert out[0]["_action"] == "idle"
        assert m_run.call_count == 1


def test_scan_and_propose_skips_test_persona():
    """过濾非生产 persona (dead_role / e2e_test 等)"""
    with mock.patch("conversation_monitor.capture_role_pane",
                    return_value="No change, idle"), \
         mock.patch("conversation_monitor.subprocess.run") as m_run:
        out = scan_and_propose(["dead_role", "e2e_test", "t"])
        assert out == [], "测试 persona 不应產出治理提案"
        assert not m_run.called

