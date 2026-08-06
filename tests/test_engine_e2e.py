#!/usr/bin/env python3
"""
真实场景 E2E 测试 — 工作流引擎 + CCS 活跃度检测 + 超时升级 + Task 自动完成

模拟真实 CCS 行为：
- 会话文件定期写入 = 正在工作
- 会话文件停止更新 = 掉线/卡住
- 写匹配 exit_condition 的 bus 消息 = 任务完成

运行：cd /home/administrator/session-pipeline && python3 tests/test_engine_e2e.py
"""
import json
import sys
import tempfile
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))

import pipeflow.engine as eng_mod
eng_mod._TIMEOUT_GRACE = 0.05
eng_mod._REMINDER_INTERVAL = 60  # 测试周期长，不干扰

from pipeflow.engine import WorkflowEngine, WorkflowRun, Step, WorkflowDef

# mock engine 全部 subprocess（tmux/ccs），防止真 spawn 挂起 30s。
# stdout="claude\n"：_ensure_role_alive 的 has-session 返回 0 → alive；
# _is_agent_alive 读到 "claude" → 命中直接返回 True，不 spawn 不 sleep 30s。
_sp_mock = patch("pipeflow.engine._sp.run",
                 lambda *a, **k: MagicMock(returncode=0, stdout="claude\n"))
_sp_mock.start()
import atexit
atexit.register(_sp_mock.stop)

HOME = Path.home()
ROLE = "e2e_test"

def _make_env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    return d

def _write_wf(wf_dir, name, steps):
    data = {"name": name, "title": name, "description": "", "steps": steps}
    (wf_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False))

def _session_dir(role):
    return HOME / ".claude" / "projects" / f"-home-administrator-ccs-workspaces-{role}"

def _touch_session(role, age_seconds=0):
    """模拟 CCS 会话更新（写入当前时间）。"""
    d = _session_dir(role)
    d.mkdir(parents=True, exist_ok=True)
    f = d / "live.jsonl"
    f.write_text(json.dumps({"type": "assistant", "timestamp": time.time()}))
    if age_seconds > 0:
        # 设置 mtime 为指定秒前
        new_mtime = time.time() - age_seconds
        os.utime(f, (new_mtime, new_mtime))

def _clean_session(role):
    d = _session_dir(role)
    if d.exists():
        import shutil
        shutil.rmtree(str(d))

import os

# ═══════════════════════════════════════════════════════════════════
# 场景 1：CCS 一直在工作 → 超时应被反复延长，不触发升级
# ═══════════════════════════════════════════════════════════════════
def test_ccs_keeps_working_extends_timeout():
    """CCS 持续更新会话 → 超时计数器始终为 0，status 始终 running。"""
    d = _make_env()
    _write_wf(d, "busy_ccs", [{
        "id": "s1", "title": "慢调研", "target_role": ROLE,
        "prompt_template": "调研{topic}",
        "exit_condition": {"bus_category": "notice", "text_contains": "NEVER_MATCH",
                           "timeout_minutes": 0.01},
        "max_retries": 2,
    }])

    eng = WorkflowEngine(d)
    rid = eng.start("busy_ccs", {"topic": "AI"})
    eng_mod._TIMEOUT_GRACE = 0.02

    # 模拟 3 轮：每轮前更新会话文件（活跃），然后 tick
    for i in range(3):
        _touch_session(ROLE)
        time.sleep(0.15)
        eng.run_once()

    s = eng.status(rid)
    r = s['retries'].get('s1', 0)
    ok = s['status'] == "running" and r == 0
    print(f"  场景1 持续工作延超时: status={s['status']} retries={r} {'✅' if ok else '❌'}")

    import shutil
    shutil.rmtree(d)
    _clean_session(ROLE)


# ═══════════════════════════════════════════════════════════════════
# 场景 2：CCS 完成工作 → 写 exit_condition → 工作流推进到下一步
# ═══════════════════════════════════════════════════════════════════
def test_ccs_completes_step_advances():
    """CCS 产出匹配 exit_condition 的 bus 消息 → 工作流推进到 s2。"""
    d = _make_env()
    _write_wf(d, "fast_ccs", [
        {"id": "s1", "title": "调研", "target_role": ROLE,
         "prompt_template": "调研",
         "exit_condition": {"bus_category": "notice", "text_contains": "DONE_S1",
                            "timeout_minutes": 30},
         "max_retries": 1},
        {"id": "s2", "title": "写报告", "target_role": ROLE,
         "prompt_template": "写报告",
         "exit_condition": {"bus_category": "notice", "text_contains": "DONE_S2",
                            "timeout_minutes": 30},
         "max_retries": 1},
    ])

    eng = WorkflowEngine(d)
    rid = eng.start("fast_ccs", {})

    # 写 bus 消息模拟 CCS 完成 s1
    eng._bb.write("notice", "调研结果 DONE_S1 完毕", src=ROLE)
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    ok = s['current_step'] == "s2"
    print(f"  场景2 完成步骤推进: current_step={s['current_step']} {'✅' if ok else '❌'}")
    import shutil
    shutil.rmtree(d)


# ═══════════════════════════════════════════════════════════════════
# 场景 3：CCS 卡住 → 超时升级 coordinator，但绝不自动失败
# ═══════════════════════════════════════════════════════════════════
def test_ccs_stuck_escalates_not_fails():
    """CCS 不更新会话文件 → 超时递增，3 轮后仍 running（升级但不失败）。"""
    d = _make_env()
    _write_wf(d, "stuck_ccs", [{
        "id": "s1", "title": "卡住任务", "target_role": ROLE,
        "prompt_template": "干活",
        "exit_condition": {"bus_category": "notice", "text_contains": "NEVER",
                           "timeout_minutes": 0.005},
        "max_retries": 1,
    }])

    eng = WorkflowEngine(d)
    rid = eng.start("stuck_ccs", {})
    eng_mod._TIMEOUT_GRACE = 0.01

    # 不更新会话文件，连续 tick 模拟长时间卡住
    max_retries = 1
    for i in range(max_retries + 3):
        time.sleep(0.15)
        eng.run_once()

    s = eng.status(rid)
    r = s['retries'].get('s1', 0)
    ok = s['status'] == "running" and r > 0
    print(f"  场景3 卡住升级不失败: status={s['status']} retries={r} {'✅' if ok else '❌'}")
    import shutil
    shutil.rmtree(d)


# ═══════════════════════════════════════════════════════════════════
# 场景 4：CCS 短暂离开后回来 → 超时重置
# ═══════════════════════════════════════════════════════════════════
def test_ccs_comes_back_resets_timeout():
    """CCS 掉线后重新活跃 → 超时计数器归零。"""
    d = _make_env()
    _write_wf(d, "back_ccs", [{
        "id": "s1", "title": "t", "target_role": ROLE,
        "prompt_template": "t",
        "exit_condition": {"bus_category": "notice", "text_contains": "NEVER",
                           "timeout_minutes": 0.008},
        "max_retries": 2,
    }])

    eng = WorkflowEngine(d)
    rid = eng.start("back_ccs", {})
    eng_mod._TIMEOUT_GRACE = 0.02

    # 第 1 阶段：掉线 → 等超时（timeout 0.008min=0.48s + grace 0.02s）
    time.sleep(0.6)
    eng.run_once()
    s = eng.status(rid)
    r1 = s['retries'].get('s1', 0)

    # 第 2 阶段：回来，更新会话 → 超时重置
    _touch_session(ROLE)
    time.sleep(0.6)
    eng.run_once()
    s = eng.status(rid)
    r2 = s['retries'].get('s1', 0)

    ok = r1 > 0 and r2 == 0
    print(f"  场景4 回来重置超时: r1={r1} r2={r2} {'✅' if ok else '❌'}")
    import shutil
    shutil.rmtree(d)
    _clean_session(ROLE)


# ═══════════════════════════════════════════════════════════════════
# 场景 5：多步骤工作流走完所有步骤 → Task 自动完成
# ═══════════════════════════════════════════════════════════════════
def test_multi_step_workflow_completes():
    """3 步工作流全部完成 → status=completed。"""
    d = _make_env()
    _write_wf(d, "multi", [
        {"id": "s1", "title": "s1", "target_role": ROLE,
         "prompt_template": "s1",
         "exit_condition": {"bus_category": "notice", "text_contains": "S1_OK",
                            "timeout_minutes": 30}},
        {"id": "s2", "title": "s2", "target_role": ROLE,
         "prompt_template": "s2",
         "exit_condition": {"bus_category": "notice", "text_contains": "S2_OK",
                            "timeout_minutes": 30}},
        {"id": "s3", "title": "s3", "target_role": ROLE,
         "prompt_template": "s3",
         "exit_condition": {"bus_category": "notice", "text_contains": "S3_OK",
                            "timeout_minutes": 30}},
    ])

    eng = WorkflowEngine(d)
    rid = eng.start("multi", {})

    for evt in ["S1_OK", "S2_OK", "S3_OK"]:
        eng._bb.write("notice", f"done {evt}", src=ROLE)
        time.sleep(0.05)
        eng.run_once()

    s = eng.status(rid)
    ok = s['status'] == "completed"
    print(f"  场景5 多步完成: status={s['status']} {'✅' if ok else '❌'}")
    import shutil
    shutil.rmtree(d)


# ═══════════════════════════════════════════════════════════════════
# 场景 6：提醒心跳 — 长时间运行但未超时
# ═══════════════════════════════════════════════════════════════════
@patch("pipeflow.engine.WorkflowEngine._send_to_role")
def test_reminder_heartbeat(mock_send):
    """长时间运行未超时 → 到达提醒间隔应触发轻提醒（非催促）。"""
    d = _make_env()
    _write_wf(d, "heartbeat", [{
        "id": "s1", "title": "长任务", "target_role": ROLE,
        "prompt_template": "长任务",
        "exit_condition": {"bus_category": "notice", "text_contains": "NEVER",
                           "timeout_minutes": 30},
        "max_retries": 2,
    }])

    eng = WorkflowEngine(d)
    rid = eng.start("heartbeat", {})

    # 模拟长时间运行：直接通过 engine 的数据库连接设置 last_reminder
    eng._lifecycle._conn.execute(
        "UPDATE workflow_instances SET step_results = json_set(COALESCE(step_results, '{}'), '$.s1.last_reminder', ?) WHERE instance_id = ?",
        (time.time() - 121, rid)
    )
    eng._lifecycle._conn.commit()

    eng.run_once()

    ok = mock_send.called
    sent_text = mock_send.call_args[0][1] if mock_send.called else ""
    print(f"  场景6 提醒心跳: called={mock_send.called} text_has_剩余={('剩余' in sent_text) if sent_text else False} {'✅' if ok else '❌'}")
    import shutil
    shutil.rmtree(d)


# ═══════════════════════════════════════════════════════════════════
# 运行
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=== 工作流引擎 E2E 测试 ===\n")
    tests = [
        ("CCS持续工作延长超时", test_ccs_keeps_working_extends_timeout),
        ("CCS完成步骤推进", test_ccs_completes_step_advances),
        ("CCS卡住升级不失败", test_ccs_stuck_escalates_not_fails),
        ("CCS回来重置超时", test_ccs_comes_back_resets_timeout),
        ("多步骤工作流完成", test_multi_step_workflow_completes),
        ("提醒心跳", test_reminder_heartbeat),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            if fn():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
