#!/usr/bin/env python3
"""
E2E workflow lifecycle tests --- complete workcycle from start to finish.
All subprocess calls (tmux, ccs) are mocked for isolation.

Run: cd ~/session-pipeline && python3 -m pytest tests/test_workcycle_e2e.py -x -q
"""
import json
import signal
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow.engine import WorkflowEngine

# Global test timeout — prevents any single test from hanging
_TEST_TIMEOUT = 20


def _make_env():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True)
    _new_tag()
    return d


_TAG = ""


def _new_tag():
    """每个用例一份唯一后缀，隔开跨用例的同名 bus 消息（见 _done）。"""
    global _TAG
    _SEQ[0] += 1
    _TAG = f"u{_SEQ[0]}"


_SEQ = [0]

def _write_wf(wf_dir, name, steps):
    data = {"name": name, "title": name, "description": "", "steps": steps}
    (wf_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False))


def _step(step_id, **overrides):
    base = {
        "id": step_id, "title": step_id, "target_role": "test_role",
        "prompt_template": f"do {step_id}",
        "exit_condition": {"bus_category": "notice",
                           "text_contains": f"DONE_{step_id.upper()}",
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(overrides)
    return base


def _done(eng, step_id, src="test_role", extra=""):
    """写步骤完成消息。文本带本次用例唯一后缀。

    WHY: 整个 pytest 进程共用一个临时 blackboard，而 bus 写入是按文本 UPSERT
    —— 同文本重写不刷新 facts.ts。而 _check_exit 的 _birth 屏障取 log 表
    MIN(ts)（首次写入时刻）→ 用例 B 复用用例 A 的文本时，该消息的 birth 恒早于
    B 的工作流 created_at，匹配永久为空。实测：HEAD 版同一份代码稳定 4 failed,
    本文用例从第二个起连锁失败。text_contains 是子串匹配，故后缀不破坏匹配。
    """
    eng._bb.write("notice", f"DONE_{step_id.upper()}{_TAG} {extra}", src=src)



def _poll_done(eng, rid, tries=25):
    """轮询到工作流离开 running。末步收尾比角色完成消息晚一个 tick：
    _advance_unsafe 对末步只返回 False（不自行 close，否则绕开部署与 loop 自触发），
    close_wf 由引擎主循环的 _advance_production_wf 在下一次 tick 执行。
    """
    for _ in range(tries):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["status"] not in ("running", "pending"):
            return eng.status(rid)
    return eng.status(rid)

def _poll(eng, rid, step_id, tries=20):
    """轮询等待步骤推进。固定 sleep(0.1)+单次 run_once 在负载下不够：
    advance 走两阶段（锁内 commit + 锁外 _send_to_role），实测 1/6 概率未完成。"""
    for _ in range(tries):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] != step_id:
            return


# ═══════════════════════════════════════════════════════════════════
# 1. Single-step lifecycle
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_single_step_lifecycle(mock_run):
    """start -> notify -> bus exit -> run_once -> completed."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "single", [_step("s1")])
    eng = WorkflowEngine(d)
    rid = eng.start("single", {})

    # Initial state: running, step not yet in results
    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["current_step"] == "s1"

    # run_once -> sends prompt, marks notified
    eng.run_once()
    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "notified", s

    # Bus exit -> 收尾（末步由下一轮 tick 的引擎主循环完成）
    _done(eng, "s1")
    s = _poll_done(eng, rid)
    assert s["status"] == "completed", f"expected completed, got {s}"

    eng.cancel(rid)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 2. Two-step sequential
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_two_step_sequential(mock_run):
    """s1 -> advance -> s2 -> complete."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "two", [_step("s1"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("two", {})

    # Notify + exit s1
    eng.run_once()
    _done(eng, "s1")
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["current_step"] == "s2", f"expected s2, got {s}"

    # 先 run_once 让 s2 标记 notified（bus_anchor=notified_at），再写 DONE_S2
    # 否则消息被 s2 的 created_after 过滤，永远无法推进
    eng.run_once()
    # Exit s2
    # Exit s2 -> 末步收尾
    _done(eng, "s2")
    s = _poll_done(eng, rid)
    assert s["status"] == "completed", f"expected completed, got {s}"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 3. Handoff auto-advance (no approval gate)
# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_handoff_advances_without_confirm(mock_run):
    """handoff 是交接不是审批：exit 命中后直接推进，不发 token、不等 confirm。

    2026-10-19 实测：旧行为给 handoff 发 approval_token + 置 step_done_ready，
    engine 审批门控遂拦住每一步等 confirm，而真实 confirm 率仅 1.4%
    （1347 步中 425 步持未消费 token、真 confirm 8 次）→ 每步白等 TTL+grace
    90min，WL-full_dev_pipeline 21 实例全 cancelled、0 走完。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "ho", [_step("s1", type="handoff"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("ho", {})

    eng.run_once()
    _done(eng, "s1", src="pm")
    # 轮询而非固定 sleep：advance 走 _advance_production_wf 两阶段（锁内 commit
    # + 锁外 _send_to_role），单次 100ms 在高负载下不够 → 间歇性误判。
    for _ in range(20):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] != "s1":
            break

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "completed", s
    assert s["current_step"] == "s2", f"handoff 应自动推进到 s2, got {s}"
    assert not eng._lifecycle.get_approval_token(rid, "s1"), \
        "handoff 不应发放审批密钥"
    _cleanup(d)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_handoff_chain_auto_flows(mock_run):
    """四步 handoff 链全程无人工 confirm 自行贯通（用户核心诉求）。

    场景：pm → 架构 → 开发 → 测试 全链交接，每步完成即自动交给下一步。
    旧行为每步发 token 等 confirm（实测真实 confirm 率 1.4%）→ 每步白等
    90min，而步骤超时上限 30min×2 先到 → 工作流被 auto-cancel，
    21 个 WL-full_dev_pipeline 实例无一走完。本用例锁死"自发贯通"。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    roles = ["pm", "product_architect", "engineer", "qa"]
    steps = [_step(f"s{i+1}", type="handoff", target_role=r)
             for i, r in enumerate(roles)]
    _write_wf(d, "chain", steps)
    eng = WorkflowEngine(d)
    rid = eng.start("chain", {})

    for i in range(1, len(steps) + 1):
        assert eng.status(rid)["current_step"] == f"s{i}", \
            f"第 {i} 步应已接手: {eng.status(rid)}"
        eng.run_once()  # 投递本步
        _done(eng, f"s{i}", src=roles[i - 1])
        for _ in range(20):
            time.sleep(0.05)
            eng.run_once()
            if eng.status(rid)["current_step"] != f"s{i}":
                break

    s = eng.status(rid)
    assert s["status"] == "completed", f"全链应跑完，实际 {s}"
    _cleanup(d)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_review_requires_confirm(mock_run):
    """review 有独立审批人时仍是质量闸门：发 token，需 confirm 才推进。

    s2 由 reviewer 执行、上一步 s1 由 pm 执行 → assigner=pm，是真审批。
    同角色自审不发 token（见 test_review_self_audit_no_deadlock）。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "rv", [
        _step("s1", target_role="pm"),
        _step("s2", type="review", target_role="reviewer"),
        _step("s3"),
    ])
    eng = WorkflowEngine(d)
    rid = eng.start("rv", {})

    eng.run_once()
    _done(eng, "s1", src="pm")

    eng.run_once()
    # s2 必须先被投递（notified）再写完成消息，否则消息早于步骤接管，
    # _check_exit 的 created_after 屏障会把旧消息挡在外面。
    for _ in range(20):
        time.sleep(0.05)
        eng.run_once()
        _sr2 = eng.status(rid)["results"].get("s2") or {}
        if _sr2.get("status") == "notified":
            break
    assert (eng.status(rid)["results"].get("s2") or {}).get("status") == "notified", \
        f"s2 应已投递: {eng.status(rid)}"
    _done(eng, "s2", src="reviewer")
    time.sleep(0.1)
    eng.run_once()
    time.sleep(0.1)
    eng.run_once()

    s = eng.status(rid)
    assert s["results"]["s2"]["status"] == "step_done_ready", s
    token = eng._lifecycle.get_approval_token(rid, "s2")
    assert token, "有独立审批人的 review 必须发放审批密钥"
    assert s["current_step"] == "s2", f"未 confirm 不得推进, got {s}"
    eng._lifecycle.confirm_step(rid, "s2", token=token, approved=True)

    s = eng.status(rid)
    assert s["current_step"] == "s3", f"confirm 后应推进到 s3, got {s}"
    _cleanup(d)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_review_self_audit_no_deadlock(mock_run):
    """自审 review 不发 token，直接推进——否则死等 TTL(1h)+grace(30min)。

    生产事故：architect_full_design/s4 的 target_role=product_architect，
    assigner 也返回它自己 → token 无人消费，步骤 step_done_ready 卡 61 分钟；
    同时 liveness 把它算作 busy，锁死同角色 3 个其它工作流（排队 7~21 次）。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "selfrv", [_step("s1", type="review"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("selfrv", {})

    eng.run_once()
    _done(eng, "s1")
    for _ in range(20):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] != "s1":
            break

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "completed", s
    assert not eng._lifecycle.get_approval_token(rid, "s1"), \
        "自审 review 不得发放审批密钥（否则无人消费，卡死 90 分钟）"
    assert s["current_step"] == "s2", f"自审 review 应直接推进, got {s}"
    _cleanup(d)

# ═══════════════════════════════════════════════════════════════════
# 4. Review rejection
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_review_rejection_keeps_running(mock_run):
    """review -> step_done_ready -> reject -> still running.

    驳回走 confirm_step(approved=False)，置 rejected 且不推进。handoff 不再有
    token 通道，故本用例改用 review 型步骤覆盖同一拒绝语义。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "hr", [_step("s1", type="review")])
    eng = WorkflowEngine(d)
    rid = eng.start("hr", {})

    eng.run_once()
    _done(eng, "s1")
    time.sleep(0.1)
    eng.run_once()

    token = eng._lifecycle.get_approval_token(rid, "s1")
    ok = eng._lifecycle.confirm_step(rid, "s1", token=token, approved=False,
                                      reason="no good")
    assert not ok

    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["results"]["s1"]["status"] == "rejected"
    _cleanup(d)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_gate_rejection_blocks_advance(mock_run):
    """gate 型评审驳回 → 不推进（回归守卫）。

    2026-10-17 实测：wf_98a18e738d 的 s3 是 type=gate，reviewer 判
    "CHANGES_REQUESTED (0 P0, 3 P1, 2 P2)"，引擎却推进到了 s4 —— 因为返工
    门禁原判据只认 type=="review"。模板里 reviewer 步骤有 review(9) 与
    gate(3) 两类，只认其一会让 3 个模板的评审意见被无视。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "gate_r", [
        _step("s1", type="gate"),
        _step("s2"),
    ])
    eng = WorkflowEngine(d)
    rid = eng.start("gate_r", {})

    eng.run_once()
    _done(eng, "s1", extra="CHANGES_REQUESTED: 3 P1 需修复")
    _poll(eng, rid, "s1")

    s = eng.status(rid)
    assert s["current_step"] == "s1", f"gate 驳回不得推进，实际到了 {s['current_step']}"
    _cleanup(d)


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_gate_approved_advances(mock_run):
    """gate 型评审通过 → 正常推进（防止上一用例修过头成永久卡死）。"""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "gate_a", [
        _step("s1", type="gate"),
        _step("s2"),
    ])
    eng = WorkflowEngine(d)
    rid = eng.start("gate_a", {})

    eng.run_once()
    _done(eng, "s1", extra="APPROVED: 0 P0, 0 P1")
    _poll(eng, rid, "s1")

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "completed" and s["current_step"] == "s2", \
        f"APPROVED 应推进到 s2，s1={s['results']['s1']} current={s.get('current_step')}"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 5. Cancel mid-workflow
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_cancel_mid_workflow(mock_run):
    """Cancel after s1 complete -> cancelled."""
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "cw", [_step("s1"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("cw", {})

    eng.run_once()
    _done(eng, "s1")
    _poll(eng, rid, "s1")
    s = eng.status(rid)
    assert s["current_step"] == "s2"

    # cancel 有"创建不足 5 分钟拒绝取消"保护 → 把 created_at 改老再 cancel
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET created_at=? WHERE instance_id=?",
        (time.time() - 600, rid))
    ok = eng.cancel(rid)
    assert ok
    s = eng.status(rid)
    assert s["status"] == "cancelled"
    assert not eng.cancel(rid)  # second cancel returns False
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 6. Invalid workflow name
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_start_invalid_workflow(mock_run):
    """Start nonexistent -> ValueError."""
    d = _make_env()
    eng = WorkflowEngine(d)
    try:
        eng.start("nonexistent", {})
        assert False, "Should raise"
    except ValueError as e:
        assert "未知工作流" in str(e)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 7. Empty steps
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_start_empty_steps(mock_run):
    """Workflow with empty steps -> ValueError."""
    d = _make_env()
    _write_wf(d, "empty", [])
    eng = WorkflowEngine(d)
    try:
        eng.start("empty", {})
        assert False, "Should raise"
    except ValueError as e:
        # 引擎加载时 Schema 校验已拦截空 steps（74ae6bb），
        # 空模板不再注册 → start 报"未知工作流"；兼容两条路径
        assert "无步骤" in str(e) or "未知工作流" in str(e)
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# 8. Status on nonexistent
# ═══════════════════════════════════════════════════════════════════

def test_status_nonexistent():
    """Status of unknown ID -> error dict."""
    d = _make_env()
    eng = WorkflowEngine(d)
    s = eng.status("wf_nonexistent_999")
    assert s.get("error") == "不存在"
    _cleanup(d)


# ── 末步收尾不可被旁路 ──

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_last_step_runs_engine_closeout(mock_run):
    """末步必须经引擎收尾，而非由 _advance_unsafe 直接写 completed。

    为什么锁死：_advance_unsafe 的末步分支原实现直接 UPDATE status='completed'
    就返回。那条路径绕开 _advance_production_wf 的 is_last 分支，连带丢掉三件
    锁外收尾：子链检查、_deploy_production_outputs（产出装到 ~/.hermes/bin）、
    wf.loop 的食物链自触发。实测单步链 prod_advance=0、deploy=0。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "closeout", [_step("s1")])
    eng = WorkflowEngine(d)

    deployed = []
    _orig = eng._deploy_production_outputs
    eng._deploy_production_outputs = lambda wf: (deployed.append(wf), _orig(wf))[1]

    rid = eng.start("closeout", {})
    eng.run_once()
    _done(eng, "s1")
    s = _poll_done(eng, rid)

    assert s["status"] == "completed", f"应完成，实际 {s}"
    assert deployed == [rid], f"末步收尾必须跑部署（实测被旁路），deployed={deployed}"
    _cleanup(d)


# ═══════════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════════

def _cleanup(d):
    import shutil
    shutil.rmtree(d, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, lambda s, f: (
            print(f"\nTIMEOUT after {_TEST_TIMEOUT}s"), sys.exit(1)))
    import traceback as tb
    tests = [
        ("单步生命周期", test_single_step_lifecycle),
        ("两步顺序推进", test_two_step_sequential),
        ("Handoff 自动交接（无审批）", test_handoff_advances_without_confirm),
        ("Handoff 链自发贯通", test_handoff_chain_auto_flows),
        ("Review 需审批确认", test_review_requires_confirm),
        ("Review 驳回不推进", test_review_rejection_keeps_running),
        ("中途取消", test_cancel_mid_workflow),
        ("无效工作流名", test_start_invalid_workflow),
        ("空步骤", test_start_empty_steps),
        ("不存在状态", test_status_nonexistent),
        ("末步经引擎收尾（不旁路部署）", test_last_step_runs_engine_closeout),
    ]
    passed, failed = 0, 0
    for name, fn in tests:
        signal.alarm(_TEST_TIMEOUT)
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            tb.print_exc()
            failed += 1
    signal.alarm(0)
    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
