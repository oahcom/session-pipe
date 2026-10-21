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
    global _TAG
    _SEQ[0] += 1
    _TAG = f"u{_SEQ[0]}"


_SEQ = [0]


def _token(step_id):
    """步骤完成 token —— exit 条件与完成消息共用同一份。

    _check_exit 按子串匹配（step_handler.py:1352 `text_filter not in f.t`）：
    条件若写成裸前缀 "DONE_S1"，则任何写 "DONE_S1*" 的消息都能点亮它。
    全量跑时同进程共用一个临时 blackboard（tests/conftest.py:54），
    test_engine_e2e.py 写的裸 "DONE_S1" 与本文件的同前缀消息互相点亮 ——
    实测 test_review_requires_confirm flaky，/tmp/full.log:17 中 s1 的
    exit_messages 混入三个用例 tag（u3/u4/u5）即现场证据。
    每用例一份 _TAG 且 token 两侧一致，故互不构成子串。
    """
    return f"TAG{_TAG}_{step_id.upper()}_DONE"

def _write_wf(wf_dir, name, steps):
    data = {"name": name, "title": name, "description": "", "steps": steps}
    (wf_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False))


def _step(step_id, **overrides):
    base = {
        "id": step_id, "title": step_id, "target_role": "test_role",
        "prompt_template": f"do {step_id}",
        # 条件与消息共用 _token：裸前缀条件会被同进程其它用例写的
        # 同前缀消息点亮（见 _token 的 WHY）。
        "exit_condition": {"bus_category": "notice",
                           "text_contains": _token(step_id),
                           "timeout_minutes": 30},
        "max_retries": 1, "type": "single",
    }
    base.update(overrides)
    return base


def _done(eng, step_id, src="test_role", extra=""):
    """写步骤完成消息。文本与 exit 条件共用 _token（见其 WHY）。

    整个 pytest 进程共用一个临时 blackboard，而 bus 写入是按文本 UPSERT
    —— 同文本重写不刷新 facts.ts。而 _check_exit 的 _birth 屏障取 log 表
    MIN(ts)（首次写入时刻）→ 用例 B 复用用例 A 的文本时，该消息的 birth 恒早于
    B 的工作流 created_at，匹配永久为空。实测：HEAD 版同一份代码稳定 4 failed,
    本文用例从第二个起连锁失败。
    """
    eng._bb.write("notice", f"{_token(step_id)} {extra}", src=src)



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
    assert "approval_token" not in (s["results"]["s1"] or {}), \
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
def test_review_advances_without_token(mock_run):
    """评审步骤按**结论**推进：不发密钥、不等 confirm（2026-10-22）。

    变异对照：旧实现给 review 发一张审批密钥等 assigner 确认。实测全库 429 张
    密钥 approval_consumed_at 全为 null，15 个有终态的评审步等待中位数
    **90.0 分钟**（= TTL 60min + grace 30min），全部由超时兜底放行——那 90 分钟
    纯粹是死等，且每个评审步都要付一次（返工链自身也带评审步）。
    本用例锁住新契约：结论通过 → 直接推进；step_results 里不留任何密钥字段。
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
    _poll(eng, rid, "s1")

    _done(eng, "s2", src="reviewer", extra="APPROVED 业务 0P0/0P1")
    _poll(eng, rid, "s2")

    s = eng.status(rid)
    assert s["current_step"] == "s3", f"评审通过应直接推进到 s3, got {s}"
    # 密钥可以留在 step_results（供人工复核/追溯），但**不得阻塞推进**：
    # 结论 APPROVED 已经让引擎往前走，没人 confirm 也不等 90 分钟。
    r2 = s["results"].get("s2") or {}
    assert not r2.get("approval_consumed_at"), f"没人 confirm 却标了已消费: {r2}"
    _cleanup(d)
@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_review_self_audit_no_deadlock(mock_run):
    """首步 review 无「上一步产出」可审 → 直接推进，不留等待态。

    生产原形：architect_full_design/s4 的 target_role 与上一步同为
    product_architect = 自审，密钥无人消费，步骤 step_done_ready 卡 61 分钟；
    同时 liveness 把它算作 busy，锁死同角色 3 个其它工作流（排队 7~21 次）。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "selfrv", [_step("s1", type="review"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("selfrv", {})

    eng.run_once()
    _done(eng, "s1")
    _poll(eng, rid, "s1")

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "completed", s
    assert "approval_token" not in (s["results"]["s1"] or {}), \
        f"自审 review 不得留下密钥字段: {s['results']['s1']}"
    assert s["current_step"] == "s2", f"自审 review 应直接推进, got {s}"
    _cleanup(d)
@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_rework_chain_first_step_review_no_token(mock_run):
    """返工链首步 review 不得等一个不知情的审批人。

    生产原形：返工链由引擎自建，assigner 写死 "reviewer"，而首步 review 的
    执行者是 maintainer——reviewer 从不知道这条链存在，密钥必然烂在库里。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "selfeq", [_step("s1", type="review"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("selfeq", {})
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET assigner=? WHERE instance_id=?",
        ("reviewer", rid))

    eng.run_once()
    _done(eng, "s1")
    _poll(eng, rid, "s1")

    s = eng.status(rid)
    assert "approval_token" not in (s["results"]["s1"] or {}), \
        f"首步 review 不得留密钥: {s['results']['s1']}"
    assert s["current_step"] == "s2", f"首步 review 应直接推进, got {s}"
    _cleanup(d)
# ═══════════════════════════════════════════════════════════════════
# 4. Review rejection
# ═══════════════════════════════════════════════════════════════════

@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_review_rejection_keeps_running(mock_run):
    """评审结论为驳回 → 步骤置 rejected 且不推进（不再靠人工 confirm）。

    旧语义靠 confirm_step(approved=False) 落 rejected；密钥退场后，
    判据改由 review_gate 直接消费完成消息里的结论词（CHANGES_REQUESTED），
    由引擎的 rejected 消费分支建返工链或重投。本用例锁住这条链路仍然成立。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "hr", [_step("s1", type="review"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("hr", {})

    eng.run_once()
    _done(eng, "s1", extra="CHANGES_REQUESTED: 3 P1 — 设计未覆盖边界情况")
    _poll(eng, rid, "s1")

    s = eng.status(rid)
    assert s["results"]["s1"]["status"] == "rejected", s
    assert s["current_step"] == "s1", f"驳回不得推进, got {s}"
    assert s["status"] == "running", s
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
def test_review_gate_persists_rejected_then_rework_reflows(mock_run):
    """驳回门禁必须落库 rejected，返工链完成后才能回流重审。

    codex D10/D20：旧实现命中驳回只 `return`，step 仍停在 step_done_ready
    → engine 的 rejected 消费分支永远看不见 → 建不了返工链、无回流入口。
    本用例直接调 `_advance_production_wf` 走引擎 tick 门禁路径，再验证
    rejected → 建链 → 链完成后 requeue 的完整闭环。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "arc", [_step("s1", type="review"), _step("s2")])
    _write_wf(d, "engineer_feature", [_step("s1", target_role="engineer")])
    _write_wf(d, "maintainer_health_monitor", [_step("s1", target_role="maintainer")])
    eng = WorkflowEngine(d)
    rid = eng.start("arc", {})

    # 模拟「产出已落盘、引擎 tick 看到 step_done_ready + 驳回文本」的现场
    sr = {"s1": {"status": "step_done_ready", "exit_messages": [{
        "text": "CHANGES_REQUESTED",
        "evidence": "【业务问题→engineer】\n- P1 | 返回值错误 | chain.py:68"}]}}
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET status='step_done_ready', step_results=? "
        "WHERE instance_id=?", (json.dumps(sr), rid))
    eng._advance_production_wf(rid, eng._lifecycle, "arc", "s1", sr)

    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?", (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    assert s1["status"] == "rejected", f"门禁命中后必须落库 rejected，实际 {s1}"
    assert row["current_step_id"] == "s1", "驳回不得推进"

    # 下一 tick 消费 rejected → 建返工链，父步仍不推进
    eng.run_once()
    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?", (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    ids = s1.get("rework_ids") or []
    assert len(ids) == 1, f"rejected 应建 1 条返工链，实际 {ids}"
    assert row["current_step_id"] == "s1"

    # 返工链完成 → 回流重审原步（status 清空，current_step 不变）
    eng._lifecycle.execute(
        "UPDATE workflow_instances SET status='completed' WHERE instance_id=?", (ids[0],))
    eng.run_once()
    row = eng._lifecycle.query_one(
        "SELECT current_step_id, step_results FROM workflow_instances WHERE instance_id=?", (rid,))
    s1 = json.loads(row["step_results"])["s1"]
    assert s1.get("status") == "", f"链完成后应重投原步，实际 {s1}"
    assert row["current_step_id"] == "s1"
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


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_rework_dispatch_follows_review_arrow(mock_run):
    """评审标注「→角色」时，返工链必须派给该角色对应的模板。

    生产原形（bus #415594）：reviewer 写【业务问题→product_architect】指
    DESIGN.md 缺陷，旧实现一律建 engineer_feature → 工程改代码、文档没动
    → 必然再驳回。本用例喂真实驳回文本锁死派发目标，并对照无箭头回退。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "src", [_step("s1", type="review")])
    _write_wf(d, "architect_full_design", [_step("s1", target_role="product_architect")])
    _write_wf(d, "engineer_feature", [_step("s1", target_role="engineer")])
    eng = WorkflowEngine(d)
    rid = eng.start("src", {})
    _review = eng._workflows["src"].steps[0]

    def _add_rework(text):
        eng._create_rework_subflow(rid, eng._lifecycle, "src", "s1", _review, None,
                                   {"s1": {"exit_messages": [{"text": text}]}})
        row = eng._lifecycle.query_one(
            "SELECT step_results FROM workflow_instances WHERE instance_id=?", (rid,))
        return json.loads(row["step_results"])["s1"].get("rework_ids", [])

    def _inst(wid):
        return eng._lifecycle.query_one(
            "SELECT template_id, assignee FROM workflow_instances WHERE instance_id=?",
            (wid,))

    # 1) 箭头指向 product_architect → 文档返工模板
    ids = _add_rework("CHANGES_REQUESTED\n【业务问题→product_architect】\n"
                      "- P1 | AC6 无对应步骤 | DESIGN.md:121")
    assert len(ids) == 1, f"应建 1 条返工链，实际 {ids}"
    assert _inst(ids[0])["template_id"] == "architect_full_design", \
        f"箭头指向 PA 应走文档模板，实际 {_inst(ids[0])}"
    assert _inst(ids[0])["assignee"] == "product_architect"

    # 2) 同类重复驳回不重复建链（逐模板防重）
    ids = _add_rework("CHANGES_REQUESTED\n【业务问题→product_architect】\n"
                      "- P1 | AC7 仍缺步骤 | DESIGN.md:130")
    assert len(ids) == 1, f"同类返工不得重复建链，实际 {ids}"

    # 3) 无箭头业务问题 → 回退 engineer_feature
    ids = _add_rework("CHANGES_REQUESTED\n【业务问题】\n- P1 | 返回值错误 | chain.py:68")
    assert len(ids) == 2, f"无箭头业务问题应另建一条，实际 {ids}"
    assert _inst(ids[1])["template_id"] == "engineer_feature"
    assert _inst(ids[1])["assignee"] == "engineer"

    # 4) 同一份驳回里两类问题都指向同一模板 → 只建一条
    # （2026-10-19 codex D7：循环内 _tpl_exists 只查循环前的 _existing_reworks，
    #  不看本轮已建链 → sys/biz 都指向 engineer_feature 时建出两条相同链）
    rid2 = eng.start("src", {})
    eng._create_rework_subflow(rid2, eng._lifecycle, "src", "s1", _review, None,
                               {"s1": {"exit_messages": [{"text":
                                 "CHANGES_REQUESTED\n【系统问题→engineer】\n- P1 | 缺日志 | a.py:1\n"
                                 "【业务问题→engineer】\n- P1 | 返回值错误 | chain.py:68"
                                 }]}})
    row2 = eng._lifecycle.query_one(
        "SELECT step_results FROM workflow_instances WHERE instance_id=?", (rid2,))
    ids2 = json.loads(row2["step_results"])["s1"].get("rework_ids", [])
    assert len(ids2) == 1, f"两类问题同模板应只建 1 条链，实际 {ids2}"
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


@patch("pipeflow.engine._sp.run", return_value=MagicMock(
    returncode=0, stdout="claude\n", stderr=b""))
def test_exit_condition_ignores_foreign_message(mock_run):
    """本步骤的完成条件不得被「同前缀的别处消息」点亮。

    旧条件是与消息同前缀的裸 token，而 _check_exit 按子串匹配
    （step_handler.py:1352）→ 同进程其它用例写的 DONE_S1uN 会点亮本文件的
    DONE_S1 条件，反之亦然。实测 test_review_requires_confirm flaky
    （/tmp/full.log:17：s1 混入三个用例 tag）。本用例锁死这一隔离。
    """
    signal.alarm(_TEST_TIMEOUT)
    d = _make_env()
    _write_wf(d, "iso", [_step("s1"), _step("s2")])
    eng = WorkflowEngine(d)
    rid = eng.start("iso", {})

    eng.run_once()
    # 模拟「同进程另一个用例」写下的同前缀消息（旧代码下 DONE_S1 是其子串）
    eng._bb.write("notice", f"DONE_S1{_TAG}", src="test_role")
    for _ in range(6):
        time.sleep(0.05)
        eng.run_once()
    assert eng.status(rid)["current_step"] == "s1", \
        f"s1 不得被同前缀消息点亮: {eng.status(rid)}"

    _done(eng, "s1")
    for _ in range(20):
        time.sleep(0.05)
        eng.run_once()
        if eng.status(rid)["current_step"] != "s1":
            break
    assert eng.status(rid)["current_step"] == "s2", \
        f"本步骤自己的完成消息必须点亮 s1: {eng.status(rid)}"
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
