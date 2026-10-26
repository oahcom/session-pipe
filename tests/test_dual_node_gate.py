"""dual_node 闸门：未拿到 codex-approved 证据前，任何完成路径都不得推进。

背景（30d 实测）：10 个到达 node1 的 dual_node 步骤有 9 个在没有
`<step>-codex-approved` bus 事实的情况下被判完成。原因有三处：
  1. completion_check 缺 else —— node1_done 一旦为真，第二 tick 直落 complete_step
     （6/9 步走这里，实测 node1→completed 仅 10.7s，而 codex review 单次 6m19s）
  2. busy exit-match 分支没有 dual_node 守卫（4/9 步）
  3. manager.complete_step 的第二道门只读 node1_done
且 `node2_sent` 完全没有判别力：它与 node1_done 由同一 dict 写入、同毫秒落章
（实测 node1_at=1792974682.8610425 / node2_sent_at=1792974682.861048）。

判据只能落在 node2_approved —— 由"实际匹配到标题含 <step>-codex-approved 的消息"
或"引擎自己跑 codex review 得到的零问题结论"置位。
"""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from lifecycle.manager import LifecycleManager  # noqa: E402
from pipeflow.step_handler import (  # noqa: E402
    _codex_review_prompt, _has_codex_approved,
)

_DUAL_STEPS = [
    {"step_id": "s1", "title": "step1", "target_role": "scout",
     "prompt_template": "调研", "dual_node": True,
     "exit_condition": {"bus_category": "evolution_report",
                        "text_contains": "s1-complete"}},
    {"step_id": "s2", "title": "step2", "target_role": "pm",
     "prompt_template": "写需求", "exit_condition": {"bus_category": "user_story"}},
]


@pytest.fixture()
def lm():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    m = LifecycleManager("test_role", db_path=path)
    m.upsert_template("dn", "双节点模板", "desc", _DUAL_STEPS,
                      trigger_scene=["test"], allowed_initiators=["tester"],
                      allowed_executors=["scout"])
    m.start_wf("wf_dn", "s1", "dn")
    yield m
    m.close()
    os.unlink(path)


def _sr(lm):
    return json.loads(lm._get_wf_unsafe("wf_dn")["step_results"] or "{}")


def test_first_complete_marks_node1_only(lm):
    """首次 complete_step 只落 node1，不推进。"""
    assert lm.complete_step("wf_dn", "s1") == "node1_done"
    assert _sr(lm)["s1"]["node1_done"] is True
    wf = lm._get_wf_unsafe("wf_dn")
    assert wf["current_step_id"] == "s1", "第一节点不得推进到 s2"


def test_second_complete_blocked_without_approved(lm):
    """node1 已到但没有 codex-approved 证据 → 必须拦住（红转绿的核心断言）。"""
    lm.complete_step("wf_dn", "s1")
    assert lm.complete_step("wf_dn", "s1") == "node2_pending"
    assert lm._get_wf_unsafe("wf_dn")["current_step_id"] == "s1", \
        "无审查证据却推进了 → 闸门仍是标签"


def test_second_complete_passes_with_approved(lm):
    """存在 node2_approved → 正常完成并推进。"""
    lm.complete_step("wf_dn", "s1")
    sr = _sr(lm)
    sr["s1"]["node2_approved"] = True
    sr["s1"]["node2_approved_by"] = "role_bus"
    lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
               (json.dumps(sr, ensure_ascii=False), "wf_dn"))
    assert lm.complete_step("wf_dn", "s1") in ("step_done_ready",
                                               "completed_and_advanced")
    assert lm._get_wf_unsafe("wf_dn")["current_step_id"] == "s2"


def test_node2_sent_alone_does_not_unlock(lm):
    """node2_sent 与 node1_done 同毫秒落章，单独存在不得放行（#456471 第五节）。"""
    lm.complete_step("wf_dn", "s1")
    sr = _sr(lm)
    sr["s1"]["node2_sent"] = True       # 引擎注入指令时会置这个
    sr["s1"]["node2_sent_at"] = sr["s1"]["node1_at"]
    lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
               (json.dumps(sr, ensure_ascii=False), "wf_dn"))
    assert lm.complete_step("wf_dn", "s1") == "node2_pending"
    assert lm._get_wf_unsafe("wf_dn")["current_step_id"] == "s1"


def test_non_dual_node_step_unaffected(lm):
    """非 dual_node 步骤走原路径（不得被新闸门卡住）。"""
    lm.upsert_template("plain", "普通模板", "desc",
                       [{"step_id": "s1", "target_role": "qa", "prompt_template": "测",
                         "exit_condition": {"bus_category": "test_report"}}],
                       trigger_scene=["test"], allowed_initiators=["tester"],
                       allowed_executors=["qa"])
    lm.start_wf("wf_plain", "s1", "plain")
    assert lm.complete_step("wf_plain", "s1") in ("step_done_ready",
                                                  "completed_and_advanced")


def test_has_codex_approved_matches_only_approved_title():
    """宽过滤 s1- 同时命中 s1-complete 与 s1-codex-approved → 只看匹配到不够。"""
    assert _has_codex_approved([{"text": "evolution_report/s1-complete wf_x"}], "s1") is False
    assert _has_codex_approved(
        [{"text": "evolution_report/s1-codex-approved wf_x"}], "s1") is True
    # 别的步骤的 approved 不能顶替本步
    assert _has_codex_approved([{"text": "s2-codex-approved wf_x"}], "s1") is False
    assert _has_codex_approved([], "s1") is False
    assert _has_codex_approved([{"text": None}], "s1") is False


def test_codex_review_prompt_asks_for_approved_marker():
    """注入的指令必须让角色写出带 approved 标记的标题，否则闸门永远等不到证据。"""
    p = _codex_review_prompt("food_chain_loop", "s1", "evolution_report")
    assert "s1-codex-approved" in p
    assert "codex review --uncommitted" in p


# ── 站点 A：completion_check 缺 else（6/9 步的路径） ──

def _engine_for_tick(tmp: Path):
    """用 __new__ 建引擎，绕开生产 DB / 工作流目录（与 test_integrity 同范式）。"""
    from pipeflow.engine import WorkflowEngine
    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng.workflows_dir = tmp
    eng.runs_dir = tmp / "runs"
    eng._workflows = {}
    eng._lm = MagicMock()
    eng._bb = MagicMock()
    eng._send_to_role = MagicMock(return_value=True)
    eng._sync_step_results = MagicMock()
    eng._consumed_exit_ids = set()
    return eng


def _run_and_step():
    from pipeflow.engine import WorkflowRun, Step
    run = WorkflowRun(id="wf_dn_tick", workflow_name="food_chain_loop",
                      context={"title": "t"}, current_step="s1",
                      status="running",
                      step_results={"s1": {"status": "notified",
                                           "ts": 1e12,  # 远未来 → 不触发 timeout
                                           "notified_at": 1e12}})
    step = Step(id="s1", title="调研", target_role="scout",
                prompt_template="调研", dual_node=True,
                exit_condition={"bus_category": "evolution_report",
                                "text_contains": "s1-complete",
                                "timeout_minutes": 45,
                                "completion_check": {"file": "", "min_size": 1}},
                max_retries=0, condition="", rollback_to="")
    return run, step


def test_completion_check_second_tick_does_not_complete(tmp_path):
    """红转绿：node1 已落章、文件判据仍成立 → 第二 tick 不得调 complete_step。

    这是 6/9 步实际走过的路径：旧代码 if 块结束后无 else，直落 complete_step。
    """
    eng = _engine_for_tick(tmp_path)
    report = tmp_path / "SCOUT_REPORT.md"
    report.write_text("x" * 600, encoding="utf-8")
    run, step = _run_and_step()
    step.exit_condition["completion_check"] = {"file": str(report), "min_size": 500}

    # 第一 tick：落 node1 + 注入审查指令，不完成
    eng._tick(run, step)
    assert run.step_results["s1"].get("node1_done") is True, "第一 tick 应落 node1"
    assert run.step_results["s1"].get("node2_approved") is not True
    eng._lm.complete_step.assert_not_called()

    # 第二 tick：node1 已在、文件仍满足 → 旧代码会在这里 complete_step
    eng._tick(run, step)
    eng._lm.complete_step.assert_not_called(), \
        "第二 tick 在无 codex-approved 证据时完成了 → completion_check 仍缺 else"


def test_completion_check_advances_after_approved(tmp_path):
    """有 node2_approved 后，第二 tick 必须正常推进（闸门不能变成死锁）。"""
    eng = _engine_for_tick(tmp_path)
    report = tmp_path / "SCOUT_REPORT.md"
    report.write_text("x" * 600, encoding="utf-8")
    run, step = _run_and_step()
    step.exit_condition["completion_check"] = {"file": str(report), "min_size": 500}
    run.step_results["s1"].update({"node1_done": True, "node1_at": 1.0,
                                   "node2_sent": True, "node2_sent_at": 1.0,
                                   "node2_approved": True})
    eng._tick(run, step)
    assert eng._lm.complete_step.called, "已批准却没推进 → 闸门变死锁"
