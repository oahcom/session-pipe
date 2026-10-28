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
import subprocess
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


# ── 站点 B：codex review 的 cwd 不是独立仓库 ──

def _scope_fixture(tmp_path, monkeypatch):
    """假 home 里放一个角色 workspace（非独立仓库），返回 (run, step, ws)。"""
    home = tmp_path / "home"
    ws = home / "ccs-workspaces" / "scout"
    ws.mkdir(parents=True)
    (home / "unrelated_elsewhere.py").write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setattr("pipeflow.step_handler._CCS_WORKSPACES",
                        home / "ccs-workspaces")
    from pipeflow.engine import WorkflowRun, Step
    run = WorkflowRun(id="wf_scope", workflow_name="food_chain_loop",
                      context={}, current_step="s1", status="running",
                      created_at=1e9)
    step = Step(id="s1", title="调研", target_role="scout", prompt_template="调研",
                exit_condition={}, dual_node=True,
                exit_schema={"required": ["SCOUT_REPORT.md"]})
    return run, step, ws


def test_codex_review_never_sends_whole_home_diff(tmp_path, monkeypatch):
    """红转绿：workspace 只是 home 的子目录（show-toplevel=/home）时，
    codex 的 cwd 不得是 workspace 或 home —— 否则整 home 未提交改动被送审。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    (ws / "SCOUT_REPORT.md").write_text("# 报告\n结论：无发现。\n", encoding="utf-8")
    eng = _engine_for_tick(tmp_path)

    calls = []
    real_run = subprocess.run

    def _fake_run(cmd, **kw):
        calls.append((list(cmd), kw.get("cwd")))
        if cmd[0] == "git":
            return real_run(cmd, **kw)          # 真建仓/真 stage，才能查里面到底有什么
        # 送审目录跑完就被 finally 清掉，必须在调用当刻抓快照
        calls.append(("snapshot", sorted(
            str(p.relative_to(kw["cwd"]))
            for p in Path(kw["cwd"]).rglob("*")
            if p.is_file() and ".git" not in p.relative_to(kw["cwd"]).parts)))
        # 空仓库里 untracked 文件在 `git diff` 下不可见 → 会恒判"无发现"假通过。
        # 抓 staged 清单，证明 codex 真能看到内容。
        calls.append(("staged", real_run(
            ["git", "-C", kw["cwd"], "diff", "--cached", "--name-only"],
            capture_output=True, text=True).stdout.split()))
        return subprocess.CompletedProcess(cmd, 0, "No findings", "")

    monkeypatch.setattr("pipeflow.step_handler.subprocess.run", _fake_run)
    res = eng._run_codex_review(run, step)
    assert res["success"] is True, res

    codex_calls = [(c, d) for c, d in calls if isinstance(c, list) and c[0] == "codex"]
    assert len(codex_calls) == 1, f"codex 应只跑一次: {calls}"
    cwd = codex_calls[0][1]
    assert cwd is not None, "旧代码在 ws 不存在时 cwd=None → 落在 daemon 自己的仓库"
    assert os.path.realpath(cwd) != os.path.realpath(ws), \
        "codex 的 cwd 仍是角色 workspace → 送审的是整个 home 的 diff"
    assert not os.path.realpath(cwd).startswith(os.path.realpath(ws) + os.sep)
    # 送审目录里只应有本步产出，没有 home 里的其它文件
    sent = [v for k, v in calls if k == "snapshot"][0]
    assert sent == ["SCOUT_REPORT.md"], f"送审范围被污染: {sent}"
    staged = [v for k, v in calls if k == "staged"][0]
    assert staged == ["SCOUT_REPORT.md"], \
        f"产出没进 index → 空 diff → 恒判无发现（假通过）: {staged}"


def test_codex_review_refuses_without_declared_outputs(tmp_path, monkeypatch):
    """无 exit_schema.required 时不猜范围：直接拒绝，不去审 home。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    step.exit_schema = None
    eng = _engine_for_tick(tmp_path)
    calls = []
    monkeypatch.setattr("pipeflow.step_handler.subprocess.run",
                        lambda cmd, **kw: calls.append(list(cmd)))
    res = eng._run_codex_review(run, step)
    assert res["success"] is False
    assert calls == [], f"范围无法确定时不得调用 codex: {calls}"


def test_codex_review_refuses_stale_outputs_only(tmp_path, monkeypatch):
    """只有历史文件（mtime 早于 run.created_at）时不得审——那是假证据。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    old = ws / "SCOUT_REPORT.md"
    old.write_text("# 旧报告\n", encoding="utf-8")
    os.utime(old, (1.0, 1.0))               # run.created_at = 1e9
    eng = _engine_for_tick(tmp_path)
    calls = []
    monkeypatch.setattr("pipeflow.step_handler.subprocess.run",
                        lambda cmd, **kw: calls.append(list(cmd)))
    res = eng._run_codex_review(run, step)
    assert res["success"] is False
    assert "缺少本实例产出" in res["error"], res
    assert calls == [], f"历史文件不得被当本步产出送审: {calls}"


def test_codex_review_refuses_partial_outputs(tmp_path, monkeypatch):
    """required 两项只写了一项 → 不得拿子集去审换"通过"。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    step.exit_schema = {"required": ["PRD.md", "REQUIREMENTS.md"]}
    (ws / "PRD.md").write_text("# PRD\n", encoding="utf-8")
    eng = _engine_for_tick(tmp_path)
    calls = []
    monkeypatch.setattr("pipeflow.step_handler.subprocess.run",
                        lambda cmd, **kw: calls.append(list(cmd)))
    res = eng._run_codex_review(run, step)
    assert res["success"] is False
    assert "REQUIREMENTS.md" in res["error"], res
    assert calls == [], f"残缺产出不得送审: {calls}"


def test_codex_review_rejects_glob_symlink_escape(tmp_path, monkeypatch):
    """通配符匹配到指向 ws 外的符号链接 → 按越权拒绝，不复制出去。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    outside = tmp_path / "secret.py"
    outside.write_text("KEY = 'x'\n", encoding="utf-8")
    (ws / "evil.py").symlink_to(outside)
    step.exit_schema = {"required": ["*.py"]}
    eng = _engine_for_tick(tmp_path)
    calls = []
    monkeypatch.setattr("pipeflow.step_handler.subprocess.run",
                        lambda cmd, **kw: calls.append(list(cmd)))
    res = eng._run_codex_review(run, step)
    assert res["success"] is False
    assert "路径越权" in res["error"], res
    assert calls == [], f"越权文件不得送审: {calls}"


def test_codex_scope_error_does_not_escape(tmp_path, monkeypatch):
    """准备阶段抛异常必须被收成 success=False，否则整个 tick 崩、角色收不到提示。"""
    run, step, ws = _scope_fixture(tmp_path, monkeypatch)
    (ws / "SCOUT_REPORT.md").write_text("# r\n", encoding="utf-8")
    eng = _engine_for_tick(tmp_path)

    def _boom(*a, **kw):
        raise OSError("EACCES")

    monkeypatch.setattr("pipeflow.step_handler.shutil.copy2", _boom)
    res = eng._run_codex_review(run, step)
    assert res["success"] is False, res
    assert "异常" in res["error"], res

# ── 站点 C：node2 结论匹配整段挂在 integrity 闸门内 → dual_node 生产不可达 ──

class _Fact:
    """bus 事实的最小面（_check_exit 只读 id/t/e/src/ts）。"""
    def __init__(self, fid, title, src="scout", ts=0.0, evidence=""):
        self.id, self.t, self.src, self.ts, self.e = fid, title, src, ts, evidence


def _engine_with_bus(tmp_path, facts):
    eng = _engine_for_tick(tmp_path)
    eng._bb.read.side_effect = lambda *a, **kw: list(facts)
    return eng


def _stuck_run_and_step():
    """生产现场：node1 已落章、审查指令已发（busy 未送达）、integrity 从未激活。

    13/13 个 dual_node 步在库里的形状就是这个 —— node1_done=True /
    node2_sent=True / node2_approved=None / 无任何 integrity_* 字段。
    """
    run, step = _run_and_step()
    run.step_results["s1"] = {"status": "notified", "ts": 1e12, "notified_at": 1e12,
                              "node1_done": True, "node1_at": 1e12,
                              "node2_sent": True, "node2_sent_at": 1e12}
    return run, step


def test_approved_fact_is_honoured_without_integrity_activation(tmp_path):
    """红转绿：node2_sent 后角色写了 s1-codex-approved → 必须置 approved 并推进。

    三个写 node2_approved 的站点（:692 busy / :1184 engine_codex / :1204 role_bus）
    原本都在 `if _integ_state.complete and not _integ_dual_pending:` 里，而
    `_integ_activate` 只在**非** dual_node 分支调用 → 该闸门对 dual_node 恒假，
    整段（含宽 filter 匹配）是死代码。生产后果：角色写对了也永远匹配不到。
    实测 4 个实例的角色确实写过 approved 事实（#439995 #459762 #461553 #462996），
    库里 node2_approved 依旧全为 None。
    """
    # ts 必须 >= 实例的 last_ts（fixture 用 1e12 表示"永不超时"），
    # 否则被 created_after 屏障挡掉，与"是否可达"无关。
    fact = _Fact(9001, "s1-codex-approved: food_chain_loop s1 审查通过 (wf_dn_tick)",
                 ts=1e12 + 10)
    eng = _engine_with_bus(tmp_path, [fact])
    run, step = _stuck_run_and_step()

    eng._tick(run, step)

    assert run.step_results["s1"].get("node2_approved") is True, \
        "角色已写 s1-codex-approved，引擎却没置 node2_approved → 宽 filter 段仍不可达"
    assert eng._lm.complete_step.called, "拿到审查结论仍未推进 → 死锁"


def test_first_node_message_alone_does_not_approve(tmp_path):
    """放宽可达性不等于放宽判据：node2_sent 后只有 s1-complete 仍不得放行。"""
    fact = _Fact(9002, "s1-complete: food_chain_loop s1 情报生产 (wf_dn_tick)",
                 ts=1e12 + 10)
    eng = _engine_with_bus(tmp_path, [fact])
    run, step = _stuck_run_and_step()

    eng._tick(run, step)

    assert not run.step_results["s1"].get("node2_approved"), \
        "第一节点自己的完成消息被当成审查结论 → 闸门形同虚设"
    # 引擎仍会调 complete_step，收口在 manager 的第二道门（返回 node2_pending）——
    # 那是 dual_node 设计的唯一收口，这里不重复断言（见
    # test_second_complete_blocked_without_approved）。
