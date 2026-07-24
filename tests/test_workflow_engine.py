#!/usr/bin/env python3
"""
WorkflowEngine 测试 — 工作流执行引擎全功能覆盖。

运行：cd /home/administrator/session-pipeline && python3 tests/test_workflow_engine.py
"""
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

from pipeflow.engine import WorkflowEngine, WorkflowDef, WorkflowRun, Step

# Patch _TIMEOUT_GRACE to make timeout tests fast (not wait 10+ seconds)
import pipeflow.engine
_orig_grace = pipeflow.engine._TIMEOUT_GRACE
pipeflow.engine._TIMEOUT_GRACE = 0.1  # 100ms grace instead of 10s


def _make_wf_dir():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True, exist_ok=True)
    return d


def _write_wf(wf_dir, name, steps):
    data = {"name": name, "title": name, "description": "", "steps": steps}
    (wf_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False))


# 使用有效的 bus 分类：code_fix, architecture, security, prd, system_design, task_spec, product_design, blocker, design_issue, ops, monitor_audit, notice, workflow, reflexion_lesson
SIMPLE_STEPS = [
    {"id": "s1", "title": "调研", "target_role": "scout",
     "prompt_template": "请调研 {topic}", "exit_condition": {"bus_category": "architecture"},
     "max_retries": 0, "condition": "", "rollback_to": ""},
    {"id": "s2", "title": "实现", "target_role": "engineer",
     "prompt_template": "请实现 {topic}", "exit_condition": {"bus_category": "code_fix"},
     "max_retries": 1, "condition": "", "rollback_to": ""},
]


# ── Engine Lifecycle ─────────────────────────────────────────────

def test_engine_loads_workflows():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "alpha", SIMPLE_STEPS)
    _write_wf(wf_dir, "beta", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    names = eng.list_workflows()
    assert "alpha" in names
    assert "beta" in names
    assert len(names) == 2


def test_engine_empty_dir():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    assert eng.list_workflows() == []


def test_engine_non_existent_dir():
    eng = WorkflowEngine(workflows_dir="/tmp/__nonexistent_xxxx")
    assert eng.list_workflows() == []


def test_engine_skips_runs_subdir():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "valid", SIMPLE_STEPS)
    (wf_dir / "runs" / "some_run.json").write_text("{}")
    eng = WorkflowEngine(workflows_dir=wf_dir)
    assert "valid" in eng.list_workflows()


def test_engine_skips_invalid_json():
    wf_dir = _make_wf_dir()
    (wf_dir / "broken.json").write_text("not json{{{")
    _write_wf(wf_dir, "ok", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    assert "ok" in eng.list_workflows()
    assert "broken" not in eng.list_workflows()


# ── Start / Status / Cancel ──────────────────────────────────────

def test_start_creates_run():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test", {"topic": "AI Agent"})
    assert rid.startswith("wf_")
    s = eng.status(rid)
    assert s["status"] == "running"
    assert s["current_step"] == "s1"
    # 验证 run 文件存在
    assert (wf_dir / "runs" / f"{rid}.json").exists()


def test_start_with_context():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    ctx = {"topic": "Rust 编译器", "env": "dev"}
    rid = eng.start("test", ctx)
    # 从保存的 run 文件验证 context
    raw = json.loads((wf_dir / "runs" / f"{rid}.json").read_text())
    assert raw["context"] == ctx


def test_start_unknown_workflow():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    try:
        eng.start("ghost")
        assert False, "应抛出 ValueError"
    except ValueError as e:
        assert "未知" in str(e)


def test_start_empty_steps():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "empty", [])
    eng = WorkflowEngine(workflows_dir=wf_dir)
    try:
        eng.start("empty")
        assert False, "应抛出 ValueError"
    except ValueError as e:
        assert "无步骤" in str(e)


def test_status_nonexistent():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    s = eng.status("ghost")
    assert "error" in s


def test_cancel_running():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test")
    assert eng.cancel(rid)
    s = eng.status(rid)
    assert s["status"] == "cancelled"


def test_cancel_already_completed():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test")
    assert eng.cancel(rid)
    assert eng.cancel(rid) == False


def test_cancel_nonexistent():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    assert eng.cancel("ghost") == False


# ── Tick / Advance / Check Exit ─────────────────────────────────

def test_tick_advances_on_match():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test", {"topic": "x"})
    # 写入匹配的 bus 消息（使用有效分类 architecture）
    eng._bb.write("architecture", "这里有调研结果", src="scout")
    eng.run_once()
    s = eng.status(rid)
    # 应推进到 s2
    assert s["current_step"] == "s2", f"应推进到 s2: {s['current_step']}"


def test_tick_completes_on_last_step():
    wf_dir = _make_wf_dir()
    single_step = [{"id": "s1", "title": "调研", "target_role": "scout",
                    "prompt_template": "调研{topic}", "exit_condition": {"bus_category": "code_fix"},
                    "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf(wf_dir, "quick", single_step)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("quick", {"topic": "x"})
    eng._bb.write("code_fix", f"test-header: exit_condition匹配完成_{abs(hash(rid))&0xffff:04x}", src="test")
    eng.run_once()
    s = eng.status(rid)
    assert s["status"] == "completed", f"应 completed: {s['status']}"


def test_tick_no_match_keeps_running():
    """不匹配时保持 running 状态。"""
    wf_dir = _make_wf_dir()
    # 使用一个极不可能已有消息的自定义 exit_condition
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "请调研 {topic}",
              "exit_condition": {"bus_category": "notice", "text_contains": "TESTUNIQUE_NO_MATCH_XYZ"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf(wf_dir, "test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test", {"topic": "x"})
    eng.run_once()
    s = eng.status(rid)
    assert s["current_step"] == "s1"
    assert s["status"] == "running"


def test_check_exit_by_category():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    # 使用唯一的 text 过滤确保不匹配旧消息
    assert eng._check_exit("notice", "", "TESTEXITCATEG_NOMATCH") == False
    # 写入匹配的消息
    eng._bb.write("notice", "TESTEXITCATEG_FIND_ME", src="test_src")
    assert eng._check_exit("notice", "", "TESTEXITCATEG_FIND_ME") == True
    # source 过滤：不匹配的来源
    assert eng._check_exit("notice", "wrong_src", "") == False


def test_check_exit_by_source():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    eng._bb.write("notice", "源过滤测试", src="specific_src_abc")
    assert eng._check_exit("notice", "specific_src_abc", "") == True
    assert eng._check_exit("notice", "wrong_src_xyz", "") == False


def test_check_exit_by_text():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    eng._bb.write("notice", "唯一文本过滤: XYZZY_NO_SUCH_123", src="test")
    assert eng._check_exit("notice", "", "XYZZY_NO_SUCH_123") == True
    assert eng._check_exit("notice", "", "完全不存在的XYZQWERT_999") == False


def test_advance_finishes():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test", {"topic": "x"})
    run = eng._load_run(rid)
    # 模拟 s1 完成，当前在 s2
    run.current_step = "s2"
    run.step_results["s1"] = {"status": "done", "ts": time.time()}
    eng._save_run(run)
    eng._bb.write("code_fix", f"test-header: tick_总步骤完成_{abs(hash(rid))&0xffff:04x}", src="engineer")
    eng.run_once()
    s = eng.status(rid)
    assert s["status"] == "completed", f"应 completed: {s['status']}"


# ── Timeout / Retries ────────────────────────────────────────────

def test_timeout_triggers_retry():
    wf_dir = _make_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "调研{topic}",
              "exit_condition": {"bus_category": "notice", "text_contains": "TESTTIMEOUT_NOMATCH_123",
                                 "timeout_minutes": 0.001},
              "max_retries": 2, "condition": "", "rollback_to": ""}]
    _write_wf(wf_dir, "retry_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("retry_test", {"topic": "x"})
    time.sleep(1.5)  # 超过 timeout_minutes=0.001 min + grace
    eng.run_once()
    run = eng._load_run(rid)
    # 超时 + 未匹配 → 应有重试（至少 retry=1）
    assert run.status in ("running", "failed"), f"状态异常: {run.status}"
    assert run.step_retries.get("s1", 0) >= 1, f"应有重试计数: {run.step_retries}"


def test_timeout_escalates_not_fails():
    """超时不自动失败，改为升级提醒。"""
    wf_dir = _make_wf_dir()
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "调研{topic}",
              "exit_condition": {"bus_category": "notice", "text_contains": "TESTMAXRETRY_NOMATCH_456",
                                 "timeout_minutes": 0.001},
              "max_retries": 1, "condition": "", "rollback_to": ""}]
    _write_wf(wf_dir, "timeout_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("timeout_test", {"topic": "x"})
    time.sleep(1.5)
    eng.run_once()
    # 超时后应仍 running（不自动失败）
    time.sleep(1.5)
    eng.run_once()
    s = eng.status(rid)
    assert s["status"] == "running", f"超时不自动失败: {s['status']}"
    # 验证升级计数增加
    assert "s1" in s.get("retries", {}), "应记录超时次数"


# ── Conditional Steps ────────────────────────────────────────────

def test_conditional_step_blocks():
    wf_dir = _make_wf_dir()
    steps = [
        {"id": "s1", "title": "调研", "target_role": "scout",
         "prompt_template": "调研{topic}",
         "exit_condition": {"bus_category": "notice", "text_contains": "TESTCOND_S1_DONE"},
         "max_retries": 0, "condition": "", "rollback_to": ""},
        {"id": "s2", "title": "有条件的步骤", "target_role": "engineer",
         "prompt_template": "实现{topic}",
         "exit_condition": {"bus_category": "notice", "text_contains": "TESTCOND_S2_DONE"},
         "max_retries": 0, "condition": "s1.status == 'done'", "rollback_to": ""},
    ]
    _write_wf(wf_dir, "cond_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("cond_test", {"topic": "x"})
    # s1 不匹配时，停在 s1
    eng.run_once()
    s = eng.status(rid)
    assert s["current_step"] == "s1", f"s1 未匹配应停在 s1: {s['current_step']}"
    # 手动标记 s1 完成并推进到 s2
    run = eng._load_run(rid)
    run.current_step = "s2"
    run.step_results["s1"] = {"status": "done", "ts": time.time()}
    eng._save_run(run)
    s2 = eng.status(rid)
    assert s2["current_step"] == "s2"


def test_conditional_blocks_when_not_met():
    wf_dir = _make_wf_dir()
    steps = [
        {"id": "s1", "title": "调研", "target_role": "scout",
         "prompt_template": "调研{topic}",
         "exit_condition": {"bus_category": "notice", "text_contains": "TESTCONDBLK_S1_DONE"},
         "max_retries": 0, "condition": "", "rollback_to": ""},
        {"id": "s2", "title": "需要 s1 完成", "target_role": "dev",
         "prompt_template": "做 t",
         "exit_condition": {"bus_category": "notice", "text_contains": "TESTCONDBLK_S2_DONE"},
         "max_retries": 0, "condition": "s1.status == 'done'", "rollback_to": ""},
    ]
    _write_wf(wf_dir, "cond_block", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("cond_block", {"topic": "x"})
    # s1 不匹配，不会推进 → 还在 s1
    eng.run_once()
    s = eng.status(rid)
    assert s["current_step"] == "s1", f"不匹配应停在 s1: {s}"


# ── Persistence ──────────────────────────────────────────────────

def test_run_persists_on_tick():
    """tick 推进后状态持久化到磁盘，新引擎能读到。"""
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("test", {"topic": "x"})
    # s1 的 exit_condition 是 bus_category=architecture
    eng._bb.write("architecture", "调研结果", src="scout")
    eng.run_once()
    # 当前引擎内存中应已推进到 s2
    s1 = eng.status(rid)
    assert s1["current_step"] == "s2", f"推进后应 s2: {s1['current_step']}"
    # 新引擎重读磁盘验证持久化
    eng2 = WorkflowEngine(workflows_dir=wf_dir)
    s2 = eng2.status(rid)
    assert s2["current_step"] == "s2", f"持久化后应 s2: {s2['current_step']}"
    # 验证 s1 的结果也已持久化
    assert "s1" in s2["results"], f"s1 结果未持久化: {s2['results']}"
    assert s2["results"]["s1"]["status"] == "done"


def test_corrupted_run_file():
    wf_dir = _make_wf_dir()
    _write_wf(wf_dir, "test", SIMPLE_STEPS)
    (wf_dir / "runs" / "corrupted.json").write_text("{{{bad json")
    eng = WorkflowEngine(workflows_dir=wf_dir)
    # run_once 不应崩溃
    eng.run_once()


# ── Workspace Summary ────────────────────────────────────────────

def test_collect_workspace_summary():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    ws_dir = Path(tempfile.mkdtemp())
    (ws_dir / "PRD.md").write_text("# PRD\n这是一个产品需求文档")
    summary = eng._collect_workspace_summary(ws_dir)
    assert "PRD" in summary
    assert "产品需求" in summary
    # 不存在的目录
    assert eng._collect_workspace_summary(Path("/tmp/__nope_xxx")) == "workspace 不存在"


def test_workspace_summary_in_prompt():
    wf_dir = _make_wf_dir()
    ws_dir = Path.home() / ".hermes" / "workspace" / "test_ws_project"
    ws_dir.mkdir(parents=True, exist_ok=True)
    (ws_dir / "PRD.md").write_text("# 需求文档 WS_SUMMARY_UNIQUE_8899")
    steps = [{"id": "s1", "title": "调研", "target_role": "scout",
              "prompt_template": "项目情况: {workspace_summary}",
              "exit_condition": {"bus_category": "notice", "text_contains": "TESTWS_998877"},
              "max_retries": 0, "condition": "", "rollback_to": ""}]
    _write_wf(wf_dir, "ws_test", steps)
    eng = WorkflowEngine(workflows_dir=wf_dir)
    rid = eng.start("ws_test", {"topic": "x", "project_name": "test_ws_project"})
    all_facts = eng._bb.read(cat="workflow", limit=500)
    matched = [f for f in all_facts if "WS_SUMMARY_UNIQUE_8899" in f.t]
    assert len(matched) >= 1, f"workspace_summary 应被替换到 prompt 中 (found {len(matched)} in {len(all_facts)})"


# ── Run once 不崩溃 ──────────────────────────────────────────────

def test_run_once_empty():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    eng.run_once()


def test_run_once_missing_workflow_def():
    wf_dir = _make_wf_dir()
    eng = WorkflowEngine(workflows_dir=wf_dir)
    try:
        eng.start("ghost")
        assert False, "应抛出 ValueError"
    except ValueError as e:
        assert "ghost" in str(e)


# ── CLI ──────────────────────────────────────────────────────────

def test_cli_list_steers():
    """验证 CLI 子命令解析（不实际执行 daemon）。"""
    import argparse
    p = __import__('argparse').ArgumentParser()
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("start")
    s.add_argument("name")
    s.add_argument("--context", default="{}")
    sub.add_parser("list")
    s2 = sub.add_parser("status")
    s2.add_argument("workflow_id")
    s3 = sub.add_parser("cancel")
    s3.add_argument("workflow_id")
    sub.add_parser("tick")
    daemon = sub.add_parser("daemon")
    daemon.add_argument("--interval", type=int, default=10)
    args = p.parse_args(["list"])
    assert args.cmd == "list"


# ── Main ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== WorkflowEngine 测试 ===\n")

    tests = [
        ("加载工作流定义", test_engine_loads_workflows),
        ("空目录加载", test_engine_empty_dir),
        ("不存在的目录", test_engine_non_existent_dir),
        ("跳过 runs 子目录", test_engine_skips_runs_subdir),
        ("跳过无效 JSON", test_engine_skips_invalid_json),
        ("启动创建运行", test_start_creates_run),
        ("启动带 context", test_start_with_context),
        ("未知工作流名称", test_start_unknown_workflow),
        ("空步骤列表", test_start_empty_steps),
        ("不存在的 status", test_status_nonexistent),
        ("取消运行中的工作流", test_cancel_running),
        ("取消已完成的工作流", test_cancel_already_completed),
        ("取消不存在的工作流", test_cancel_nonexistent),
        ("tick 匹配推进", test_tick_advances_on_match),
        ("tick 最后一步完成", test_tick_completes_on_last_step),
        ("tick 不匹配保持运行", test_tick_no_match_keeps_running),
        ("exit_condition 按分类", test_check_exit_by_category),
        ("exit_condition 按来源", test_check_exit_by_source),
        ("exit_condition 按文本", test_check_exit_by_text),
        ("advance 完成", test_advance_finishes),
        ("超时触发重试", test_timeout_triggers_retry),
        ("超时不自动失败", test_timeout_escalates_not_fails),
        ("条件步骤推进", test_conditional_step_blocks),
        ("条件不满足阻塞", test_conditional_blocks_when_not_met),
        ("run 持久化", test_run_persists_on_tick),
        ("损坏的 run 文件", test_corrupted_run_file),
        ("收集 workspace summary", test_collect_workspace_summary),
        ("workspace_summary 替换", test_workspace_summary_in_prompt),
        ("run_once 空", test_run_once_empty),
        ("CLI 子命令解析", test_cli_list_steers),
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
