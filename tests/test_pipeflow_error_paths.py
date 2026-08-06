#!/usr/bin/env python3
"""
pipeflow 错误路径测试 — 覆盖 engine.py 关键未测试分支:
  1. _ensure_role_alive — tmux 检测 + CCS 拉起
  2. _send_to_role — 主命令失败降级到 fallback
  3. _load_workflows SQLite 分支 — 模板从 DB 加载

运行: cd /home/administrator/session-pipeline && python3 tests/test_pipeflow_error_paths.py
"""
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

# 必须在其他导入前禁用 Template 验证 (防止 hermes_bus 加载触发 TTL pruner)
os.environ.setdefault("SESSION_PIPELINE_SKIP_TTL_PRUNER", "1")

import pipeflow.engine as engine_mod
from pipeflow.engine import WorkflowEngine, WorkflowRun, Step

# ── Helpers ────────────────────────────────────────────────────────────


def _build_eng() -> WorkflowEngine:
    """手动构造 WorkflowEngine 实例，跳过 __init__ 的 DB/文件系统依赖。"""
    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng.workflows_dir = Path(tempfile.mkdtemp())
    eng.runs_dir = eng.workflows_dir / "runs"
    eng._workflows = {}
    eng._lm = MagicMock()
    eng._bb = MagicMock()
    eng._sync_step_results = MagicMock()
    return eng


def _make_run(**kw) -> WorkflowRun:
    defaults = dict(id="wf_test", workflow_name="test", context={},
                    current_step="s1", status="running",
                    step_results={"s1": {"status": "notified", "ts": time.time() - 5}})
    defaults.update(kw)
    return WorkflowRun(**defaults)


def _make_step(**kw) -> Step:
    defaults = dict(id="s1", title="t", target_role="test_role",
                    prompt_template="{topic}",
                    exit_condition={"bus_category": "code_fix"},
                    max_retries=0, condition="", rollback_to="")
    defaults.update(kw)
    return Step(**defaults)


# ═══════════════════════════════════════════════════════════════════════
# 1. _ensure_role_alive — tmux 子进程路径
# ═══════════════════════════════════════════════════════════════════════
# engine.py uses `import subprocess as _sp`, so patch pipeflow.engine._sp

def test_ensure_role_alive_already_running():
    """tmux has-session 返回 0 -> 直接返回 True。"""
    eng = _build_eng()
    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        assert eng._ensure_role_alive("scout")
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "has-session" in args
        assert "ccs-scout" in args


def test_ensure_role_alive_dead_started():
    """tmux 不在运行 -> subprocess 拉起 CCS -> 轮询直到存活 -> 返回 True。"""
    eng = _build_eng()
    returns = iter([MagicMock(returncode=1),
                    MagicMock(returncode=0),
                    MagicMock(returncode=0),
                    ])
    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.side_effect = lambda *a, **kw: next(returns)
        assert eng._ensure_role_alive("scout")
    start_calls = [c for c in mock_run.call_args_list if "start" in str(c)]
    assert len(start_calls) >= 1, "应调用 CCS start"


def test_ensure_role_alive_still_dead():
    """拉起 CCS 后仍不存活 -> 返回 False。"""
    eng = _build_eng()
    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1)
        assert not eng._ensure_role_alive("scout")


def test_ensure_role_alive_subprocess_raises():
    """CCS 拉起阶段 _sp.run 抛异常 -> 返回 False。"""
    eng = _build_eng()
    returns = iter([MagicMock(returncode=1),
                    FileNotFoundError("no ccs")])
    def _side(*a, **kw):
        v = next(returns)
        if isinstance(v, Exception):
            raise v
        return v
    with patch.object(engine_mod._sp, "run", side_effect=_side):
        assert not eng._ensure_role_alive("scout")


# ═══════════════════════════════════════════════════════════════════════
# 2. _send_to_role — 主命令失败 + fallback 路径
# ═══════════════════════════════════════════════════════════════════════

def test_send_to_role_primary_fails_fallback_succeeds():
    """主 CCS CLI subprocess 抛异常 -> fallback session-launcher 路径被调用。"""
    eng = _build_eng()
    eng._ensure_role_alive = MagicMock(return_value=True)

    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.side_effect = [FileNotFoundError("primary missing"),
                                MagicMock(returncode=0)]
        eng._send_to_role("scout", "test prompt", wf_id="wf_123", step_id="s1")

    assert mock_run.call_count >= 2, f"应调用 >=2 次: {mock_run.call_count}"
    bb_cats = [c.args[0] for c in eng._bb.write.call_args_list]
    assert "task_spec" in bb_cats


def test_send_to_role_both_fail_no_crash():
    """主命令 + fallback 都失败 -> 不崩溃。"""
    eng = _build_eng()
    eng._ensure_role_alive = MagicMock(return_value=True)

    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.side_effect = [FileNotFoundError("primary"),
                                FileNotFoundError("fallback")]
        eng._send_to_role("scout", "test prompt")

    bb_cats = [c.args[0] for c in eng._bb.write.call_args_list]
    assert "task_spec" in bb_cats


def test_send_to_role_creates_task_spec():
    """_send_to_role 应写入 task_spec 到 bus。"""
    eng = _build_eng()
    eng._ensure_role_alive = MagicMock(return_value=True)

    with patch.object(engine_mod._sp, "run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        eng._send_to_role("scout", "请调研 {topic}", wf_id="wf_abc", step_id="s1")

    bb_write_calls = eng._bb.write.call_args_list
    task_spec_calls = [c for c in bb_write_calls if c.args[0] == "task_spec"]
    assert len(task_spec_calls) >= 1, "应写入 task_spec"
    title = task_spec_calls[0].args[1]
    assert "needs_implementation" in title
    assert "wf_abc" in title


# ═══════════════════════════════════════════════════════════════════════
# 3. _load_workflows SQLite 分支
# ═══════════════════════════════════════════════════════════════════════

def _make_lm_row(template_id, name, description, steps_json):
    return {"template_id": template_id, "name": name,
            "description": description, "steps_json": steps_json}


def test_load_workflows_sqlite_basic():
    """SQLite 分支加载有效模板到 _workflows。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_sqlite_1", "SQLite Template", "描述",
                      json.dumps([{"step_id": "s1", "title": "步骤1",
                                    "target_role": "scout",
                                    "prompt_template": "你好",
                                    "exit_condition": {}}]))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert "tmpl_sqlite_1" in eng._workflows
    wf = eng._workflows["tmpl_sqlite_1"]
    assert wf.name == "tmpl_sqlite_1"
    assert wf.title == "SQLite Template"
    assert len(wf.steps) == 1
    assert wf.steps[0].id == "s1"
    assert wf.steps[0].target_role == "scout"
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_skip_duplicate():
    """SQLite 模板名称与已有 JSON 工作流重复 -> 跳过。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._workflows["existing_tmpl"] = "already loaded"
    eng._lm.query.return_value = [
        _make_lm_row("existing_tmpl", "Override", "",
                      json.dumps([{"step_id": "s1", "target_role": "scout",
                                    "prompt_template": "x", "exit_condition": {}}]))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert eng._workflows["existing_tmpl"] == "already loaded"
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_skip_missing_target_role():
    """steps_json 中某步骤缺少 target_role -> 该步骤被跳过，模板仍可加载。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    steps = [
        {"step_id": "s1", "title": "有效步骤", "target_role": "scout",
         "prompt_template": "调研", "exit_condition": {}},
        {"step_id": "s2", "title": "无效步骤",
         "prompt_template": "做某事", "exit_condition": {}},
    ]
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_skip_bad_step", "Filtrable", "", json.dumps(steps))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    wf = eng._workflows.get("tmpl_skip_bad_step")
    assert wf is not None, "模板应加载"
    step_ids = [s.id for s in wf.steps]
    assert "s1" in step_ids
    assert "s2" not in step_ids, "无 target_role 的步骤应被跳过"
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_skip_empty_steps():
    """空 steps 列表 -> 该模板不被注册。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_empty", "Empty", "", json.dumps([]))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert "tmpl_empty" not in eng._workflows
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_step_id_mapped():
    """SQLite 中 step_id 字段应映射到 Step.id。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_mapped", "Mapped", "",
                      json.dumps([{"step_id": "my_step_1", "title": "步骤1",
                                    "target_role": "scout",
                                    "prompt_template": "请执行",
                                    "exit_condition": {"bus_category": "notice"}}]))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    wf = eng._workflows.get("tmpl_mapped")
    assert wf is not None, f"template not loaded: {list(eng._workflows.keys())}"
    assert wf.steps[0].id == "my_step_1"
    assert wf.steps[0].exit_condition.get("bus_category") == "notice"
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_exception_graceful():
    """SQLite query 抛异常 -> 仅打印警告，不崩溃。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.side_effect = RuntimeError("DB 连接失败")

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_skip_non_dict_or_list():
    """steps_json 非 list/dict 格式 -> 跳过。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_bad_json", "Bad", "", '"not a list"')
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert "tmpl_bad_json" not in eng._workflows
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_skip_wrong_type():
    """steps_json 是 list 但元素不是 dict -> 空列表不注册。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_wrong_type", "Wrong", "",
                      json.dumps(["not_a_dict", 42]))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert "tmpl_wrong_type" not in eng._workflows
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_exit_condition_default():
    """SQLite 加载的步骤缺少 exit_condition -> 默认空 dict 不崩溃。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    steps = [{"step_id": "s1", "title": "步骤", "target_role": "scout",
              "prompt_template": "请执行"}]
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_no_exit", "NoExit", "", json.dumps(steps))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    wf = eng._workflows.get("tmpl_no_exit")
    assert wf is not None
    assert wf.steps[0].exit_condition == {}
    shutil.rmtree(str(test_dir))


def test_load_workflows_sqlite_step_creation_exception_skip():
    """验证 SQLite 分支不会因坏步骤崩溃。"""
    eng = _build_eng()
    test_dir = Path(tempfile.mkdtemp())
    eng.workflows_dir = test_dir
    steps = [{"step_id": "s1", "target_role": "scout",
              "prompt_template": "x", "exit_condition": {}}]
    eng._lm.query.return_value = [
        _make_lm_row("tmpl_ok", "OK", "", json.dumps(steps))
    ]

    with patch("pipeflow.engine._WORKFLOWS_DIR", test_dir):
        eng._load_workflows()

    assert "tmpl_ok" in eng._workflows
    shutil.rmtree(str(test_dir))


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=== pipeflow 错误路径测试 ===\n")

    tests = [
        # _ensure_role_alive
        ("ERA-1 已存活", test_ensure_role_alive_already_running),
        ("ERA-2 拉起后存活", test_ensure_role_alive_dead_started),
        ("ERA-3 拉起后仍死", test_ensure_role_alive_still_dead),
        ("ERA-4 subprocess 异常", test_ensure_role_alive_subprocess_raises),
        # _send_to_role
        ("STR-1 主失败降级", test_send_to_role_primary_fails_fallback_succeeds),
        ("STR-2 都失败不崩溃", test_send_to_role_both_fail_no_crash),
        ("STR-3 task_spec 写入", test_send_to_role_creates_task_spec),
        # _load_workflows SQLite
        ("LWS-1 SQLite 基本", test_load_workflows_sqlite_basic),
        ("LWS-2 跳过重复", test_load_workflows_sqlite_skip_duplicate),
        ("LWS-3 跳过缺 target_role", test_load_workflows_sqlite_skip_missing_target_role),
        ("LWS-4 跳过空 steps", test_load_workflows_sqlite_skip_empty_steps),
        ("LWS-5 step_id 映射", test_load_workflows_sqlite_step_id_mapped),
        ("LWS-6 SQLite 异常", test_load_workflows_sqlite_exception_graceful),
        ("LWS-7 跳过非 list", test_load_workflows_sqlite_skip_non_dict_or_list),
        ("LWS-8 跳过错误类型", test_load_workflows_sqlite_skip_wrong_type),
        ("LWS-9 exit_condition 默认", test_load_workflows_sqlite_exit_condition_default),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  OK  {name}")
        except Exception as e:
            print(f"  X   {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
