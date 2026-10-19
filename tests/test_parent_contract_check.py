"""子链完成 ≠ 父步交付契约达成的回归守卫（pytest 风格）。

背景：父步（handoff 容器）有自己的 exit_schema，而干活的是子链——子链模板是
另一个文件，未必产出父步要的每一项。实测 wf_26f108c663e4/s2（architect_review
要 REVIEW.md，子链 architect_full_design 从不产出该文件）：子链全绿、REVIEW.md
从未落盘、父步照样 completed 且推进。全历史 161 个「有 schema 且已推进」的父步
中 23 个契约未达成，此前全部静默无记录。

用唯一文件名写真实 scout workspace（与 tests/test_exit_condition.py 同惯例），
用完即删。
"""

import os
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from paths import CCS_WORKSPACES as _CCSWS  # noqa: E402
from pipeflow.step_handler import StepHandlerMixin  # noqa: E402
from pipeflow.task_generator import TaskGeneratorMixin  # noqa: E402

_WS = _CCSWS / "scout"
_UNIQUE = f"PC_{int(time.time() * 1000) % 1_000_000:06d}"


class _Eng(TaskGeneratorMixin, StepHandlerMixin):
    """最小 engine 替身：_parent_contract_errs 在 task_generator，校验在 step_handler。"""

    def __init__(self, workflows=None):
        self._workflows = workflows or {}


def _step(exit_schema=None, sid="s1"):
    return SimpleNamespace(id=sid, exit_schema=exit_schema, target_role="scout")


def _fresh_file(name, content="x" * 200):
    _WS.mkdir(parents=True, exist_ok=True)
    p = _WS / name
    p.write_text(content, encoding="utf-8")
    return p


def test_missing_required_file_is_unmet():
    """required 文件不存在 → 契约未达成。"""
    name = f"{_UNIQUE}_MISSING.md"
    eng = _Eng()
    run = SimpleNamespace(id="wf_t", created_at=1.0, effective_roles={}, step_results={})
    ok, errs = eng._validate_exit_schema(_step({"required": [name]}), run)
    assert not ok and any(name in e for e in errs), errs


def test_stale_file_does_not_satisfy_contract():
    """文件存在但早于工作流创建 → 不算本次产出（陈旧文件绕过）。"""
    name = f"{_UNIQUE}_STALE.md"
    _fresh_file(name)
    eng = _Eng()
    run = SimpleNamespace(id="wf_t", created_at=9_999_999_999.0,
                          effective_roles={}, step_results={})
    ok, errs = eng._validate_exit_schema(
        _step({"required": [name], "properties": {name: {"minLength": 50}}}), run)
    assert not ok, "陈旧文件不应满足契约（mtime 早于工作流创建）"


def test_fresh_file_with_content_is_met():
    """新鲜且内容达标 → 契约达成。"""
    name = f"{_UNIQUE}_OK.md"
    _fresh_file(name, "APPROVED " * 40)
    eng = _Eng()
    run = SimpleNamespace(id="wf_t", created_at=1.0, effective_roles={}, step_results={})
    ok, errs = eng._validate_exit_schema(
        _step({"required": [name],
               "properties": {name: {"minLength": 50, "mustContain": ["APPROVED"]}}}), run)
    assert ok, errs


def test_no_schema_is_vacuous_pass():
    """无 exit_schema → 无事可判，返回 ok（守卫不得成为新的卡死源）。"""
    eng = _Eng()
    run = SimpleNamespace(id="wf_t", created_at=1.0, effective_roles={}, step_results={})
    ok, errs = eng._validate_exit_schema(_step(None), run)
    assert ok and not errs


def test_parent_contract_errs_reads_db_and_template():
    """端到端：从 DB 读实例 + 从 _workflows 取父模板 schema，报告缺失项。"""
    name = f"{_UNIQUE}_DB.md"
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE workflow_instances "
                 "(instance_id TEXT PRIMARY KEY, template_id TEXT, created_at REAL, "
                 " effective_roles TEXT, step_results TEXT)")
    conn.execute("INSERT INTO workflow_instances VALUES (?,?,?,?,?)",
                 ("wf_pc", "tpl", 1.0, "{}", "{}"))
    eng = _Eng(workflows={"tpl": SimpleNamespace(steps=[_step({"required": [name]})])})
    errs = eng._parent_contract_errs(conn, "wf_pc", "s1")
    assert any(name in e for e in errs), errs


def test_parent_contract_errs_fail_open_on_bad_input():
    """查不到实例/模板 → 返回空表（fail-open），不得抛异常中断 tick。"""
    eng = _Eng()
    assert eng._parent_contract_errs(None, "wf_x", "s1") == []


def test_effective_role_redirect_uses_executor_workspace():
    """equivalent-role split: effective_roles={"s1":"pg"} → 按 pg 目录校验。

    回归保护：_contract_run 必须把 DB 里的字符串包进 _inst_row，否则
    _eff_role 回退模板角色，pg 的产出被当成缺失（假 contract_unmet）。
    """
    from paths import CCS_WORKSPACES as _WSROOT
    name = f"{_UNIQUE}_REROUTE.md"
    pg = _WSROOT / "pg"
    pg.mkdir(parents=True, exist_ok=True)
    (pg / name).write_text("x" * 200, encoding="utf-8")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE workflow_instances "
                 "(instance_id TEXT PRIMARY KEY, template_id TEXT, created_at REAL, "
                 " effective_roles TEXT, step_results TEXT)")
    conn.execute("INSERT INTO workflow_instances VALUES (?,?,?,?,?)",
                 ("wf_pc2", "tpl", 1.0, '{"s1":"pg"}', "{}"))
    # 模板角色 scout 目录没有该文件；只有按 pg 校验才会 PASS
    eng = _Eng(workflows={"tpl": SimpleNamespace(steps=[_step({"required": [name]})])})
    errs = eng._parent_contract_errs(conn, "wf_pc2", "s1")
    try:
        assert not errs, f"pg 产出不应报缺失: {errs}"
    finally:
        (pg / name).unlink(missing_ok=True)


def teardown_module(module):
    for p in _WS.glob(f"{_UNIQUE}*"):
        p.unlink(missing_ok=True)
