"""R118 链路完整性三处实证缺陷的回归守卫（只测外部行为）。

现场（2026-10-21，只读取证）：
  A) exit_schema 通配符匹配到的全是工作流创建前的旧文件时，`*.py`+minLength
     （无 minCount）返回 ok —— 实测两条 engineer 子链 11:30-11:45 workspace
     零写入仍被判完成，父链继续推进（Q3-A）。
  B) `wf confirm --reject` 只写 rejection_reason，_create_rework_subflow 只读
     exit_messages → 驳回意见建不出返工链，重投 3 次后步骤永久停摆
     （coordinator 2026-10-20 13:15 实录）。
  C) 跨实例守卫按 WL- 前缀全名精确匹配，角色发出的标题是裸模板名 → 守卫恒
     不生效，8.7 小时前兄弟实例的消息可闭合本步（Q3 第 7 类）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_r118_completion_integrity.py -q
"""
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeflow.step_handler as sh  # noqa: E402
from pipeflow.step_handler import StepHandlerMixin, _claims_own_step_and_foreign_instance  # noqa: E402
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402


def _eng(tmp_path, role="scout"):
    """最小引擎替身：只依赖 _validate_exit_schema 真正读的两个方法。"""
    eng = StepHandlerMixin.__new__(StepHandlerMixin)
    eng._eff_role = lambda run, step: role
    return eng


def test_stale_only_glob_fails(tmp_path, monkeypatch):
    """通配符只匹配到旧文件 = 本步没有产出，必须判失败。"""
    ws = tmp_path / "scout"
    ws.mkdir()
    old = ws / "legacy.py"
    old.write_text("x" * 500, encoding="utf-8")
    old_mtime = time.time() - 7 * 24 * 3600
    import os
    os.utime(old, (old_mtime, old_mtime))
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time(), workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is False, f"旧文件冒充产出仍被判通过: {errs}"
    assert errs, "失败时必须给出可核对的错误项"


def test_fresh_output_passes(tmp_path, monkeypatch):
    """本步真产出文件时不得误伤（守卫不为拦截而拦截）。"""
    ws = tmp_path / "scout"
    ws.mkdir()
    (ws / "new.py").write_text("y" * 500, encoding="utf-8")
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time() - 1, workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is True, f"本步新产出被判失败: {errs}"


def test_claim_ignores_workflow_id_prefix():
    """父链注册名带 WL- 前缀、角色发出的标题是裸名时，守卫必须命中。"""
    text = "wf_sub_deadbeef0001 · full_dev_pipeline/s4-complete 已产出"
    assert _claims_own_step_and_foreign_instance(
        text, "wf_sub_mine00000001", "WL-full_dev_pipeline", "s4") is True
    # 同实例自己的消息不算串味
    assert _claims_own_step_and_foreign_instance(
        "wf_sub_mine00000001 · full_dev_pipeline/s4-complete",
        "wf_sub_mine00000001", "WL-full_dev_pipeline", "s4") is False
    # 没声称本步完成的跨实例引用（coordinator 派发）仍要放行
    assert _claims_own_step_and_foreign_instance(
        "参考 wf_sub_deadbeef0001 的结论", "wf_sub_mine00000001",
        "WL-full_dev_pipeline", "s4") is False


class _ReworkHost(WorkflowLifecycleMixin):
    """只测驳回文本的取用来源，绕过 __init__ 与 DB 写入。"""

    def __init__(self):
        self._bb = MagicMock()
        self._workflows = {}
        self.captured = ""

    def _subflow_last_step_text(self, *a, **kw):
        return ""

    def _parse_review_issues(self, text):
        self.captured = text
        return ([], ["工程须补齐"], None, "engineer")


def test_reject_reason_feeds_rework():
    """wf confirm --reject 写的 rejection_reason 必须进返工解析。"""
    host = _ReworkHost()
    results = {"s2": {"status": "rejected", "rejection_reason":
                      "【业务问题→engineer】\n- P1 | 契约未满足 | a.py:1"}}
    ok = host._create_rework_subflow(
        "wf_x", MagicMock(_conn_tx=MagicMock(), query=MagicMock()),
        "tpl", "s2", SimpleNamespace(id="s2", title="review", target_role="reviewer"),
        SimpleNamespace(id="s3"), results)
    assert "P1 | 契约未满足" in host.captured, f"驳回理由未进入解析: {host.captured!r}"
    assert ok is False  # 走不到建链（lm 返回 None 行），但解析源已验证
