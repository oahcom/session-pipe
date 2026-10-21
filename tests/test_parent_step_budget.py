#!/usr/bin/env python3
"""子链排队预算必须取到父步声明，不能被模板名键空间失配吞掉。

现场（2026-10-21 06:44 UTC+8，bus blocker「排队超 60 分钟仍未投出」）：
  wf_sub_974230147e28（architect_full_design/s1，声明 timeout_minutes=15）
  父步 WL-full_dev_pipeline/s1 未声明 → 应回退 30min，预算 = 30×60×5 = 150min。
  实际判定上限 60min = 15×60×4 —— 父步的 30min 保护没生效。

根因：`_parent_step_minutes` 用 `self._workflows.get(裸名)` 查父模板，
而 `_workflows` 以 `WF-` 前缀注册（engine.py:441），get 返回 None →
return 0.0 → declared 只取子步自己的 15min。manager.py:174 / wf_lifecycle.py:803
都已有同款回退，此处漏了。

运行: cd ~/session-pipeline && python3 tests/test_parent_step_budget.py
"""
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lifecycle.manager import LifecycleManager       # noqa: E402
from pipeflow.step_handler import StepHandlerMixin   # noqa: E402

_STEPS = [{"step_id": "s1", "title": "设计", "target_role": "product_architect",
           "prompt_template": "do", "exit_condition": {}}]


def _with_db(fn):
    tmp = tempfile.mkdtemp(prefix="psb_")
    lm = LifecycleManager("engineer", db_path=os.path.join(tmp, "wf.db"))
    try:
        fn(lm)
    finally:
        try:
            lm._conn.close()
        except Exception:
            pass


def _seed(lm):
    """父链用裸名、子链挂 s1；内存缓存里只注册 WF- 前缀名（真实键空间）。"""
    lm._conn.execute(
        "INSERT INTO workflow_instances (instance_id, task_id, template_id, status, "
        "current_step_id, assigner, assignee, created_at, step_results) "
        "VALUES ('wf_p','t_p','WL-full_dev_pipeline','running','s1',"
        "'workflow_engine','product_architect',?,?)", (0, "{}"))
    lm._conn.execute(
        "INSERT INTO workflow_instances (instance_id, task_id, template_id, status, "
        "current_step_id, assigner, assignee, parent_wf_id, subflow_source_step_id, "
        "created_at, step_results) "
        "VALUES ('wf_sub_c','t_c','architect_full_design','running','s1',"
        "'workflow_engine','product_architect','wf_p','s1',?,?)", (0, "{}"))
    lm._conn.commit()


class _Handler(StepHandlerMixin):
    """只装配 _parent_step_minutes 用到的两个属性；其余 mixin 依赖不触碰。"""

    def __init__(self, workflows):
        self._workflows = workflows
        self._lifecycle = None


def test_parent_budget_found_via_wf_prefix():
    """父链存裸名、缓存注册 WF- 前缀 → 必须取到父步声明的 30min。

    变异验证：把 `or self._workflows.get(f"WF-{_ptid}")` 删掉，本用例失败。
    """
    def _run(lm):
        _seed(lm)
        parent_wf = SimpleNamespace(steps=[
            SimpleNamespace(id="s1", exit_condition={}),   # 未声明 → 回退 30
        ])
        inst = _Handler({"WF-WL-full_dev_pipeline": parent_wf})
        inst._lifecycle = lm
        got = inst._parent_step_minutes({"instance_id": "wf_sub_c"})
        assert got == 30.0, f"父步预算未取到（键空间失配）: {got!r}，期望 30.0"
    _with_db(_run)


def test_parent_budget_reads_declared_value():
    """父步显式声明 60min 时必须返回 60，不是默认 30。"""
    def _run(lm):
        _seed(lm)
        parent_wf = SimpleNamespace(steps=[
            SimpleNamespace(id="s1", exit_condition={"timeout_minutes": 60}),
        ])
        inst = _Handler({"WL-full_dev_pipeline": parent_wf})   # 裸名直接命中
        inst._lifecycle = lm
        got = inst._parent_step_minutes({"instance_id": "wf_sub_c"})
        assert got == 60.0, f"父步声明未生效: {got!r}"
    _with_db(_run)


def test_non_subflow_returns_zero():
    """非子链（无 parent_wf_id）返回 0，不因键空间回退误报。"""
    def _run(lm):
        _seed(lm)
        inst = _Handler({"WF-WL-full_dev_pipeline": SimpleNamespace(steps=[])})
        inst._lifecycle = lm
        got = inst._parent_step_minutes({"instance_id": "wf_p"})
        assert got == 0.0, f"非子链应返回 0: {got!r}"
    _with_db(_run)


def test_unknown_parent_template_returns_zero():
    """父模板两个键都查不到 → 0，不抛异常。"""
    def _run(lm):
        _seed(lm)
        inst = _Handler({})
        inst._lifecycle = lm
        got = inst._parent_step_minutes({"instance_id": "wf_sub_c"})
        assert got == 0.0, f"未知模板应返回 0: {got!r}"
    _with_db(_run)


if __name__ == "__main__":
    _fns = (test_parent_budget_found_via_wf_prefix,
            test_parent_budget_reads_declared_value,
            test_non_subflow_returns_zero,
            test_unknown_parent_template_returns_zero)
    _fails = 0
    for _fn in _fns:
        try:
            _fn()
            print(f"PASS {_fn.__name__}")
        except AssertionError as _e:
            _fails += 1
            print(f"FAIL {_fn.__name__}: {_e}")
    sys.exit(1 if _fails else 0)
