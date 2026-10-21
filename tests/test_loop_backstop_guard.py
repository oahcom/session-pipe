#!/usr/bin/env python3
"""self-check for the loop bound backstop (_resolve_loop_max_iter).

Verifies the defense-in-depth guard that prevents "max_iterations<=0 AND no value_gate"
from spinning forever (bus #336367: maintainer 健康巡检 无限自旋事故).

Run: python3 -m pytest tests/test_loop_backstop_guard.py -x -q
"""
import logging
import inspect

from pipeflow.wf_lifecycle import _resolve_loop_max_iter, WorkflowLifecycleMixin

_logger = logging.getLogger("test")


def test_no_limit_and_no_gate_capped_to_one():
    # 历史事故模式：max_iterations=0 (∞) 且无 value_gate → 必须兜底降为 1
    assert _resolve_loop_max_iter(0, "", "wf_x", _logger) == 1
    assert _resolve_loop_max_iter(None, "", "wf_x", _logger) == 1


def test_explicit_limit_preserved():
    # 显式上限原样返回，不被兜底覆盖
    assert _resolve_loop_max_iter(3, "", "wf_x", _logger) == 3
    assert _resolve_loop_max_iter(1, "", "wf_x", _logger) == 1


def test_unbounded_with_gate_untouched():
    # food_chain_loop 类：无迭代上限但有 value_gate → 不触发兜底（保留 ∞ 语义）
    assert _resolve_loop_max_iter(0, "VALUE_AUDIT.md", "wf_fc", _logger) == 0


def test_unbounded_with_gate_workspace_only_still_capped():
    # 仅配了 workspace 但缺 file → 仍视为无价值门禁 → 兜底降到 1
    assert _resolve_loop_max_iter(0, "", "wf_x", _logger) == 1


def test_resolve_called_before_stop_guard():
    # 回归保护 (codex review 发现的顺序 bug): 若 _resolve_loop_max_iter 被误移到
    # 停止条件 `if _max_iter > 0 and _iteration >= _max_iter` 之后, 兜底值 1 永不到达
    # 守卫 → 无限自旋复活 (#336367). 此测试锁定"解算先于停止条件"的源码顺序。
    src = inspect.getsource(WorkflowLifecycleMixin._handle_wf_loop)
    resolve_pos = src.find("_resolve_loop_max_iter(")
    guard_pos = src.find("if _max_iter > 0 and _iteration >= _max_iter:")
    assert resolve_pos != -1 and guard_pos != -1, "expected both tokens present"
    assert resolve_pos < guard_pos, "防御性兜底必须排在停止条件之前"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print("all loop-backstop guard checks green")
