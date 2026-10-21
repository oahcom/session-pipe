#!/usr/bin/env python3
"""越界计数不得按 tick 累加（bus #418065 去重诉求）。

pane 回显会保留已执行命令多轮；旧实现每个 tick 都 +1，
一次越界 5 个 tick 后就 cancel_step（wf_6ff71a38016c 的 s1 累计 26 次）。
契约：同一批命令集合只计一次；出现新命令才 +1。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.step_handler import _bump_violation_count


def test_same_commands_counted_once():
    """同批命令连续 5 个 tick → 计数保持 1，不触发 cancel 阈值。"""
    state = {}
    for _ in range(5):
        n = _bump_violation_count(state, ["edit_config: systemctl restart"])
    assert n == 1, f"同批命令应只计 1 次，实得 {n}"
    assert state["violation_count"] == 1


def test_new_commands_increment():
    """出现新命令 → 计为新一次越界。"""
    state = {}
    assert _bump_violation_count(state, ["edit_config: a"]) == 1
    assert _bump_violation_count(state, ["edit_config: b"]) == 2
    assert _bump_violation_count(state, ["edit_config: b"]) == 2, "重复命令不再累加"


def test_duplicate_items_in_one_batch_are_set():
    """同一批里的重复项按集合计（顺序不同也算同一批）。"""
    state = {}
    assert _bump_violation_count(state, ["x", "y", "x"]) == 1
    assert _bump_violation_count(state, ["y", "x"]) == 1, "同集合不同顺序不重复计数"


def test_empty_commands_still_count():
    """命令列表为空但 violation_found 为真 → 仍计一次（不能把越界吞掉）。"""
    state = {}
    assert _bump_violation_count(state, []) == 1
    assert _bump_violation_count(state, []) == 2, "空集合再次命中视为新一次，防吞信号"


if __name__ == "__main__":
    for _name, _fn in list(globals().items()):
        if _name.startswith("test_"):
            _fn()
    print("SELF-TEST PASS（4 例）")
