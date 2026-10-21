"""主循环轮转：预算中断后从断点继续，任何实例都不会被永久跳过（R132）。

生产证据（2026-10-22 05:40）：tick 预算 60s 超限即 break 本轮剩余实例，而查询
无 ORDER BY（SQLite 按插入序）→ 尾部实例每轮都排不到：22 条在飞实例 150s 内
零变化，wf_rw_2664ec3aada3 等 3 条返工链 queued 卡在 235 从未投出，父链被回收。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_loop_rotation.py -q
"""
from pipeflow.engine import _rotate_after


def _rows(*ids):
    return [{"instance_id": i} for i in ids]


def test_resume_after_last_processed():
    rows = _rows("a", "b", "c", "d")
    assert [r["instance_id"] for r in _rotate_after(rows, "b")] == ["c", "d", "a", "b"]


def test_wraps_when_breakpoint_is_last():
    rows = _rows("a", "b", "c")
    assert [r["instance_id"] for r in _rotate_after(rows, "c")] == ["a", "b", "c"]


def test_no_resume_id_keeps_order():
    rows = _rows("a", "b")
    assert [r["instance_id"] for r in _rotate_after(rows, "")] == ["a", "b"]


def test_unknown_resume_id_keeps_order():
    """断点实例已终态/被删：保持原序，不丢任何实例。"""
    rows = _rows("a", "b")
    assert [r["instance_id"] for r in _rotate_after(rows, "gone")] == ["a", "b"]


def test_rotation_gives_every_instance_a_turn():
    """模拟预算只够 2 条：连续 3 轮必须覆盖全部 5 条（按插入序则永远只有前 2 条）。"""
    ids = ["a", "b", "c", "d", "e"]
    rows = _rows(*ids)
    seen, resume = set(), ""
    for _ in range(3):
        rotated = _rotate_after(rows, resume)
        for x in rotated[:2]:
            seen.add(x["instance_id"])
        resume = rotated[1]["instance_id"]
    assert seen == set(ids)
