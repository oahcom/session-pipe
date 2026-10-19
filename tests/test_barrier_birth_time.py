#!/usr/bin/env python3
"""created_after 屏障须锚定「首次写入时间」，而非 facts.ts。

背景（2026-10-19 实证）：facts.t 是 UNIQUE，同文本重写走 UPSERT
（blackboard_legacy.py:238-242）会原地改写 ts 和 cat。生产实测：1698 条 fact
被重写（最多 1091 次、跨 54819s），36 条 exit_message 的 ts 因此从屏障前被抬到
屏障后 → 早已完成的旧消息「复活」，被后序步骤当作 exit 消费。

修复前 `_check_exit` 比较 `f.ts < created_after`，读的正是这个可变列。
log 表留存每次写入，取 MIN(ts) 即真实产生时刻。
"""
import sqlite3
import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src/pipeflow"))

from pipeflow.engine import WorkflowEngine


def _fact(mid: int, text: str, ts: float, src: str = "qa"):
    return type("F", (), {"id": mid, "t": text, "ts": ts, "src": src})()


def _eng(births: dict):
    """构造引擎，_bb.read 由调用方设置；log 表用内存库提供首次写入时间。"""
    with patch.object(WorkflowEngine, "__init__", lambda self: None):
        eng = WorkflowEngine()
    eng._bb = MagicMock()
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE log (ts REAL, typ TEXT, cat TEXT, fid INTEGER)")
    for fid, b in births.items():
        conn.execute("INSERT INTO log VALUES (?, 'write', 'x', ?)", (b, fid))

    @contextmanager
    def _conn():
        yield conn

    eng._bb._conn = _conn
    return eng


class TestBarrierAnchorsFirstWrite(unittest.TestCase):
    """重写抬高的 ts 不得让旧消息穿越屏障。"""

    def test_rewritten_message_does_not_cross_barrier(self):
        """写于屏障前、被重写抬到屏障后 → 不得匹配。"""
        now = time.time()
        eng = _eng({7: now - 1000})
        eng._bb.read = MagicMock(return_value=[
            _fact(7, "wf_x/s1-complete — 完成", now)])       # facts.ts 已被抬高
        _ts, msgs = eng._check_exit("workflow", "", "s1-complete", created_after=now - 10)
        self.assertEqual(_ts, 0.0, "重写抬高的 ts 不得让旧消息穿越屏障")
        self.assertEqual(msgs, [], "不应返回匹配消息")

    def test_fresh_message_still_matches(self):
        """首次写入在屏障之后 → 照常匹配（防过度拦截）。"""
        now = time.time()
        eng = _eng({8: now - 5})
        eng._bb.read = MagicMock(return_value=[
            _fact(8, "wf_y/s1-complete — 完成", now - 5)])
        _ts, msgs = eng._check_exit("workflow", "", "s1-complete", created_after=now - 10)
        self.assertTrue(_ts, "屏障后的新消息必须匹配")
        self.assertEqual(len(msgs), 1, "应匹配到 1 条消息")

    def test_missing_log_row_falls_back_to_facts_ts(self):
        """log 无记录时退化到 facts.ts，不得误拦。"""
        now = time.time()
        eng = _eng({})                                   # log 里没有 fid=9
        eng._bb.read = MagicMock(return_value=[_fact(9, "wf_z/s1-complete", now - 5)])
        _ts, _ = eng._check_exit("workflow", "", "s1-complete", created_after=now - 10)
        self.assertTrue(_ts, "log 缺记录时应退化到 facts.ts，不得误拦")

    def test_no_created_after_skips_birth_lookup(self):
        """created_after=None 时不做查询、不放行判定受影响。"""
        now = time.time()
        eng = _eng({10: now - 9999})
        eng._bb.read = MagicMock(return_value=[_fact(10, "wf_w/s1-complete", now - 5)])
        _ts, _ = eng._check_exit("workflow", "", "s1-complete")
        self.assertTrue(_ts, "不传 created_after 时行为不变")

    def test_wait_roles_path_also_uses_birth_time(self):
        """per-role 补查路径同样须用首次写入时间（第二处调用点）。"""
        now = time.time()
        eng = _eng({1: now - 1, 2: now - 1000})
        fresh = _fact(1, "wf_v/s1-complete — qa 完成", now - 1, "qa")
        stale = _fact(2, "wf_v/s1-complete — reviewer 完成", now, "reviewer")  # ts 被抬高
        # 第 1 次是主查询（stale 因不含 filter 被滤掉），第 2 次是 reviewer 补查
        eng._bb.read = MagicMock(side_effect=[[fresh, stale], [stale]])
        _ts, msgs = eng._check_exit("workflow", "", "s1-complete",
                                    created_after=now - 10, wait_roles=["qa", "reviewer"])
        self.assertEqual(_ts, 0.0,
                         "补查路径读到被抬高的 ts 会让协作门被旧消息满足")
        self.assertEqual(msgs, [], "不应返回匹配消息")


if __name__ == "__main__":
    unittest.main()
