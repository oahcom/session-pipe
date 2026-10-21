"""实例循环排序：最饿的先吃（R132）。

生产证据（2026-10-22）：tick 预算 60s，超预算即 break 本轮剩余实例；
实例查询无 ORDER BY 时 SQLite 按插入序遍历 → 尾部实例每轮都排不到。
实测 22 条在飞实例 150s 内零变化，wf_rw_2664ec3aada3 等 3 条返工链
queued 卡在 235 不动，最后被回收器当僵尸回收。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_instance_order.py -q
"""
import sqlite3

from pipeflow.engine import _INSTANCE_ORDER


def _db():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE workflow_instances ("
               "instance_id TEXT, current_step_id TEXT, step_results TEXT)")
    return db


def test_never_dispatched_sorts_first():
    """没有 ts（从未派出去）的实例必须排最前：它正是被预算 break 饿死的那一批。"""
    db = _db()
    db.executemany("INSERT INTO workflow_instances VALUES (?,?,?)", [
        ("wf_fresh", "s1", '{"s1":{"status":"notified","ts":1792618786.3}}'),
        ("wf_starved", "s1", '{"s1":{"queued":235}}'),
    ])
    rows = db.execute("SELECT instance_id FROM workflow_instances " + _INSTANCE_ORDER).fetchall()
    assert [r[0] for r in rows] == ["wf_starved", "wf_fresh"]


def test_least_recently_moved_first():
    """已派出的实例按 ts 升序：最久没动的先吃，最近动的排后面。"""
    db = _db()
    db.executemany("INSERT INTO workflow_instances VALUES (?,?,?)", [
        ("wf_mid", "s1", '{"s1":{"ts":1000}}'),
        ("wf_new", "s1", '{"s1":{"ts":3000}}'),
        ("wf_old", "s2", '{"s2":{"ts":500}}'),
    ])
    rows = db.execute("SELECT instance_id FROM workflow_instances " + _INSTANCE_ORDER).fetchall()
    assert [r[0] for r in rows] == ["wf_old", "wf_mid", "wf_new"]
