#!/usr/bin/env python3
"""步级 delivery_blocked 必须可被恢复扫描看见 — 防"行级活跃"的永久孤儿。

根因（2026-10-19 生产实测）：
  _tick 达投递上限只把 **step_results 里该步** 写成 delivery_blocked，
  close_wf（打行级终态）只在特定分支被调用 → 绝大多数实例**行级仍是 running**。
  而 _recover_delivery_blocked 的扫描条件是 `WHERE status='delivery_blocked'`
  （行级）→ 这类实例对本函数完全不可见：既不推进也不被回收
  （run_once 只查 pending/running/step_done_ready，_cleanup_stale 只查
  running/step_done_ready）→ 永久孤儿。

实测生产库：行级 delivery_blocked = 0 条，步级 = 239 条。存活父链
wf_22b96b1b0c 的 s3 正是该形态（行级 running + 步级 blocked），恢复机制
对它失效 40 分钟，是"pm→架构→开发→测试 自发贯通"断在 s3 的直接原因。

契约（三条，缺一则该形态重回孤儿）：
  1. 扫描同时覆盖行级 delivery_blocked 与"行级活跃 + 步级 blocked"
  2. 达恢复上限的收敛 UPDATE 两个行级形态都能命中（否则写 blocker 仍孤儿）
  3. 行级还原不得无条件覆盖（会抹掉 step_done_ready 的待推进标记）

运行: cd /home/administrator/session-pipeline && python3 tests/test_delivery_blocked_step_level.py
"""
import re
import sqlite3
import sys
from pathlib import Path

_SRC_FILE = Path(__file__).resolve().parents[1] / "src" / "pipeflow" / "engine.py"
_LIVE_DB = Path.home() / ".hermes" / "state" / "workflows.db"


def _recover_fn_body() -> str:
    code = _SRC_FILE.read_text(encoding="utf-8")
    m = re.search(r"def _recover_delivery_blocked\(self\).*?(?=\n    def |\ndef )",
                  code, re.S)
    assert m, "未找到 _recover_delivery_blocked（被重命名？）"
    return m.group(0)


def test_scan_covers_step_level_blocked():
    """扫描条件必须含'行级活跃 + 步级 blocked'分支。"""
    body = _recover_fn_body()
    assert "step_results LIKE '%delivery_blocked%'" in body, \
        "扫描缺少步级分支：行级 running 的 blocked 实例永不可见"
    assert "status = 'delivery_blocked'" in body, \
        "扫描丢了行级分支：close_wf 打的终态实例会失去恢复路径"
    assert re.search(r"status IN \('pending','running','step_done_ready'\)", body), \
        "步级分支未限定行级活跃集（会扫到 cancelled/failed 的历史行）"


def test_convergence_update_matches_both_shapes():
    """达上限的收敛 UPDATE 必须两个行级形态都能命中。"""
    body = _recover_fn_body()
    m = re.search(r"SET status='failed'\s*\"?\s*\"?WHERE instance_id=\?", body)
    assert m, "未找到收敛 UPDATE"
    assert "status IN " in body and "'delivery_blocked'" in body.split("SET status='failed'")[1][:200], \
        "收敛 UPDATE 只认行级 delivery_blocked：步级形态命中 0 行，写 blocker 后仍是孤儿"


def test_restore_does_not_clobber_step_done_ready():
    """行级还原必须条件化，不能无条件写 running。"""
    body = _recover_fn_body()
    assert "CASE WHEN status='delivery_blocked' THEN 'running'" in body, \
        "行级还原未条件化：会把 step_done_ready 的待推进标记抹成 running"
    assert not re.search(r"SET step_results=\?, status='running'", body), \
        "仍存在无条件行级覆盖"


def test_live_db_step_level_orphans_are_visible():
    """对生产库实跑扫描条件：步级 blocked 的存活实例必须被选中。"""
    if not _LIVE_DB.exists():
        print("    (跳过：生产库不存在)"); return
    c = sqlite3.connect(f"file:{_LIVE_DB}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    rows = list(c.execute(
        "SELECT instance_id, status FROM workflow_instances "
        "WHERE status = 'delivery_blocked' "
        "   OR (status IN ('pending','running','step_done_ready') "
        "       AND step_results LIKE '%delivery_blocked%')"))
    old = list(c.execute(
        "SELECT instance_id FROM workflow_instances WHERE status='delivery_blocked'"))
    assert len(rows) >= len(old), \
        f"新扫描命中 {len(rows)} 条 < 旧扫描 {len(old)} 条：行级分支被削弱"
    print(f"    新扫描命中 {len(rows)} 条（旧扫描 {len(old)} 条）")


if __name__ == "__main__":
    _fns = (test_scan_covers_step_level_blocked,
            test_convergence_update_matches_both_shapes,
            test_restore_does_not_clobber_step_done_ready,
            test_live_db_step_level_orphans_are_visible)
    _fails = 0
    for _fn in _fns:
        try:
            _fn()
            print(f"  OK  {_fn.__name__}")
        except AssertionError as _e:
            _fails += 1
            print(f"  FAIL {_fn.__name__}: {_e}")
    print(f"\n结果: {len(_fns) - _fails} 通过, {_fails} 失败")
    sys.exit(1 if _fails else 0)
