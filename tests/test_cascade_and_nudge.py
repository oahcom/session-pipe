#!/usr/bin/env python3
"""父链判死必须级联子链。

现场：72h 内 76 条子链挂在已 cancelled/failed 的父链下，wf_sub_ee03a71c3
在父链死后仍 running 21min（不级联 → 孤儿占角色、产出无人消费）。
"""
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lifecycle.manager import LifecycleManager       # noqa: E402
from pipeflow.step_handler import StepHandlerMixin   # noqa: E402
import pipeflow.wf_lifecycle as wf_lifecycle        # noqa: E402

_STEPS = [{"step_id": "s1", "title": "产出", "target_role": "engineer",
           "prompt_template": "do", "exit_condition": {"bus_category": "task_spec"}}]


def _lm(path):
    lm = LifecycleManager("engineer", db_path=path)
    lm.upsert_template("tpl", "模板", "d", _STEPS,
                       trigger_scene=["t"], allowed_initiators=["engineer"],
                       allowed_executors=["engineer"])
    return lm


def _start(lm, wid):
    lm.start_wf(wid, "s1", "tpl", initiator="engineer", assignee="engineer")


def _add_child(lm, parent_id, child_id):
    now = time.time()
    lm._conn.execute(
        "INSERT INTO workflow_instances (instance_id, template_id, task_id, assigner,"
        " assignee, status, current_step_id, step_results, created_at,"
        " parent_wf_id, subflow_source_step_id) "
        "VALUES (?, 'tpl', ?, 'engineer', 'engineer', 'running', 's1', '{}', ?, ?, 's1')",
        (child_id, "task_" + child_id, now, parent_id))
    lm._conn.execute(
        "INSERT INTO tasks (task_id, title, assigner, assignee, status, created_at, updated_at) "
        "VALUES (?, 'c', 'engineer', 'engineer', 'running', ?, ?)",
        ("task_" + child_id, now, now))
    lm._conn.commit()


def _status(lm, wid):
    return lm._conn.execute(
        "SELECT status FROM workflow_instances WHERE instance_id=?", (wid,)).fetchone()["status"]


def _with_db(fn):
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    lm = _lm(path)
    try:
        fn(lm)
    finally:
        lm.close()
        os.unlink(path)


def test_cancel_parent_cascades_to_child():
    """父链 cancelled → 子链与其 task 同步 cancelled，不留孤儿。"""
    def _run(lm):
        _start(lm, "wf_p")
        _add_child(lm, "wf_p", "wf_c")
        lm.close_wf("wf_p", status="cancelled")
        assert _status(lm, "wf_c") == "cancelled", "父链取消后子链仍活"
        t = lm._conn.execute(
            "SELECT status FROM tasks WHERE task_id='task_wf_c'").fetchone()
        assert t["status"] == "cancelled", "子链的 task 未同步取消"
    _with_db(_run)


def test_cascade_child_log_inherits_parent_reason():
    """子链的 wf_cancelled 必须带父链原因，否则排查要回查父链才知道为什么死。"""
    def _run(lm):
        _start(lm, "wf_pr")
        _add_child(lm, "wf_pr", "wf_cr")
        lm.close_wf("wf_pr", status="cancelled", reason="queue 45min >= 30min")
        d = lm._conn.execute(
            "SELECT detail FROM workflow_logs WHERE workflow_instance_id='wf_cr'"
            " AND action='wf_cancelled'").fetchone()["detail"]
        assert "queue 45min >= 30min" in d, f"子链日志未继承父链原因: {d!r}"
    _with_db(_run)

def test_complete_parent_does_not_cancel_child():
    """completed 不级联：正常收尾时子链已自行完成，误杀会丢失产出。"""
    def _run(lm):
        _start(lm, "wf_p2")
        _add_child(lm, "wf_p2", "wf_c2")
        lm.close_wf("wf_p2", status="completed")
        assert _status(lm, "wf_c2") == "running", "completed 不应取消子链"
    _with_db(_run)


def test_delivery_blocked_parent_cascades_to_child():
    """投递达上限也是判死：父链 delivery_blocked 后无人再消费子链产出 → 必须级联。"""
    def _run(lm):
        _start(lm, "wf_db")
        _add_child(lm, "wf_db", "wf_db_c")
        lm.close_wf("wf_db", status="delivery_blocked")
        assert _status(lm, "wf_db_c") == "cancelled", "父链 delivery_blocked 后子链继续占角色"
    _with_db(_run)


def test_repeat_close_is_idempotent():
    """多条收尾路径先后关同一台 → 不得重复写日志或改写 completed_at。"""
    def _run(lm):
        _start(lm, "wf_i")
        lm.close_wf("wf_i", status="completed")
        first = lm.get_wf("wf_i")["completed_at"]
        n_logs = lm._conn.execute(
            "SELECT COUNT(*) n FROM workflow_logs WHERE workflow_instance_id='wf_i'"
            " AND action='wf_closed'").fetchone()["n"]
        assert lm.close_wf("wf_i", status="completed") is True
        assert lm.get_wf("wf_i")["completed_at"] == first, "completed_at 被第二次改写"
        assert lm._conn.execute(
            "SELECT COUNT(*) n FROM workflow_logs WHERE workflow_instance_id='wf_i'"
            " AND action='wf_closed'").fetchone()["n"] == n_logs, "重复写 wf_closed 日志"
    _with_db(_run)


def test_repeat_close_heals_orphan_child():
    """父链已 cancelled 但子链还活着、task 仍 running（旧代码遗留）→ 再次 close 两样都补。"""
    def _run(lm):
        _start(lm, "wf_r")
        _tid = lm.get_wf("wf_r")["task_id"]
        lm.close_wf("wf_r", status="cancelled")
        _add_child(lm, "wf_r", "wf_c_orphan")   # 旧代码留下的孤儿
        # 旧代码裸 UPDATE 关实例却漏同步 task：把该存量演回现场再 close。
        lm.execute("UPDATE tasks SET status='running' WHERE task_id=?", (_tid,))
        lm.close_wf("wf_r", status="cancelled")
        assert _status(lm, "wf_c_orphan") == "cancelled", "幂等短路吃掉了级联，孤儿滞留"
        assert lm._conn.execute("SELECT status FROM tasks WHERE task_id=?", (_tid,)
                                ).fetchone()["status"] == "cancelled", \
            "幂等早退漏了父 task 同步，legacy task 永久 running"
    _with_db(_run)


def test_terminal_state_is_irreversible():
    """终态之间不得互相覆写：cancelled 的链被后续终态关闭改回去会诈尸。

    实测（2026-10-21）：先 close(cancelled) 再 close(completed)，实例状态变成
    completed。回收/超时兜底/手工作业多条收尾路径任意先后到达即可触发，
    一条已判死、子链已级联取消的链会变成“完成”——完成率统计与下游消费全被骗。

    必须用 cancelled → failed 验证守卫先于级联：completed 分支本身不级联，
    改回 completed 测不出顺序问题（首版测试就是这么漏掉的，变异 M9 未被捕获）。
    """
    def _run(lm):
        _start(lm, "wf_t")
        lm.close_wf("wf_t", status="cancelled")
        _add_child(lm, "wf_t", "wf_t_c")
        assert lm.close_wf("wf_t", status="failed") is False, "终态回退未被拒绝"
        assert _status(lm, "wf_t") == "cancelled", "已判死的链被改回 failed（诈尸）"
        assert _status(lm, "wf_t_c") == "running", \
            "被拒绝的 close 不该级联杀子链——守卫先于级联，否则子死父活倒挂"
        # completed 也是终态：不得覆写 cancelled
        assert lm.close_wf("wf_t", status="completed") is False, "completed 同样不可回退"
        assert _status(lm, "wf_t") == "cancelled"
    _with_db(_run)


def test_unknown_close_status_is_rejected():
    """拼错状态（canceled）会让行入终态却不级联 → 必须直接拒绝。"""
    def _run(lm):
        _start(lm, "wf_u")
        try:
            lm.close_wf("wf_u", status="canceled")
        except ValueError:
            pass
        else:
            raise AssertionError("未知状态被接受：子链将不会级联")
        assert lm.get_wf("wf_u")["status"] == "running", "拒绝后不得改状态"
    _with_db(_run)


def test_reclaim_end_to_end_cancels_child():
    """端到端：真实 LifecycleManager + 真实回收路径 → 子链随父链一起 cancelled。

    上面的 _LmStub 用例只验 wiring；这里跑真库真事务，覆盖 task 同步与级联实现。
    """
    import json
    def _run(lm):
        from pipeflow.wf_lifecycle import WorkflowLifecycleMixin
        _start(lm, "wf_e2e")
        _add_child(lm, "wf_e2e", "wf_e2e_sub")
        # 停滞必须超过排队预算（默认声明 30min × 4 = 7200s）才构成条件4：
        # tc>=1 只证明"超时过"，判死还要"真的没在动"（两道信号 AND）。
        sd = {"s1": {"timeout_count": 2, "ts": time.time() - 10800, "queued": 0}}
        lm.execute("UPDATE workflow_instances SET created_at=?, step_results=?"
                   " WHERE instance_id=?",
                   (time.time() - 7200, json.dumps(sd), "wf_e2e"))
        from types import SimpleNamespace as _SN
        inst = _SN(_workflows={}, _notify_role=lambda *a, **k: None, _lifecycle=lm)
        _orig = wf_lifecycle._role_producing_recently
        wf_lifecycle._role_producing_recently = lambda role, **k: False
        try:
            WorkflowLifecycleMixin._cleanup_stale_workflows(inst, lm)
        finally:
            wf_lifecycle._role_producing_recently = _orig
        assert _status(lm, "wf_e2e") == "cancelled", "父链未被回收"
        assert _status(lm, "wf_e2e_sub") == "cancelled", "子链未随父链级联取消"
        assert lm._conn.execute(
            "SELECT status FROM tasks WHERE task_id='task_wf_e2e_sub'"
        ).fetchone()["status"] == "cancelled", "子链 task 未同步"

    from unittest.mock import patch
    with patch("liveness.tmux_pane_active", lambda role, **k: False):
        _with_db(_run)


def test_timeout_marker_alone_does_not_kill():
    """只凭 timeout_count>=1 判死会杀掉正在干活的角色。

    实测：qa 子链 tc=1、停滞 0.2h 被回收，而角色在跑别人的 prompt，8.5min 后重投即恢复。
    条件4 是「死因信号 AND 停滞 ≥ 排队预算」，两条必须同时成立。
    """
    import json
    def _run(lm):
        from pipeflow.wf_lifecycle import WorkflowLifecycleMixin
        _start(lm, "wf_fresh")
        sd = {"s1": {"timeout_count": 3, "ts": time.time() - 600, "queued": 0}}
        lm.execute("UPDATE workflow_instances SET created_at=?, step_results=?"
                   " WHERE instance_id=?",
                   (time.time() - 7200, json.dumps(sd), "wf_fresh"))
        inst = SimpleNamespace(_workflows={}, _notify_role=lambda *a, **k: None,
                               _lifecycle=lm)
        _orig = wf_lifecycle._role_producing_recently
        wf_lifecycle._role_producing_recently = lambda role, **k: False
        try:
            WorkflowLifecycleMixin._cleanup_stale_workflows(inst, lm)
        finally:
            wf_lifecycle._role_producing_recently = _orig
        assert _status(lm, "wf_fresh") == "running", "10 分钟前才产出的角色被当成卡死回收"
    with patch("liveness.tmux_pane_active", lambda role, **k: False):
        _with_db(_run)


def test_ccs_parser_rejects_from_flag():
    """真实 argparse 复核旧参数确实 rc=2。

    必须同时带 --by-role：只给 --from 时"缺必填参数"也返 rc=2，测不出
    "--from 被拒"。目标角色故意不存在 + --no-auto-wake：万一 --from 被接受，
    force_send 只返回"不在线"，不会向任何真实角色发消息。
    """
    from workflow.client import get_ccs_cli
    import subprocess
    cli = str(get_ccs_cli())
    bad = subprocess.run(["python3", cli, "send-safe", "__no_such_role__", "m",
                          "--by-role", "workflow_engine", "--from", "workflow_engine",
                          "--no-auto-wake"],
                         capture_output=True, timeout=30)
    assert bad.returncode == 2, f"--from 竟未被拒绝（rc={bad.returncode}），用例前提失效"
    src = (Path(cli).parent / "ccs.py").read_text(encoding="utf-8")
    _blk = src[src.index('"send-safe"'):src.index('"codex"')]
    assert '"--by-role"' in _blk, "send-safe 不再接受 --by-role，催办通道需重挑参数"


if __name__ == "__main__":
    tests = [
        ("父链取消级联子链", test_cancel_parent_cascades_to_child),
        ("completed 不误杀子链", test_complete_parent_does_not_cancel_child),
        ("delivery_blocked 也级联", test_delivery_blocked_parent_cascades_to_child),
        ("重复关闭幂等", test_repeat_close_is_idempotent),
        ("终态不可回退", test_terminal_state_is_irreversible),
        ("重复关闭仍级联孤儿", test_repeat_close_heals_orphan_child),
        ("拒绝未知终态", test_unknown_close_status_is_rejected),
        ("回收端到端级联", test_reclaim_end_to_end_cancels_child),
        ("催办 argv 用 --by-role", test_nudge_argv_uses_by_role),
        ("真实 argparse 拒绝 --from", test_ccs_parser_rejects_from_flag),
    ]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  OK  {name}")
        except Exception as ex:
            failed += 1
            print(f"  X   {name}: {type(ex).__name__}: {ex}")
    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
