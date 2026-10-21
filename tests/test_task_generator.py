"""test_task_generator.py — 工作流任务去重与限流（DESIGN §4 S4）

验证三层防线的可执行契约：
1. _event_key 确定性 + 维度敏感（同输入同键，assignee/template 变则键变）
2. _can_assign_role backpressure：running >= MAX_RUNNING_PER_ROLE → False
3. fan-out 去重：notify_roles 同角色重复项只通知一次（DESIGN §2.3）
"""
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, WorkflowDef, Step, WorkflowRun
from pipeflow.task_generator import _event_key, MAX_RUNNING_PER_ROLE


def _get_test_eng():
    """获取测试用 WorkflowEngine（conftest 已隔离 DB）。"""
    from pipeflow.engine import _WORKFLOWS_DIR
    return WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)


def test_event_key_deterministic_and_dimension_sensitive():
    """同输入 → 同键；assignee 或 template 任一变化 → 键不同（防跨角色误去重）。"""
    k1 = _event_key("标题A", "qa", "wl_demo")
    k2 = _event_key("标题A", "qa", "wl_demo")
    k3 = _event_key("标题A", "devops", "wl_demo")   # assignee 变
    k4 = _event_key("标题A", "qa", "wl_other")      # template 变
    assert k1 == k2, "同 输入必须同键"
    assert len(k1) == 16, f"hash 截断应为 16 字符, got {len(k1)}"
    assert k1 != k3, "assignee 变化必须产生不同键"
    assert k1 != k4, "template 变化必须产生不同键"
    print("✅ _event_key 确定性 + 维度敏感 ✓")


def test_can_assign_role_backpressure():
    """running >= MAX_RUNNING_PER_ROLE → False（backpressure）；低于水位线 → 放行到 L1/L2。"""
    eng = _get_test_eng()
    eng._lm = MagicMock()  # _lifecycle 是 property，实现在 _lm 上
    # L1/L2 mock：tmux 存活 + health 可用且不忙 → 唯一拒绝理由只能是 L3 backpressure
    _fake_res = {"busy": False, "signals": {1: {"available": True}}}
    with patch("liveness._tmux_has_session", return_value=True), \
         patch("liveness.role_liveness", return_value=_fake_res):
        # 达到水位线
        eng._lifecycle.query.return_value = [{"c": MAX_RUNNING_PER_ROLE}]
        assert eng._can_assign_role("qa") is False, \
            f"running={MAX_RUNNING_PER_ROLE} 应触发 backpressure"
        # 超过水位线
        eng._lifecycle.query.return_value = [{"c": MAX_RUNNING_PER_ROLE + 5}]
        assert eng._can_assign_role("qa") is False, "超水位线同样拒绝"
        # 低于水位线 → 放行（L1/L2 均通过）
        eng._lifecycle.query.return_value = [{"c": MAX_RUNNING_PER_ROLE - 1}]
        assert eng._can_assign_role("qa") is True, "低于水位线应放行"
        # 查询异常 → 不阻断（fail-open，保守放行交给 L1/L2）
        eng._lifecycle.query.side_effect = RuntimeError("db locked")
        assert eng._can_assign_role("qa") is True, "L3 查询异常应 fail-open"
    print(f"✅ MAX_RUNNING_PER_ROLE={MAX_RUNNING_PER_ROLE} backpressure ✓")


def test_backlog_alert_cooldown_suppresses_repeat():
    """同角色积压告警 30min 内只放行一次（DESIGN §2.6 S4）。"""
    eng = _get_test_eng()
    eng._lm = MagicMock()
    eng._BACKLOG_ALERT_TS.clear()
    assert eng._backlog_alert_allowed("qa") is True, "首次告警应放行"
    assert eng._backlog_alert_allowed("qa") is False, "同角色 30min 内应被冷却拦截"
    assert eng._backlog_alert_allowed("devops") is True, "不同角色互不影响"
    # 冷却窗口过后 → 再次放行
    eng._BACKLOG_ALERT_TS["qa"] = time.time() - eng._BACKLOG_ALERT_COOLDOWN_S - 1
    assert eng._backlog_alert_allowed("qa") is True, "冷却窗口过后应重新放行"
    print("✅ 积压告警 30min 冷却 ✓")

def test_notify_role_cooldown_suppresses_repeat_in_same_window():
    """同一 (role,title) 30min 只投一次；标题不同照常投；状态是进程级。

    实测 maintainer 30h 收 265 次同标题告警（正文 1→0 反复翻转，滚动 1h 内 100 次、
    最小间隔 8.4s），每次都占一个完整上下文回合。状态没变就不该再叫醒角色。
    """
    from pipeflow.role_lifecycle import RoleLifecycleMixin
    RoleLifecycleMixin._ALERT_TS.clear()
    eng = _get_test_eng()
    eng._bb = MagicMock()
    with patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0)):
        eng._notify_role("maintainer", "工作流多步骤超时: wf_x", "2 个步骤…")
        eng._notify_role("maintainer", "工作流多步骤超时: wf_x", "0 个步骤…")
        eng._notify_role("maintainer", "僵尸工作流自动回收: 3 台", "…")
    assert eng._bb.write.call_count == 2, f"重复标题应被抑制, got {eng._bb.write.call_count}"
    # 冷却必须跨 engine 实例生效（daemon 每 tick 新建实例也照样限流）
    eng2 = _get_test_eng()
    eng2._bb = MagicMock()
    with patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0)):
        eng2._notify_role("maintainer", "工作流多步骤超时: wf_x", "2 个步骤…")
    assert eng2._bb.write.call_count == 0, "冷却不得随 engine 实例重置"
    RoleLifecycleMixin._ALERT_TS.clear()
    print("✅ 告警 (role,title) 冷却（进程级）✓")


def test_notify_role_backs_off_after_failed_send():
    """rc≠0/抛错不还回整窗，只退避 600s：既不每 tick 重投，也不静默 30min。

    旧实现 pop 掉键 → 下一个 10s tick 立刻重投；实测错误态角色 30h 收 261 条
    同一告警（中位间隔 20s）。两条不变式：窗口内不重投、退避到期必须重投。
    """
    from pipeflow.role_lifecycle import RoleLifecycleMixin
    key = ("maintainer", "工作流多步骤超时: wf_y")
    RoleLifecycleMixin._ALERT_TS.clear()
    eng = _get_test_eng()
    eng._bb = MagicMock()
    with patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=1)):
        eng._notify_role("maintainer", "工作流多步骤超时: wf_y", "2 个步骤…")
    ts = RoleLifecycleMixin._ALERT_TS.get(key)
    assert ts is not None, "rc≠0 也要记账，否则每 tick 重投"
    assert 1190 < time.time() - ts < 1210, f"应退避 600s，实际 {time.time() - ts:.0f}s"
    with patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0)) as _run:
        eng._notify_role("maintainer", "工作流多步骤超时: wf_y", "2 个步骤…")
    assert _run.call_count == 0, "退避窗口内不得重投"
    RoleLifecycleMixin._ALERT_TS[key] = time.time() - RoleLifecycleMixin._ALERT_COOLDOWN_S
    with patch("pipeflow.engine._sp.run", return_value=MagicMock(returncode=0)) as _run2:
        eng._notify_role("maintainer", "工作流多步骤超时: wf_y", "2 个步骤…")
    assert _run2.call_count == 1, "退避到期后必须重投"
    RoleLifecycleMixin._ALERT_TS.clear()


if __name__ == "__main__":
    for t in (test_event_key_deterministic_and_dimension_sensitive,
              test_can_assign_role_backpressure,
              test_fanout_dedup_skips_duplicate_role,
              test_backlog_alert_cooldown_suppresses_repeat,
              test_notify_role_cooldown_suppresses_repeat_in_same_window):
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
    print("\n--- 全部测试完成 ---")
