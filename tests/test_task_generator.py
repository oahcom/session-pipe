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


def test_fanout_dedup_skips_duplicate_role():
    """notify_roles 列表含重复角色 → 同一超时事件只通知一次（DESIGN §2.3）。"""
    eng = _get_test_eng()
    eng._lm = MagicMock()  # _lifecycle 是 property，实现在 _lm 上
    eng._consumed_exit_ids = set()

    # bus 上有一条非 target_role 的旧消息 → track 判超时
    _fact = MagicMock()
    _fact.src = "survival_monitor"
    _fact.ts = time.time() - 1000
    eng._bb = MagicMock()
    eng._bb.read.return_value = [_fact]

    _sent = []
    eng._ensure_role_alive = lambda r: None
    eng._send_to_role = lambda r, p, **kw: _sent.append((r, p)) or True

    step = Step(
        id="s1", title="步骤1", target_role="engineer", prompt_template="继续",
        exit_condition={},  # cat 为空 → track 分支照常执行
        track={"bus_category": "deadlock_signal", "timeout_sec": 1,
               "notify_roles": ["qa", "qa", "devops"]},  # qa 重复
    )
    run = WorkflowRun(id="wf_dedup_test01", workflow_name="dedup_test",
                      context={},
                      step_results={"s1": {"notified_at": time.time() - 100}})

    with patch("pipeflow.step_handler.get_conversation_signal", return_value=None), \
         patch("pipeflow.step_handler._integ_load",
               return_value=MagicMock(pending=False, complete=False)):
        eng._tick(run, step)

    _sent_roles = [r for r, _ in _sent]
    assert _sent_roles.count("qa") == 1, f"qa 应只通知 1 次, got {_sent_roles}"
    assert _sent_roles.count("devops") == 1, f"devops 应通知 1 次, got {_sent_roles}"
    assert len(_sent) == 2, f"去重后应共 2 次通知, got {len(_sent)}: {_sent_roles}"
    print("✅ fan-out 同事件同角色去重 ✓")


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

if __name__ == "__main__":
    for t in (test_event_key_deterministic_and_dimension_sensitive,
              test_can_assign_role_backpressure,
              test_fanout_dedup_skips_duplicate_role,
              test_backlog_alert_cooldown_suppresses_repeat):
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
    print("\n--- 全部测试完成 ---")
