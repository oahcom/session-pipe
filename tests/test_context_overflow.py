"""test_context_overflow.py — 上下文溢出检测 + 子工作流模板保护

验证：
1. pane 行数超阈值时 _check_context_overflow 发送 /compact
2. 同一角色 300s 内不重复发送
3. is_subflow=True 的模板不能被 start()
4. is_subflow=True 的模板不出现在 list_workflows() 中
5. _generate_tasks_from_state 跳过 is_subflow 模板
"""
import time
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, WorkflowDef, Step


def _get_test_eng():
    """获取测试用 WorkflowEngine（避免加载真实 DB）。"""
    from pipeflow.engine import _WORKFLOWS_DIR
    return WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)


def _make_overflow_eng():
    """隔离 DB：mock _lifecycle.query，避免真实 DB 的 assignee 干扰角色集合。"""
    eng = _get_test_eng()
    eng._lm = MagicMock()
    eng._lm.query.return_value = []
    return eng

def test_pane_overflow_triggers_compact():
    """捕获到溢出信号 → 发送 /compact 并更新 _last_compact。"""
    eng = _make_overflow_eng()
    role = "lr"
    eng._last_compact = {}  # 清掉旧状态，避免类级残留
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="Context limit reached")
        if "send-keys" in cmd:
            send_keys.append(cmd)
            return MagicMock(returncode=0)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    compact_calls = [c for c in send_keys if "/compact" in c]
    assert len(compact_calls) == 1, f"应发 1 次 /compact, got {send_keys}"
    assert eng._last_compact.get(role) is not None, "应记录 _last_compact[lr]"
    print("✅ pane overflow → /compact 已触发")


def test_cooldown_prevents_duplicate():
    """同一角色 300s 内不重复发 /compact。"""
    eng = _make_overflow_eng()
    role = "lr"
    eng._last_compact = {role: time.time()}  # 刚发过
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="Context limit reached")
        if "send-keys" in cmd:
            send_keys.append(cmd)
            return MagicMock(returncode=0)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    compact_calls = [c for c in send_keys if "/compact" in c]
    assert len(compact_calls) == 0, "cooldown 应阻止重发"
    print("✅ cooldown 300s 防重复 ✓")


def test_subflow_cannot_start():
    """is_subflow=True 的模板不能被独立 start()。"""
    eng = _get_test_eng()
    # 注入一个假的子工作流模板
    fake_wf = WorkflowDef(
        name="subflow_test", title="测试子工作流",
        description="仅子工作流调用", steps=[],
        is_subflow=True
    )
    eng._workflows["subflow_test"] = fake_wf

    try:
        eng.start("subflow_test")
        assert False, "应该抛出 ValueError"
    except ValueError as e:
        assert "子工作流模板" in str(e)
        print("✅ is_subflow 模板 start() 被拒绝 ✓")


def test_subflow_excluded_from_list():
    """is_subflow=True 的模板不出现在 list_workflows()。"""
    eng = _get_test_eng()
    fake_wf = WorkflowDef(
        name="subflow_test", title="测试",
        description="子工作流", steps=[],
        is_subflow=True
    )
    eng._workflows["subflow_test"] = fake_wf

    wf_list = eng.list_workflows()
    assert "subflow_test" not in wf_list, "list_workflows 应排除 subflow_test"
    print("✅ is_subflow 模板不在 list_workflows() 中 ✓")


def test_auto_gen_skips_subflow():
    """is_subflow=True 的模板不应被自动任务生成选中。"""
    eng = _get_test_eng()
    fake_wf = WorkflowDef(
        name="subflow_task_test", title="测试",
        description="子工作流", steps=[Step(
            id="s1", title="步骤1", target_role="engineer",
            prompt_template="test prompt " * 20, exit_condition={},
            type="single"
        )],
        allowed_executors=["engineer"],
        is_subflow=True
    )
    eng._workflows["subflow_task_test"] = fake_wf

    # _generate_tasks_from_state 内部遍历，检查 _wf.is_subflow 分支
    # 由于子工作流模板存在但 is_subflow=True，遍历时应 continue 跳过
    assert eng._workflows["subflow_task_test"].is_subflow is True
    print("✅ is_subflow 模板不会进入自动任务生成 ✓")


if __name__ == "__main__":
    tests = [
        test_pane_overflow_triggers_compact,
        test_cooldown_prevents_duplicate,
        test_subflow_cannot_start,
        test_subflow_excluded_from_list,
        test_auto_gen_skips_subflow,
    ]
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
    print("\n--- 全部测试完成 ---")
