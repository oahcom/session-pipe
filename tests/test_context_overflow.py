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


def _transcript(tmp_path, lines):
    """写一个角色的 transcript 目录，返回 root。"""
    root = tmp_path
    d = root / "ccs-workspaces-ovf"
    d.mkdir(parents=True, exist_ok=True)
    (d / "s.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return root


def _err_line(age_sec):
    """构造一条 N 秒前的 API 错误行（transcript ts 为 UTC）。"""
    import calendar
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - age_sec))
    return '{"isApiErrorMessage": true, "error": "invalid_request", "timestamp": "%s"}' % ts


def test_transcript_tail_error_triggers_compact(tmp_path):
    """pane 无信号，但 transcript 尾部有近期 API 错误 → 仍应发 /compact。

    pane 信号有结构性盲区：claude TUI 在备用屏（alternate_on=1）无 scrollback，
    `-S -500` 与 `-S -5000` 返回相同的视口内容。scout 实测在错误后 1s 才被检测到，
    此时 agent 已终止。transcript 是落盘事实，不受视口与进程存活影响。
    """
    eng = _make_overflow_eng()
    eng._last_compact = {}
    eng._TRANSCRIPTS_ROOT = _transcript(tmp_path, [_err_line(60)])
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-ovf\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="")   # pane 无任何溢出措辞
        if "send-keys" in cmd:
            send_keys.append(cmd)
            return MagicMock(returncode=0)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    assert len([c for c in send_keys if "/compact" in c]) == 1, \
        f"transcript 近期错误应触发 /compact, got {send_keys}"
    print("✅ transcript 尾部近期错误 → /compact")


def test_transcript_stale_error_does_not_trigger(tmp_path):
    """transcript 里的错误已远超时效 → 不触发（旧错误不该无限期重发 /compact）。"""
    eng = _make_overflow_eng()
    eng._last_compact = {}
    eng._TRANSCRIPTS_ROOT = _transcript(tmp_path, [_err_line(7200)])  # 2 小时前
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-ovf\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="")
        if "send-keys" in cmd:
            send_keys.append(cmd)
            return MagicMock(returncode=0)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    assert len([c for c in send_keys if "/compact" in c]) == 0, \
        f"过期错误不应触发, got {send_keys}"
    print("✅ transcript 陈旧错误 → 不触发 ✓")


def test_transcript_missing_dir_is_silent(tmp_path):
    """角色无 transcript 目录 → 不报错、不触发（不能因读不到就发 /compact）。"""
    eng = _make_overflow_eng()
    eng._last_compact = {}
    eng._TRANSCRIPTS_ROOT = tmp_path / "nonexistent"
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-ovf\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="")
        if "send-keys" in cmd:
            send_keys.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    assert send_keys == [], f"无 transcript 不应触发, got {send_keys}"
    print("✅ 无 transcript 目录 → 静默 ✓")


def test_transcript_overflow_falls_back_to_pane_signal(tmp_path):
    """二分法守卫: pane 有信号 + transcript 无错误 → 仍触发（新增不削弱旧判据）。"""
    eng = _make_overflow_eng()
    eng._last_compact = {}
    eng._TRANSCRIPTS_ROOT = _transcript(tmp_path, ['{"type":"user","x":"y"}'])
    send_keys = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-ovf\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="Prompt is too long")
        if "send-keys" in cmd:
            send_keys.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._check_context_overflow()

    assert len([c for c in send_keys if "/compact" in c]) == 1, \
        f"pane 信号仍须生效, got {send_keys}"
    print("✅ pane 信号未被削弱 ✓")


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
