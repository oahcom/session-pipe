"""超时回收前的现场快照（R167）。

WHY：7 天 147 个非完成实例，多数死在「步骤超时 N 次、目标角色全程零工具调用」。
回收时只写一句「超时自动回收」，事后无法区分「CLI 卡在重试」「pane 活着但没读消息」
「角色没被唤起」。这里在回收前抓一次 CLI/pane 现场，写成一条 blocker。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_timeout_forensics.py -q
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, Step, WorkflowRun


def _eng():
    with patch.object(WorkflowEngine, "__init__", lambda s: None):
        eng = WorkflowEngine()
    eng._bb = MagicMock()
    return eng


def _run():
    return WorkflowRun(id="wf_deadbeef1234", workflow_name="architect_full_design",
                       context={}, current_step="s1", status="running", created_at=0.0)


STEP = Step(id="s1", title="需求分析", target_role="product_architect",
            prompt_template="x", exit_condition={"timeout_minutes": 45},
            type="handoff", max_retries=0)


def test_writes_one_blocker_with_cli_and_pane_state():
    eng = _eng()
    with patch("pipeflow.step_handler.subprocess.run") as sp, \
         patch("liveness.pane_cli_busy", return_value=None), \
         patch("liveness.tmux_pane_active", return_value=True):
        sp.return_value = MagicMock(returncode=0, stdout="line1\n\nline2\n❯ \nRetrying in 4m\n")
        eng._write_timeout_forensics(_run(), STEP, "product_architect", 4)

    assert eng._bb.write.call_count == 1, eng._bb.write.call_args_list
    cat, title = eng._bb.write.call_args.args
    ev = eng._bb.write.call_args.kwargs["evidence"]
    assert cat == "blocker"
    assert "product_architect" in title and "超时 4 次" in title, title
    assert "CLI busy: None" in ev, ev          # 探测失败要如实写 None，不能写成 False
    assert "pane 存活: True" in ev
    assert "45 分钟/次" in ev                   # 声明值来自 step.exit_condition
    assert "Retrying in 4m" in ev              # pane 尾保留，能看出卡在重试


def test_tmux_failure_does_not_break_reclaim():
    """抓不到 pane 也要静默跳过：诊断是旁路，不能挡住回收。"""
    eng = _eng()
    with patch("pipeflow.step_handler.subprocess.run", side_effect=OSError("no tmux")):
        eng._write_timeout_forensics(_run(), STEP, "qa", 2)   # 不应抛
    assert eng._bb.write.call_count == 0
