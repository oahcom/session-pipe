"""bug_report 豁免 wf-echo 闸门（#466798 / 源事实 #466637）。

缺陷：qa 的缺陷报告标题天然是 `qa_test_execute/s2-complete wf_sub_xxx ...`，
`_WF_ECHO_RE` 命中 `/s2-complete` → 整条报告被判成「已完成事件的汇报」，
不建任务。带 3×P1 的 #466637 因此在 bus 里漂了 6 小时，tasks 里零行。

判据边界：只豁免 bug_report。design_issue / optimization 同窗口的 echo 标题
实测全是 relay 转录（`【触发】relay: [blocker] wf_x ...`），豁免只会复活转发噪音。
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

# #466637 首行原文（截到 120 字符，保留完整判据形态：/s2-complete + wf_ id）
FACT_466637 = (
    "qa_test_execute/s2-complete wf_sub_51cf86f431c6 缺陷报告 P0 0 / P1 3 / P2 1 / P3 1 "
    "— 302 二跳不过闸、*.nip.io 整类放行、4 份网段表共存"
)


def test_bug_report_title_is_not_echo():
    from routing.routes import _is_wf_echo_title
    assert _is_wf_echo_title(FACT_466637), "样本必须命中正则，否则本用例测不到豁免"
    assert not _is_wf_echo_title(FACT_466637, "bug_report")


def test_other_content_signal_categories_still_gated():
    """design_issue / optimization 不豁免 —— 它们的 echo 标题是 relay 转录。"""
    from routing.routes import _is_wf_echo_title
    for cat in ("optimization", "design_issue", "test_report", ""):
        assert _is_wf_echo_title(FACT_466637, cat), cat


def test_bug_report_template_is_engineer_feature():
    """验收 a 的另一半：建出来的实例模板必须是 WF-engineer_feature。"""
    from workflow.client import CATEGORY_TEMPLATE
    assert CATEGORY_TEMPLATE["bug_report"] == "WF-engineer_feature", CATEGORY_TEMPLATE.get("bug_report")


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_mints_task_for_echo_shaped_bug_report(mock_metrics, mock_hb):
    """验收 a：同形态标题的 bug_report 能建出任务（此前 0 行）。"""
    from routing.routes import route_to_ccs
    msgs = [{"id": 466637, "category": "bug_report", "src": "qa",
             "text": FACT_466637, "priority": 1}]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt, \
         patch("routing.routes.sys") as mock_sys, \
         patch("subprocess.run") as mock_run, \
         patch("routing.routes.WorkflowClient") as mock_wf:
        mock_rt.get_router.return_value = MagicMock()
        status, send = MagicMock(), MagicMock()
        status.stdout, status.returncode = "running", 0
        send.stdout, send.stderr, send.returncode = "", "", 0
        mock_run.side_effect = [status, send]
        mock_sys.executable = "/usr/bin/python3"
        mock_wf.return_value.check_task.return_value = None
        route_to_ccs("engineer", dry_run=False)
    mock_wf.return_value.create_task_v2.assert_called_once()
    kwargs = mock_wf.return_value.create_task_v2.call_args.kwargs
    assert kwargs["bus_category"] == "bug_report", kwargs
