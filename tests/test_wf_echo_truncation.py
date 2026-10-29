"""wf-echo 闸门必须在 80 字符截断之前判定（PRD AC4 / TASKS T4）。

基线缺陷：`#439048` 的 `wf_rw_666710cad266` 位于首行第 110 字符，两条路由路径
先 `[:80]` 再匹配 → 正则失守 → 缺陷事实被当成新需求建链。同源的 `#439027`
（`wf_` 在第 13 字符）却被拦 —— 同一闸门对同源两事实给出相反结果。
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

# #439048 首行原文（299 字符，wf_ 在第 110 字符）—— 截断后正则不命中
FACT_439048 = (
    "[bug_report] 更正 #439027: 7ced8af0/ed77ff25 不是伪造, 而是当时未提交的工作区状态"
    "(qa/bug_report.md 19:24 独立记录同一组 md5); 但更关键的新事实是 wf_rw_666710cad266 "
    "返工清单 4 条业务问题对当前代码全部已失效 — 行数 19/14/5 非 27/30/36, "
    "stderr 已区分故障点(line 313 ScanIncomplete vs 316 OSError), 顶层 j=_load() 已不存在, "
    "/mnt/c/Windows 已改 tmp_path 夹具且本机存在. 该返工链在改已修好的代码"
)
# 对照组：wf_ 在第 13 字符，截断前后都命中
FACT_439027 = (
    "[bug_report] wf_413e05355b/s4 判定用了 7.5h 前的陈旧子链快照: rejected_at 03:58 "
    "引用 wf_sub_5bb44259a715 于 20:25 的 CHANGES_REQUESTED, 而其锚定 md5 "
    "7ced8af0/ed77ff25 在 git 历史中不存在"
)


def test_fixtures_keep_the_offsets_that_made_the_bug_invisible():
    """样例偏移是判据本身：wf_ 必须在 80 之后，否则本例测不到缺陷。"""
    from routing.routes import _is_wf_echo_title
    assert FACT_439048.find("wf_") >= 80, FACT_439048.find("wf_")
    assert not _is_wf_echo_title(FACT_439048[:80])
    assert _is_wf_echo_title(FACT_439048)
    assert _is_wf_echo_title(FACT_439027[:80])


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_echo_gate_sees_untruncated_title(mock_metrics, mock_hb):
    """route_to_ccs：wf_ 在 80 字符后 → 仍判 echo，不建任务。

    cat 用 optimization 而非 bug_report：成员判据是「看未截断首行」，
    与分类无关；bug_report 已豁免 echo 闸门（缺陷报告要能建链），
    用它做样本测的就不再是截断了。标签同步改成 [optimization]，否则
    _is_role_report_title 会因「标签≠分类」先行拦截，本用例就测不到 echo 闸门。
    """
    from routing.routes import route_to_ccs, _is_role_report_title, _is_wf_echo_title
    _text = FACT_439048.replace("[bug_report]", "[optimization]", 1)
    assert _text.find("wf_") >= 80, _text.find("wf_")
    assert not _is_role_report_title(_text[:80], "optimization", "qa")
    assert _is_wf_echo_title(_text, "optimization")
    msgs = [{"id": 439048, "category": "optimization", "src": "qa",
             "text": _text, "priority": 1}]
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
    mock_wf.return_value.create_task_v2.assert_not_called()


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_never_mints_so_only_route_to_ccs_gates(mock_metrics, mock_hb):
    """route_all 的可投递类在 :285 就 delegated+continue，建链分支不可达。

    因此 TASKS T4 里「route_all(:313) 与 route_to_ccs(:581) 必须同步改」不成立：
    只有 route_to_ccs 是活的闸门。本用例把这条前提锁住——若日后 route_all
    恢复建链，它会在 80 字符处漏判，#439048 会重新被派成新链。
    """
    from routing.routes import route_all
    msgs = [{"id": 439048, "category": "bug_report", "src": "qa",
             "text": FACT_439048, "priority": 1, "consumers": ["engineer"]}]
    mock_router = MagicMock()
    mock_router.routing = {"engineer": {"consume": ["bug_report"]}}
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt, \
         patch("bus_protocol.Blackboard"), \
         patch("routing.routes.WorkflowClient") as mock_wf, \
         patch("routing.routes.IDEMPOTENT_CONSUME") as mock_idem, \
         patch("routing.routes.ACK_TRACKER"), \
         patch("routing.routes.CIRCUIT_BREAKER") as mock_cb:
        mock_rt.get_router.return_value = mock_router
        mock_idem.safe_consume.return_value = {"status": "consumed"}
        mock_cb.call.side_effect = lambda fn: fn()
        result = route_all(consumer="test", dry_run=False)
    mock_wf.assert_not_called()
    assert result["details"][0]["assigned"][0]["status"] == "delegated"


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_real_request_with_wf_like_word_still_mints(mock_metrics, mock_hb):
    """误杀保护：真需求文案不得因为放宽闸门而被吞。"""
    from routing.routes import route_to_ccs
    msgs = [{"id": 930001, "category": "bug_report", "src": "qa",
             "text": "请新增 s3-complete 状态的审计脚本", "priority": 1}]
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


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_stored_title_still_truncated_for_cross_path_dedupe(mock_metrics, mock_hb):
    """落库标题仍按 [:80]：两路径标题口径不一致会让按 title 去重失效。"""
    from routing.routes import route_to_ccs
    msgs = [{"id": 930002, "category": "bug_report", "src": "qa",
             "text": "修复登录模块非json文本 " + "x" * 200, "priority": 1}]
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
    stored = mock_wf.return_value.create_task_v2.call_args[0][0]
    assert len(stored) == 80, len(stored)
