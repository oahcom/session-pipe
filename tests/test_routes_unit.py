"""Unit tests for routing/routes.py — _match_investigator, dispatch_investigator, route_all, route_to_ccs, route_all_to_ccs.

Mocks all external deps (bus, subprocess, reliability globals).
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))


# ── _match_investigator: 纯函数，直接测 ──


def test_match_investigator_empty_returns_general():
    from routing.routes import _match_investigator
    assert _match_investigator("") == "investigator_general"
    assert _match_investigator(None) == "investigator_general"


def test_match_investigator_python_keywords():
    from routing.routes import _match_investigator
    assert _match_investigator("Traceback (most recent call last):") == "investigator_python"
    assert _match_investigator("ImportError: No module named 'foo'") == "investigator_python"
    assert _match_investigator("pip install requests") == "investigator_python"
    assert _match_investigator("pytest test_foo.py") == "investigator_python"


def test_match_investigator_senior_keywords():
    from routing.routes import _match_investigator
    assert _match_investigator("segfault at address 0xdeadbeef") == "investigator_senior"
    assert _match_investigator("deadlock detected in thread pool") == "investigator_senior"
    assert _match_investigator("OOM killed process 1234") == "investigator_senior"
    assert _match_investigator("race condition in shared dict") == "investigator_senior"


def test_match_investigator_python_beats_senior():
    """Python 关键字优先于 senior 关键字。"""
    from routing.routes import _match_investigator
    result = _match_investigator("Traceback crash segfault")
    assert result == "investigator_python"


def test_match_investigator_fallback_general():
    from routing.routes import _match_investigator
    assert _match_investigator("normal message, no special keywords") == "investigator_general"


def test_match_investigator_case_insensitive():
    from routing.routes import _match_investigator
    assert _match_investigator("SEGFAULT in kernel") == "investigator_senior"


# ── dispatch_investigator ──


def _make_fact(fact_id, cat, title, evidence=""):
    """构造 Blackboard Fact 哑对象。"""
    f = MagicMock()
    f.id = fact_id
    f.cat = cat
    f.t = title
    f.e = evidence
    return f


@patch("routing.routes._rt_mod")
@patch("bus_protocol.Blackboard")
def test_dispatch_investigator_filters_needs_investigation(mock_bb_cls, mock_rt):
    """只处理含 needs_investigation/needs_triage 的消息。"""
    from routing.routes import dispatch_investigator
    bb = MagicMock()
    mock_bb_cls.return_value = bb
    bb.read.return_value = [
        _make_fact(1, "code_fix", "bug report", "needs_investigation — traceback detected"),
        _make_fact(2, "architecture", "design doc", "no filter keyword here"),
        _make_fact(3, "code_fix", "error log", "needs_triage — crash dump"),
    ]
    result = dispatch_investigator(category="code_fix", dry_run=True)
    assert result["total"] == 2
    roles = [d["assigned_investigator"] for d in result["details"]]
    assert "investigator_python" in roles


@patch("routing.routes._rt_mod")
@patch("bus_protocol.Blackboard")
def test_dispatch_investigator_dry_run_no_write(mock_bb_cls, mock_rt):
    """dry_run=True → 不写入 bus。"""
    from routing.routes import dispatch_investigator
    bb = MagicMock()
    mock_bb_cls.return_value = bb
    bb.read.return_value = [
        _make_fact(1, "code_fix", "err", "needs_investigation — ImportError"),
    ]
    result = dispatch_investigator(category="code_fix", dry_run=True)
    assert result["dispatched"] == 0
    bb.write.assert_not_called()


@patch("routing.routes._rt_mod")
@patch("bus_protocol.Blackboard")
def test_dispatch_investigator_real_write(mock_bb_cls, mock_rt):
    """dry_run=False → 写入 notice 分类。"""
    from routing.routes import dispatch_investigator
    bb = MagicMock()
    mock_bb_cls.return_value = bb
    bb.read.return_value = [
        _make_fact(1, "code_fix", "err", "needs_investigation — segfault"),
    ]
    result = dispatch_investigator(category="code_fix", dry_run=False)
    assert result["dispatched"] == 1
    bb.write.assert_called_once()
    assert bb.write.call_args[0][0] == "notice"


# ── route_all: mock routing.auto.poll_unconsumed + 局部依赖 ──


@patch("routing.routes.set_last_cursor")
@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_empty_messages(mock_metrics, mock_hb, mock_cursor):
    """空消息列表 → routed=0。"""
    from routing.routes import route_all
    with patch("routing.auto.poll_unconsumed", return_value=[]), \
         patch("routing.routes._rt_mod"), \
         patch("bus_protocol.Blackboard"):
        result = route_all(consumer="test", dry_run=True, instance_id="test_1")
    assert result["routed"] == 0
    assert result["total"] == 0


@patch("routing.routes.set_last_cursor")
@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_error_message(mock_metrics, mock_hb, mock_cursor):
    """消息含 error → routed=0, route_errors_total 递增。"""
    from routing.routes import route_all
    with patch("routing.auto.poll_unconsumed", return_value=[{"error": "bus down"}]), \
         patch("routing.routes._rt_mod"), \
         patch("bus_protocol.Blackboard"):
        result = route_all(consumer="test", dry_run=True, instance_id="test_2")
    assert result["routed"] == 0
    mock_metrics.inc.assert_any_call("route_errors_total")


@patch("routing.routes.set_last_cursor")
@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_dry_run_returns_plan(mock_metrics, mock_hb, mock_cursor):
    """dry_run=True → details 列出分配方案，不实际消费。"""
    from routing.routes import route_all
    msgs = [{
        "id": 10, "category": "code_fix", "text": "fix bug",
        "priority": 1, "consumers": ["engineer", "closer"],
    }]
    mock_router = MagicMock()
    mock_router.routing = {
        "engineer": {"consume": ["code_fix"]},
        "closer": {"consume": ["code_fix"]},
    }
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("bus_protocol.Blackboard"):
        mock_rt_mod.get_router.return_value = mock_router
        result = route_all(consumer="test", dry_run=True, instance_id="test_3")
    assert result["routed"] == 1
    assert len(result["details"]) == 1
    assert "engineer" in result["details"][0]["assigned"]


@patch("routing.routes.set_last_cursor")
@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_no_consumer(mock_metrics, mock_hb, mock_cursor):
    """消息 consumers 为空列表 → reason=no_consumer。"""
    from routing.routes import route_all
    msgs = [{
        "id": 11, "category": "code_fix", "text": "orphan",
        "priority": 5, "consumers": [],
    }]
    mock_router = MagicMock()
    mock_router.routing = {}
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("bus_protocol.Blackboard"):
        mock_rt_mod.get_router.return_value = mock_router
        result = route_all(consumer="test", dry_run=True, instance_id="test_4")
    assert result["routed"] == 0
    assert result["details"][0]["reason"] == "no_consumer"


@patch("routing.routes.set_last_cursor")
@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_system_category_skips_workflow(mock_metrics, mock_hb, mock_cursor):
    """SYSTEM_CATEGORIES 消费后不创建 workflow。"""
    from routing.routes import route_all, SYSTEM_CATEGORIES
    cat = list(SYSTEM_CATEGORIES)[0]
    msgs = [{
        "id": 20, "category": cat, "text": "system notice",
        "priority": 0, "consumers": ["coordinator"],
    }]
    mock_router = MagicMock()
    mock_router.routing = {"coordinator": {"consume": ["*"]}}
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("bus_protocol.Blackboard") as mock_bb_cls, \
         patch("routing.routes.WorkflowClient") as mock_wf, \
         patch("routing.routes.IDEMPOTENT_CONSUME") as mock_idem, \
         patch("routing.routes.ACK_TRACKER") as mock_ack, \
         patch("routing.routes.CIRCUIT_BREAKER") as mock_cb:
        mock_rt_mod.get_router.return_value = mock_router
        mock_idem.safe_consume.return_value = {"status": "consumed"}
        mock_cb.call.side_effect = lambda fn: fn()
        bb = MagicMock()
        mock_bb_cls.return_value = bb
        route_all(consumer="test", dry_run=False, instance_id="test_5")
    mock_wf.assert_not_called()


# ── route_to_ccs: _send/_is_running 内部用 import subprocess as _sp（局部），patch subprocess.run ──


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_empty_messages(mock_metrics, mock_hb):
    """空消息 → routed=0。"""
    from routing.routes import route_to_ccs
    with patch("routing.auto.poll_unconsumed", return_value=[]), \
         patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = MagicMock()
        result = route_to_ccs("engineer", dry_run=True)
    assert result["routed"] == 0
    assert result["total"] == 0


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_dry_run(mock_metrics, mock_hb):
    """dry_run=True → action=dry_run，不发送。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 30, "category": "code_fix", "text": "fix now",
        "priority": 1, "evidence": "traceback detected",
    }]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = MagicMock()
        result = route_to_ccs("engineer", dry_run=True)
    assert result["routed"] == 0
    assert result["details"][0]["action"] == "dry_run"


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_not_running_skips(mock_metrics, mock_hb):
    """CCS 未运行 → action=skipped。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 31, "category": "code_fix", "text": "fix",
        "priority": 1,
    }]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("subprocess.run") as mock_run:
        mock_rt_mod.get_router.return_value = MagicMock()
        status_result = MagicMock()
        status_result.stdout = "未运行"
        status_result.returncode = 0
        mock_run.return_value = status_result
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "skipped"


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_send_success(mock_hb, mock_metrics):
    """发送成功 → action=routed。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 32, "category": "code_fix", "text": "non-json plain text",
        "priority": 1,
    }]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("routing.routes.sys") as mock_sys, \
         patch("subprocess.run") as mock_run, \
         patch("routing.routes.WorkflowClient") as mock_wf:
        mock_rt_mod.get_router.return_value = MagicMock()
        status_result = MagicMock()
        status_result.stdout = "running"
        status_result.returncode = 0
        send_result = MagicMock()
        send_result.stdout = ""
        send_result.stderr = ""
        send_result.returncode = 0
        mock_run.side_effect = [status_result, send_result]
        mock_sys.executable = "/usr/bin/python3"
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "routed"
    # 短标题(<80)不创建 workflow（防空模板循环）
    mock_wf.assert_not_called()


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_send_failure(mock_metrics, mock_hb):
    """发送失败 → action=failed。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 33, "category": "code_fix", "text": "some text",
        "priority": 1,
    }]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("routing.routes.sys") as mock_sys, \
         patch("subprocess.run") as mock_run:
        mock_rt_mod.get_router.return_value = MagicMock()
        status_result = MagicMock()
        status_result.stdout = "running"
        status_result.returncode = 0
        send_result = MagicMock()
        send_result.stdout = '{"error": "connection refused"}'
        send_result.stderr = ""
        send_result.returncode = 1
        mock_run.side_effect = [status_result, send_result]
        mock_sys.executable = "/usr/bin/python3"
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "failed"
    assert "connection refused" in result["details"][0]["error"]


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_system_category_skips_workflow(mock_metrics, mock_hb):
    """system category → routed 成功，但不创建 WorkflowClient。"""
    from routing.routes import route_to_ccs, SYSTEM_CATEGORIES
    cat = list(SYSTEM_CATEGORIES)[0]
    msgs = [{
        "id": 34, "category": cat, "text": "system notice",
        "priority": 0,
    }]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("routing.routes.sys") as mock_sys, \
         patch("subprocess.run") as mock_run, \
         patch("routing.routes.WorkflowClient") as mock_wf:
        mock_rt_mod.get_router.return_value = MagicMock()
        status_result = MagicMock()
        status_result.stdout = "running"
        status_result.returncode = 0
        send_result = MagicMock()
        send_result.stdout = ""
        send_result.stderr = ""
        send_result.returncode = 0
        mock_run.side_effect = [status_result, send_result]
        mock_sys.executable = "/usr/bin/python3"
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "routed"
    mock_wf.assert_not_called()


# ── route_all_to_ccs ──


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_to_ccs_empty_roles(mock_metrics, mock_hb):
    """空路由表 → routed=0。"""
    from routing.routes import route_all_to_ccs
    mock_router = MagicMock()
    mock_router.routing = {}
    with patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = mock_router
        result = route_all_to_ccs(dry_run=True)
    assert result["routed"] == 0
    assert result["total"] == 0


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_all_to_ccs_single_role(mock_metrics, mock_hb):
    """有角色的路由表 → 调用 route_to_ccs。"""
    from routing.routes import route_all_to_ccs
    mock_router = MagicMock()
    mock_router.routing = {"engineer": {"produce": ["code_fix"], "consume": ["code_fix"]}}
    with patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("routing.routes.route_to_ccs") as mock_r2c:
        mock_rt_mod.get_router.return_value = mock_router
        mock_r2c.return_value = {"role": "engineer", "routed": 2, "total": 2, "details": []}
        result = route_all_to_ccs(dry_run=False)
    assert result["routed"] == 2
    assert result["total"] == 2
    mock_r2c.assert_called_once_with("engineer", False)
