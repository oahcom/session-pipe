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
    bb.unconsumed.return_value = [
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
    bb.unconsumed.return_value = [
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
    bb.unconsumed.return_value = [
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
         patch("routing.routes.ACK_TRACKER"), \
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
    # 载体分类用 bug_report（待办请求）而非 code_fix：后者自 2026-10-17 起归入
    # _SYSTEM_CATEGORIES（它承载的是"已修复"回报，不是请求），不再 mint 任务。
    # 本用例验证的是"含内容词的标题应建 workflow"的路由逻辑，与载体分类无关。
    msgs = [{
        "id": 32, "category": "bug_report", "text": "修复登录模块非json文本",
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
        # 忙检测 check_task：无活跃工作流 → 不忙 → 走发送路径
        mock_wf.return_value.check_task.return_value = None
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "routed"
    # 标题含内容词且 ≥8 字符时创建 workflow（与 route_all 阈值统一）
    mock_wf.return_value.create_task_v2.assert_called_once()


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
         patch("subprocess.run") as mock_run, \
         patch("routing.routes.WorkflowClient") as mock_wf:
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
        # 忙检测 check_task：无活跃工作流 → 不忙 → 走发送路径
        mock_wf.return_value.check_task.return_value = None
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
        # 忙检测 check_task：无活跃工作流 → 不忙 → 走发送路径
        mock_wf.return_value.check_task.return_value = None
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "routed"
    # system category 不创建 workflow 任务（防空模板循环）
    mock_wf.return_value.create_task_v2.assert_not_called()


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_busy_defers(mock_metrics, mock_hb):
    """角色有活跃工作流 → action=deferred，不发送、不创建任务。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 35, "category": "code_fix", "text": "fix now",
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
        mock_run.side_effect = [status_result]
        mock_sys.executable = "/usr/bin/python3"
        # 忙检测 check_task：有活跃工作流 → 忙 → deferred
        mock_wf.return_value.check_task.return_value = {"instance_id": "wf_1"}
        result = route_to_ccs("engineer", dry_run=False)
    assert result["details"][0]["action"] == "deferred"
    assert "工作流进行中" in result["details"][0]["reason"]
    # 忙时不发送、不创建任务
    mock_run.assert_called_once()  # 只有 _is_running，无 _send
    mock_wf.return_value.create_task_v2.assert_not_called()


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


# ── _is_role_report_title: 防 fan-out 纯函数 ──
# 实测 2026-10-17：无此判定时 [self-reflect] 复盘消息被 fan-out 到 9 个角色
# （16 源事件 → 54 条"创建任务"，3.4 倍放大）。

def test_is_role_report_title_blocks_bracketed_tags():
    """[标签] 开头一律是系统自述/产出汇报，不建 workflow。"""
    from routing.routes import _is_role_report_title
    assert _is_role_report_title("[self-reflect] 任务《x》无产出") is True
    assert _is_role_report_title("[deploy] wf_xxx 产出已部署") is True
    assert _is_role_report_title("[engineer] s1-complete (wf_xxx)") is True
    assert _is_role_report_title("[maintainer] 创建任务: xxx") is True
    assert _is_role_report_title("[qa] 唤醒: wf_x/s4 测试报告已就绪") is True


def test_is_role_report_title_allows_real_tasks():
    """真实任务标题不误杀。"""
    from routing.routes import _is_role_report_title
    assert _is_role_report_title("needs_implementation @qa 工作流任务") is False
    assert _is_role_report_title("修复登录超时 bug") is False
    assert _is_role_report_title("") is False
    assert _is_role_report_title(None) is False


def test_is_self_echo_suppresses_only_own_messages():
    """回声抑制：只拦自己写的，不误杀他人（RFC 3834）。"""
    from routing.routes import _is_self_echo
    assert _is_self_echo({"src": "consumer"}, "consumer") is True
    assert _is_self_echo({"src": "engineer"}, "consumer") is False
    assert _is_self_echo({"src": "workflow_engine"}, "consumer") is False
    assert _is_self_echo({}, "consumer") is False
    assert _is_self_echo({"src": ""}, "consumer") is False


def test_needs_investigation_catches_maintainer_handoff():
    """维护者发现缺陷但越界不改 → 移交 investigator 接手。"""
    from routing.routes import _needs_investigation
    assert _needs_investigation(_make_fact(
        1, "code_fix", "[maintainer] 9Router 端口漂移",
        "缺陷: launcher 兜底默认值指向死端口。建议（越界不自己改，交 engineer）恢复兜底")) is True


def test_needs_investigation_skips_deploy_report():
    """已完成回报（[deploy] 产出已部署）不是待办，不派 investigator。"""
    from routing.routes import _needs_investigation
    assert _needs_investigation(_make_fact(
        1, "code_fix", "[deploy] wf_x 产出已部署: eval_consistency.py",
        "路径: ~/.hermes/bin/eval_consistency.py")) is False


def test_needs_investigation_skips_noop_health_report():
    """健康巡检含"无需修复"，即使同时含 handoff 词也不派（否定式优先）。"""
    from routing.routes import _needs_investigation
    assert _needs_investigation(_make_fact(
        1, "code_fix", "[maintainer] 健康检查: 服务全绿零僵尸(非修复,no-op报告)",
        "本报告为健康巡检no-op(无异常无需修复),不应触发下游建修复任务")) is False


def test_needs_investigation_no_marker_no_signal_is_false():
    """无显式标记也无移交信号 → 默认不派，防淹没 investigator。"""
    from routing.routes import _needs_investigation
    assert _needs_investigation(_make_fact(
        1, "code_fix", "CODE_FIX: wf_x/s2 代码开发 — 部署件安全抽查固化",
        "新增 src/deploy_artifact_audit.py")) is False

# ── 注入文本前缀幂等（R106）：角色不该收到「[cat] [cat]」「证据: 证据:」 ──

@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_prefix_is_idempotent(mock_metrics, mock_hb):
    """fact 自带 [cat] 前缀与「证据: 」时不得重复拼接。"""
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 40, "category": "test_report",
        "text": "[test_report] qa 三文档口径收敛",
        "priority": 2, "evidence": "证据: 实跑核对 4 项",
    }]
    sent = {}

    def _fake_run(cmd, *a, **k):
        # status-role 返回存活；ccs.py send 的最后一个参数是被投递的正文
        if any("status-role" in str(c) for c in cmd):
            return MagicMock(returncode=0, stdout='{"alive": true}')
        if "send" in cmd:
            sent["body"] = cmd[-1]
        return MagicMock(returncode=0, stdout="")

    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("subprocess.run", side_effect=_fake_run), \
         patch("workflow.client.WorkflowClient") as mock_wfc:
        mock_rt_mod.get_router.return_value = MagicMock()
        mock_wfc.return_value.check_task.return_value = None   # 无活跃工作流 → 不 deferred
        route_to_ccs("engineer", dry_run=False)

    body = sent.get("body", "")
    assert body.count("[test_report]") == 1, f"分类前缀重复: {body!r}"
    assert body.count("证据:") == 1, f"证据前缀重复: {body!r}"
