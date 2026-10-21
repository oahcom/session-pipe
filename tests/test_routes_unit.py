"""Unit tests for routing/routes.py — route_all, route_to_ccs, route_all_to_ccs.

Mocks all external deps (bus, subprocess, reliability globals).
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))


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
def test_route_to_ccs_bug_report_label_mints_task(mock_hb, mock_metrics):
    """[bug_report] 标签 + bug_report 分类 = 真实缺陷报告，必须建任务。

    回归 #421072/#421074：旧判据"[xxx] 开头一律拦"使 36 条 qa 缺陷报告
    在 tasks 表零记录，缺陷无人接手。
    """
    from routing.routes import route_to_ccs
    msgs = [{
        "id": 421072, "category": "bug_report", "src": "qa",
        "text": "[bug_report] [P1] architect_full_design s1/s2 缺 require_wf",
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
        mock_wf.return_value.check_task.return_value = None
        route_to_ccs("engineer", dry_run=False)
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


def test_is_role_report_title_allows_content_category_labels():
    """标签复用总线分类名 = 内容型待办，必须建任务（#421072 被静默吞掉）。"""
    from routing.routes import _is_role_report_title
    assert _is_role_report_title("[bug_report] [P1] s1/s2 缺 require_wf", "bug_report", "qa") is False
    assert _is_role_report_title("[design_issue] 单点瓶颈风险", "design_issue", "engineer") is False


def test_is_role_report_title_still_blocks_engine_audit():
    """引擎自写审计压倒一切：放行会形成引擎写审计→建任务→再写审计的自喂环。

    真实形态：`[workflow] lr_tech_decision 完成: 3 步`（cat=workflow, src=workflow_engine，
    历史 35 条）—— 标签与分类同名，只靠 src 守卫拦下。
    """
    from routing.routes import _is_role_report_title
    assert _is_role_report_title("[workflow] lr_tech_decision 完成: 3 步", "workflow", "workflow_engine") is True
    assert _is_role_report_title("[step_output] wf_x/s3 reviewer 产出审计", "workflow", "workflow_engine") is True


def test_has_content_words_exempts_content_signal_categories():
    """问题描述类分类不靠动词判定：无动词也必须放行（#423275/#422911 被吞）。"""
    from routing.routes import _has_content_words
    assert _has_content_words("[bug_report] cleanup_apply state_dir 仍取自计划", "bug_report") is True
    assert _has_content_words("[bug_report] workflow_engine pipeflow 引擎链串味", "bug_report") is True


def test_has_content_words_still_rejects_placeholders():
    """豁免不外溢：普通分类仍要求内容词，纯占位标题依旧拦。"""
    from routing.routes import _has_content_words
    assert _has_content_words("ok", "workflow") is False
    assert _has_content_words("[bug_report] cleanup 已完成", "code_fix") is False


def test_is_role_report_title_blocks_when_label_differs_from_category():
    """标签与分类不一致 = 角色汇报（[optimizer] cat=optimization）。"""
    from routing.routes import _is_role_report_title
    assert _is_role_report_title("[optimizer] 本轮优化 3 项", "optimization", "optimizer") is True


def test_is_self_echo_suppresses_only_own_messages():
    """回声抑制：只拦自己写的，不误杀他人（RFC 3834）。"""
    from routing.routes import _is_self_echo
    assert _is_self_echo({"src": "consumer"}, "consumer") is True
    assert _is_self_echo({"src": "engineer"}, "consumer") is False
    assert _is_self_echo({"src": "workflow_engine"}, "consumer") is False
    assert _is_self_echo({}, "consumer") is False
    assert _is_self_echo({"src": ""}, "consumer") is False



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
         patch("routing.routes.WorkflowClient") as mock_wfc:
        mock_rt_mod.get_router.return_value = MagicMock()
        # WorkflowClient 直连实时生产 DB：不固定 check_task，本用例结果就取决于
        # 当刻是否有 engineer 在跑工作流（全量套件里实测被判 deferred）。
        mock_wfc.return_value.check_task.return_value = None
        route_to_ccs("engineer", dry_run=False)

    body = sent.get("body", "")
    assert body.count("[test_report]") == 1, f"分类前缀重复: {body!r}"
    assert body.count("证据:") == 1, f"证据前缀重复: {body!r}"

@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_route_to_ccs_thinking_role_is_deferred_not_failed(mock_metrics, mock_hb):
    """角色思考中（ccs send 返回 busy）→ deferred，且**不标记消费**。

    busy 与投递失败必须分开：失败累计连败/告警，busy 只是排队。关键不变量是
    不调 mark_consumed —— bus 本身就是队列，未消费的消息下轮自然重推同一 id，
    既不会丢也不会重复投递。
    """
    from routing.routes import route_to_ccs
    msgs = [{"id": 77, "category": "task_spec", "text": "do the thing", "priority": 2}]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod, \
         patch("routing.routes.sys") as mock_sys, \
         patch("subprocess.run") as mock_run, \
         patch("routing.routes.WorkflowClient") as mock_wf, \
         patch("bus_protocol.Blackboard") as mock_bb:
        mock_rt_mod.get_router.return_value = MagicMock()
        status_result = MagicMock()
        status_result.stdout = '{"alive": true}'
        status_result.returncode = 0
        send_result = MagicMock()
        send_result.returncode = 2
        send_result.stdout = '{"success": false, "reason": "busy"}'
        send_result.stderr = ""
        mock_run.side_effect = [status_result, send_result]
        mock_sys.executable = "/usr/bin/python3"
        mock_wf.return_value.check_task.return_value = None  # 无活跃工作流
        result = route_to_ccs("engineer", dry_run=False)
    d = result["details"][0]
    assert d["action"] == "deferred", d
    # 未消费 → 下轮重推同一 id，不丢
    mock_bb.return_value.mark_consumed.assert_not_called()
    # 不建任务
    mock_wf.return_value.create_task_v2.assert_not_called()


# ── 定向消息绕过分类过滤 ──

def _engineer_router():
    """真实的 engineer 订阅表：不含 notice/blocker（生产 routes 表原值）。"""
    mock = MagicMock()
    mock.routing = {"engineer": {"consume": [
        "architecture", "task_spec", "user_story", "code_review", "bug_report",
        "test_report", "test_plan", "documentation", "security_audit",
        "system_design", "code_fix", "scheduler"]}}
    return mock


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_directed_mention_bypasses_consume_filter(mock_metrics, mock_hb):
    """@engineer 的 notice 不在 engineer 的 consume 列表，但点名了就必须送到。

    实测 24h 内 62 条 @engineer 中 26 条（42%）被分类过滤丢弃，
    其中 notice×13、blocker×4 —— 都是点名发给 engineer 的。
    """
    from routing.routes import route_to_ccs
    msgs = [{"id": 88, "category": "notice", "text": "@engineer 你的修复没生效",
             "priority": 2}]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = _engineer_router()
        result = route_to_ccs("engineer", dry_run=True)
    assert result["total"] == 1, "定向消息被分类过滤丢弃"
    assert result["details"][0]["action"] == "dry_run"


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_undirected_unsubscribed_category_still_filtered(mock_metrics, mock_hb):
    """没有 @ 的消息仍受分类过滤约束——广播抑制不能被这次改动拆掉。"""
    from routing.routes import route_to_ccs
    msgs = [{"id": 89, "category": "notice", "text": "系统广播：某服务重启了",
             "priority": 2}]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = _engineer_router()
        result = route_to_ccs("engineer", dry_run=True)
    assert result["total"] == 0, "未点名的无关分类不应投递给 engineer"


@patch("routing.routes.HEARTBEAT")
@patch("routing.routes.METRICS")
def test_directed_task_spec_not_routed(mock_metrics, mock_hb):
    """task_spec 是派发记录：写入者自己已 ccs send，路由再投就是双投。

    实测 product_architect 存量 528 条 @它 的 task_spec 中位年龄 100h，
    绕过分类过滤会把这批早已投过的锚点整批重放。
    """
    from routing.routes import route_to_ccs
    msgs = [{"id": 90, "category": "task_spec",
             "text": "needs_implementation @engineer 工作流任务: wf_abc123/s1",
             "priority": 1}]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
         patch("routing.routes._rt_mod") as mock_rt_mod:
        mock_rt_mod.get_router.return_value = _engineer_router()
        result = route_to_ccs("engineer", dry_run=True)
    assert result["total"] == 0, "task_spec 锚点不得经路由重投"
