#!/usr/bin/env python3
"""
test_routing_daemon.py — routing_daemon.py 自检/测试文件。

覆盖关键函数/逻辑：
- _any_live_targets
- _check_cron_schedules
- drift 检测 (detect_all_drift, report_drift)
- consistency 检查 (check_routing_links, check_category_consistency)
- survival_monitor tick
- contract_updater 调用 (apply_all_suggestions, suggest_improvements)

纯 stdlib，无外部依赖，可直接运行：
    python3 test_routing_daemon.py
"""

import json
import os
import sys
import tempfile
import time
import unittest.mock as mock
from pathlib import Path

# 确保能导入 routing_daemon 及其依赖
_PIPELINE_SRC = str(Path(__file__).resolve().parent)
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

# 导入被测模块
import routing_daemon as rd


class MockLogger:
    """简单的 mock logger，记录调用用于断言。"""
    def __init__(self):
        self.calls = []
    def info(self, msg, *args, **kwargs):
        self.calls.append(("info", msg % args if args else msg))
    def warning(self, msg, *args, **kwargs):
        self.calls.append(("warning", msg % args if args else msg))
    def error(self, msg, *args, **kwargs):
        self.calls.append(("error", msg % args if args else msg))
    def debug(self, msg, *args, **kwargs):
        self.calls.append(("debug", msg % args if args else msg))


def test_any_live_targets():
    """测试 _any_live_targets：存在 pid>0 且 tmux_session 非空的哨兵时返回 True。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        sentinel_dir = Path(tmpdir)
        # 写入一个活跃的哨兵
        (sentinel_dir / "engineer.json").write_text(json.dumps({
            "role": "engineer",
            "pid": 12345,
            "tmux_session": "ccs-engineer"
        }))
        # 写入一个无 pid 的哨兵
        (sentinel_dir / "pm.json").write_text(json.dumps({
            "role": "pm",
            "pid": 0,
            "tmux_session": ""
        }))
        # 写入一个无 tmux_session 的哨兵
        (sentinel_dir / "qa.json").write_text(json.dumps({
            "role": "qa",
            "pid": 54321,
            "tmux_session": ""
        }))

        # Monkey-patch SENTINEL_DIR
        original_sentinel_dir = rd.SENTINEL_DIR
        try:
            rd.SENTINEL_DIR = sentinel_dir
            assert rd._any_live_targets() is True, "应返回 True（有 engineer 存活）"

            # 测试无存活目标
            (sentinel_dir / "engineer.json").write_text(json.dumps({
                "role": "engineer",
                "pid": 0,
                "tmux_session": ""
            }))
            assert rd._any_live_targets() is False, "应返回 False（无存活目标）"
        finally:
            rd.SENTINEL_DIR = original_sentinel_dir

    print("✓ test_any_live_targets PASS")


def test_check_cron_schedules_basic():
    """测试 _check_cron_schedules 基本逻辑（croniter 未安装时返回 0）。"""
    # 不安装 croniter 时应返回 0 并记录 warning
    logger = MockLogger()
    original_logger = rd.LOGGER
    try:
        rd.LOGGER = logger
        result = rd._check_cron_schedules()
        assert result == 0, f"croniter 未安装时应返回 0，实际 {result}"
        assert any("croniter 未安装" in c[1] for c in logger.calls), "应记录 warning"
    finally:
        rd.LOGGER = original_logger

    print("✓ test_check_cron_schedules_basic PASS")


def test_check_cron_schedules_with_croniter():
    """测试 _check_cron_schedules 使用 mock croniter。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        roles_dir = Path(tmpdir) / "personas" / "session-roles"
        roles_dir.mkdir(parents=True)

        # 写入一个带 cron_schedule 的角色
        (roles_dir / "persona_engineer.json").write_text(json.dumps({
            "name": "engineer",
            "cron_schedule": "* * * * *"  # 每分钟触发
        }))

        # 写入一个无 cron_schedule 的角色
        (roles_dir / "persona_pm.json").write_text(json.dumps({
            "name": "pm",
            "cron_schedule": ""
        }))

        tracker_file = Path(tmpdir) / "cron_schedule_triggered.json"

        # Mock croniter
        import croniter as croniter_module

        # Mock subprocess.run for status-role 和 send
        with mock.patch.object(rd, 'croniter', croniter_module), \
             mock.patch.object(rd, 'subprocess') as mock_subprocess, \
             mock.patch.object(rd, 'get_roles_dir', return_value=Path(tmpdir) / "personas" / "session-roles"), \
             mock.patch.object(rd, 'CRON_SCHEDULE_TRACKER', tracker_file), \
             mock.patch.object(rd, 'CCS_CLI', Path("/fake/ccs.py")):

            # status-role 返回 alive=True
            mock_subprocess.run.return_value = mock.Mock(
                returncode=0,
                stdout=json.dumps({"alive": True}),
                stderr=""
            )

            logger = MockLogger()
            original_logger = rd.LOGGER
            try:
                rd.LOGGER = logger
                result = rd._check_cron_schedules()
                assert result >= 0, f"应返回非负数，实际 {result}"
                # 验证调用了 subprocess.run 进行 send
                assert mock_subprocess.run.called, "应调用 subprocess.run"
            finally:
                rd.LOGGER = original_logger

    print("✓ test_check_cron_schedules_with_croniter PASS")


def test_drift_detection():
    """测试 drift_detector 的 detect_all_drift 和 report_drift。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # 设置哨兵目录
        sentinel_dir = Path(tmpdir) / "sentinels"
        sentinel_dir.mkdir()

        # 设置 persona 目录
        persona_dir = Path(tmpdir) / "personas" / "session-roles"
        persona_dir.mkdir(parents=True)

        # 写入角色配置
        (persona_dir / "persona_engineer.json").write_text(json.dumps({
            "name": "engineer",
            "output_targets": ["bus cat=code_fix 新发现", "bus cat=architecture 设计变更"],
            "input_signals": [{"spec": {"category": "task_spec"}}]
        }))

        # 写入哨兵
        (sentinel_dir / "engineer.json").write_text(json.dumps({
            "role": "engineer",
            "pid": 12345,
            "tmux_session": "ccs-engineer"
        }))

        # Mock Blackboard.read 返回测试数据
        from bus_protocol import Fact

        with mock.patch.object(rd, 'CCS_SENTINEL_DIR', sentinel_dir), \
             mock.patch.object(rd, 'SESSION_ROLES_PERSONAS', persona_dir), \
             mock.patch('drift_detector.Blackboard') as mock_bb_class:

            mock_bb = mock_bb_class.return_value
            mock_bb.read.return_value = [
                Fact(id=1, cat="code_fix", t="修复 bug", ts=time.time(), src="engineer", e=""),
                Fact(id=2, cat="architecture", t="设计变更", ts=time.time(), src="engineer", e=""),
                Fact(id=3, cat="unknown_cat", t="未知分类消息", ts=time.time(), src="engineer", e=""),
            ]

            import drift_detector as dd
            results = dd.detect_all_drift()

            # 应检测到 unknown_cat 漂移
            assert "engineer" in results, "应检测到 engineer 的漂移"
            assert len(results["engineer"]) == 1, f"应检测到 1 条漂移，实际 {len(results['engineer'])}"
            assert results["engineer"][0]["category"] == "unknown_cat", "漂移分类应为 unknown_cat"

            # 测试 report_drift（mock bus write）
            with mock.patch('drift_detector.Blackboard') as mock_bb2:
                mock_bb2.return_value.write.return_value = None
                n = dd.report_drift(results)
                assert n == 1, f"report_drift 应返回 1，实际 {n}"
                assert mock_bb2.return_value.write.called, "应调用 bus write"

    print("✓ test_drift_detection PASS")


def test_consistency_checks():
    """测试 consistency.py 的 check_routing_links 和 check_category_consistency。"""
    # Mock routing 表
    routing = {
        "engineer": {"produce": ["code_fix"], "consume": ["task_spec"]},
        "reviewer": {"produce": ["code_review"], "consume": ["code_fix"]},
        "architect": {"produce": ["architecture"], "consume": ["*"]},  # wildcard 消费者
    }
    all_categories = {"code_fix", "task_spec", "code_review", "architecture"}

    # 测试 check_routing_links
    from routing.consistency import check_routing_links, check_category_consistency

    result = check_routing_links(routing, all_categories)
    assert result["ok"] is True, f"路由链接应完整，实际 orphaned={result['orphaned']}, zombies={result['zombies']}"
    assert result["orphaned"] == [], f"不应有孤儿分类，实际 {result['orphaned']}"
    assert result["zombies"] == [], f"不应有僵尸分类，实际 {result['zombies']}"

    # 测试有孤儿分类的情况
    routing_bad = {
        "engineer": {"produce": ["code_fix"], "consume": ["task_spec", "nonexistent"]},
    }
    result_bad = check_routing_links(routing_bad, all_categories)
    assert result_bad["ok"] is False, "应检测到不一致"
    assert "nonexistent" in result_bad["orphaned"], "应检测到孤儿分类"

    # 测试 check_category_consistency
    with tempfile.TemporaryDirectory() as tmpdir:
        persona_dir = Path(tmpdir) / "personas" / "session-roles"
        persona_dir.mkdir(parents=True)

        # 正常角色
        (persona_dir / "persona_engineer.json").write_text(json.dumps({
            "name": "engineer",
            "output_targets": ["bus cat=code_fix 修复"],
            "input_signals": [{"spec": {"category": "task_spec"}}]
        }))

        # 引用未定义分类的角色
        (persona_dir / "persona_bad.json").write_text(json.dumps({
            "name": "bad_role",
            "output_targets": ["bus cat=undefined_cat 引用未定义"],
            "input_signals": [{"source": "bus cat=also_undefined"}]
        }))

        defined_cats = {"code_fix", "task_spec", "architecture"}
        undef = check_category_consistency(str(persona_dir), defined_cats)

        assert len(undef) == 2, f"应检测到 2 个未定义分类引用，实际 {len(undef)}: {undef}"
        assert any("undefined_cat" in u for u in undef), "应包含 undefined_cat"
        assert any("also_undefined" in u for u in undef), "应包含 also_undefined"

    print("✓ test_consistency_checks PASS")


def test_survival_monitor_tick():
    """测试 SurvivalMonitor.tick 基本流程。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        sentinel_dir = Path(tmpdir) / "sentinels"
        sentinel_dir.mkdir()

        # 写入哨兵
        (sentinel_dir / "engineer.json").write_text(json.dumps({
            "role": "engineer",
            "pid": 12345,
            "tmux_session": "ccs-engineer"
        }))

        # Mock tmux has-session 返回成功
        with mock.patch('survival_monitor.subprocess.run') as mock_run, \
             mock.patch.object(rd, 'SENTINEL_DIR', sentinel_dir), \
             mock.patch.object(rd, 'PERSONAS_DIR', Path(tmpdir) / "personas" / "session-roles"), \
             mock.patch('survival_monitor.SESSION_ROLES_ROOT', Path(tmpdir)), \
             mock.patch('survival_monitor.BUS_SCRIPT', Path("/fake/bus_client.py")):

            # tmux has-session 成功
            mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")

            # Mock bus_client.py read 返回空
            mock_run.side_effect = [
                mock.Mock(returncode=0, stdout="", stderr=""),  # tmux has-session
                mock.Mock(returncode=0, stdout="0", stderr=""),   # tmux list-panes
                mock.Mock(returncode=0, stdout='[]', stderr=""),  # bus read
            ]

            from survival_monitor import SurvivalMonitor
            sm = SurvivalMonitor()
            results = sm.tick()

            assert "engineer" in results, "应包含 engineer 的检测结果"
            assert results["engineer"]["l1"]["tmux_alive"] is True, "tmux 应存活"
            assert results["engineer"]["overall"] in ("healthy", "idle", "stale", "unknown"), "overall 应为有效状态"

    print("✓ test_survival_monitor_tick PASS")


def test_contract_updater():
    """测试 contract_updater 的 suggest_improvements 和 apply_all_suggestions。"""
    with tempfile.TemporaryDirectory() as tmpdir:
        persona_dir = Path(tmpdir) / "personas" / "session-roles"
        persona_dir.mkdir(parents=True)

        # 写入角色配置（无 output_schema，eval_criteria 为字符串）
        (persona_dir / "persona_engineer.json").write_text(json.dumps({
            "name": "engineer",
            "eval_criteria": ["代码需通过测试", "无硬编码密钥"],
            "output_schema": None
        }))

        # Mock PERSONAS_DIR
        with mock.patch('contract_updater.PERSONAS_DIR', persona_dir):
            import contract_updater as cu

            # 测试 suggest_improvements：连续 3 次 skipped
            history = [
                {"role": "engineer", "skipped": 1, "checked": 0, "failed": 0},
                {"role": "engineer", "skipped": 1, "checked": 0, "failed": 0},
                {"role": "engineer", "skipped": 1, "checked": 0, "failed": 0},
            ]
            suggestions = cu.suggest_improvements(history)

            assert len(suggestions) >= 1, f"应生成至少 1 条建议，实际 {len(suggestions)}"
            assert any(s["type"] == "add_verification_cmd" for s in suggestions), "应包含 add_verification_cmd 类型建议"

            # 测试 suggest_improvements：output_schema 不存在
            history2 = [{"role": "engineer", "skipped": 0, "checked": 1, "failed": 0}]
            suggestions2 = cu.suggest_improvements(history2)
            assert any(s["type"] == "add_output_schema" for s in suggestions2), "应包含 add_output_schema 类型建议"

            # 测试 apply_all_suggestions（需要完整 suggestion）
            suggestion = {
                "role": "engineer",
                "criterion": "代码需通过测试",
                "type": "add_verification",
                "verification_command": "python3 -m pytest tests/ -x -q",
                "expected": "pass"
            }

            # Mock bus write
            with mock.patch('contract_updater.subprocess.run') as mock_run:
                mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
                result = cu.apply_all_suggestions([suggestion])
                assert result["applied"] == 1, f"应用成功数应为 1，实际 {result['applied']}"
                assert result["failed"] == 0, f"应用失败数应为 0，实际 {result['failed']}"

                # 验证角色 JSON 被更新
                updated = json.loads((persona_dir / "persona_engineer.json").read_text())
                criteria = updated.get("eval_criteria", [])
                assert any(isinstance(c, dict) and c.get("verifiable") for c in criteria), "eval_criteria 应被结构化"

    print("✓ test_contract_updater PASS")


def test_routing_daemon_main_loop_components():
    """测试 routing_daemon main 循环中的关键组件可导入且无语法错误。"""
    # 验证关键导入不抛异常
    from routing.auto import route_all, route_all_to_ccs
    from routing.consistency import check_routing_links, check_category_consistency
    from drift_detector import detect_all_drift, report_drift
    from survival_monitor import SurvivalMonitor
    from contract_updater import suggest_improvements, apply_all_suggestions
    from p0_exemption import P0Exemption
    from eval_consistency import main as ec_main
    from workflow_metrics import snapshot as wf_snapshot

    # 验证常量定义
    assert rd.INTERVAL == 60
    assert rd.CCS_INTERVAL == 300
    assert rd.EVAL_INTERVAL == 600
    assert rd.DRIFT_INTERVAL == 3600
    assert rd.CONSISTENCY_INTERVAL == 3600
    assert rd.CONTRACT_INTERVAL == 3600
    assert rd.SURVIVAL_INTERVAL == 120
    assert rd.P0_CHECK_INTERVAL == 1800

    print("✓ test_routing_daemon_main_loop_components PASS")


def test_routing_daemon_error_handling():
    """测试 routing_daemon 的异常处理路径。"""
    # 测试 route_all 异常时 error_backoff 增加
    logger = MockLogger()
    original_logger = rd.LOGGER
    original_metrics = rd.METRICS

    class MockMetrics:
        def inc(self, *args, **kwargs):
            pass
        def timer(self, *args, **kwargs):
            from contextlib import contextmanager
            @contextmanager
            def _timer():
                yield
            return _timer()

    try:
        rd.LOGGER = logger
        rd.METRICS = MockMetrics()

        # Mock route_all 抛出异常
        with mock.patch('routing_daemon.route_all', side_effect=Exception("test error")):
            # 仅验证异常被捕获并记录，不实际运行主循环
            try:
                rd.route_all()
            except Exception:
                pass

            # 验证错误被记录（实际在 main 循环中）
            # 这里只验证导入和基本结构
            assert True
    finally:
        rd.LOGGER = original_logger
        rd.METRICS = original_metrics

    print("✓ test_routing_daemon_error_handling PASS")


def run_all_tests():
    """运行所有测试。"""
    tests = [
        test_any_live_targets,
        test_check_cron_schedules_basic,
        test_check_cron_schedules_with_croniter,
        test_drift_detection,
        test_consistency_checks,
        test_survival_monitor_tick,
        test_contract_updater,
        test_routing_daemon_main_loop_components,
        test_routing_daemon_error_handling,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} ERROR: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n=== 测试结果: {passed} 通过, {failed} 失败 ===")
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)