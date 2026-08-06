"""Unit tests for routing/polling.py — poll_unconsumed error paths.
Mocks all external deps (Blackboard, config, circuit breaker, metrics, cursor)."""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))


class FakeFact:
    def __init__(self, id, cat, t, e=None):
        self.id = id
        self.cat = cat
        self.t = t
        self.e = e or ""


def test_poll_config_read_failure_logs_warning():
    """config 读取失败时打 warning 日志 + metrics 计数，不沉默。"""
    from routing.polling import poll_unconsumed

    with (
        patch("bus_protocol.Blackboard") as mock_bb,
        patch("config_loader.get_config") as mock_cfg,
        patch("routing.polling.METRICS") as mock_metrics,
        patch("routing.polling.LOGGER") as mock_logger,
        patch("routing.polling.CIRCUIT_BREAKER") as mock_cb,
        patch("routing.polling.get_last_cursor", return_value=0),
        patch("routing.polling.HEARTBEAT"),
        patch("routing.polling._rt_mod.get_router"),
    ):
        mock_cfg.side_effect = RuntimeError("config corrupted")
        mock_bb.return_value.unconsumed.return_value = []
        mock_cb.call.return_value = []

        result = poll_unconsumed()

        assert result == [], f"expected empty list, got {result}"
        mock_logger.warning.assert_called_once()
        args, _ = mock_logger.warning.call_args
        assert "config read failed" in args[0]
        mock_metrics.inc.assert_any_call("config_errors_total")


def test_poll_circuit_breaker_error_returns_error_dict():
    """熔断器异常时返回 [{'error': ...}] 而不是沉默传播。"""
    from routing.polling import poll_unconsumed

    with (
        patch("bus_protocol.Blackboard"),
        patch("config_loader.get_config") as mock_cfg,
        patch("routing.polling.METRICS") as mock_metrics,
        patch("routing.polling.LOGGER"),
        patch("routing.polling.CIRCUIT_BREAKER") as mock_cb,
        patch("routing.polling.get_last_cursor", return_value=0),
        patch("routing.polling.HEARTBEAT"),
        patch("routing.polling._rt_mod.get_router"),
    ):
        mock_cfg.return_value.nested_get.return_value = 100
        mock_cb.call.side_effect = RuntimeError("bus unreachable")

        result = poll_unconsumed()

        assert len(result) == 1
        assert "error" in result[0]
        assert "bus unreachable" in result[0]["error"]
        mock_metrics.inc.assert_any_call("poll_errors_total")


def test_poll_success_returns_sorted_messages():
    """正常路径：消息按 priority 排序，metrics 递增加心跳。"""
    from routing.polling import poll_unconsumed

    facts = [FakeFact(1, "performance", "slow query"), FakeFact(2, "code_fix", "bug")]
    priorities = {"performance": 2, "code_fix": 0, "security": 1}

    with (
        patch("bus_protocol.Blackboard") as mock_bb,
        patch("config_loader.get_config") as mock_cfg,
        patch("routing.polling.METRICS") as mock_metrics,
        patch("routing.polling.LOGGER"),
        patch("routing.polling.CIRCUIT_BREAKER") as mock_cb,
        patch("routing.polling.get_last_cursor", return_value=0),
        patch("routing.polling.HEARTBEAT") as mock_hb,
        patch("routing.polling._rt_mod") as mock_rt,
    ):
        mock_cfg.return_value.nested_get.return_value = 100
        mock_bb.return_value.unconsumed.return_value = facts
        mock_cb.call.side_effect = lambda fn: fn()
        mock_rt.priority.side_effect = lambda cat: priorities.get(cat, 99)
        mock_rt.get_router.return_value.get_consumers_prioritized.return_value = ["claude"]

        result = poll_unconsumed()

        assert len(result) == 2
        assert result[0]["category"] == "code_fix"
        assert result[0]["priority"] == 0
        assert result[1]["category"] == "performance"
        assert result[1]["priority"] == 2
        mock_hb.beat.assert_called_once_with("pipeline")
        mock_metrics.inc.assert_any_call("poll_count")
        mock_metrics.observe.assert_called_once_with("backlog_size", 2)


def test_poll_category_filter():
    """指定 category 时只返回匹配的消息。"""
    from routing.polling import poll_unconsumed

    facts = [FakeFact(1, "security", "intrusion"), FakeFact(2, "code_fix", "typo")]

    with (
        patch("bus_protocol.Blackboard") as mock_bb,
        patch("config_loader.get_config") as mock_cfg,
        patch("routing.polling.METRICS"),
        patch("routing.polling.LOGGER"),
        patch("routing.polling.CIRCUIT_BREAKER") as mock_cb,
        patch("routing.polling.get_last_cursor", return_value=0),
        patch("routing.polling.HEARTBEAT"),
        patch("routing.polling._rt_mod") as mock_rt,
    ):
        mock_cfg.return_value.nested_get.return_value = 100
        mock_bb.return_value.unconsumed.return_value = facts
        mock_cb.call.side_effect = lambda fn: fn()
        mock_rt.priority.return_value = 0
        mock_rt.get_router.return_value.get_consumers_prioritized.return_value = ["claude"]

        result = poll_unconsumed(category="security")

        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["category"] == "security"
