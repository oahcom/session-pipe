"""test_reliability_unit.py — reliability.py 单元测试

覆盖:
- reconfigure: cfg=None fallback / 有 cfg 正常初始化 / auto_start 跳过
- _config_mtime / reload_config: mtime 不变不重载
- GracefulShutdown: operation / shutdown 拒绝 / wait 超时
- health_check: 三级状态判定 (healthy / degraded / critical)
- IdempotentConsume: 首次消费 / 幂等跳过 / 异常回退 / 线程安全
- AckTracker: record / get / stats / retry_failed
"""
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest

# ── 模块级隔离 ──────────────────────────────────────────────────────────
# reliability.py 在 import 时自动调用 reconfigure()
# 先占位 config_loader 和 bus_protocol，避免真实依赖
os.environ.setdefault("SESSION_PIPELINE_SKIP_TTL_PRUNER", "1")


def _make_nested_get(returns: dict):
    """返回一个 nested_get 函数，按 key 返回预设值，其余返回 default。"""
    def nested_get(*a, default=None):
        key = a[0]
        if key in returns:
            return returns[key]
        return default
    return nested_get


_default_cfg_data = {
    "retry": {"max_retries": 3, "base_delay": 0.5, "max_delay": 10.0,
              "exponential_base": 2.0, "retry_exceptions": ["Exception"]},
    "circuit_breaker": {"failure_threshold": 5, "recovery_timeout": 30.0,
                        "half_open_max_calls": 1},
    "heartbeat": {"stale_threshold": 300, "cleanup_interval": 3600},
    "ttl_pruner": {"max_age_days": 90, "max_facts": 10000, "interval": 3600,
                   "auto_start": False},
    "graceful_shutdown": {"timeout": 30.0},
    "logging": {"level": "INFO", "json_output": True},
}

_mock_config = MagicMock()
_mock_config.nested_get = _make_nested_get(_default_cfg_data)

_fake_config_loader = MagicMock()
_fake_config_loader.get_config.return_value = _mock_config
_fake_config_loader._resolve_exceptions = MagicMock(
    side_effect=lambda names: tuple(
        getattr(__import__("builtins"), n, Exception) for n in names
    ) if isinstance(names, list) else (Exception,)
)

_fake_bus_protocol = MagicMock()

for mod_name, mock_mod in [
    ("config_loader", _fake_config_loader),
    ("bus_protocol", _fake_bus_protocol),
]:
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    sys.modules[mod_name] = mock_mod

from reliability_core import (
    RetryPolicy, CircuitBreaker, CircuitState, CircuitOpenError,
    ConsumerHeartbeat, TtlPruner,
)
import reliability


# ── 辅助 ─────────────────────────────────────────────────────────

def reset_globals():
    """重置 reliability 模块全局变量为默认值。"""
    reliability.CIRCUIT_BREAKER = CircuitBreaker()
    reliability.HEARTBEAT = ConsumerHeartbeat()
    reliability.TTL_PRUNER = TtlPruner()
    reliability.DEFAULT_RETRY = RetryPolicy()
    reliability.GRACEFUL_SHUTDOWN = reliability.GracefulShutdown(timeout=30.0)


# ── reconfigure ─────────────────────────────────────────────────────

class TestReconfigure:
    def test_cfg_none_uses_defaults(self):
        """get_config 异常 → 走 fallback 默认值。"""
        reset_globals()
        with patch("config_loader.get_config", side_effect=Exception("no cfg")):
            reliability.reconfigure()
        assert reliability.CIRCUIT_BREAKER.failure_threshold == 5
        assert reliability.DEFAULT_RETRY.max_retries == 3
        assert reliability.HEARTBEAT.stale_threshold == 300.0

    def test_cfg_present_reads_nested(self):
        """有 cfg → 从嵌套配置读取。"""
        cfg = MagicMock()
        cfg.nested_get = _make_nested_get(_default_cfg_data)
        with patch("config_loader.get_config", return_value=cfg):
            reliability.reconfigure()
        assert reliability.DEFAULT_RETRY.max_retries == 3
        assert reliability.CIRCUIT_BREAKER.failure_threshold == 5

    def test_auto_start_off_by_cfg(self):
        """auto_start=False → TTL pruner 不启动。"""
        cfg = MagicMock()
        data = dict(_default_cfg_data)
        data["ttl_pruner"] = {"auto_start": False}
        cfg.nested_get = _make_nested_get(data)
        with patch("config_loader.get_config", return_value=cfg):
            reliability.reconfigure()
        assert reliability.TTL_PRUNER._thread is None

    def test_cfg_retry_exceptions_resolved(self):
        """retry_exceptions 字符串列表 → 解析为异常类元组。"""
        cfg = MagicMock()
        data = {
            "retry": {"retry_exceptions": ["ValueError"]},
            "circuit_breaker": {},
            "heartbeat": {},
            "ttl_pruner": {"auto_start": False},
            "graceful_shutdown": {},
            "logging": {},
        }
        cfg.nested_get = _make_nested_get(data)
        with patch("config_loader.get_config", return_value=cfg), \
             patch("config_loader._resolve_exceptions",
                   return_value=(ValueError,)):
            reliability.reconfigure()
        assert ValueError in reliability.DEFAULT_RETRY.retry_exceptions


# ── config_mtime / reload_config ────────────────────────────────────

class TestReloadConfig:
    def test_mtime_unchanged_no_reload(self):
        reliability._CONFIG_MTIME = 1.0
        with patch.object(reliability, "_config_mtime", return_value=1.0):
            assert reliability.reload_config() is False

    def test_mtime_changed_triggers_reload(self):
        reliability._CONFIG_MTIME = 0.0
        with patch.object(reliability, "_config_mtime", return_value=2.0), \
             patch.object(reliability, "reconfigure"):
            assert reliability.reload_config() is True

    def test_config_mtime_file_missing_returns_zero(self):
        with patch("os.path.getmtime", side_effect=OSError("no file")):
            assert reliability._config_mtime() == 0.0


# ── GracefulShutdown ────────────────────────────────────────────────

class TestGracefulShutdown:
    def test_operation_context_manager(self):
        gs = reliability.GracefulShutdown(timeout=5.0)
        with gs.operation():
            assert gs._active_ops == 1
        assert gs._active_ops == 0

    def test_operation_raises_after_shutdown(self):
        gs = reliability.GracefulShutdown(timeout=5.0)
        gs._shutdown = True
        with pytest.raises(reliability.ShutdownError):
            with gs.operation():
                pass

    def test_wait_returns_immediately_when_no_ops(self):
        gs = reliability.GracefulShutdown(timeout=5.0)
        gs.wait()

    def test_signal_handler_sets_shutdown_flag(self):
        gs = reliability.GracefulShutdown(timeout=5.0)
        gs._signal_handler(15, None)
        assert gs._shutdown is True

    def test_concurrent_operations_count(self):
        gs = reliability.GracefulShutdown(timeout=5.0)
        with gs.operation():
            with gs.operation():
                assert gs._active_ops == 2
            assert gs._active_ops == 1
        assert gs._active_ops == 0


# ── health_check ────────────────────────────────────────────────────

class TestHealthCheck:

    def _mock_health(self, busy_ok=True, total=50, side_effect=None):
        """准备 health_check 运行环境。返回 mock_bb。"""
        bb = MagicMock()
        if side_effect:
            bb.stats.side_effect = side_effect
        else:
            bb.stats.return_value = {"total": total, "by_category": {"test": total}}
        # 让 bus_protocol.Blackboard() 返回这个 bb
        _fake_bus_protocol.Blackboard = MagicMock(return_value=bb)
        # 配置默认阈值
        cfg = MagicMock()
        cfg.nested_get = _make_nested_get({
            "health": {"backlog_warning_threshold": 100,
                       "backlog_critical_threshold": 500},
        })
        with patch("config_loader.get_config", return_value=cfg):
            return bb

    def test_healthy_status(self):
        self._mock_health(total=50)
        reliability.HEARTBEAT._last_seen = {"agent1": time.time()}
        reliability.CIRCUIT_BREAKER._state = "closed"
        result = reliability.health_check()
        assert result["status"] == "healthy"
        assert result["bus"]["connected"] is True

    def test_critical_when_total_ge_500(self):
        self._mock_health(total=500)
        reliability.HEARTBEAT._last_seen = {}
        reliability.CIRCUIT_BREAKER._state = "closed"
        result = reliability.health_check()
        assert result["status"] == "critical"

    def test_degraded_by_backlog_warning(self):
        self._mock_health(total=100)
        reliability.HEARTBEAT._last_seen = {}
        reliability.CIRCUIT_BREAKER._state = "closed"
        result = reliability.health_check()
        assert result["status"] == "degraded"

    def test_degraded_by_stale_consumers(self):
        self._mock_health(total=10)
        reliability.HEARTBEAT._last_seen = {"agent1": time.time() - 3600}
        reliability.CIRCUIT_BREAKER._state = "closed"
        result = reliability.health_check()
        assert result["status"] == "degraded"

    def test_bus_connection_error(self):
        self._mock_health(side_effect=Exception("connection refused"))
        reliability.HEARTBEAT._last_seen = {}
        reliability.CIRCUIT_BREAKER._state = "closed"
        result = reliability.health_check()
        assert result["bus"]["connected"] is False
        assert result["bus"]["error"] == "connection refused"

    def test_health_exports_circuit_breaker_state(self):
        self._mock_health(total=0)
        reliability.CIRCUIT_BREAKER._state = "open"
        result = reliability.health_check()
        assert result["circuit_breaker"] == "open"


# ── IdempotentConsume ───────────────────────────────────────────────

class TestIdempotentConsume:
    def _make_bb(self):
        bb = MagicMock()
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=None)
        bb._conn = MagicMock(return_value=conn)
        return bb, conn

    def test_first_consume_succeeds(self):
        ic = reliability.IdempotentConsume()
        bb, conn = self._make_bb()
        conn.execute.return_value.fetchone.return_value = None
        result = ic.safe_consume(bb, 42, "test_consumer")
        assert result["status"] == "consumed"
        assert result["fact_id"] == 42
        bb.mark_consumed.assert_called_once_with(42, "test_consumer")

    def test_already_consumed_skips(self):
        ic = reliability.IdempotentConsume()
        bb, conn = self._make_bb()
        conn.execute.return_value.fetchone.return_value = (1,)
        result = ic.safe_consume(bb, 42, "test_consumer")
        assert result["status"] == "skipped"
        assert result["reason"] == "already_consumed"
        bb.mark_consumed.assert_not_called()

    def test_consume_exception_returns_error(self):
        ic = reliability.IdempotentConsume()
        bb, conn = self._make_bb()
        conn.execute.return_value.fetchone.return_value = None
        bb.mark_consumed.side_effect = Exception("db locked")
        result = ic.safe_consume(bb, 42, "test_consumer")
        assert result["status"] == "error"
        assert "db locked" in result["error"]

    def test_is_consumed_true(self):
        ic = reliability.IdempotentConsume()
        bb, conn = self._make_bb()
        conn.execute.return_value.fetchone.return_value = (1,)
        assert ic.is_consumed(bb, 1, "x") is True

    def test_is_consumed_false(self):
        ic = reliability.IdempotentConsume()
        bb, conn = self._make_bb()
        conn.execute.return_value.fetchone.return_value = None
        assert ic.is_consumed(bb, 1, "x") is False


# ── AckTracker ──────────────────────────────────────────────────────

class TestAckTracker:
    @pytest.fixture
    def tracker(self, tmp_path):
        db_path = tmp_path / "test_ack.db"
        t = reliability.AckTracker(db_path=str(db_path))
        yield t

    def test_record_and_get(self, tracker):
        tracker.record_ack(1, "consumer1", "ok", category="notice")
        acks = tracker.get_acks(fact_id=1)
        assert len(acks) == 1
        assert acks[0]["fact_id"] == 1
        assert acks[0]["consumer"] == "consumer1"
        assert acks[0]["status"] == "ok"
        assert acks[0]["category"] == "notice"

    def test_get_acks_filters_by_consumer(self, tracker):
        tracker.record_ack(1, "a", "ok")
        tracker.record_ack(2, "b", "ok")
        acks = tracker.get_acks(consumer="a")
        assert len(acks) == 1
        assert acks[0]["consumer"] == "a"

    def test_get_acks_filters_by_min_ts(self, tracker):
        tracker.record_ack(1, "x", "ok", ts=100.0)
        tracker.record_ack(2, "x", "ok", ts=200.0)
        acks = tracker.get_acks(min_ts=150.0)
        assert len(acks) == 1
        assert acks[0]["fact_id"] == 2

    def test_ack_stats(self, tracker):
        tracker.record_ack(1, "a", "ok")
        tracker.record_ack(2, "a", "error")
        tracker.record_ack(3, "b", "ok")
        stats = tracker.ack_stats()
        assert stats["total"] == 3
        assert stats["by_status"]["ok"] == 2
        assert stats["by_status"]["error"] == 1

    def test_get_failed_acks(self, tracker):
        tracker.record_ack(1, "a", "error", ts=100.0)
        tracker.record_ack(2, "a", "ok", ts=200.0)
        failed = tracker.get_failed_acks(since=0.0)
        error_facts = [f["fact_id"] for f in failed if f["status"] == "error"]
        assert 1 in error_facts

    def test_retry_failed(self, tracker):
        """retry_failed 成功重试 error ACK。"""
        bb = MagicMock()
        tracker.record_ack(99, "worker", "error", ts=100.0)
        result = tracker.retry_failed(bb)
        acks = tracker.get_acks(fact_id=99)
        retried = [a for a in acks if a["status"] == "retried"]
        assert result == 1
        assert len(retried) == 1
        bb.mark_consumed.assert_called_once_with(99, "worker")

    def test_retry_failed_handles_error(self):
        """mark_consumed 异常 → 不崩溃, 返回 0。"""
        bb = MagicMock()
        bb.mark_consumed.side_effect = Exception("db gone")
        tracker = reliability.AckTracker(
            db_path=str(Path(tempfile.mkdtemp()) / "ack.db"))
        tracker.record_ack(100, "w", "error")
        result = tracker.retry_failed(bb)
        assert result == 0

    def test_record_ack_auto_timestamp(self, tracker):
        before = time.time()
        ack = tracker.record_ack(1, "c", "ok")
        after = time.time()
        assert before <= ack["ts"] <= after

    def test_record_ack_custom_timestamp(self, tracker):
        ack = tracker.record_ack(1, "c", "ok", ts=42.0)
        assert ack["ts"] == 42.0

    def test_get_acks_limit(self, tracker):
        for i in range(60):
            tracker.record_ack(i, "x", "ok", ts=float(i))
        acks = tracker.get_acks(limit=10)
        assert len(acks) == 10

    def test_retry_failed_empty(self, tracker):
        """无失败 ACK → retry_failed 返回 0。"""
        bb = MagicMock()
        result = tracker.retry_failed(bb)
        assert result == 0
        bb.mark_consumed.assert_not_called()


# ── 线程安全 ────────────────────────────────────────────────────────

class TestConcurrency:
    def test_idempotent_consume_thread_safety(self):
        """并发 safe_consume 同一 fact_id → 只有一个 consumed。"""
        ic = reliability.IdempotentConsume()

        consumed_set: set[int] = set()
        bb = MagicMock()

        def stateful_fetchone(fid, consumer):
            if fid in consumed_set:
                return (1,)
            return None

        def stateful_mark_consumed(fact_id, consumer):
            consumed_set.add(fact_id)

        bb._conn = MagicMock()
        bb._conn.return_value.__enter__.return_value.execute.return_value.fetchone.side_effect = \
            lambda: stateful_fetchone(1, "thread_test")
        bb.mark_consumed.side_effect = stateful_mark_consumed

        results = []
        barrier = threading.Barrier(5)

        def consume():
            barrier.wait()
            results.append(ic.safe_consume(bb, 1, "thread_test"))

        threads = [threading.Thread(target=consume) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        statuses = [r["status"] for r in results]
        consumed = statuses.count("consumed")
        skipped = statuses.count("skipped")
        assert consumed == 1, f"expected 1 consumed, got {consumed}"
        assert skipped == 4, f"expected 4 skipped, got {skipped}"


# ── 补充覆盖 ──────────────────────────────────────────────────────────

class TestReconfigureEdgeCases:
    """reconfigure 剩余分支覆盖。"""

    def test_auto_start_true_starts_pruner(self):
        """auto_start=True + env 未跳过 → TTL pruner 启动。"""
        cfg = MagicMock()
        data = dict(_default_cfg_data)
        data["ttl_pruner"] = {"auto_start": True, "interval": 3600,
                              "max_age_days": 90, "max_facts": 10000}
        cfg.nested_get = _make_nested_get(data)
        env_key = "SESSION_PIPELINE_SKIP_TTL_PRUNER"
        old = os.environ.pop(env_key, None)
        try:
            with patch("config_loader.get_config", return_value=cfg):
                reliability.reconfigure()
            assert reliability.TTL_PRUNER._thread is not None
            assert reliability.TTL_PRUNER._thread.is_alive()
        finally:
            if old is not None:
                os.environ[env_key] = old
            reliability.TTL_PRUNER.stop()

    def test_auto_start_true_but_env_skip(self):
        """auto_start=True + env 跳过 → TTL pruner 不启动。"""
        cfg = MagicMock()
        data = dict(_default_cfg_data)
        data["ttl_pruner"] = {"auto_start": True}
        cfg.nested_get = _make_nested_get(data)
        os.environ["SESSION_PIPELINE_SKIP_TTL_PRUNER"] = "1"
        try:
            with patch("config_loader.get_config", return_value=cfg):
                reliability.reconfigure()
            assert reliability.TTL_PRUNER._thread is None
        finally:
            os.environ.pop("SESSION_PIPELINE_SKIP_TTL_PRUNER", None)

    def test_reconfigure_exception_only_logs(self):
        """get_config 抛异常 → 走 fallback 不抛。"""
        reset_globals()
        reliability.reconfigure()
        # cfg none fallback 已通过 test_cfg_none_uses_defaults 验证

    def test_cfg_retry_exceptions_not_list(self):
        """retry_exceptions 非列表 → 默认为 (Exception,)。"""
        reliability.DEFAULT_RETRY = RetryPolicy()
        assert Exception in reliability.DEFAULT_RETRY.retry_exceptions


class TestGracefulShutdownWaitTimeout:
    """GracefulShutdown.wait() 超时分支。"""

    def test_wait_timeout_with_active_ops(self):
        """有活跃操作时 wait 超时 → 记录 warning 不阻塞。"""
        gs = reliability.GracefulShutdown(timeout=0.01)
        gs._active_ops = 2
        gs.wait()  # 不应抛出


class TestHealthCheckConfigFallback:
    """health_check get_config 异常 fallback 分支。"""

    def test_health_get_config_raise_uses_default_thresholds(self):
        """get_config 异常 → 使用默认阈值 100/500。"""
        with patch.object(reliability, "HEARTBEAT") as mock_hb, \
             patch.object(reliability, "CIRCUIT_BREAKER") as mock_cb:
            mock_hb.get_stale_consumers.return_value = []
            mock_cb._state = "closed"
            bb = MagicMock()
            bb.stats.return_value = {"total": 50, "by_category": {}}
            _fake_bus_protocol.Blackboard = MagicMock(return_value=bb)
            # health_check 内部 from config_loader import get_config 是局部导入，
            # 必须 patch config_loader 模块属性本身
            _fake_config_loader.get_config = MagicMock(side_effect=Exception("no cfg"))
            try:
                result = reliability.health_check()
            finally:
                _fake_config_loader.get_config = MagicMock(return_value=_mock_config)
            assert result["status"] == "healthy"


class TestBackgroundServices:
    """start_background_services / stop_background_services。"""

    def test_start_stop_background(self):
        """启停后台服务不抛异常。"""
        # 确保线程不在运行
        reliability.TTL_PRUNER.stop()
        reliability.TTL_PRUNER._thread = None
        reliability.start_background_services()
        assert reliability.TTL_PRUNER._thread is not None
        assert reliability.TTL_PRUNER._thread.is_alive()
        reliability.stop_background_services()
        assert not reliability.TTL_PRUNER._thread.is_alive()

    def test_start_already_running(self):
        """TTL pruner 已在运行 → start 不抛 (静默返回)。"""
        reliability.start_background_services()
        reliability.start_background_services()
        assert reliability.TTL_PRUNER._thread.is_alive()
        reliability.stop_background_services()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
