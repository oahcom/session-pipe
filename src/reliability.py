"""reliability.py — 可靠性基础设施（精简版）

从 reliability_core 导入核心类/函数。
负责模块级实例初始化、配置重载、健康检查。
"""
from reliability_core import (
    RetryPolicy, with_retry, CircuitState, CircuitBreaker,
    CircuitOpenError, ConsumerHeartbeat, TtlPruner,
    MetricsCollector, METRICS,
    setup_logging, JsonFormatter, with_trace_id, LOGGER,
    init_cursor_db, get_last_cursor, set_last_cursor,
)

import os
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional

import logging as _logging




# 占位符，在文件末尾由 reconfigure() 填充
CIRCUIT_BREAKER = None
HEARTBEAT = None
TTL_PRUNER = None
DEFAULT_RETRY = None
GRACEFUL_SHUTDOWN = None


def reconfigure() -> None:
    """从 config.yaml 读取配置并初始化全局实例。"""
    global CIRCUIT_BREAKER, HEARTBEAT, TTL_PRUNER, DEFAULT_RETRY, GRACEFUL_SHUTDOWN
    from config_loader import get_config
    try:
        cfg = get_config()
    except Exception:
        cfg = None

    if cfg is None:
        CIRCUIT_BREAKER = CircuitBreaker()
        HEARTBEAT = ConsumerHeartbeat()
        TTL_PRUNER = TtlPruner()
        DEFAULT_RETRY = RetryPolicy()
        GRACEFUL_SHUTDOWN = GracefulShutdown()
        return

    r = cfg.nested_get("retry", default={})
    from config_loader import _resolve_exceptions
    retry_exc_raw = r.get("retry_exceptions", ["Exception"])
    retry_exc = _resolve_exceptions(retry_exc_raw) if isinstance(retry_exc_raw, list) else (Exception,)
    DEFAULT_RETRY = RetryPolicy(
        max_retries=r.get("max_retries", 3),
        base_delay=r.get("base_delay", 0.5),
        max_delay=r.get("max_delay", 10.0),
        exponential_base=r.get("exponential_base", 2.0),
        retry_exceptions=retry_exc,
    )
    cb = cfg.nested_get("circuit_breaker", default={})
    CIRCUIT_BREAKER = CircuitBreaker(
        failure_threshold=cb.get("failure_threshold", 5),
        recovery_timeout=cb.get("recovery_timeout", 30.0),
        half_open_max_calls=cb.get("half_open_max_calls", 1),
    )
    hb = cfg.nested_get("heartbeat", default={})
    HEARTBEAT = ConsumerHeartbeat(
        stale_threshold=hb.get("stale_threshold", 300),
        cleanup_interval=hb.get("cleanup_interval", 3600),
    )
    ttl = cfg.nested_get("ttl_pruner", default={})
    TTL_PRUNER = TtlPruner(
        max_age_days=ttl.get("max_age_days", 90),
        max_facts=ttl.get("max_facts", 10000),
        interval=ttl.get("interval", 3600),
    )
    # Fix 2: 应用 ttl_pruner.auto_start 配置
    # 环境变量 SESSION_PIPELINE_SKIP_TTL_PRUNER=1 可在测试时跳过自动启动
    if ttl.get("auto_start", True) and not os.environ.get("SESSION_PIPELINE_SKIP_TTL_PRUNER"):
        TTL_PRUNER.start()
    gs = cfg.nested_get("graceful_shutdown", default={})
    GRACEFUL_SHUTDOWN = GracefulShutdown(timeout=gs.get("timeout", 30.0))
    # 应用 logging 配置（Fix 2：logging.level + logging.json_output）
    log_cfg = cfg.nested_get("logging", default={})
    log_level_name = log_cfg.get("level", "INFO").upper()
    log_level = getattr(_logging, log_level_name, _logging.INFO)
    log_json = log_cfg.get("json_output", True)
    setup_logging(level=log_level, json_output=log_json)


# ── 配置热加载 ──────────────────────────────────────────────────────

# 记录上次加载时 config.yaml 的 mtime，变化时才重建全局实例
_CONFIG_MTIME: float = 0.0


_CONFIG_FILE = Path(os.environ.get(
    "SESSION_PIPELINE_CONFIG",
    str(Path.home() / "session-pipeline" / "config" / "config.yaml"),
))


def _config_mtime() -> float:
    """返回 config.yaml 的修改时间戳，文件不存在返回 0。"""
    try:
        return os.path.getmtime(_CONFIG_FILE)
    except OSError:
        return 0.0


def reload_config() -> bool:
    """基于 mtime 对比决定是否重载。返回 True 表示实际发生了重载。"""
    global _CONFIG_MTIME
    mtime = _config_mtime()
    if mtime == _CONFIG_MTIME:
        return False
    _CONFIG_MTIME = mtime
    reconfigure()
    LOGGER.info("Config reloaded (mtime changed)", extra=with_trace_id())
    return True


# ── 优雅关闭 ────────────────────────────────────────────────────────

class GracefulShutdown:
    """处理 SIGTERM/SIGINT，等待进行中操作完成。"""
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._shutdown = False
        self._active_ops = 0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        LOGGER.info(f"Received signal {signum}, initiating graceful shutdown...", extra=with_trace_id())
        self._shutdown = True

    @contextmanager
    def operation(self):
        """标记一个进行中的操作。"""
        with self._lock:
            if self._shutdown:
                raise ShutdownError("Shutdown in progress")
            self._active_ops += 1
        try:
            yield
        finally:
            with self._lock:
                self._active_ops -= 1
                if self._active_ops == 0:
                    self._cond.notify_all()

    def wait(self):
        """等待所有操作完成或超时。"""
        with self._lock:
            if self._active_ops == 0:
                return
            self._cond.wait(timeout=self.timeout)
            if self._active_ops > 0:
                LOGGER.warning(f"Graceful shutdown timeout, {self._active_ops} ops still running", extra=with_trace_id())


class ShutdownError(Exception):
    """关闭期间拒绝新操作。"""
    pass


# ── 健康检查 ────────────────────────────────────────────────────────

def health_check() -> dict:
    """健康检查端点返回字典。使用 config.yaml 中的 backlog 阈值（Fix 2）。"""
    from bus_protocol import Blackboard
    from config_loader import get_config
    bb = Blackboard()
    try:
        stats = bb.stats()
        bus_ok = True
        bus_error = None
    except Exception as e:
        bus_ok = False
        bus_error = str(e)
        stats = {"total": 0, "by_category": {}}

    stale_consumers = HEARTBEAT.get_stale_consumers()
    total_facts = stats.get("total", 0)
    # 使用 config 中的 backlog 阈值（Fix 2）
    cfg = get_config()
    warn_thresh = cfg.nested_get("health", "backlog_warning_threshold", default=100)
    crit_thresh = cfg.nested_get("health", "backlog_critical_threshold", default=500)
    if total_facts >= crit_thresh:
        status = "critical"
    elif total_facts >= warn_thresh or stale_consumers:
        status = "degraded"
    else:
        status = "healthy"

    return {
        "status": status,
        "bus": {
            "connected": bus_ok,
            "error": bus_error,
            "total_facts": total_facts,
        },
        "consumers": {
            "stale": stale_consumers,
            "stale_count": len(stale_consumers),
        },
        "circuit_breaker": CIRCUIT_BREAKER._state,
        "ttl_pruner": {
            "last_run": TTL_PRUNER._last_run,
            "interval": TTL_PRUNER.interval,
        },
        "timestamp": time.time(),
    }


# ── 便捷函数 ────────────────────────────────────────────────────────

def start_background_services():
    """启动所有后台服务（TTL pruner 等）。"""
    TTL_PRUNER.start()
    LOGGER.info("Background services started", extra=with_trace_id())


def stop_background_services():
    """停止所有后台服务。"""
    TTL_PRUNER.stop()
    LOGGER.info("Background services stopped", extra=with_trace_id())


# ── 幂等消费 + 乐观锁 claim ────────────────────────────────────────

class IdempotentConsume:
    """幂等消费：consumer + fact_id 唯一索引防止重复 consume。

    SQLite 不支持 SELECT FOR UPDATE，用乐观锁模拟：
    检查 log 表是否已有 (fid=目标, typ=consume) → 有则跳过。
    """

    def __init__(self):
        self._lock = threading.Lock()

    def is_consumed(self, bb, fact_id: int, consumer: str) -> bool:
        """检查指定 fact 是否已被指定 consumer 消费。"""
        with bb._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM log WHERE fid = ? AND typ = 'consume' AND cat = ? LIMIT 1",
                (fact_id, consumer),
            ).fetchone()
            return row is not None

    def safe_consume(self, bb, fact_id: int, consumer: str) -> dict:
        """幂等消费：已消费则返回 skipped，否则执行 consume。

        返回：
        - {"status": "consumed", "fact_id": N, "consumer": "X"}
        - {"status": "skipped", "fact_id": N, "consumer": "X", "reason": "already_consumed"}
        - {"status": "error", "fact_id": N, "consumer": "X", "error": "..."}
        """
        with self._lock:
            if self.is_consumed(bb, fact_id, consumer):
                LOGGER.debug(f"Idempotent skip: #{fact_id} already consumed by {consumer}",
                           extra=with_trace_id(str(fact_id)))
                return {
                    "status": "skipped",
                    "fact_id": fact_id,
                    "consumer": consumer,
                    "reason": "already_consumed",
                }

            try:
                bb.mark_consumed(fact_id, consumer)
                LOGGER.info(f"Consumed #{fact_id} by {consumer}",
                           extra=with_trace_id(str(fact_id)))
                return {
                    "status": "consumed",
                    "fact_id": fact_id,
                    "consumer": consumer,
                }
            except Exception as e:
                LOGGER.error(f"Consume #{fact_id} failed: {e}",
                           extra=with_trace_id(str(fact_id)))
                return {
                    "status": "error",
                    "fact_id": fact_id,
                    "consumer": consumer,
                    "error": str(e),
                }


class OptimisticClaim:
    """乐观锁 claim：在 route-all 时防止多实例抢同一消息。

    策略：用 SQLite 事务包裹 check + consume，
    如果 check 到已消费（并发竞争）则跳过。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._idempotent = IdempotentConsume()

    def claim_message(self, bb, fact_id: int, consumer: str) -> dict:
        """原子 claim：检查未消费 → consume。返回 claim 结果。"""
        with self._lock:
            # 乐观检查
            if self._idempotent.is_consumed(bb, fact_id, consumer):
                return {"status": "claimed", "fact_id": fact_id, "consumer": consumer, "already": True}

            # 执行 consume
            result = self._idempotent.safe_consume(bb, fact_id, consumer)
            result["already"] = False
            return result

    def claim_batch(self, bb, messages: list[dict], consumer: str) -> list[dict]:
        """批量 claim：对每条消息原子 claim，返回所有结果。"""
        results = []
        for msg in messages:
            result = self.claim_message(bb, msg["id"], consumer)
            results.append(result)
        return results


# 全局实例
IDEMPOTENT_CONSUME = IdempotentConsume()
OPTIMISTIC_CLAIM = OptimisticClaim()


# ── ACK 确认机制 ────────────────────────────────────────────────────

class AckTracker:
    """消费回执/ACK 确认机制，SQLite 持久化。

    每次 consume 操作记录 ACK（fact_id, consumer, status, timestamp），
    支持：
    - trace: 追踪所有消费记录（谁在什么时候消费了什么）
    - retry: 失败 ACK 可在下次 route 时重试
    - stats: ACK 状态统计
    """

    _DEFAULT_DB = Path.home() / ".hermes" / "state" / "ack_tracker.db"

    def __init__(self, db_path: str | None = None):
        self._db_path = Path(db_path) if db_path else self._DEFAULT_DB
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS acks("
            "fact_id INT, consumer TEXT, category TEXT, "
            "status TEXT, error TEXT, ts REAL)"
        )
        conn.close()

    def _conn(self):
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def record_ack(self, fact_id: int, consumer: str, status: str,
                   category: str = "", error: str = "", ts: float = 0.0) -> dict:
        """记录一条消费 ACK。返回 ACK 记录。"""
        ts_val = ts or time.time()
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "INSERT INTO acks(fact_id, consumer, category, status, error, ts) "
                    "VALUES(?,?,?,?,?,?)",
                    (fact_id, consumer, category, status, error, ts_val),
                )
                conn.commit()
            finally:
                conn.close()
        return {
            "fact_id": fact_id, "consumer": consumer, "category": category,
            "status": status, "error": error, "ts": ts_val,
        }

    def get_acks(self, fact_id: int = 0, consumer: str = "",
                 min_ts: float = 0.0, limit: int = 50) -> list[dict]:
        """查询 ACK 记录。"""
        conditions = []
        params: list = []
        if fact_id:
            conditions.append("fact_id = ?")
            params.append(fact_id)
        if consumer:
            conditions.append("consumer = ?")
            params.append(consumer)
        if min_ts > 0:
            conditions.append("ts >= ?")
            params.append(min_ts)

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT fact_id, consumer, category, status, error, ts FROM acks{where} ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(sql, params).fetchall()
            finally:
                conn.close()
        return [
            {"fact_id": r[0], "consumer": r[1], "category": r[2],
             "status": r[3], "error": r[4], "ts": r[5]}
            for r in rows
        ]

    def get_failed_acks(self, since: float = 0.0) -> list[dict]:
        """获取最近失败的 ACK（可重试）。"""
        return self.get_acks(min_ts=since)[:20]

    def ack_stats(self) -> dict:
        """ACK 状态统计。"""
        with self._lock:
            conn = self._conn()
            try:
                total = conn.execute("SELECT COUNT(*) FROM acks").fetchone()[0]
                rows = conn.execute(
                    "SELECT status, COUNT(*) FROM acks GROUP BY status"
                ).fetchall()
            finally:
                conn.close()
        return {"total": total, "by_status": {r[0]: r[1] for r in rows}}

    def retry_failed(self, bb) -> int:
        """重试所有失败的 ACK（status=error）。

        返回成功重试的数量。
        """
        from bus_protocol import Blackboard
        failed = self.get_failed_acks()
        retried = 0
        for ack in failed:
            try:
                bb.mark_consumed(ack["fact_id"], ack["consumer"])
                with self._lock:
                    conn = self._conn()
                    try:
                        conn.execute(
                            "UPDATE acks SET status='retried', error='' "
                            "WHERE fact_id=? AND consumer=? AND status='error'",
                            (ack["fact_id"], ack["consumer"]),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                retried += 1
            except Exception:
                LOGGER.exception(f"Retry failed for ack #{ack.get('fact_id')}")
        return retried


ACK_TRACKER = AckTracker()

# 模块导入时从 config 初始化全局实例
reconfigure()
_CONFIG_MTIME = _config_mtime()


if __name__ == "__main__":
    import sys
    reconfigure()
    print("Reliability module self-check:")
    print(f"  CircuitBreaker: {CIRCUIT_BREAKER._state}")
    print(f"  Heartbeat stale threshold: {HEARTBEAT.stale_threshold}s")
    print(f"  TTL Pruner: interval={TTL_PRUNER.interval}s, max_age={TTL_PRUNER.max_age_days}d")
    print(f"  Metrics: {len(METRICS._counters)} counters, {len(METRICS._histograms)} histograms")
    print(f"  Health check: {health_check()}")
    print("OK")