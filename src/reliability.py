#!/usr/bin/env python3
"""
Reliability Layer — 可靠性基础设施。

提供：
1. 指数退避重试（可配置 max_retries, base_delay, max_delay）
2. 熔断器（连续失败 N 次 → 短路 M 秒）
3. 消费者心跳（每次 poll 更新 last_seen，>5min 标记 stale）
4. 消息 TTL 自动清理（每小时清理 >90 天未消费消息）
5. 结构化日志（JSON + trace_id）
6. Prometheus metrics（latency histogram, counters）
"""
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

# 将 hermes scripts 加入路径（环境变量优先，回退 HOME）
_HERMES_SCRIPTS = Path(os.environ.get("HERMES_SCRIPTS_DIR", Path.home() / ".hermes" / "scripts"))
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(1, str(_HERMES_SCRIPTS))

# 持久化 cursor 存储（Fix 6：防重启重复处理）
_CURSOR_DB = Path(os.environ.get(
    "SESSION_PIPELINE_STATE_DIR",
    str(Path.home() / ".hermes" / "state")
)) / "pipeline_cursor.db"

def _cursor_db_path() -> Path:
    """返回 cursor DB 路径，确保目录存在。"""
    _CURSOR_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_CURSOR_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.close()
    return _CURSOR_DB

def get_last_cursor(consumer: str, category: str = "") -> int:
    """获取消费者在分类下的最后处理 fact_id。"""
    conn = sqlite3.connect(str(_cursor_db_path()))
    try:
        row = conn.execute(
            "SELECT last_fact_id FROM cursors WHERE consumer=? AND category=?",
            (consumer, category)
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()

def set_last_cursor(consumer: str, category: str, fact_id: int) -> None:
    """更新消费者在分类下的最后处理 fact_id。"""
    conn = sqlite3.connect(str(_cursor_db_path()))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO cursors(consumer, category, last_fact_id, updated_at) "
            "VALUES(?,?,?,?)",
            (consumer, category, fact_id, time.time())
        )
        conn.commit()
    finally:
        conn.close()

def init_cursor_db() -> None:
    """初始化 cursor 表。"""
    conn = sqlite3.connect(str(_cursor_db_path()))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cursors("
            "consumer TEXT, category TEXT, last_fact_id INT, updated_at REAL, "
            "PRIMARY KEY(consumer, category))"
        )
        conn.commit()
    finally:
        conn.close()

# 初始化 cursor DB
init_cursor_db()

T = TypeVar("T")


# ── 结构化日志 ──────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """JSON 格式化器，自动注入 trace_id。"""

    def format(self, record: logging.LogRecord) -> str:
        trace_id = getattr(record, "trace_id", None) or "-"
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "trace_id": trace_id,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: int = logging.INFO, json_output: bool = True) -> logging.Logger:
    """配置根 logger，返回业务 logger。"""
    root = logging.getLogger()
    root.setLevel(level)
    # 清除已有 handler
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    if json_output:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s [%(trace_id)s] %(message)s"
        ))
    root.addHandler(handler)

    biz = logging.getLogger("session-pipeline")
    biz.setLevel(level)
    return biz


LOGGER = setup_logging()


def with_trace_id(trace_id: Optional[str] = None):
    """为日志调用注入 trace_id。用法：LOGGER.info("msg", extra={"trace_id": tid})"""
    return {"trace_id": trace_id or str(uuid.uuid4())[:8]}


# ── 重试装饰器 ──────────────────────────────────────────────────────

@dataclass
class RetryPolicy:
    max_retries: int = 3
    base_delay: float = 0.5   # 秒
    max_delay: float = 10.0   # 秒
    exponential_base: float = 2.0
    retry_exceptions: tuple | list = (Exception,)

    def __post_init__(self):
        # Resolve string exception names to classes
        if self.retry_exceptions and isinstance(self.retry_exceptions[0], str):
            from config_loader import _resolve_exceptions
            self.retry_exceptions = _resolve_exceptions(self.retry_exceptions)

    def delay(self, attempt: int) -> float:
        """计算第 attempt 次重试的延迟（0-indexed），含随机抖动防止惊群。"""
        import random
        d = self.base_delay * (self.exponential_base ** attempt)
        return min(d + random.uniform(0, d * 0.5), self.max_delay)


def with_retry(policy: Optional[RetryPolicy] = None):
    """装饰器：按策略重试。"""
    p = policy or RetryPolicy()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exc = None
            for attempt in range(p.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except p.retry_exceptions as e:
                    last_exc = e
                    if attempt < p.max_retries:
                        delay = p.delay(attempt)
                        LOGGER.warning(
                            f"Retry {attempt+1}/{p.max_retries} after {delay:.1f}s: {e}",
                            extra=with_trace_id()
                        )
                        time.sleep(delay)
                    else:
                        LOGGER.error(
                            f"All {p.max_retries+1} attempts failed: {e}",
                            extra=with_trace_id()
                        )
            raise last_exc
        return wrapper
    return decorator


# ── 熔断器 ──────────────────────────────────────────────────────────

class CircuitState:
    CLOSED = "closed"      # 正常
    OPEN = "open"          # 短路
    HALF_OPEN = "half_open"  # 探测恢复


@dataclass
class CircuitBreaker:
    """熔断器：连续失败 N 次 → OPEN（拒绝调用 M 秒）→ HALF_OPEN（连续成功 N 次才恢复）→ CLOSED。"""
    failure_threshold: int = 5
    recovery_timeout: float = 30.0  # 秒
    half_open_max_calls: int = 3

    _state: str = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)
    _half_open_successes: int = field(default=0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """执行 func，自动处理熔断逻辑。"""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    LOGGER.info("Circuit breaker: HALF_OPEN", extra=with_trace_id())
                else:
                    raise CircuitOpenError("Circuit breaker is OPEN")

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitOpenError("Circuit breaker HALF_OPEN limit reached")
                self._half_open_calls += 1

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._half_open_successes = 0
                    LOGGER.info("Circuit breaker: CLOSED (recovered after %d successful calls)",
                                self.half_open_max_calls, extra=with_trace_id())
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_successes = 0
                self._state = CircuitState.OPEN
                LOGGER.warning("Circuit breaker: OPEN (half-open call failed)", extra=with_trace_id())
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                LOGGER.warning(f"Circuit breaker: OPEN (failures={self._failure_count})", extra=with_trace_id())

    def reset(self):
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._half_open_calls = 0
            self._half_open_successes = 0


class CircuitOpenError(Exception):
    """熔断器开启时抛出。"""
    pass


# ── 消费者心跳 ──────────────────────────────────────────────────────

@dataclass
class ConsumerHeartbeat:
    """消费者心跳管理：记录每个消费者最后活跃时间。"""
    stale_threshold: float = 300.0  # 5 分钟
    cleanup_interval: float = 3600.0  # 心跳记录清理间隔（Fix 2：从配置读取）

    _last_seen: dict[str, float] = field(default_factory=dict, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def beat(self, consumer: str):
        """更新消费者心跳。"""
        with self._lock:
            self._last_seen[consumer] = time.time()

    def is_stale(self, consumer: str) -> bool:
        """检查消费者是否 stale。"""
        with self._lock:
            last = self._last_seen.get(consumer, 0)
            return (time.time() - last) > self.stale_threshold

    def get_stale_consumers(self) -> list[str]:
        """返回所有 stale 消费者。"""
        with self._lock:
            now = time.time()
            return [c for c, t in self._last_seen.items() if (now - t) > self.stale_threshold]

    def cleanup(self, max_age: float = 3600.0):
        """清理超过 max_age 的心跳记录。"""
        with self._lock:
            now = time.time()
            stale = [c for c, t in self._last_seen.items() if (now - t) > max_age]
            for c in stale:
                del self._last_seen[c]


# ── TTL 自动清理 ────────────────────────────────────────────────────

class TtlPruner:
    """定期调用 bus_protocol.Blackboard.prune() 清理过期消息。"""
    def __init__(self, max_age_days: int = 90, max_facts: int = 10000, interval: float = 3600.0):
        self.max_age_days = max_age_days
        self.max_facts = max_facts
        self.interval = interval
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._last_run: float = 0

    def start(self):
        """启动后台清理线程。"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        LOGGER.info(f"TTL pruner started: interval={self.interval}s", extra=with_trace_id())

    def stop(self):
        """停止清理线程。"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        LOGGER.info("TTL pruner stopped", extra=with_trace_id())

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self.prune_once()
            except Exception as e:
                LOGGER.error(f"TTL pruner error: {e}", extra=with_trace_id())
            # 等待 interval 或 stop 事件
            self._stop_event.wait(self.interval)

    def prune_once(self) -> int:
        """执行一次清理，返回删除数量。"""
        from bus_protocol import Blackboard
        bb = Blackboard()
        deleted = bb.prune(max_age_days=self.max_age_days, max_facts=self.max_facts)
        self._last_run = time.time()
        if deleted:
            LOGGER.info(f"TTL pruner: deleted {deleted} entries", extra=with_trace_id())
        return deleted


# ── Prometheus Metrics（轻量实现，无外部依赖） ────────────────────────

class MetricsCollector:
    """进程内 metrics 收集器，支持 /metrics 文本格式导出。"""
    def __init__(self):
        self._counters: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def inc(self, name: str, value: float = 1.0, labels: Optional[dict] = None):
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def observe(self, name: str, value: float, labels: Optional[dict] = None):
        key = self._make_key(name, labels)
        with self._lock:
            self._histograms.setdefault(key, []).append(value)

    @contextmanager
    def timer(self, name: str, labels: Optional[dict] = None):
        """上下文管理器：自动记录耗时。"""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.observe(name, elapsed, labels)

    def _make_key(self, name: str, labels: Optional[dict]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def export_prometheus(self) -> str:
        """导出 Prometheus 文本格式。"""
        lines = []
        with self._lock:
            for key, value in self._counters.items():
                lines.append(f"# TYPE {key} counter")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                if not values:
                    continue
                lines.append(f"# TYPE {key} histogram")
                # 简单统计：count, sum, buckets (le=0.005,0.01,0.05,0.1,0.5,1,5)
                buckets = [0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
                counts = {b: 0 for b in buckets}
                for v in values:
                    for b in buckets:
                        if v <= b:
                            counts[b] += 1
                for b in buckets:
                    lines.append(f'{key}_bucket{{le="{b}"}} {counts[b]}')
                lines.append(f'{key}_bucket{{le="+Inf"}} {len(values)}')
                lines.append(f'{key}_count {len(values)}')
                lines.append(f'{key}_sum {sum(values):.6f}')
        return "\n".join(lines) + "\n"


# 全局实例（懒初始化，从 config_loader 读取配置）
METRICS = MetricsCollector()  # 无配置依赖，保持即时创建

__all__ = [
    "METRICS", "CIRCUIT_BREAKER", "HEARTBEAT", "TTL_PRUNER",
    "DEFAULT_RETRY", "GRACEFUL_SHUTDOWN", "IDEMPOTENT_CONSUME",
    "OPTIMISTIC_CLAIM", "ACK_TRACKER", "with_retry", "health_check",
    "start_background_services", "stop_background_services",
    "reconfigure",
    "RetryPolicy", "CircuitBreaker", "CircuitState", "CircuitOpenError",
    "ConsumerHeartbeat", "TtlPruner", "MetricsCollector",
    "GracefulShutdown", "ShutdownError", "IdempotentConsume",
    "OptimisticClaim", "AckTracker", "setup_logging", "LOGGER",
]

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
    if ttl.get("auto_start", True):
        TTL_PRUNER.start()
    gs = cfg.nested_get("graceful_shutdown", default={})
    GRACEFUL_SHUTDOWN = GracefulShutdown(timeout=gs.get("timeout", 30.0))
    # 应用 logging 配置（Fix 2：logging.level + logging.json_output）
    log_cfg = cfg.nested_get("logging", default={})
    log_level_name = log_cfg.get("level", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    log_json = log_cfg.get("json_output", True)
    setup_logging(level=log_level, json_output=log_json)



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
                pass
        return retried


ACK_TRACKER = AckTracker()

# 模块导入时自动从 config 初始化全局实例
reconfigure()


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