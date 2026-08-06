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

from paths import ensure_paths
ensure_paths()

# 持久化 cursor 存储（Fix 6：防重启重复处理）
_CURSOR_DB = Path(os.environ.get(
    "SESSION_PIPELINE_STATE_DIR",
    str(Path.home() / ".hermes" / "state")
)) / "pipeline_cursor.db"

# 模块级持久连接（get/set_last_cursor 高频调用，避免每次 connect+close）
# check_same_thread=False 供多线程轮询共享；锁防并发写
_CURSOR_LOCK = threading.Lock()
_CURSOR_CONN = None

def _get_cursor_conn() -> sqlite3.Connection:
    """惰性创建 cursor DB 持久连接（模块级，避免高频 connect+close）。"""
    global _CURSOR_CONN
    if _CURSOR_CONN is None:
        _CURSOR_DB.parent.mkdir(parents=True, exist_ok=True)
        _CURSOR_CONN = sqlite3.connect(str(_CURSOR_DB), check_same_thread=False)
        _CURSOR_CONN.execute("PRAGMA journal_mode=WAL")
    return _CURSOR_CONN

def get_last_cursor(consumer: str, category: str = "", instance_id: str = "") -> int:
    """获取消费者在分类下的最后处理 fact_id。"""
    _ensure_cursor_db()
    with _CURSOR_LOCK:
        row = _get_cursor_conn().execute(
            "SELECT last_fact_id FROM cursors WHERE consumer=? AND category=? AND instance_id=?",
            (consumer, category, instance_id)
        ).fetchone()
        return row[0] if row else 0

def set_last_cursor(consumer: str, category: str, fact_id: int, instance_id: str = "") -> None:
    """更新消费者在分类下的最后处理 fact_id。"""
    _ensure_cursor_db()
    with _CURSOR_LOCK:
        conn = _get_cursor_conn()
        conn.execute(
            "INSERT OR REPLACE INTO cursors(consumer, category, instance_id, last_fact_id, updated_at) "
            "VALUES(?,?,?,?,?)",
            (consumer, category, instance_id, fact_id, time.time())
        )
        conn.commit()

def init_cursor_db() -> None:
    """初始化 cursor 表。"""
    conn = sqlite3.connect(str(_cursor_db_path()))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cursors("
            "consumer TEXT, category TEXT, instance_id TEXT DEFAULT '', "
            "last_fact_id INT, updated_at REAL, "
            "PRIMARY KEY(consumer, category, instance_id))"
        )
        conn.commit()
        # 迁移：旧 schema 缺少 instance_id 列时补充
        cols = [r[1] for r in conn.execute("PRAGMA table_info(cursors)").fetchall()]
        if "instance_id" not in cols:
            conn.execute("ALTER TABLE cursors ADD COLUMN instance_id TEXT DEFAULT ''")
            conn.execute("CREATE TABLE cursors_new("
                         "consumer TEXT, category TEXT, instance_id TEXT DEFAULT '', "
                         "last_fact_id INT, updated_at REAL, "
                         "PRIMARY KEY(consumer, category, instance_id))")
            conn.execute("INSERT INTO cursors_new SELECT consumer, category, '', last_fact_id, updated_at FROM cursors")
            conn.execute("DROP TABLE cursors")
            conn.execute("ALTER TABLE cursors_new RENAME TO cursors")
        conn.commit()
    finally:
        conn.close()

# 初始化 cursor DB（延迟到首次使用，避免 import 时副作用）
_CURSOR_INITIALIZED = False

def _ensure_cursor_db():
    """惰性初始化 cursor DB，仅在首次调用时建表。"""
    global _CURSOR_INITIALIZED
    if _CURSOR_INITIALIZED:
        return
    init_cursor_db()
    _CURSOR_INITIALIZED = True

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
    # 不清除已有 handler（避免覆盖其他模块的日志配置），仅检查是否已有 StreamHandler
    has_stream = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
    if has_stream:
        return logging.getLogger("reliability")
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
    def __init__(self, max_age_days: int = 7, max_facts: int = 5000, interval: float = 900.0):
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
                # 提取不含标签的基础指标名
                _base_key = key.split("{")[0] if "{" in key else key
                lines.append(f"# TYPE {_base_key} counter")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                _base_key = key.split("{")[0] if "{" in key else key
                lines.append(f"# TYPE {_base_key} histogram")
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
    "RetryPolicy", "with_retry", "CircuitState", "CircuitBreaker",
    "CircuitOpenError", "ConsumerHeartbeat", "TtlPruner",
    "MetricsCollector",
    "setup_logging", "JsonFormatter", "with_trace_id", "LOGGER",
    "init_cursor_db", "get_last_cursor", "set_last_cursor",
]
