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
import signal
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

# 将 hermes scripts 加入路径
_HERMES_SCRIPTS = Path.home() / ".hermes" / "scripts"
if str(_HERMES_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_HERMES_SCRIPTS))

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
    retry_exceptions: tuple = (Exception,)

    def delay(self, attempt: int) -> float:
        """计算第 attempt 次重试的延迟（0-indexed）。"""
        d = self.base_delay * (self.exponential_base ** attempt)
        return min(d, self.max_delay)


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
    """熔断器：连续失败 N 次 → OPEN（拒绝调用 M 秒）→ HALF_OPEN（放行 1 次）→ 恢复/再次 OPEN。"""
    failure_threshold: int = 5
    recovery_timeout: float = 30.0  # 秒
    half_open_max_calls: int = 1

    _state: str = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)
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
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                LOGGER.info("Circuit breaker: CLOSED (recovered)", extra=with_trace_id())
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._state == CircuitState.HALF_OPEN:
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


class CircuitOpenError(Exception):
    """熔断器开启时抛出。"""
    pass


# ── 消费者心跳 ──────────────────────────────────────────────────────

@dataclass
class ConsumerHeartbeat:
    """消费者心跳管理：记录每个消费者最后活跃时间。"""
    stale_threshold: float = 300.0  # 5 分钟

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


# 全局实例
METRICS = MetricsCollector()
CIRCUIT_BREAKER = CircuitBreaker()
HEARTBEAT = ConsumerHeartbeat()
TTL_PRUNER = TtlPruner()

# 默认重试策略
DEFAULT_RETRY = RetryPolicy(max_retries=3, base_delay=0.5, max_delay=10.0)


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


GRACEFUL_SHUTDOWN = GracefulShutdown()


# ── 健康检查 ────────────────────────────────────────────────────────

def health_check() -> dict:
    """健康检查端点返回字典。"""
    from bus_protocol import Blackboard
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

    return {
        "status": "healthy" if bus_ok and not stale_consumers else "degraded",
        "bus": {
            "connected": bus_ok,
            "error": bus_error,
            "total_facts": stats.get("total", 0),
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


if __name__ == "__main__":
    # 自检
    import sys
    print("Reliability module self-check:")
    print(f"  CircuitBreaker: {CIRCUIT_BREAKER._state}")
    print(f"  Heartbeat stale threshold: {HEARTBEAT.stale_threshold}s")
    print(f"  TTL Pruner: interval={TTL_PRUNER.interval}s, max_age={TTL_PRUNER.max_age_days}d")
    print(f"  Metrics: {len(METRICS._counters)} counters, {len(METRICS._histograms)} histograms")
    print(f"  Health check: {health_check()}")
    print("OK")