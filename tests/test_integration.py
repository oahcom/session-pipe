#!/usr/bin/env python3
"""
集成测试：真实 bus 读写 + 路由 + 消费联动全链路。

运行：cd session-pipeline && python3 tests/test_integration.py
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

# 确保路径
_src_dir = str(Path(__file__).resolve().parents[1] / "src")
_hermes_scripts = str(Path.home() / ".hermes" / "scripts")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
if _hermes_scripts not in sys.path:
    sys.path.insert(0, _hermes_scripts)


def test_bus_write_and_unconsumed():
    """bus 写入 → unconsumed 能读到。"""
    from bus_protocol import Blackboard
    bb = Blackboard()
    import uuid
    title = f"测试写入后能读到 {uuid.uuid4().hex[:8]}"
    fid = bb.write("code_fix", title, evidence="集成测试", src="test")
    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    assert len(found) == 1, f"写入后应能读到，实际未找到 #{fid}"
    print("  ✓ bus write → unconsumed 读取")


def test_bus_mark_consumed():
    """标记消费后 unconsumed 不再返回。"""
    from bus_protocol import Blackboard
    bb = Blackboard()
    import uuid
    title = f"测试消费后消失 {uuid.uuid4().hex[:8]}"
    fid = bb.write("code_fix", title, evidence="集成测试", src="test")
    bb.mark_consumed(fid, "test_consumer")
    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    assert len(found) == 0, f"消费后不应读到 #{fid}"
    print("  ✓ bus mark_consumed → unconsumed 消失")


def test_route_all_e2e():
    """route-all 端到端：写入消息 → route_all 消费 → 状态 idle。"""
    from bus_protocol import Blackboard
    from auto_route import route_all, status

    bb = Blackboard()
    import uuid
    title = f"测试 route-all 端到端 {uuid.uuid4().hex[:8]}"
    fid = bb.write("performance", title, evidence="集成测试", src="test")

    result = route_all(consumer="test_integration")
    assert result["routed"] >= 1, f"route_all 应至少消费 1 条，实际 {result['routed']}"
    assert result["total"] >= 1
    # 验证该消息已被消费
    unconsumed = bb.unconsumed()
    found = [f for f in unconsumed if f.id == fid]
    assert len(found) == 0, f"route_all 后 #{fid} 应已消费"
    print("  ✓ route_all e2e 消费成功")


def test_priority_routing():
    """优先级路由：security 消息应被 security 消费者先消费。"""
    from bus_protocol import Blackboard
    from auto_route import route_all
    from router import priority

    bb = Blackboard()
    import uuid
    # 写入不同优先级消息
    fid_sec = bb.write("security", f"安全告警测试 {uuid.uuid4().hex[:8]}", evidence="优先级测试", src="test")
    fid_perf = bb.write("performance", f"性能发现测试 {uuid.uuid4().hex[:8]}", evidence="优先级测试", src="test")

    result = route_all(consumer="test_priority")
    # 安全消息应有更高优先级
    sec_details = [d for d in result["details"] if d["id"] == fid_sec]
    perf_details = [d for d in result["details"] if d["id"] == fid_perf]
    if sec_details and perf_details:
        assert sec_details[0]["priority"] < perf_details[0]["priority"], \
            f"security 优先级应更低(更高优先级)，实际 sec={sec_details[0]['priority']} perf={perf_details[0]['priority']}"
    print("  ✓ 优先级路由正确")


def test_reliability_retry_policy():
    """重试策略：指数退避延迟正确。"""
    from reliability import RetryPolicy
    p = RetryPolicy(base_delay=1.0, exponential_base=2.0, max_delay=10.0)
    assert p.delay(0) == 1.0
    assert p.delay(1) == 2.0
    assert p.delay(2) == 4.0
    assert p.delay(10) == 10.0  # 上限
    print("  ✓ RetryPolicy 指数退避")


def test_reliability_circuit_breaker():
    """熔断器：连续失败 → OPEN → 恢复。"""
    from reliability import CircuitBreaker, CircuitState, CircuitOpenError

    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
    call_count = 0

    def fail():
        raise ValueError("test error")

    def succeed():
        return "ok"

    # 触发熔断
    for _ in range(2):
        try:
            cb.call(fail)
        except ValueError:
            pass

    assert cb._state == CircuitState.OPEN, "应进入 OPEN 状态"

    # OPEN 期间拒绝调用
    try:
        cb.call(succeed)
        assert False, "OPEN 期间应抛出 CircuitOpenError"
    except CircuitOpenError:
        pass

    # 等待恢复
    time.sleep(0.15)
    result = cb.call(succeed)
    assert result == "ok"
    assert cb._state == CircuitState.CLOSED, "恢复后应 CLOSED"
    print("  ✓ CircuitBreaker 熔断/恢复")


def test_reliability_heartbeat():
    """心跳管理：beat → is_stale。"""
    from reliability import ConsumerHeartbeat
    hb = ConsumerHeartbeat(stale_threshold=0.05)
    hb.beat("agent_a")
    assert not hb.is_stale("agent_a"), "刚 beat 不应 stale"
    time.sleep(0.06)
    assert hb.is_stale("agent_a"), "超时后应 stale"
    print("  ✓ ConsumerHeartbeat 心跳管理")


def test_reliability_metrics():
    """Metrics 计数和直方图。"""
    from reliability import MetricsCollector
    m = MetricsCollector()
    m.inc("test_counter", labels={"a": "b"})
    m.inc("test_counter", labels={"a": "b"})
    m.observe("test_latency", 0.01)
    m.observe("test_latency", 0.05)
    export = m.export_prometheus()
    assert 'test_counter{a="b"} 2.0' in export
    assert "test_latency_count 2" in export
    print("  ✓ Metrics 收集/导出")


def test_reliability_health_check():
    """健康检查返回正确格式。"""
    from reliability import health_check
    hc = health_check()
    assert "status" in hc, "应含 status"
    assert hc["status"] in ("healthy", "degraded"), f"status 应为 healthy/degraded，实际 {hc['status']}"
    assert "bus" in hc
    assert "consumers" in hc
    assert "circuit_breaker" in hc
    print("  ✓ health_check 格式")


def test_reliability_ttl_pruner():
    """TTL pruner 能执行。"""
    from reliability import TtlPruner
    pruner = TtlPruner(max_age_days=1, max_facts=100000)
    deleted = pruner.prune_once()
    assert isinstance(deleted, int), f"返回值应是 int，实际 {type(deleted)}"
    print("  ✓ TtlPruner prune_once")


if __name__ == "__main__":
    print("=== 集成测试: bus + routing + reliability ===\n")

    tests = [
        test_bus_write_and_unconsumed,
        test_bus_mark_consumed,
        test_route_all_e2e,
        test_priority_routing,
        test_reliability_retry_policy,
        test_reliability_circuit_breaker,
        test_reliability_heartbeat,
        test_reliability_metrics,
        test_reliability_health_check,
        test_reliability_ttl_pruner,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__}: {e}")
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
