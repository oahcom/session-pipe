"""单元测试：routing/polling.py — 轮询与通知逻辑。

直接测试 polling.py 的两个公共函数，mock 外部依赖（Blackboard、config、reliability）。
"""
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

_src_dir = str(Path(__file__).resolve().parents[1] / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from routing.router import priority as _real_priority


def _make_fact(fid: int, cat: str, text: str, evidence: str = ""):
    return SimpleNamespace(id=fid, cat=cat, t=text, e=evidence)


@contextmanager
def _poll_ctx(facts, *, config_getter=None, cursor=0,
              circuit_side_effect=None):
    """一键 mock polling.py 所有外部依赖。

    Yields dict with bb, router, metrics, heartbeat mocks 供断言。
    """
    cb_mock = mock.MagicMock()
    if circuit_side_effect is not None:
        cb_mock.call.side_effect = circuit_side_effect
    else:
        cb_mock.call.side_effect = lambda fn: fn()

    bb_mock = mock.MagicMock()
    bb_mock.unconsumed.return_value = facts

    config_mock = mock.MagicMock()
    if config_getter is None:
        config_mock.nested_get.return_value = 100
    else:
        config_mock.nested_get = config_getter

    router_mock = mock.MagicMock()
    router_mock.get_consumers_prioritized.return_value = ["engineer"]

    rt_mock = mock.MagicMock()
    rt_mock.get_router.return_value = router_mock
    rt_mock.priority = _real_priority

    metrics_mock = mock.MagicMock()
    hb_mock = mock.MagicMock()
    logger_mock = mock.MagicMock()

    with mock.patch("routing.polling._rt_mod", rt_mock), \
         mock.patch("routing.polling.get_last_cursor", return_value=cursor), \
         mock.patch("routing.polling.CIRCUIT_BREAKER", cb_mock), \
         mock.patch("routing.polling.METRICS", metrics_mock), \
         mock.patch("routing.polling.HEARTBEAT", hb_mock), \
         mock.patch("routing.polling.LOGGER", logger_mock), \
         mock.patch("bus_protocol.Blackboard", return_value=bb_mock), \
         mock.patch("config_loader.get_config", return_value=config_mock):
        yield {
            "bb": bb_mock,
            "router": router_mock,
            "metrics": metrics_mock,
            "heartbeat": hb_mock,
        }


# ═══════════════════════════════════════════════════════════════
# poll_unconsumed
# ═══════════════════════════════════════════════════════════════

class TestPollUnconsumed:

    def test_正常返回_按优先级排序(self):
        facts = [
            _make_fact(10, "architecture", "arch"),
            _make_fact(9,  "code_fix",      "fix"),
            _make_fact(11, "security",      "sec"),
        ]
        with _poll_ctx(facts):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed(limit=10)

        assert len(msgs) == 3
        priorities = [m["priority"] for m in msgs]
        assert priorities == sorted(priorities)

    def test_category过滤(self):
        facts = [
            _make_fact(1, "code_fix",      "fix"),
            _make_fact(2, "architecture",  "arch"),
            _make_fact(3, "code_fix",      "fix2"),
        ]
        with _poll_ctx(facts):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed(category="code_fix")

        assert all(m["category"] == "code_fix" for m in msgs)
        assert len(msgs) == 2

    def test_limit裁剪(self):
        facts = [_make_fact(i, "code_fix", f"m{i}") for i in range(20)]
        with _poll_ctx(facts):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed(limit=5)

        assert len(msgs) == 5

    def test_max_messages_per_poll生效(self):
        facts = [_make_fact(i, "code_fix", f"m{i}") for i in range(30)]
        cfg = mock.MagicMock()
        cfg.nested_get.return_value = 3
        with _poll_ctx(facts, config_getter=cfg.nested_get):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed(limit=100)

        assert len(msgs) == 3

    def test_config异常_fallback(self):
        facts = [_make_fact(1, "code_fix", "msg")]
        with _poll_ctx(facts, config_getter=mock.MagicMock(side_effect=RuntimeError("broken"))):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed()

        assert len(msgs) == 1
        assert msgs[0]["id"] == 1

    def test_熔断器异常_返回error(self):
        with _poll_ctx([], circuit_side_effect=RuntimeError("circuit open")) as deps:
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed()

        assert len(msgs) == 1
        assert "circuit open" in msgs[0]["error"]
        deps["metrics"].inc.assert_called_with("poll_errors_total")

    def test_熔断器异常_记录trace_id(self):
        with _poll_ctx([], circuit_side_effect=ValueError("bad")) as deps:
            from routing.polling import poll_unconsumed
            poll_unconsumed()

        deps["metrics"].inc.assert_any_call("poll_errors_total")

    def test_空bus(self):
        with _poll_ctx([]):
            from routing.polling import poll_unconsumed
            assert poll_unconsumed() == []

    def test_consumer光标跳过已处理(self):
        facts = [
            _make_fact(1, "code_fix", "old"),
            _make_fact(2, "code_fix", "old2"),
            _make_fact(3, "code_fix", "new"),
        ]
        with _poll_ctx(facts, cursor=2):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed(consumer="engineer")

        assert len(msgs) == 1
        assert msgs[0]["id"] == 3

    def test_无consumer不使用光标(self):
        cursor_spy = mock.MagicMock(return_value=0)
        with mock.patch("routing.polling._rt_mod"), \
             mock.patch("routing.polling.get_last_cursor", cursor_spy), \
             mock.patch("routing.polling.CIRCUIT_BREAKER"), \
             mock.patch("routing.polling.METRICS"), \
             mock.patch("routing.polling.HEARTBEAT"), \
             mock.patch("routing.polling.LOGGER"), \
             mock.patch("bus_protocol.Blackboard"), \
             mock.patch("config_loader.get_config"):
            from routing.polling import poll_unconsumed
            poll_unconsumed()

        cursor_spy.assert_not_called()

    def test_心跳beat被调用(self):
        with _poll_ctx([]) as deps:
            from routing.polling import poll_unconsumed
            poll_unconsumed()

        deps["heartbeat"].beat.assert_called_once_with("pipeline")

    def test_指标poll_count(self):
        with _poll_ctx([_make_fact(1, "code_fix", "m")]) as deps:
            from routing.polling import poll_unconsumed
            poll_unconsumed()

        deps["metrics"].inc.assert_any_call("poll_count")

    def test_指标backlog_size(self):
        with _poll_ctx([_make_fact(1, "code_fix", "m")]) as deps:
            from routing.polling import poll_unconsumed
            poll_unconsumed()

        deps["metrics"].observe.assert_any_call("backlog_size", 1)

    def test_消息字段截断(self):
        facts = [_make_fact(1, "code_fix", "x" * 200, "y" * 200)]
        with _poll_ctx(facts):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed()

        assert len(msgs[0]["text"]) == 100
        assert len(msgs[0]["evidence"]) == 120

    def test_每条消息含consumers(self):
        with _poll_ctx([_make_fact(1, "code_fix", "fix")]):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed()

        assert msgs[0]["consumers"] == ["engineer"]

    def test_每条消息含priority(self):
        with _poll_ctx([_make_fact(1, "code_fix", "fix")]):
            from routing.polling import poll_unconsumed
            msgs = poll_unconsumed()

        assert msgs[0]["priority"] == _real_priority("code_fix")


# ═══════════════════════════════════════════════════════════════
# notify_consumers
# ═══════════════════════════════════════════════════════════════

class TestNotifyConsumers:

    def test_按消费者分组打印(self, capsys):
        msgs = [
            {"id": 1, "category": "code_fix", "text": "f1", "priority": 2,
             "consumers": ["engineer", "closer"]},
            {"id": 2, "category": "architecture", "text": "a1", "priority": 3,
             "consumers": ["engineer"]},
        ]
        from routing.polling import notify_consumers
        notify_consumers(msgs)

        out = capsys.readouterr().out
        assert "engineer: 2 条待消费" in out
        assert "closer: 1 条待消费" in out

    def test_空列表(self, capsys):
        from routing.polling import notify_consumers
        notify_consumers([])
        assert capsys.readouterr().out == ""

    def test_无consumers字段不报错(self, capsys):
        from routing.polling import notify_consumers
        notify_consumers([{"id": 1, "category": "code_fix", "text": "x", "priority": 2}])
        assert capsys.readouterr().out == ""

    def test_有evidence打印(self, capsys):
        from routing.polling import notify_consumers
        notify_consumers([{"id": 1, "category": "security", "text": "v", "priority": 1,
                          "consumers": ["engineer"], "evidence": "CVE-2024-1234"}])
        assert "CVE-2024-1234" in capsys.readouterr().out

    def test_无evidence无箭头(self, capsys):
        from routing.polling import notify_consumers
        notify_consumers([{"id": 1, "category": "code_fix", "text": "f", "priority": 2,
                          "consumers": ["engineer"], "evidence": ""}])
        assert "→" not in capsys.readouterr().out

    def test_同消费者按优先级排序(self, capsys):
        msgs = [
            {"id": 3, "category": "security",     "text": "s", "priority": 1,
             "consumers": ["engineer"]},
            {"id": 2, "category": "code_fix",     "text": "f", "priority": 2,
             "consumers": ["engineer"]},
            {"id": 1, "category": "architecture", "text": "a", "priority": 5,
             "consumers": ["engineer"]},
        ]
        from routing.polling import notify_consumers
        notify_consumers(msgs)

        prio = [l.strip() for l in capsys.readouterr().out.split("\n") if "[P" in l]
        assert prio[0].startswith("[P1]")
        assert prio[1].startswith("[P2]")
        assert prio[2].startswith("[P5]")

    def test_消费者按字母序(self, capsys):
        from routing.polling import notify_consumers
        notify_consumers([{"id": 1, "category": "code_fix", "text": "a",
                          "priority": 2, "consumers": ["zebra", "alpha"]}])

        out = capsys.readouterr().out
        assert out.find("alpha:") < out.find("zebra:")

    def test_多消费者多消息交叉(self, capsys):
        msgs = [
            {"id": 1, "category": "security", "text": "s", "priority": 1,
             "consumers": ["engineer", "closer"]},
            {"id": 2, "category": "code_fix", "text": "f", "priority": 2,
             "consumers": ["closer"]},
        ]
        from routing.polling import notify_consumers
        notify_consumers(msgs)

        out = capsys.readouterr().out
        assert "engineer: 1 条待消费" in out
        assert "closer: 2 条待消费" in out


# ── 模块导入 ──

def test_模块可导入():
    from routing import polling
    assert hasattr(polling, "poll_unconsumed")
    assert hasattr(polling, "notify_consumers")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-q"])
