"""route_all 必须把「已接管」记到 poll 的 consumer 视图上。

生产证据（2026-10-22 04:20 实测 bus 库）：
- `unconsumed(consumer='pipeline')` 返回 **10 003 条 = facts 全表**，其中 7 777 条 >1 天；
- 原因是各角色消费写 `log.cat=<role>`，poll 只认 `log.cat='pipeline'`；
- 后果：routing_daemon 每轮把最新 100 条重投一遍，同一 preview 每 5~6 分钟重复 3 次
  （04:14/04:19/04:25 实测），角色上下文被同一批内容反复污染。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_route_claim.py -q
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _msg(cat):
    return {"id": 4242, "category": cat, "text": f"[{cat}] x\n证据 y",
            "src": "claude", "evidence": "", "priority": 1,
            "consumers": ["engineer"]}


def _run(cat="performance", ack="consumed"):
    """cat=performance 走 delegated（本地不消费）；cat=code_review 走逐角色消费。"""
    from routing.routes import route_all
    bb = MagicMock()
    _rt = MagicMock()
    _rt.get_router.return_value.routing = {"engineer": {"consume": [cat]}}
    with patch("routing.auto.poll_unconsumed", return_value=[_msg(cat)]), \
            patch("routing.routes._rt_mod", _rt), \
            patch("bus_protocol.Blackboard", return_value=bb), \
            patch("routing.routes.HEARTBEAT"), \
            patch("routing.routes.METRICS"):
        import routing.routes as routes
        with patch.object(routes.IDEMPOTENT_CONSUME, "safe_consume",
                          return_value={"status": ack}):
            route_all(consumer="pipeline", dry_run=False)
    return bb


def test_delegated_fact_is_claimed():
    """交给 ccs-route 送达的类也要记接管：送达重试在 route_to_ccs 的
    consumer=<role> 视图里，与这笔记录互不影响。"""
    bb = _run("performance")
    assert any(c.args == (4242, "pipeline") for c in bb.mark_consumed.call_args_list), \
        bb.mark_consumed.call_args_list


def test_consumed_fact_is_claimed():
    bb = _run("code_review", ack="consumed")
    assert any(c.args == (4242, "pipeline") for c in bb.mark_consumed.call_args_list), \
        bb.mark_consumed.call_args_list


def test_errored_fact_is_left_for_retry():
    """有角色报错 → 不记接管，下一轮还要重试（否则该条静默丢失）。"""
    bb = _run("code_review", ack="error")
    assert not any(c.args[0] == 4242 for c in bb.mark_consumed.call_args_list), \
        bb.mark_consumed.call_args_list


def test_dry_run_never_writes_claim():
    """--dry-run 是操作员安全开关：只展示方案，不能写 bus（含 no_consumer / stale 两条新分支）。"""
    import time as _t
    from routing.routes import route_all, STALE_AFTER_S
    bb = MagicMock()
    _rt = MagicMock()
    _rt.get_router.return_value.routing = {"engineer": {"consume": ["code_fix"]}}
    msgs = [
        {"id": 6161, "category": "code_fix", "text": "[code_fix] @dkk 无主\n证据",
         "src": "claude", "evidence": "", "priority": 1, "consumers": ["engineer"]},
        {"id": 6162, "category": "code_fix", "text": "[code_fix] 过期\n证据",
         "src": "claude", "evidence": "", "priority": 1, "consumers": ["engineer"],
         "ts": _t.time() - STALE_AFTER_S - 60},
    ]
    with patch("routing.auto.poll_unconsumed", return_value=msgs), \
            patch("routing.routes._rt_mod", _rt), \
            patch("bus_protocol.Blackboard", return_value=bb), \
            patch("routing.routes.HEARTBEAT"), \
            patch("routing.routes.METRICS"):
        route_all(consumer="pipeline", dry_run=True)
    assert not bb.mark_consumed.called, bb.mark_consumed.call_args_list


def test_stale_fact_is_claimed_but_not_delivered():
    """超过 STALE_AFTER_S 的事实只 claim 不投递。

    视图里积压 8.6k 条历史事实（7.7k >24h）。无主事实被 claim 后窗口向老事实滑动，
    不设闸就会把一天前的任务重新派给角色、并 mint 同标题旧工作流。
    """
    import time as _t
    from routing.routes import route_all, STALE_AFTER_S
    bb = MagicMock()
    _rt = MagicMock()
    _rt.get_router.return_value.routing = {"engineer": {"consume": ["code_fix"]}}
    msg = {"id": 5151, "category": "code_fix", "text": "[code_fix] 一天前的修复\n证据",
           "src": "claude", "evidence": "", "priority": 1, "consumers": ["engineer"],
           "ts": _t.time() - STALE_AFTER_S - 60}
    with patch("routing.auto.poll_unconsumed", return_value=[msg]), \
            patch("routing.routes._rt_mod", _rt), \
            patch("bus_protocol.Blackboard", return_value=bb), \
            patch("routing.routes.HEARTBEAT"), \
            patch("routing.routes.METRICS"):
        res = route_all(consumer="pipeline", dry_run=False)
    assert res["routed"] == 0, res
    assert res["details"][0]["reason"] == "stale", res["details"][0]
    assert any(c.args == (5151, "pipeline") for c in bb.mark_consumed.call_args_list), \
        bb.mark_consumed.call_args_list


def test_orphan_fact_is_claimed_so_view_can_drain():
    """@tag 过滤后没人可派 → 也要记接管。

    不记则它永远留在 pipeline 视图里被每 60s 重扫：24h 实测 416/1813=22.9%
    的无主事实，领取年龄中位 136min（≈136 轮），视图只能靠 prune 删最旧收场。
    """
    from routing.routes import route_all
    bb = MagicMock()
    _rt = MagicMock()
    _rt.get_router.return_value.routing = {"engineer": {"consume": ["code_review"]}}
    msg = {"id": 4343, "category": "code_review",
           "text": "[code_review] @dkk 只给总线看\n证据 y", "src": "claude",
           "evidence": "", "priority": 1, "consumers": ["engineer"]}
    with patch("routing.auto.poll_unconsumed", return_value=[msg]), \
            patch("routing.routes._rt_mod", _rt), \
            patch("bus_protocol.Blackboard", return_value=bb), \
            patch("routing.routes.HEARTBEAT"), \
            patch("routing.routes.METRICS"):
        route_all(consumer="pipeline", dry_run=False)
    assert any(c.args == (4343, "pipeline") for c in bb.mark_consumed.call_args_list), \
        bb.mark_consumed.call_args_list
