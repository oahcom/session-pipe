"""源码热加载限速：多 session 并发改 src/** 时，execv 不得每个 tick 来一次。

实测 2026-10-22 03:46-04:26 的 40 分钟内 execv 8 次（约 5 分钟一次），
每次重执行丢掉当前 tick 并重跑启动。限速到 1 次/5 分钟。
"""
from pipeflow.daemon import _RELOAD_MIN_INTERVAL_SEC, _reload_allowed


def test_reload_throttled_within_interval():
    assert _RELOAD_MIN_INTERVAL_SEC == 300
    assert _reload_allowed(1000.0, 0.0) is True            # 从未加过速 → 允许
    assert _reload_allowed(1000.0, 900.0) is False         # 100s 前刚重执行 → 拒绝
    assert _reload_allowed(1000.0, 700.0) is True          # 300s → 允许
    assert _reload_allowed(1000.0, 1000.0) is False        # 同一时刻 → 拒绝
