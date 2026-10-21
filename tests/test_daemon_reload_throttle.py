
def test_throttled_update_stays_pending():
    """限速期看到的更新必须排队，冷却后才执行（codex P1 回归）。"""
    from pipeflow.daemon import _reload_decision

    # 冷却期内：看到新 mtime → pending 收下，但不重执行
    pending, do = _reload_decision(now_mtime=500.0, seen_mtime=100.0,
                                   pending_mtime=0.0, now_ts=1000.0, last_reload_ts=900.0)
    assert pending == 500.0 and do is False

    # 下一轮 mtime 没再变，冷却已过 → 仍要执行（关键：不能被当成已处理丢掉）
    pending2, do2 = _reload_decision(now_mtime=500.0, seen_mtime=100.0,
                                     pending_mtime=pending, now_ts=1301.0, last_reload_ts=900.0)
    assert pending2 == 500.0 and do2 is True

    # mtime 未变化时不该触发
    _, do3 = _reload_decision(now_mtime=100.0, seen_mtime=100.0,
                              pending_mtime=0.0, now_ts=9999.0, last_reload_ts=0.0)
    assert do3 is False
