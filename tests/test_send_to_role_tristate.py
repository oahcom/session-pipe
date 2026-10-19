"""
test_send_to_role_tristate — _send_to_role 三态契约验证（6763a84 + 4fe4086）

契约:
  True  = 已投递 → 调用方可盖戳 notified
  "busy" = 角色忙（正常排队）→ 非故障，不计 delivery_fail_count，不盖戳
  False = 真失败 → 累计 delivery_fail_count

风险: "busy" 是 truthy 字符串，bool("busy")=True。若调用点用 not 判断，
      "busy" 被误当成功 → 不盖戳也不计失败 → 死循环重试。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from unittest.mock import MagicMock
from pipeflow.role_lifecycle import RoleLifecycleMixin
from pipeflow.engine import WorkflowEngine, Step, WorkflowRun


class _Harness(RoleLifecycleMixin):
    """最小 harness：可注入 _is_role_busy / _wait_for_role_ready 结果。"""

    def __init__(self, busy=False, ready=True):
        self._bb = MagicMock()
        self._lm = MagicMock()
        self._lifecycle = MagicMock()
        self._busy = busy
        self._ready = ready

    def _is_role_busy(self, role, exclude_wf_id="", pane_fallback=True):
        return self._busy

    def _wait_for_role_ready(self, role, wf_id="", step_id=""):
        return self._ready


# ── 分支级: guard 返回值（不触达 ccs send） ──

def test_busy_when_role_has_active_workflow():
    h = _Harness(busy=True)
    r = h._send_to_role("qa", "hello", wf_id="wf_x", step_id="s1")
    assert r == "busy"
    assert r is not False
    assert isinstance(r, str)


def test_false_when_unmatched_var():
    h = _Harness(busy=False)
    r = h._send_to_role("qa", "处理 {unmatched}", wf_id="wf_x", step_id="s1")
    assert r is False


def test_false_when_goal_too_short():
    h = _Harness(busy=False)
    r = h._send_to_role("qa", "p", wf_id="wf_x", step_id="s1")
    assert r is False


def test_busy_when_role_not_ready():
    """未就绪是临时态（忙/启动中/探测抖动）→ 返回 "busy" 让 engine 排队重试。

    返回 False 会被 engine 计入 delivery_fail_count，3 次即灌 delivery_blocked
    终态且无恢复路径（该方法唯一的 False 路径是 90s 空转超时）。
    """
    h = _Harness(busy=False, ready=False)
    r = h._send_to_role("qa", "hello world", wf_id="wf_x", step_id="s1")
    assert r == "busy"


# ── 调用点级: 源码契约（三态处理分支存在于正确位置） ──

def test_engine_run_once_handles_busy_semantics():
    """engine.run_once 对 'busy' 分支: 不计失败、不盖戳（源码契约）。"""
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    assert '_delivered == "busy"' in src, \
        "run_once 缺 'busy' 分支 → 'busy' 会被当真失败累计 delivery_fail_count"
    assert '跳过计数' in src, "'busy' 分支应跳过失败计数"


def test_engine_run_once_skips_recount_when_notified():
    """已 notified 的 step 不重复计数（幂等护栏，源码契约）。"""
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    assert '已 notified' in src or 'skip' in src.lower(), \
        "run_once 缺幂等护栏 → 重复 tick 会让 delivery_fail_count 从旧值继续涨"


def test_engine_engine_has_delivered_guard():
    """run_once 仅当真实投递成功才盖戳 notified。

    原断言 `'if not _delivered:' in src` **锁定了错误行为**：`not "busy"` 恒为
    False，把 busy 漏进盖戳分支，从未投递的 step 被判 notified（R88 P0）。
    正确契约是显式三态判定（2026-10-17）。
    """
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    # 剥掉注释行：修复说明本身会引用旧写法，断言必须针对**代码**而非注释文本
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert "if not _delivered:" not in code, \
        "run_once 不得用 `not _delivered` 判真值 → 'busy' 穿透，误盖戳 notified"
    assert 'if _delivered == "busy":' in code, \
        "run_once 必须以 == 显式判定 'busy'"
    assert "if _delivered is not True:" in code, \
        "run_once 必须以 `is not True` 判定真失败（'busy' 已在上游 continue）"


def test_task_generator_heal_uses_is_not_false():
    """task_generator heal 路径必须用 `is not False` 区分 'busy'（源码契约）。"""
    import inspect
    from pipeflow import task_generator
    src = inspect.getsource(task_generator)
    assert "is not False" in src, \
        "task_generator heal 必须用 `is not False` 而非 bool()，否则 'busy' 被误判为失败"


def test_wf_lifecycle_has_delivery_fallback():
    """wf_lifecycle 推进路径必须对 False 回退 step 状态（源码契约）。"""
    import inspect
    from pipeflow import wf_lifecycle
    src = inspect.getsource(wf_lifecycle)
    assert "_mark_step_delivery_failed" in src, \
        "wf_lifecycle 缺投递失败回退 → 角色未收到却被判已通知"


def test_busy_is_truthy_string():
    """文档化风险: 'busy' 是 truthy，调用点必须用 is not False / == 'busy' 判断。"""
    assert bool("busy") is True
    assert "busy" is not False
    assert "busy" != False


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))


def test_undelivered_step_keeps_empty_status_for_retry():
    """投递失败时 status 必须留空 —— 空状态是**投递重试机制本身**（R88 修正）。

    run_once 的投递分支入口是 `if not step_status`，只有空状态才会每 tick 重试投递。
    曾试图改为保留 'notified' 以便 _tick 的超时机制接管，但那样会让投递重试永不发生
    （实测 test_delivery_fail_count_accumulates_when_unnotified 从 3 跌到 1）。
    投递失败的兜底是 _reached_cap 后的 delivery_blocked 终态 + blocker 通知。
    """
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert '"delivery_blocked" if _reached_cap else ""' in code, \
        "投递失败非终态时必须保留空 status，否则每 tick 重试投递失效"


def test_busy_writes_queue_ledger_for_reclaimer():
    """busy 分支必须写 queued 账本 —— 否则回收器条件4 不可达（codex P1-1）。

    本步 status 留空（投递重试需要），而 _tick 的入口是
    `step_status in ("notified","running")` → 排队链与回收器条件4
    （需 timeout_count>=1 或 queued>=3）全部不可达 → 实例只能等 24h 全局兜底。
    """
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    _i = code.find('if _delivered == "busy":')
    assert _i >= 0, "缺 busy 分支"
    _blk = code[_i:_i + 1400]
    assert '"queued"' in _blk, "busy 分支必须写 queued 账本，否则回收器看不到排队"
    assert '"queued_since"' in _blk, "必须写 queued_since（回收器据它算真实停滞起点）"


def test_busy_does_not_stamp_notified():
    """busy 分支不得盖 status='notified' —— 否则投递永不重试。

    窗口取到该分支自身的 `continue` 为止，避免越界到大分支后续代码。
    """
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    _i = code.find('if _delivered == "busy":')
    _end = code.find("continue", _i)
    assert _i >= 0 and _end > _i, "缺 busy 分支或其 continue"
    _blk = code[_i:_end]
    assert '"status"' not in _blk, \
        "busy 分支写了 status → 投递重试失效（投递分支入口是 `if not step_status`）"


def test_advance_busy_also_reverts_step():
    """advance 路径 busy 也必须回退 —— 否则 next_step 卡在 notified 无人重投（P1-2）。"""
    import inspect
    from pipeflow import wf_lifecycle
    src = inspect.getsource(wf_lifecycle.WorkflowLifecycleMixin._advance_production_wf)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    assert "if _delivered is not True:" in code, \
        "advance 必须用 `is not True` 判三态（busy 也要走回退）"
    _i = code.find("if _delivered is not True:")
    _blk = code[_i:_i + 400]
    assert "_mark_step_delivery_failed" in _blk, \
        "advance 的 `is not True` 分支必须调 _mark_step_delivery_failed（busy 同路径）"
    assert "return" in _blk, "advance 回退后必须 return，否则继续建子工作流/部署"
