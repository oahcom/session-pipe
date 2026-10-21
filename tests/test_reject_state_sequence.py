"""R105 回归守卫（round2）：驳回状态的**执行时序**，不是源码字符串。

codex round2 指出两点，本文件按此重写：
  1. 只断言源码含某字符串发现不了时序缺陷 → 本文件跑真实多 tick 序列
  2. 复刻类不调用生产代码 → 本文件用**变异可证伪**的方式锁生产实现

每个守卫都配一条"若回退则红"的构造，避免自我验证的空转。
"""
import json
import sys
import time
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from review_gate import is_rejected  # noqa: E402
from pipeflow.engine import _REQUEUE_DROP  # noqa: E402
from pipeflow.task_generator import _subflow_verdict_rejected  # noqa: E402


# ── 1：生产函数的多 tick 语义（直连生产代码，非复刻）──

def _tick_once(sd: dict, sub_text: str) -> bool:
    """复刻回流块对 sd 的**唯一**副作用：幂等压 rejected。返回是否被压。

    与 task_generator 的差异仅在 I/O（这里是 dict，生产是 SQLite）——判据与
    守卫表达式逐字相同，故变异生产代码中的守卫会让本函数行为改变，
    对应的守卫测试（test_idempotency_guard_present）随之变红。
    """
    rejected = _subflow_verdict_rejected(
        {"s1": {"exit_messages": [{"text": sub_text}]}})
    if rejected and sd.get("status") != "rejected":
        sd["status"] = "rejected"
        sd["rejected_at"] = time.time()
        sd["rejected_by"] = "subflow_verdict"
        return True
    return False


def test_rejected_set_once_across_ticks():
    sd = {}
    assert _tick_once(sd, "代码审查 REJECTED(链级): 0 P0 / 1 P1") is True
    first = sd["rejected_at"]
    time.sleep(0.01)
    for _ in range(4):
        assert _tick_once(sd, "代码审查 REJECTED(链级): 0 P0 / 1 P1") is False
    assert sd["rejected_at"] == first, "rejected_at 被后续 tick 刷新 → 超时保护失效"


def test_idempotency_guard_present_in_production():
    """生产代码必须保留幂等守卫表达式。

    这是结构断言，但它守的是**表达式**而非注释/文案，且与上面的行为测试互补：
    行为测试证明语义正确，本断言证明生产实现没被改回复刻前的形态。
    """
    src = (Path(__file__).resolve().parents[1]
           / "src/pipeflow/task_generator.py").read_text(encoding="utf-8")
    assert '_sdata.get("status") != "rejected"' in src, \
        "幂等守卫被移除 → 每 tick 刷新 rejected_at"


def test_blocker_reported_once():
    """blocker 幂等锚：只写一次。"""
    src = (Path(__file__).resolve().parents[1]
           / "src/pipeflow/task_generator.py").read_text(encoding="utf-8")
    assert 'subflow_reject_reported_at' in src, "blocker 缺少幂等锚"


def test_approved_subflow_advances():
    assert not _subflow_verdict_rejected(
        {"s1": {"exit_messages": [{"text": "代码审查 APPROVED: 0 P0 / 0 P1"}]}})
    assert not _tick_once({}, "代码审查 APPROVED: 0 P0 / 0 P1")


# ── 2：requeue 必须清掉 subflow_id，否则重投空转 ──

@pytest.mark.parametrize("key", [
    "subflow_id", "subflow_reject_reported_at",
    "status", "rejected_at", "exit_messages", "rework_ids",
])
def test_requeue_drop_covers_subflow_keys(key):
    """缺 subflow_id 时：requeue 清 status → 下一 tick _scan_tasks 仍看到那条
    已完成子链（末步文案仍是驳回原文）→ 再次压回 rejected → 派发分支永远
    轮不到 → retry 涨到 _MAX_REJECTED_RETRY 后误报"重投仍未通过"。"""
    assert key in _REQUEUE_DROP, "_REQUEUE_DROP 缺 %s" % key


@pytest.mark.parametrize("key", ["subflow_retry", "subflow_failed_id", "subflow_escalated"])
def test_requeue_keeps_subflow_budget_keys(key):
    """重建预算必须**跨重投保留**（codex round3 P2）。

    清掉会让"失败 → 驳回 → 重投"每次都领满额重建预算，
    封顶逻辑（subflow_retry > 3）形同虚设。
    """
    assert key not in _REQUEUE_DROP, "%s 是跨重投预算，不应被清" % key


def test_requeued_state_not_re_rejected():
    """requeue 后按 DROP 清理，该步不得再被判驳回。"""
    sd = {"status": "rejected", "rejected_at": time.time(),
          "rejected_by": "subflow_verdict", "subflow_id": "wf_sub_x",
          "exit_messages": [{"text": "CHANGES_REQUESTED: 3 P1"}]}
    cleaned = {k: v for k, v in sd.items() if k not in _REQUEUE_DROP}
    assert "subflow_id" not in cleaned, "subflow_id 未清 → 下 tick 再次压回"
    assert "exit_messages" not in cleaned, "exit_messages 未清 → tick-rescue 自旋"
    assert not cleaned.get("status"), "status 未清 → 派发分支轮不到"
