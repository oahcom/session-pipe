#!/usr/bin/env python3
"""条件4 僵尸回收必须过"角色 pane 是否还在产出"这一关。

根因（2026-10-19 生产实测）：
  timeout_count 递增时 step_handler 会把该步 ts 重置为 now（给角色响应窗口），
  故 _stall_h 的结构上限恒为一个超时窗（<1h）。条件4 的 tc 分支只看
  `_cur_tc >= 1` 不看 stall → "超时过一次 → 实例满 1h → 杀"，与角色是否
  仍在工作无关。实测 wf_sub_fb975ff26813：reviewer 正在写 133 行报告
  （pane 显示 esc to interrupt），19:52:11 被回收，日志自证矛盾 ——
  `步骤停滞 0.0h / 实例 1.0h (timeout_count=2, queued=0)`。

契约（缺一则重回误杀）：
  1. tc/queued 分支成立时，pane 明确静默超窗（False）才回收
  2. pane 有活动（True）→ 豁免，交条件1（timeout_count>=6）/条件3（24h）兜底
  3. 探测不可用（None）→ 保守不回收，不把 tmux 抖动翻译成死亡
  4. 主查询必须 SELECT assignee（漏选 → 角色解析恒空 → 探测恒 None → 条件4 失效）
  5. queued 分支原有的 `_stall_h > 1` 门控不得被本修复改动

运行: cd /home/administrator/session-pipeline && python3 tests/test_stale_reclaim_pane_gate.py
"""
import json
import re
import sys
import time
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

import liveness                                              # noqa: E402
import pipeflow.wf_lifecycle as wf_lifecycle                 # noqa: E402
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin     # noqa: E402

_SRC_FILE = _ROOT / "src" / "pipeflow" / "wf_lifecycle.py"
_TPL = "reviewer_pr_review"      # 不在 _ZOMBIE_TEMPLATES、非 loop 模板


class _FakeLM:
    """只回放一行 fixture；execute 仅记录 SQL（不写生产库）。"""

    def __init__(self, row):
        self._row = row
        self.executed = []

    def query(self, sql, params=()):
        return [self._row]

    def execute(self, sql, params=()):
        self.executed.append((sql, params))


class _LmStub:
    """回收路径已改为走 close_wf（级联子链 + task 同步）；stub 需实现它才验得到 cancel。"""

    def __init__(self, lm):
        self._lm = lm
        self.closed = []

    def complete_step(self, *a, **k):
        return None

    def close_wf(self, wf_id, status="completed"):
        self.closed.append((wf_id, status))
        self._lm.execute(
            f"UPDATE workflow_instances SET status='{status}' WHERE instance_id=?", (wf_id,))
        return True


# 任何写实例状态的 UPDATE（参数化/换格式都覆盖）；日志 INSERT 不带这个前缀。
_STATUS_WRITE = re.compile(r"UPDATE\s+workflow_instances\s+SET\s+.*?\bstatus\b", re.I | re.S)


def _cancelled_wfs(inst, lm, wf_id):
    """返回被置 cancelled 的 wf_id；泄漏 path 必须是经 close_wf 的那一条。

    1) 未调 close_wf 却出现实例状态写 → 裸 UPDATE，task 同步与子链级联都
       不会发生（旧缺陷形态）。用正则而非字面量前缀，参数化 SQL 也逃不掉。
    2) 有调用时校验状态为 cancelled 且**目标就是这台**——只要求"曾调用过
       close_wf"会让误杀别的实例照样通过。
    """
    _raw = [s for s, _p in lm.executed if _STATUS_WRITE.search(s)]
    _closed = inst._lifecycle.closed
    if not _closed:
        assert not _raw, f"回收未经 close_wf 就写了实例状态: {_raw}"
        return []
    assert all(_st == "cancelled" for _, _st in _closed), \
        f"回收传给 close_wf 的状态不是 cancelled: {_closed}"
    return [_w for _w, _st in _closed if _st == "cancelled" and _w == wf_id]


def _run(step_sd, pane_state, assignee="reviewer"):
    """跑一次 _cleanup_stale_workflows，stub 掉 pane 探测，返回被取消的 wf_id 列表。"""
    now = time.time()
    row = {
        "instance_id": "wf_test_r98",
        "template_id": _TPL,
        "created_at": now - 7200,          # age_h=2：> 1h 门控，< 24h 全局阈值
        "status": "running",
        "current_step_id": "s1",
        "assignee": assignee,
        "step_results": json.dumps({"s1": step_sd}),
    }
    inst = SimpleNamespace(_workflows={}, _notify_role=lambda *a, **k: None)
    lm = _FakeLM(row)
    inst._lifecycle = _LmStub(lm)
    orig = liveness.tmux_pane_active
    orig_prod = wf_lifecycle._role_producing_recently
    liveness.tmux_pane_active = lambda role, **k: pane_state
    # 第二道门（转录新鲜度）与本用例无关，固定为 False 让结论只由 pane 状态决定；
    # 否则同一实例在跑测试的机器上可能因真实 transcript 新鲜而豁免，测试不确定。
    wf_lifecycle._role_producing_recently = lambda role, **k: False
    try:
        WorkflowLifecycleMixin._cleanup_stale_workflows(inst, lm)
    finally:
        liveness.tmux_pane_active = orig
        wf_lifecycle._role_producing_recently = orig_prod
    return _cancelled_wfs(inst, lm, row["instance_id"])


_TWO_TIMEOUTS = {"timeout_count": 2, "ts": time.time() - 60, "queued": 0}


def test_active_pane_role_is_not_cancelled():
    """tc 分支成立但角色仍在产出 → 不得回收。"""
    assert _run(_TWO_TIMEOUTS, True) == [], \
        "角色 pane 有活动仍被回收：条件4 缺活性门控（reviewer 写报告中被杀）"


def test_silent_pane_role_is_cancelled():
    """pane 静默超窗 → 正常回收路径必须保留。"""
    assert _run(_TWO_TIMEOUTS, False), \
        "pane 静默超窗仍未回收：回收路径被削弱，真卡死实例会永驻"


def test_unknown_pane_liveness_skips_reclaim():
    """探测不可用 → 保守放行，不把 tmux 抖动翻译成死亡。"""
    assert _run(_TWO_TIMEOUTS, None) == [], \
        "探测不可用却判死：tmux 短暂不可达会误杀在跑的角色"


def test_empty_assignee_skips_reclaim():
    """角色解析不出（空 assignee）→ 探测拿到空角色必然不可用 → 不得判死。"""
    assert _run(_TWO_TIMEOUTS, False, assignee="  ") == [], \
        "空 assignee 仍被回收：解析失败被当成角色死亡"


def test_queued_branch_keeps_stall_gate():
    """queued 达降级阈值但真实停滞 <1h → 不回收（原语义不得被本修复改动）。"""
    sd = {"queued": 3, "queued_since": time.time() - 600, "ts": time.time() - 10}
    assert _run(sd, False) == [], "queued 分支丢了 _stall_h > 1 停滞门控"


def test_queued_branch_reclaims_when_silent_and_stalled():
    """queued 达阈值 + 真实停滞 >1h + pane 静默 → 回收（原语义保留）。"""
    sd = {"queued": 3, "queued_since": time.time() - 7200, "ts": time.time() - 10}
    assert _run(sd, False), "queued 分支真实停滞 >1h 且 pane 静默却未回收"


def test_pane_gate_is_load_bearing():
    """对照：同一实例只翻转 pane 状态，回收结论必须相反。"""
    assert _run(_TWO_TIMEOUTS, True) != _run(_TWO_TIMEOUTS, False), \
        "pane 状态不影响结论：门控未接入判定，静默/活跃无区别"


def _run_with_subflow(step_sd, sub_status, pane_state, assignee="product_architect", age_h=2):
    """父步挂着子链时跑一次回收，返回被置 cancelled 的 wf_id 列表。"""
    now = time.time()
    row = {
        "instance_id": "wf_test_parent",
        "template_id": _TPL,
        "created_at": now - age_h * 3600,
        "status": "running",
        "current_step_id": "s1",
        "assignee": assignee,
        "step_results": json.dumps({"s1": step_sd}),
    }

    class _LM(_FakeLM):
        def query(self, sql, params=()):
            if "WHERE instance_id=?" in sql and params and params[0] != "wf_test_parent":
                return [{"status": sub_status}] if sub_status else []
            return [self._row]

    inst = SimpleNamespace(_workflows={}, _notify_role=lambda *a, **k: None)
    lm = _LM(row)
    inst._lifecycle = _LmStub(lm)
    orig = liveness.tmux_pane_active
    orig_prod = wf_lifecycle._role_producing_recently
    liveness.tmux_pane_active = lambda role, **k: pane_state
    wf_lifecycle._role_producing_recently = lambda role, **k: False
    try:
        WorkflowLifecycleMixin._cleanup_stale_workflows(inst, lm)
    finally:
        liveness.tmux_pane_active = orig
        wf_lifecycle._role_producing_recently = orig_prod
    return _cancelled_wfs(inst, lm, row["instance_id"])

def test_live_subflow_blocks_parent_reclaim():
    """子链还在跑 → 父步没有新产出是正常态，不得按停滞判死（bus #425593）。"""
    sd = {"subflow_id": "wf_sub_live", "queued": 20,
          "queued_since": time.time() - 7200, "ts": time.time() - 10}
    assert _run_with_subflow(sd, "running", False) == [], \
        "子链 running 时父链仍被回收：139min 子链工时落到空处"

def test_dead_subflow_still_allows_reclaim():
    """子链已终态 → 回收路径必须保留，否则真卡死父链永驻。"""
    sd = {"subflow_id": "wf_sub_dead", "queued": 20,
          "queued_since": time.time() - 7200, "ts": time.time() - 10}
    assert _run_with_subflow(sd, "cancelled", False), "子链已死却连父链也不回收"

def test_live_subflow_blocks_global_24h_reclaim():
    """24h 兜底 + 级联会杀掉仍在产出的子链 → 子链活着时条件3 也必须放行。"""
    sd = {"subflow_id": "wf_sub_live2", "ts": time.time() - 10}
    assert _run_with_subflow(sd, "running", False, age_h=26) == [], \
        "父链过 24h 阈值时活子链被级联杀掉，在途产出丢失"

def test_select_includes_assignee():
    """主查询漏选 assignee → 角色解析恒空 → 探测恒 None → 条件4 静默失效。"""
    code = _SRC_FILE.read_text(encoding="utf-8")
    m = re.search(r"def _cleanup_stale_workflows\(self, lm\).*?(?=\n    def |\ndef )",
                  code, re.S)
    assert m, "未找到 _cleanup_stale_workflows（被重命名？）"
    head = m.group(0).split("FROM workflow_instances")[0]
    assert "assignee" in head, \
        "主查询漏选 assignee：pane 探活拿不到角色"


if __name__ == "__main__":
    _fns = (test_active_pane_role_is_not_cancelled,
            test_silent_pane_role_is_cancelled,
            test_unknown_pane_liveness_skips_reclaim,
            test_empty_assignee_skips_reclaim,
            test_queued_branch_keeps_stall_gate,
            test_queued_branch_reclaims_when_silent_and_stalled,
            test_pane_gate_is_load_bearing,
            test_live_subflow_blocks_parent_reclaim,
            test_dead_subflow_still_allows_reclaim,
            test_live_subflow_blocks_global_24h_reclaim,
            test_select_includes_assignee)
    _fails = 0
    for _fn in _fns:
        try:
            _fn()
            print(f"  OK  {_fn.__name__}")
        except AssertionError as _e:
            _fails += 1
            print(f"  FAIL {_fn.__name__}: {_e}")
    print(f"\n结果: {len(_fns) - _fails} 通过, {_fails} 失败")
    sys.exit(1 if _fails else 0)
