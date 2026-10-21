#!/usr/bin/env python3
"""回归守卫：暂缓判定不得判死 + rejected 升级后须有终结期限。

两个缺陷同源——「没人推进的等待」没有出口：

A. _send_to_role 把 core.send 的**暂缓**结果当成永久失败。
   core.send 对第 1、2 次探测失败返回 "探测失败 N/3（暂缓 fallback，防抖动误报）"，
   第 1、2 次 tmux 投递失败返回 "tmux 投递失败 N/3"，语义都是「稍后重试」。
   引擎按失败计 → 10s 一 tick 三连推即达 delivery_fail_count>=3 → 实例终态
   delivery_blocked。红线 3（3 连败才判 down）被引擎 3 倍放大成 30s 判死。

B. rejected 升级后无终结期限。三个升级入口都只落 rework_escalated_at 再 continue，
   没人处置就一直停着，只能等 24h 全局 cancel 陪葬已推进步骤
   （实测 wf_a4da845a61 16.3h / wf_83a7b58c4a 11.6h）。
"""
import sys
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


class _FakeCompleted:
    def __init__(self, stdout, returncode=1):
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = b""


def _mk_engine():
    from pipeflow.engine import WorkflowEngine

    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng._bb = MagicMock()
    eng._last_bus_write = {}
    eng._wait_for_role_ready = MagicMock(return_value=True)
    eng._is_role_busy = MagicMock(return_value=False)
    return eng


def _send(stdout, prompt="x" * 200):
    eng = _mk_engine()
    with patch("pipeflow.engine._sp.run", return_value=_FakeCompleted(stdout)):
        return eng._send_to_role("engineer", prompt, wf_id="wf_test", step_id="s1")


# ── A. 暂缓判定 ──────────────────────────────────────────────

def test_probe_deferred_returns_busy():
    """探测失败 1/3、2/3 是暂缓（红线 3），不是投递失败。"""
    for n in (1, 2):
        out = json.dumps({"success": False, "error":
                          f"CCS engineer[0] 探测失败 {n}/3（暂缓 fallback，防抖动误报）"},
                         ensure_ascii=False)
        assert _send(out) == "busy", f"探测失败 {n}/3 应判 busy"


def test_tmux_deferred_returns_busy():
    """tmux 投递失败 1/3、2/3 同样是暂缓。"""
    for n in (1, 2):
        out = json.dumps({"success": False,
                          "error": f"tmux 投递失败 engineer[0] ({n}/3)"},
                         ensure_ascii=False)
        assert _send(out) == "busy", f"tmux 投递失败 {n}/3 应判 busy"


def test_confirmed_failures_stay_false():
    """连败确认与门禁拒绝才是真失败——不得被暂缓分支吞成 busy。"""
    cases = [
        json.dumps({"success": False, "error": "tmux 投递失败 engineer[0] (3/3)"},
                   ensure_ascii=False),
        json.dumps({"success": False, "error": "CCS engineer[0] 未运行"},
                   ensure_ascii=False),
        json.dumps({"success": False, "error": "三源验证拒绝: a→b"}, ensure_ascii=False),
        json.dumps({"success": False, "error": "敏感操作门禁拒绝: rm -rf"},
                   ensure_ascii=False),
    ]
    for out in cases:
        assert _send(out) is False, f"应为 False: {out}"


def test_success_returns_true():
    """rc=0 才算投递成功——ccs.py 对 success:false 一律 rc=1 退出。"""
    eng = _mk_engine()
    out = json.dumps({"success": True, "sent_chars": 10})
    with patch("pipeflow.engine._sp.run", return_value=_FakeCompleted(out, returncode=0)):
        r = eng._send_to_role("engineer", "x" * 200, wf_id="wf_test", step_id="s1")
    assert r is True


# ── B. rejected 升级后的终结期限 ─────────────────────────────

def test_escalated_deadline_constant_exists():
    from pipeflow import engine
    assert engine._REJECTED_ESCALATED_MAX > engine._REJECTED_WAIT_MAX, \
        "终结期限必须晚于升级启动阈值，否则升级刚发出就被判死"


def test_rejected_branch_closes_after_escalation_deadline():
    """源码契约：rejected 分支入口处必须判升级锚是否超期并 close failed。

    短路在 branch 入口而非三处升级分支各写一份——漏一处就是一个永久出口缺失。
    """
    import inspect
    from pipeflow import engine
    src = inspect.getsource(engine.WorkflowEngine.run_once)
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith("#"))
    _i = code.find('if step_status == "rejected":')
    assert _i >= 0
    _blk = code[_i:_i + 1600]
    assert "rework_escalated_at" in _blk, "缺升级锚读取 → 期限判不了"
    assert "_REJECTED_ESCALATED_MAX" in _blk, "缺期限判据"
    assert 'status="failed"' in _blk, "超期必须 close failed 收口，否则永远等 24h 兜底"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
