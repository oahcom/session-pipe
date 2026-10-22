"""_ensure_role_alive 三态与误重启防护回归。

判别契约：
- 探测失败（tmux 瞬时不可达，快照 None）→ 视为存活，绝不拉起
- pane 真消失 → 直接拉起（没有 pane 可 kill，不再有 kill-session 动作）
- pane 在 → 不动它
"""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, (Path(__file__).resolve().parent.parent / "src"))

import pipeflow.engine as em  # noqa: E402
from pipeflow.role_lifecycle import RoleLifecycleMixin  # noqa: E402


@pytest.fixture
def lm():
    obj = RoleLifecycleMixin.__new__(RoleLifecycleMixin)
    return obj


def _mk(pane_results, cmds):
    """构造 _sp.run：ccs start 恒成功；list-panes 按 pane_results 依次返回。"""
    seq = {"i": 0}

    def _run(cmd, *a, **k):
        line = " ".join(cmd) if isinstance(cmd, (list, tuple)) else str(cmd)
        cmds.append(line)
        if "list-panes" not in line:
            return MagicMock(returncode=0, stdout="")
        item = pane_results[min(seq["i"], len(pane_results) - 1)]
        seq["i"] += 1
        if isinstance(item, Exception):
            raise item
        # 快照形态：一次 list-panes -a 返回全部会话（session\tpid\tactivity）。
        # 用例里的角色名各不相同，所以每条都按同一 pid 播出去。
        _now = int(time.time())
        out = "".join(f"ccs-{r}\t{item.strip()}\t{_now}\n"
                      for r in ("probe_err", "real_dead", "race", "healthy", "x"))
        return MagicMock(returncode=0, stdout=out)

    return _run


def _starts(cmds):
    return [c for c in cmds if " start " in f" {c} "]


def test_probe_error_treated_as_alive_not_restarted(lm):
    """探测失败 → 判不出 ≠ 判死，不得拉起（tmux 抖动实测存在）。"""
    cmds = []
    with patch.object(em._sp, "run", _mk([em._sp.TimeoutExpired("tmux", 5)], cmds)):
        assert lm._ensure_role_alive("probe_err") is True
    assert not _starts(cmds), f"探测失败不得拉起: {cmds}"


def test_pane_gone_restarted_without_kill(lm):
    """pane 消失 → 直接拉起；没有 pane 可 kill，不得再跑 kill-session。"""
    cmds = []
    with patch.object(em._sp, "run", _mk(["", "", "4242\n"], cmds)):
        assert lm._ensure_role_alive("real_dead") is True
    assert not [c for c in cmds if "kill-session" in c], f"没有 pane 可 kill: {cmds}"
    assert _starts(cmds), f"应拉起: {cmds}"


def test_alive_untouched(lm):
    """pane 在 → 不拉起。"""
    cmds = []
    with patch.object(em._sp, "run", _mk(["777\n"], cmds)):
        assert lm._ensure_role_alive("healthy") is True
    assert not _starts(cmds), f"存活不得拉起: {cmds}"


def test_is_agent_alive_returns_none_on_timeout(lm):
    """_is_agent_alive 超时 → None（三态），不是 False。"""
    def _raise(*a, **k):
        raise em._sp.TimeoutExpired("tmux", 5)

    with patch.object(em._sp, "run", _raise):
        assert lm._is_agent_alive("x") is None


def test_is_agent_alive_false_when_pane_gone(lm):
    """pane 真消失（returncode!=0 或空 stdout）→ False。"""
    with patch.object(em._sp, "run",
                      lambda *a, **k: MagicMock(returncode=0, stdout="")):
        assert lm._is_agent_alive("x") is False
