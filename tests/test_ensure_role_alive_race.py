"""_ensure_role_alive 三态与误杀防护回归。

判别契约：
- 探测失败（tmux 瞬时不可达）→ 视为存活，绝不 kill
- pane 真消失（二次确认仍空）→ 恰好 kill 一次
- 首判空、二次确认有 → 竞态假阴性，不得 kill
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))

import pipeflow.engine as em  # noqa: E402
from pipeflow.role_lifecycle import RoleLifecycleMixin  # noqa: E402


@pytest.fixture
def lm():
    obj = RoleLifecycleMixin.__new__(RoleLifecycleMixin)
    return obj


def _mk(pane_results, kill_log):
    """构造 _sp.run：has-session 恒存活；list-panes 按 pane_results 依次返回。"""
    seq = {"i": 0}

    def _run(cmd, *a, **k):
        s = " ".join(cmd) if isinstance(cmd, (list, tuple)) else str(cmd)
        if "kill-session" in s:
            kill_log.append(s)
            return MagicMock(returncode=0, stdout="")
        if "has-session" in s:
            return MagicMock(returncode=0, stdout="")
        if "list-panes" in s:
            item = pane_results[min(seq["i"], len(pane_results) - 1)]
            seq["i"] += 1
            if isinstance(item, Exception):
                raise item
            return MagicMock(returncode=0, stdout=item)
        return MagicMock(returncode=0, stdout="")

    return _run


def test_probe_error_treated_as_alive_not_killed(lm):
    """探测失败 → 判不出 ≠ 判死，不得 kill（tmux 抖动实测存在）。"""
    kills = []
    with patch.object(em._sp, "run", _mk([em._sp.TimeoutExpired("tmux", 5)], kills)):
        r = lm._ensure_role_alive("probe_err")
    assert r is True, "探测失败必须视为存活"
    assert kills == [], f"探测失败不得 kill，实际 {kills}"


def test_real_death_killed_exactly_once(lm):
    """pane 真消失且二次确认仍空 → 恰好 kill 一次，拉起后恢复。

    mock 序列：两次判死 → kill → 拉起 → 第三次 list-panes 返回 pid（角色已就绪）。
    避免拉起分支的 has-session 等待循环空转（每次 sleep(1)+sleep(3)）。
    """
    kills = []
    with patch.object(em._sp, "run", _mk(["", "", "4242\n"], kills)):
        r = lm._ensure_role_alive("real_dead")
    assert len(kills) == 1, f"真死应 kill 一次，实际 {len(kills)}"
    assert r is True, "拉起成功后应返回 True"


def test_race_false_negative_not_killed(lm):
    """首判空、二次确认有 pid → 竞态假阴性，不得 kill。"""
    kills = []
    with patch.object(em._sp, "run", _mk(["", "4242\n"], kills)):
        r = lm._ensure_role_alive("race")
    assert r is True
    assert kills == [], f"竞态假阴性不得 kill，实际 {kills}"


def test_alive_untouched(lm):
    """正常存活 → 不 kill 不重启。"""
    kills = []
    with patch.object(em._sp, "run", _mk(["777\n"], kills)):
        r = lm._ensure_role_alive("healthy")
    assert r is True
    assert kills == []


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
