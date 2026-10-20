#!/usr/bin/env python3
"""_recover_rate_limited_roles 回归守卫。

生产事故：curator / maintainer 的 pane 卡在
「Please run /login · API Error: 403 FreeTierError」数小时无人恢复。
根因是恢复函数只从 workflow_instances 的 assignee 取角色，而这两个角色
没有在跑的工作流 → 永远不在候选集里。tmux 兜底必须覆盖到它们。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_rate_recovery.py -q
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import WorkflowEngine, _WORKFLOWS_DIR


def _engine():
    eng = WorkflowEngine(workflows_dir=_WORKFLOWS_DIR)
    eng._lm = MagicMock()
    eng._lm.query.return_value = []          # DB 里没有活跃工作流
    eng._last_rate_recover = {}
    return eng


def test_no_workflow_role_recovered_via_tmux():
    """不在任何工作流里、但 tmux 会话存在的角色，403 时也要被恢复。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\nccs-maintainer\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0,
                             stdout="● Please run /login · API Error: 403 FreeTierError")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    resumed = {c[c.index("-t") + 1][4:].split(":")[0]
               for c in sent if "继续" in c}
    assert resumed == {"curator", "maintainer"}, f"应恢复两个孤儿角色, got {sent}"


def test_upstream_connection_lost_is_recovered():
    """pane 停在 "API Error: upstream connection lost" 也要恢复。

    实测 curator 12:39 起 21min 无产出：进程活着、pane 停在错误，
    恢复词表里没有这一种 → 引擎视而不见。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0,
                             stdout="● API Error: upstream connection lost\n✻ Sautéed for 48s")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert any("继续" in c for c in sent), f"upstream 中断应发继续, got {sent}"


def test_recovered_role_not_pinged_again_on_scrollback():
    """错误滚到 -S -60 的旧内容里、角色已恢复 → 不得再发「继续」。

    实测 6h 内 35 条 `[rate_limit] 已发继续`：按全文匹配，旧错误每档退避
    都命中一次，每次换来一段无进展的自检长文。
    """
    eng = _engine()
    sent = []
    pane = ("● API Error: 403 FreeTierError\n" + "  " + "\n".join(["working…"] * 40)
            + "\n✻ Cogitated for 2m\n❯ ")

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-curator\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=pane)
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert not any("继续" in c for c in sent), f"已恢复角色不应被重发继续, got {sent}"


def test_healthy_role_not_touched():
    """pane 没有额度错误文案 → 不发「继续」。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="✻ Cogitated for 3m\n❯ ")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert sent == [], f"健康角色不应被打扰, got {sent}"


def test_generating_role_not_interrupted_by_stale_error_text():
    """pane 里残留旧 403 文案但角色正在生成 → 不发 C-u/「继续」。

    -S -60 会带上滚出视口的旧错误行；只按"含错误文案"判会把正在干活的角色
    打断（C-u 清输入行）。
    """
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout=(
                "● Please run /login · API Error: 403 FreeTierError\n"
                "✻ Gesticulating… (13s · ↓ 492 tokens)\n"
                "────────\n❯ \n────────\n"
                "  bypass permissions on (shift+tab to cycle) · esc to interrupt"))
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()

    assert sent == [], f"生成中的角色不应被打断, got {sent}"


def test_recovery_resets_backoff_after_healthy_tick():
    """403 → 恢复 → 再次 403：第二次仍从 attempt=1 起步，而不是累加到 2。

    不清退避时档位只增不减，反复的间歇性 403 会把等待推到 1800s 封顶，
    恢复一次要等半小时。
    """
    eng = _engine()
    state = {"mode": "403"}

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            if state["mode"] == "403":
                return MagicMock(returncode=0, stdout="API Error: 403 FreeTierError")
            return MagicMock(returncode=0, stdout="✻ Cogitated for 3m\n❯ ")
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()            # 首次 403 → attempt=1
        assert eng._last_rate_recover["lr"][1] == 1
        state["mode"] = "ok"
        eng._recover_rate_limited_roles()            # 恢复 → 清退避
        assert "lr" not in eng._last_rate_recover, "恢复后应清掉退避状态"
        state["mode"] = "403"
        eng._recover_rate_limited_roles()            # 再次 403

    assert eng._last_rate_recover["lr"][1] == 1, "退避应重新从第 1 档起步"


def test_backoff_blocks_immediate_retry():
    """第二次调用落在退避窗口内 → 不再发。"""
    eng = _engine()
    sent = []

    def fake_run(cmd, *a, **k):
        if "list-sessions" in cmd:
            return MagicMock(returncode=0, stdout="ccs-lr\n")
        if "capture-pane" in cmd:
            return MagicMock(returncode=0, stdout="API Error: 429 Rate limit exceeded")
        if "send-keys" in cmd:
            sent.append(cmd)
        return MagicMock(returncode=0, stdout="")

    with patch("pipeflow.engine._sp.run", side_effect=fake_run):
        eng._recover_rate_limited_roles()
        eng._recover_rate_limited_roles()

    resumed = [c for c in sent if "继续" in c]
    assert len(resumed) == 1, f"退避窗口内应只发 1 次, got {len(resumed)}"


if __name__ == "__main__":
    for fn in (test_no_workflow_role_recovered_via_tmux,
               test_healthy_role_not_touched,
               test_backoff_blocks_immediate_retry):
        fn()
        print(f"OK {fn.__name__}")
