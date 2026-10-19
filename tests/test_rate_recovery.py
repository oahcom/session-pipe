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
