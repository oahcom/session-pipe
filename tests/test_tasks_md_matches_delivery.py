"""TASKS.md 必须等于**真正投递成功**的任务。

现场（2026-10-22 05:33，pm）：TASKS.md 主块是旧任务（BetterGI），末尾一行
[排队] 新任务 wf_sub_db062335d6c2/s1 待当前任务完成后执行，而同刻 ccs send 投出去
的 prompt 已经是新任务（落地页）。角色拿两份互相矛盾的任务书：按 TASKS.md 干活
就干错，按 prompt 干活 TASKS.md 又一直说排队。原因是 TASKS.md 写在发消息之前，
队列分支只追加标记、发送照旧。
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeflow.role_lifecycle as rl
from pipeflow.role_lifecycle import RoleLifecycleMixin

OLD = ("# 工作流任务\n\n## 标题\n旧任务\n\n## 描述\n旧描述\n\n"
       "## 来源\n工作流 wf_old0000000001 / 步骤 s1\n\n---\n"
       "[排队] 新任务 wf_new0000000002/s1 待当前任务完成后执行\n")
PROMPT = "请把这次的任务做完，正文要有足够长度以通过投递前置校验"


class _Harness(RoleLifecycleMixin):
    def __init__(self):
        self._bb = MagicMock()
        self._lm = MagicMock()
        self._lifecycle = MagicMock()
        self._lifecycle.query.return_value = [{"title": "新任务", "description": "新描述"}]

    def _is_role_busy(self, role, exclude_wf_id="", pane_fallback=True):
        return False

    def _wait_for_role_ready(self, role, wf_id="", step_id=""):
        return True


def _workspace(tmp_path):
    d = tmp_path / "qa"
    d.mkdir()
    (d / "TASKS.md").write_text(OLD, encoding="utf-8")
    return d / "TASKS.md"


def test_tasks_md_follows_delivered_task(tmp_path, monkeypatch):
    """投递成功 → TASKS.md 换成这次的任务，排队的旧记号消失。"""
    monkeypatch.setattr(rl, "_CCS_WORKSPACES", tmp_path)
    tasks = _workspace(tmp_path)
    h = _Harness()
    with patch.object(rl._engine_mod._sp, "run",
                      return_value=MagicMock(returncode=0, stdout='{"success": true}')):
        r = h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1")
    assert r is True
    txt = tasks.read_text(encoding="utf-8")
    assert "新任务" in txt and "wf_new0000000002" in txt, txt
    assert "[排队]" not in txt, "投递成功的任务不该还标着排队"


def test_tasks_md_untouched_when_delivery_fails(tmp_path, monkeypatch):
    """投递失败 → 不得把 TASKS.md 指向一个角色没收到的任务。"""
    monkeypatch.setattr(rl, "_CCS_WORKSPACES", tmp_path)
    tasks = _workspace(tmp_path)
    h = _Harness()
    with patch.object(rl._engine_mod._sp, "run",
                      return_value=MagicMock(returncode=1,
                                             stdout='{"success": false, "error": "未运行"}')):
        r = h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1")
    assert r is False
    txt = tasks.read_text(encoding="utf-8")
    assert "## 标题\n旧任务" in txt, "没投出去的任务怎能成为角色的任务书：" + txt
    assert "工作流 wf_old0000000001" in txt
