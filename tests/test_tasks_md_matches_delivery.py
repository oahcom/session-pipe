"""TASKS.md 必须等于**真正投递成功**的任务。

现场（2026-10-22 05:33，pm）：TASKS.md 主块是旧任务（BetterGI），末尾一行
[排队] 新任务 wf_sub_db062335d6c2/s1 待当前任务完成后执行，而同刻 ccs send 投出去
的 prompt 已经是新任务（落地页）。角色拿两份互相矛盾的任务书：按 TASKS.md 干活
就干错，按 prompt 干活 TASKS.md 又一直说排队。原因是 TASKS.md 写在发消息之前，
队列分支只追加标记、发送照旧。
"""
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest

import pipeflow.role_lifecycle as rl
from pipeflow.role_lifecycle import RoleLifecycleMixin


@pytest.fixture(autouse=True)
def _clear_send_cache():
    """去重窗口是类级缓存，会跨用例串味（同 prompt 第二次被判 busy）。"""
    RoleLifecycleMixin._LAST_SEND.clear()
    yield
    RoleLifecycleMixin._LAST_SEND.clear()

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

def test_sqlite_locked_after_send_still_returns_true(tmp_path, monkeypatch):
    """投递已成功、同步任务书撞库锁：必须吞掉，不能让整个 tick 停摆。

    _send_to_role 的返回值决定 engine.run_once 是否继续推进后面的实例；
    sqlite3.OperationalError 冒出去会被 run_once 的兜底 except 接住，
    本轮剩余实例全部不推进（与 TimeoutExpired 同类的单点停整轮）。
    """
    monkeypatch.setattr(rl, "_CCS_WORKSPACES", tmp_path)
    _workspace(tmp_path)
    h = _Harness()
    h._lifecycle.query.side_effect = sqlite3.OperationalError("database is locked")
    with patch.object(rl._engine_mod._sp, "run",
                      return_value=MagicMock(returncode=0, stdout='{"success": true}')):
        assert h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1") is True


def test_writes_when_lazy_db_missing_but_lifecycle_present(tmp_path, monkeypatch):
    """_lm 未懒加载不等于没有库：_lifecycle 能给任务行就必须写文件。

    旧实现在这里判 getattr(self, "_lm", None) is None 就 return —— 语义是
    「还没查过库」而不是「没有库」，任何新投递入口都会静默不写任务书。
    """
    monkeypatch.setattr(rl, "_CCS_WORKSPACES", tmp_path)
    tasks = _workspace(tmp_path)
    h = _Harness()
    h._lm = None
    h._send_to_role.__func__  # 确认走真实实现（不是被替换的桩）
    with patch.object(rl._engine_mod._sp, "run",
                      return_value=MagicMock(returncode=0, stdout='{"success": true}')):
        assert h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1") is True
    # 看主块来源行而不是子串：OLD 的排队行里本来就有 wf_new0000000002
    assert "## 来源\n工作流 wf_new0000000002 / 步骤 s1" in tasks.read_text(encoding="utf-8"), \
        "_lm 未懒加载就被静默跳过，任务书还停在旧任务"


def test_same_task_not_rewritten_keeps_mtime(tmp_path, monkeypatch):
    """同一任务重投不重写文件：每 tick 刷新 mtime 会让热加载/监控误判产出。"""
    monkeypatch.setattr(rl, "_CCS_WORKSPACES", tmp_path)
    tasks = _workspace(tmp_path)
    h = _Harness()
    with patch.object(rl._engine_mod._sp, "run",
                      return_value=MagicMock(returncode=0, stdout='{"success": true}')):
        assert h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1") is True
        first = tasks.stat().st_mtime_ns
        # force：跳过 900s 同 prompt 去重窗口，走真正的二次投递
        assert h._send_to_role("qa", PROMPT, wf_id="wf_new0000000002", step_id="s1",
                               force=True) is True
    assert tasks.stat().st_mtime_ns == first, "同内容不该重写"
