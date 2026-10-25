#!/usr/bin/env python3
"""开发角色池不得把步骤改投到「API 错误态」的角色。

现场：2026-10-24T06:10Z–10-25T16:40Z 单个 pg 会话 106 条 `API Error: Request
rejected (429)`、**0 次工具调用**；同期 7 条改投 pg 的实例全停在 notified 后被
cancel（其中 4 条 timeout_count 打满，3 条被僵尸回收）。

根因不是「忙闲判断错」，而是「空闲」被当成了「可用」：_is_role_busy 只看有没有
在跑的步骤 / pane 是否活跃，被 429 打停的角色两者都为假；_role_producing_recently
看转录 mtime，而错误轮也在写转录 → 两路判据都读成「闲着，可以派活」。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_dev_pool_error_state.py -q
"""
import json
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeflow.role_lifecycle as rl  # noqa: E402
from pipeflow.engine import Step, WorkflowEngine  # noqa: E402
from pipeflow.role_lifecycle import role_erroring  # noqa: E402


def _transcript(root: Path, role: str, turns: list) -> None:
    d = root / f"-home-administrator-ccs-workspaces-{role.replace('_', '-')}"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "s.jsonl"
    with f.open("w", encoding="utf-8") as fh:
        for rec in turns:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    now = time.time()
    import os
    os.utime(f, (now, now))  # mtime 必须新鲜：要证明「转录在写」这条信号挡不住错误态


def _asst(text: str, err: bool = False) -> dict:
    rec = {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}
    if err:
        rec["isApiErrorMessage"] = True
    return rec


def _api_request() -> dict:
    return {"type": "api-request", "message": {"content": None}}


@contextmanager
def _transcripts(turns_by_role: dict):
    """_TRANSCRIPT_ROOT 是 import 期常量，patch Path.home 对它无效。"""
    root = Path(tempfile.mkdtemp())
    for role, turns in turns_by_role.items():
        _transcript(root, role, turns)
    with patch.object(rl, "_TRANSCRIPT_ROOT", root):
        yield root


def test_last_turn_429_is_erroring():
    with _transcripts({"pg": [_asst("干活中"), _api_request(),
                              _asst("API Error: Request rejected (429)", err=True)]}):
        assert role_erroring("pg") is True


def test_last_turn_login_403_is_erroring():
    # 结构字段 isApiErrorMessage 命中「Please run /login · API Error: 403」——
    # 只按 'API Error' 前缀匹配会漏掉这一族（历史错误轮里占 39%）。
    with _transcripts({"pg": [_asst("Please run /login · API Error: 403", err=True)]}):
        assert role_erroring("pg") is True


def test_healthy_role_is_not_erroring():
    with _transcripts({"engineer": [_asst("API Error: Request rejected (429)", err=True),
                                    _asst("已修复 src/x.py:12，pytest 12 passed")]}):
        assert role_erroring("engineer") is False


def test_missing_role_dir_is_not_erroring():
    with _transcripts({}):
        assert role_erroring("nobody") is False


@contextmanager
def _engine(tmp: Path):
    """_lifecycle 是只读 property（真实实现自己建连接），只能从类上换掉。

    必须用 with 收尾：类级 patch 泄漏到后续用例会让整轮 10 条无关用例全红
    （本轮实测：.start() 不 stop，stability/chain_convergence 一起挂）。
    """
    eng = WorkflowEngine(workflows_dir=tmp)
    eng._is_role_busy = MagicMock(return_value=False)
    _lm = MagicMock()
    with patch.object(type(eng), "_lifecycle", new_callable=PropertyMock,
                      return_value=_lm):
        yield eng, _lm


def _step() -> Step:
    return Step(id="s0", title="t", target_role="engineer", prompt_template="x",
                exit_condition={})


def test_redirect_skipped_when_alt_erroring():
    with _transcripts({"pg": [_asst("API Error: Request rejected (429)", err=True)]}), \
            _engine(Path(tempfile.mkdtemp())) as (eng, lm):
        eff = eng._resolve_effective_role({"instance_id": "wf_t1"}, _step(), {})
    assert eff == "engineer", "备选角色在错误态时不得改投"
    lm.execute.assert_not_called()


def test_quoted_error_in_normal_turn_is_not_erroring():
    """角色在正常轮里复述错误文案（"上次的 API Error 已恢复"）不是错误态。

    只看词表会把这种轮判成不可用（删掉 isApiErrorMessage 检查时本用例必须红）；
    结构字段 isApiErrorMessage 才是「这一轮本身就是错误」的权威信号。
    """
    with _transcripts({"pg": [_asst("上次的 API Error: Request rejected (429) 已恢复，继续跑 pytest")]}):
        assert role_erroring("pg") is False


def test_overflow_turn_is_not_erroring():
    """词表边界：'Prompt is too long' 也是 isApiErrorMessage，但它归 /compact 那条路，
    给错误态判据会把正在恢复的角色永久踢出池子（删掉词表检查时本用例必须红）。"""
    with _transcripts({"pg": [_asst("Prompt is too long", err=True)]}):
        assert role_erroring("pg") is False


def test_newest_transcript_wins():
    """同目录多个 jsonl：旧的错误 / 新的健康 → 取最新（真实 pg 目录有 33 个文件）。"""
    root = Path(tempfile.mkdtemp())
    _transcript(root, "pg", [_asst("API Error: Request rejected (429)", err=True)])
    d = root / "-home-administrator-ccs-workspaces-pg"
    old = d / "s.jsonl"
    import os
    os.utime(old, (time.time() - 600, time.time() - 600))
    newer = d / "new.jsonl"
    newer.write_text(json.dumps(_asst("已修复 3 处，pytest 8 passed")) + "\n",
                     encoding="utf-8")
    with patch.object(rl, "_TRANSCRIPT_ROOT", root):
        assert role_erroring("pg") is False


def test_redirect_happens_when_alt_healthy():
    with _transcripts({"pg": [_asst("已提交 src/y.py:30，3 passed")]}), \
            _engine(Path(tempfile.mkdtemp())) as (eng, lm):
        eff = eng._resolve_effective_role({"instance_id": "wf_t1"}, _step(), {})
    assert eff == "pg", "备选健康时仍应分流（别把功能改死）"
    _sql, _args = lm.execute.call_args[0]
    assert json.loads(_args[0]) == {"s0": "pg"} and _args[1] == "pg", "落库的必须是改投后的角色"


def test_pinned_healthy_role_is_kept():
    """钉住的角色健康时不得解钉 —— 否则每 tick 解钉/再钉，正是注释要防的「反复横跳」。"""
    inst = {"instance_id": "wf_t3", "effective_roles": json.dumps({"s0": "pg"})}
    with _transcripts({"pg": [_asst("已提交 src/z.py:9，2 passed")]}), \
            _engine(Path(tempfile.mkdtemp())) as (eng, lm):
        eff = eng._resolve_effective_role(inst, _step(), {})
    assert eff == "pg"
    lm.execute.assert_not_called()


def test_pinned_erroring_role_is_unpinned():
    """已写坏的 effective_roles 必须能被解钉 —— 现场那 3 条正是「先钉上、后 429」。"""
    inst = {"instance_id": "wf_t2", "effective_roles": json.dumps({"s0": "pg"})}
    with _transcripts({"pg": [_asst("API Error: Request rejected (429)", err=True)]}), \
            _engine(Path(tempfile.mkdtemp())) as (eng, lm):
        eff = eng._resolve_effective_role(inst, _step(), {})
    assert eff == "engineer", "钉住的角色进错误态时必须退回模板角色"
    _sql, _args = lm.execute.call_args[0]
    assert json.loads(_args[0]) == {"s0": "engineer"} and _args[1] == "engineer"
