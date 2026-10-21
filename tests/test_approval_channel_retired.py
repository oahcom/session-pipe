#!/usr/bin/env python3
"""审批密钥通道已退场 —— 评审步按结论推进，不签发、不投递审批请求。

现场（2026-10-22）：引擎侧早已不再等待 confirm（step_handler「评审步骤不再在
tick 里等审批」），但 manager 仍在签发密钥并向审批人发「[workflow] 步骤审批请求」：
24h 实测 107 条，指令不可执行（bus 里密钥被脱敏成 ***，拉取通道对多数角色无效），
等于把角色的注意力花在一条完成不了的任务上。

契约（能量化的两条）：
  1. 评审步完成后 step_results 里没有任何 approval_* 字段
  2. 完成后不向任何角色投递含「步骤审批请求」的消息

反向验证：把 manager.py 回退到上一个提交后本用例必红 —— 签发路径复活。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_approval_channel_retired.py -q
"""
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lifecycle.manager import LifecycleManager  # noqa: E402

_STEPS = [
    {"step_id": "s1", "title": "产出", "target_role": "engineer",
     "prompt_template": "do", "exit_condition": {"bus_category": "task_spec"}},
    {"step_id": "s2", "title": "门禁", "target_role": "qa", "type": "review",
     "prompt_template": "gate", "exit_condition": {"bus_category": "test_report"}},
]


def _lm(path):
    lm = LifecycleManager("product_architect", db_path=path)
    lm.upsert_template("tpl", "模板", "描述", _STEPS,
                       trigger_scene=["t"], allowed_initiators=["engineer"],
                       allowed_executors=["engineer", "qa"])
    lm.start_wf("wf_ret", "s1", "tpl", initiator="engineer")
    return lm


def test_review_step_issues_no_token_and_no_request():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    path = tmp.name
    tmp.close()
    try:
        with patch("subprocess.run") as sp:
            sp.return_value = MagicMock(returncode=0, stderr=b"")
            lm = _lm(path)
            lm.complete_step("wf_ret", "s1")
            lm.complete_step("wf_ret", "s2")
            sr = lm._parse_results(lm._get_wf_unsafe("wf_ret")).get("s2") or {}
            leaked = sorted(k for k in sr if k.startswith("approval_"))
            assert not leaked, f"评审步仍签发审批字段: {leaked}"
            sent = [c for c in sp.call_args_list if "步骤审批请求" in str(c)]
            assert not sent, f"仍向审批人投递审批请求: {sent[:1]}"
            lm.close()
    finally:
        os.unlink(path)
