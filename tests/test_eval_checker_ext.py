#!/usr/bin/env python3
"""
eval_checker 新增功能测试: 权重采样、report-unverified、role 过滤。

运行: cd /home/administrator/session-pipeline && python3 -m pytest tests/test_eval_checker_ext.py -x -q
"""
import sys
from pathlib import Path
from unittest.mock import patch

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import eval_checker as ec


# ── 测试数据 ──

_UNVERIFIED_ROLE = {
    "name": "无命令角色",
    "eval_criteria": [
        "检查服务是否正常运行（自然语言描述，无可执行命令）",
    ],
}

_VERIFIED_ROLE = {
    "name": "正常角色",
    "eval_criteria": [
        "验证: systemctl --user is-active sister-agent-ssk.service 输出 active",
        "验证: systemctl --user is-active sister-agent-dkk.service 输出 active",
    ],
}

_VERIFIED_ROLE_2 = {
    "name": "维护者",
    "eval_criteria": [
        "验证: systemctl --user is-active cron-worker.service 输出 active",
    ],
}

_ALL_ROLES = [_UNVERIFIED_ROLE, _VERIFIED_ROLE, _VERIFIED_ROLE_2]


def test_report_unverified():
    """_find_unverified 只返回全自然语言（无可执行命令）的角色。"""
    result = ec._find_unverified(_ALL_ROLES)
    assert len(result) == 1
    assert result[0]["name"] == "无命令角色"


def test_report_unverified_all_verified():
    """所有角色都有可执行命令时返回空列表。"""
    all_verified = [_VERIFIED_ROLE, _VERIFIED_ROLE_2]
    assert ec._find_unverified(all_verified) == []


def test_role_filter():
    """run_eval_check(role_filter=...) 只检查指定角色。"""
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "_run",
                     return_value={"ok": True, "stdout": "active", "stderr": "", "returncode": 0}),
        patch.object(ec, "_write_bus"),
    ):
        s = ec.run_eval_check(role_filter="维护者")

    assert s == {"checked": 1, "passed": 1, "failed": 0, "skipped": 0, "notices": 0, "blockers": 0}


def test_role_filter_nonexistent():
    """不存在的角色名返回全 0。"""
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "LOGGER"),
    ):
        s = ec.run_eval_check(role_filter="不存在的角色")
    assert s["checked"] == 0


def test_weights_sampling():
    """权重 = 含"验证:"的 criterion 数（最少 1），验证传给 random.choices。"""
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "_run",
                     return_value={"ok": True, "stdout": "active", "stderr": "", "returncode": 0}),
        patch.object(ec, "_write_bus"),
        patch.object(ec.random, "choices") as mock_choices,
    ):
        ec.run_eval_check()

    _args, kwargs = mock_choices.call_args
    assert "weights" in kwargs
    # 无命令角色: 0 条含"验证:" → max(1, 0) = 1
    # 正常角色:  2 条 → 2
    # 维护者:  1 条 → 1
    assert kwargs["weights"] == [1, 2, 1]
