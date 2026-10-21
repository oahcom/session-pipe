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


def test_role_filter():
    """run_eval_check(role_filter=...) 只检查指定角色。"""
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "_run",
                     return_value={"ok": True, "stdout": "active", "stderr": "", "returncode": 0}),
        patch.object(ec, "_write_bus"),
    ):
        s = ec.run_eval_check(role_filter="维护者")

    # leverage_count 可能由新检查器产生，不强制为 0
    assert s["checked"] == 1 and s["passed"] == 1 and s["failed"] == 0 and s["skipped"] == 0 and s["notices"] == 0 and s["blockers"] == 0 and s["avg_score"] == 1.0


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


def test_is_tool_error():
    """命令自身执行错误（检查器盲区）判定：grep 语法错/文件不存在/选项错误。"""
    assert ec._is_tool_error({"returncode": 2, "stderr": "grep: unrecognized option", "stdout": ""})
    assert ec._is_tool_error({"returncode": 2, "stderr": "grep: foo: No such file or directory", "stdout": ""})
    assert ec._is_tool_error({"returncode": 2, "stderr": "usage: grep", "stdout": ""})
    # 真实故障不是工具错误
    assert not ec._is_tool_error({"returncode": 1, "stderr": "", "stdout": ""})
    assert not ec._is_tool_error({"returncode": 0, "stderr": "", "stdout": "ok"})
    assert not ec._is_tool_error({"returncode": -3, "stderr": "命令不在白名单", "stdout": ""})


def test_tool_error_skipped_not_failed():
    """工具错误归 skipped，不写 FAIL bus。"""
    captured = []
    orig = ec._write_bus
    def spy(cat, text, *, evidence="", src=""):
        captured.append((cat, text))
    ec._write_bus = spy
    try:
        with (
            patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
            patch.object(ec, "_run",
                         return_value={"ok": False, "stdout": "", "stderr": "grep: bad pattern", "returncode": 2}),
        ):
            s = ec.run_eval_check()
    finally:
        ec._write_bus = orig
    assert s["failed"] == 0
    assert s["skipped"] >= 1
    assert captured == []


# ── 恒真命令护栏 ──

def test_tautology_commands_rejected_before_whitelist():
    """恒真命令必须被拒（而非被"修好"后放行）。

    缺陷实证（2026-10-16）：eval_criteria 写 `python3 -c "print('PASS')"` 时，
    旧实现走"引号规范化"把内层引号替换成单引号**使它能跑通**——即帮一个恒真
    命令通过，而不是拒绝它。命令返回 0 就被记为 PASS，伪造通过率。
    """
    for cmd in ('echo "PASS"', "echo 'pass'", "echo PASS",
                'python3 -c "print(\'PASS\')"',
                "python3 -c 'import sys; print(\"PASS\")'",
                "exit 0", "exit(0)"):
        r = ec._run(cmd)
        assert r["ok"] is False, f"恒真命令未拦截: {cmd}"
        assert r["returncode"] == -3, f"应以 -3 拒绝: {cmd}"
        assert "恒真" in r["stderr"], f"拒绝原因应说明是恒真: {cmd}"


def test_real_readonly_checks_still_allowed():
    """真实只读检查不得被新护栏误伤。"""
    for cmd in ("journalctl --user -u session-pipeline-daemon -n 5", "ps aux"):
        r = ec._run(cmd)
        assert r["returncode"] != -3, f"真实检查被误拒: {cmd} -> {r.get('stderr')}"


def test_is_tautology_normalizes_spaces():
    """空格不影响判定（`echo    PASS` 也应拒）。"""
    for cmd in ('echo    "PASS"', "python3 -c 'import sys; print(  \"PASS\"  )'"):
        assert ec._is_tautology(cmd) is True, f"应判为恒真: {cmd}"
