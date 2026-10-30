#!/usr/bin/env python3
"""
eval_checker 新增功能测试: 抽样策略（判据加权 / 通用均匀）、report-unverified、role 过滤。

运行: cd /home/administrator/session-pipeline && python3 -m pytest tests/test_eval_checker_ext.py -x -q
"""
import sys
from pathlib import Path
from unittest.mock import patch

_SRC = str(Path(__file__).resolve().parent.parent / "src")
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

    assert s["checked"] == 1 and s["passed"] == 1 and s["failed"] == 0 and s["skipped"] == 0 and s["notices"] == 0 and s["blockers"] == 0 and s["avg_score"] == 1.0


def test_role_filter_nonexistent():
    """不存在的角色名返回全 0。"""
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "LOGGER"),
    ):
        s = ec.run_eval_check(role_filter="不存在的角色")
    assert s["checked"] == 0


class _StubTrajectory:
    """轨迹检查替身：真检查器读线上转录，结果与本用例无关且随时钟变化。"""

    def check(self, *args, **kwargs):
        return {"passed": True, "findings": [], "checked_traces": 0, "subagent": {}}


class _Recorder:
    """通用检查替身：只记录被抽到的角色，结果恒通过。"""

    def __init__(self, sink):
        self.sink = sink

    def check(self, role, *args, **kwargs):
        self.sink.append(role)
        return {"passed": True, "findings": [], "checked_traces": 0,
                "checked_files": 0, "checked_requirements": 0,
                "subagent": {}, "leverage_count": 0}


def test_sampling_covers_roles_without_criteria():
    """零判据角色也必须被抽到，且同一轮不能重复抽同一角色。

    旧版 weights=可执行判据数 + 有放回：0 判据角色权重 1 被挤掉，同一角色
    还可能占掉两个名额。这里断言行为（抽到了谁），不锁 random.sample 的签名
    —— 签名断言既会误杀等价重构，也测不到「饿死」这个真问题。
    """
    picked: list[str] = []
    seen: set[str] = set()
    with (
        patch.object(ec, "_load_roles", return_value=_ALL_ROLES),
        patch.object(ec, "_run",
                     return_value={"ok": True, "stdout": "active", "stderr": "", "returncode": 0}),
        patch.object(ec, "_write_bus"),
        patch.object(ec, "OutputSemanticChecker", lambda: _Recorder(picked)),
        patch.object(ec, "RequirementConfirmationChecker", lambda: _Recorder(picked)),
        patch.object(ec, "TrajectoryQualityChecker", lambda: _Recorder(picked)),
        # create=True：main 基线还没有这个检查器，副本/旧版上补丁也成立
        patch.object(ec, "LeverageAccumulationChecker", lambda: _Recorder(picked), create=True),
    ):
        for _ in range(20):
            picked.clear()
            ec.run_eval_check()
            assert len(set(picked)) == 2, f"单轮应抽 2 个不同角色: {sorted(set(picked))}"
            seen.update(picked)
    assert "无命令角色" in seen, "零判据角色 20 轮都没被抽到 —— 加权饿死回归"


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


def test_word_boundary_not_substring():
    """判据按词边界比对：'active' 不得被 'inactive' 点亮。"""
    assert ec._has_word("inactive", ["active"]) is False
    assert ec._has_word("active", ["active"]) is True
    assert ec._has_word("服务已 not-approved", ["approved"]) is False


def test_cjk_expected_uses_substring():
    """CJK 没有词边界：'检查通过' 应命中 '通过'，不能被 ASCII 边界误杀。"""
    assert ec._has_word("检查通过", ["通过"]) is True
    assert ec._has_word("不通过", ["通过"]) is False
    assert ec._has_word("未通过", ["通过"]) is False


def test_all_lines_must_satisfy_when_quanbu():
    """带「全部返回」的判据：任一行不满足即 FAIL（旧写法 any() + 子串恒真）。"""
    cmd = "for s in a b; do systemctl --user is-active $s; done"
    ok = {"stdout": "active\nactive\n", "stderr": "", "returncode": 0, "ok": True}
    bad = {"stdout": "active\ninactive\n", "stderr": "", "returncode": 0, "ok": True}
    assert ec._check_passed(ok, cmd + " 全部返回 active", "active")[0] is True
    assert ec._check_passed(bad, cmd + " 全部返回 active", "active")[0] is False


def test_reachable_criteria_counts_only_runtime_window():
    """窗口外的可执行判据不算权数：运行时根本不跑（codex 20 维 F1/N1）。"""
    late = {"name": "late", "eval_criteria": [
        "叙述一", "叙述二", "叙述三", "验证: echo ok 输出 ok"]}
    assert ec._reachable_criteria(late) == 0, "第 4 条判据在窗口外，不贡献权重"
    early = {"name": "early", "eval_criteria": ["验证: echo ok 输出 ok", "叙述"]}
    assert ec._reachable_criteria(early) == 1


def test_weighted_sample_prefers_roles_with_criteria():
    """持判据的角色不能被均匀抽样稀释（maintainer 曾从 31 次/天掉到 7.6 次/天）。"""
    import random as _r
    rich = {"name": "rich", "eval_criteria": ["验证: echo ok 输出 ok"] * 3}
    blind = {"name": "blind", "eval_criteria": ["叙述一", "叙述二"]}
    _r.seed(20261022)
    hits = sum(1 for _ in range(200)
               if ec._weighted_role_sample([rich, blind], 1)[0]["name"] == "rich")
    assert hits >= 120, f"权重 3:1 时应抽到 rich ≥60%，实测 {hits}/200"
    assert ec._weighted_role_sample([rich, blind], 2) != [], "k=2 仍要返回两个不同角色"

def test_systemctl_flags_before_subcommand_are_readonly():
    """systemctl --user is-active X 是只读，但旧实现只看 parts[1] 就判成写操作。

    maintainer 的判据 `systemctl --user is-active a b | grep -c` 因此永久「不兼容」，
    daemon 每 600s 一条 WARNING（10:37 journal），该判据一天都没真跑过。
    """
    assert ec._cmd_is_readonly("systemctl", "systemctl --user is-active sister-agent-dkk.service")
    assert ec._cmd_is_readonly("systemctl", "systemctl is-active x")
    assert not ec._cmd_is_readonly("systemctl", "systemctl --user restart x"), "写操作仍须拒绝"
    assert not ec._cmd_is_readonly("systemctl", "systemctl --now disable x")

def test_word_boundary_not_substring():
    """判据按词边界比对：'active' 不得被 'inactive' 点亮。"""
    assert ec._has_word("inactive", ["active"]) is False
    assert ec._has_word("active", ["active"]) is True
    assert ec._has_word("服务已 not-approved", ["approved"]) is False


def test_all_lines_must_satisfy_when_quanbu():
    """带「全部返回」的判据：任一行不满足即 FAIL（旧写法 any() + 子串恒真）。"""
    cmd = "for s in a b; do systemctl --user is-active $s; done"
    ok = {"stdout": "active\nactive\n", "stderr": "", "returncode": 0, "ok": True}
    bad = {"stdout": "active\ninactive\n", "stderr": "", "returncode": 0, "ok": True}
    assert ec._check_passed(ok, cmd + " 全部返回 active", "active")[0] is True
    assert ec._check_passed(bad, cmd + " 全部返回 active", "active")[0] is False


def test_cjk_expected_uses_substring():
    """CJK 没有词边界：'检查通过' 应命中 '通过'；否定语义要写成负向 expected。"""
    assert ec._has_word("检查通过", ["通过"]) is True
    assert ec._has_word("inactive", ["active"]) is False
