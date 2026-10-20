#!/usr/bin/env python3
"""eval 静态预检必须与运行时执行器同口径。

此前 eval_consistency 自带一份 _extract_cmd/_check_cmd 副本，且模块级
`from eval_checker import _ALLOWED_CMDS` 会触发循环导入回退到 51 项的静态表
（真表 29 项）——同一批 persona 在静态检查报 10 项不兼容、运行时只拒 3 项，
其中 7 项是引号内 '|' 与 for 首词被当成管道/命令的误报。

运行: pytest tests/test_eval_consistency_parity.py -q
"""
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import eval_checker as ec  # noqa: E402
import eval_shared_loader as esl  # noqa: E402


def test_shared_checker_is_preflight():
    """静态检查器与运行时共用同一个 _preflight，不得各写一份。"""
    import eval_consistency
    assert eval_consistency._check_cmd is esl._check_cmd


def test_whitelist_independent_of_import_order():
    """白名单不得依赖导入顺序（循环导入回退曾导致两套表）。"""
    assert esl._check_cmd("df -h / | head -1") == []


def test_quoted_pipe_not_treated_as_pipeline():
    """grep 模式里的 '|' 不是管道，不得判为不兼容。"""
    assert esl._check_cmd("""journalctl --user -u x | grep -cE "ERROR|Traceback" """) == []


def test_for_loop_is_allowed_by_preflight():
    """for 循环骨架是白名单内命令的组合，运行时本就会执行。"""
    assert esl._check_cmd("for s in a b; do systemctl --user is-active $s; done") == []


def test_rejections_match_runtime():
    """静态判为不兼容的命令，运行时也必须拒绝（同口径）。"""
    cases = [
        "pytest -x --tb=short",                       # 不在白名单
        'echo "PASS"',                                # 恒真
        'python3 -c "open(\'x\').read()"',            # 沙箱阻断 API
    ]
    for cmd in cases:
        issues = esl._check_cmd(cmd)
        with_preflight = ec._preflight(cmd)[1]
        assert issues and with_preflight is not None, f"未拒绝: {cmd}"
        assert issues[0] == with_preflight["stderr"], f"口径不一致: {cmd}"


def test_extract_shared_between_modules():
    """命令提取也必须是单一实现。"""
    criterion = "判据 | 验证: ps aux 输出 active"
    assert esl._extract_cmd(criterion) == ec._extract_cmd(criterion)[0]
