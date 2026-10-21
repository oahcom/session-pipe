#!/usr/bin/env python3
"""_cmd_is_readonly 的 systemctl 判定：选项必须跳过，写动词必须拒。

缺陷实证（2026-10-22 生产日志 rc=1）：maintainer 的 eval 判据写
systemctl --user is-active <svc> | grep -c ^active$，
旧实现取 parts[1] = "--user" 当作子命令 → 报"写操作被拒绝" → 该判据永远跑不了。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from eval_checker import _cmd_is_readonly


def test_user_flag_before_readonly_verb_is_allowed():
    assert _cmd_is_readonly("systemctl", "systemctl --user is-active a.service b.service")


def test_short_flag_before_readonly_verb_is_allowed():
    assert _cmd_is_readonly("systemctl", "systemctl -q is-active a.service")


def test_write_verb_still_rejected():
    for cmd in ("systemctl --user start a.service",
                "systemctl --user --now enable a.service",
                "systemctl stop a.service"):
        assert not _cmd_is_readonly("systemctl", cmd), cmd


def test_value_taking_flag_fails_closed():
    # -n 取一个值：值会被当成子命令 → 拒绝。方向安全（只读判据可改写，写操作绝不能放行）。
    assert not _cmd_is_readonly("systemctl", "systemctl -n 20 status a.service")
