#!/usr/bin/env python3
"""eval 判据窗口按「可执行」取，纯文本判据不再挤掉唯一验证命令。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from eval_shared_loader import select_window  # noqa: E402


def test_window_skips_prose_to_reach_executable():
    crit = ["说明一", "说明二", "说明三", "说明四", "产出 | 验证: echo ok"]
    assert select_window(crit, 3) == [crit[4]]


def test_window_caps_executable_count():
    crit = [f"c{i} | 验证: echo ok" for i in range(5)]
    picked = select_window(crit, 3)
    assert picked == crit[:3]


if __name__ == "__main__":
    import pytest as _p
    sys.exit(_p.main([__file__, "-v"]))
