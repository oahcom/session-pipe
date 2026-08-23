#!/usr/bin/env python3
"""test_cron_scheduler.py — CronScheduler 生存方法测试覆盖。

TestParseInterval 已删除 — _parse_interval 已剪枝死代码。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

if __name__ == "__main__":
    import pytest as _p
    import sys as _s
    _s.exit(_p.main([__file__, "-v"]))
