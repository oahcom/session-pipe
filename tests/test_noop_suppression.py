#!/usr/bin/env python3
"""test_noop_suppression.py — no-op 健康报告抑制（DESIGN §9）。

覆盖：
1. 措辞命中：no-op 报告（无职责内修复动作）→ _is_noop_report True（不 mint）
2. 真修复放行：真修复报告 → _is_noop_report False
   （原 "状态无 delta → _noop_delta" 用例已随死代码 _noop_delta 一起删：合并后
    src 侧 0 调用方，main 剪枝把它删了；保住测试等于给死代码续命）
4. 冷却/放行边界：泛词（无异常）不在 mint 抑制子集 → 不误杀
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture
def scheduler(tmp_path, monkeypatch):
    """隔离 idle-state 文件，避免污染生产 ~/.hermes/run/cron_idle_state.json。

    patch 必须覆盖整个用例而不只是构造：_noop_delta/report_idle 调
    _save_idle_state() 时才读模块全局 _IDLE_STATE_PATH（实测每轮用例往生产
    文件写 2 次）。
    """
    import cron_scheduler as cs
    monkeypatch.setattr(cs, "_IDLE_STATE_PATH", tmp_path / "idle.json")
    monkeypatch.setattr(cs, "PERSONAS_DIR", tmp_path / "nonexistent")
    return cs.CronScheduler()


def test_noop_report_phrase_hit():
    """DESIGN §9.1：'无职责内修复动作' 报告 → _is_noop_report True。"""
    from cron_scheduler import _is_noop_report
    assert _is_noop_report("[修复] 无异常核验: 服务全绿零僵尸, 无职责内修复动作") is True
    assert _is_noop_report("任意标题", "结论: 服务健康无需维护动作") is True
    # 回归(2026-10-17)：措辞变体须全覆盖——"服务健康零僵尸"匹配不到"服务全绿零僵尸"
    # (全绿≠健康)，改用核心子串"零僵尸"，三类真实标题不再 MISS
    assert _is_noop_report("[修复] 无异常核验: 服务全绿零僵尸; 幂等确认") is True
    assert _is_noop_report("[修复] 无异常核验(no-op非真修复): 服务全绿零僵尸") is True
    assert _is_noop_report("[maintainer] 健康全绿零僵尸(幂等核验)") is True
    # 反例：合法任务标题不含"零僵尸"，不得误杀
    assert _is_noop_report("修复无异常处理逻辑") is False
    print("✅ no-op 措辞命中 ✓")


def test_real_fix_passes():
    """DESIGN §9.3：真修复报告 → 非 no-op → 放行。"""
    from cron_scheduler import _is_noop_report
    _fix = "修复 routing 死锁: 门控误锚跨工作流失败判定, 改动 step_handler"
    assert _is_noop_report(_fix) is False, "真修复不命中 no-op"
    print("✅ 真修复放行 ✓")


def test_generic_words_not_over_suppressed():
    """DESIGN §9.4：泛词（'无异常'）不在 mint 抑制子集 → 含该词的合法任务不被误杀。"""
    from cron_scheduler import _is_noop_report, _NOOP_PATTERNS
    assert "无异常" not in _NOOP_PATTERNS, "泛词不得进 mint 抑制子集"
    assert _is_noop_report("[修复] 修复无异常处理逻辑导致进程退出") is False
    print("✅ 泛词不误杀 ✓")


if __name__ == "__main__":
    import pytest as _p
    sys.exit(_p.main([__file__, "-v"]))
