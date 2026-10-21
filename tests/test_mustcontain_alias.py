#!/usr/bin/env python3
"""mustContain 中英别名兜底（bus #413783）。

模板写 mustContain=['passed','failed','coverage']，qa 报告用中文
「通过 17 / 失败 1 / 覆盖率 82%」时旧实现判缺内容 → 合格报告被门禁挡下，
6 个模板受影响。引擎侧按别名判定，模板无需逐条改动。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.step_handler import _token_hit


def test_english_tokens_still_work():
    """英文原文保持命中（不因加别名而回归）。"""
    c = "PASS=17 FAIL=1 coverage=82%"
    assert _token_hit("passed", c) and _token_hit("failed", c) and _token_hit("coverage", c)


def test_chinese_report_hits():
    """纯中文报告：通过/失败/覆盖率 分别满足三个 token。"""
    c = "本轮 17 项通过，1 项失败，覆盖率 82%。"
    assert _token_hit("passed", c)
    assert _token_hit("failed", c)
    assert _token_hit("coverage", c), "覆盖率 应视为 coverage 命中"


def test_missing_token_still_fails():
    """确实缺失时不得放行（别名不能变成万能通过）。"""
    c = "本轮全部通过，覆盖率 82%。"
    assert _token_hit("failed", c) is False, "没有失败字样却含'通过'不应命中 failed"


def test_unknown_token_is_plain_substring():
    """未登记别名的 token 退回原子串语义（不误伤自定义词）。"""
    assert _token_hit("ADR", "含 ADR 章节") is True
    assert _token_hit("ADR", "无技术选型") is False


if __name__ == "__main__":
    for _name, _fn in list(globals().items()):
        if _name.startswith("test_"):
            _fn()
    print("SELF-TEST PASS（4 例）")
