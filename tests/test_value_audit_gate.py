"""Unit tests for _value_audit_failed — food_chain_loop 第3轮死锁修复。

背景（blocker #292530）：旧逻辑 `"FAIL" in _text.upper()` 把"说明 FAIL 情形"的
模板文字误判为结论 FAIL，导致循环永久停在第 3 轮。新函数仅认结构化结论
（首行 / verdict: 字段）。
"""
import sys
from pathlib import Path

_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

import unittest
from pipeflow.wf_lifecycle import _value_audit_failed


class TestValueAuditGate(unittest.TestCase):
    def test_首行判PASS_不误伤模板文字(self):
        # lr/VALUE_AUDIT.md 真实形态：首行 PASS，含"反向判定(FAIL情形)"说明段
        _md = (
            "PASS\n\n"
            "# 价值审计\n\n"
            "## 独立判断\n本轮判定 PASS（有收益）——非 FAIL。\n\n"
            "## 反向判定（FAIL 情形）\n"
            "若证据不成立则判 FAIL 并停止循环。\n"
        )
        self.assertFalse(_value_audit_failed(_md),
                         "含 FAIL 说明段的 PASS 文档不应判为失败")

    def test_首行FAIL_停循环(self):
        self.assertTrue(_value_audit_failed("FAIL\n\n原因：不可运行\n"))

    def test_首行REJECT_停循环(self):
        self.assertTrue(_value_audit_failed("REJECT\n\n无收益\n"))

    def test_verdict字段FAIL_停循环(self):
        _md = "本轮审计完成\nverdict: FAIL\n"
        self.assertTrue(_value_audit_failed(_md))

    def test_verdict字段PASS_不停(self):
        _md = "本轮审计完成\nverdict: PASS\n含 FAIL 字样说明段\n"
        self.assertFalse(_value_audit_failed(_md),
                         "显式 verdict:PASS 应优先于正文 FAIL 字样")

    def test_空文本_不停(self):
        self.assertFalse(_value_audit_failed(""))

    def test_含FAIL正文但无结论_不停(self):
        # 仅正文出现 FAIL，首行与 verdict 均无结论标记
        _md = "审计进行中\n记录了若干 FAIL 风险点，待评估\n"
        self.assertFalse(_value_audit_failed(_md))


if __name__ == "__main__":
    unittest.main()
