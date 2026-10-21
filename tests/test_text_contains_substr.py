#!/usr/bin/env python3
"""text_contains 匹配语义回归。

背景（2026-10-19 实证）：曾把 exit_condition.text_contains 从子串匹配改成
词边界 lookaround，理由是"防止一条消息同时命中 s2/s3 两个门控"。该改动被打回，
两条证据：
  (1) dual_node 宽匹配用 re.sub(r"-complete$", "-") 生成 filter（如 "s1-"），
      依赖子串语义命中 "s1-complete"/"s1-codex-approved"。词边界的 (?!\\w)
      被 filter 末尾的 "-" 之后那个 "c" 触发 → node2 消息恒不命中 →
      food_chain_loop/WL-food_chain_loop 的 dual_node 步骤永久卡死。
  (2) 词边界并不拦跳步：真实消息「security_audit_scan/s2-complete s3-complete
      — …」确实同时含两个标记，两种语义下都双命中。防跳步靠 _check_exit 的
      created_after 屏障，不靠匹配算法。
      （注：原表述为「屏障 272 实例满足、0 违反」，2026-10-19 更正：该基准是引擎
       自己记录的 bus_anchor，属循环论证。屏障实际可被穿越——见
       test_barrier_birth_time.py 与 memory/bus-ts-mutable-barrier-bypass。）
"""
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path.home() / "session-pipeline" / "src"))
sys.path.insert(0, str(Path.home() / "session-pipeline" / "src/pipeflow"))

from pipeflow.engine import WorkflowEngine


def _fact(mid: str, text: str, ts: float, src: str = "qa"):
    return type("F", (), {"id": mid, "t": text, "ts": ts, "src": src})()


def _eng(facts):
    with patch.object(WorkflowEngine, "__init__", lambda self: None):
        eng = WorkflowEngine()
    eng._bb = MagicMock()
    eng._bb.read = MagicMock(return_value=facts)
    return eng


class TestTextContainsSubstringSemantics(unittest.TestCase):
    """dual_node 宽匹配（filter 以 "-" 结尾）必须靠子串语义命中。"""

    def test_wide_filter_hits_complete_marker(self):
        """宽匹配 "s1-" 命中 "s1-complete"（node1 完成消息）。"""
        ts = time.time()
        eng = _eng([_fact("m1", "food_chain_loop/s1-complete — 完成", ts)])
        _ts, msgs = eng._check_exit("test_report", "", "s1-", created_after=ts - 10)
        self.assertTrue(_ts, "宽匹配 s1- 必须命中 s1-complete，否则 dual_node node2 永久卡死")
        self.assertEqual(len(msgs), 1, "应匹配到 1 条消息")

    def test_wide_filter_hits_codex_approved(self):
        """宽匹配 "s1-" 命中 "s1-codex-approved"（node2 完成消息）。"""
        ts = time.time()
        eng = _eng([_fact("m2", "s1-codex-approved — codex 审查通过", ts)])
        _ts, msgs = eng._check_exit("test_report", "", "s1-", created_after=ts - 10)
        self.assertTrue(_ts, "宽匹配 s1- 必须命中 s1-codex-approved")
        self.assertEqual(len(msgs), 1, "应匹配到 1 条消息")

    def test_full_slash_prefix_marker_hits(self):
        """带工作流前缀的完整标记（如 food_chain_loop/s2-complete）必须被裸 filter 命中。"""
        ts = time.time()
        eng = _eng([_fact("m3", "food_chain_loop/s2-complete — 已交付", ts)])
        _ts, _ = eng._check_exit("test_report", "", "s2-complete", created_after=ts - 10)
        self.assertTrue(_ts, "子串语义下带前缀标记应命中裸 filter（'/' 前后是标记边界）")

    def test_unrelated_message_not_matched(self):
        """不含标记的消息不得命中（反向保护）。"""
        ts = time.time()
        eng = _eng([_fact("m4", "无关消息：本轮无完成标记", ts)])
        _ts, msgs = eng._check_exit("test_report", "", "s2-complete", created_after=ts - 10)
        self.assertFalse(_ts, "不含标记的消息不应命中")
        self.assertEqual(msgs, [], "不应返回匹配消息")


if __name__ == "__main__":
    unittest.main()
