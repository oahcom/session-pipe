#!/usr/bin/env python3
"""self-check: task_evidence 的产出评分闸门（直接测生产判据，不复刻规则）。

旧版在本文件里复刻了一遍判定分支 —— 生产代码顺序改回旧版也全绿，闸门失效。
现在只调 task_evidence._classify_outputs。

Run: python3 -m pytest tests/test_task_evidence_quality_gate.py -x -q
"""
import importlib.util
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


te = _load_module("task_evidence", _SRC / "task_evidence.py")


def test_exit_messages_presence_not_positive():
    assert te._classify_outputs(0, 0, "exit_messages", False) == "neutral"


def test_summary_presence_not_positive():
    assert te._classify_outputs(0, 0, "summary", False) == "neutral"


def test_real_output_file_is_positive():
    assert te._classify_outputs(2, 3000, "", False) == "positive"


def test_no_evidence_is_negative():
    assert te._classify_outputs(0, 0, "", False) == "negative"
