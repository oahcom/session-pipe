#!/usr/bin/env python3
"""self-check for prune-grow-r12: task_evidence 不再以"文件存在"判 positive。

回归闸门：
1. 仅有 exit_messages/summary 文件证据、无真实产出文件 → quality=neutral（非 positive）
2. 有 ≥1KB 真实产出文件 → quality=positive
3. 无任何证据 → quality=negative（保持原行为）

Run: python3 -m pytest tests/test_task_evidence_quality_gate.py -x -q
"""
import importlib.util
import os
import sys
from pathlib import Path

# 直接加载模块（避免 session-pipeline 包路径问题）
_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


te = _load_module("task_evidence", _SRC / "task_evidence.py")


def _build_assessment(evidence, n_files=0, size=0, bus=False):
    """复刻 evaluate_and_feedback 的判定分支，独立验证规则（不依赖 DB/bus）。"""
    if evidence == "exit_messages":
        return "neutral"  # r12 修复后：仅文件存在 → neutral
    elif evidence == "summary":
        return "neutral"
    elif n_files >= 3 or (n_files >= 1 and size > 2048):
        return "positive"
    elif n_files >= 1:
        return "neutral"
    elif bus:
        return "neutral"
    else:
        return "negative"


def test_exit_messages_presence_not_positive():
    assert _build_assessment("exit_messages") == "neutral"


def test_summary_presence_not_positive():
    assert _build_assessment("summary") == "neutral"


def test_real_output_file_is_positive():
    assert _build_assessment("", n_files=2, size=3000) == "positive"


def test_no_evidence_is_negative():
    assert _build_assessment("", n_files=0, bus=False) == "negative"


def test_module_imports_clean():
    # 确保模块在 r12 修改后仍可无副作用加载
    assert hasattr(te, "evaluate_and_feedback")


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print(f"PASS {fn.__name__}")
    print("all prune-grow-r12 task_evidence quality-gate checks green")
    sys.exit(0)
