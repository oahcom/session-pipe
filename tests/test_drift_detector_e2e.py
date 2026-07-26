#!/usr/bin/env python3
"""test_drift_detector_e2e.py — 漂移检测器端到端测试"""

import json
import sys
from pathlib import Path
from unittest import mock

_src = str(Path(__file__).resolve().parents[1] / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import pytest
from drift_detector import detect_drift, detect_all_drift


class FakeFact:
    """模拟 Blackboard Fact 对象。"""
    def __init__(self, cat, text, ts="2026-07-26T00:00:00"):
        self.cat = cat
        self.t = text
        self.ts = ts


CONFIG_VERIFIER = {
    "name": "verifier",
    "output_targets": ["cat=review", "cat=approve", "cat=summary"],
}

CONFIG_REBUTTER = {
    "name": "rebutter",
    "output_targets": ["cat=argument", "cat=rebuttal"],
}


def test_detect_drift_no_drift():
    """正常角色 output_targets 匹配时无漂移"""
    with mock.patch("drift_detector._load_role_config", return_value=CONFIG_VERIFIER):
        with mock.patch("drift_detector.Blackboard") as mock_bb:
            mock_bb.return_value.read.return_value = [
                FakeFact("review", "review content"),
                FakeFact("approve", "approve content"),
                FakeFact("summary", "summary content"),
                FakeFact("architecture", "always allowed"),
                FakeFact("notice", "always allowed"),
            ]
            result = detect_drift("verifier")
    assert result == []


def test_detect_drift_with_drift():
    """角色产出超 range 时有漂移"""
    with mock.patch("drift_detector._load_role_config", return_value=CONFIG_VERIFIER):
        with mock.patch("drift_detector.Blackboard") as mock_bb:
            mock_bb.return_value.read.return_value = [
                FakeFact("review", "ok"),
                FakeFact("malware_scan", "unexpected category"),
            ]
            result = detect_drift("verifier")
    assert len(result) == 1
    assert result[0]["category"] == "malware_scan"
    assert "unexpected" in result[0]["text_preview"]


def test_detect_all_drift(tmp_path):
    """多角色时调用不崩溃，正确区分有/无漂移角色"""
    sentinel_dir = tmp_path / "sentinels"
    sentinel_dir.mkdir()
    (sentinel_dir / "verifier.json").write_text(json.dumps({"role": "verifier"}))
    (sentinel_dir / "rebutter.json").write_text(json.dumps({"role": "rebutter"}))

    config_map = {"verifier": CONFIG_VERIFIER, "rebutter": CONFIG_REBUTTER}

    with mock.patch("drift_detector.CCS_SENTINEL_DIR", sentinel_dir):
        with mock.patch("drift_detector._load_role_config",
                        side_effect=lambda r: config_map[r]):
            with mock.patch("drift_detector.Blackboard") as mock_bb:
                # sentinel 排序: rebutter.json < verifier.json
                mock_bb.return_value.read.side_effect = [
                    [FakeFact("argument", "ok")],           # rebutter: no drift
                    [FakeFact("random_cat", "drift content")],  # verifier: drift
                ]
                result = detect_all_drift()
    assert "verifier" in result
    assert len(result["verifier"]) == 1
    assert result["verifier"][0]["category"] == "random_cat"
    assert "rebutter" not in result
