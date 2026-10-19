#!/usr/bin/env python3
"""exit_schema 互斥判据必须是 OR（mustContainAnyOf），不得用 AND（mustContain）。

生产事故：food_chain_loop/s7 要求 VALUE_AUDIT.md 同时含 'PASS' 与 'FAIL'
（mustContain 是 AND 语义），而模板 prompt 规定"第一行必须为 PASS **或** FAIL"。
一份正确的审计文件只可能有一种结论 → 校验数学上必然失败，父步契约永远达不成，
引擎每轮写 blocker（wf_5f5eebc881/s7，bus #419376）。同类缺陷 3 模板：
 - food_chain_loop(/WL-)/s7 VALUE_AUDIT.md ['PASS','FAIL']
 - architect_full_design(/WF-)/s4 GATE_REPORT.md ['通过','失败']
 - architect_review(/WF-)/s2 REVIEW.md ['APPROVED','REJECTED','严重度']

修法：互斥结论词改 mustContainAnyOf；仍恒需的正向词（如'严重度'）留在 mustContain。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_exit_schema_verdict_anyof.py -q
"""
import json
from pathlib import Path

WORKFLOWS = Path.home() / ".hermes" / "workflows"

# 互斥结论词对：同一 mustContain 列表内出现一正一负 = 自相矛盾（AND 下必然失败）
POS = {"PASS", "通过", "APPROVED", "成功", "满足"}
NEG = {"FAIL", "失败", "REJECTED", "拒绝", "不通过"}


def _schemas():
    """遍历所有模板 JSON，yield (模板名, 步骤 id, 文件名, props)。"""
    for f in sorted(WORKFLOWS.glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        for step in data.get("steps", []):
            for fname, props in ((step.get("exit_schema") or {}).get("properties") or {}).items():
                yield f.name, step.get("id") or step.get("step_id"), fname, props


def test_no_contradictory_must_contain():
    """mustContain 内不得同时出现正向与负向结论词（AND 语义下必失败）。"""
    bad = []
    for tpl, sid, fname, props in _schemas():
        mc = props.get("mustContain") or []
        pos = [w for w in mc if w in POS]
        neg = [w for w in mc if w in NEG]
        if pos and neg:
            bad.append(f"{tpl}/{sid} {fname}: mustContain={mc}")
    assert not bad, "互斥结论词留在 AND 语义 mustContain 中（应改 mustContainAnyOf）:\n  " + "\n  ".join(bad)


def test_verdict_sites_use_anyof():
    """三个已知站点必须显式使用 mustContainAnyOf（防回退成 AND）。"""
    expected = {
        ("food_chain_loop.json", "s7", "VALUE_AUDIT.md"): {"PASS", "FAIL"},
        ("architect_full_design.json", "s4", "GATE_REPORT.md"): {"通过", "失败"},
        ("architect_review.json", "s2", "REVIEW.md"): {"APPROVED", "REJECTED"},
    }
    seen = {}
    for tpl, sid, fname, props in _schemas():
        key = (tpl, sid, fname)
        if key in expected:
            seen[key] = set(props.get("mustContainAnyOf") or [])
    for key, want in expected.items():
        assert key in seen, f"站点缺失: {key}"
        assert seen[key] == want, f"{key} mustContainAnyOf={seen[key]} != {want}"
