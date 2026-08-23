#!/usr/bin/env python3
"""表征测试: contract_updater 必须能从 review_gate 真实 schema 提取角色并生成有效建议。

锁定回归: 旧实现读 entry.get("role")，但 review_gate history 无 role 字段，
导致 31 角色全缺 output_schema 却只产生 1 条 role=unknown 的无效建议。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import contract_updater as cu


def _make_history(target: str, with_schema: bool) -> dict:
    """模拟一条 review_gate history 记录（真实 schema）。"""
    return {
        "verdict": "BLOCK",
        "target": target,
        "target_type": "file",
        "round_id": "1",
        "dims": json.dumps([
            {"dim_id": 1, "status": "WARN", "parties": ["codex"]},
            {"dim_id": 9, "status": "FAIL", "parties": ["probe"]},
        ]),
        "retry_reasons": "[1, 9]",
        "summary": "verdict=BLOCK",
    }


def test_parties_role_resolves():
    # review_gate 真实角色在 dims[].parties, 应优先解析而非文件名回退
    hist = _make_history("/home/administrator/session-launcher/src/user_intent_reminder.py", False)
    role = cu._parties_role(hist)
    assert role == "codex-dev", f"parties[codex] 应部分匹配 codex-dev, 得 {role}"
    print(f"  [ok] _parties_role -> {role}")


def test_resolve_role_prefers_parties_over_filename():
    # 文件 target 无 persona 归属, 但 parties 有真角色 → 不应误告警为文件映射缺失
    hist = _make_history("session-launcher/src/user_intent_reminder.py", False)
    role = cu._resolve_role_for_suggestion(hist)
    assert role == "codex-dev", f"应取 parties 角色而非文件名, 得 {role}"
    print(f"  [ok] _resolve_role_for_suggestion -> {role}")


def test_suggest_from_reviewgate_schema():
    # 用真实 persona 名作为 target 影射 (coordinator 存在)
    hist = _make_history("coordinator", False)
    sug = cu.suggest_improvements([hist])
    assert len(sug) >= 1, "review_gate schema 下应能产生建议"
    assert sug[0]["role"] != "unknown", f"建议 role 不应为 unknown: {sug[0]}"
    print(f"  [ok] suggest_improvements -> {len(sug)} 条, role={sug[0]['role']}")


if __name__ == "__main__":
    test_parties_role_resolves()
    test_resolve_role_prefers_parties_over_filename()
    test_suggest_from_reviewgate_schema()
    print("ALL PASS")
