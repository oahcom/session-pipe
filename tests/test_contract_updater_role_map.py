#!/usr/bin/env python3
"""表征測試: contract_updater 必須能從 review_gate(target=文件) 與 eval_passrate(role_filter=中/英)
正確映射到 persona name, 讓 add_output_schema 建議對 31 persona 真生效。

鎖定回歸: 輪3 修復後從 review_gate 提取的 role=文件名(user_intent_reminder.py),
_has_output_schema 按文件名查 persona 失敗 → add_output_schema 對 persona 無效。
eval_passrate 的 role_filter 含中文 title(维护者) 與英文(engineer), 需映射回 name。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import contract_updater as cu


def test_resolve_role_from_filename():
    # review_gate target 是文件, 需映射到所屬角色
    # user_intent_reminder.py 屬於 launcher, 但 persona 無文件歸屬 → 應回退文件名(而非崩潰)
    r = cu._resolve_role_for_suggestion({"target": "session-launcher/src/user_intent_reminder.py"})
    assert r, "文件名應作為回退角色標識"
    print(f"  [ok] _resolve_role_from_filename -> {r}")


def test_resolve_role_from_eval_passrate():
    # eval_passrate role_filter 中文 title 應映射到 persona name
    r = cu._resolve_role_for_suggestion({"role_filter": "维护者"})
    assert r == "maintainer", f"中文 title 應映射回 maintainer, 得 {r}"
    r2 = cu._resolve_role_for_suggestion({"role_filter": "engineer"})
    assert r2 == "engineer", f"英文應直過, 得 {r2}"
    print(f"  [ok] _resolve_role_from_eval_passrate -> {r}, {r2}")


def test_add_output_schema_reaches_persona():
    # 映射後 add_output_schema 建議應對真實 persona 生效(不再 unknown/文件名)
    hist = [{"role_filter": "维护者"}]
    sugs = cu.suggest_improvements(hist)
    roles = {s["role"] for s in sugs}
    assert "maintainer" in roles, f"建議應含 maintainer, 得 {roles}"
    assert "unknown" not in roles, "不應有 unknown"
    print(f"  [ok] add_output_schema -> roles={roles}")


if __name__ == "__main__":
    test_resolve_role_from_filename()
    test_resolve_role_from_eval_passrate()
    test_add_output_schema_reaches_persona()
    print("ALL PASS")
