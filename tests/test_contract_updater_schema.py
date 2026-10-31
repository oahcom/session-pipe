#!/usr/bin/env python3
"""apply 后的 eval_criteria 必须仍能被 eval_checker 解析（字符串 schema）。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import contract_updater  # noqa: E402
from eval_shared_loader import _extract_cmd  # noqa: E402


def test_apply_writes_parseable_string(tmp_path, monkeypatch):
    persona = {
        "name": "demo",
        "eval_criteria": ["产出必须落地"],
    }
    f = tmp_path / "persona_demo.json"
    f.write_text(json.dumps(persona), encoding="utf-8")
    monkeypatch.setattr(contract_updater, "PERSONAS_DIR", tmp_path)

    ok = contract_updater._apply_suggestion({
        "role": "demo",
        "criterion": "产出必须落地",
        "type": "add_verification_cmd",
        "verification_command": "echo ok",
        "expected": "ok",
    })
    assert ok is True

    crit = json.loads(f.read_text())["eval_criteria"][0]
    assert isinstance(crit, str)
    assert _extract_cmd(crit) == "echo ok"
    from eval_checker import _extract_cmd as _full
    assert _full(crit) == ("echo ok", "ok")


if __name__ == "__main__":
    import pytest as _p
    sys.exit(_p.main([__file__, "-v"]))
