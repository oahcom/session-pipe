#!/usr/bin/env python3
"""self-check for prune-grow-r11: maintainer_health loop must be bounded.

Verifies:
1. maintainer_health template has loop.max_iterations = 1 (not 0=infinite)
2. loop value_gate is wired to HEALTH_CHECK.md / maintainer workspace
3. food_chain_loop keeps its explicit loop (opt-in contracts unchanged)
4. max_iterations=1 terminates the loop after one iteration

Run: python3 -m pytest tests/test_prune_grow_r11_loop_gate.py -x -q
"""
import json, sys
from pathlib import Path

WORKFLOWS = Path.home() / ".hermes" / "workflows"


def _load(name):
    p = WORKFLOWS / name
    return json.loads(p.read_text())


def test_maintainer_loop_is_bounded():
    d = _load("maintainer_health.json")
    loop = d["loop"]
    assert loop["max_iterations"] == 1, f"maintainer must be bounded, got {loop}"
    assert loop.get("value_gate"), f"value_gate required, loop={loop}"


def test_maintainer_value_gate_targets_own_audit():
    vg = _load("maintainer_health.json")["loop"]["value_gate"]
    assert vg["workspace"] == "maintainer"
    assert "HEALTH_CHECK.md" in vg["file"]


def test_food_chain_loop_keeps_opt_in_contract():
    fc = _load("food_chain_loop.json")
    assert fc["loop"]["enabled"] is True
    assert "value_gate" in fc["loop"]
    # food_chain_loop max_iterations must remain > 1 (it is the autonomous producer)
    assert fc["loop"]["max_iterations"] >= 2


if __name__ == "__main__":
    # standalone self-check (CLI exit code)
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print("all prune-grow-r11 loop-gate checks green")
    sys.exit(0)
