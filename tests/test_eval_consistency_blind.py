#!/usr/bin/env python3
"""eval_consistency 盲区可见化：零可执行判据的角色必须按名字列出。

反向验证：把 exec_count 改回「只看 criteria_count」，test_blind_roles_named 必须变红
（见 README 的 mutation 命令）。
"""
import json
import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import eval_consistency as ec


def _roles():
    return [
        {"name": "prose_only", "eval_criteria": ["服务正常运行", "无报错"], "issues": []},
        {"name": "has_exec", "eval_criteria": ["验证: echo ok 输出 ok"], "issues": []},
        {"name": "none", "eval_criteria": [], "issues": []},
    ]


def test_blind_roles_named(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(ec, "_load_roles", lambda d=None: _roles())
    rc = ec.main(["--json", "--personas", str(tmp_path)])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["blind_roles"] == ["prose_only", "none"]
    assert out["exec_total"] == 1


def test_incompatible_criterion_not_counted_as_executable(monkeypatch, tmp_path, capsys):
    """能抽出命令但预检拒绝，不能算可执行 —— 否则盲区名单假绿。"""
    monkeypatch.setattr(ec, "_load_roles", lambda d=None: [
        {"name": "rejected", "eval_criteria": ["验证: rm -rf / 输出 ok"], "issues": []},
    ])
    monkeypatch.setattr(ec, "_extract_cmd", lambda c: "rm -rf /")
    monkeypatch.setattr(ec, "_check_cmd", lambda cmd: ["不在白名单"])
    rc = ec.main(["--json", "--personas", str(tmp_path)])
    out = json.loads(capsys.readouterr().out)
    assert rc == 1, "不兼容仍返回 1（旧语义不变）"
    assert out["blind_roles"] == ["rejected"]
    assert out["incompatible_roles"] == 1
