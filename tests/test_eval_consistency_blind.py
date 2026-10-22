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

def test_criteria_outside_runtime_window_not_counted(monkeypatch, tmp_path, capsys):
    """窗口外的判据永远不执行：第 5 条才可执行 ≠ 非盲区（codex 20 维 F1）。

    knowledge_curator 实测：5 条判据里唯一可执行的在第 4 条（exec_idx=[4]），
    旧口径按全表统计 → 判它非盲区，运行时却零判据执行。
    """
    monkeypatch.setattr(ec, "_load_roles", lambda d=None: [
        {"name": "late_exec",
         "eval_criteria": ["叙述一", "叙述二", "叙述三", "叙述四", "验证: echo ok 输出 ok"],
         "issues": []},
    ])
    ec.main(["--json", "--personas", str(tmp_path)])
    out = json.loads(capsys.readouterr().out)
    assert out["blind_roles"] == ["late_exec"], "第 5 条判据运行时跑不到，必须算盲区"
    assert out["exec_total"] == 0


def test_blind_roles_are_not_in_last_two_lines(monkeypatch, tmp_path, capsys):
    """反例锁：旧 daemon 取 splitlines()[-2:] 拿不到名单（实测恒为 details/}）。

    谁把 routing_daemon 改回 [-2:]，这条说明它看不到盲区 —— 必须解析 JSON。
    """
    monkeypatch.setattr(ec, "_load_roles", lambda d=None: _roles())
    ec.main(["--json", "--personas", str(tmp_path)])
    out = capsys.readouterr().out
    assert "blind_roles" not in "\n".join(out.strip().splitlines()[-2:])
    assert json.loads(out)["blind_roles"] == ["prose_only", "none"]
