#!/usr/bin/env python3
"""容器步（有 subflow_template）的 exit_schema 必须能被委派的子链满足。

WHY：子链接管后干活的是子链，父步的 prompt 与 contract 都不再发给角色——
父步要的产出子链从不写，引擎只能每轮记一笔「契约未达成」（bus #438635），
而父链照常 completed。实测 2026-10-22 全量模板 8 个站点是这么挂上去的
（architect_review/s1 s2、food_chain_loop/s1 s7、full_dev_pipeline/s5、
coordinator_schedule/s1 s2、coordinator_standup/s1 s2），其中
architect_review/s1 有 3 个实例把 IMPACT_REPORT.md 记成缺失。

判据：父步 required 里的每个文件名，必须出现在**子链模板**的某一步
prompt_template 或 exit_schema.required 里（glob 跳过：`*.md` 这类不指具体文件）。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_container_contract_satisfiable.py -q
"""
import json
import os
import re
from pathlib import Path

WORKFLOWS = Path(os.environ.get("HERMES_WORKFLOWS_DIR", Path.home() / ".hermes" / "workflows"))
_FILE = re.compile(r"[A-Za-z0-9_\-]+\.(?:md|json|py|txt)")


def _load():
    tpl = {}
    for f in sorted(WORKFLOWS.glob("*.json")):
        try:
            tpl[f.name[:-5]] = json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue  # 坏模板由 template_registry 校验，这里只管契约可达性
    return tpl


def _child_speaks(tpl, child):
    """子链模板里提到过的文件名（prompt + 自己的 required）。"""
    names = set()
    for step in (tpl.get(child) or {}).get("steps", []):
        names.update((step.get("exit_schema") or {}).get("required") or [])
        names.update(_FILE.findall(step.get("prompt_template") or ""))
    return names


def grafted_steps(tpl):
    """返回 [(模板, 步骤, 子链, 子链从不产出的文件名)]。"""
    bad = []
    for name, wf in tpl.items():
        for step in wf.get("steps", []):
            child = step.get("subflow_template")
            req = (step.get("exit_schema") or {}).get("required") or []
            if not child or not req:
                continue
            spoken = _child_speaks(tpl, child)
            miss = [r for r in req if "*" not in r and r not in spoken]
            if miss:
                bad.append((name, step.get("id"), child, miss))
    return bad


def test_container_contract_is_reachable():
    bad = grafted_steps(_load())
    assert not bad, "容器步要的产出子链从不写（要么去掉 subflow_template，要么让子链产出）:\n  " + "\n  ".join(
        f"{n}/{s} → {c} 从不产出 {m}" for n, s, c, m in bad)


def test_checker_catches_synthetic_graft():
    """反例自检：判据必须能抓到一个刚挂上去的、不可达的容器契约。"""
    tpl = {
        "parent": {"steps": [{"id": "s1", "subflow_template": "child",
                              "exit_schema": {"required": ["SCHEDULE.md"]}}]},
        "child": {"steps": [{"id": "s1", "prompt_template": "写 DESIGN.md"}]},
    }
    bad = grafted_steps(tpl)
    assert bad and bad[0][3] == ["SCHEDULE.md"], bad


def test_checker_accepts_reachable_contract():
    """反例的反面：子链 prompt 提到该文件 → 不算错挂。"""
    tpl = {
        "parent": {"steps": [{"id": "s1", "subflow_template": "child",
                              "exit_schema": {"required": ["SCHEDULE.md"]}}]},
        "child": {"steps": [{"id": "s1", "prompt_template": "写 SCHEDULE.md"}]},
    }
    assert grafted_steps(tpl) == []
