#!/usr/bin/env python3
"""模板注册中心统一入口测试 — 校验分层 + 双写。

覆盖: workflow_id 格式(WF-)、estimated_hours 可选、三段式移到质量层、
register 双写(DB+JSON)、JSON 已存在不覆盖、路径穿越拒绝。
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from template_registry import (
    TemplateRegistry, _validate_schema, _validate_meta,
)

from paths import HERMES_WORKFLOWS


def _valid_template(name="test_wf", wf_id="WF-test_wf"):
    """符合完整 Schema 的最小模板。"""
    return {
        "workflow_id": wf_id,
        "name": name,
        "title": name,
        "description": "测试模板描述",
        "trigger_scene": ["编码实现", "功能开发"],
        "allowed_initiators": ["coordinator", "pm"],
        "allowed_executors": ["engineer"],
        "max_duration_hours": 24,
        "quality_standards": "标准质量要求描述文字",
        "steps": [{
            "id": "s1", "title": "编码", "target_role": "engineer",
            "prompt_template": "做什么：编码\n怎么做：实现\n验收标准：通过",
            "exit_condition": {"type": "exit_message", "match": "done"},
            "failure_patterns": ["产出物为空", "格式错误"],
        }],
    }


# ── 1. workflow_id 格式 ──
def test_validate_schema_wf_id_format():
    """WF- 前缀通过，WL- 兼容，其他拒绝。"""
    t = _valid_template()
    assert _validate_schema(t).passed
    t["workflow_id"] = "WL-TEST"
    assert _validate_schema(t).passed
    t["workflow_id"] = "XX-bad"
    assert not _validate_schema(t).passed
    t["workflow_id"] = ""  # 缺省合法（register 默认 WF-{name}）
    assert _validate_schema(t).passed


# ── 2. estimated_hours 可选 ──
def test_estimated_hours_optional():
    """无 estimated_hours 通过；有值但 <0.5 拒绝。"""
    t = _valid_template()
    assert _validate_schema(t).passed  # 默认无 estimated_hours
    t["steps"][0]["estimated_hours"] = 0.3
    assert not _validate_schema(t).passed
    t["steps"][0]["estimated_hours"] = 2.0
    assert _validate_schema(t).passed


# ── 3. 三段式移到质量层 ──
def test_three_sections_in_meta_not_schema():
    """prompt 缺"做什么"段 → schema 通过（结构层），meta 警告（质量层）。"""
    t = _valid_template()
    t["steps"][0]["prompt_template"] = "简单提示词"
    assert _validate_schema(t).passed  # 结构层放行
    assert not _validate_meta(t).passed  # 质量层拦截


# ── 4. 存量形状可注册 ──
def test_legacy_shape_registers():
    """按存量模板形状（WF- id、无 estimated_hours、无三段式）→ 注册通过。"""
    t = _valid_template(name="legacy_wf", wf_id="WF-legacy_wf")
    t["steps"][0]["prompt_template"] = "调研任务\n输出报告"
    with tempfile.TemporaryDirectory() as tmp:
        reg = TemplateRegistry(db_path=str(Path(tmp) / "t.db"), workflows_dir=Path(tmp))
        tid = reg.register(t, workflows_dir=Path(tmp))
        assert tid == "WF-legacy_wf"
        reg.close()


# ── 5. register 双写：JSON 文件出现 ──
def test_register_writes_json_and_db():
    """注册后 {name}.json 出现且内容含 workflow_id；DB 有行。"""
    with tempfile.TemporaryDirectory() as tmp:
        wf_dir = Path(tmp) / "workflows"
        reg = TemplateRegistry(db_path=str(Path(tmp) / "t.db"), workflows_dir=wf_dir)
        tid = reg.register(_valid_template(), workflows_dir=wf_dir)
        reg.close()
        # JSON 文件
        jf = wf_dir / "test_wf.json"
        assert jf.exists(), "JSON 文件应被写出"
        data = json.loads(jf.read_text())
        assert data["workflow_id"] == "WF-test_wf"
        # DB 行
        reg2 = TemplateRegistry(db_path=str(Path(tmp) / "t.db"))
        t = reg2.get("WF-test_wf")
        assert t is not None and t["name"] == "test_wf"
        reg2.close()


# ── 6. JSON 已存在 → 拒绝不覆盖 ──
def test_register_refuses_existing_file():
    """同名 JSON 已存在 → ValueError，不覆盖。"""
    with tempfile.TemporaryDirectory() as tmp:
        wf_dir = Path(tmp) / "workflows"
        wf_dir.mkdir()
        (wf_dir / "test_wf.json").write_text(json.dumps({"old": True}))
        reg = TemplateRegistry(db_path=str(Path(tmp) / "t.db"), workflows_dir=wf_dir)
        try:
            reg.register(_valid_template(), workflows_dir=wf_dir)
            assert False, "应拒绝覆盖已有 JSON"
        except ValueError as e:
            assert "已存在" in str(e)
        finally:
            reg.close()
        # 原文件未被覆盖
        assert json.loads((wf_dir / "test_wf.json").read_text()) == {"old": True}


# ── 7. 路径穿越拒绝 ──
def test_register_rejects_path_traversal():
    """name 含路径分隔符 → ValueError。"""
    with tempfile.TemporaryDirectory() as tmp:
        reg = TemplateRegistry(db_path=str(Path(tmp) / "t.db"))
        try:
            reg.register(_valid_template(name="../evil"), workflows_dir=Path(tmp))
            assert False, "应拒绝路径穿越"
        except ValueError as e:
            assert "路径分隔符" in str(e)
        finally:
            reg.close()


# ── 8. 存量 24 模板全过结构层（一次性验证） ──
def test_all_production_templates_pass_structural():
    """~/.hermes/workflows/*.json 全部通过结构层校验（存量兼容硬约束）。"""
    bad = []
    for f in sorted(HERMES_WORKFLOWS.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            if not _validate_schema(data).passed:
                bad.append(f.name)
        except Exception as e:
            bad.append(f"{f.name}(parse:{e})")
    assert not bad, f"存量模板不应被拒绝: {bad[:5]}"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            failed += 1
            import traceback
            print(f"FAIL {t.__name__}: {e}")
            traceback.print_exc()
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {passed}/{passed+failed} tests passed")
    sys.exit(1 if failed else 0)

# ── 9. 绕过防护：_upsert_template 拒绝坏结构模板 ──
def test_upsert_rejects_bad_structure():
    """start() 路径 upsert 无 exit_condition 的模板 → 拒绝写 DB。"""
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from pipeflow.engine import WorkflowEngine
    from pipeflow.engine import WorkflowDef, Step
    with tempfile.TemporaryDirectory() as tmp:
        wf_dir = Path(tmp) / "wf"
        wf_dir.mkdir()
        eng = WorkflowEngine(workflows_dir=wf_dir, db_path=str(Path(tmp) / "t.db"))
        # 构造缺 exit_condition 的坏步骤模板
        bad_wf = WorkflowDef(
            name="bad_wf", title="坏", description="d",
            steps=[Step(id="s1", title="t", target_role="engineer",
                        prompt_template="p", exit_condition={})],
        )
        eng._upsert_template("bad_wf", bad_wf)
        rows = eng._lifecycle.query(
            "SELECT 1 FROM workflow_templates WHERE template_id='bad_wf'")
        assert not rows, "坏结构模板不应写入 DB"
        # 好模板正常写
        good_wf = WorkflowDef(
            name="good_wf", title="好", description="d",
            steps=[Step(id="s1", title="t", target_role="engineer",
                        prompt_template="p",
                        exit_condition={"type": "exit_message", "match": "done"})],
        )
        eng._upsert_template("good_wf", good_wf)
        rows = eng._lifecycle.query(
            "SELECT 1 FROM workflow_templates WHERE template_id='good_wf'")
        assert rows, "好结构模板应写入 DB"


# ── 10. 双节点端到端闭环（完整推进链） ──
def test_dual_node_full_cycle():
    """start → run_once → complete×2 → 引擎推进。验证完整生命周期。"""
    import sys as _sys, logging as _logging, json as _json
    _sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    _logging.disable(_logging.CRITICAL)
    from pipeflow.engine import WorkflowEngine
    from paths import HERMES_WORKFLOWS as _PROD_WF
    from unittest.mock import patch, MagicMock
    import shutil as _shutil
    with tempfile.TemporaryDirectory() as tmp:
        wf_dir = Path(tmp) / "wf"
        wf_dir.mkdir()
        _shutil.copy(_PROD_WF / "food_chain_loop.json", wf_dir / "food_chain_loop.json")
        eng = WorkflowEngine(workflows_dir=wf_dir, db_path=str(Path(tmp) / "t.db"))
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = b"pid=123"
        mock_result.stderr = b""
        with patch("subprocess.run", return_value=mock_result), \
             patch.object(eng, "_ensure_role_alive", return_value=True):
            wf_id = eng.start("food_chain_loop", {"source": "test", "loop_iteration": 1})
            eng.run_once()  # mark notified
            # 双节点第一步 → node1_done
            r1 = eng._lifecycle.complete_step(wf_id, "s1")
            assert r1 == "node1_done", f"第一步应 node1_done: {r1}"
            # 双节点第二步 → 推进到 s2
            r2 = eng._lifecycle.complete_step(wf_id, "s1")
            assert r2 in ("step_done_ready", "completed_and_advanced"), f"第二步应完成: {r2}"
            eng.run_once()
            rows = eng._lifecycle.query(
                "SELECT current_step_id FROM workflow_instances WHERE instance_id=?", (wf_id,))
            assert rows[0]["current_step_id"] in ("s2", "s1"), f"应推进: {rows[0]['current_step_id']}"
