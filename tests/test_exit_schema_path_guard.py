"""exit_schema 路径必须落在执行角色 workspace 内（security_auditor 加固补丁）。

回归的两种越界形态，2026-10-21 实测均可让引擎把 workspace 外的文件当本步产出：
  1. properties 分支完全没有越界检查：schema 写 `../../../../tmp/X.md` 即读任意文件。
  2. required 分支用裸 startswith：角色 dev 的 ws=/ws/dev 放行兄弟目录 /ws/devops
     （本机同时存在 dev/devops、architect/architecture、research/researcher）。
"""
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from paths import CCS_WORKSPACES  # noqa: E402
from pipeflow.step_handler import StepHandlerMixin  # noqa: E402

_UNIQUE = f"PG_{int(time.time() * 1000) % 1_000_000:06d}"


class _Eng(StepHandlerMixin):
    pass


def _run(role):
    return SimpleNamespace(id="wf_pg", created_at=time.time() - 5,
                           effective_roles={"sX": role}, step_results={})


def _step(schema, role):
    return SimpleNamespace(id="sX", target_role=role, exit_schema=schema)


def test_properties_branch_rejects_traversal_outside_workspace():
    """properties 键指到工作区外 → 路径越权，不读内容。"""
    outside = Path("/tmp") / f"{_UNIQUE}_OUT.md"
    outside.write_text("TOKEN_" + _UNIQUE + " " + "z" * 200)
    try:
        schema = {"properties": {f"../../../../tmp/{outside.name}": {"mustContain": ["TOKEN_" + _UNIQUE]}}}
        ok, errs = _Eng()._validate_exit_schema(_step(schema, "engineer"), _run("engineer"))
        assert not ok, "工作区外文件被当成本步产出"
        assert any("路径越权" in e for e in errs), errs
    finally:
        outside.unlink()


def test_required_branch_rejects_sibling_dir_with_common_prefix():
    """dev 的契约不能由兄弟目录 devops 满足（startswith 缺分隔符边界）。"""
    ws = CCS_WORKSPACES / "dev"
    sibling = CCS_WORKSPACES / "devops"
    if not (ws.is_dir() and sibling.is_dir()):
        return  # 本机没有这对角色目录时不适用
    ok, errs = _Eng()._validate_exit_schema(
        _step({"required": ["../devops"]}, "dev"), _run("dev"))
    assert not ok, "兄弟目录被当成自身 workspace"
    assert any("路径越权" in e for e in errs), errs


def test_normal_contract_still_checked():
    """正常路径不受影响：缺文件仍报缺少产出，而不是路径越权。"""
    name = f"{_UNIQUE}_MISSING.md"
    ok, errs = _Eng()._validate_exit_schema(
        _step({"required": [name]}, "engineer"), _run("engineer"))
    assert not ok
    assert any("缺少产出" in e for e in errs), errs
