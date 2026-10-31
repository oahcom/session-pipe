#!/usr/bin/env python3
"""食物链模板集成测试 — 真实 WorkflowEngine + 真实 SQLite（mock 仅限 subprocess 外部副作用）。

验证链路: eng.start("food_chain_loop") → DB 落库 → has_active_template 识别 → active_workflow_count 计数。
非 mock 的全链路验证，排除"测试 mock 永远成功"盲区（2026-08-11 事故根因）。
（daemon 的饥饿自启已删除，见 daemon.py 注释。）
"""

import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from paths import HERMES_WORKFLOWS as _PROD_WORKFLOWS

from pipeflow.engine import WorkflowEngine


def _make_engine(tmp: str) -> WorkflowEngine:
    """真实模板目录 + 独立测试 DB（非生产 workflows.db）。"""
    wf_dir = Path(tmp) / "workflows"
    wf_dir.mkdir(parents=True, exist_ok=True)
    # 复制生产模板（food_chain_loop 是 starve 的触发目标）
    src = Path(_PROD_WORKFLOWS) / "food_chain_loop.json"
    shutil.copy(src, wf_dir / "food_chain_loop.json")
    return WorkflowEngine(workflows_dir=wf_dir, db_path=str(wf_dir / "integration.db"))


def test_start_persists_to_db():
    """真实链路: eng.start → DB 出现 pending 实例 → active_workflow_count=1。"""
    with tempfile.TemporaryDirectory() as tmp:
        eng = _make_engine(tmp)
        # mock 外部提交（提示词发送走 subprocess）
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            wf_id = eng.start("food_chain_loop", context={
                "source": "starve_detector", "reason": "集成测试",
            })
        assert isinstance(wf_id, str) and wf_id.startswith("wf_"), f"返回值异常: {wf_id!r}"
        # 真实 DB 查询：实例已落库且 pending
        rows = eng._lifecycle.query(
            "SELECT template_id, status FROM workflow_instances WHERE instance_id=?", (wf_id,))
        assert rows and rows[0]["template_id"] == "food_chain_loop"
        assert rows[0]["status"] in ("pending", "running"), f"状态异常: {rows[0]['status']}"
        # 活跃计数
        assert eng.active_workflow_count() >= 1


def test_has_active_template_true_after_start():
    """真实链路: start 后 has_active_template 返回 True（竞态防护的前置依赖）。"""
    with tempfile.TemporaryDirectory() as tmp:
        eng = _make_engine(tmp)
        # returncode 必须显式配置：未配置时是 MagicMock，与 0 比较为 False,
        # 会被 _wait_for_role_ready 判为"角色没起来"而真去拉起 tmux，导致测试挂死。
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            eng.start("food_chain_loop", context={"source": "starve_detector"})
        assert eng.has_active_template("food_chain_loop") is True


def test_has_active_template_false_when_none():
    """无实例时返回 False。"""
    with tempfile.TemporaryDirectory() as tmp:
        eng = _make_engine(tmp)
        assert eng.has_active_template("food_chain_loop") is False
        assert eng.has_active_template("不存在的模板") is False


def test_has_active_template_false_after_complete():
    """实例完成后（cancelled）不再活跃。"""
    with tempfile.TemporaryDirectory() as tmp:
        eng = _make_engine(tmp)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            wf_id = eng.start("food_chain_loop", context={"source": "starve_detector"})
        assert eng.has_active_template("food_chain_loop") is True
        eng._lifecycle.close_wf(wf_id, status="cancelled")
        assert eng.has_active_template("food_chain_loop") is False


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