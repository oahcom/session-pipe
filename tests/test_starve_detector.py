#!/usr/bin/env python3
"""饥饿检测器功能测试 — 验证核心逻辑路径，无外部依赖。"""

import json
import sys
import time
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pipeflow import daemon


# ── 1. 活跃状态通过 engine 公共接口查询 ──
def test_active_statuses():
    from unittest.mock import MagicMock
    eng = MagicMock()
    eng.active_workflow_count.return_value = 2
    assert isinstance(daemon._active_count(eng), int)


# ── 2. _STARVE_ROUNDS = 36（6 分钟 @ 10s 间隔） ──
def test_starve_rounds():
    assert daemon._STARVE_ROUNDS == 36
    assert daemon._STARVE_COOLDOWN == 6 * 3600


# ── 3. _active_count 正常返回 ──
def test_active_count_returns_int():
    eng = MagicMock()
    eng.active_workflow_count.return_value = 5
    assert daemon._active_count(eng) == 5


def test_active_count_empty():
    eng = MagicMock()
    eng.active_workflow_count.return_value = 0
    assert daemon._active_count(eng) == 0


def test_active_count_db_error_returns_zero():
    """engine.active_workflow_count 内部捕获 DB 错误并返回 0。"""
    from pipeflow.engine import WorkflowEngine
    from unittest.mock import MagicMock, patch
    eng = WorkflowEngine(db_path=":memory:")
    mock_lm = MagicMock()
    mock_lm.query.side_effect = Exception("db locked")
    eng._lm = mock_lm  # 直接注入内部缓存，绕过 @property
    assert eng.active_workflow_count() == 0


# ── 4. 冷却期内不重复触发 ──
def test_cooldown_blocks_refire():
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        state_path.write_text(json.dumps({
            "last_fire": time.time() - 100,  # 100 秒前触发过
            "last_wf": "wf_test",
        }))
        with patch.object(daemon, "_STARVE_STATE", state_path):
            eng = MagicMock()
            with patch.object(daemon, "subprocess"):
                daemon._starve_signal(eng)
            # eng.start 不应被调用（冷却期内）
            eng.start.assert_not_called()


# ── 5. 冷却到期后正常触发 ──
def test_cooldown_expired_fires():
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        state_path.write_text(json.dumps({
            "last_fire": time.time() - daemon._STARVE_COOLDOWN - 1,  # 已过冷却
        }))
        with patch.object(daemon, "_STARVE_STATE", state_path), \
             patch.object(daemon, "_ensure_role_alive", return_value=True), \
             patch.object(daemon, "subprocess") as mock_sub:
            eng = MagicMock()
            eng.start.return_value = "wf_new123"
            daemon._starve_signal(eng)
            eng.start.assert_called_once_with(
                "food_chain_loop",
                context={"source": "starve_detector", "reason": "连续6分钟无活跃工作流"},
            )
            # bus 写入也触发了
            mock_sub.run.assert_called()


# ── 6. 首次触发（无 state 文件） ──
def test_first_fire_without_state_file():
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"  # 不存在
        with patch.object(daemon, "_STARVE_STATE", state_path), \
             patch.object(daemon, "_ensure_role_alive", return_value=True), \
             patch.object(daemon, "subprocess"):
            eng = MagicMock()
            eng.start.return_value = "wf_first"
            daemon._starve_signal(eng)
            eng.start.assert_called_once()
            assert state_path.exists()
            saved = json.loads(state_path.read_text())
            assert saved["last_wf"] == "wf_first"


# ── 7. 工作流创建失败时不写状态文件 ──
def test_workflow_create_failure_no_state():
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        with patch.object(daemon, "_STARVE_STATE", state_path), \
             patch.object(daemon, "_ensure_role_alive", return_value=True), \
             patch.object(daemon, "subprocess"):
            eng = MagicMock()
            eng.start.side_effect = Exception("模板不存在")
            daemon._starve_signal(eng)
            # state 文件不应被写入（提前 return）
            assert not state_path.exists()


# ── 8. run_once 饥饿计数器逻辑模拟 ──
def test_starve_counter_logic():
    """模拟 daemon 主循环中的计数器行为。"""
    starve_rounds = 0
    threshold = daemon._STARVE_ROUNDS
    fired = []

    # 模拟 35 轮空转（未达阈值）
    for _ in range(35):
        starve_rounds += 1
        if starve_rounds >= threshold:
            fired.append(True)
    assert len(fired) == 0  # 未触发

    # 第 36 轮触发
    starve_rounds += 1
    if starve_rounds >= threshold:
        fired.append(True)
    assert len(fired) == 1  # 触发了
    starve_rounds = 0  # 重置

    # 重置后新一轮不触发
    for _ in range(10):
        starve_rounds += 1
        if starve_rounds >= threshold:
            fired.append(True)
    assert len(fired) == 1  # 没有新增触发

    # 活跃工作流出现 → 计数器归零
    starve_rounds = 0
    for _ in range(50):
        starve_rounds += 1
    starve_rounds = 0  # 活跃检测重置
    assert starve_rounds == 0


# ── 9. 端到端：starve 触发 → 工作流存在 ──
def test_end_to_end_starve_creates_workflow():
    """验证完整触发链路（mock 替换 IO）。"""
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        with patch.object(daemon, "_STARVE_STATE", state_path), \
             patch.object(daemon, "_ensure_role_alive", return_value=True), \
             patch.object(daemon, "subprocess"):
            eng = MagicMock()
            eng.start.return_value = "wf_e2e_test"
            daemon._starve_signal(eng)
            saved = json.loads(state_path.read_text())
            assert saved["last_wf"] == "wf_e2e_test"
            assert "last_fire" in saved
            # 验证冷却机制：第二次不触发
            daemon._starve_signal(eng)
            assert eng.start.call_count == 1  # 仍是 1 次


# ── 10. _ensure_role_alive 调用 ccs.py ──
def test_ensure_role_alive_calls_ccs():
    with patch.object(daemon, "subprocess") as mock_sub:
        mock_sub.run.return_value = MagicMock(returncode=0, stderr="")
        ok = daemon._ensure_role_alive("scout")
        assert ok is True
        call_args = mock_sub.run.call_args[0][0]
        # [python3, ccs.py, start, scout, --no-attach, --no-auto-send]
        assert "ccs.py" in call_args[1]
        assert "scout" in call_args


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {passed}/{passed+failed} tests passed")
    sys.exit(1 if failed else 0)
