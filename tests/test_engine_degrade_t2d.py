#!/usr/bin/env python3
"""T2D 评估面显式降级：三引擎 fixture 冒烟（契约 4.5 修正口径：status-role JSON）。

伪造 ccs.py status-role JSON 与引擎判定通道，验证：
- opencode / codex / 未知引擎值 / engine 缺失 → 显式降级第三态（status=degraded + 原因 + 未纳入评估；勿静默当 ccs）
- ccs（显式 engine=ccs 字段）→ 与改动前同型
- 降级 ≠ 失败 ≠ 成功：不计入 passed/failed 任一侧
- 降级标记进可见面（_mark_transcript_degraded 被调用：结构化日志 + bus）

运行: cd ~/session-pipeline && python3 -m pytest tests/test_engine_degrade_t2d.py -q
"""
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import eval_checker as ec
from pipeflow import role_lifecycle as rl
from pipeflow import wf_lifecycle as wfl


@pytest.fixture(autouse=True)
def _clear_caches():
    rl._ENGINE_CACHE.clear()
    rl._DEGRADED_MARK_TS.clear()
    yield
    rl._ENGINE_CACHE.clear()
    rl._DEGRADED_MARK_TS.clear()


# ── 1. 引擎判定：契约 4.5 消费 status-role JSON 的 engine 字段 ──

def test_status_json_engine_field_consumed():
    with patch.object(rl, "_fetch_status_role",
                      lambda role: {"role": role, "engine": "opencode"}):
        assert rl.session_engine_for("x") == "opencode"


def test_status_json_engine_ccs_field_consumed():
    with patch.object(rl, "_fetch_status_role",
                      lambda role: {"role": role, "engine": "ccs"}):
        assert rl.session_engine_for("x") == "ccs"


def test_status_json_engine_field_absent_is_unknown():
    """契约 4.5 修正口径：engine 缺失 ≠ ccs，缺证据显式降级（勿静默当 ccs）。"""
    with patch.object(rl, "_fetch_status_role",
                      lambda role: {"role": role, "alive": True}):
        assert rl.session_engine_for("x") == "unknown"


def test_status_json_engine_unknown_value_is_unknown():
    """取值域外（未知值）同样显式降级，不猜。"""
    with patch.object(rl, "_fetch_status_role",
                      lambda role: {"role": role, "engine": "gemini-cli"}):
        assert rl.session_engine_for("x") == "unknown"


def test_engine_channel_failure_is_unknown():
    """缺证据 → unknown（显式降级），不冒充 ccs。"""
    def _boom(role):
        raise RuntimeError("ccs.py status-role 通道故障(fixture)")
    with patch.object(rl, "_fetch_status_role", _boom):
        assert rl.session_engine_for("x") == "unknown"


# ── 2. 评估入口：三引擎输出形态 ──

@pytest.mark.parametrize("eng", ["opencode", "codex", "unknown"])
def test_trajectory_degraded_for_non_ccs(eng, monkeypatch):
    monkeypatch.setattr(rl, "session_engine_for", lambda role: eng)
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append((role, state)))
    res = ec.TrajectoryQualityChecker().check("t2d-deg", "t")
    assert res["status"] == "degraded"
    assert res["passed"] is None            # 第三态：非 True（静默通过）非 False（失败）
    assert res["note"] == "未纳入评估"
    assert "显式降级" in res["reason"] and "未纳入评估" in res["reason"]
    assert res["checked_traces"] == 0
    assert marks and marks[0][1]["state"] == "degraded"


def test_trajectory_ccs_same_shape_as_before(monkeypatch):
    """ccs 会话评估路径零改动：无转录分支返回与改动前逐字段同型。"""
    monkeypatch.setattr(rl, "session_engine_for", lambda role: "ccs")
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append((role, state)))
    res = ec.TrajectoryQualityChecker().check("t2d-nosuch-role", "t")
    assert res == {"passed": True, "findings": [], "checked_traces": 0}
    assert marks == []


# ── 3. run_eval_check 聚合：降级独立计数，不折进 passed/failed ──

class _DegradedTrajectory:
    def check(self, *args, **kwargs):
        return {"passed": None, "status": "degraded", "engine": "opencode",
                "reason": "opencode 会话转录不在 ~/.claude/projects，评估面显式降级（未纳入评估）",
                "note": "未纳入评估", "findings": [], "checked_traces": 0}


def test_run_eval_check_degraded_third_state():
    with (
        patch.object(ec, "_load_roles",
                     return_value=[{"name": "t2d-deg", "eval_criteria": []}]),
        patch.object(ec, "_write_bus") as bus,
        patch.object(ec, "TrajectoryQualityChecker", _DegradedTrajectory),
    ):
        s = ec.run_eval_check(role_filter="t2d-deg")
    assert s["degraded"] == 1
    d = s["degraded_detail"][0]
    assert d["role"] == "t2d-deg" and d["engine"] == "opencode"
    assert "未纳入评估" in d["reason"]
    assert s["failed"] == 0 and s["passed"] == 0 and s["checked"] == 0
    bus_texts = [c.args[1] for c in bus.call_args_list if len(c.args) > 1]
    assert any("未纳入评估" in t for t in bus_texts), bus_texts


# ── 4. 转录读取入口（role_lifecycle）：非 ccs 显式降级，不静默空集 ──

def test_role_lifecycle_read_points_degrade(monkeypatch):
    monkeypatch.setattr(rl, "session_engine_for", lambda role: "opencode")
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append(state))
    assert rl._read_transcript_tail("t2d-op") == []
    assert rl._transcript_stalled("t2d-op") is False
    assert rl._transcript_shows_overflow("t2d-op") is False
    assert rl._transcript_last_turn_is_api_error("t2d-op") is False
    # 每个读点都显式标记（谓词经 _read_transcript_tail 复用标记）
    assert len(marks) >= 2 and all(m["state"] == "degraded" for m in marks)


def test_role_lifecycle_ccs_no_marker(monkeypatch):
    monkeypatch.setattr(rl, "session_engine_for", lambda role: "ccs")
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append(state))
    assert rl._transcript_stalled("t2d-ccs-nosuch") is False   # 现状语义
    assert marks == []


# ── 5. 工作流生命周期读取入口（wf_lifecycle）：同一降级语义 ──

def test_wf_producing_recently_degrades(monkeypatch):
    monkeypatch.setattr(rl, "session_engine_for", lambda role: "codex")
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append(state))
    assert wfl._role_producing_recently("t2d-op") is False
    assert marks and marks[0]["state"] == "degraded"


def test_wf_producing_recently_ccs_unchanged(monkeypatch):
    monkeypatch.setattr(rl, "session_engine_for", lambda role: "ccs")
    marks: list = []
    monkeypatch.setattr(rl, "_mark_transcript_degraded",
                        lambda role, state: marks.append(state))
    assert wfl._role_producing_recently("t2d-ccs-nosuch") is False
    assert marks == []
