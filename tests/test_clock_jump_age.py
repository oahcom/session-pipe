#!/usr/bin/env python3
"""墙钟前跳后，回收器的年龄必须扣掉「实例创建之后」发生的跳变。

根因（生产实测两次）：
  2026-10-30 +8.14h  → 三条真实龄 3.05h 的链报「运行7h」被杀（bus #469787）
  2026-11-02 +9.561h → wf_d0ea6941a0 真实龄 0.40h 报「运行10h」（bus #477117）

WSL2 宿主挂起恢复时 guest 墙钟被外部 step，CLOCK_MONOTONIC 不计挂起。
回收器用 now - created_at 算年龄，跳变后所有在跑实例年龄一次性放大。

本文件钉住的判据（任一翻转都会重回误杀）：
  1. 跳变后算出的年龄 = 真实龄，不是墙钟龄
  2. 只扣「实例创建之后」的跳变 —— 创建前的跳变已烙进 created_at
  3. 跳变会持续影响后续所有 tick（不是只跳过当轮）
  4. 真超龄实例照常判死（校正不得变成免死金牌）

运行: cd ~/wt-live && python3 -m pytest tests/test_clock_jump_age.py -q
"""
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

import pipeflow.wf_lifecycle as wf_lifecycle          # noqa: E402


def _reset(jumps):
    """把模块内的跳变列表置为给定值（跳过磁盘加载）。"""
    wf_lifecycle._CLOCK_JUMPS = list(jumps)
    wf_lifecycle._CLOCK_JUMPS_LOADED = True


def test_age_uses_real_elapsed_after_forward_jump(tmp_path, monkeypatch):
    """2026-11-02 实例：墙钟 9.96h，真实 0.40h，阈值 2h → 校正后不得判死。"""
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", tmp_path / "clock-anchor.json")
    created = 1_000_000.0
    real_age = 0.40 * 3600
    jump = 9.561 * 3600
    now = created + real_age + jump          # 墙钟视角已过 9.96h
    _reset([{"at": created + real_age, "delta": jump}])

    age_h = wf_lifecycle._corrected_age_s(now, created) / 3600
    assert abs(age_h - 0.40) < 1e-6, f"校正后年龄应为 0.40h，实得 {age_h:.3f}h（会过 2h 阈值被杀）"
    assert age_h <= 2, "校正后的年龄不得跨过 2h 阈值"


def test_jump_before_creation_is_not_deducted(tmp_path, monkeypatch):
    """创建前发生的跳变已烙进 created_at —— 再扣一次会让老实例永不回收。"""
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", tmp_path / "clock-anchor.json")
    jump = 9.561 * 3600
    jump_at = 1_000_000.0
    created = jump_at + 3600                 # 跳变发生后才建的实例
    now = created + 20 * 3600                # 真实跑了 20h
    _reset([{"at": jump_at, "delta": jump}])

    age_h = wf_lifecycle._corrected_age_s(now, created) / 3600
    assert abs(age_h - 20.0) < 1e-6, \
        f"创建前的跳变不该被扣：应得 20h，实得 {age_h:.2f}h（少算会导致真僵尸永不回收）"


def test_correction_persists_across_ticks(tmp_path, monkeypatch):
    """只跳过跳变当轮无效：下一 tick 年龄仍是墙钟龄，必须持续校正。"""
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", tmp_path / "clock-anchor.json")
    created = 1_000_000.0
    jump = 9.561 * 3600
    jump_at = created + 0.40 * 3600
    _reset([{"at": jump_at, "delta": jump}])

    for extra in (0, 10, 60):                # 跳变后第 1/2/N tick
        now = created + 0.40 * 3600 + jump + extra
        age_h = wf_lifecycle._corrected_age_s(now, created) / 3600
        assert age_h < 2, f"+{extra}s 的 tick 上年龄 {age_h:.2f}h 仍超阈值 —— 校正没持续生效"


def test_genuinely_old_instance_still_reclaimed(tmp_path, monkeypatch):
    """校正不得成为免死金牌：真跑了 30h 的实例仍须超 24h 全局阈值。"""
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", tmp_path / "clock-anchor.json")
    created = 1_000_000.0
    _reset([{"at": created + 5 * 3600, "delta": 9.561 * 3600}])   # 中途跳了一次
    # 真实 30h = 墙钟 30h + 跳变 9.561h
    now = created + 30 * 3600 + 9.561 * 3600
    age_h = wf_lifecycle._corrected_age_s(now, created) / 3600
    assert abs(age_h - 30.0) < 1e-6, f"应得 30h，实得 {age_h:.2f}h"
    assert age_h > 24, "真超龄实例必须仍被回收"


def test_over_deduction_clamps_to_zero_never_negative(tmp_path, monkeypatch):
    """扣过头只能钳到 0，绝不返回负龄。

    大跨度回拨会让 at 落在 created 之后但 delta 为负，或前跳事件累积超过真实龄。
    两种都只能让回收**推迟**：负龄会让 `age_s < 300` 与全局阈值同时失效，
    实例反而完全不回收。
    """
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", tmp_path / "clock-anchor.json")
    created = 1_000_000.0
    _reset([{"at": created + 60, "delta": 9.561 * 3600}])   # 跳变远大于真实龄

    age_s = wf_lifecycle._corrected_age_s(created + 120, created)
    assert age_s == 0.0, f"过度扣减应钳到 0，实得 {age_s}"
    assert age_s >= 0, "年龄不得为负"



def test_observe_clock_detects_jump_and_records_once(tmp_path, monkeypatch):
    """偏移突变超容忍阈才记跳变；正常微调不记（否则真实耗时被抹掉）。"""
    anchor = tmp_path / "clock-anchor.json"
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", anchor)
    _reset([])

    mono = 5000.0
    wf_lifecycle._observe_clock(wall=1_000_000.0, mono=mono)          # 首次：建锚点
    assert wf_lifecycle._CLOCK_JUMPS == [], "首次观测不该记跳变"

    wf_lifecycle._observe_clock(wall=1_000_010.0, mono=mono + 10)     # 正常 10s 推进
    assert wf_lifecycle._CLOCK_JUMPS == [], "正常推进不该记跳变"
    jump = 9.561 * 3600
    # 墙钟与单调同步前进 10s，再叠加跳变 —— 否则测量值会少一个 tick 间隔
    wf_lifecycle._observe_clock(wall=1_000_020.0 + jump, mono=mono + 20)
    assert len(wf_lifecycle._CLOCK_JUMPS) == 1, "跳变必须被记录"
    assert abs(wf_lifecycle._CLOCK_JUMPS[0]["delta"] - jump) < 1.0, \
        "测量值应在 1s 内（同步推进时无采样误差）"

    wf_lifecycle._observe_clock(wall=1_000_030.0 + jump, mono=mono + 30)
    assert len(wf_lifecycle._CLOCK_JUMPS) == 1, "跳变后的正常推进不得重复记录"

    import json
    on_disk = json.loads(anchor.read_text(encoding="utf-8"))
    assert len(on_disk["jumps"]) == 1, "跳变必须落盘，daemon 重启后才不丢"


def test_jump_measurement_tolerance_is_one_tick(tmp_path, monkeypatch):
    """采样粒度是一个 tick：墙钟未动而单调走了 10s，测出的跳变会少 10s。

    10s 误差对 9.5h 的校正是噪声级；但这里钉住它，避免将来误以为是 bug 去
    「修正」成把 tick 间隔也计进去（那会把真实流逝时间从年龄里抹掉）。
    """
    anchor = tmp_path / "clock-anchor.json"
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", anchor)
    _reset([])
    mono = 5000.0
    jump = 9.561 * 3600
    wf_lifecycle._observe_clock(wall=1_000_000.0, mono=mono)
    wf_lifecycle._observe_clock(wall=1_000_010.0, mono=mono + 10)
    wf_lifecycle._observe_clock(wall=1_000_010.0 + jump, mono=mono + 20)  # wall 本 tick 未前进
    assert len(wf_lifecycle._CLOCK_JUMPS) == 1
    err = abs(wf_lifecycle._CLOCK_JUMPS[0]["delta"] - jump)
    assert 0 <= err <= 15, f"采样误差应在半个到两个 tick 之间，实得 {err:.1f}s"


def test_corrected_age_self_loads_from_disk(tmp_path, monkeypatch):
    """engine.cancel() 也要查龄，但它不在回收器 tick 路径上 —— 必须自加载。

    否则先跑 cancel 再跑回收器的进程拿到空列表，两处按不同年龄下判断。
    """
    anchor = tmp_path / "clock-anchor.json"
    monkeypatch.setattr(wf_lifecycle, "_CLOCK_ANCHOR_PATH", anchor)
    jump = 9.561 * 3600
    created = 1_000_000.0
    anchor.write_text(json.dumps({"jumps": [{"at": created + 60, "delta": jump}]}),
                      encoding="utf-8")
    wf_lifecycle._CLOCK_JUMPS = []
    wf_lifecycle._CLOCK_JUMPS_LOADED = False        # 模拟刚启动的进程

    age_h = wf_lifecycle._corrected_age_s(created + 120 + jump, created) / 3600
    assert wf_lifecycle._CLOCK_JUMPS_LOADED is True, "应自加载而不依赖 _observe_clock 先跑"
    assert abs(age_h - 120 / 3600) < 1e-6, f"自加载后应得 0.033h，实得 {age_h:.3f}h"
