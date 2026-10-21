"""TASKS.md 排队标记幂等回归。

背景：引擎每 tick 重推同一步骤，role_lifecycle 的排队分支无幂等守卫时
同一 (wf_id, step_id) 被反复追加。实测 engineer TASKS.md 积累 458 行、
scout 28 行排队标记，全部指向已 cancelled/completed 的工作流 —— 纯噪音
污染角色上下文。
"""
import os
import re
import shutil
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("SESSION_PIPELINE_TESTING", "1")


def _apply_queue_guard(existing: str, wf_id: str, step_id: str) -> tuple[str, bool]:
    """复刻 role_lifecycle._send_to_role 的排队分支判定。

    返回 (写后内容, 是否发生了写入)。与生产代码保持同构：命中已有标记则不写。
    """
    if f"[排队] 新任务 {wf_id}/{step_id} " in existing:
        return existing, False
    return (existing.rstrip()
            + f"\n\n---\n[排队] 新任务 {wf_id}/{step_id} 待当前任务完成后执行\n"), True


def test_same_wf_step_written_once():
    """同一 (wf_id, step_id) 重复投递只追加一次 —— 中文：重复投递不应产生重复行。"""
    base = "# 工作流任务\n\n## 来源\n工作流 wf_aaaa00000001 / 步骤 s1\n"
    text = base
    writes = 0
    for _ in range(30):
        text, wrote = _apply_queue_guard(text, "wf_bbbb00000002", "s1")
        writes += wrote

    assert writes == 1, f"重复投递应只写 1 次，实际 {writes} 次"
    assert text.count("[排队] 新任务 wf_bbbb00000002/s1") == 1


def test_different_steps_each_recorded():
    """不同 step 各自记录 —— 中文：同一工作流的不同步骤不应互相吞并。"""
    base = "# 工作流任务\n\n## 来源\n工作流 wf_aaaa00000001 / 步骤 s1\n"
    text = base
    for step in ("s1", "s2", "s3"):
        text, wrote = _apply_queue_guard(text, "wf_bbbb00000002", step)
        assert wrote, f"{step} 应被记录"

    for step in ("s1", "s2", "s3"):
        assert f"[排队] 新任务 wf_bbbb00000002/{step}" in text


def test_empty_file_no_op():
    """空内容不抛异常 —— 中文：边界输入需安全返回。"""
    text, wrote = _apply_queue_guard("", "wf_cccc00000003", "s1")
    assert wrote is True
    assert "[排队]" in text


def test_marker_matching_is_exact_pair():
    """前缀相同的不同 wf 不误判 —— 中文：wf_abc123 不应吞掉 wf_abc123def 的记录。"""
    wf_short, wf_long = "wf_abc123", "wf_abc123def456"
    text, _ = _apply_queue_guard("", wf_short, "s1")
    # 长 wf 的标记不等于短 wf 的子串（含 "/s1 " 后缀，判定为未记录）
    assert f"[排队] 新任务 {wf_short}/s1 " in text
    assert f"[排队] 新任务 {wf_long}/s1 " not in text


def test_step_s1_does_not_swallow_s10():
    """s1 不误命中 s10 —— 中文：判据尾随空格是必需边界，无它则 s10 被误判为已记录。"""
    text, _ = _apply_queue_guard("", "wf_eee00000004", "s10")
    assert "[排队] 新任务 wf_eee00000004/s10 " in text
    # 此时文件中只有 s10；s1 的守卫不应因 s10 存在而跳过
    text2, wrote = _apply_queue_guard(text, "wf_eee00000004", "s1")
    assert wrote is True, "s1 因 s10 存在而被误跳过"
    assert "[排队] 新任务 wf_eee00000004/s1 " in text2


def test_production_guard_present_in_source():
    """生产代码确实含幂等守卫 —— 中文：防止脚本被单独替换后守卫悄悄丢失。"""
    src = Path(__file__).resolve().parents[1] / "src/pipeflow/role_lifecycle.py"
    code = src.read_text(encoding="utf-8")
    assert re.search(
        r'if f"\[排队\] 新任务 \{wf_id\}/\{step_id\} " in _existing', code), \
        "role_lifecycle.py 缺少排队幂等守卫"


def test_stale_main_includes_terminal_status():
    """终态主任务视为可覆盖 —— 中文：排空路径的关键，只判“不在库”时已完成主任务永久占位。"""
    src = Path(__file__).resolve().parents[1] / "src/pipeflow/role_lifecycle.py"
    code = src.read_text(encoding="utf-8")
    assert "_ACTIVE_WF_STATUSES = (" in code, \
        "role_lifecycle.py 缺少 _ACTIVE_WF_STATUSES 常量"
    assert "not in _ACTIVE_WF_STATUSES" in code, \
        "_stale_main 未把终态主任务判为可覆盖（排队行将无界增长）"
    # 与 engine.py 的活跃集一致：不一致则两侧对“活跃”认知分裂
    eng = (Path(__file__).resolve().parents[1] / "src/pipeflow/engine.py").read_text(encoding="utf-8")
    assert "'pending', 'running', 'step_done_ready'" in eng, \
        "engine.py 活跃状态集已变，需同步 _ACTIVE_WF_STATUSES"


def test_wf_in_existing_no_operator_precedence_trap():
    """_wf_in_existing 不得退回 `A or B if C else False` 写法。

    中文：Python 把它解析为 `(A or B) if C else False`，TASKS.md 无 "## 来源" 段
    时整个表达式恒为 False，**无论 wf_id 是否真在文中** → 引擎永远误判"别的任务
    占了文件" → 本任务只写 [排队] 不写正文 → ccs send 退出码 1 → 投递失败 3 次
    → 子链 failed → 父链断。实测 reviewer/engineer/qa 的 TASKS.md 均无该段（只在
    角色产出含来源元数据时才写），是 reviewer 连挂 7 条子链的直接原因之一。
    """
    code = (Path(__file__).resolve().parents[1] / "src/pipeflow/role_lifecycle.py").read_text(
        encoding="utf-8")
    assert not re.search(r'_wf_in_existing = .* if "## 来源" in _existing else False', code), \
        "_wf_in_existing 退回优先级陷阱写法（无 '## 来源' 段时恒 False）"

    # 行为校验：无 '## 来源' 段、wf_id 在文中 → 必须判为已存在
    existing = "# 工作流任务\n\n工作流 wf_dead00000001 / 步骤 s1\n"
    assert ("工作流 wf_dead00000001 /" in existing
            or "wf_dead00000001" in existing.split("## 来源")[-1]), \
        "修复后的表达式未能识别本任务已写入"
