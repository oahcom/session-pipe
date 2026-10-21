#!/usr/bin/env python3
"""_advance_production_wf 推进时同步 assignee — 防背压配额失真。

根因（2026-10-19 生产实测）：assignee 只在建实例时设一次（首步角色），
engine 侧推进（wf_lifecycle._advance_production_wf）只写 current_step_id
不写 assignee → 实测 7 个活跃实例 5 个不一致（71%）：
  wf_22b96b1b0c step=s2 要求 product_architect 却仍归 pm
  wf_59263c95f86b step=s5 要求 writer 却仍归 pg
背压计数（`query DISTINCT assignee`）与任务生成配额据此失真。

姊妹路径 manager._advance_unsafe:1024 已在同步 assignee，此处是遗漏的另一半。

契约：推进到 next_step 时，assignee 必须等于 next_step.target_role。

运行: cd /home/administrator/session-pipeline && python3 tests/test_advance_assignee_sync.py
"""
import re
import sys
from pathlib import Path

_SRC = str(Path.home() / "session-pipeline" / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_SRC_FILE = Path(_SRC) / "pipeflow" / "wf_lifecycle.py"


def test_advance_updates_assignee():
    """推进 SQL 必须同时写 current_step_id 与 assignee。"""
    src = _SRC_FILE.read_text(encoding="utf-8")
    # 定位 `else:` 分支内的推进 SQL（紧跟 "推进到下一步" 注释之后）
    m = re.search(
        r"# 推进到下一步.*?_adv = conn\.execute\((.*?)\)\n",
        src, re.S)
    assert m, "未找到推进到下一步的 SQL 块（注释被改动？）"
    block = m.group(1)
    assert "current_step_id=?, assignee=?" in block, \
        f"推进 SQL 未同步 assignee：{block[:200]}"
    assert "next_step.target_role" in block, \
        f"assignee 未取自 next_step.target_role：{block[:200]}"


def test_assignee_null_guard():
    """target_role 缺失时写 NULL 而非空串（避免退化成无主 assignee）。"""
    src = _SRC_FILE.read_text(encoding="utf-8")
    assert "next_step.target_role or None" in src, \
        "缺少 target_role 缺失时的 None 兜底（空串会污染背压分组键）"


if __name__ == "__main__":
    _fails = 0
    for _fn in (test_advance_updates_assignee, test_assignee_null_guard):
        try:
            _fn()
            print(f"  OK  {_fn.__name__}")
        except AssertionError as _e:
            _fails += 1
            print(f"  FAIL {_fn.__name__}: {_e}")
    print(f"\n结果: {2 - _fails} 通过, {_fails} 失败")
    sys.exit(1 if _fails else 0)
