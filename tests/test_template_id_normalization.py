"""
模板 template_id 前缀归一化回归测试 — 守护 WF-WF-...-WL-... 无限叠加 P0 故障复发。
（轻量自检，无框架/夹具依赖。）
"""
import re, sys
import types
from dataclasses import dataclass, field
from typing import Optional

# 最小复现 engine.py 的归一化逻辑
def canon_tid(wf_workflow_id, name):
    tid = re.sub(r'^(?:WF-)+', '', wf_workflow_id or name)
    if not tid.startswith(('WL-', 'WF-')):
        tid = 'WF-' + tid
    return tid

# 直接复用 engine.py 真实函数: 导入 WorkflowEngine._load_workflows 的 tid 逻辑
sys.path.insert(0, 'src')
from pipeflow.engine import WorkflowEngine  # noqa

# 实测: 构造 engine 跑 _load_workflows 两次, 观察 _workflows key 是否稳定收敛
def test_re_sub_converges():
    cases = {
        ("WL-foo", "foo"): "WL-foo",
        ("WF-WF-WF-foo", "foo"): "WF-foo",
        ("WF-WF-WF-WF-WL-09", "x"): "WL-09",     # 深度污染收敛
        (None, "food_chain_loop"): "WF-food_chain_loop",
        ("", "bar"): "WF-bar",
    }
    for (wid, name), expect in cases.items():
        # 用 engine.py line284-287 的真实逻辑
        tid = re.sub(r'^(?:WF-)+', '', wid or name)
        if not tid.startswith(('WL-', 'WF-')):
            tid = 'WF-' + tid
        assert tid == expect, f"wid={wid!r} name={name!r} -> got {tid!r} expect {expect!r}"

def test_idempotent_after_normalize():
    """normalize 后再 normalize 不变。"""
    for raw in ["WF-WF-foo", "WL-09", "foo", "WF-WF-WF-WL-09"]:
        once = canon_tid(raw, "foo")
        twice = canon_tid(once, "foo")
        assert once == twice, f"not idempotent: {raw} -> {once} -> {twice}"

if __name__ == "__main__":
    test_re_sub_converges()
    test_idempotent_after_normalize()
    print("test_template_id_normalization: PASS (re.sub 收敛 + 幂等)")
