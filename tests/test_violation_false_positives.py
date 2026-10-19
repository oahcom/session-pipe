"""越界检测误报回归守卫（pytest）。

生产事故（2026-10-20）：engineer / reviewer / maintainer / optimizer / scout /
writer / closer / curator / pm 九个角色在 `_FORBIDDEN_MAP` 里没有条目 → 全部落到
`conversation_monitor._DEFAULT_FORBIDDEN`，而该默认表含 `run_tests`。

后果：
- engineer 本职「编码实现 + 自测」，跑 pytest 被判越界 → 引擎反复注入
  「请立即停止并撤销该操作」，并把该提醒混进任务 prompt（bus #368015）
- reviewer 职责是验证他人代码，跑测试是核心手段，同样被拦
- knowledge_curator 独立复验（bus #417927）结论：「非角色卡死，是
  conversation_monitor 越界计数」导致子链被误取消
- 生产 DB 42 个步骤有 violation_count，其中 6 个 ≥3 次会触发 cancel_step

第二个缺陷：禁区定义有磁盘缓存，改了 roles.py 但缓存不失效 → 修复不生效。
"""

import importlib.util
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

_LAUNCHER_SRC = Path.home() / "session-launcher" / "src"


def _launcher_roles():
    """按文件路径直接加载 launcher 的 routing.roles。

    不能靠 sys.path.insert：全量测试里其他用例会改写 sys.path / sys.modules，
    单独跑通过、混跑时 ModuleNotFoundError。按 spec 加载不受污染。
    """
    mod = sys.modules.get("_lp_routing_roles")
    if mod is not None:
        return mod
    spec = importlib.util.spec_from_file_location(
        "_lp_routing_roles", _LAUNCHER_SRC / "routing" / "roles.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_lp_routing_roles"] = mod
    spec.loader.exec_module(mod)
    return mod

# 测试角色：既要有产出能力（engineer/reviewer 必须能跑测试），
# 又要有决策角色（coordinator/architect 不该自己跑测试）。
_ROLES_THAT_MUST_RUN_TESTS = ("engineer", "reviewer", "maintainer",
                              "optimizer", "scout", "closer", "curator")
_ROLES_THAT_MUST_NOT_RUN_TESTS = ("coordinator", "product_architect")
_ROLES_THAT_MUST_NOT_WRITE_CODE = ("scout", "writer", "pm")


def test_engineer_and_reviewer_can_run_tests():
    """跑测试是 engineer/reviewer 的本职，不得判越界。"""
    _FORBIDDEN_MAP = _launcher_roles()._FORBIDDEN_MAP
    for role in _ROLES_THAT_MUST_RUN_TESTS:
        ops = _FORBIDDEN_MAP.get(role)
        assert ops is not None, (
            f"{role} 在 _FORBIDDEN_MAP 无条目 → 会落到含 run_tests 的默认表，"
            f"跑 pytest 被误判越界（bus #368015）")
        assert "run_tests" not in ops, f"{role} 不应禁 run_tests，实际禁了: {ops}"


def test_decision_roles_still_forbid_tests():
    """决策角色不该自己跑测试（该派给 qa），禁区必须保留。"""
    _FORBIDDEN_MAP = _launcher_roles()._FORBIDDEN_MAP
    for role in _ROLES_THAT_MUST_NOT_RUN_TESTS:
        assert "run_tests" in (_FORBIDDEN_MAP.get(role) or []), \
            f"{role} 应保留 run_tests 禁区"


def test_content_roles_still_forbid_write_code():
    """只产出内容/文档的角色仍禁写代码 —— 放宽 run_tests 不得把 write_code 一并放开。"""
    _FORBIDDEN_MAP = _launcher_roles()._FORBIDDEN_MAP
    for role in _ROLES_THAT_MUST_NOT_WRITE_CODE:
        assert "write_code" in (_FORBIDDEN_MAP.get(role) or []), \
            f"{role} 应保留 write_code 禁区"


def test_cache_invalidates_when_source_newer(tmp_path, monkeypatch):
    """缓存 mtime 早于 roles.py → 必须重新生成（否则改了禁区不生效）。"""
    import json
    import conversation_monitor as cm

    cache = tmp_path / "forbidden_ops_cache.json"
    cache.write_text(json.dumps({
        "forbidden_ops": {"reviewer": ["run_tests"]},  # 故意写入旧的错误表
        "version": 2,
    }))
    os.utime(cache, (1000.0, 1000.0))  # 缓存远早于源码

    monkeypatch.setattr(cm, "_FORBIDDEN_CACHE", cache)
    monkeypatch.setattr(cm, "_forbidden_ops_loaded", False)
    monkeypatch.setattr(cm, "_FORBIDDEN_OPS", {})
    monkeypatch.setattr(cm, "_load_forbidden_ops_from_launcher",
                        lambda: {"reviewer": ["edit_config"]})

    cm._ensure_forbidden_ops()
    assert cm._FORBIDDEN_OPS.get("reviewer") == ["edit_config"], \
        "源码更新后必须重算，不得沿用陈旧缓存的 run_tests 禁区"


def test_cache_used_when_fresh(tmp_path, monkeypatch):
    """缓存新鲜（晚于源码）且版本匹配 → 直接用，不重新生成。"""
    import json
    import time
    import conversation_monitor as cm

    cache = tmp_path / "forbidden_ops_cache.json"
    cache.write_text(json.dumps({
        "forbidden_ops": {"reviewer": ["edit_config"]}, "version": 2}))
    future = time.time() + 3600
    os.utime(cache, (future, future))

    calls = {"n": 0}

    def _boom():
        calls["n"] += 1
        return {"reviewer": ["should_not_be_used"]}

    monkeypatch.setattr(cm, "_FORBIDDEN_CACHE", cache)
    monkeypatch.setattr(cm, "_forbidden_ops_loaded", False)
    monkeypatch.setattr(cm, "_FORBIDDEN_OPS", {})
    monkeypatch.setattr(cm, "_load_forbidden_ops_from_launcher", _boom)

    cm._ensure_forbidden_ops()
    assert calls["n"] == 0, "缓存新鲜时不应重新加载"
    assert cm._FORBIDDEN_OPS.get("reviewer") == ["edit_config"]
