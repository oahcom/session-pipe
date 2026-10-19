"""R83 交付链修复回归测试（无框架，纯 assert）。

覆盖 2026-10-17 R83 的四处引擎级修复：
  1. /goal 前缀条件化 — 仅真实任务 (force=False) 加，运维催办不加
  2. deadlock 文案稳定化 — 去掉每秒变化的 age，否则 _event_key 去重恒失效
  3. 排队降级阈值 _QUEUE_DEGRADE_AT — 破 busy 自我互锁
  4. 排队降级不触发 relaxed-advance — 防误推进
"""

import sys
import os
import re
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def test_goal_prefix_only_for_real_tasks():
    """force=True（运维催办）不加 /goal；force=False（真实任务）加。

    背景：Claude Code 把 /goal 变成会话级 Stop hook 硬条件。催办消息形态是
    "[deadlock] X 超时 6769s，请继续"，age 每秒 +1 使条件永不成立 → 角色永久
    锁死（实测 qa 单会话 215 条该 hook、助理文本 0 条）。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/pipeflow/role_lifecycle.py"),
               encoding="utf-8").read()
    assert "if not force:" in src, "role_lifecycle 应含 `if not force:` 条件化的 /goal 分支"
    # 定位该分支并确认其后紧跟 /goal 拼接
    idx = src.index("if not force:")
    seg = src[idx:idx + 120]
    assert 'prompt = "/goal " + prompt' in seg, \
        "force=False 分支内应拼接 /goal 前缀"
    # 反向：确认不存在无条件拼接
    assert not re.search(r'^\s{8}prompt = "/goal " \+ prompt', src, re.M), \
        "不应存在无条件的 /goal 拼接"
    print("✓ test_goal_prefix_only_for_real_tasks PASS")


def test_queue_degrade_constant_exists_and_is_sane():
    """_QUEUE_DEGRADE_AT 存在且在既有升级链 (>=6 通知 / >=10 cancel) 之内。

    背景：排队分支原本永久 return（不递增 timeout_count），而 busy 判据自带
    `timeout_count < 3` 前提 → 同角色两个卡死实例互判 busy → 双方计数恒 0 →
    永远 busy。降级阈值必须 < 6，否则永远到不了降级点。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/pipeflow/step_handler.py"),
               encoding="utf-8").read()
    m = re.search(r"^_QUEUE_DEGRADE_AT = (\d+)$", src, re.M)
    assert m, "应定义模块级 _QUEUE_DEGRADE_AT"
    val = int(m.group(1))
    assert 1 <= val < 6, \
        f"_QUEUE_DEGRADE_AT={val} 应 >=1 且 <6（既有升级链 >=6 通知、>=10 cancel）"
    assert "_queued >= _QUEUE_DEGRADE_AT" in src, "排队分支应使用该阈值"
    print(f"✓ test_queue_degrade_constant_exists_and_is_sane PASS (={val})")


def test_queue_degrade_excluded_from_relaxed_advance():
    """排队降级路径不得触发 relaxed-advance。

    背景：降级使 timeout_count 首次变 1，而 relaxed-advance 的门槛正是
    `timeout_count >= 1`，且它只要求角色写过任意同 wf 消息（不限分类）→
    若不排除，排队中的步骤会被误判为"已完成"。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/pipeflow/step_handler.py"),
               encoding="utf-8").read()
    assert "_from_queue_degrade" in src, "应存在 _from_queue_degrade 守卫"
    # 守卫必须用在 relaxed-advance 的条件里
    idx = src.index("if (timeout_count >= 1 and not step.dual_node")
    seg = src[idx:idx + 200]
    assert "_from_queue_degrade" in seg, \
        "relaxed-advance 条件应含 `and not _from_queue_degrade`"
    # 不得用 locals() 探测（_queued 只在 busy 分支定义）
    assert 'locals().get("_queued"' not in src, \
        "不应使用 locals() 探测 _queued（非 busy 路径该变量不存在）"
    # 阈值语义：守卫必须真的用 _QUEUE_DEGRADE_AT 比较，而非写死一个大数（如 >=99）
    # 否则守卫名义存在但事实失效——第一轮审查的变异测试正是这样漏检的。
    # 读源可以是 run.step_results 直取，也可以走 _step_sd(run, step) 归一化
    # （后者对非 dict 的 step_results 值免疫——生产 DB 实测 413 个非 dict 值）。
    m = re.search(r"_from_queue_degrade = \((?:run\.step_results|self\._step_sd\(run, step\))"
                  r"[\s\S]{0,160}?get\(\"queued\", 0\)\s*\n?\s*>=\s*_QUEUE_DEGRADE_AT\)", src)
    assert m, \
        "守卫阈值必须比较 `queued >= _QUEUE_DEGRADE_AT`（不得写死其他数字）"
    print("✓ test_queue_degrade_excluded_from_relaxed_advance PASS")


def test_queue_degrade_fallthrough_reaches_timeout():
    """降级分支必须 fall-through 到超时计数，而非 return。

    若降级后仍 return，timeout_count 不会递增 → 回收器（依赖 tc>=1）仍不工作
    → 降级形同虚设。这是"修了等于没修"的典型陷阱。

    达阈值**且**排队满 30 分钟后才降级：只按次数会让 4 个 tick 绕过排队门槛
    （codex P1），互锁破环在 30 分钟后仍成立。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/pipeflow/step_handler.py"),
               encoding="utf-8").read()
    # 条件可能跨行（排队降级另加 30 分钟窗口），用 \s* 吸收换行与续行括号
    m = re.search(
        r"if \(?_queued >= _QUEUE_DEGRADE_AT[\s\S]{0,400}?else:\s*\n\s+return", src)
    assert m, (
        "降级分支应为 if/else 结构：达阈值时不 return（fall-through 到超时计数），"
        "未达阈值才 return")
    # 降级块内不得出现无条件 return
    block = m.group(0)
    body_after_log = block.split("LOGGER.warning", 1)[-1]
    assert "return" not in body_after_log.split("else:")[0], \
        "降级分支（LOGGER.warning 之后）不得 return，必须 fall-through"
    print("✓ test_queue_degrade_fallthrough_reaches_timeout PASS")


def test_blackboard_import_is_packaged():
    """blackboard 必须从 hermes_bus 包导入。

    背景：真身是 ~/.hermes/scripts/hermes_bus/blackboard.py，裸 `from blackboard
    import` 恒失败 → 审批告警全部丢失（journal 每 20s 报一次 No module named）。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/lifecycle/manager.py"),
               encoding="utf-8").read()
    assert "from hermes_bus.blackboard import Blackboard" in src, \
        "应使用 hermes_bus.blackboard 包路径"
    assert not re.search(r"^\s+from blackboard import", src, re.M), \
        "不应残留裸 blackboard 导入"
    print("✓ test_blackboard_import_is_packaged PASS")


def test_survival_monitor_warns_on_idle():
    """idle（L3 产出存活为假）必须被纳入告警。

    背景：stalled 只含 stale/dead，idle 既不告警也不进恢复队列 → 17 角色中
    10 个恒 idle 永久静默（"最近产出 7140s 前"无人处置）。
    """
    src = open(os.path.join(os.path.dirname(__file__), "..",
                            "src/routing/survival_monitor.py"),
               encoding="utf-8").read()
    assert 'idled = [r for r, h in results.items() if h["overall"] == "idle"]' in src, \
        "应收集 idle 角色列表"
    assert "存活告警 idle" in src, "应输出 idle 告警日志"
    print("✓ test_survival_monitor_warns_on_idle PASS")


def _selfcheck():
    """.py 直接执行时跑全部（无 pytest 依赖）。"""
    fns = [test_goal_prefix_only_for_real_tasks,
           test_deadlock_prompt_has_no_age,
           test_queue_degrade_constant_exists_and_is_sane,
           test_queue_degrade_excluded_from_relaxed_advance,
           test_queue_degrade_fallthrough_reaches_timeout,
           test_blackboard_import_is_packaged,
           test_survival_monitor_warns_on_idle]
    for fn in fns:
        fn()
    print(f"\n✅ R83 delivery-chain regression: {len(fns)}/{len(fns)} PASS")


if __name__ == "__main__":
    _selfcheck()
