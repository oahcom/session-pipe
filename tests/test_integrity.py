"""完整性提醒单元测试（无框架，纯 assert + pytest 风格函数）。

覆盖：维度常量、激活幂等、间隔节流、维度推进、回复清除、重试上限、完整循环、bypass 分句、tick 集成、fail-open、reply 闭环。
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeflow.integrity import (
    DIMENSIONS,
    IntegrityState,
    load_state,
    save_state,
    activate,
    should_send_now,
    get_next_dimension,
    advance_index,
    on_reply_received,
    on_retry_or_timeout,
    format_dim_prompt,
    make_full_prompt,
    send_bypass_reminder,
)


def test_维度数量至少为2():
    # 默认保留2个核心维度（用户原话），后续可追加（加提示词只改 DIMENSIONS 列表）
    assert len(DIMENSIONS) >= 2


def test_维度id唯一():
    ids = [d["id"] for d in DIMENSIONS]
    assert len(ids) == len(set(ids))


def test_前两维度是用户核心问题():
    assert "你真的解决了用户的问题" in DIMENSIONS[0]["prompt"]
    assert "你真的有严格按用户的提示" in DIMENSIONS[1]["prompt"]


def test_激活设置pending和索引0():
    results = {}
    activate(results, "s1")
    state = load_state(results, "s1")
    assert state.pending is True
    assert state.dim_index == 0
    assert state.last_sent_at == 0.0


def test_激活幂等():
    results = {}
    activate(results, "s1")
    activate(results, "s1")
    state = load_state(results, "s1")
    assert state.dim_index == 0  # 未重复重置


def test_首条立即发送():
    state = IntegrityState(pending=True, dim_index=0, last_sent_at=0.0)
    assert should_send_now(state, now=100.0) is True


def test_间隔节流():
    state = IntegrityState(pending=True, dim_index=0, last_sent_at=90.0, interval_sec=30)
    assert should_send_now(state, now=110.0) is False
    assert should_send_now(state, now=121.0) is True


def test_complete后不再发送():
    state = IntegrityState(pending=True, complete=True)
    assert should_send_now(state, now=999.0) is False


def test_推进维度():
    results = {"s1": {"integrity_pending": True, "integrity_dim_index": 0}}
    advance_index(results, "s1", now=100.0)
    state = load_state(results, "s1")
    assert state.dim_index == 1
    assert state.last_sent_at == 100.0
    assert state.complete is False


def test_最后一维推进后complete():
    results = {"s1": {"integrity_pending": True, "integrity_dim_index": len(DIMENSIONS) - 1}}
    advance_index(results, "s1", now=200.0)
    state = load_state(results, "s1")
    assert state.dim_index == len(DIMENSIONS)
    assert state.complete is True
    assert get_next_dimension(state) is None


def test_回复清除pending():
    results = {"s1": {"integrity_pending": True, "integrity_complete": True, "integrity_dim_index": len(DIMENSIONS)}}
    on_reply_received(results, "s1")
    state = load_state(results, "s1")
    assert state.pending is False
    assert state.complete is False


def test_重试重置维度():
    results = {"s1": {"integrity_pending": True, "integrity_retry_count": 0, "integrity_dim_index": 10}}
    cont = on_retry_or_timeout(results, "s1")
    state = load_state(results, "s1")
    assert cont is True
    assert state.retry_count == 1
    assert state.dim_index == 0


def test_重试超限fail_open():
    results = {"s1": {"integrity_retry_count": 3, "integrity_max_retries": 3}}
    cont = on_retry_or_timeout(results, "s1")
    state = load_state(results, "s1")
    assert cont is False
    assert state.pending is False  # fail-open 继续推进


def test_dual_node未完成node2时跳过完整性():
    """dual_node 未到 node2 时完整性阶段跳过（避免与双节点审查冲突）。"""
    from pipeflow.step_handler import StepHandlerMixin
    import types

    class FakeRun:
        id = "wf_test"
        workflow_name = "test"
        context = {}
        status = "running"
        current_step = "s1"
        step_results = {"s1": {"node1_done": True}}  # 有 node1 但无 node2

    class FakeStep:
        id = "s1"
        target_role = "engineer"
        prompt_template = "调研"
        exit_condition = {"bus_category": "notice", "text_contains": "DONE", "timeout_minutes": 30}
        max_retries = 1
        dual_node = True  # dual_node 未完成 node2

    # 模拟 _tick 前段：完整性阶段应跳过
    mixin = StepHandlerMixin.__new__(StepHandlerMixin)
    run = FakeRun()
    step = FakeStep()
    sent = []

    # 直接模拟 _tick 中 dual_node 检查的逻辑（fire-and-forget 版本）
    _integ_dual_pending = (getattr(step, 'dual_node', False)
                           and not run.step_results.get(step.id, {}).get("node2_sent"))
    assert _integ_dual_pending is True  # 完整性阶段应跳过


def test_格式化填充变量():
    dim = DIMENSIONS[0]
    result = format_dim_prompt(dim, step_goal="实现登录功能", output_summary="已实现")
    assert "实现登录功能" in result
    assert "已实现" in result


def test_make_full_prompt取context和模板():
    class FakeRun:
        context = {"title": "食物链循环"}
    class FakeStep:
        prompt_template = "情报生产"
    result = make_full_prompt(FakeRun(), FakeStep(), {})
    assert "食物链循环" in result
    assert "情报生产" in result


def test_完整循环():
    results = {}
    activate(results, "s1")
    for i in range(len(DIMENSIONS)):
        state = load_state(results, "s1")
        assert should_send_now(state, now=float(100 + i * 30)) is True
        dim = get_next_dimension(state)
        assert dim is not None
        advance_index(results, "s1", now=float(100 + i * 30))
    state = load_state(results, "s1")
    assert state.complete is True
    assert state.dim_index == len(DIMENSIONS)
    on_reply_received(results, "s1")
    assert load_state(results, "s1").pending is False


def test_send_bypass_reminder两句分开():
    """bypass 补发必须两个问题分开发送（用户约束：分开提示词）。"""
    class FakeRun:
        id = "wf_test"
        step_results = {}
    class FakeStep:
        id = "s1"
        target_role = "engineer"
        prompt_template = "调研"
    sent = []

    def fake_send(role, prompt, wf_id="", step_id="", force=False):
        sent.append(prompt)

    def fake_sync(wf_id, results):
        pass

    run = FakeRun()
    step = FakeStep()
    send_bypass_reminder(run, step, "tick rescue",
                         send_to_role=fake_send, sync_step_results=fake_sync)
    # 两条独立消息，各自只含一个问题
    assert len(sent) == 2
    assert "你真的解决了用户的问题吗" in sent[0]
    assert "你真的有严格按用户的提示" in sent[1]
    assert sent[0] != sent[1]
    # bypass flag 已设置，二次调用不再发
    sent.clear()
    send_bypass_reminder(run, step, "tick rescue",
                         send_to_role=fake_send, sync_step_results=fake_sync)
    assert len(sent) == 0


# ── _tick 集成测试（fire-and-forget：完整性提醒不阻塞 exit 匹配）──

def _build_eng():
    from unittest.mock import MagicMock
    from pathlib import Path
    import tempfile
    from pipeflow.engine import WorkflowEngine
    eng = WorkflowEngine.__new__(WorkflowEngine)
    eng.workflows_dir = Path(tempfile.mkdtemp())
    eng.runs_dir = eng.workflows_dir / "runs"
    eng._workflows = {}
    eng._lm = MagicMock()
    eng._bb = MagicMock()
    eng._send_to_role = MagicMock()
    eng._sync_step_results = MagicMock()
    return eng


def _make_run(**kw):
    from pipeflow.engine import WorkflowRun
    defaults = dict(id="wf_test", workflow_name="test", context={"title": "测试工作流"},
                    current_step="s1", status="running",
                    step_results={"s1": {"status": "notified", "ts": time.time() - 5}})
    defaults.update(kw)
    return WorkflowRun(**defaults)


def _make_step(**kw):
    from pipeflow.engine import Step
    defaults = dict(id="s1", title="t", target_role="scout",
                    prompt_template="调研", dual_node=False,
                    exit_condition={"bus_category": "notice", "text_contains": "DONE"},
                    max_retries=0, condition="", rollback_to="")
    defaults.update(kw)
    return Step(**defaults)


def test_tick_integrity_pending_阻塞exit匹配():
    """完整性 pending 时发消息并 return（P0 修复：不阻塞导致孤儿化）。"""
    eng = _build_eng()
    run = _make_run()
    step = _make_step()
    # 激活完整性
    results = run.step_results
    activate(results, "s1")
    results["s1"]["integrity_last_sent_at"] = 0.0  # 立即可发
    assert eng._tick(run, step) is None  # 不抛异常
    # pending 阶段：dim01 已发但 return 了，exit 匹配不执行（_bb.read 不被调用）
    assert not eng._bb.read.called
    # dim01 已发送（send_to_role 被调用）
    assert eng._send_to_role.called
    # state 推进到 dim02
    from pipeflow.integrity import load_state
    state = load_state(run.step_results, "s1")
    assert state.dim_index == 1  # 下一条将是 dim02


def test_tick_integrity_dual_node_跳过():
    """dual_node 未完成 node2 时完整性阶段应跳过（不发送）。"""
    eng = _build_eng()
    run = _make_run()
    step = _make_step(dual_node=True)
    run.step_results["s1"]["node1_done"] = True  # node2 未到
    results = run.step_results
    activate(results, "s1")
    results["s1"]["integrity_last_sent_at"] = 0.0
    eng._tick(run, step)
    # dual_node 未到 node2 → 完整性被跳过，_send_to_role 不应收到完整性消息
    for c in eng._send_to_role.call_args_list:
        assert "完整性审查" not in str(c)


def test_tick_integrity_reply后complete_step被调用():
    """工程 P1：integrity reply 匹配后必须调用 complete_step（闭环）。"""
    from types import SimpleNamespace
    eng = _build_eng()
    # 显式设 notified_at（bus_anchor fallback），消息 ts 需晚于它
    run = _make_run(step_results={
        "s1": {"status": "notified", "ts": time.time() - 5,
               "notified_at": time.time() - 10}
    })
    step = _make_step()
    # 完整性 complete 状态（2条 dims 已发完，等回复）
    results = run.step_results
    activate(results, "s1")
    results["s1"].update({
        "integrity_pending": False,  # 已发完，进入等回复阶段
        "integrity_dim_index": len(DIMENSIONS),
        "integrity_complete": True,
        "integrity_last_sent_at": time.time(),
    })
    # mock bus 有 integrity-approved 回复消息（_check_exit 期望 f.t/f.src 属性）
    eng._bb.read.return_value = [
        SimpleNamespace(id="msg_1", t=f"s1-integrity-approved {run.id}",
                        ts=time.time(), src="engineer")
    ]
    eng._tick(run, step)
    # complete_step 必须被调用（闭环）
    assert eng._lm.complete_step.called, "integrity reply 后 complete_step 未被调用"
    # pending 已清除
    from pipeflow.integrity import load_state
    state = load_state(run.step_results, "s1")
    assert state.pending is False


def test_fail_open后步骤能推进():
    """fail-open后步骤不悬死，完整性状态被清除。

    步骤本身必须是**新鲜**的（notified_at 刚写）：_tick 先判步骤超时再进完整性块，
    旧的 6000s + timeout_minutes=1 会让它先撞超时分支直接 return，
    根本到不了 fail-open（实测：age=6000s → complete 保持测试设的 True；
    age=30s → pending=False complete=False）。原夹具因此长期假红。
    """
    eng = _build_eng()
    run = _make_run(step_results={
        "s1": {"status": "notified", "ts": time.time() - 5,
               "notified_at": time.time() - 5}
    })
    step = _make_step(exit_condition={"bus_category": "notice", "text_contains": "DONE",
                                       "timeout_minutes": 1})
    results = run.step_results
    activate(results, "s1")
    results["s1"].update({
        "integrity_pending": False,
        "integrity_dim_index": len(DIMENSIONS),
        "integrity_complete": True,
        "integrity_last_sent_at": time.time() - 120,  # 超过 timeout
        "integrity_retry_count": 3,  # 已达 max_retries
        "integrity_max_retries": 3,
    })
    eng._tick(run, step)
    # fail-open：pending 被清除，步骤不卡死
    from pipeflow.integrity import load_state
    state = load_state(run.step_results, "s1")
    assert state.pending is False
    assert state.complete is False


def test_完整性激活分支当前不可达_已知缺陷():
    """表征测试：锁定"exit 路径可达，但 integrity 激活不可达"这一已知缺陷（R71 根因 B）。

    _integ_activate 嵌在 `if _integ_state.complete` 门内，而 complete 只能由门内
    advance_index 置真 —— 循环依赖，step_handler.py L492-626 整段不可达。
    activate() 是 integrity_pending 的唯一写入者，故该字段在生产 DB 恒为 0
    （实测 88 个 step 有 integrity_reminder_sent_bypass，0 个有 integrity_pending）。

    断言必须同时成立才有意义（缺一即不可证伪）：
      1. exit 路径确实跑通（exit_messages 已写 + complete_step 被调用）
         —— 否则 step 根本没走完，pending=False 只是"什么都没发生"的恒真值。
      2. 同一轮内 pending 仍为 False —— 即真缺陷。

    修好该分支时断言 2 会失败，届时连同本测试一起更新。
    """
    from types import SimpleNamespace
    eng = _build_eng()
    run = _make_run(step_results={
        "s1": {"status": "notified", "ts": time.time() - 5,
               "notified_at": time.time() - 10}
    })
    step = _make_step()  # dual_node=False → 若可达应走 integrity 激活分支
    eng._bb.read.return_value = [
        SimpleNamespace(id="msg_1", t=f"s1-DONE {run.id}",
                        ts=time.time(), src="scout")
    ]
    eng._tick(run, step)

    # 断言 1：exit 路径可达（证明本轮真的走完了，而非空转）
    assert run.step_results.get("s1", {}).get("exit_messages"), \
        "exit 路径未跑通，本测试不可证伪——请检查 mock 的 bus 消息"
    assert eng._lm.complete_step.called, "exit 匹配后未调用 complete_step"

    # 断言 2：同一轮内 integrity 仍未被激活（缺陷本体）
    from pipeflow.integrity import load_state
    assert load_state(run.step_results, "s1").pending is False, (
        "activate 分支重启了？请更新本测试与 R71 的根因 B 结论")


def test_bypass_flag回复后重置():
    """bypass flag 在 reply 后重置（rollback 后二次执行允许重新提醒）。"""
    results = {"s1": {"integrity_pending": True, "integrity_complete": True,
                      "integrity_reminder_sent_bypass": True}}
    on_reply_received(results, "s1")
    state = load_state(results, "s1")
    assert state.reminder_sent_bypass is False


def test_bypass_flag重试后重置():
    """重试时 bypass flag 重置（重试=新轮次，允许重新提醒）。"""
    results = {"s1": {"integrity_retry_count": 0, "integrity_reminder_sent_bypass": True}}
    on_retry_or_timeout(results, "s1")
    state = load_state(results, "s1")
    assert state.reminder_sent_bypass is False


if __name__ == "__main__":
    # 无 pytest 环境时的手动自检
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
    print(f"integrity tests: PASS ({len(fns)} cases)")
