"""宽松推进不得抢在正常 exit 匹配之前吞掉合规消息（bus #457649 P2）。

背景：_relaxed 只筛 src/时间窗/实例 id，不筛 cat。步骤超时 >=1 次后，角色写出的
**完全合规**完成消息（cat/src/text 全中）会先被宽松推进吞下，并写一条
「分类不匹配（期望 code_review）」的 blocker —— 文案与事实相反，下游据此误判角色失职。
生产实例 wf_4eaa331e88ed/s6：cat=code_review src=reviewer 三项全中，仍 relaxed_advance=True。

修法：_relaxed 排除「cat 与文本都已匹配」的消息，把这类留给本 tick 后面的正常 exit 匹配。

Run: cd ~/session-pipeline && python3 -m pytest tests/test_relaxed_advance_cat_order.py -q
"""
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeflow.engine import Step, WorkflowRun  # noqa: E402

from test_exit_condition import _mocked_eng  # noqa: E402

_WF = "wf_relaxed_order_test"


class _Fact:
    def __init__(self, id, cat, src, t, ts, e=""):
        self.id, self.cat, self.src, self.t, self.ts, self.e = id, cat, src, t, ts, e


def _run_and_step():
    now = time.time()
    step = Step(
        id="s6", title="代码审查", target_role="reviewer",
        prompt_template="review {topic}",
        exit_condition={"bus_category": "code_review", "source_contains": "reviewer",
                        "text_contains": "s6-complete", "timeout_minutes": 30},
        type="review", completion_check=None, max_retries=0, condition="",
        rollback_to="", verify="", failure_patterns=None, subflow_template="",
        exit_schema=None,
    )
    # ts 放在 40 分钟前 → elapsed > timeout，走超时段
    run = WorkflowRun(
        id=_WF, workflow_name="food_chain_loop", context={}, current_step="s6",
        status="running",
        # 已超时过一次（生产实例 s6 是 2）：timeout_count 递增到 2 才满足宽松推进门槛，
        # 同时跳过「首次超时重推」分支——那条路径要走 DB 取 task title，与本用例无关。
        step_results={"s6": {"status": "notified", "ts": now - 40 * 60,
                             "notified_at": now - 40 * 60, "timeout_count": 1}},
        created_at=now - 3600, updated_at=now,
    )
    return run, step


def _tick_with_facts(facts, require_wf=False):
    eng = _mocked_eng()
    eng._bb.read.return_value = facts
    eng._check_exit = MagicMock(return_value=(0.0, []))  # 本用例只验宽松推进分支
    eng._send_to_role = MagicMock()
    eng._ensure_role_alive = MagicMock(return_value=True)
    eng._wait_for_role_ready = MagicMock(return_value=False)
    eng._is_role_busy = MagicMock(return_value=False)
    run, step = _run_and_step()
    if require_wf:
        step.exit_condition["require_wf"] = True
    eng._tick(run, step)
    return eng, run


def _anchor(now):
    return _Fact(1, "task_spec", "workflow_engine",
                 f"[task] {_WF} 代码审查", now - 3000, f"实例 {_WF}")


def test_compliant_message_not_consumed_by_relaxed():
    """cat/src/text 三项全中 → 不得置 relaxed_advance（留给正常 exit 匹配）。"""
    now = time.time()
    ok = _Fact(2, "code_review", "reviewer",
               f"s6-complete {_WF} — 代码审查 CONDITIONAL_APPROVED", now - 200, f"实例 {_WF}")
    eng, run = _tick_with_facts([_anchor(now), ok])
    sr = run.step_results["s6"]
    assert not sr.get("relaxed_advance"), f"合规消息被宽松推进吞下: {sr}"
    texts = [c[0][1] for c in eng._bb.write.call_args_list]
    assert not any("分类不匹配" in t for t in texts), f"写了与事实相反的 blocker: {texts}"


def test_mismatched_cat_still_relaxed():
    """分类真的不匹配时兜底仍要生效（否则本修复会退化成死锁）。"""
    now = time.time()
    wrong = _Fact(3, "architecture", "reviewer",
                  f"s6-complete {_WF} — 审查结论", now - 200, f"实例 {_WF}")
    eng, run = _tick_with_facts([_anchor(now), wrong])
    sr = run.step_results["s6"]
    assert sr.get("relaxed_advance"), f"真分类不匹配时应兜底推进: {sr}"
    texts = [c[0][1] for c in eng._bb.write.call_args_list]
    assert any("收到 architecture" in t for t in texts), \
        f"blocker 应报出实际收到的分类: {texts}"


def test_mismatched_text_still_relaxed():
    """cat 对但 text_contains 不中 → 仍属未匹配，兜底应推进。"""
    now = time.time()
    wrong = _Fact(4, "code_review", "reviewer",
                  f"审查结论已出 {_WF}（标题里没有契约要求的关键词）", now - 200, f"实例 {_WF}")
    eng, run = _tick_with_facts([_anchor(now), wrong])
    assert run.step_results["s6"].get("relaxed_advance"), "文本不匹配时应兜底推进"


def test_compliant_plus_mismatched_does_not_relax():
    """合规消息与不匹配消息并存时，宽松推进仍不得抢跑（原实现只逐条排除）。"""
    now = time.time()
    ok = _Fact(5, "code_review", "reviewer",
               f"s6-complete {_WF} — 通过", now - 200, f"实例 {_WF}")
    wrong = _Fact(6, "architecture", "reviewer",
                  f"s6-complete {_WF} — 另一条", now - 200, f"实例 {_WF}")
    eng, run = _tick_with_facts([_anchor(now), ok, wrong])
    assert not run.step_results["s6"].get("relaxed_advance"), \
        "存在合规消息时仍被宽松推进吞下"


def test_require_wf_evidence_only_id_still_relaxes():
    """require_wf 只认标题；实例 id 只写在正文时，正常匹配拒绝 → 兜底必须接住。"""
    now = time.time()
    ev = _Fact(7, "code_review", "reviewer",
               "s6-complete — 通过", now - 200, f"实例 {_WF}")
    eng, run = _tick_with_facts([_anchor(now), ev], require_wf=True)
    assert run.step_results["s6"].get("relaxed_advance"), \
        "严格归属匹配不到、宽松推进又排除 → 两边都不推进（死锁）"


if __name__ == "__main__":
    unittest.main(verbosity=2)
