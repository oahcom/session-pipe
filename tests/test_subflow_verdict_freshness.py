"""子链判定时效守卫：父步更晚的明确通过结论覆盖子链陈旧快照。

生产事故（#439027，2026-10-22）：wf_413e05355b/s4 在 03:58 被压回 rejected，
依据是子链 wf_sub_5bb44259a715 于 20:25 的 CHANGES_REQUESTED。父步自己
01:16 已收到 reviewer 的 APPROVED 完成消息，间隔 7h32m 仍按旧快照驳回，
据此建立的返工链对象已失效。

根因：子链完成后 step_results 被冻结（整键替换；bus_anchor 锚在最早匹配，
step_handler.py:1676），而 verdict 判据没有时序概念（task_generator.py:80）。

本测试锁定守卫：子链驳回消息之后，父步若有**更晚且明确通过**的结论 →
不判驳回；更晚的驳回、delta 叙述、更早的通过均不放行。
"""
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipeflow.task_generator import _superseded_by_parent  # noqa: E402

SUB_TS = 1792585543.8252943      # 2026-10-21T12:25:43Z = 20:25:43 本地
APPROVED = ("full_dev_pipeline/s4-complete wf_413e05355b —— APPROVED "
            "业务 0P0/0P1/1P2/3P3；系统 1P1/2P2/1P3")
DELTA = ("full_dev_pipeline/s4-complete wf_413e05355b — delta: 产出已补齐，"
         "schema 已过，唯一阻塞是 verify 缺陷")
REJECTED_LATER = ("full_dev_pipeline/s4-complete wf_413e05355b —— 代码审查 "
                  "CHANGES_REQUESTED：0 P0 / 3 P1")


def _sub(text, ts=SUB_TS):
    return {"status": "completed", "exit_messages": [{"id": 1, "text": text, "ts": ts}]}


def _parent(*msgs):
    """父步本步的 step_results 条目（生产形态：一条完成消息或多轮新消息）。"""
    return {"status": "completed",
            "exit_messages": [{"id": i, "text": t, "ts": ts}
                              for i, (t, ts) in enumerate(msgs, start=100)]}


def test_later_approved_supersedes_stale_reject():
    """生产形态：子链 20:25 驳回 → 父步 01:16 APPROVED → 守卫放行。"""
    assert _superseded_by_parent(_parent((APPROVED, SUB_TS + 27_600)),
                                 _sub("CHANGES_REQUESTED: 0 P0 / 2 P1"))


def test_earlier_approved_does_not_supersede():
    """更早的通过不能覆盖更晚的驳回（时序方向必须正确）。"""
    assert not _superseded_by_parent(_parent((APPROVED, SUB_TS - 600)),
                                     _sub("CHANGES_REQUESTED"))


def test_later_reject_does_not_supersede():
    """父步更晚的驳回不得被子链驳回守卫放过（返工后再次驳回必须仍拦）。"""
    assert not _superseded_by_parent(_parent((REJECTED_LATER, SUB_TS + 600)),
                                     _sub("CHANGES_REQUESTED"))


def test_later_delta_without_verdict_does_not_supersede():
    """delta 叙述无锚点结论（verdict=None）不算通过，不得放行。

    生产父步四条完成消息里三条是 delta，只有最早那条带 APPROVED —— 若把
    「无驳回词」当作通过，守卫会在任何后续 delta 上误放行。
    """
    assert not _superseded_by_parent(_parent((DELTA, SUB_TS + 600)),
                                     _sub("CHANGES_REQUESTED"))


def test_confirmed_parent_step_ignored():
    """父步该步已审批确认 → 由既有 confirmed_at 分支处置，时效守卫不必介入。

    confirmed_at 的判定在调用点（task_generator.py 回流处 `and not _sdata.get("confirmed_at")`），
    本函数只看消息时序。
    """
    sr = _parent((APPROVED, SUB_TS + 27_600))
    sr["confirmed_at"] = SUB_TS + 30_000
    assert _superseded_by_parent(sr, _sub("CHANGES_REQUESTED")), \
        "confirmed_at 不改变时序事实；调用点已另行短路"


def test_no_parent_messages_keeps_reject():
    """父步无完成消息 → 守卫不介入，原驳回语义保持（R105 守卫不放松）。"""
    assert not _superseded_by_parent({"status": "notified"}, _sub("CHANGES_REQUESTED"))
    assert not _superseded_by_parent({}, _sub("CHANGES_REQUESTED"))


def test_message_without_ts_is_ignored():
    """缺 ts 的消息不参与时序比较（旧存量数据未被 step_handler 写 ts）。"""
    sr = {"status": "completed",
          "exit_messages": [{"id": 100, "text": APPROVED}]}
    assert not _superseded_by_parent(sr, _sub("CHANGES_REQUESTED"))


def test_latest_verdict_wins_in_mixed_batch():
    """同批更晚的驳回必须压过更早的通过（codex 复核 P0，实测旧实现返回 True）。

    父步 [(APPROVED,+100s),(CHANGES_REQUESTED,+200s)]：旧实现看到"存在一条更晚
    的 APPROVED"就放行，返工后的再次驳回被覆盖，rejected 步被置 completed。
    """
    assert not _superseded_by_parent(
        _parent((APPROVED, SUB_TS + 100), (REJECTED_LATER, SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_non_list_exit_messages_does_not_raise():
    """生产 408 行 step_results 形状非法（"exit_messages": 17）：当空处理，不冒泡。"""
    assert not _superseded_by_parent({"exit_messages": 17},
                                     _sub("CHANGES_REQUESTED"))
    assert not _superseded_by_parent({"exit_messages": [{"ts": 1, "text": APPROVED}]},
                                     {"exit_messages": 17})


def test_soft_pass_words_do_not_supersede():
    """verdict() 是词表信号不是结论：「构建通过」「产出已就绪」「没有通过审核，
    需返工」实测都返回 True（codex P0）。没有硬通过词就不是本步结论。"""
    for text in ("构建通过，继续下一步", "产出已就绪", "没有通过审核，需返工"):
        assert not _superseded_by_parent(_parent((text, SUB_TS + 100)),
                                         _sub("CHANGES_REQUESTED")), text


def test_quoted_approval_of_other_step_not_superseding():
    """转述别人的通过（上游 s3 APPROVED）不是本步结论。"""
    assert not _superseded_by_parent(
        _parent(("上游 s3 APPROVED，本步待返工", SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_same_ts_reject_wins():
    """同一时刻的通过+驳回（同批写入没有先后）按驳回算。"""
    assert not _superseded_by_parent(
        _parent((APPROVED, SUB_TS + 100), (REJECTED_LATER, SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_cutoff_uses_reject_ts_not_latest_delta():
    """子链 [驳回 t1, delta t2]：截止点取驳回时刻，父步在 t1 之后的通过仍放行。"""
    sub = {"status": "completed",
           "exit_messages": [{"id": 1, "text": "CHANGES_REQUESTED: 3 P1",
                              "ts": SUB_TS + 100},
                             {"id": 2, "text": "delta: 说明补丁范围", "ts": SUB_TS + 900}]}
    assert _superseded_by_parent(_parent((APPROVED, SUB_TS + 300)), sub), \
        "用 delta 的 ts 当截止点会把 t1~t2 之间的合法通过也拦掉"


def test_call_site_actually_uses_the_guard():
    """只测 helper 抓不到守卫被摘除：删掉调用点的
    and not _superseded_by_parent(...) 后其余用例仍全绿（codex P1）。
    这里直接钉住调用点，比给 mixin 造一整套 DB 轻量。"""
    src = (Path(__file__).resolve().parents[1] / "src/pipeflow/task_generator.py").read_text()
    assert "and not _superseded_by_parent(" in src, "调用点已不在，时效守卫等于没接"


def test_malformed_exit_messages_does_not_break_earlier_guard():
    """_subflow_verdict_rejected 在守卫之前执行：它崩了守卫永远跑不到。

    生产实测 408 行 step_results 形状非法（exit_messages 为 int/str）。
    """
    from pipeflow.task_generator import _subflow_verdict_rejected
    assert _subflow_verdict_rejected({"s1": {"exit_messages": 17}}) is False
    assert _subflow_verdict_rejected({"s1": {"exit_messages": "x"}}) is False
    assert _subflow_verdict_rejected(
        {"s1": {"exit_messages": [{"text": "CHANGES_REQUESTED"}]}}) is True


def test_mixed_approve_and_reject_in_one_message_blocks():
    """一条消息里既有 APPROVED 又有 CHANGES_REQUESTED：按驳回算（codex P0）。

    旧实现把 is_rejected 命中置 None，等于跳过最新那条，回落到更早的 APPROVED。
    """
    assert not _superseded_by_parent(
        _parent(("full_dev_pipeline/s4-complete —— APPROVED 但 CHANGES_REQUESTED: 2 P1",
                 SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_earlier_approved_plus_later_mixed_message_blocks():
    """P0 锁定：更早的 APPROVED + 更晚的混合消息 → 必须仍拦。

    只放一条混合消息时旧实现也返回 False（它的 verdict 就是 False），
    真正能抓 P0 的是"更早有一条 APPROVED"——跳过最新那条会回落到它。
    """
    assert not _superseded_by_parent(
        _parent((APPROVED, SUB_TS + 100),
                ("s4-complete —— APPROVED 但 CHANGES_REQUESTED: 2 P1", SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_long_attribution_phrase_not_counted_as_approval():
    """转述排除不能被长距离绕过：「上游 s1 的最终验收结论为 APPROVED」不算本步通过。"""
    assert not _superseded_by_parent(
        _parent(("上游 s1 的最终验收结论为 APPROVED", SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
