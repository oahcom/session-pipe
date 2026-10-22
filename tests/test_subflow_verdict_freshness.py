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


def test_long_attribution_phrase_not_counted_as_approval():
    """转述排除不能被长距离绕过：「上游 s1 的最终验收结论为 APPROVED」不算本步通过。"""
    assert not _superseded_by_parent(
        _parent(("上游 s1 的最终验收结论为 APPROVED", SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_earlier_approved_plus_later_mixed_message_blocks():
    """P0 锁定：更早的 APPROVED + 更晚的混合消息 → 必须仍拦。

    只放一条混合消息时旧实现也返回 False（它自己的 verdict 就是驳回），
    真正能抓 P0 的是「更早有一条 APPROVED」——只要跳过最新那条就回落到它。
    """
    assert not _superseded_by_parent(
        _parent((APPROVED, SUB_TS + 100),
                ("full_dev_pipeline/s4-complete —— APPROVED 但 CHANGES_REQUESTED: 2 P1",
                 SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_later_rejection_in_body_blocks():
    """更晚那条的驳回写在正文（首行只是标题）也必须拦（codex P1）。"""
    assert not _superseded_by_parent(
        _parent((APPROVED, SUB_TS + 100),
                ("full_dev_pipeline/s4-complete — 代码审查完成\n\n"
                 "结论：CHANGES_REQUESTED: 3 P1", SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_long_attribution_phrase_with_timestamp_not_approval():
    """转述句再长也不算本步通过：加 24 字上限会被时间戳绕开（codex P1）。

    实测旧窗口对该句返回 True：前缀 30 字，超出 24 字窗口。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion(
        "上游 s1 的最终验收结论已在 10:20 复核之后确认为 APPROVED") is None
    assert not _superseded_by_parent(
        _parent(("上游 s1 的最终验收结论已在 10:20 复核之后确认为 APPROVED",
                 SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_delta_mentioning_rejection_is_still_narrative():
    """delta 正文提到的旧驳回是返工记录，不是本步驳回（codex P2）。

    若先判全文驳回，返工后的 delta 会成为「最新的结论=驳回」，
    父步更晚的 APPROVED 永远压不过它（#439027 的陈旧驳回复现）。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion("full_dev_pipeline/s4-complete — delta\n\n"
                       "已修复 CHANGES_REQUESTED 的 3 项") is None
    assert _superseded_by_parent(
        _parent(("full_dev_pipeline/s4-complete — delta\n\n已修复 CHANGES_REQUESTED 的 3 项",
                 SUB_TS + 50),
                ("full_dev_pipeline/s4-complete — APPROVED", SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_delta_first_line_progress_words_not_approval():
    """delta 首行里的「构建通过」「产出已就绪」是进度，不是通过结论（codex P1/P3）。

    旧版对 delta 首行直接跑 review_gate.verdict，词表含「通过/就绪」→ True，
    于是一条 delta 就能把被子链驳回的父步放行。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion("full_dev_pipeline/s4-complete — delta: 构建通过，剩余 3 项待修") is None
    assert _conclusion("full_dev_pipeline/s4-complete — delta: 产出已就绪") is None
    assert _conclusion("full_dev_pipeline/s4-complete — delta: 已修复 CHANGES_REQUESTED 的 3 项") is None
    assert _conclusion("full_dev_pipeline/s4-complete — delta: 上游 s3 APPROVED 的 2 项已合并") is None


def test_attribution_before_dash_does_not_block_real_approval():
    """结论段之前的转述不影响结论段：真通过要放行（codex P2）。

    生产标题形如「wf_x/s4-complete — 参考上游 reviewer 的 3 项 P1 全部返工 — APPROVED」，
    把左窗口扩到整行会把这种真实通过吞掉，返工后的步继续被陈旧驳回卡住。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion(
        "wf_413e05355b/s4-complete — 参考上游 reviewer 的 3 项 P1 意见全部返工完成 — APPROVED") is True
    assert _superseded_by_parent(
        _parent(("wf_413e05355b/s4-complete — 参考上游 reviewer 的 3 项 P1 意见全部返工完成 — APPROVED",
                 SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_delta_with_hard_rejection_is_still_rejection():
    """delta 段自带硬驳回、且没写「已修复」时，仍是驳回（codex 第 8 轮 P2）。

    早退成 None 会让守卫跳过这条更晚的驳回、回落到更早的 APPROVED，
    把被驳回的步盖成 completed。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion("s4-complete — delta: CHANGES_REQUESTED 3 项未修复") is False
    # 英文否定词不构成「已修复」（第 9 轮 P2：unfixed/unresolved 曾被当成 fixed）
    assert _conclusion("s4-complete — delta: CHANGES_REQUESTED 3 items unfixed") is False
    assert _conclusion("s4-complete — delta: 已修复 CHANGES_REQUESTED 的 3 项") is None
    assert not _superseded_by_parent(
        _parent(("APPROVED", SUB_TS + 100),
                ("s4-complete — delta: CHANGES_REQUESTED 3 项未修复", SUB_TS + 200)),
        _sub("CHANGES_REQUESTED"))


def test_approved_next_to_chinese_still_counts():
    """「复核APPROVED」这类中文紧贴的通过词不能被 lookbehind 误伤（codex 第 8 轮 P3）。"""
    from pipeflow.task_generator import _conclusion
    assert _conclusion("s4-complete — 复核APPROVED") is True
    assert _conclusion("s4-complete — 已APPROVED") is True
    assert _conclusion("s4-complete — s1-integrity-approved") is None


def test_conditional_approved_counts_as_pass():
    """CONDITIONAL_APPROVED 是生产里主流的通过形态，不是叙述。

    实测 47 个带该结论的步骤状态全是 completed / step_done_ready；判成叙述会让
    父步的更新结论永远压不过子链旧驳回（#439027 那类卡死）。
    """
    from pipeflow.task_generator import _conclusion
    assert _conclusion(
        "reviewer_pr_review/s1-complete wf_sub_83b60ec1e2f9 — 结论: CONDITIONAL_APPROVED 0 P0 / 2 P1") is True
    assert _conclusion("WL-food_chain_loop/s6-complete wf_x — 结论: CONDITIONAL_APPROVED 0 P0 / 2 P1") is True
    assert _conclusion("上游 s3 CONDITIONAL_APPROVED") is None
    assert _conclusion("s4-complete — CONDITIONAL_APPROVED 不成立，CHANGES_REQUESTED 2 P1") is False


def test_quoted_approval_in_title_not_superseding():
    """标题形态（模板前缀）里转述上游的通过不算本步结论（codex P2）。"""
    assert not _superseded_by_parent(
        _parent(("full_dev_pipeline/s4-complete — 参照上游 s3 APPROVED，本步待返工",
                 SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


def test_approval_in_body_not_a_conclusion():
    """APPROVED 只出现在正文里时不是本步的通过结论。

    exit_messages 的 text 是「首行标题 + 空行 + 正文」，旧实现扫全文，
    正文里带过的 APPROVED 就能让被驳回的返工步整步放行。
    """
    assert not _superseded_by_parent(
        _parent(("full_dev_pipeline/s4-complete — delta\n\n"
                 "s3 APPROVED 的两项已合并，剩余 P1 待修", SUB_TS + 100)),
        _sub("CHANGES_REQUESTED"))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
