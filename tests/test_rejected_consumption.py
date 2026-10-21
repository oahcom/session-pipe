"""rejected 步骤消费的回归守卫（2026-10-19）。

对应根因：rejected 曾是只写不读的死状态 —— 写入点 2 处（manager.py 的
confirm_step 与 _advance_unsafe），读取点 0 处。engine.run_once 主扫描的四
个分支（空/审批/notified+running/step_done_ready+completed）全不匹配它，
cleanup 五条件也全假 → 静默卡死至 24h 全局兜底。

本守卫覆盖：
  ① _parse_review_issues 对 evidence 形态（角色真实书写）的解析
  ② 出口决策逻辑：有 rework_ids → 等回流；无 → 回退重投 + 清锚点
"""
import os
import sys
import json
import time
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeflow.wf_lifecycle import WorkflowLifecycleMixin as W
from pipeflow.engine import (_MAX_REJECTED_RETRY, _REJECTED_WAIT_MAX, _REQUEUE_DROP,
                             WorkflowEngine, _rejected_wait_anchor, _rework_exit)
from test_workcycle_e2e import _cleanup, _make_env, _step, _write_wf


class TestParseReviewIssuesEvidence:
    """evidence 形态（角色真实书写）解析。

    角色实际写「【系统问题→maintainer】」（带流向标注）且 bus evidence 常被
    压成单行。旧实现要求全串「【系统问题】」独占一行 → 恒不命中。
    """

    def test_arrow_annotation_parsed(self):
        sys_txt, biz_txt, sys_role, biz_role = W._parse_review_issues(
            "【系统问题→maintainer】\n- P1 | exit_condition 与 prompt 不一致"
        )
        assert sys_txt == ["P1 | exit_condition 与 prompt 不一致"]
        assert sys_role == "maintainer", "箭头角色必须解析出来用于返工派发"
        assert biz_role == ""
        assert biz_txt == []

    def test_single_line_compressed_parsed(self):
        """单行压缩形态（bus evidence 常见）。"""
        sys_txt, biz_txt, _, _ = W._parse_review_issues(
            "【系统问题】P2|任务未注入 wf_id; P2|引擎 s4 text 带模板前缀契约漂移"
        )
        assert "P2|任务未注入 wf_id" in sys_txt
        assert any("契约漂移" in x for x in sys_txt)
        assert biz_txt == []

    def test_biz_section_parsed(self):
        sys_txt, biz_txt, _, _ = W._parse_review_issues(
            "【业务问题】\n- P1 | 返回值错误 | chain.py:68"
        )
        assert biz_txt == ["P1 | 返回值错误 | chain.py:68"]
        assert sys_txt == []

    def test_arrow_target_dispatches_to_documenter(self):
        """【业务问题→product_architect】：真实驳回（bus #415040/#415594）
        指向文档缺陷，旧实现一律建 engineer_feature → 工程改代码、文档没动
        → 必然再驳回。"""
        _, biz_txt, _, biz_role = W._parse_review_issues(
            "【业务问题→product_architect】\n- P1 | AC6 无对应步骤 | DESIGN.md:121"
        )
        assert biz_txt and biz_role == "product_architect"

    def test_arrow_alias_pa_normalized(self):
        """角色写 PA 缩写时归一为 persona name（bus #413276）。"""
        _, _, _, biz_role = W._parse_review_issues(
            "【业务问题→PA】\n- P1 | s3/s4 退出契约缺模板前缀"
        )
        assert biz_role == "product_architect"

    def test_colon_style_parsed(self):
        """行内叙述形态「系统问题: ...」无括号无列表前缀（bus #415484）。"""
        sys_txt, _, _, _ = W._parse_review_issues(
            "结论维持 CHANGES_REQUESTED。系统问题: 引擎对 rejected 步骤反复重臂, 属空转"
        )
        assert sys_txt and "反复重臂" in sys_txt[0]

    def test_bare_rejection_not_parsed(self):
        """裸驳回（无分类段）不得解析出任何问题。"""
        sys_txt, biz_txt, sys_role, biz_role = W._parse_review_issues(
            "DONE_S1 CHANGES_REQUESTED: 3 P1 需修复")
        assert sys_txt == []
        assert biz_txt == []
        assert sys_role == "" and biz_role == ""

    def test_empty_not_parsed(self):
        assert W._parse_review_issues("") == ([], [], "", "")


class TestRequeueResetForReview:
    """回退重投必须清掉的字段集合（防锚点/链集合残留致自旋）。

    出口决策已抽为 engine 真函数 _rework_exit 与常量 _REQUEUE_DROP，
    此处不再复刻 —— 复刻曾与引擎漂移（漏 rework_ids）。
    """

    def test_requeue_clears_rejected_fields(self):
        """回退重投必须清 rejected 字段与 bus 锚点（否则无限循环）。"""
        sr = {
            "status": "rejected",
            "rejected_at": 100.0,
            "rejected_by": "reviewer",
            "rejection_reason": "P1 需修复",
            "poll_since": 100.0,
            "bus_anchor": 100.0,
            "notified_at": 100.0,
        }
        DROP = _REQUEUE_DROP
        reset = {k: v for k, v in sr.items() if k not in DROP}
        reset["rejected_retry"] = (sr.get("rejected_retry", 0) or 0) + 1
        reset["status"] = ""
        assert reset["status"] == ""
        assert reset["rejected_retry"] == 1
        for f in _REQUEUE_DROP:
            if f == "status":
                continue  # status 是「重置为空串」而非删除
            assert f not in reset, f"字段 {f} 未清除 → 锚点/链集合残留致自旋"


def _rework_action(sts: list, retry: int) -> str:
    """返工链终态 + 重投次数 → 出口。真函数直调，不再复刻。"""
    if _rework_exit(sts) == "requeue" and retry > _MAX_REJECTED_RETRY:
        return "escalate_capped"
    return _rework_exit(sts)


class TestReworkReflow:
    """返工链回流出口：只有「已完成」才算修复交付（防自旋）。"""

    def test_all_completed_requeues_for_review(self):
        """全部 completed = 修复已交付 → 重审原步。"""
        assert _rework_action(["completed", "completed"], 1) == "requeue"

    def test_cancelled_rework_escalates_not_requeues(self):
        """cancelled 返工链 = 修复没做完。

        回归自旋：旧实现把 cancelled 也当「终态 → 重审」，而产物一字未改
        → 必然再被驳回 → 再建链。实测 wf_3f71a2a78 8 分钟 retry 涨到 6。
        """
        assert _rework_action(["cancelled"], 1) == "escalate_unfixed"

    def test_failed_rework_escalates(self):
        assert _rework_action(["failed"], 1) == "escalate_unfixed"

    def test_partial_completion_still_escalates(self):
        """系统链 completed + 业务链 cancelled → 仍有未交付 → 升级。"""
        assert _rework_action(["completed", "cancelled"], 1) == "escalate_unfixed"

    def test_running_rework_waits(self):
        """链仍在跑 → 本步保持等它，不得重投。"""
        for s in ("running", "pending", "step_done_ready", "delivery_blocked"):
            assert _rework_action([s], 1) == "wait", f"{s} 应等待而非重投"

    def test_retry_cap_escalates(self):
        """重投封顶：无返工链时反复重投同样产物 → 必然再驳回。"""
        assert _rework_action([], _MAX_REJECTED_RETRY) == "requeue"
        assert _rework_action([], _MAX_REJECTED_RETRY + 1) == "escalate_capped"


class TestEscalateRejectedAnchor:
    """升级幂等锚：投递未成功不得落锚（否则本轮升级永久丢失）。"""

    def test_wait_max_bounded_and_anchor_dropped_on_requeue(self):
        """等返工链必须有上限；升级锚必须随重投清。

        返工链自身无 timeout（实测全 31 个模板 timeout=None），链内卡一步父链
        就永远等，出路只剩 24h 全局兜底 cancel——届时已推进的产物全部陪葬。
        """
        assert _REJECTED_WAIT_MAX > 0, "等返工链无上限 → 只能熬 24h 兜底"
        assert "rework_escalated_at" in _REQUEUE_DROP, (
            "升级锚不随重投清 → 下一轮升级被永久抑制（该发的消息一条不发）")

    def test_wait_anchor_falls_back_when_rejected_at_missing(self):
        """rejected_at 缺失时仍须能触发——否则唯一的保护点 fail-open。

        审批超时兜底路径不写 rejected_at（只有 manager.py:481 角色 confirm
        驳回会写）。实测唯一需要本保护的实例 wf_256be862ea86/s3 它的值正是
        None；若只用 `or time.time()` 差值恒 0 → 永不升级。
        直调引擎真函数——复刻会与引擎漂移（本文件 :95 已记过同型事故）。
        """
        now = time.time()
        old = now - _REJECTED_WAIT_MAX - 60

        # A：唯一的真实形态——rejected_at 缺失，退化到 notified_at
        sr_a = {"status": "rejected", "rejected_at": None, "notified_at": old}
        assert _rejected_wait_anchor(sr_a, {}) == old, "notified_at 回退未生效"
        assert now - _rejected_wait_anchor(sr_a, {}) > _REJECTED_WAIT_MAX

        # B：两个字段都缺 → 退化到实例创建时间
        assert _rejected_wait_anchor({}, {"created_at": old}) == old

        # C：首选字段在则优先用它
        sr_c = {"rejected_at": now - 10, "notified_at": old}
        assert _rejected_wait_anchor(sr_c, {"created_at": old}) == now - 10

        # D：全缺 → None（调用方 `if _since` 拦住，不得误报超时）
        assert _rejected_wait_anchor({}, {}) is None
        assert _rejected_wait_anchor({}, None) is None

    @patch("subprocess.run",
           return_value=MagicMock(returncode=0, stdout="claude\n", stderr=b""))
    def test_escalation_preserves_concurrently_written_rework_ids(self, _mock_run):
        """升级写锚不得抹掉同 tick 刚落的 rework_ids。

        wait 分支紧跟 _create_rework_subflow，同一 tick 内可达；建链走自己的 CAS
        写 rework_ids，而本地 results（tick 起始快照）里没有它（建链前提就是
        `not _rej_sr.get("rework_ids")`）。若升级擕整份 results 写回，刚落的
        rework_ids 被抹 → 下 tick 变空 → 再建一条同角色链，返工翻倍。
        """
        d = _make_env()
        try:
            _write_wf(d, "esc_clobber", [_step("s1")])
            eng = WorkflowEngine(d)
            wf_id = eng.start("esc_clobber", {})
            lm = eng._lifecycle

            # 模拟：DB 里 s1 已有建链刚落的 rework_ids；本地 results 是旧快照
            lm.execute("UPDATE workflow_instances SET step_results=? WHERE instance_id=?",
                       (json.dumps({"s1": {"status": "rejected",
                                           "rework_ids": ["wf_rw_concurrent"]}},
                                  ensure_ascii=False), wf_id))
            results = {"s1": {"status": "rejected"}}   # 无 rework_ids
            eng._send_to_role = lambda *a, **k: True
            eng._escalate_rejected({"instance_id": wf_id, "created_at": time.time()},
                                   "s1", results["s1"], results, lm, "clobber 测试")

            got = json.loads(lm.query_one(
                "SELECT step_results FROM workflow_instances WHERE instance_id=?",
                (wf_id,))["step_results"])
            assert got["s1"].get("rework_ids") == ["wf_rw_concurrent"], (
                "升级擕整份旧快照写回，抹掉了并发落的 rework_ids → 下 tick 重建链")
            assert got["s1"].get("rework_escalated_at"), "锚未落库 → 每 tick 重发"
        finally:
            _cleanup(d)

    @patch("subprocess.run",
           return_value=MagicMock(returncode=0, stdout="claude\n", stderr=b""))
    def test_anchor_set_only_after_confirmed_delivery(self, _mock_run):
        """False/busy 皆非送达，不得落锚：落了锚则本轮升级丢失且不再重试。

        subprocess 必须 mock：不 mock 则 eng.start 走 _wait_for_role_ready，
        真的去 `ccs start test_role`（不在注册表）→ 阻塞到 pytest 超时。
        """
        d = _make_env()
        try:
            _write_wf(d, "esc_anchor", [_step("s1")])
            eng = WorkflowEngine(d)
            inst = {"instance_id": eng.start("esc_anchor", {})}
            results = {'s1': {'rejected_at': 1.0}}
            sr, lm = results['s1'], eng._lifecycle

            for not_delivered in (False, 'busy'):
                eng._send_to_role = lambda *a, _r=not_delivered, **k: _r
                eng._escalate_rejected(inst, 's1', sr, results, lm, '锚测试-未送达')
                assert sr.get('rework_escalated_at'), (
                    f'投递={not_delivered!r} 非送达却未落锚（改走 bus）→ 每 tick 重发升级')

            eng._send_to_role = lambda *a, **k: True
            eng._escalate_rejected(inst, 's1', sr, results, lm, '锚测试-已送达')
            assert sr.get('rework_escalated_at'), '送达后必须落锚防每 tick 刷屏'
            assert eng.status(inst['instance_id'])['results']['s1'].get(
                'rework_escalated_at'), '锚必须落库：只改内存副本则下轮重读又是 None'

            sent = []
            eng._send_to_role = lambda *a, **k: sent.append(1) or True
            eng._escalate_rejected(inst, 's1', sr, results, lm, '锚测试-二次')
            assert sent == [], '锚已存在仍重发 → 每 tick 刷屏'
        finally:
            _cleanup(d)
