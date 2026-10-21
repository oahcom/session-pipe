"""宽松推进的跨实例归属校验回归（无框架，纯 assert，可直接 python3 运行）。

背景：relaxed-advance 在 exit 未匹配时兜底推进，原实现只按 (角色, 时间窗) 取消息，
归属锚点用 wf_id[:8] 前缀匹配。同前缀实例（wf_sub_4e63… / wf_sub_b548…）互相串味，
生产实测 31 次宽松推进有 29 次匹配的是**别的工作流**的消息——wf_sub_b548d0e2495a
借用了 wf_sub_4e63ad633d39 的完成声明直接盖 completed，父链据此继续推进。
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeflow.step_handler import _mentions  # noqa: E402


class _F:
    def __init__(self, t="", e=""):
        self.t, self.e = t, e


def test_cross_instance_completion_is_rejected():
    """b548 不能靠 4e63 的完成声明推进（生产实例 #418887）。"""
    fact = _F("engineer_feature/s1-complete [wf_sub_4e63ad633d39] — 门禁脚本",
              "【实例】wf_sub_4e63ad633d39（父 wf_654cd82159）")
    assert not _mentions(fact, "wf_sub_b548d0e2495a"), "跨实例完成声明必须被拒"
    assert _mentions(fact, "wf_sub_4e63ad633d39"), "本实例完成声明应通过"


def test_instance_id_in_evidence_counts():
    """实例 id 只写在 evidence 正文里时仍算本工作流产出。"""
    fact = _F("engineer_feature/s1-complete — 落地页门禁",
              "【实例】wf_sub_4e63ad633d39 (engineer_feature/s1)")
    assert _mentions(fact, "wf_sub_4e63ad633d39")


def test_bare_id_form_is_accepted():
    """角色常照抄裸 id（去掉 wf_ 前缀，与 _check_exit 同一形态），应被接受。"""
    assert _mentions(_F("完成 sub_4e63ad633d39/s1"), "wf_sub_4e63ad633d39")


def test_id_prefix_collision_does_not_match():
    """共同前缀不得当成本实例：wf_sub_4e63a 不是 wf_sub_4e63ad633d39。"""
    assert not _mentions(_F("见 wf_sub_4e63ad633d39x 的说明"), "wf_sub_4e63ad633d39")
    assert not _mentions(_F("见 wf_sub_4e63 的说明"), "wf_sub_4e63ad633d39")


def test_unrelated_message_is_rejected():
    """与本实例无关的任意消息不得推进（历史 29 例的共同形态）。"""
    for t in ("delivery_chain_init_complete",
              "[engineer] 创建任务: [deploy] wf_9dd6f826e 产出已部署",
              "[closer] 创建任务: [step_output] wf_7110db1f8/s2 reviewer 产出审计"):
        assert not _mentions(_F(t), "wf_ae31fd1c4c11"), f"无关消息不应推进: {t}"


def test_empty_wf_id_never_matches():
    """无 id 调用必须拒绝（旧的 `'wf_' in t` 写法会放行无 id 消息）。"""
    assert not _mentions(_F("wf_sub_4e63ad633d39 完成"), "")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name} PASS")
    print("all relaxed-advance ownership checks passed")

def test_strict_ownership_ignores_body_mentions():
    # 严格路径（t 字段）不得因正文顺带提及本 id 而放行
    f = _F("无关标题", e="顺带提到 wf_sub_4e63ad633d39")
    assert not _mentions(f, "wf_sub_4e63ad633d39", fields="t"), "严格路径只看标题"
    assert _mentions(f, "wf_sub_4e63ad633d39"), "宽松路径含正文"

def test_subflow_verdict_only_checks_last_step():
    """历史步骤里的驳回文案不得触发末步驳回告警（codex P2）。

    子链 s1 曾被驳回、s2 重新通过后，s1 的 exit_messages 仍留在
    step_results 里。若全量扫描，每次子链完成都会误报"末步驳回"。
    """
    from pipeflow.task_generator import _subflow_verdict_rejected
    sr = {
        "s1": {"exit_messages": [{"text": "CHANGES_REQUESTED"}]},  # 历史，已确认放行
        "s2": {"exit_messages": [{"text": "s2-complete APPROVED"}]},
    }
    # 末步单独判：不含驳回词
    assert not _subflow_verdict_rejected({"s2": sr["s2"]}), "末步 APPROVED 不应判驳回"
    # 全量判会误报 —— 这正是修复前行为
    assert _subflow_verdict_rejected(sr), "全量扫描确实会命中历史驳回（说明必须只看末步）"
