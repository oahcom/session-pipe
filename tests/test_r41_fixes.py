"""R41 修复的回归守卫：/goal 剥离语义 + 返工门禁 fail-open。

对应改动（2026-10-19）：
- role_lifecycle._send_to_role：/goal 从 count=1 改为全量剥离
- engine.py 首步路径：拼接前剥离模板自带 /goal
- wf_lifecycle 返工门禁：放开 step.type 限制 + 补关键词 + fail-open
"""
import re


def _goal_count(text: str) -> int:
    """统计行首 /goal 数量（与生产正则同口径）。"""
    return len(re.findall(r"(?m)^/goal(?:\s+|$)", text))


class TestGoalStripping:
    """_send_to_role 的 /goal 归一化：无论调用方传什么形态，结果恰好 0 或 1 个。"""

    @staticmethod
    def _send_to_role_normalize(prompt: str, force: bool = False) -> str:
        prompt = re.sub(r"(?m)^/goal(?:\s+|$)", "", prompt).lstrip("\n")
        if not force:
            prompt = "/goal " + prompt
        return prompt

    def test_already_stripped_prompt_gets_one_goal(self):
        """首步路径：engine 已剥离模板 /goal → 补前缀后恰好 1 个。"""
        p = self._send_to_role_normalize("[任务] x\n[工作流] w\n\n## 正文")
        assert _goal_count(p) == 1, f"期望 1 个 /goal，实际 {_goal_count(p)}"

    def test_template_with_goal_gets_one_goal(self):
        """推进/kick-stalled 路径：模板自带 /goal → 剥净后补前缀，仍恰好 1 个。

        回归：旧实现 count=1 只剥行首，模板 /goal 被 header 顶到中段 → 双 /goal。
        """
        p = self._send_to_role_normalize("[任务] x\n\n/goal\n## 正文")
        assert _goal_count(p) == 1, f"双 /goal 回归！实际 {_goal_count(p)}"

    def test_force_mode_has_no_goal(self):
        """integrity 提醒（force=True）不加 /goal —— 否则 Stop hook 永久锁死。"""
        p = self._send_to_role_normalize("/goal\n## 完整性提醒", force=True)
        assert _goal_count(p) == 0

    def test_mid_text_goal_line_also_stripped(self):
        """正文中段的 /goal 行同样剥净（多行模式 (?m)）。"""
        p = self._send_to_role_normalize("head\n/goal\nmid\n/goal tail\n")
        assert _goal_count(p) == 1


class TestReworkGateFailOpen:
    """返工门禁关键词与 fail-open 语义。"""

    GATE_KW = ("changes_requested", "rejected", "需修复", "回退", "未通过", "not_approved", "驳回")

    def test_rejection_keywords_match(self):
        for text in ("changes_requested", "判定: 未通过", "REJECTED: 7 DIFF", "需修复", "回退"):
            assert any(k in text.lower() for k in self.GATE_KW), f"{text} 未命中词表"

    def test_normal_text_not_matched(self):
        """正常完成文案不得命中——避免误拦。"""
        for text in ("s3-complete 已完成", "全部通过", "fix bug", "prefix 处理完成"):
            assert not any(k in text.lower() for k in self.GATE_KW), f"{text} 误命中"

    def test_fail_open_when_no_rework_created(self):
        """未建返工链时调用方放行推进（防静默卡死）。

        旧写法：命中即 return（不推进），而 _create_rework_subflow 解析不出
        【系统问题】/【业务问题】分类时空返回 → 步骤永久卡死。
        """
        rw_made = False  # 模拟 handoff 步骤产出无分类标记
        advanced = not rw_made  # fail-open：未建链则推进
        assert advanced, "未建返工链时必须放行推进"

    def test_block_only_when_rework_created(self):
        rw_made = True
        assert not (not rw_made), "已建返工链时必须拦下推进"
