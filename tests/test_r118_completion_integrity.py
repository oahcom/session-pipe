"""R118 链路完整性三处实证缺陷的回归守卫（只测外部行为）。

现场（2026-10-21，只读取证）：
  A) exit_schema 通配符匹配到的全是工作流创建前的旧文件时，`*.py`+minLength
     （无 minCount）返回 ok —— 实测两条 engineer 子链 11:30-11:45 workspace
     零写入仍被判完成，父链继续推进（Q3-A）。
  B) `wf confirm --reject` 只写 rejection_reason，_create_rework_subflow 只读
     exit_messages → 驳回意见建不出返工链，重投 3 次后步骤永久停摆
     （coordinator 2026-10-20 13:15 实录）。
  C) 跨实例守卫按 WL- 前缀全名精确匹配，角色发出的标题是裸模板名 → 守卫恒
     不生效，8.7 小时前兄弟实例的消息可闭合本步（Q3 第 7 类）。

运行: cd ~/session-pipeline && python3 -m pytest tests/test_r118_completion_integrity.py -q
"""
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pipeflow.step_handler as sh  # noqa: E402
from pipeflow.step_handler import StepHandlerMixin, _claims_own_step_and_foreign_instance  # noqa: E402
from pipeflow.wf_lifecycle import WorkflowLifecycleMixin  # noqa: E402


def _eng(tmp_path, role="scout"):
    """最小引擎替身：只依赖 _validate_exit_schema 真正读的两个方法。"""
    eng = StepHandlerMixin.__new__(StepHandlerMixin)
    eng._eff_role = lambda run, step: role
    return eng


def test_stale_only_glob_fails(tmp_path, monkeypatch):
    """通配符只匹配到旧文件 = 本步没有产出，必须判失败。"""
    ws = tmp_path / "scout"
    ws.mkdir()
    old = ws / "legacy.py"
    old.write_text("x" * 500, encoding="utf-8")
    old_mtime = time.time() - 7 * 24 * 3600
    import os
    os.utime(old, (old_mtime, old_mtime))
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time(), workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is False, f"旧文件冒充产出仍被判通过: {errs}"
    assert errs, "失败时必须给出可核对的错误项"


def test_fresh_output_passes(tmp_path, monkeypatch):
    """本步真产出文件时不得误伤（守卫不为拦截而拦截）。"""
    ws = tmp_path / "scout"
    ws.mkdir()
    (ws / "new.py").write_text("y" * 500, encoding="utf-8")
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time() - 1, workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is True, f"本步新产出被判失败: {errs}"


def test_claim_matches_engine_instance_title_format():
    """引擎让角色发的完成标题是 `<实例id>/sN-complete`，不是 `<模板名>/sN-complete`。

    只认模板名时守卫对真实消息恒失效：实测 #439707 一条 db062 的 s1-complete
    同时闭合了 wf_sub_5a1305523c87 与 wf_sub_b0f5817b0e6e 的 s1（后者的 pm 从未
    写出任何字节）。反向用例保证本步自己的消息仍放行。
    """
    real = "wf_sub_db062335d6c2/s1-complete 需求确认与 PRD · 单文件落地页"
    assert _claims_own_step_and_foreign_instance(
        real, "wf_sub_b0f5817b0e6e", "pm_requirements", "s1") is True
    assert _claims_own_step_and_foreign_instance(
        real, "wf_sub_db062335d6c2", "pm_requirements", "s1") is False
    # 声称别的步骤完成不算本步（step_id 边界）
    assert _claims_own_step_and_foreign_instance(
        "wf_sub_deadbeef0001/s2-complete 故事拆分完成",
        "wf_sub_mine00000001", "pm_requirements", "s1") is False


def test_claim_requires_completion_suffix_and_ignores_own_id():
    """两侧误判的回归钉子（codex 三方审查 B1/B2）。

    B1 假阴性：出现本实例 id ≠ 没替别实例报工 → `交付给本实例` 仍要拦。
    B2 假阳性：`wf_x/s1/README` `wf_x/s1.5-complete` 是路径/版本，不是完成声明。
    """
    f = _claims_own_step_and_foreign_instance
    # B1
    assert f("wf_sub_other/s1-complete 已交付给 wf_sub_mine",
             "wf_sub_mine", "pm_requirements", "s1") is True
    # B2
    assert f("wf_sub_other/s1/README 已更新",
             "wf_sub_mine", "pm_requirements", "s1") is False


def test_claim_recognizes_chinese_completion_suffix():
    """角色自发通知写 `wf_xxx/sN 完成`（全库 1291 条完成类标题中 26 条该形态），
    模板名与实例 id 两种前缀都要认；只认 -complete 时守卫对这批消息失效
    （2026-10-22 回归：收紧 -complete 打挂 TestSoftAttribution，bus #440248）。"""
    f = _claims_own_step_and_foreign_instance
    assert f("wf_sub_deadbeef0001/s1 完成，通知下一角色进入 s2",
             "wf_sub_mine00000001", "pm_requirements", "s1") is True
    assert f("PRD 落盘 — 实例 wf_sibling999 / arch/s2 完成",
             "wf_mine111", "arch", "s2") is True
    # `完成` 后面接文字的是叙述不是完成声明，仍放行
    assert f("wf_sub_deadbeef0001/s1 完成清单见 README",
             "wf_sub_mine00000001", "pm_requirements", "s1") is False
    assert f("wf_sub_other/s1.5-complete 完成",
             "wf_sub_mine", "pm_requirements", "s1") is False


def test_claim_recognizes_chinese_completion_variants():
    """返工 s6 P2 实测漏拦 4 类：已/了/于时间 收尾，以及斜杠两侧带空格。

    这 4 条在旧后缀下全部 False —— 角色写「已完成」「完成了」「完成于 10:00」
    都是真实写法，漏拦等于给同模板并发链留同一个口子。
    """
    f = _claims_own_step_and_foreign_instance
    args = ("wf_sub_mine00000001", "pm_requirements", "s1")
    for text in ("wf_other/s1 已完成",
                 "wf_other/s1 完成了",
                 "wf_other/s1 完成于 10:00",
                 "wf_other / s1 完成"):
        assert f(text, *args) is True, f"漏拦: {text}"


def test_claim_matches_display_name_when_template_is_registered_number():
    """登记名是纯数字（WL-02）时剥前缀只剩 '02'，与角色标题里的显示名对不上。

    这类模板没有可读名（显示名长在另一行模板上），模板名分支必须退化为
    匹配任意模板名，否则 WL-02/WL-09 的模板名分支恒失效。
    """
    f = _claims_own_step_and_foreign_instance
    # WL-02 的显示名是「需求流转」
    assert f("wf_other · 需求流转/s1-complete 已产出",
             "wf_sub_mine", "WL-02", "s1") is True
    assert f("wf_other · 需求流转/s1 完成",
             "wf_sub_mine", "WL-02", "s1") is True
    # 退化只放宽模板名，不放开完成形态与步号边界
    assert f("wf_other · 需求流转/s1/README 已更新",
             "wf_sub_mine", "WL-02", "s1") is False


def test_claim_ignores_workflow_id_prefix():
    """父链注册名带 WL- 前缀、角色发出的标题是裸名时，守卫必须命中。"""
    text = "wf_sub_deadbeef0001 · full_dev_pipeline/s4-complete 已产出"
    assert _claims_own_step_and_foreign_instance(
        text, "wf_sub_mine00000001", "WL-full_dev_pipeline", "s4") is True
    # 同实例自己的消息不算串味
    assert _claims_own_step_and_foreign_instance(
        "wf_sub_mine00000001 · full_dev_pipeline/s4-complete",
        "wf_sub_mine00000001", "WL-full_dev_pipeline", "s4") is False
    # 没声称本步完成的跨实例引用（coordinator 派发）仍要放行
    assert _claims_own_step_and_foreign_instance(
        "参考 wf_sub_deadbeef0001 的结论", "wf_sub_mine00000001",
        "WL-full_dev_pipeline", "s4") is False


class _ReworkHost(WorkflowLifecycleMixin):
    """只测驳回文本的取用来源，绕过 __init__ 与 DB 写入。"""

    def __init__(self):
        self._bb = MagicMock()
        self._workflows = {}
        self.captured = ""

    def _subflow_last_step_text(self, *a, **kw):
        return ""

    def _parse_review_issues(self, text):
        self.captured = text
        return ([], ["工程须补齐"], None, "engineer")


def test_reject_reason_feeds_rework():
    """wf confirm --reject 写的 rejection_reason 必须进返工解析。"""
    host = _ReworkHost()
    results = {"s2": {"status": "rejected", "rejection_reason":
                      "【业务问题→engineer】\n- P1 | 契约未满足 | a.py:1"}}
    ok = host._create_rework_subflow(
        "wf_x", MagicMock(_conn_tx=MagicMock(), query=MagicMock()),
        "tpl", "s2", SimpleNamespace(id="s2", title="review", target_role="reviewer"),
        SimpleNamespace(id="s3"), results)
    assert "P1 | 契约未满足" in host.captured, f"驳回理由未进入解析: {host.captured!r}"
    assert ok is False  # 走不到建链（lm 返回 None 行），但解析源已验证


def test_recursive_glob_matches_subdir_output(tmp_path, monkeypatch):
    """schema 写 **/*.py 时必须递归：角色产出常在 src/、tools/、cli/ 下。"""
    ws = tmp_path / "scout"
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "mod.py").write_text("z" * 500, encoding="utf-8")
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time() - 1, workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"**/*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is True, f"子目录里的真产出被判没有: {errs}"


class _RejHost(WorkflowLifecycleMixin):
    """真实解析 + 伪造写入点：验证无标记的驳回能否合成一条业务返工链。"""

    def __init__(self):
        self._bb = MagicMock()
        self._workflows = {"engineer_feature": SimpleNamespace(
            steps=[SimpleNamespace(id="s1", target_role="engineer", title="修",
                                   prompt_template="fix", exit_condition={}, exit_schema=None,
                                   max_retries=0, type="single")])}
        self.ins = []

    def _subflow_last_step_text(self, *a, **kw):
        return ""

    def _rework_tpl_for(self, *a, **kw):
        return "engineer_feature"


def test_unclassified_reject_creates_rework():
    """驳回只有人话（无【业务问题】标记）也要建返工链，不能空手回退重投。"""
    host = _RejHost()
    conn = MagicMock()

    def _exec(sql, params=()):
        host.ins.append(" ".join(sql.split()))
        m = MagicMock()
        m.fetchone.return_value = ({"step_results": "{}"} if "SELECT step_results" in sql else None)
        return m

    conn.execute.side_effect = _exec
    tx = MagicMock()
    tx.__enter__.return_value = conn
    tx.__exit__.return_value = False
    lm = MagicMock()
    lm._conn_tx.return_value = tx

    results = {"s2": {"status": "rejected", "rejection_reason": "PRD 缺验收标准，回退重写"}}
    ok = host._create_rework_subflow(
        "wf_y", lm, "tpl", "s2",
        SimpleNamespace(id="s2", title="review", target_role="engineer"),
        SimpleNamespace(id="s3"), results)
    assert ok is True, "无标记驳回没有建出返工链"
    assert any(s.startswith("INSERT OR IGNORE INTO workflow_instances") for s in host.ins)

def test_glob_falls_back_to_subdir_when_root_empty(tmp_path, monkeypatch):
    """契约写 `*.py` 但角色把产出落在 src/：根目录零匹配时回落到递归。"""
    ws = tmp_path / "scout"
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "real.py").write_text("q" * 500, encoding="utf-8")
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time() - 1, workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is True, f"子目录里的本次产出被判没有: {errs}"


def test_subdir_fallback_still_rejects_stale_only(tmp_path, monkeypatch):
    """回落不能放过旧文件：根目录与子目录都只有旧文件时仍判失败。"""
    ws = tmp_path / "scout"
    (ws / "src").mkdir(parents=True)
    old = ws / "src" / "old.py"
    old.write_text("x" * 500, encoding="utf-8")
    import os
    t = time.time() - 7 * 24 * 3600
    os.utime(old, (t, t))
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time(), workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is False, f"子目录旧文件冒充产出仍通过: {errs}"

def test_stale_root_plus_fresh_subdir_passes(tmp_path, monkeypatch):
    """生产形态（wf_rw_6f51b0）：根目录 44 个旧 .py，本步产出落在 src/ 下。

    旧实现只在"根目录零匹配"时回落递归，于是新产出不可见、步骤被判缺产出。
    """
    ws = tmp_path / "scout"
    (ws / "src").mkdir(parents=True)
    import os
    t_old = time.time() - 7 * 24 * 3600
    for i in range(3):
        p = ws / f"legacy{i}.py"
        p.write_text("x" * 500, encoding="utf-8")
        os.utime(p, (t_old, t_old))
    (ws / "src" / "mine.py").write_text("w" * 500, encoding="utf-8")
    monkeypatch.setattr(sh, "_CCS_WORKSPACES", tmp_path)

    run = SimpleNamespace(created_at=time.time() - 1, workflow_name="t")
    step = SimpleNamespace(id="s1", target_role="scout",
                           exit_schema={"properties": {"*.py": {"minLength": 50}}})
    ok, errs = _eng(tmp_path)._validate_exit_schema(step, run)
    assert ok is True, f"根目录旧文件遮蔽了子目录新产出: {errs}"


def test_gate_reject_walks_back_to_rework_role():
    """门禁步(s4 reviewer)驳上一步 qa 产物：返工对象应回到最近的 engineer。

    生产 3/3 条真实驳回都是这个形态，旧写法因 reviewer/qa 不在返工映射里而跳过，
    步骤 retry 封顶后永久停摆。
    """
    host = _RejHost()
    host._workflows["WL-full_dev_pipeline"] = SimpleNamespace(steps=[
        SimpleNamespace(id="s2", target_role="engineer", title="e", prompt_template="x",
                        exit_condition={}, exit_schema=None, max_retries=0, type="single"),
        SimpleNamespace(id="s3", target_role="qa", title="q", prompt_template="x",
                        exit_condition={}, exit_schema=None, max_retries=0, type="single"),
        SimpleNamespace(id="s4", target_role="reviewer", title="r", prompt_template="x",
                        exit_condition={}, exit_schema=None, max_retries=0, type="gate"),
    ])
    conn = MagicMock()

    def _exec(sql, params=()):
        host.ins.append(" ".join(sql.split()))
        m = MagicMock()
        m.fetchone.return_value = ({"step_results": "{}"} if "SELECT step_results" in sql else None)
        return m

    conn.execute.side_effect = _exec
    tx = MagicMock()
    tx.__enter__.return_value = conn
    tx.__exit__.return_value = False
    lm = MagicMock()
    lm._conn_tx.return_value = tx

    results = {"s4": {"status": "rejected",
                      "rejection_reason": "子链 x 末步驳回: CODE_REVIEW reviewer_pr_review/s1-complete — CHANGES_REQUESTED 业务 1P1"}}
    ok = host._create_rework_subflow(
        "wf_z", lm, "WL-full_dev_pipeline", "s4",
        SimpleNamespace(id="s4", title="review", target_role="reviewer"),
        SimpleNamespace(id="s5"), results)
    assert ok is True, "门禁驳回没有建出返工链"
    assert any(s.startswith("INSERT OR IGNORE INTO workflow_instances") for s in host.ins)

