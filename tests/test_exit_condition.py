#!/usr/bin/env python3
"""
exit_condition 匹配逻辑专项测试 — _check_exit / _validate_exit_schema / _tick 组合。

覆盖范围:
  _check_exit: 空 facts、分类/来源/文本/时间戳、多过滤器组合、无 cat fallback
  _validate_exit_schema: 文件缺失、minLength、mustContain、mustExist、checksum、minFiles、minCount、空 schema
  _tick 组合: exit_schema 失败阻塞推进、verify 命令失败阻塞推进

运行: cd ~/session-pipeline && python3 -m pytest tests/test_exit_condition.py -v
"""
import hashlib
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_PIPELINE_SRC = str(Path.home() / "session-pipeline" / "src")
if _PIPELINE_SRC not in sys.path:
    sys.path.insert(0, _PIPELINE_SRC)

from pipeflow.engine import Step, WorkflowEngine, WorkflowRun

# 与被测引擎同源：走 paths.CCS_WORKSPACES（conftest 已隔离到临时目录）。
# 硬编码 Path.home() 会让测试写生产 workspace，且隔离后读写路径不一致导致误报。
from paths import CCS_WORKSPACES as _CCSWS
_WS_SCOUT = _CCSWS / "scout"

_UNIQUE = f"EC_{int(time.time() * 1000) % 1_000_000:06d}"


def _make_eng():
    d = Path(tempfile.mkdtemp())
    (d / "runs").mkdir(parents=True, exist_ok=True)
    return WorkflowEngine(workflows_dir=d)


def _mocked_eng():
    """引擎 + mock Blackboard + lifecycle，测试 _tick 时不写真实 bus。"""
    eng = _make_eng()
    eng._bb = MagicMock()
    eng._lm = MagicMock()  # _lifecycle 是 property, 需要 mock _lm 直接
    return eng


def _step(**overrides):
    defaults = dict(
        id="s1", title="test", target_role="scout",
        prompt_template="do {topic}", exit_condition={}, type="single",
        completion_check=None, max_retries=0, condition="", rollback_to="",
        verify="", failure_patterns=None, subflow_template="", exit_schema=None,
    )
    defaults.update(overrides)
    return Step(**defaults)


def _run(step_results=None, status="running", current_step="s1"):
    now = time.time()
    return WorkflowRun(
        id=f"wf_{_UNIQUE}", workflow_name="test_wf",
        context={}, current_step=current_step, status=status,
        step_results=step_results or {}, created_at=now, updated_at=now,
    )


class TestCheckExitEmpty(unittest.TestCase):
    def test_empty_bus(self):
        eng = _make_eng()
        ts, msgs = eng._check_exit(f"__ec_empty_{_UNIQUE}", "", "")
        self.assertFalse(ts)


class TestCheckExitFilters(unittest.TestCase):
    def test_text_mismatch(self):
        eng = _make_eng()
        eng._bb.write("notice", f"no match {_UNIQUE}", src="test_src")
        ts, msgs = eng._check_exit("notice", "", "XYZZY_NOMATCH")
        self.assertFalse(ts)

    def test_combined_src_and_text(self):
        eng = _make_eng()
        marker = f"COMBO_{_UNIQUE}"
        eng._bb.write("notice", marker, src="combo_src")
        ts, msgs = eng._check_exit("notice", "combo_src", marker)
        self.assertTrue(ts)
        ts, msgs = eng._check_exit("notice", "wrong_src", marker)
        self.assertFalse(ts)
        ts, msgs = eng._check_exit("notice", "combo_src", "WRONG")
        self.assertFalse(ts)


class TestCheckExitTimestamp(unittest.TestCase):
    def test_created_after_filters_old(self):
        eng = _make_eng()
        eng._bb.write("notice", f"old msg {_UNIQUE}", src="ts_src")
        future = time.time() + 3600
        ts, msgs = eng._check_exit("notice", "ts_src", "", created_after=future)
        self.assertFalse(ts)

    def test_created_after_allows_recent(self):
        eng = _make_eng()
        marker = f"NEW_{_UNIQUE}"
        eng._bb.write("notice", marker, src="ts_src2")
        past = time.time() - 3600
        ts, msgs = eng._check_exit("notice", "ts_src2", marker, created_after=past)
        self.assertTrue(ts)

    def test_created_after_no_facts(self):
        eng = _make_eng()
        ts, msgs = eng._check_exit("notice", "", "", created_after=time.time())
        self.assertFalse(ts)


class TestCheckExitNoCat(unittest.TestCase):
    def test_empty_cat_fallback(self):
        eng = _make_eng()
        marker = f"NOCAT_{_UNIQUE}"
        eng._bb.write("architecture", marker, src="nocat_src")
        ts, msgs = eng._check_exit("", "nocat_src", marker)
        self.assertTrue(ts)



class TestCheckExitStrictWfOwnership(unittest.TestCase):
    """require_wf 严格归属：消息必须含当前实例 id。

    旧实现 "'wf_' in t" 才校验 → 标题无 wf_ 的消息一律放行，两条同模板
    返工链被同一条 code_fix 同时闭合（生产 #415562 实测）。
    """

    def test_message_without_wf_id_not_matched(self):
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_fix", f"engineer_feature/s1-complete {marker}", src="engineer")
        ts, msgs = eng._check_exit("code_fix", "engineer", marker, wf_id="wf_aaaa1111")
        self.assertFalse(ts, "无实例 id 的消息不应匹配 require_wf 步骤")

    def test_message_with_wf_id_matched(self):
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_fix", f"engineer_feature/s1-complete ({marker} wf_bbbb2222/s1)",
                      src="engineer")
        ts, msgs = eng._check_exit("code_fix", "engineer", marker, wf_id="wf_bbbb2222")
        self.assertTrue(ts)

    def test_bare_substring_does_not_match_longer_id(self):
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_fix", f"{marker} wf_cccc33334/s1", src="engineer")
        ts, msgs = eng._check_exit("code_fix", "engineer", marker, wf_id="wf_cccc3333")
        self.assertFalse(ts, "abc123 不应误中 abc1234")

    def test_wait_roles_branch_also_strict(self):
        """协作门补查路径与主路径同源：无 id 消息不能一票满足缺失角色。"""
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_fix", f"{marker} wf_dddd5555/s1", src="engineer")
        eng._bb.write("code_fix", f"{marker} 无实例标记", src="qa")
        ts, msgs = eng._check_exit("code_fix", "engineer", marker,
                                   wf_id="wf_dddd5555", wait_roles=["engineer", "qa"])
        self.assertFalse(ts, "qa 的消息不带实例 id，协作门不应被满足")

    def test_cjk_punctuation_adjacent_id_matched(self):
        """引擎注入的 header 是「（完成消息标题必须含此 id）」，角色照抄即全角紧邻。"""
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_review", f"{marker} wf_eeee6666（完成）产出已交付", src="reviewer")
        ts, _ = eng._check_exit("code_review", "reviewer", marker, wf_id="wf_eeee6666")
        self.assertTrue(ts, "全角标点紧邻的实例 id 必须命中")

    def test_cjk_fullwidth_brackets_both_sides_matched(self):
        eng = _make_eng()
        marker = f"STRICT_{_UNIQUE}"
        eng._bb.write("code_review", f"【完成】{marker}（wf_ffff7777）", src="reviewer")
        ts, _ = eng._check_exit("code_review", "reviewer", marker, wf_id="wf_ffff7777")
        self.assertTrue(ts, "两侧全角括号包裹的实例 id 必须命中")

class TestValidateExitSchema(unittest.TestCase):
    def test_empty_schema_passes(self):
        eng = _make_eng()
        ok, errs = eng._validate_exit_schema(_step(), _run())
        self.assertTrue(ok)
        self.assertEqual(errs, [])

    def test_missing_file_fails(self):
        eng = _make_eng()
        step = _step(exit_schema={"required": ["REPORT.md"]})
        ok, errs = eng._validate_exit_schema(step, _run())
        self.assertFalse(ok)
        self.assertTrue(any("缺少产出" in e for e in errs))

    def test_min_length_pass(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"minlen_{_UNIQUE}.md"
        fpath.write_text("x" * 100)
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"minlen_{_UNIQUE}.md"],
                "properties": {f"minlen_{_UNIQUE}.md": {"minLength": 50}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            fpath.unlink(missing_ok=True)

    def test_min_length_fail(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"short_{_UNIQUE}.md"
        fpath.write_text("hi")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"short_{_UNIQUE}.md"],
                "properties": {f"short_{_UNIQUE}.md": {"minLength": 50}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any("内容不足" in e for e in errs))
        finally:
            fpath.unlink(missing_ok=True)

    def test_must_contain_pass(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"content_{_UNIQUE}.md"
        fpath.write_text("结论: 测试通过")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"content_{_UNIQUE}.md"],
                "properties": {f"content_{_UNIQUE}.md": {"mustContain": ["结论"]}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            fpath.unlink(missing_ok=True)

    def test_must_contain_fail(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"nocon_{_UNIQUE}.md"
        fpath.write_text("nothing special here")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"nocon_{_UNIQUE}.md"],
                "properties": {f"nocon_{_UNIQUE}.md": {"mustContain": ["结论"]}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any("缺少必需内容" in e for e in errs))
        finally:
            fpath.unlink(missing_ok=True)

    def test_must_contain_url_pass(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"urlok_{_UNIQUE}.md"
        fpath.write_text("参考: https://github.com/example/proj\n")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"urlok_{_UNIQUE}.md"],
                "properties": {f"urlok_{_UNIQUE}.md": {"mustContainUrl": True}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            fpath.unlink(missing_ok=True)

    def test_must_contain_url_fail(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"nourl_{_UNIQUE}.md"
        fpath.write_text("本地调研结论，无外部链接")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"nourl_{_UNIQUE}.md"],
                "properties": {f"nourl_{_UNIQUE}.md": {"mustContainUrl": True}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any("URL" in e for e in errs))
        finally:
            fpath.unlink(missing_ok=True)

    def test_checksum_pass(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"cksum_{_UNIQUE}.md"
        content = f"checksum test {_UNIQUE}"
        fpath.write_text(content)
        expected = hashlib.md5(content.encode()).hexdigest()
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"cksum_{_UNIQUE}.md"],
                "properties": {f"cksum_{_UNIQUE}.md": {"checksum": expected}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            fpath.unlink(missing_ok=True)

    def test_checksum_fail(self):
        ws = _WS_SCOUT
        ws.mkdir(parents=True, exist_ok=True)
        fpath = ws / f"cksum2_{_UNIQUE}.md"
        fpath.write_text("some content")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"cksum2_{_UNIQUE}.md"],
                "properties": {f"cksum2_{_UNIQUE}.md": {"checksum": "wrong_checksum"}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any("checksum" in e for e in errs))
        finally:
            fpath.unlink(missing_ok=True)

    def test_min_files_pass(self):
        ws = _WS_SCOUT
        d = ws / f"pkg_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)
        for i in range(3):
            (d / f"mod{i}.py").write_text(f"# {i}")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"pkg_{_UNIQUE}"],
                "properties": {f"pkg_{_UNIQUE}": {"minFiles": 3, "extension": ".py"}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_min_files_fail(self):
        ws = _WS_SCOUT
        d = ws / f"pkg2_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "only.py").write_text("# one file")
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "required": [f"pkg2_{_UNIQUE}"],
                "properties": {f"pkg2_{_UNIQUE}": {"minFiles": 3, "extension": ".py"}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any(".py 文件" in e for e in errs))
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_min_count_pass(self):
        ws = _WS_SCOUT
        d = ws / f"mc_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)
        for i in range(3):
            (d / f"f{i}.txt").write_text(str(i))
        try:
            eng = _make_eng()
            # 用通配符匹配目录下文件，而非目录本身
            step = _step(exit_schema={
                "required": [f"mc_{_UNIQUE}/*"],
                "properties": {f"mc_{_UNIQUE}/*": {"minCount": 3}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok)
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_min_count_fail(self):
        ws = _WS_SCOUT
        d = ws / f"mc2_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "only.txt").write_text("x")
        try:
            eng = _make_eng()
            # 期望 5 个文件但只有 1 个
            step = _step(exit_schema={
                "required": [f"mc2_{_UNIQUE}/*"],
                "properties": {f"mc2_{_UNIQUE}/*": {"minCount": 5}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok)
            self.assertTrue(any("glob 匹配" in e for e in errs))
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_min_count_ignores_stale_files(self):
        """回归: 只有历史遗留文件（mtime 早于 run.created_at-2s）时 fresh=0 必须 FAIL。

        旧实现用 _gl.glob() 全量计数，历史文件凑够 minCount 就判通过。
        """
        ws = _WS_SCOUT
        d = ws / f"mc3_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)
        old = time.time() - 3600
        for i in range(3):
            p = d / f"stale{i}.py"
            p.write_text("x = 1\n")
            import os as _os
            _os.utime(p, (old, old))
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "properties": {f"mc3_{_UNIQUE}/*.py": {"minCount": 1}},
            })
            # run.created_at=now → 3 个历史文件均在 2s 容差之外
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok, f"历史文件不应满足 minCount: {errs}")
            self.assertTrue(any("glob 匹配" in e for e in errs), errs)

            # 新鲜文件写入后应通过
            (d / "fresh.py").write_text("y = 2\n")
            ok2, errs2 = eng._validate_exit_schema(step, _run())
            self.assertTrue(ok2, errs2)
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    def test_min_count_zero_matches_fails(self):
        """回归: 通配符零匹配时 minCount 也必须判 FAIL（旧实现会被 early continue 跳过）。"""
        ws = _WS_SCOUT
        d = ws / f"mc4_{_UNIQUE}"
        d.mkdir(parents=True, exist_ok=True)  # 空目录，零匹配
        try:
            eng = _make_eng()
            step = _step(exit_schema={
                "properties": {f"mc4_{_UNIQUE}/*.py": {"minCount": 1}},
            })
            ok, errs = eng._validate_exit_schema(step, _run())
            self.assertFalse(ok, f"零匹配不应通过 minCount: {errs}")
            self.assertTrue(any("glob 匹配" in e for e in errs), errs)
        finally:
            import shutil
            shutil.rmtree(d, ignore_errors=True)


class TestTickExitSchemaBlocked(unittest.TestCase):
    def test_exit_schema_fail_blocks_advance(self):
        eng = _mocked_eng()
        eng._check_exit = MagicMock(return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}]))
        eng._validate_exit_schema = MagicMock(return_value=(False, ["schema error"]))
        eng._send_to_role = MagicMock()

        # 需要 bus_category 才能进入 schema 校验路径（空 cat 直接 return）
        step = _step(exit_condition={"bus_category": "notice"}, exit_schema={"required": ["nonexistent"]})
        run = _run()
        eng._tick(run, step)

        # 与 TestTickVerifyBlocked 同一口径：同一 _tick 里角色启动失败也会写 blocker，
        # 次数取决于启动重试/冷却状态 → 全量跑间歇红、单跑绿。只锁契约：
        # schema 失败必须留下带 "schema" 的 blocker。
        titles = [c[0][1] for c in eng._bb.write.call_args_list]
        self.assertTrue(any("schema" in t for t in titles), titles)


class TestTickVerifyBlocked(unittest.TestCase):
    @patch("pipeflow.engine._sp")
    def test_verify_fail_blocks_advance(self, mock_sp):
        eng = _mocked_eng()
        # 角色拉起是另一条路径（30×1s 真实 sleep + 自己写 blocker）。本用例只验
        # verify 阻断推进；不隔离它就会让"写了几次 blocker"取决于重试状态，
        # 全量跑间歇红、单跑绿，还额外烧 90s。
        eng._ensure_role_alive = MagicMock(return_value=True)
        # 投递路径里的 _wait_for_role_ready 会真实 sleep（单跑 90s）——本用例不验它。
        eng._wait_for_role_ready = MagicMock(return_value=False)
        eng._check_exit = MagicMock(return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}]))
        eng._validate_exit_schema = MagicMock(return_value=(True, []))

        failed_proc = MagicMock()
        failed_proc.returncode = 1
        failed_proc.stderr = b"verify error output"
        mock_sp.run.return_value = failed_proc

        step = _step(exit_condition={"bus_category": "notice"}, verify="test_command")
        run = _run()
        eng._tick(run, step)

        # 不能断言"只写一次"：同一 _tick 里角色启动失败也会写 blocker（实测 7 次），
        # 次数取决于启动重试与冷却缓存状态 → 全量跑间歇红、单跑绿。这里只锁定契约：
        # verify 失败必须留下带 "verify" 的 blocker。
        titles = [c[0][1] for c in eng._bb.write.call_args_list]
        self.assertTrue(any("verify" in t for t in titles), titles)


class TestTickCompletedRunSkipped(unittest.TestCase):
    def test_completed_run_returns_early(self):
        eng = _mocked_eng()
        eng._check_exit = MagicMock()
        step = _step()
        run = _run(status="completed")
        eng._tick(run, step)
        eng._check_exit.assert_not_called()


class TestTickNoMatchUpdatesPollSince(unittest.TestCase):
    def test_poll_since_updated_when_no_match(self):
        """退出条件未匹配时引擎推 poll_since，防止重复匹配旧消息。"""
        eng = _mocked_eng()
        eng._check_exit = MagicMock(return_value=(0.0, []))

        # 需要 bus_category 才能进入 no-match 路径（空 cat 直接 return）
        step = _step(exit_condition={"bus_category": "notice"})
        run = _run()
        run.step_results = {"s1": {}}
        eng._tick(run, step)

        sr = run.step_results["s1"]
        self.assertIn("poll_since", sr)


class TestSoftAttribution(unittest.TestCase):
    """裸匹配步骤的软归属：拦"点名别实例却声称完成本步"，放行其他一切。

    生产 #421072：architect_full_design 同批拉起两条链，wf_sub_ad604 的 s2
    完成消息把 wf_sub_7afc1 的 s2 一起闭合，自有产出晚 7 分钟。

    不用 require_wf=true 的硬校验：164 条历史裸匹配完成消息里 105 条不带
    实例 id（no-op 巡检），硬校验会把它们全卡成超时。
    """

    def test_foreign_instance_claiming_own_step_not_matched(self):
        eng = _make_eng()
        marker = f"SOFT_{_UNIQUE}"
        eng._bb.write("system_design",
                      f"SYSTEM_DESIGN: 磁盘清理方案 {marker} — 实例 wf_sibling999 / arch/s2 完成",
                      src="product_architect")
        ts, _ = eng._check_exit("system_design", "product_architect", marker,
                                own_wf_id="wf_mine111", template="arch", step_id="s2")
        self.assertFalse(ts, "兄弟实例声称完成本步，不得闭合本实例")

    def test_own_instance_claiming_own_step_matched(self):
        eng = _make_eng()
        marker = f"SOFT_{_UNIQUE}"
        eng._bb.write("system_design", f"设计完成 {marker} 实例 wf_mine111 / arch/s2 完成",
                      src="product_architect")
        ts, _ = eng._check_exit("system_design", "product_architect", marker,
                                own_wf_id="wf_mine111", template="arch", step_id="s2")
        self.assertTrue(ts)

    def test_no_instance_id_still_matched(self):
        """no-op 巡检类消息不带 id —— 保持放行，否则步骤熬到超时。"""
        eng = _make_eng()
        marker = f"SOFT_{_UNIQUE}"
        eng._bb.write("root_cause_analysis", f"[修复] 服务全绿零僵尸 {marker}", src="maintainer")
        ts, _ = eng._check_exit("root_cause_analysis", "maintainer", marker,
                                own_wf_id="wf_mine111", template="maint", step_id="s2")
        self.assertTrue(ts, "无实例 id 的完成消息必须放行")

    def test_foreign_id_without_step_claim_still_matched(self):
        """派发/建任务类消息天然提到别的工作流 id，不是串味。"""
        eng = _make_eng()
        marker = f"SOFT_{_UNIQUE}"
        eng._bb.write("task_spec", f"[closer] 创建任务 {marker} 审计 wf_other222/s2 产出",
                      src="closer")
        ts, _ = eng._check_exit("task_spec", "closer", marker,
                                own_wf_id="wf_mine111", template="closer_close_loop", step_id="s1")
        self.assertTrue(ts, "只引用别实例、未声称本步完成 → 放行")

    def test_reverse_attribution_not_applied_when_require_wf(self):
        """require_wf 已是硬校验，软归属不重复生效。"""
        eng = _make_eng()
        marker = f"SOFT_{_UNIQUE}"
        eng._bb.write("code_fix", f"{marker} 实例 wf_other222 / t/s1 完成", src="engineer")
        ts, _ = eng._check_exit("code_fix", "engineer", marker, wf_id="wf_mine111",
                                own_wf_id="wf_mine111", template="t", step_id="s1")
        self.assertFalse(ts)


class TestConcurrentInstancesNoCrossClosure(unittest.TestCase):
    """并发夹具：同模板两实例同时推进，各自只认自己的完成消息。

    修的是 23 个裸步骤（无 text_contains / require_wf）：兄弟实例的
    s2-complete 会把本实例同步闭合（#421072 实测差 7 分钟）。
    两条链共用同一模板标记（text_contains 相同），唯一区分量是实例 id。
    """

    def _eng(self):
        eng = _make_eng()
        eng._eff_role = lambda run, step: step.target_role
        eng._sync_step_results = MagicMock()
        eng._send_to_role = MagicMock()
        eng._lm = MagicMock()
        return eng

    def _pair(self, eng, cat="notice"):
        # 每个用例独立后缀：同文本重写会走 UPSERT 抬高 ts，屏障按首次写入
        # (MIN(ts)) 判定 → 复用 marker 会让第二个用例的消息被判为"屏障前"。
        _tok = f"{_UNIQUE}{time.time_ns() % 1_000_000_000:09d}"
        marker = f"CONC_{_tok}"
        ec = {"bus_category": cat, "source_contains": "scout",
              "text_contains": marker, "require_wf": True, "timeout_minutes": 30}
        step = _step(exit_condition=ec)
        a = _run(); a.workflow_name = "conc_wf"
        b = _run(); b.workflow_name = "conc_wf"
        # _run() 的 id 取自模块级 _UNIQUE，两个实例会拿到同一个 id → 夹具假绿
        a.id, b.id = f"wf_{_tok}a", f"wf_{_tok}b"
        # 锚点必须早于完成消息：notified_at 缺失时 tick 用 time.time() 兜底，
        # 会跳过刚写入的消息（真实实例在上一 tick 已持久化 poll_since）。
        _anchor = time.time() - 60
        a.step_results = {"s1": {"notified_at": _anchor}}
        b.step_results = {"s1": {"notified_at": _anchor}}
        return marker, step, a, b

    def test_sibling_completion_does_not_close_other_instance(self):
        eng = self._eng()
        marker, step, a, b = self._pair(eng)
        eng._bb.write(step.exit_condition["bus_category"],
                      f"{marker} [实例] {b.id} 完成 s1", src="scout")
        eng._tick(a, step)
        self.assertFalse(a.step_results["s1"].get("exit_messages"),
                         "B 的完成消息不得闭合 A")
        eng._tick(b, step)
        ids = {m["text"] for m in b.step_results["s1"]["exit_messages"]}
        self.assertEqual(len(ids), 1)
        self.assertIn(b.id, b.step_results["s1"]["exit_messages"][0]["text"])

    def test_both_instances_close_only_on_own_message(self):
        eng = self._eng()
        marker, step, a, b = self._pair(eng)
        cat = step.exit_condition["bus_category"]
        eng._bb.write(cat, f"{marker} [实例] {a.id} 完成 s1", src="scout")
        eng._bb.write(cat, f"{marker} [实例] {b.id} 完成 s1", src="scout")
        eng._tick(a, step)
        eng._tick(b, step)
        for run in (a, b):
            msgs = run.step_results["s1"].get("exit_messages") or []
            self.assertEqual(len(msgs), 1, f"{run.id} 只应吃自己的消息: {msgs}")
            self.assertIn(run.id, msgs[0]["text"])
            others = [m for m in msgs if (a.id if run is b else b.id) in m["text"]]
            self.assertEqual(others, [], f"{run.id} 不得消费兄弟实例消息")

    def test_idless_completion_not_matched_by_require_wf(self):
        """require_wf 下不带实例 id 的完成消息不闭合（否则回退到裸匹配）。"""
        eng = self._eng()
        marker, step, a, _b = self._pair(eng)
        eng._bb.write(step.exit_condition["bus_category"],
                      f"{marker} s1 完成", src="scout")
        eng._tick(a, step)
        self.assertFalse(a.step_results["s1"].get("exit_messages"))



_WORKFLOWS_DIR = Path.home() / ".hermes" / "workflows"


@unittest.skipUnless(_WORKFLOWS_DIR.is_dir(), "生产模板目录缺失（CI/沙箱）")
class TestTemplatesBindIdentity(unittest.TestCase):
    """回归门：任何带 bus_category 的步骤都必须能区分实例。

    23/141 步裸匹配（只有 bus_category+source_contains）会把兄弟实例的完成
    消息当本步完成（#421072）。判据是模板数据本身，不是扫描脚本。
    """

    def test_no_naked_exit_condition(self):
        import json
        naked = []
        for f in sorted(_WORKFLOWS_DIR.glob("*.json")):
            data = json.loads(f.read_text())
            for st in data.get("steps", []):
                ec = st.get("exit_condition") or {}
                if ec.get("bus_category") and not (ec.get("text_contains") or ec.get("require_wf")):
                    naked.append(f"{f.name}/{(st.get('step_id') or st.get('id'))}")
        self.assertEqual(naked, [], f"裸匹配步骤会把兄弟实例消息当本步完成: {naked}")

    def test_bound_steps_also_instruct_instance_id(self):
        """require_wf 步骤的提示词必须要求标题带实例 id，否则步骤必超时。"""
        import json
        bad = []
        for f in sorted(_WORKFLOWS_DIR.glob("*.json")):
            data = json.loads(f.read_text())
            for st in data.get("steps", []):
                ec = st.get("exit_condition") or {}
                if not (ec.get("require_wf") and str(ec.get("text_contains", "")).count("-complete")):
                    continue
                _pt = st.get("prompt_template") or ""
                # 两种既有措辞：「本实例 id」/「[实例] 行给出的实例 id」
                if "本实例 id" not in _pt and "[实例] 行给出的实例 id" not in _pt:
                    bad.append(f"{f.name}/{(st.get('step_id') or st.get('id'))}")
        self.assertEqual(bad, [], f"提示词未要求实例 id: {bad}")

if __name__ == "__main__":
    unittest.main()
