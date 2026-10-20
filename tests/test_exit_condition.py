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

        eng._bb.write.assert_called_once()
        call_args = eng._bb.write.call_args
        self.assertIn("schema", call_args[0][1])


class TestTickVerifyBlocked(unittest.TestCase):
    @patch("pipeflow.engine._sp")
    def test_verify_fail_blocks_advance(self, mock_sp):
        eng = _mocked_eng()
        eng._check_exit = MagicMock(return_value=(time.time(), [{"id": 1, "text": "match", "ts": time.time(), "src": "test"}]))
        eng._validate_exit_schema = MagicMock(return_value=(True, []))

        failed_proc = MagicMock()
        failed_proc.returncode = 1
        failed_proc.stderr = b"verify error output"
        mock_sp.run.return_value = failed_proc

        step = _step(exit_condition={"bus_category": "notice"}, verify="test_command")
        run = _run()
        eng._tick(run, step)

        eng._bb.write.assert_called_once()
        call_args = eng._bb.write.call_args
        self.assertIn("verify", call_args[0][1])


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


if __name__ == "__main__":
    unittest.main()
