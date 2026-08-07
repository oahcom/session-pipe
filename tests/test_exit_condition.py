#!/usr/bin/env python3
"""
exit_condition 匹配逻辑专项测试 — _check_exit / _validate_exit_schema / _eval_cond / _tick 组合。

覆盖范围:
  _check_exit: 空 facts、分类/来源/文本/时间戳、多过滤器组合、无 cat fallback
  _validate_exit_schema: 文件缺失、minLength、mustContain、mustExist、checksum、minFiles、minCount、空 schema
  _eval_cond: 各类正则匹配、不存在的步骤、畸形表达式、空步骤状态
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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
        ws = Path.home() / "ccs-workspaces" / "scout"
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


class TestEvalCond(unittest.TestCase):
    def test_match_done(self):
        eng = _make_eng()
        r = _run(step_results={"s1": {"status": "done"}})
        self.assertTrue(eng._eval_cond("s1.status == 'done'", r))

    def test_mismatch_status(self):
        eng = _make_eng()
        r = _run(step_results={"s1": {"status": "running"}})
        self.assertFalse(eng._eval_cond("s1.status == 'done'", r))

    def test_nonexistent_step(self):
        eng = _make_eng()
        r = _run()
        self.assertFalse(eng._eval_cond("s1.status == 'done'", r))

    def test_step_num_parsing(self):
        eng = _make_eng()
        r = _run(step_results={"s42": {"status": "done"}})
        self.assertTrue(eng._eval_cond("s42.status == 'done'", r))

    def test_malformed_expr(self):
        eng = _make_eng()
        r = _run()
        self.assertFalse(eng._eval_cond("nonsense", r))

    def test_empty_step_results(self):
        eng = _make_eng()
        r = _run(step_results={})
        self.assertFalse(eng._eval_cond("s1.status == 'completed'", r))


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
