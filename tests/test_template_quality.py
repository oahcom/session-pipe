"""模板质量与推荐系统测试。不依赖 Blackboard。"""
import os, sys, tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

os.environ["SESSION_PIPELINE_SKIP_TTL_PRUNER"] = "1"
os.environ["SESSION_PIPELINE_TEST"] = "1"

from template_registry import _validate_meta, _validate_step_content, ValidationReport

LONG_PROMPT = (
    "/goal\n## 做什么\n完成具体编码任务，理解需求并实现功能。\n## 怎么做\n1. 理解需求背景和验收标准\n2. 编写代码并遵循项目规范\n3. 编写单元测试覆盖正常/边界/异常路径\n4. 运行测试确保全部通过\n## 验收标准\n- 代码通过 review，无 P0 问题\n- 测试覆盖率 >= 80%\n- 接口兼容性已验证\n## 完成\n写 bus 标记完成。"
)


class TestValidation:

    def test_valid_template_passes(self):
        tpl = {
            "trigger_scene": ["编码实现", "功能开发"],
            "allowed_initiators": ["coordinator", "pm"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 24,
            "quality_standards": "单元测试覆盖率 >=80%，接口测试通过率 100%",
            "steps": [{
                "id": "s1", "title": "编码", "type": "handoff",
                "target_role": "engineer",
                "prompt_template": LONG_PROMPT,
                "failure_patterns": ["编译失败", "测试未通过", "代码风格不合规"],
            }],
        }
        report = _validate_meta(tpl)
        assert report.passed, f"valid template should pass: {report.errors}"

    def test_missing_trigger_scene_fails(self):
        tpl = {
            "allowed_initiators": ["coordinator"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 8,
            "quality_standards": "通过 code review 并确认无回归",
            "steps": [],
        }
        report = _validate_meta(tpl)
        assert not report.passed
        assert any("trigger_scene" in e for e in report.errors)

    def test_empty_trigger_scene_fails(self):
        tpl = {
            "trigger_scene": [],
            "allowed_initiators": ["coordinator"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 8,
            "quality_standards": "通过 code review 并确认无回归",
            "steps": [],
        }
        report = _validate_meta(tpl)
        assert not report.passed
        assert any("trigger_scene" in e for e in report.errors)

    def test_short_quality_standards_fails(self):
        tpl = {
            "trigger_scene": ["编码实现"],
            "allowed_initiators": ["coordinator"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 8,
            "quality_standards": "通过",
            "steps": [],
        }
        report = _validate_meta(tpl)
        assert not report.passed

    def test_generic_quality_standards_warns(self):
        tpl = {
            "trigger_scene": ["编码实现"],
            "allowed_initiators": ["coordinator"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 8,
            "quality_standards": "通过审查，确保质量",
            "steps": [],
        }
        report = _validate_meta(tpl)
        assert not report.passed

    def test_missing_completion_header_fails(self):
        report = ValidationReport()
        _validate_step_content({
            "id": "s1",
            "prompt_template": (
                "/goal\n## 上网搜索\n搜索信息。\n## 产出\n输出结果。\n"
            ),
            "failure_patterns": ["失败场景A"],
        }, 0, report)
        assert not report.passed
        assert any("## 完成" in e for e in report.errors)

    def test_has_completion_header_passes(self):
        report = ValidationReport()
        _validate_step_content({
            "id": "s1",
            "prompt_template": LONG_PROMPT,
            "failure_patterns": ["失败场景A"],
        }, 0, report)
        assert report.passed, f"should pass: {report.errors}"

    def test_boilerplate_failure_patterns_fails(self):
        report = ValidationReport()
        _validate_step_content({
            "id": "s1",
            "prompt_template": LONG_PROMPT,
            "failure_patterns": ["产出物为空或仅模板占位符"],
        }, 0, report)
        assert not report.passed
        assert any("generic failure_pattern" in e for e in report.errors)

    def test_short_prompt_fails(self):
        report = ValidationReport()
        _validate_step_content({
            "id": "s1",
            "prompt_template": "/goal",
            "failure_patterns": ["失败场景A"],
        }, 0, report)
        assert not report.passed
        assert any("prompt too short" in e for e in report.errors)


class TestRecommend:

    def setup_method(self):
        from template_registry import TemplateRegistry
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.reg = TemplateRegistry(db_path=self.db_path)
        self.reg.register({
            "workflow_id": "WL-TEST",
            "name": "test_engineer",
            "description": "engineer 编码实现流程",
            "trigger_scene": ["编码实现任务", "功能开发任务"],
            "allowed_initiators": ["coordinator", "pm"],
            "allowed_executors": ["engineer"],
            "max_duration_hours": 24,
            "quality_standards": "单元测试覆盖率 >=80%，接口测试通过率 100%",
            "steps": [{
                "id": "s1", "title": "编码", "type": "handoff",
                "target_role": "engineer",
                "prompt_template": LONG_PROMPT,
                "failure_patterns": ["编译失败", "测试未通过", "代码风格不合规"],
                "estimated_hours": 4,
                "exit_condition": {"bus_category": "code_fix"},
            }],
        })

    def teardown_method(self):
        self.reg.close()
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_recommend_by_scene(self):
        results = self.reg.recommend("编码实现任务", initiator_role="coordinator", assignee="engineer")
        assert len(results) >= 1
        assert results[0]["template_id"] == "WL-TEST"

    def test_recommend_by_bus_category(self):
        results = self.reg.recommend("无匹配标题", initiator_role="coordinator",
                                     assignee="engineer", bus_category="code_fix")
        assert len(results) >= 1
        assert results[0]["template_id"] == "WL-TEST"

    def test_recommend_rejects_wrong_initiator(self):
        results = self.reg.recommend("编码实现任务", initiator_role="scout", assignee="engineer")
        assert len(results) == 0

    def test_recommend_rejects_wrong_executor(self):
        results = self.reg.recommend("编码实现任务", initiator_role="coordinator", assignee="scout")
        assert len(results) == 0


class TestCreateTaskAutoRecommend:

    def setup_method(self):
        from workflow.client import WorkflowClient
        from lifecycle.manager import LifecycleManager
        import uuid
        self.db_path = f"/tmp/test_wf_{uuid.uuid4().hex[:8]}.db"
        self.lm = LifecycleManager("test_role", db_path=self.db_path)
        self.lm._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workflow_templates (
                template_id TEXT PRIMARY KEY, name TEXT, description TEXT,
                steps_json TEXT, trigger_scene TEXT, allowed_initiators TEXT,
                allowed_executors TEXT, max_duration_hours REAL,
                quality_standards TEXT, is_active INTEGER DEFAULT 1,
                created_at REAL);
            INSERT OR REPLACE INTO workflow_templates
                (template_id, name, description, steps_json, trigger_scene,
                 allowed_initiators, allowed_executors, max_duration_hours,
                 quality_standards, is_active, created_at)
            VALUES ('WL-AUTO', 'auto-test', 'desc',
                '[]',
                '["编码实现","功能开发"]', '["coordinator","pm"]', '["engineer"]',
                24, 'test standard with enough chars here', 1, 1234567890);
        """)
        self.lm._conn.commit()
        self.client = WorkflowClient("coordinator", db_path=self.db_path)

    def teardown_method(self):
        self.client.close()
        self.lm.close()
        try:
            os.unlink(self.db_path)
        except OSError:
            pass

    def test_create_without_template_id(self):
        tid, wid = self.client.create_task_v2(
            "编码实现", assignee="engineer", initiator_role="coordinator")
        assert tid and wid
        assert wid.startswith("wf_")

    def test_create_with_bus_category(self):
        tid, wid = self.client.create_task_v2(
            "编码实现", assignee="engineer", initiator_role="coordinator",
            bus_category="code_fix")
        assert tid and wid

    def test_create_fails_with_no_match(self):
        import pytest
        with pytest.raises(ValueError, match="无法为任务"):
            self.client.create_task_v2(
                "xyzzy_no_match", assignee="engineer", initiator_role="coordinator")


class TestTitlePlaceholder:
    """_title_is_placeholder 边界测试（中英文判定）。"""

    def test_chinese_3_chars_is_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("测试A") is True

    def test_chinese_4_chars_not_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("编码实现") is False

    def test_empty_string_is_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("") is True

    def test_whitespace_only_is_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("   ") is True

    def test_ascii_short_is_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("fix") is True

    def test_ascii_long_enough_not_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("implement user auth flow") is False

    def test_engineer_task_1_is_placeholder(self):
        from workflow.client import WorkflowClient
        assert WorkflowClient._title_is_placeholder("engineer task #1") is True


class TestMetaHardErrors:
    """R77 回归守卫：建议性错误不得阻断推荐，结构性错误必须阻断。

    背景：recommend() 曾用 _validate_meta().passed 做门禁，导致 34 个模板中
    29 个（含全部角色专属模板）被"缺三段式/长度不足"这类文体建议挡掉，
    路由 daemon 无法为任何角色建任务 → 13 个 CCS 角色无限空转。
    """

    def _tpl(self, pt):
        return {
            "trigger_scene": ["测试场景"], "allowed_initiators": ["pm"],
            "allowed_executors": ["engineer"], "max_duration_hours": 8,
            "quality_standards": "单元测试覆盖率 >=80%，接口测试通过率 100%",
            "steps": [{"id": "s1", "title": "步骤", "type": "handoff",
                       "target_role": "engineer", "prompt_template": pt + "\n## 完成\n写 bus。",
                       "exit_condition": {"bus_category": "code_fix"}}],
        }

    def test_missing_three_sections_is_advisory_not_hard(self):
        from template_registry import _meta_hard_errors
        assert _meta_hard_errors(self._tpl("/goal\n一段没有三段式的 prompt，长度足够" * 5)) == []

    def test_short_prompt_is_advisory_not_hard(self):
        from template_registry import _meta_hard_errors
        assert _meta_hard_errors(self._tpl("/goal\n短")) == []

    def test_missing_required_meta_field_is_hard(self):
        from template_registry import _meta_hard_errors
        t = self._tpl("/goal\n" + "内容" * 100)
        del t["quality_standards"]
        assert any("missing meta field: quality_standards" in e
                   for e in _meta_hard_errors(t))

    def test_generic_quality_standards_wording_is_advisory(self):
        from template_registry import _meta_hard_errors
        t = self._tpl("/goal\n" + "内容" * 100)
        t["quality_standards"] = "通过"
        assert _meta_hard_errors(t) == []

    def test_empty_trigger_scene_is_hard(self):
        from template_registry import _meta_hard_errors
        t = self._tpl("/goal\n" + "内容" * 100)
        t["trigger_scene"] = []
        assert any("trigger_scene" in e for e in _meta_hard_errors(t))


class TestRecommendSystemInitiator:
    """R77 回归守卫：路由 daemon 以 initiator_role='system' 建任务时，
    不得因 allowed_initiators 不含 'system' 而被排除。

    自建隔离 DB + 临时模板，不依赖生产模板库。
    """
    _SCENE = "产品设计与架构评审任务"

    def setup_method(self):
        from template_registry import TemplateRegistry
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.reg = TemplateRegistry(db_path=self.db_path)
        self.reg.register({
            "workflow_id": "WL-RT77", "name": "r77_guard",
            "description": "R77 回归守卫用的最小角色专属模板",
            "trigger_scene": [self._SCENE],
            "allowed_initiators": ["pm"], "allowed_executors": ["engineer"],
            "max_duration_hours": 24,
            "quality_standards": "单元测试覆盖率 >=80%，接口测试通过率 100%",
            "steps": [{
                "id": "s1", "title": "编码", "type": "handoff",
                "target_role": "engineer", "prompt_template": LONG_PROMPT,
                "failure_patterns": ["编译失败", "测试未通过"],
                "exit_condition": {"bus_category": "code_fix"},
            }],
        })

    def teardown_method(self):
        self.reg.close()
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_system_initiator_equivalent_to_empty(self):
        a = self.reg.recommend(self._SCENE, initiator_role="", assignee="engineer")
        b = self.reg.recommend(self._SCENE, initiator_role="system", assignee="engineer")
        assert len(a) == 1 and len(b) == len(a), \
            f"system 应与空发起等效且推荐到 1 个，实际 {len(a)} vs {len(b)}"

    def test_system_initiator_reaches_role_specific_template(self):
        """核心回归：修复前 recommend 把 'system' 当业务角色比对
        allowed_initiators（只含 'pm'），engineer 的专属模板被排除 → n=0 →
        路由 daemon 无法为任何角色建任务。"""
        got = self.reg.recommend(self._SCENE, initiator_role="system", assignee="engineer")
        assert got and got[0]["template_id"] == "WL-RT77", \
            f"system 发起应能推荐到角色专属模板，实际 {got}"

    def test_real_role_initiator_still_filtered(self):
        """安全：真实业务角色仍受 allowed_initiators 约束，权限模型不放宽。"""
        assert self.reg.recommend(
            self._SCENE, initiator_role="scout", assignee="engineer") == []

    def test_wrong_executor_still_filtered(self):
        """安全：assignee 不在 allowed_executors → 不推荐。"""
        assert self.reg.recommend(
            self._SCENE, initiator_role="system", assignee="qa") == []
