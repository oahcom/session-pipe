"""test_role_validator.py — 角色名校验装饰器测试覆盖。"""
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


class TestIsValidRole:
    @pytest.fixture(autouse=True)
    def _setup_registry(self, monkeypatch, tmp_path):
        """在 tmp_path 下构造 persona JSON，注入 session-pipeline paths 路径。"""
        # 写两个 persona
        persona = tmp_path / "persona_01_engineer.json"
        persona.write_text(json.dumps({"name": "engineer"}))
        persona2 = tmp_path / "persona_02_pm.json"
        persona2.write_text(json.dumps({"name": "pm"}))
        # mock paths import
        import role_validator
        role_validator._ROLE_REGISTRY.clear()
        monkeypatch.setattr(role_validator, "SESSION_ROLES_PERSONAS", tmp_path)
        # force reload
        role_validator._ROLE_REGISTRY.clear()

    def test_valid_role(self):
        from role_validator import is_valid_role
        assert is_valid_role("engineer") is True

    def test_invalid_role(self):
        from role_validator import is_valid_role
        assert is_valid_role("nonexistent") is False

    def test_pm_valid(self):
        from role_validator import is_valid_role
        assert is_valid_role("pm") is True


class TestValidateRoleDecorator:
    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, tmp_path):
        persona = tmp_path / "persona_01_engineer.json"
        persona.write_text(json.dumps({"name": "engineer"}))
        import role_validator
        role_validator._ROLE_REGISTRY.clear()
        monkeypatch.setattr(role_validator, "SESSION_ROLES_PERSONAS", tmp_path)
        role_validator._ROLE_REGISTRY.clear()

    def test_valid_role_passes(self):
        from role_validator import validate_role

        @validate_role()
        def do_something(role):
            return {"ok": True, "role": role}

        assert do_something("engineer") == {"ok": True, "role": "engineer"}

    def test_invalid_role_raises(self):
        from role_validator import validate_role

        @validate_role()
        def do_something(role):
            return {"ok": True}

        with pytest.raises(PermissionError, match="not found"):
            do_something("nonexistent")

    def test_system_role_allowed_by_default(self):
        from role_validator import validate_role

        @validate_role()
        def do_something(role):
            return {"ok": True}

        assert do_something("pipeline") == {"ok": True}

    def test_system_role_blocked_when_not_allowed(self):
        from role_validator import validate_role

        @validate_role(allow_system=False)
        def do_something(role):
            return {"ok": True}

        with pytest.raises(PermissionError):
            do_something("pipeline")

    def test_on_invalid_callback(self):
        from role_validator import validate_role
        calls = []

        @validate_role(on_invalid=lambda r: calls.append(r))
        def do_something(role):
            return {}

        with pytest.raises(PermissionError):
            do_something("bad")
        assert calls == ["bad"]

    def test_missing_role_raises_type_error(self):
        from role_validator import validate_role

        @validate_role()
        def do_something(role):
            return {}

        with pytest.raises(TypeError):
            do_something()  # missing role


if __name__ == "__main__":
    import pytest as _p
    import sys as _s
    _s.exit(_p.main([__file__, "-v"]))
