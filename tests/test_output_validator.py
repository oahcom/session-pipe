"""test_output_validator.py — output_schema 校验测试覆盖。"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from output_validator import _load_role_schema, validate_output


def _persona_mock(name: str, data: dict) -> MagicMock:
    """Build a mock Path with read_text returning persona JSON."""
    f = MagicMock(spec=Path)
    f.name = f"persona_{name}.json"
    f.read_text.return_value = json.dumps(data)
    f.__lt__ = lambda self, other: self.name < other.name
    return f


class TestLoadRoleSchema:
    @patch("output_validator._ROLES_DIR")
    def test_not_a_dir(self, mock_dir):
        mock_dir.is_dir.return_value = False
        assert _load_role_schema("engineer", "code_fix") is None

    @patch("output_validator._ROLES_DIR")
    def test_role_not_found(self, mock_dir):
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = []
        assert _load_role_schema("nonexistent", "x") is None

    @patch("output_validator._ROLES_DIR")
    def test_role_no_schema(self, mock_dir):
        f = _persona_mock("engineer", {"name": "engineer"})
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        assert _load_role_schema("engineer", "code_fix") is None

    @patch("output_validator._ROLES_DIR")
    def test_role_has_schema(self, mock_dir):
        f = _persona_mock("engineer", {
            "name": "engineer",
            "output_schema": {"code_fix": {"title": "str", "evidence": "str"}}
        })
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        schema = _load_role_schema("engineer", "code_fix")
        assert schema == {"title": "str", "evidence": "str"}


class TestValidateOutput:
    @patch("output_validator._ROLES_DIR")
    def test_no_schema_returns_unknown(self, mock_dir):
        mock_dir.is_dir.return_value = False
        result = validate_output("engineer", "code_fix", {"title": "x"})
        assert result == {"valid": True, "missing": [], "severity": "unknown"}

    @patch("output_validator._ROLES_DIR")
    def test_all_fields_present(self, mock_dir):
        f = _persona_mock("engineer", {
            "name": "engineer",
            "output_schema": {"code_fix": {"title": "str", "evidence": "str"}}
        })
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        result = validate_output("engineer", "code_fix", {"title": "a", "evidence": "b"})
        assert result["valid"] is True
        assert result["missing"] == []
        assert result["severity"] == "ok"

    @patch("output_validator._ROLES_DIR")
    def test_missing_fields(self, mock_dir):
        f = _persona_mock("engineer", {
            "name": "engineer",
            "output_schema": {"code_fix": {"title": "str", "evidence": "str"}}
        })
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        result = validate_output("engineer", "code_fix", {"title": "a"})
        assert result["valid"] is False
        assert "evidence" in result["missing"]
        assert result["severity"] == "error"

    @patch("output_validator._ROLES_DIR")
    def test_nested_schema_fields(self, mock_dir):
        f = _persona_mock("architect", {
            "name": "architect",
            "output_schema": {
                "architecture": {
                    "description": "arch insight",
                    "fields": {"title": "str", "rationale": "str"}
                }
            }
        })
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        assert validate_output("architect", "architecture", {"title": "a", "rationale": "b"})["valid"] is True
        assert validate_output("architect", "architecture", {"title": "a"})["valid"] is False

    @patch("output_validator._ROLES_DIR")
    def test_nested_schema_without_fields_treats_description_as_required(self, mock_dir):
        """嵌套 schema 但无 fields 键时，description 被视为必填字段。"""
        f = _persona_mock("minimalist", {
            "name": "minimalist",
            "output_schema": {"x": {"description": "no fields here"}}
        })
        mock_dir.is_dir.return_value = True
        mock_dir.glob.return_value = [f]
        # content 必须包含 "description" 才通过
        assert validate_output("minimalist", "x", {"description": "y"})["valid"] is True
        assert validate_output("minimalist", "x", {})["valid"] is False


if __name__ == "__main__":
    import pytest as _p
    import sys as _s
    _s.exit(_p.main([__file__, "-v"]))
