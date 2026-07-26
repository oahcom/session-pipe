#!/usr/bin/env python3
"""Unit tests for output_validator.py.

Use a temporary directory with mock role JSON files to avoid
depending on the real ~/hermes-session-roles tree.
"""
import json
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from src import output_validator

_REVIEWER = "reviewer"
_CATEGORY = "code_review"
_SCHEMA = {
    "code_review": {
        "fields": {
            "file_path": "string",
            "issues": "list",
            "summary": "string",
        }
    }
}
_SCHEMA_FILE = {"name": _REVIEWER, "output_schema": _SCHEMA}

_NO_SCHEMA_FILE = {"name": "no_schema_role"}


@pytest.fixture
def roles_dir():
    """Patch _ROLES_DIR with a temp directory containing test persona files."""
    d = tempfile.mkdtemp()
    (Path(d) / f"{_REVIEWER}.json").write_text(json.dumps(_SCHEMA_FILE), encoding="utf-8")
    (Path(d) / "no_schema_role.json").write_text(
        json.dumps(_NO_SCHEMA_FILE), encoding="utf-8"
    )
    # A corrupted file that isn't valid JSON
    (Path(d) / "corrupted.json").write_text("this is not json", encoding="utf-8")
    with mock.patch.object(output_validator, "_ROLES_DIR", Path(d)):
        yield


class TestValidateOutput:
    def test_validate_no_schema(self, roles_dir):
        """Role with no output_schema → valid=True, severity='unknown'."""
        result = output_validator.validate_output(
            "no_schema_role", _CATEGORY, {"a": 1}
        )
        assert result == {"valid": True, "missing": [], "severity": "unknown"}

    def test_validate_complete(self, roles_dir):
        """Content matches every required field → valid=True."""
        content = {"file_path": "main.py", "issues": ["bug"], "summary": "ok"}
        result = output_validator.validate_output(_REVIEWER, _CATEGORY, content)
        assert result == {"valid": True, "missing": [], "severity": "ok"}

    def test_validate_missing_fields(self, roles_dir):
        """Missing required fields → valid=False with the missing keys."""
        content = {"file_path": "main.py"}
        result = output_validator.validate_output(_REVIEWER, _CATEGORY, content)
        assert result["valid"] is False
        assert result["severity"] == "error"
        assert "issues" in result["missing"]
        assert "summary" in result["missing"]

    def test_validate_non_json(self, roles_dir):
        """Corrupted / nonexistent role file silently skips validation."""
        result = output_validator.validate_output("undefined", _CATEGORY, {"x": 1})
        assert result == {"valid": True, "missing": [], "severity": "unknown"}


class TestLoadRoleSchema:
    def test_load_role_schema_exists(self, roles_dir):
        """Return the category schema dict for a known role."""
        schema = output_validator._load_role_schema(_REVIEWER, _CATEGORY)
        assert schema == _SCHEMA[_CATEGORY]

    def test_load_role_schema_none(self, roles_dir):
        """Return None for a role that doesn't exist."""
        schema = output_validator._load_role_schema("nonexistent", _CATEGORY)
        assert schema is None
