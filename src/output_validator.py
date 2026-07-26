"""Output validation for session pipeline roles.

Validates that agent-produced content matches the declared output_schema
from the role's persona JSON file.  Handles both flat schemas::

    {"field_name": "type_or_description", ...}

and nested schemas::

    {"description": "...", "fields": {"field_name": ..., ...}}
"""

from pathlib import Path
import json
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

_ROLES_DIR = Path.home() / "hermes-session-roles" / "personas" / "session-roles"


def _load_role_schema(role: str, category: str) -> dict[str, Any] | None:
    """Return the output_schema dict for *role* / *category*, or None."""
    if not _ROLES_DIR.is_dir():
        return None
    for fpath in sorted(_ROLES_DIR.glob("persona_*.json")):
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("name") != role:
            continue
        schema = data.get("output_schema")
        if not isinstance(schema, dict):
            LOGGER.debug("role %r found in %s but output_schema missing or not a dict", role, fpath)
            return None
        return schema.get(category)
    return None


def validate_output(role: str, category: str, content: dict[str, Any]) -> dict[str, Any]:
    """Validate *content* against the role's declared output_schema.

    Parameters
    ----------
    role:
        Role name as it appears in the persona file's ``name`` field.
    category:
        Output category key in the schema (e.g. ``"code_fix"``).
    content:
        Dict produced by the agent.

    Returns
    -------
    dict
        ``{"valid": bool, "missing": list[str], "severity": str}``.

        * ``valid`` — True when all required fields are present.
        * ``missing`` — keys absent or ``None`` in *content*.
        * ``severity`` — ``"error"`` when any field is missing,
          ``"unknown"`` when the role has no output_schema at all,
          ``"ok"`` on success.

        When no output_schema exists for the role the result is always::

            {"valid": True, "missing": [], "severity": "unknown"}
    """
    cat_schema = _load_role_schema(role, category)

    if cat_schema is None:
        return {"valid": True, "missing": [], "severity": "unknown"}

    # Two schema forms:
    #   1) flat:  {"field": "type_or_desc", ...}
    #   2) nested: {"description": "...", "fields": {"field": ..., ...}}
    if isinstance(cat_schema, dict) and "fields" in cat_schema:
        required = list(cat_schema["fields"].keys())
    elif isinstance(cat_schema, dict):
        required = list(cat_schema.keys())
    else:
        required = []

    missing = [k for k in required if k not in content or content[k] is None]

    if missing:
        return {"valid": False, "missing": missing, "severity": "error"}

    return {"valid": True, "missing": [], "severity": "ok"}
