"""Shared config loading helpers.

This module centralizes low-level parsing and secret-file loading logic.
"""

from __future__ import annotations

import os
from pathlib import Path


def config_error(message: str, hint: str | None = None) -> None:
    """Raise a configuration error with an optional remediation hint."""
    detail = message if hint is None else f"{message} {hint}"
    raise ValueError(detail)


def default_schema_mode(app_env: str) -> str:
    """Return the safest default schema policy for the active environment."""
    if app_env in {"production", "prod"}:
        return "validate"
    return "apply"


def parse_csv_list(value: str | None) -> list[str]:
    """Parse a comma-separated configuration string into trimmed values."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def get_env_or_file(name: str, default: str | None = None) -> str | None:
    """Return a config value from NAME or NAME_FILE."""
    value = os.getenv(name)
    if value is not None:
        return value

    file_path = os.getenv(f"{name}_FILE")
    if file_path:
        resolved_path = Path(file_path)
        if not resolved_path.exists():
            config_error(
                (
                    f"{name}_FILE points to '{resolved_path}', but that "
                    "file does not exist."
                ),
                hint=(
                    "Mount the secret file at that path or set "
                    f"{name} directly."
                ),
            )
        try:
            return resolved_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            config_error(
                f"Failed to read {name}_FILE from '{resolved_path}'.",
                hint=(
                    "Check file permissions and contents. "
                    f"Original error: {exc}"
                ),
            )

    return default
