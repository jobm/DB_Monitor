"""Compatibility wrapper for canonical config loading helpers."""

from __future__ import annotations

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

from db_monitor.core.config_loader import (
    config_error,
    default_schema_mode,
    get_env_or_file,
    parse_csv_list,
)

__all__ = [
    "config_error",
    "default_schema_mode",
    "get_env_or_file",
    "parse_csv_list",
]
