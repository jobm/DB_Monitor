"""Compatibility wrapper for canonical API factory helpers."""

from __future__ import annotations

import importlib

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

create_application = importlib.import_module(
    "db_monitor.api.factory"
).create_application

__all__ = ["create_application"]
