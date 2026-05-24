"""Compatibility wrapper for canonical API router exports."""

from __future__ import annotations

import importlib

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

router = importlib.import_module("db_monitor.api.router").router

__all__ = ["router"]
