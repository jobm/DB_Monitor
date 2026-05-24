"""Canonical routes namespace package."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_legacy_module = importlib.import_module("routes")
router = _legacy_module.router

__all__ = ["router"]
