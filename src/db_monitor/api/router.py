"""Canonical API router exports for package-standardized imports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

router = importlib.import_module("routes").router

__all__ = ["router"]
