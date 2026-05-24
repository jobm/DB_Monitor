"""Canonical ingestion change-processor exports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

ChangeProcessor = importlib.import_module(
    "change_processor"
).ChangeProcessor

__all__ = ["ChangeProcessor"]
