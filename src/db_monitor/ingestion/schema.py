"""Canonical ingestion schema exports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_schema_discovery = importlib.import_module("schema_discovery")

SchemaDiscovery = _schema_discovery.SchemaDiscovery
extract_operation = _schema_discovery.extract_operation
schema_cache_backplane = _schema_discovery.schema_cache_backplane

__all__ = [
    "SchemaDiscovery",
    "extract_operation",
    "schema_cache_backplane",
]
