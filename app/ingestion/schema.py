"""Compatibility wrapper for schema discovery state."""

from __future__ import annotations

import schema_discovery as _schema_discovery

SCHEMA_CACHE_BACKPLANE_CHANNEL = (
    _schema_discovery.SCHEMA_CACHE_BACKPLANE_CHANNEL
)
SCHEMA_CACHE_BACKPLANE_DSN = (
    _schema_discovery.SCHEMA_CACHE_BACKPLANE_DSN
)
SchemaCacheBackplane = _schema_discovery.SchemaCacheBackplane
SchemaDiscovery = _schema_discovery.SchemaDiscovery
extract_operation = _schema_discovery.extract_operation
schema_cache_backplane = _schema_discovery.schema_cache_backplane
