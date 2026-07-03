"""Module-level state and cache invalidation for SchemaDiscovery instances."""

from __future__ import annotations

import weakref

_registered_schema_discoveries = weakref.WeakSet()


def _invalidate_registered_schema_caches(
    service_name: str,
    table_name: str,
) -> None:
    """Invalidate one table cache across all live discovery instances."""
    for discovery in list(_registered_schema_discoveries):
        discovery._invalidate_table_cache(service_name, table_name)
