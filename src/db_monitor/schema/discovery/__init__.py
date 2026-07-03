"""Schema discovery sub-package for extracting and registering table schemas."""

from db_monitor.schema.discovery.core import SchemaDiscovery
from db_monitor.schema.discovery.state import _invalidate_registered_schema_caches

__all__ = [
    "SchemaDiscovery",
    "_invalidate_registered_schema_caches",
]
