"""Canonical startup entrypoint for DB Monitor.

Topic configuration from manifest, Uvicorn server startup,
signal handling, and startup/shutdown hooks.
"""

from __future__ import annotations

import os as _os

from db_monitor.start_monitor.manifest import (
    _configure_topics,
    _derived_topics_from_manifest,
    _read_manifest_payload,
)
from db_monitor.start_monitor.paths import (
    CONNECTOR_MANIFEST,
    CONNECTOR_MANIFEST_SCHEMA,
    _default_manifest_path,
    _default_manifest_schema_path,
)
from db_monitor.start_monitor.server import (
    _register_shutdown_signals,
    _run_server,
    main,
)
from db_monitor.start_monitor.validation import (
    _manifest_schema_rules,
    _validate_manifest_payload,
)

# Re-export the os module so tests can do start_monitor.os.environ.
os = _os

__all__ = [
    "CONNECTOR_MANIFEST",
    "CONNECTOR_MANIFEST_SCHEMA",
    "_configure_topics",
    "_default_manifest_path",
    "_default_manifest_schema_path",
    "_derived_topics_from_manifest",
    "_manifest_schema_rules",
    "_read_manifest_payload",
    "_register_shutdown_signals",
    "_run_server",
    "_validate_manifest_payload",
    "main",
    "os",
]
