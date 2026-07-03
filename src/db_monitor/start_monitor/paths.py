"""Canonical manifest and schema path resolution for DB Monitor startup."""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def _default_manifest_schema_path() -> Path:
    """Return the default connector manifest schema path."""
    container_schema = Path("/connectors/sources.schema.json")
    if container_schema.exists():
        return container_schema

    return (
        Path(__file__).resolve().parents[3]
        / "connectors"
        / "sources.schema.json"
    )


def _default_manifest_path() -> Path:
    """Return the default connector manifest path.

    This works for both local and container runs.
    """
    container_manifest = Path("/connectors/sources.json")
    if container_manifest.exists():
        return container_manifest

    return Path(__file__).resolve().parents[3] / "connectors" / "sources.json"


CONNECTOR_MANIFEST = Path(
    os.getenv("CONNECTOR_SOURCES_FILE", str(_default_manifest_path()))
)
CONNECTOR_MANIFEST_SCHEMA = Path(
    os.getenv(
        "CONNECTOR_SOURCES_SCHEMA_FILE",
        str(_default_manifest_schema_path()),
    )
)
