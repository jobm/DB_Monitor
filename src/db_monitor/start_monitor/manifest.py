"""Connector manifest reading, topic derivation, and environment configuration."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import cast

from db_monitor.start_monitor.paths import CONNECTOR_MANIFEST
from db_monitor.start_monitor.validation import _validate_manifest_payload

logger = logging.getLogger(__name__)


def _log_no_sources_guidance() -> None:
    """Log a warning with guidance when no source databases are configured."""
    logger.warning(
        "No source databases configured. CDC ingestion will not start until "
        "at least one source is defined in the source manifest. "
        "See https://github.com/jobm/DB_Monitor#readme or "
        "docs/configuration.md for how to configure your sources at "
        "connectors/sources.json or via the CONNECTOR_SOURCES_FILE env var."
    )


def _read_manifest_payload(manifest_path: Path) -> dict[str, object] | None:
    """Read and validate one source manifest file."""
    try:
        raw_payload: object = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        logger.warning(
            "Source manifest is not valid JSON at %s: %s. "
            "Starting with empty topic subscription backplane.",
            manifest_path,
            exc,
        )
        return None

    validation_errors = _validate_manifest_payload(raw_payload)
    if validation_errors:
        logger.warning(
            "Source manifest at %s failed schema validation: %s. "
            "Starting with empty topic subscription backplane.",
            manifest_path,
            "; ".join(validation_errors),
        )
        return None

    return cast(dict[str, object], raw_payload)


def _derived_topics_from_manifest(manifest_path: Path) -> list[str]:
    """Build the subscribed Kafka topic list from enabled connectors."""
    if not manifest_path.exists():
        return []

    payload = _read_manifest_payload(manifest_path)
    if payload is None:
        return []

    topics: list[str] = []
    connectors = cast(list[object], payload.get("connectors", []))
    for connector in connectors:
        if not isinstance(connector, dict):
            continue
        if not connector.get("enabled", True):
            continue

        source_name = connector.get("source_name")
        tables = connector.get("tables", [])
        if not source_name or not isinstance(tables, list):
            continue

        topics.extend(
            f"{source_name}.{table}"
            for table in tables
            if isinstance(table, str) and table
        )

    # Preserve manifest order while removing duplicates.
    return list(dict.fromkeys(topics))


def _configure_topics() -> None:
    """Populate KAFKA_TOPICS from the manifest when the env var is absent."""
    if os.getenv("KAFKA_TOPICS"):
        return

    if not CONNECTOR_MANIFEST.exists():
        logger.warning(
            "No connector source manifest found at %s. Mount a manifest "
            "or set CONNECTOR_SOURCES_FILE. Starting with empty topic "
            "subscription backplane.",
            CONNECTOR_MANIFEST,
        )
        _log_no_sources_guidance()
        return

    topics = _derived_topics_from_manifest(CONNECTOR_MANIFEST)
    if topics:
        os.environ["KAFKA_TOPICS"] = ",".join(topics)
    else:
        logger.warning(
            "Source manifest found at %s but contains no active/enabled "
            "connectors. Starting with empty topic subscription backplane.",
            CONNECTOR_MANIFEST,
        )
        _log_no_sources_guidance()
