from __future__ import annotations

import json
import os
from pathlib import Path

import uvicorn


def _default_manifest_path() -> Path:
    """Return the default connector manifest path.

    This works for both local and container runs.
    """
    container_manifest = Path("/connectors/sources.json")
    if container_manifest.exists():
        return container_manifest

    return Path(__file__).resolve().parents[1] / "connectors" / "sources.json"


CONNECTOR_MANIFEST = Path(
    os.getenv("CONNECTOR_SOURCES_FILE", str(_default_manifest_path()))
)


def _derived_topics_from_manifest(manifest_path: Path) -> list[str]:
    """Build the subscribed Kafka topic list from enabled connectors."""
    if not manifest_path.exists():
        return []

    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        print(
            "WARNING: Source manifest is not valid JSON "
            f"({manifest_path}): {exc}.\n"
            "Starting with empty topic subscription backplane."
        )
        return []

    topics: list[str] = []
    for connector in payload.get("connectors", []):
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
        print(
            "WARNING: No connector source manifest found at default paths.\n"
            "If integrating DB Monitor with custom systems, please mount a configuration file\n"
            "or set 'CONNECTOR_SOURCES_FILE' to target a custom source manifest.\n"
            "Starting with empty topic subscription backplane."
        )
        return

    topics = _derived_topics_from_manifest(CONNECTOR_MANIFEST)
    if topics:
        os.environ["KAFKA_TOPICS"] = ",".join(topics)
    else:
        print(
            "WARNING: Source manifest found but contains no active or enabled connectors.\n"
            "Starting with empty topic subscription backplane."
        )


def main() -> None:
    """Start the API after aligning topics with the connector manifest."""
    _configure_topics()
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
