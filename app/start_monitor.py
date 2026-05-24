from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
import re
import signal

import uvicorn


logger = logging.getLogger(__name__)


_SOURCE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
_TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$")
_CONNECTOR_REQUIRED_FIELDS = {
    "source_name",
    "database_hostname",
    "tables",
}
_CONNECTOR_ALLOWED_FIELDS = {
    "source_name",
    "database_hostname",
    "tables",
    "enabled",
    "connector_name",
    "slot_name",
    "history_topic",
    "config_overrides",
}


def _default_manifest_schema_path() -> Path:
    """Return the default connector manifest schema path."""
    container_schema = Path("/connectors/sources.schema.json")
    if container_schema.exists():
        return container_schema

    return (
        Path(__file__).resolve().parents[1]
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

    return Path(__file__).resolve().parents[1] / "connectors" / "sources.json"


CONNECTOR_MANIFEST = Path(
    os.getenv("CONNECTOR_SOURCES_FILE", str(_default_manifest_path()))
)
CONNECTOR_MANIFEST_SCHEMA = Path(
    os.getenv(
        "CONNECTOR_SOURCES_SCHEMA_FILE",
        str(_default_manifest_schema_path()),
    )
)


def _manifest_schema_rules(
    schema_path: Path,
) -> tuple[set[str], set[str], str, str]:
    """Load validation rules from the JSON schema file when available."""
    try:
        schema = json.loads(schema_path.read_text())
    except FileNotFoundError:
        return (
            _CONNECTOR_REQUIRED_FIELDS,
            _CONNECTOR_ALLOWED_FIELDS,
            _SOURCE_NAME_PATTERN.pattern,
            _TABLE_NAME_PATTERN.pattern,
        )
    except json.JSONDecodeError as exc:
        logger.warning(
            "Connector schema at %s is invalid JSON (%s). "
            "Falling back to built-in manifest validation rules.",
            schema_path,
            exc,
        )
        return (
            _CONNECTOR_REQUIRED_FIELDS,
            _CONNECTOR_ALLOWED_FIELDS,
            _SOURCE_NAME_PATTERN.pattern,
            _TABLE_NAME_PATTERN.pattern,
        )

    connector_item = (
        schema.get("properties", {})
        .get("connectors", {})
        .get("items", {})
    )
    schema_required = set(connector_item.get("required", []))
    schema_allowed = set(connector_item.get("properties", {}).keys())
    source_pattern = (
        connector_item.get("properties", {})
        .get("source_name", {})
        .get("pattern", _SOURCE_NAME_PATTERN.pattern)
    )
    table_pattern = (
        connector_item.get("properties", {})
        .get("tables", {})
        .get("items", {})
        .get("pattern", _TABLE_NAME_PATTERN.pattern)
    )

    if not schema_required or not schema_allowed:
        return (
            _CONNECTOR_REQUIRED_FIELDS,
            _CONNECTOR_ALLOWED_FIELDS,
            _SOURCE_NAME_PATTERN.pattern,
            _TABLE_NAME_PATTERN.pattern,
        )

    return schema_required, schema_allowed, source_pattern, table_pattern


def _validate_manifest_payload(payload: object) -> list[str]:
    """Validate the source manifest payload against expected schema rules."""
    errors: list[str] = []
    (
        connector_required_fields,
        connector_allowed_fields,
        source_name_pattern,
        table_name_pattern,
    ) = _manifest_schema_rules(CONNECTOR_MANIFEST_SCHEMA)

    if not isinstance(payload, dict):
        return ["Manifest root must be a JSON object."]

    unknown_root_fields = set(payload) - {"connectors"}
    if unknown_root_fields:
        errors.append(
            "Unknown root fields: "
            + ", ".join(sorted(str(item) for item in unknown_root_fields))
        )

    connectors = payload.get("connectors")
    if not isinstance(connectors, list) or not connectors:
        errors.append("'connectors' must be a non-empty array.")
        return errors

    for index, connector in enumerate(connectors):
        path = f"connectors[{index}]"
        if not isinstance(connector, dict):
            errors.append(f"{path} must be an object.")
            continue

        missing_fields = connector_required_fields - set(connector)
        if missing_fields:
            missing_text = ", ".join(sorted(missing_fields))
            errors.append(f"{path} missing required fields: {missing_text}")

        unknown_fields = set(connector) - connector_allowed_fields
        if unknown_fields:
            unknown_text = ", ".join(sorted(unknown_fields))
            errors.append(f"{path} has unknown fields: {unknown_text}")

        source_name = connector.get("source_name")
        source_name_valid = isinstance(source_name, str) and (
            re.fullmatch(source_name_pattern, source_name) is not None
        )
        if not source_name_valid:
            errors.append(
                f"{path}.source_name must match {source_name_pattern}"
            )

        database_hostname = connector.get("database_hostname")
        if not isinstance(database_hostname, str) or not database_hostname:
            errors.append(
                f"{path}.database_hostname must be a non-empty string"
            )

        tables = connector.get("tables")
        if not isinstance(tables, list) or not tables:
            errors.append(f"{path}.tables must be a non-empty array")
        else:
            for table_index, table_name in enumerate(tables):
                table_name_valid = isinstance(table_name, str) and (
                    re.fullmatch(table_name_pattern, table_name) is not None
                )
                if not table_name_valid:
                    errors.append(
                        f"{path}.tables[{table_index}] must match "
                        f"{table_name_pattern}"
                    )

        enabled = connector.get("enabled")
        if enabled is not None and not isinstance(enabled, bool):
            errors.append(f"{path}.enabled must be a boolean")

        for field in (
            "connector_name",
            "slot_name",
            "history_topic",
        ):
            field_value = connector.get(field)
            if field_value is not None and not isinstance(field_value, str):
                errors.append(f"{path}.{field} must be a string")

        config_overrides = connector.get("config_overrides")
        if config_overrides is not None and not isinstance(
            config_overrides,
            dict,
        ):
            errors.append(f"{path}.config_overrides must be an object")

    return errors


def _read_manifest_payload(manifest_path: Path) -> dict[str, object] | None:
    """Read and validate one source manifest file."""
    try:
        raw_payload = json.loads(manifest_path.read_text())
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

    return raw_payload


def _derived_topics_from_manifest(manifest_path: Path) -> list[str]:
    """Build the subscribed Kafka topic list from enabled connectors."""
    if not manifest_path.exists():
        return []

    payload = _read_manifest_payload(manifest_path)
    if payload is None:
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
        logger.warning(
            "No connector source manifest found at %s. Mount a manifest "
            "or set CONNECTOR_SOURCES_FILE. Starting with empty topic "
            "subscription backplane.",
            CONNECTOR_MANIFEST,
        )
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


def _register_shutdown_signals(
    loop: asyncio.AbstractEventLoop,
    server: uvicorn.Server,
) -> None:
    """Register SIGINT/SIGTERM handlers that trigger a graceful stop."""

    def _request_shutdown(sig_name: str) -> None:
        if server.should_exit:
            return
        logger.info("Received %s; requesting graceful shutdown", sig_name)
        server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                _request_shutdown,
                sig.name,
            )
        except (NotImplementedError, RuntimeError, ValueError):
            # Some environments (e.g. Windows/sub-threads) do not support
            # add_signal_handler. Uvicorn's defaults still apply there.
            logger.debug(
                "Signal handler registration unsupported for %s",
                sig.name,
            )


async def _run_server() -> None:
    """Run the Uvicorn server with graceful signal handling hooks."""
    config = uvicorn.Config(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
    server = uvicorn.Server(config)
    _register_shutdown_signals(asyncio.get_running_loop(), server)
    await server.serve()


def main() -> None:
    """Start the API after aligning topics with the connector manifest."""
    _configure_topics()
    asyncio.run(_run_server())


if __name__ == "__main__":
    main()
