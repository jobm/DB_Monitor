"""Connector manifest validation — schema rules and payload validation."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from db_monitor.start_monitor import paths as _paths

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
    ) = _manifest_schema_rules(_paths.CONNECTOR_MANIFEST_SCHEMA)

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
