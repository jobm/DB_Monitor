"""Column extraction utilities for Debezium CDC event payloads."""

from __future__ import annotations

from typing import Any, Optional

from db_monitor.schema.operations import infer_data_type, normalize_field_type


def extract_column_definitions(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract column definitions from a Debezium event payload.

    Prefers the schema section of the message when available; falls
    back to inferring columns from the after / before rows.
    """
    schema = payload.get("schema")
    if not isinstance(schema, dict):
        nested_payload = payload.get("payload")
        if isinstance(nested_payload, dict):
            nested_schema = nested_payload.get("schema")
            if isinstance(nested_schema, dict):
                schema = nested_schema

    column_definitions = (
        _extract_columns_from_schema(schema)
        if isinstance(schema, dict)
        else []
    )
    if column_definitions:
        return column_definitions

    return _extract_columns_from_payload(payload)


def _extract_columns_from_schema(
    schema: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract columns from the Debezium schema section."""
    row_fields = _extract_row_fields(schema.get("fields"))
    if not row_fields:
        return []

    columns: list[dict[str, Any]] = []
    for field_def in row_fields:
        if not isinstance(field_def, dict):
            continue

        field_name = field_def.get("field") or field_def.get("name")
        if not field_name or str(field_name).startswith("__"):
            continue

        field_type = (
            field_def.get("type") or field_def.get("name") or "unknown"
        )
        columns.append(
            {
                "column_name": str(field_name),
                "data_type": normalize_field_type(field_type),
                "is_primary_key": bool(field_def.get("primaryKey", False)),
                "is_nullable": bool(field_def.get("optional", True)),
                "audit_enabled": True,
            }
        )

    return columns


def _extract_row_fields(fields: Any) -> list[dict[str, Any]]:
    """Extract row-level field definitions from schema fields."""
    if isinstance(fields, list):
        row_container = _find_row_schema(fields)
        if isinstance(row_container, dict) and isinstance(
            row_container.get("fields"), list
        ):
            return [
                field
                for field in row_container["fields"]
                if isinstance(field, dict)
            ]
        return [field for field in fields if isinstance(field, dict)]

    if isinstance(fields, dict):
        for candidate_name in ("after", "before"):
            candidate = fields.get(candidate_name)
            if isinstance(candidate, dict) and isinstance(
                candidate.get("fields"), list
            ):
                return [
                    field
                    for field in candidate["fields"]
                    if isinstance(field, dict)
                ]

        extracted_fields: list[dict[str, Any]] = []
        for field_name, field_def in fields.items():
            if isinstance(field_def, dict):
                extracted_fields.append({"field": field_name, **field_def})
        return extracted_fields

    return []


def _find_row_schema(
    fields: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Find the after/before schema container in a field list."""
    for candidate in fields:
        field_name = candidate.get("field") or candidate.get("name")
        if field_name in {"after", "before"} and isinstance(
            candidate.get("fields"), list
        ):
            return candidate
    return None


def _extract_columns_from_payload(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Infer columns from after/before row payloads when no schema is available."""
    row_versions: list[dict[str, Any]] = []
    for row_key in ("after", "before"):
        row_data = payload.get(row_key)
        if not isinstance(row_data, dict):
            nested_payload = payload.get("payload")
            if isinstance(nested_payload, dict):
                nested_row_data = nested_payload.get(row_key)
                if isinstance(nested_row_data, dict):
                    row_data = nested_row_data

        if isinstance(row_data, dict):
            row_versions.append(row_data)

    if not row_versions:
        return []

    ordered_columns: list[str] = []
    for row_version in row_versions:
        for column_name in row_version:
            if (
                column_name.startswith("__")
                or column_name in ordered_columns
            ):
                continue
            ordered_columns.append(column_name)

    columns: list[dict[str, Any]] = []
    for column_name in ordered_columns:
        sample_value = next(
            (
                row_version[column_name]
                for row_version in row_versions
                if column_name in row_version
                and row_version[column_name] is not None
            ),
            None,
        )
        columns.append(
            {
                "column_name": column_name,
                "data_type": infer_data_type(sample_value),
                "is_primary_key": False,
                "is_nullable": any(
                    column_name in row_version
                    and row_version[column_name] is None
                    for row_version in row_versions
                ),
                "audit_enabled": True,
            }
        )

    return columns
