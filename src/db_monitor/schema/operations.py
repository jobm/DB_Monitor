"""Standalone schema operations: operation extraction and type helpers."""

from __future__ import annotations

from typing import Any


def extract_operation(payload: dict[str, Any]) -> str:
    """Extract operation type from Debezium payload.

    Returns one of INSERT, UPDATE, DELETE (defaults to UPDATE when ambiguous).
    """
    op = (
        payload.get("op")
        or (
            payload.get("payload", {}).get("op")
            if isinstance(payload.get("payload"), dict)
            else None
        )
        or "u"
    )
    op_map = {
        "c": "INSERT",
        "r": "INSERT",
        "u": "UPDATE",
        "d": "DELETE",
    }
    return op_map.get(op, "UPDATE")


def normalize_field_type(field_type: Any) -> str:
    """Normalize a Debezium field type to a string representation."""
    if isinstance(field_type, dict):
        return str(
            field_type.get("name") or field_type.get("type") or "unknown"
        )
    return str(field_type)


def infer_data_type(value: Any) -> str:
    """Infer a column data type from a sample Python value."""
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"
