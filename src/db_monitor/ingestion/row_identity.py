"""Helpers for extracting stable row identity from CDC payloads."""

from __future__ import annotations

from typing import Any, Optional


def _event_payload(event_data: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Return the Debezium payload envelope from stored event data."""
    if not isinstance(event_data, dict):
        return {}

    payload = event_data.get("payload")
    if isinstance(payload, dict):
        return payload
    return event_data


def extract_row_identity(
    event_data: Optional[dict[str, Any]],
    primary_key_columns: Optional[list[str]] = None,
) -> Optional[dict[str, Any]]:
    """Return a best-effort row identity for a CDC event.

    Preference order:
    1. Declared primary key columns when available.
    2. Conventional identity columns such as `id` and `*_id`.
    """

    payload = _event_payload(event_data)
    after_value = payload.get("after")
    before_value = payload.get("before")
    row_data = after_value if isinstance(after_value, dict) else None
    if row_data is None:
        row_data = before_value if isinstance(before_value, dict) else None
    if row_data is None:
        return None

    candidate_keys: list[str] = []
    if primary_key_columns:
        candidate_keys.extend(
            column_name
            for column_name in primary_key_columns
            if column_name in row_data
        )

    if not candidate_keys and "id" in row_data:
        candidate_keys.append("id")

    candidate_keys.extend(
        key
        for key in sorted(row_data)
        if key not in candidate_keys and key.endswith("_id")
    )
    candidate_keys.extend(
        key
        for key in sorted(row_data)
        if key not in candidate_keys and "id" in key.lower()
    )

    if not candidate_keys:
        return None

    row_identity = {
        key: row_data.get(key)
        for key in candidate_keys
        if row_data.get(key) is not None
    }
    return row_identity or None
