"""Shared helper utilities for API route modules."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import HTTPException


def parse_timestamp(value: str | None, field_name: str) -> datetime | None:
    """Parse optional ISO timestamps used by query parameters."""
    if value is None:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name} format. "
                "Expected an ISO 8601 timestamp such as "
                "'2024-01-01T00:00:00Z'."
            ),
        ) from exc


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def parse_json_object_query(
    value: str | None,
    field_name: str,
) -> dict[str, object] | None:
    """Parse a query parameter expected to contain a JSON object."""
    if value is None:
        return None

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name}. Expected a JSON object string such as "
                "'{\"id\": 42}'."
            ),
        ) from exc

    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name}. Expected a JSON object string such as "
                "'{\"id\": 42}'."
            ),
        )
    return parsed


def timestamp_age_seconds(value: str | None) -> float | None:
    """Return the age of an ISO timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((utc_now() - parsed).total_seconds(), 0.0)
