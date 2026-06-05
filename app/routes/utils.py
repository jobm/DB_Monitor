"""Shared helper utilities for API route modules."""

from __future__ import annotations

import base64
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


# ── Cursor token helpers ──────────────────────────────────────────────

_CURSOR_VERSION = 1


def encode_cursor_token(
    timestamp: datetime,
    id_value: int,
) -> str:
    """Encode a keyset cursor into an opaque URL-safe token.

    Produces a base64-encoded JSON payload that clients can pass back
    verbatim as the ``?cursor=`` query parameter.
    """
    payload = {
        "v": _CURSOR_VERSION,
        "t": timestamp.isoformat(),
        "i": id_value,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor_token(
    token: str | None,
    field_name: str = "cursor",
) -> tuple[datetime, int] | None:
    """Decode a cursor token back into ``(timestamp, id)``.

    Returns ``None`` when *token* is ``None`` or empty so callers can
    treat a missing cursor as a no-op.  Raises ``HTTPException`` (400)
    when the token is present but malformed.
    """
    if token is None:
        return None

    stripped = token.strip()
    if not stripped:
        return None

    # Backward-compat: accept legacy JSON-object cursors
    if stripped.startswith("{"):
        return _decode_legacy_json_cursor(stripped, field_name)

    try:
        # Restore base64 padding
        padding = 4 - len(stripped) % 4
        if padding != 4:
            stripped += "=" * padding
        raw = base64.urlsafe_b64decode(stripped)
        payload = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name} token. "
                "Pass the value returned in next_cursor unchanged."
            ),
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name} token. "
                "Pass the value returned in next_cursor unchanged."
            ),
        )

    version = payload.get("v")
    if version != _CURSOR_VERSION:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported {field_name} token version "
                f"({version!r}). Obtain a fresh cursor from a recent "
                "request."
            ),
        )

    timestamp = parse_timestamp(payload.get("t"), f"{field_name}.t")
    if timestamp is None:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} token is missing the timestamp field.",
        )

    id_raw = payload.get("i")
    if id_raw is None:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} token is missing the id field.",
        )

    try:
        id_value = int(id_raw)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} token has an invalid id field.",
        )

    if id_value <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} token id must be positive.",
        )

    return timestamp, id_value


def _decode_legacy_json_cursor(
    raw: str,
    field_name: str,
) -> tuple[datetime, int] | None:
    """Decode a legacy JSON-object cursor string for backward compat."""
    parsed = parse_json_object_query(raw, field_name)
    if parsed is None:
        return None

    # Try events-style fields
    cursor_id = parsed.get("event_id") or parsed.get("id")
    cursor_time = (
        parsed.get("capture_time")
        or parsed.get("changed_at")
        or parsed.get("event_time")
    )
    if cursor_id is None or cursor_time is None:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{field_name} must include id/event_id and "
                "capture_time/changed_at."
            ),
        )

    timestamp = parse_timestamp(str(cursor_time), f"{field_name}.time")
    if timestamp is None:
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} contains an invalid timestamp.",
        )

    try:
        id_value = int(cursor_id)
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=f"{field_name} contains an invalid id.",
        )

    return timestamp, id_value
