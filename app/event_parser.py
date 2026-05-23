"""Event parsing utilities.

This module provides best-effort parsing of incoming Kafka message
payloads into structured fields that can be stored and queried
efficiently.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from source_metadata import extract_source_service_name

# ── Payload key constants ──────────────────────────────────────────
# Top-level event keys
KEY_EVENT_TYPE = "event_type"
KEY_TYPE = "type"
KEY_EVENT_TIME = "event_time"
KEY_TIMESTAMP_MS = "timestamp_ms"
KEY_TS = "ts"
KEY_USER_ID = "user_id"
KEY_USER_ID_CAMEL = "userId"
KEY_USER = "user"
KEY_SERVICE_NAME = "service_name"
KEY_SERVICE = "service"

# Debezium envelope keys
KEY_OP = "op"
KEY_PAYLOAD = "payload"
KEY_TS_MS = "ts_ms"

# Fallback/status values
VAL_UNKNOWN = "unknown"
VAL_UNPARSED = "unparsed"
VAL_DATA = "data"


@dataclass(frozen=True)
class ParsedEvent:
    """Structured representation of an incoming event payload."""

    event_type: str
    event_time: datetime
    user_id: Optional[str]
    service_name: Optional[str]
    event_data: Optional[dict[str, Any]]
    raw_payload: str


def _coerce_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware in UTC."""

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 datetime string, accepting a trailing 'Z'."""

    try:
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        return _coerce_utc(datetime.fromisoformat(candidate))
    except Exception:
        return None


def parse_event_payload(raw_payload: str) -> ParsedEvent:
    """Parse a raw Kafka message payload into structured event fields.

    Best-effort extraction:
    - event_type: prefer explicit keys, else Debezium `op`, else "unknown"
    - event_time: prefer `event_time`, else Debezium `ts_ms`, else now()
    - user_id/service_name: best-effort from common keys

    If the payload is not valid JSON, event_data will be None and
    event_type will be "unparsed".
    """

    now = datetime.now(timezone.utc)

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return ParsedEvent(
            event_type=VAL_UNPARSED,
            event_time=now,
            user_id=None,
            service_name=None,
            event_data=None,
            raw_payload=raw_payload,
        )

    data: dict[str, Any]
    if isinstance(payload, dict):
        data = payload
    else:
        data = {VAL_DATA: payload}

    event_type_value = (
        data.get(KEY_EVENT_TYPE)
        or data.get(KEY_TYPE)
        or data.get(KEY_OP)
        or (
            data.get(KEY_PAYLOAD, {}).get(KEY_OP)
            if isinstance(data.get(KEY_PAYLOAD), dict)
            else None
        )
        or VAL_UNKNOWN
    )
    event_type = str(event_type_value)

    event_time: datetime = now
    explicit_event_time = (
        data.get(KEY_PAYLOAD, {}).get(KEY_TS_MS)
        if isinstance(data.get(KEY_PAYLOAD), dict)
        else None
    ) or data.get(KEY_EVENT_TIME)
    if isinstance(explicit_event_time, str):
        parsed = _parse_iso_datetime(explicit_event_time)
        if parsed is not None:
            event_time = parsed
    elif isinstance(explicit_event_time, (int, float)):
        seconds = explicit_event_time
        if seconds > 10_000_000_000:
            seconds = seconds / 1000.0
        event_time = datetime.fromtimestamp(seconds, tz=timezone.utc)

    if event_time == now:
        ts_ms = data.get(KEY_TS_MS) or data.get(KEY_TIMESTAMP_MS) or data.get(KEY_TS)
        if isinstance(ts_ms, (int, float)):
            seconds = ts_ms
            if seconds > 10_000_000_000:
                seconds = seconds / 1000.0
            event_time = datetime.fromtimestamp(seconds, tz=timezone.utc)

    user_id_value = (
        data.get(KEY_USER_ID) or data.get(KEY_USER_ID_CAMEL) or data.get(KEY_USER)
    )
    user_id = str(user_id_value) if user_id_value is not None else None

    service_name_value = data.get(KEY_SERVICE_NAME) or data.get(KEY_SERVICE)
    if service_name_value is None:
        service_name_value = extract_source_service_name(data)
    service_name = (
        str(service_name_value) if service_name_value is not None else None
    )

    return ParsedEvent(
        event_type=event_type,
        event_time=event_time,
        user_id=user_id,
        service_name=service_name,
        event_data=data,
        raw_payload=raw_payload,
    )
