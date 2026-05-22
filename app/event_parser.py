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
            event_type="unparsed",
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
        data = {"data": payload}

    event_type_value = (
        data.get("event_type")
        or data.get("type")
        or data.get("op")
        or (
            data.get("payload", {}).get("op")
            if isinstance(data.get("payload"), dict)
            else None
        )
        or "unknown"
    )
    event_type = str(event_type_value)

    event_time: datetime = now
    explicit_event_time = (
        data.get("payload", {}).get("ts_ms")
        if isinstance(data.get("payload"), dict)
        else None
    ) or data.get("event_time")
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
        ts_ms = data.get("ts_ms") or data.get("timestamp_ms") or data.get("ts")
        if isinstance(ts_ms, (int, float)):
            seconds = ts_ms
            if seconds > 10_000_000_000:
                seconds = seconds / 1000.0
            event_time = datetime.fromtimestamp(seconds, tz=timezone.utc)

    user_id_value = (
        data.get("user_id") or data.get("userId") or data.get("user")
    )
    user_id = str(user_id_value) if user_id_value is not None else None

    service_name_value = data.get("service_name") or data.get("service")
    if service_name_value is None:
        source = data.get("source")
        if not isinstance(source, dict):
            source = data.get("payload", {}).get("source")
        if isinstance(source, dict):
            service_name_value = source.get("name")
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
