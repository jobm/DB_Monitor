from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from rich.text import Text


def extract_event_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Return the Debezium payload envelope for an event."""
    payload = event.get("event_data") or {}
    nested_payload = payload.get("payload")
    if isinstance(nested_payload, dict):
        return nested_payload
    return payload if isinstance(payload, dict) else {}


def extract_event_source(event: dict[str, Any]) -> dict[str, Any]:
    """Return the event source block when present."""
    payload = extract_event_payload(event)
    source = payload.get("source")
    return source if isinstance(source, dict) else {}


def event_service_and_table(
    event: dict[str, Any],
    table_names_by_id: Optional[dict[int, str]] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Return the service and table names associated with an event."""
    table_id = event.get("source_table_id")
    if (
        isinstance(table_id, int)
        and table_names_by_id
        and table_id in table_names_by_id
    ):
        service_name, _, table_name = table_names_by_id[table_id].partition(
            "."
        )
        return service_name or None, table_name or None

    source = extract_event_source(event)
    service_name = source.get("name") or event.get("service_name")
    table_name = source.get("table")
    return (
        str(service_name) if service_name is not None else None,
        str(table_name) if table_name is not None else None,
    )


def _decode_decimal(value: dict[str, Any]) -> Optional[str]:
    """Decode Debezium variable-scale decimals into readable strings."""
    if set(value.keys()) != {"scale", "value"}:
        return None

    scale = value.get("scale")
    encoded = value.get("value")
    if not isinstance(scale, int) or not isinstance(encoded, str):
        return None

    try:
        raw_bytes = base64.b64decode(encoded)
    except Exception:
        return None

    if not raw_bytes:
        return "0"

    integer = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    normalized = Decimal(integer).scaleb(-scale)
    return format(normalized, "f")


def _normalize_timestamp(
    value: int,
    field_name: Optional[str],
) -> Optional[str]:
    """Best-effort normalization for Debezium timestamp-like integers."""
    if not field_name:
        return None

    lowered_name = field_name.lower()
    if not any(token in lowered_name for token in ("time", "date", "_at")):
        return None

    try:
        if value >= 100_000_000_000_000:
            timestamp = datetime.fromtimestamp(
                value / 1_000_000,
                tz=timezone.utc,
            )
        elif value >= 10_000_000_000:
            timestamp = datetime.fromtimestamp(
                value / 1_000,
                tz=timezone.utc,
            )
        else:
            timestamp = datetime.fromtimestamp(value, tz=timezone.utc)
    except Exception:
        return None

    return timestamp.isoformat()


def normalize_value(value: Any, field_name: Optional[str] = None) -> Any:
    """Normalize Debezium transport values into display-friendly values."""
    if isinstance(value, dict):
        decoded_decimal = _decode_decimal(value)
        if decoded_decimal is not None:
            return decoded_decimal

        return {
            key: normalize_value(nested_value, field_name=key)
            for key, nested_value in value.items()
        }

    if isinstance(value, list):
        return [normalize_value(item, field_name=field_name) for item in value]

    if isinstance(value, int):
        normalized_timestamp = _normalize_timestamp(value, field_name)
        if normalized_timestamp is not None:
            return normalized_timestamp

    return value


def format_value(value: Any, field_name: Optional[str] = None) -> str:
    """Render a normalized value for compact UI display."""
    normalized = normalize_value(value, field_name=field_name)
    if normalized is None:
        return "null"
    if isinstance(normalized, (dict, list)):
        return json.dumps(normalized, sort_keys=True)
    return str(normalized)


def truncate_text(value: str, max_length: int = 64) -> str:
    """Trim long values for dense table views without losing context."""
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "..."


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp string into an aware datetime."""
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def format_relative_time(value: Optional[str]) -> str:
    """Render a compact relative-time label for a timestamp."""
    parsed = _parse_timestamp(value)
    if parsed is None:
        return "unknown"

    now = datetime.now(timezone.utc)
    parsed_utc = parsed.astimezone(timezone.utc)
    delta_seconds = int((now - parsed_utc).total_seconds())
    future = delta_seconds < 0
    total_seconds = abs(delta_seconds)

    if total_seconds < 60:
        label = f"{total_seconds}s"
    elif total_seconds < 3600:
        label = f"{total_seconds // 60}m"
    elif total_seconds < 86_400:
        label = f"{total_seconds // 3600}h"
    else:
        label = f"{total_seconds // 86_400}d"

    if future:
        return f"in {label}"
    return f"{label} ago"


def format_time_label(
    value: Optional[str],
    compact: bool = False,
) -> str:
    """Combine absolute and relative time for faster visual scanning."""
    parsed = _parse_timestamp(value)
    if parsed is None:
        return str(value or "unknown")

    parsed_utc = parsed.astimezone(timezone.utc)
    absolute = parsed_utc.strftime("%H:%M:%S")
    if not compact:
        absolute = parsed_utc.isoformat()

    relative = format_relative_time(value)
    return f"{absolute} ({relative})"


def event_change_pairs(
    event: dict[str, Any],
) -> list[tuple[str, str, str]]:
    """Return changed field triples as field, old, new strings."""
    payload = extract_event_payload(event)
    before_value = payload.get("before")
    after_value = payload.get("after")
    before = before_value if isinstance(before_value, dict) else {}
    after = after_value if isinstance(after_value, dict) else {}

    changes: list[tuple[str, str, str]] = []
    for field_name in sorted(set(before) | set(after)):
        old_value = before.get(field_name)
        new_value = after.get(field_name)
        if old_value == new_value:
            continue
        changes.append(
            (
                field_name,
                format_value(old_value, field_name=field_name),
                format_value(new_value, field_name=field_name),
            )
        )
    return changes


def summarize_event_changes(event: dict[str, Any]) -> str:
    """Return a compact event-summary string suitable for table views."""
    changes = event_change_pairs(event)
    if not changes:
        operation = str(event.get("operation") or "UPDATE")
        return f"{operation.title()} event"

    if len(changes) == 1:
        field_name, old_value, new_value = changes[0]
        return f"{field_name}: {old_value} -> {new_value}"

    if len(changes) <= 3:
        changed_fields = ", ".join(field_name for field_name, _, _ in changes)
        return f"{changed_fields} changed"

    return f"{len(changes)} fields changed"


def format_operation_label(operation: Optional[str]) -> str:
    """Return a compact, visually distinct operation label."""
    labels = {
        "INSERT": "+ INSERT",
        "UPDATE": "~ UPDATE",
        "DELETE": "- DELETE",
    }
    return labels.get(
        str(operation or "UPDATE").upper(),
        str(operation or "UPDATE"),
    )


def operation_badge(operation: Optional[str]) -> Text:
    """Return a colorized operation badge for table-centric views."""
    normalized = str(operation or "UPDATE").upper()
    if normalized == "INSERT":
        return Text("+ INSERT", style="bold green")
    if normalized == "DELETE":
        return Text("- DELETE", style="bold red")
    if normalized == "UPDATE":
        return Text("~ UPDATE", style="bold yellow")
    return Text(normalized, style="bold")


def table_display_name(
    event: dict[str, Any],
    table_names_by_id: Optional[dict[int, str]] = None,
) -> str:
    """Return a readable service.table label for an event."""
    service_name, table_name = event_service_and_table(
        event,
        table_names_by_id=table_names_by_id,
    )
    if service_name and table_name:
        return f"{service_name}.{table_name}"
    if table_name:
        return table_name
    if service_name:
        return service_name
    return "unknown"


def record_identity(event: dict[str, Any]) -> str:
    """Return a business-friendly record label for an event."""
    payload = extract_event_payload(event)
    after_value = payload.get("after")
    before_value = payload.get("before")
    current_row = after_value if isinstance(after_value, dict) else None
    previous_row = (
        before_value if isinstance(before_value, dict) else None
    )
    candidate = current_row or previous_row or {}
    stored_row_identity = event.get("row_identity")
    if isinstance(stored_row_identity, dict) and stored_row_identity:
        candidate = {**candidate, **stored_row_identity}

    if not isinstance(candidate, dict) or not candidate:
        return "unknown"

    preferred_keys = [
        "email",
        "username",
        "name",
        "full_name",
        "display_name",
        "order_number",
        "customer_number",
        "tracking_number",
        "shipment_number",
        "sku",
        "product_name",
        "category_name",
        "title",
        "code",
    ]
    selected_keys: list[str] = []
    for key in preferred_keys:
        if key in candidate and candidate.get(key) not in (None, ""):
            selected_keys.append(key)
            break

    ordered_keys: list[str] = []
    if "id" in candidate:
        ordered_keys.append("id")
    ordered_keys.extend(
        sorted(
            key
            for key in candidate
            if key not in ordered_keys and key.endswith("_id")
        )
    )
    ordered_keys.extend(
        sorted(
            key
            for key in candidate
            if key not in ordered_keys and "id" in key.lower()
        )
    )

    if not ordered_keys:
        ordered_keys.extend(
            key
            for key in candidate
            if candidate.get(key) is not None
        )

    if not ordered_keys:
        return "unknown"

    for key in ordered_keys:
        if key not in selected_keys:
            selected_keys.append(key)
        if len(selected_keys) >= 2:
            break

    return ", ".join(
        f"{key}={format_value(candidate.get(key), field_name=key)}"
        for key in selected_keys
    )
