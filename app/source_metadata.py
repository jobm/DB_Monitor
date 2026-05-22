"""Helpers for normalizing Debezium source metadata across databases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceCoordinates:
    """Normalized source coordinates for a CDC payload."""

    service_name: str | None
    database_name: str | None
    namespace_name: str | None
    raw_table_name: str | None

    @property
    def table_name(self) -> str | None:
        """Return the stored table identifier including namespace when set."""
        if self.raw_table_name is None:
            return None
        if self.namespace_name:
            return f"{self.namespace_name}.{self.raw_table_name}"
        return self.raw_table_name

    def table_filter_names(self) -> list[str]:
        """Return candidate table filter names for config matching."""
        candidates: list[str] = []
        for candidate in (
            self.raw_table_name,
            self.table_name,
        ):
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        if self.database_name:
            for candidate in (
                self.raw_table_name,
                self.table_name,
            ):
                if not candidate:
                    continue
                qualified = f"{self.database_name}.{candidate}"
                if qualified not in candidates:
                    candidates.append(qualified)

        return candidates


def extract_source_record(event_data: dict[str, Any] | None) -> dict[str, Any]:
    """Return the Debezium source envelope from an event payload."""
    if not isinstance(event_data, dict):
        return {}

    source = event_data.get("source")
    if isinstance(source, dict):
        return source

    payload = event_data.get("payload")
    if isinstance(payload, dict):
        nested_source = payload.get("source")
        if isinstance(nested_source, dict):
            return nested_source

    return {}


def extract_source_coordinates(
    event_data: dict[str, Any] | None,
) -> SourceCoordinates | None:
    """Normalize service, database, namespace, and table coordinates."""
    source = extract_source_record(event_data)
    if not source:
        return None

    service_name = _first_text_value(source, "name", "server")
    database_name = _first_text_value(
        source,
        "db",
        "database",
        "catalog",
    )
    namespace_name = _first_text_value(source, "schema", "schema_name")
    raw_table_name = _first_text_value(source, "table", "collection")

    if raw_table_name is None:
        return None

    return SourceCoordinates(
        service_name=service_name,
        database_name=database_name,
        namespace_name=namespace_name,
        raw_table_name=raw_table_name,
    )


def extract_source_service_name(
    event_data: dict[str, Any] | None,
) -> str | None:
    """Return the connector or server name from the source envelope."""
    source = extract_source_record(event_data)
    if not source:
        return None
    return _first_text_value(source, "name", "server")


def _first_text_value(
    source: dict[str, Any],
    *keys: str,
) -> str | None:
    """Return the first non-empty text value from the source mapping."""
    for key in keys:
        value = source.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None
