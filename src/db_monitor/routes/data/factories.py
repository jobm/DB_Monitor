"""Factory functions and helpers for data route endpoints."""

from __future__ import annotations

from fastapi import HTTPException

from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.changes import ChangeProcessor
from db_monitor.repositories.events import EventsRepository
from db_monitor.schema import SchemaDiscovery


def get_schema_discovery() -> SchemaDiscovery:
    """Return a schema discovery service bound to the app DB session."""
    return SchemaDiscovery(AsyncSessionLocal)


def get_change_processor() -> ChangeProcessor:
    """Return a change processor service bound to the app DB session."""
    return ChangeProcessor(AsyncSessionLocal)


def get_events_repository() -> EventsRepository:
    """Return a read-model repository for events queries."""
    return EventsRepository(AsyncSessionLocal)


def resolve_table_reference(
    table_name: str,
    service_name: str | None = None,
) -> tuple[str, str]:
    """Resolve either explicit or service-qualified table references."""
    normalized_table_name = table_name.strip()
    if service_name:
        return service_name, normalized_table_name

    if "." not in normalized_table_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Table reference is ambiguous. Pass service_name explicitly, "
                "for example service_name=orderdb&table_name=orders, or use "
                "table_name=orderdb.orders."
            ),
        )

    resolved_service_name, resolved_table_name = normalized_table_name.split(
        ".",
        1,
    )
    if not resolved_service_name or not resolved_table_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid table reference. Expected '<service>.<table>', "
                "such as 'orderdb.orders'."
            ),
        )

    return resolved_service_name, resolved_table_name
