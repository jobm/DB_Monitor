"""Bulk schema discovery for batches of events."""

from __future__ import annotations

from typing import Any, Optional

from db_monitor.core.models import KafkaEvent, MonitoredColumn, MonitoredTable
from db_monitor.ingestion.source_metadata import extract_source_coordinates
from db_monitor.schema.columns import extract_column_definitions
from sqlalchemy.dialects.postgresql import insert as pg_insert


async def process_events_batch(
    events: list[KafkaEvent],
    session,
) -> tuple[dict[int, Optional[int]], list[tuple[str, str, str]]]:
    """Discover schema for a batch of events in a single round-trip.

    Collects unique (service, db, table) combinations across all events,
    upserts tables once, then upserts columns per table once. Returns a
    mapping of ``id(event) -> table_id`` and the list of (service, db, table)
    keys that were processed (for cache invalidation).
    """
    result: dict[int, Optional[int]] = {}

    if not events:
        return result, []

    # ---- Phase 1: collect unique (service, db, table) entries ----
    table_keys: dict[tuple[str, str, str], dict[str, object]] = {}
    event_table_keys: list[tuple[int, tuple[str, str, str] | None]] = []

    for event in events:
        if not event.event_data:
            result[id(event)] = None
            event_table_keys.append((id(event), None))
            continue

        payload = event.event_data
        source_coordinates = extract_source_coordinates(payload)
        if source_coordinates is None:
            result[id(event)] = None
            event_table_keys.append((id(event), None))
            continue

        table_identifier = source_coordinates.table_name
        if not table_identifier:
            result[id(event)] = None
            event_table_keys.append((id(event), None))
            continue

        db_name = source_coordinates.database_name or "unknown"
        service_name = source_coordinates.service_name or "unknown"
        topic_parts = (event.kafka_topic or "").split(".")
        if topic_parts and topic_parts[0]:
            service_name = topic_parts[0]
        topic_name = event.kafka_topic or ""
        key = (service_name, db_name, table_identifier)
        table_keys[key] = {
            "service_name": service_name,
            "database_name": db_name,
            "table_name": table_identifier,
            "topic_name": topic_name,
        }
        event_table_keys.append((id(event), key))

    if not table_keys:
        return result, []

    # ---- Phase 2: bulk upsert all unique tables ----
    table_values = list(table_keys.values())
    insert_stmt = (
        pg_insert(MonitoredTable)
        .values(table_values)
        .on_conflict_do_update(
            index_elements=[
                "service_name",
                "database_name",
                "table_name",
            ],
            set_={
                "topic_name": (
                    pg_insert(MonitoredTable)
                    .excluded.topic_name
                ),
                "is_active": True,
            },
        )
        .returning(
            MonitoredTable.id,
            MonitoredTable.service_name,
            MonitoredTable.database_name,
            MonitoredTable.table_name,
        )
    )
    table_result = await session.execute(insert_stmt)
    table_id_map: dict[tuple[str, str, str], int] = {
        (row.service_name, row.database_name, row.table_name): row.id
        for row in table_result.fetchall()
    }

    # ---- Phase 3: collect column definitions per table ----
    table_columns: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    event_by_id: dict[int, KafkaEvent] = {id(e): e for e in events}

    for event_id, table_key in event_table_keys:
        if table_key is None or table_key not in table_id_map:
            result[event_id] = None
            continue
        result[event_id] = table_id_map[table_key]
        discovered_event = event_by_id.get(event_id)
        if discovered_event is None or not discovered_event.event_data:
            continue
        column_defs = extract_column_definitions(discovered_event.event_data)
        if column_defs:
            existing = table_columns.setdefault(table_key, [])
            seen = {c["column_name"] for c in existing}
            for col_def in column_defs:
                if col_def["column_name"] not in seen:
                    existing.append(col_def)
                    seen.add(col_def["column_name"])

    # ---- Phase 4: bulk upsert columns per table ----
    for key, column_defs in table_columns.items():
        table_id = table_id_map[key]
        if column_defs:
            insert_col_stmt = pg_insert(MonitoredColumn).values(  # type: ignore[attr-defined]
                [
                    {**col, "table_id": table_id}
                    for col in column_defs
                ]
            )
            col_stmt = insert_col_stmt.on_conflict_do_update(
                index_elements=["table_id", "column_name"],
                set_={
                    "data_type": (
                        insert_col_stmt.excluded.data_type  # type: ignore[attr-defined]
                    ),
                    "is_primary_key": (
                        insert_col_stmt.excluded.is_primary_key  # type: ignore[attr-defined]
                    ),
                    "is_nullable": (
                        insert_col_stmt.excluded.is_nullable  # type: ignore[attr-defined]
                    ),
                },
            )
            await session.execute(col_stmt)

    return result, list(table_keys.keys())
