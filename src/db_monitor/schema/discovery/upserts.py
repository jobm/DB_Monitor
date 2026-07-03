"""Standalone upsert operations for schema discovery — tables and columns."""

from __future__ import annotations

from typing import Any

from db_monitor.core.models import MonitoredColumn, MonitoredTable
from sqlalchemy.dialects.postgresql import insert as pg_insert


async def upsert_table(
    session,
    service_name: str,
    database_name: str,
    table_name: str,
    topic_name: str,
) -> int:
    """Upsert a monitored table and return its ID."""
    stmt = (
        pg_insert(MonitoredTable)
        .values(
            service_name=service_name,
            database_name=database_name,
            table_name=table_name,
            topic_name=topic_name,
            is_active=True,
        )
        .on_conflict_do_update(
            index_elements=["service_name", "database_name", "table_name"],
            set_={
                "topic_name": topic_name,
                "is_active": True,
            },
        )
        .returning(MonitoredTable.id)
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def upsert_columns(
    session,
    table_id: int,
    column_definitions: list[dict[str, Any]],
) -> None:
    """Upsert columns from Debezium schema or inferred row payloads."""
    if not column_definitions:
        return

    insert_stmt = pg_insert(MonitoredColumn).values(
        [
            {**column, "table_id": table_id}
            for column in column_definitions
        ]
    )
    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["table_id", "column_name"],
        set_={
            "data_type": insert_stmt.excluded.data_type,
            "is_primary_key": insert_stmt.excluded.is_primary_key,
            "is_nullable": insert_stmt.excluded.is_nullable,
        },
    )
    await session.execute(stmt)
