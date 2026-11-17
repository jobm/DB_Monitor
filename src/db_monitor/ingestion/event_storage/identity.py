"""Row identity population for events and event batches."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db_monitor.core.models import KafkaEvent, MonitoredColumn
from db_monitor.ingestion.row_identity import extract_row_identity


async def _populate_row_identity(
    session: AsyncSession, event_obj: KafkaEvent
) -> None:
    """Populate row identity on an event before it is inserted."""
    primary_key_columns: list[str] = []
    if event_obj.source_table_id is not None:
        result = await session.execute(
            select(MonitoredColumn.column_name).where(
                MonitoredColumn.table_id == event_obj.source_table_id,
                MonitoredColumn.is_primary_key.is_(True),
            )
        )
        primary_key_columns = [
            column_name
            for column_name in result.scalars().all()
            if column_name
        ]

    event_obj.row_identity = extract_row_identity(
        event_obj.event_data,
        primary_key_columns=primary_key_columns,
    )


async def _populate_row_identity_batch(
    session: AsyncSession,
    events: list[KafkaEvent],
) -> None:
    """Populate row identity for a batch of events in one round-trip.

    Fetches primary-key column names once per unique ``source_table_id``
    found in the batch, then applies ``extract_row_identity`` to each
    event in memory.
    """
    table_ids: set[int] = {
        e.source_table_id
        for e in events
        if e.source_table_id is not None
    }
    if not table_ids:
        return

    pk_result = await session.execute(
        select(
            MonitoredColumn.table_id,
            MonitoredColumn.column_name,
        ).where(
            MonitoredColumn.table_id.in_(table_ids),
            MonitoredColumn.is_primary_key.is_(True),
        )
    )
    pk_by_table: dict[int, list[str]] = {}
    for table_id, col_name in pk_result.fetchall():
        pk_by_table.setdefault(table_id, []).append(col_name)

    for event_obj in events:
        pk_cols = pk_by_table.get(
            event_obj.source_table_id, []
        )
        event_obj.row_identity = extract_row_identity(
            event_obj.event_data,
            primary_key_columns=pk_cols,
        )
