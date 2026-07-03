"""Row identity population for events and event batches."""

from __future__ import annotations

from sqlalchemy import select

from db_monitor.core.models import MonitoredColumn
from db_monitor.ingestion.row_identity import extract_row_identity


async def _populate_row_identity(
    session: object, event_obj: object
) -> None:
    """Populate row identity on an event before it is inserted."""
    primary_key_columns: list[str] = []
    if event_obj.source_table_id is not None:  # type: ignore[union-attr]
        result = await session.execute(  # type: ignore[union-attr]
            select(MonitoredColumn.column_name).where(
                MonitoredColumn.table_id == event_obj.source_table_id,  # type: ignore[union-attr]
                MonitoredColumn.is_primary_key.is_(True),
            )
        )
        primary_key_columns = [
            column_name
            for column_name in result.scalars().all()
            if column_name
        ]

    event_obj.row_identity = extract_row_identity(  # type: ignore[union-attr]
        event_obj.event_data,  # type: ignore[union-attr]
        primary_key_columns=primary_key_columns,
    )


async def _populate_row_identity_batch(
    session: object,
    events: list[object],
) -> None:
    """Populate row identity for a batch of events in one round-trip.

    Fetches primary-key column names once per unique ``source_table_id``
    found in the batch, then applies ``extract_row_identity`` to each
    event in memory.
    """
    table_ids: set[int] = {
        e.source_table_id  # type: ignore[union-attr]
        for e in events
        if e.source_table_id is not None  # type: ignore[union-attr]
    }
    if not table_ids:
        return

    pk_result = await session.execute(  # type: ignore[union-attr]
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
            event_obj.source_table_id, []  # type: ignore[union-attr]
        )
        event_obj.row_identity = extract_row_identity(  # type: ignore[union-attr]
            event_obj.event_data,  # type: ignore[union-attr]
            primary_key_columns=pk_cols,
        )
