"""PostgreSQL COPY-based bulk insertion for derived column changes."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db_monitor.core.models import ColumnChange
from db_monitor.ingestion.pg_copy import asyncpg_connection, jsonb

_STAGING_TABLE = "column_changes_batch_staging"

_CHANGE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "table_id",
    "column_id",
    "operation",
    "row_identity",
    "old_value",
    "new_value",
    "changed_at",
)

_CREATE_STAGING_SQL = f"""
CREATE TEMP TABLE {_STAGING_TABLE} (
    event_id integer,
    table_id integer,
    column_id integer,
    operation text,
    row_identity jsonb,
    old_value jsonb,
    new_value jsonb,
    changed_at timestamptz
) ON COMMIT DROP
"""

_INSERT_FROM_STAGING_SQL = f"""
INSERT INTO column_changes ({", ".join(_CHANGE_COLUMNS)})
SELECT {", ".join(_CHANGE_COLUMNS)}
FROM {_STAGING_TABLE}
"""


def _change_record(change: ColumnChange) -> tuple[Any, ...]:
    """Flatten one change row into staging-column order."""
    return (
        change.event_id,
        change.table_id,
        change.column_id,
        change.operation,
        jsonb(change.row_identity),
        jsonb(change.old_value),
        jsonb(change.new_value),
        change.changed_at,
    )


async def copy_column_changes(
    session: AsyncSession,
    changes: list[ColumnChange],
) -> None:
    """COPY derived ``column_changes`` rows and insert them in one statement."""
    if not changes:
        return

    records = [_change_record(change) for change in changes]
    await session.execute(text(_CREATE_STAGING_SQL))
    driver_connection = await asyncpg_connection(session)
    await driver_connection.copy_records_to_table(
        _STAGING_TABLE,
        records=records,
        columns=list(_CHANGE_COLUMNS),
    )
    await session.execute(text(_INSERT_FROM_STAGING_SQL))
