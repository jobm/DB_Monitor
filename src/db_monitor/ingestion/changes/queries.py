"""Standalone query functions for column-level change data."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from db_monitor.core.models import ColumnChange, MonitoredColumn, MonitoredTable
from sqlalchemy import and_, func, or_, select


async def get_changes(
    session_factory: Callable,
    table_id: int,
    column_id: Optional[int] = None,
    row_identity: Optional[dict[str, Any]] = None,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
    cursor_id: int | None = None,
    cursor_time: datetime | None = None,
    include_total: bool = True,
) -> tuple[list[dict[str, Any]], int | None]:
    """Query column changes with filters and optional keyset pagination.

    Uses keyset ``(changed_at DESC, id DESC)`` when both
    ``cursor_time`` and ``cursor_id`` are provided.  Falls back to
    offset pagination otherwise.  Returns ``(changes, total_or_none)``.
    """
    async with session_factory() as session:
        stmt = (
            select(ColumnChange, MonitoredColumn)
            .join(
                MonitoredColumn,
                ColumnChange.column_id == MonitoredColumn.id,
            )
            .where(ColumnChange.table_id == table_id)
        )

        if column_id:
            stmt = stmt.where(ColumnChange.column_id == column_id)
        if row_identity is not None:
            stmt = stmt.where(ColumnChange.row_identity == row_identity)
        if from_time:
            stmt = stmt.where(ColumnChange.changed_at >= from_time)
        if to_time:
            stmt = stmt.where(ColumnChange.changed_at <= to_time)
        keyset = cursor_time is not None and cursor_id is not None
        if keyset:
            stmt = stmt.where(
                or_(
                    ColumnChange.changed_at < cursor_time,
                    and_(
                        ColumnChange.changed_at == cursor_time,
                        ColumnChange.id < cursor_id,
                    ),
                )
            )
        elif cursor_id is not None:
            stmt = stmt.where(ColumnChange.id < cursor_id)

        total: int | None = None
        if include_total and not keyset and cursor_id is None:
            count_stmt = select(func.count()).select_from(
                stmt.subquery()
            )
            total_result = await session.execute(count_stmt)
            total = int(total_result.scalar() or 0)

        effective_offset = 0 if keyset or cursor_id is not None else offset
        stmt = (
            stmt.order_by(
                ColumnChange.changed_at.desc(),
                ColumnChange.id.desc(),
            )
            .limit(limit)
            .offset(effective_offset)
        )

        result = await session.execute(stmt)
        rows = result.all()

        changes = [
            {
                "id": change.id,
                "event_id": change.event_id,
                "column_name": col.column_name,
                "operation": change.operation,
                "row_identity": change.row_identity,
                "old_value": change.old_value,
                "new_value": change.new_value,
                "changed_at": (
                    change.changed_at.isoformat()
                    if change.changed_at
                    else None
                ),
            }
            for change, col in rows
        ]
        return changes, total


async def get_value_at_time(
    session_factory: Callable,
    service_name: str,
    table_name: str,
    column_name: str,
    timestamp: datetime,
) -> Optional[Any]:
    """Get the value of a column at a specific point in time."""
    async with session_factory() as session:
        table_stmt = select(MonitoredTable).where(
            MonitoredTable.service_name == service_name,
            MonitoredTable.table_name == table_name,
            MonitoredTable.is_active.is_(True),
        )
        table_result = await session.execute(table_stmt)
        table = table_result.scalar_one_or_none()
        if not table:
            return None

        col_stmt = select(MonitoredColumn).where(
            MonitoredColumn.table_id == table.id,
            MonitoredColumn.column_name == column_name,
        )
        col_result = await session.execute(col_stmt)
        column = col_result.scalar_one_or_none()
        if not column:
            return None

        change_stmt = (
            select(ColumnChange)
            .where(
                ColumnChange.table_id == table.id,
                ColumnChange.column_id == column.id,
                ColumnChange.changed_at <= timestamp,
            )
            .order_by(ColumnChange.changed_at.desc())
            .limit(1)
        )
        change_result = await session.execute(change_stmt)
        change = change_result.scalar_one_or_none()

        if change:
            return change.new_value

        return None
