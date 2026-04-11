"""Change processor - extracts column-level changes from Debezium events."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import ColumnChange, KafkaEvent, MonitoredColumn, MonitoredTable

logger = logging.getLogger(__name__)


class ChangeProcessor:
    """Processes events to extract and store column-level changes."""

    def __init__(self, session_factory: Callable):
        self.session_factory = session_factory

    async def process_event(self, event: KafkaEvent) -> None:
        """Process an event and extract column changes."""
        if not event.event_data or not event.source_table_id:
            return

        payload = event.event_data
        operation = event.operation

        if operation == "INSERT":
            await self._process_insert(event, payload)
        elif operation == "UPDATE":
            await self._process_update(event, payload)
        elif operation == "DELETE":
            await self._process_delete(event, payload)

    async def _process_insert(self, event: KafkaEvent, payload: dict[str, Any]) -> None:
        """Process INSERT - new values in 'after' field."""
        after = payload.get("after", {})
        if not after:
            return

        await self._save_changes(event, None, after)

    async def _process_update(self, event: KafkaEvent, payload: dict[str, Any]) -> None:
        """Process UPDATE - old values in 'before', new in 'after'."""
        before = payload.get("before", {})
        after = payload.get("after", {})

        if not after:
            return

        await self._save_changes(event, before, after)

    async def _process_delete(self, event: KafkaEvent, payload: dict[str, Any]) -> None:
        """Process DELETE - old values in 'before'."""
        before = payload.get("before", {})
        if not before:
            return

        await self._save_changes(event, before, None)

    async def _save_changes(
        self,
        event: KafkaEvent,
        old_data: Optional[dict[str, Any]],
        new_data: Optional[dict[str, Any]],
    ) -> None:
        """Save column changes to the database."""
        async with self.session_factory() as session:
            col_stmt = select(MonitoredColumn).where(
                MonitoredColumn.table_id == event.source_table_id,
                MonitoredColumn.audit_enabled == True,
            )
            result = await session.execute(col_stmt)
            columns = result.scalars().all()

            changes_to_insert = []
            for col in columns:
                old_val = old_data.get(col.column_name) if old_data else None
                new_val = new_data.get(col.column_name) if new_data else None

                if old_val != new_val:
                    changes_to_insert.append({
                        "event_id": event.id,
                        "table_id": event.source_table_id,
                        "column_id": col.id,
                        "operation": event.operation,
                        "old_value": old_val,
                        "new_value": new_val,
                        "changed_at": event.event_time,
                    })

            if changes_to_insert:
                session.add_all([ColumnChange(**c) for c in changes_to_insert])
                await session.commit()

    async def get_changes(
        self,
        table_id: int,
        column_id: Optional[int] = None,
        from_time: Optional[Any] = None,
        to_time: Optional[Any] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query column changes with filters."""
        async with self.session_factory() as session:
            stmt = (
                select(ColumnChange, MonitoredColumn)
                .join(MonitoredColumn, ColumnChange.column_id == MonitoredColumn.id)
                .where(ColumnChange.table_id == table_id)
            )

            if column_id:
                stmt = stmt.where(ColumnChange.column_id == column_id)
            if from_time:
                stmt = stmt.where(ColumnChange.changed_at >= from_time)
            if to_time:
                stmt = stmt.where(ColumnChange.changed_at <= to_time)

            stmt = stmt.order_by(ColumnChange.changed_at.desc()).limit(limit)

            result = await session.execute(stmt)
            rows = result.all()

            return [
                {
                    "id": change.id,
                    "column_name": col.column_name,
                    "operation": change.operation,
                    "old_value": change.old_value,
                    "new_value": change.new_value,
                    "changed_at": change.changed_at.isoformat() if change.changed_at else None,
                }
                for change, col in rows
            ]

    async def get_value_at_time(
        self,
        table_name: str,
        column_name: str,
        timestamp: Any,
    ) -> Optional[Any]:
        """Get the value of a column at a specific point in time."""
        async with self.session_factory() as session:
            table_stmt = select(MonitoredTable).where(MonitoredTable.table_name == table_name)
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
