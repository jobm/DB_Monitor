"""Change processor - extracts column-level changes from Debezium events."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from models import ColumnChange, KafkaEvent, MonitoredColumn, MonitoredTable
from sqlalchemy import select

logger = logging.getLogger(__name__)


class ChangeProcessor:
    """Processes events to extract and store column-level changes."""

    def __init__(self, session_factory: Callable):
        self.session_factory = session_factory

    async def process_event(self, event: KafkaEvent, session=None) -> None:
        """Process an event and extract column changes."""
        if session is None:
            async with self.session_factory() as owned_session:
                async with owned_session.begin():
                    await self._process_event(event, owned_session)
            return

        await self._process_event(event, session)

    async def _process_event(self, event: KafkaEvent, session) -> None:
        """Process an event within an existing database session."""
        if not event.event_data or not event.source_table_id:
            return

        payload = event.event_data
        operation = event.operation

        if operation == "INSERT":
            await self._process_insert(session, event, payload)
        elif operation == "UPDATE":
            await self._process_update(session, event, payload)
        elif operation == "DELETE":
            await self._process_delete(session, event, payload)

    async def _process_insert(
        self,
        session,
        event: KafkaEvent,
        payload: dict[str, Any],
    ) -> None:
        """Process INSERT - new values in 'after' field."""
        after = payload.get("after", {})
        if not after:
            return

        await self._save_changes(session, event, None, after)

    async def _process_update(
        self,
        session,
        event: KafkaEvent,
        payload: dict[str, Any],
    ) -> None:
        """Process UPDATE - old values in 'before', new in 'after'."""
        before = payload.get("before", {})
        after = payload.get("after", {})

        if not after:
            return

        await self._save_changes(session, event, before, after)

    async def _process_delete(
        self,
        session,
        event: KafkaEvent,
        payload: dict[str, Any],
    ) -> None:
        """Process DELETE - old values in 'before'."""
        before = payload.get("before", {})
        if not before:
            return

        await self._save_changes(session, event, before, None)

    async def _save_changes(
        self,
        session,
        event: KafkaEvent,
        old_data: Optional[dict[str, Any]],
        new_data: Optional[dict[str, Any]],
    ) -> None:
        """Save column changes to the database."""
        col_stmt = select(MonitoredColumn).where(
            MonitoredColumn.table_id == event.source_table_id,
            MonitoredColumn.audit_enabled.is_(True),
        )
        result = await session.execute(col_stmt)
        columns = result.scalars().all()

        changes_to_insert = []
        for col in columns:
            old_val = old_data.get(col.column_name) if old_data else None
            new_val = new_data.get(col.column_name) if new_data else None

            if old_val != new_val:
                changes_to_insert.append(
                    {
                        "event_id": event.id,
                        "table_id": event.source_table_id,
                        "column_id": col.id,
                        "operation": event.operation,
                        "row_identity": event.row_identity,
                        "old_value": old_val,
                        "new_value": new_val,
                        "changed_at": event.event_time,
                    }
                )

        if changes_to_insert:
            session.add_all([ColumnChange(**c) for c in changes_to_insert])

    async def get_changes(
        self,
        table_id: int,
        column_id: Optional[int] = None,
        row_identity: Optional[dict[str, Any]] = None,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Query column changes with filters."""
        async with self.session_factory() as session:
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

            stmt = (
                stmt.order_by(ColumnChange.changed_at.desc())
                .limit(limit)
                .offset(offset)
            )

            result = await session.execute(stmt)
            rows = result.all()

            return [
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

    async def get_value_at_time(
        self,
        service_name: str,
        table_name: str,
        column_name: str,
        timestamp: datetime,
    ) -> Optional[Any]:
        """Get the value of a column at a specific point in time."""
        async with self.session_factory() as session:
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
