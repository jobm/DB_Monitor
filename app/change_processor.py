"""Change processor - extracts column-level changes from Debezium events."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from core.models import (
    ColumnChange,
    KafkaEvent,
    MonitoredColumn,
    MonitoredTable,
)
from sqlalchemy import and_, func, or_, select

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

    async def process_events_batch(
        self,
        events: list[KafkaEvent],
        session,
    ) -> None:
        """Extract column changes for a batch of events in one round-trip.

        Fetches audit-enabled columns once per unique table_id, computes all
        per-event column deltas in memory, and bulk-inserts the resulting
        ``ColumnChange`` rows in a single ``session.add_all()`` call.
        """
        if not events:
            return

        # Collect events that are eligible for change extraction
        eligible: list[KafkaEvent] = []
        table_ids: set[int] = set()
        for event in events:
            if (
                event.id is not None
                and event.source_table_id is not None
                and event.event_data
            ):
                eligible.append(event)
                table_ids.add(event.source_table_id)

        if not eligible:
            return

        # Fetch audit-enabled columns once per unique table
        col_stmt = select(MonitoredColumn).where(
            MonitoredColumn.table_id.in_(table_ids),
            MonitoredColumn.audit_enabled.is_(True),
        )
        col_result = await session.execute(col_stmt)
        all_columns = col_result.scalars().all()

        columns_by_table: dict[int, list[MonitoredColumn]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # Compute all changes in memory and bulk-insert
        all_changes: list[ColumnChange] = []
        for event in eligible:
            payload = event.event_data or {}
            operation = event.operation
            table_id = event.source_table_id
            if table_id is None:
                continue

            old_data: dict[str, Any] | None = None
            new_data: dict[str, Any] | None = None

            if operation == "INSERT":
                new_data = payload.get("after", {})
            elif operation == "UPDATE":
                old_data = payload.get("before", {})
                new_data = payload.get("after", {})
            elif operation == "DELETE":
                old_data = payload.get("before", {})

            if new_data is None and old_data is None:
                continue

            columns = columns_by_table.get(table_id, [])
            for col in columns:
                old_val = (
                    old_data.get(col.column_name) if old_data else None
                )
                new_val = (
                    new_data.get(col.column_name) if new_data else None
                )
                if old_val == new_val:
                    continue
                all_changes.append(
                    ColumnChange(
                        event_id=event.id,
                        table_id=table_id,
                        column_id=col.id,
                        operation=event.operation,
                        row_identity=event.row_identity,
                        old_value=old_val,
                        new_value=new_val,
                        changed_at=event.event_time,
                    )
                )

        if all_changes:
            session.add_all(all_changes)

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
        cursor_id: int | None = None,
        cursor_time: datetime | None = None,
        include_total: bool = True,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Query column changes with filters and optional keyset pagination.

        Uses keyset ``(changed_at DESC, id DESC)`` when both
        ``cursor_time`` and ``cursor_id`` are provided.  Falls back to
        offset pagination otherwise.  Returns ``(changes, total_or_none)``.
        """
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
                # Backward-compat: id-only cursor
                stmt = stmt.where(ColumnChange.id < cursor_id)

            total: int | None = None
            if include_total and not keyset:
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
