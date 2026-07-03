"""Change processor - extracts column-level changes from Debezium events."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from db_monitor.core.models import (
    ColumnChange,
    KafkaEvent,
    MonitoredColumn,
)
from db_monitor.ingestion.changes.queries import (
    get_changes as _get_changes_impl,
    get_value_at_time as _get_value_at_time_impl,
)
from sqlalchemy import select

logger = logging.getLogger(__name__)

__all__ = ["ChangeProcessor"]


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

    # ── Query delegation methods ─────────────────────────────────────

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
        """Query column changes with filters and optional keyset pagination."""
        return await _get_changes_impl(
            session_factory=self.session_factory,
            table_id=table_id,
            column_id=column_id,
            row_identity=row_identity,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
            offset=offset,
            cursor_id=cursor_id,
            cursor_time=cursor_time,
            include_total=include_total,
        )

    async def get_value_at_time(
        self,
        service_name: str,
        table_name: str,
        column_name: str,
        timestamp: datetime,
    ) -> Optional[Any]:
        """Get the value of a column at a specific point in time."""
        return await _get_value_at_time_impl(
            session_factory=self.session_factory,
            service_name=service_name,
            table_name=table_name,
            column_name=column_name,
            timestamp=timestamp,
        )
