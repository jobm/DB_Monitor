"""Repository helpers for Kafka event read queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, or_, select

from core.models import KafkaEvent


@dataclass(slots=True)
class EventQueryFilters:
    """Supported filters for event listing queries."""

    service_name: str | None = None
    source_table_id: int | None = None
    row_identity: dict[str, Any] | None = None
    event_type: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    search_term: str | None = None


class EventsRepository:
    """Read-model repository for event endpoints."""

    def __init__(self, session_factory):
        """Initialize with a SQLAlchemy async session factory."""
        self._session_factory = session_factory

    async def list_events(
        self,
        *,
        filters: EventQueryFilters,
        limit: int,
        offset: int,
        cursor_id: int | None = None,
        cursor_time: datetime | None = None,
        include_total: bool = True,
    ) -> tuple[list[KafkaEvent], int | None]:
        """Return paginated events for filters.

        Uses keyset pagination ``(capture_time DESC, id DESC)`` when
        both ``cursor_time`` and ``cursor_id`` are provided.  Falls
        back to offset pagination otherwise.
        """
        async with self._session_factory() as session:
            query = select(KafkaEvent).order_by(
                KafkaEvent.capture_time.desc(),
                KafkaEvent.id.desc(),
            )
            query = self._apply_filters(query, filters)

            keyset = cursor_time is not None and cursor_id is not None
            if keyset:
                query = query.where(
                    or_(
                        KafkaEvent.capture_time < cursor_time,
                        and_(
                            KafkaEvent.capture_time == cursor_time,
                            KafkaEvent.id < cursor_id,
                        ),
                    )
                )
            elif cursor_id is not None:
                # Backward-compat: id-only cursor
                query = query.where(KafkaEvent.id < cursor_id)

            total: int | None = None
            if include_total and not keyset and cursor_id is None:
                count_query = select(func.count()).select_from(
                    query.subquery()
                )
                total_result = await session.execute(count_query)
                total = int(total_result.scalar() or 0)

            effective_offset = (
                0 if (keyset or cursor_id is not None) else offset
            )
            result = await session.execute(
                query.limit(limit).offset(effective_offset)
            )
            events = result.scalars().all()
            return events, total

    async def event_stats(self) -> dict[str, object]:
        """Return aggregate statistics for monitored events."""
        async with self._session_factory() as session:
            by_service = await session.execute(
                select(
                    KafkaEvent.service_name,
                    func.count(KafkaEvent.id),
                ).group_by(KafkaEvent.service_name)
            )
            by_type = await session.execute(
                select(
                    KafkaEvent.event_type,
                    func.count(KafkaEvent.id),
                ).group_by(KafkaEvent.event_type)
            )
            by_operation = await session.execute(
                select(
                    KafkaEvent.operation,
                    func.count(KafkaEvent.id),
                ).group_by(KafkaEvent.operation)
            )
            total = await session.execute(select(func.count(KafkaEvent.id)))

        return {
            "total_events": total.scalar(),
            "by_service": {
                row[0] or "unknown": row[1]
                for row in by_service.fetchall()
            },
            "by_type": {
                row[0] or "unknown": row[1] for row in by_type.fetchall()
            },
            "by_operation": {
                row[0] or "unknown": row[1]
                for row in by_operation.fetchall()
            },
        }

    @staticmethod
    def _apply_filters(query, filters: EventQueryFilters):
        """Apply configured list filters to a SQLAlchemy query."""
        if filters.service_name:
            query = query.where(
                KafkaEvent.service_name == filters.service_name
            )
        if filters.source_table_id:
            query = query.where(
                KafkaEvent.source_table_id == filters.source_table_id
            )
        if filters.row_identity is not None:
            query = query.where(
                KafkaEvent.row_identity == filters.row_identity
            )
        if filters.event_type:
            query = query.where(KafkaEvent.event_type == filters.event_type)
        if filters.start_time is not None:
            query = query.where(KafkaEvent.event_time >= filters.start_time)
        if filters.end_time is not None:
            query = query.where(KafkaEvent.event_time <= filters.end_time)
        if filters.search_term:
            query = query.where(
                KafkaEvent.raw_payload.ilike(f"%{filters.search_term}%")
            )
        return query
