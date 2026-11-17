"""Schema discovery service — core class.

Extracts and registers table and column schemas from Debezium messages.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Optional

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.source_metadata import (
    extract_source_coordinates,
    extract_source_record,
)
from db_monitor.metrics import columns_discovered_total, tables_discovered_total
from db_monitor.schema.cache import schema_cache_backplane
from db_monitor.schema.columns import extract_column_definitions
from db_monitor.schema.discovery.upserts import upsert_columns, upsert_table
from db_monitor.schema.discovery.queries import (
    get_all_tables as _get_all_tables,
    get_table_by_id as _get_table_by_id,
    get_table_by_name as _get_table_by_name,
)
from sqlalchemy import func, select

from db_monitor.core.models import MonitoredColumn, MonitoredTable

logger = logging.getLogger(__name__)


class SchemaDiscovery:
    """Discovers and maintains schema catalog from Debezium CDC events."""

    def __init__(
        self, session_factory: Callable, cache_ttl_seconds: float = 60.0
    ):
        self.session_factory = session_factory
        self._cache_ttl_seconds = cache_ttl_seconds
        self._table_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        # Import here to avoid circular import:
        # state -> core -> state
        from db_monitor.schema.discovery.state import _registered_schema_discoveries

        _registered_schema_discoveries.add(self)

    async def process_event(
        self,
        event: KafkaEvent,
        session=None,
    ) -> Optional[int]:
        """Process an event to discover or update schema.

        Returns the resolved source table ID.
        """
        if session is None:
            async with self.session_factory() as owned_session:
                async with owned_session.begin():
                    return await self._process_event(event, owned_session)

        return await self._process_event(event, session)

    async def process_events_batch(
        self,
        events: list[KafkaEvent],
        session,
    ) -> dict[int, Optional[int]]:
        """Discover schema for a batch of events in a single round-trip."""
        from db_monitor.schema.discovery.batch import process_events_batch as _batch_impl

        result, table_key_list = await _batch_impl(events, session)

        # Invalidate caches and refresh metrics
        for key in table_key_list:
            await self._invalidate_cluster_cache(key[0], key[2])
        await self._refresh_schema_metrics(session)

        return result

    async def _process_event(
        self, event: KafkaEvent, session: object  # noqa: ANN401
    ) -> Optional[int]:
        """Process an event within an existing database session."""
        if not event.event_data:
            return None

        payload = event.event_data
        source = extract_source_record(payload)
        source_coordinates = extract_source_coordinates(payload)
        if not source or source_coordinates is None:
            return None

        table_identifier = source_coordinates.table_name
        if not table_identifier:
            return None

        db_name = source_coordinates.database_name or "unknown"
        service_name = source_coordinates.service_name or "unknown"

        topic_parts = event.service_name.split(".")
        if len(topic_parts) >= 2:
            service_name = topic_parts[0]

        topic_name = event.service_name

        table_id = await self._upsert_table(
            session=session,
            service_name=service_name,
            database_name=db_name,
            table_name=table_identifier,
            topic_name=topic_name,
        )

        column_definitions = extract_column_definitions(payload)
        if table_id and column_definitions:
            await self._upsert_columns(session, table_id, column_definitions)

        await self._invalidate_cluster_cache(service_name, table_identifier)

        return table_id

    async def _upsert_table(
        self,
        session,
        service_name: str,
        database_name: str,
        table_name: str,
        topic_name: str,
    ) -> int:
        """Upsert a monitored table and return its ID."""
        table_id = await upsert_table(
            session,
            service_name=service_name,
            database_name=database_name,
            table_name=table_name,
            topic_name=topic_name,
        )
        await self._refresh_schema_metrics(session)
        return table_id

    async def _upsert_columns(
        self,
        session,
        table_id: int,
        column_definitions: list[dict[str, Any]],
    ) -> None:
        """Upsert columns from Debezium schema or inferred row payloads."""
        await upsert_columns(session, table_id, column_definitions)
        if column_definitions:
            await self._refresh_schema_metrics(session)

    def _invalidate_table_cache(
        self, service_name: str, table_name: str
    ) -> None:
        """Remove a table from the local cache."""
        self._table_cache.pop(self._cache_key(service_name, table_name), None)

    async def _invalidate_cluster_cache(
        self,
        service_name: str,
        table_name: str,
    ) -> None:
        """Invalidate this table cache locally and across replicas."""
        from db_monitor.schema.discovery.state import (
            _invalidate_registered_schema_caches,
        )

        _invalidate_registered_schema_caches(service_name, table_name)
        await schema_cache_backplane.publish_invalidation(
            service_name,
            table_name,
        )

    def _cache_key(self, service_name: str, table_name: str) -> str:
        return f"{service_name}:{table_name}"

    def _extract_column_definitions(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Extract column definitions from a Debezium event payload."""
        return extract_column_definitions(payload)

    async def _refresh_schema_metrics(self, session) -> None:
        """Update Prometheus gauge metrics for table/column counts."""
        table_count_result = await session.execute(
            select(func.count())
            .select_from(MonitoredTable)
            .where(MonitoredTable.is_active)
        )
        column_count_result = await session.execute(
            select(func.count()).select_from(MonitoredColumn)
        )
        tables_discovered_total.set(int(table_count_result.scalar() or 0))
        columns_discovered_total.set(int(column_count_result.scalar() or 0))

    async def get_all_tables(self) -> list[dict[str, Any]]:
        """Get all monitored tables."""
        return await _get_all_tables(self.session_factory)

    async def get_table_by_id(self, table_id: int) -> Optional[dict[str, Any]]:
        """Get a table by ID with its columns."""
        return await _get_table_by_id(self.session_factory, table_id)

    async def get_table_by_name(
        self, service_name: str, table_name: str
    ) -> Optional[dict[str, Any]]:
        """Get a table by service and table name with caching."""
        return await _get_table_by_name(
            self.session_factory,
            self._table_cache,
            self._cache_ttl_seconds,
            service_name,
            table_name,
        )
