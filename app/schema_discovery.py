"""Schema discovery service.

Extracts and registers table and column schemas from Debezium messages.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, Optional

from metrics import columns_discovered_total, tables_discovered_total
from models import KafkaEvent, MonitoredColumn, MonitoredTable
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)


class SchemaDiscovery:
    """Discovers and maintains schema catalog from Debezium CDC events."""

    def __init__(
        self, session_factory: Callable, cache_ttl_seconds: float = 60.0
    ):
        self.session_factory = session_factory
        self._cache_ttl_seconds = cache_ttl_seconds
        self._table_cache: dict[str, tuple[float, dict[str, Any]]] = {}

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

    async def _process_event(
        self, event: KafkaEvent, session
    ) -> Optional[int]:
        """Process an event within an existing database session."""
        if not event.event_data:
            return None

        payload = event.event_data
        source = self._extract_source(payload)
        if not isinstance(source, dict):
            return None

        table_identifier = source.get("table")
        if not table_identifier:
            return None

        db_name = source.get("db", "unknown")
        service_name = source.get("name", source.get("server", "unknown"))

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

        column_definitions = self._extract_column_definitions(payload)
        if table_id and column_definitions:
            await self._upsert_columns(session, table_id, column_definitions)

        self._invalidate_table_cache(service_name, table_identifier)

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
        stmt = (
            pg_insert(MonitoredTable)
            .values(
                service_name=service_name,
                database_name=database_name,
                table_name=table_name,
                topic_name=topic_name,
                is_active=True,
            )
            .on_conflict_do_update(
                index_elements=["service_name", "database_name", "table_name"],
                set_={
                    "topic_name": topic_name,
                    "is_active": True,
                },
            )
            .returning(MonitoredTable.id)
        )
        result = await session.execute(stmt)
        table_id = result.scalar_one()
        await self._refresh_schema_metrics(session)
        return table_id

    async def _upsert_columns(
        self,
        session,
        table_id: int,
        column_definitions: list[dict[str, Any]],
    ) -> None:
        """Upsert columns from Debezium schema or inferred row payloads."""
        if column_definitions:
            insert_stmt = pg_insert(MonitoredColumn).values(
                [
                    {**column, "table_id": table_id}
                    for column in column_definitions
                ]
            )
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=["table_id", "column_name"],
                set_={
                    "data_type": insert_stmt.excluded.data_type,
                    "is_primary_key": insert_stmt.excluded.is_primary_key,
                    "is_nullable": insert_stmt.excluded.is_nullable,
                },
            )
            await session.execute(stmt)
            await self._refresh_schema_metrics(session)

    def _extract_source(
        self, payload: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        source = payload.get("source")
        if isinstance(source, dict):
            return source

        nested_payload = payload.get("payload")
        if isinstance(nested_payload, dict):
            nested_source = nested_payload.get("source")
            if isinstance(nested_source, dict):
                return nested_source

        return None

    def _extract_column_definitions(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        schema = payload.get("schema")
        if not isinstance(schema, dict):
            nested_payload = payload.get("payload")
            if isinstance(nested_payload, dict):
                nested_schema = nested_payload.get("schema")
                if isinstance(nested_schema, dict):
                    schema = nested_schema

        column_definitions = (
            self._extract_columns_from_schema(schema)
            if isinstance(schema, dict)
            else []
        )
        if column_definitions:
            return column_definitions

        return self._extract_columns_from_payload(payload)

    def _extract_columns_from_schema(
        self, schema: dict[str, Any]
    ) -> list[dict[str, Any]]:
        row_fields = self._extract_row_fields(schema.get("fields"))
        if not row_fields:
            return []

        columns: list[dict[str, Any]] = []
        for field_def in row_fields:
            if not isinstance(field_def, dict):
                continue

            field_name = field_def.get("field") or field_def.get("name")
            if not field_name or str(field_name).startswith("__"):
                continue

            field_type = (
                field_def.get("type") or field_def.get("name") or "unknown"
            )
            columns.append(
                {
                    "column_name": str(field_name),
                    "data_type": self._normalize_field_type(field_type),
                    "is_primary_key": bool(field_def.get("primaryKey", False)),
                    "is_nullable": bool(field_def.get("optional", True)),
                    "audit_enabled": True,
                }
            )

        return columns

    def _extract_row_fields(self, fields: Any) -> list[dict[str, Any]]:
        if isinstance(fields, list):
            row_container = self._find_row_schema(fields)
            if isinstance(row_container, dict) and isinstance(
                row_container.get("fields"), list
            ):
                return [
                    field
                    for field in row_container["fields"]
                    if isinstance(field, dict)
                ]
            return [field for field in fields if isinstance(field, dict)]

        if isinstance(fields, dict):
            for candidate_name in ("after", "before"):
                candidate = fields.get(candidate_name)
                if isinstance(candidate, dict) and isinstance(
                    candidate.get("fields"), list
                ):
                    return [
                        field
                        for field in candidate["fields"]
                        if isinstance(field, dict)
                    ]

            extracted_fields: list[dict[str, Any]] = []
            for field_name, field_def in fields.items():
                if isinstance(field_def, dict):
                    extracted_fields.append({"field": field_name, **field_def})
            return extracted_fields

        return []

    def _find_row_schema(
        self, fields: list[dict[str, Any]]
    ) -> Optional[dict[str, Any]]:
        for candidate in fields:
            field_name = candidate.get("field") or candidate.get("name")
            if field_name in {"after", "before"} and isinstance(
                candidate.get("fields"), list
            ):
                return candidate
        return None

    def _extract_columns_from_payload(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        row_versions: list[dict[str, Any]] = []
        for row_key in ("after", "before"):
            row_data = payload.get(row_key)
            if not isinstance(row_data, dict):
                nested_payload = payload.get("payload")
                if isinstance(nested_payload, dict):
                    nested_row_data = nested_payload.get(row_key)
                    if isinstance(nested_row_data, dict):
                        row_data = nested_row_data

            if isinstance(row_data, dict):
                row_versions.append(row_data)

        if not row_versions:
            return []

        ordered_columns: list[str] = []
        for row_version in row_versions:
            for column_name in row_version:
                if (
                    column_name.startswith("__")
                    or column_name in ordered_columns
                ):
                    continue
                ordered_columns.append(column_name)

        columns: list[dict[str, Any]] = []
        for column_name in ordered_columns:
            sample_value = next(
                (
                    row_version[column_name]
                    for row_version in row_versions
                    if column_name in row_version
                    and row_version[column_name] is not None
                ),
                None,
            )
            columns.append(
                {
                    "column_name": column_name,
                    "data_type": self._infer_data_type(sample_value),
                    "is_primary_key": False,
                    "is_nullable": any(
                        column_name in row_version
                        and row_version[column_name] is None
                        for row_version in row_versions
                    ),
                    "audit_enabled": True,
                }
            )

        return columns

    def _normalize_field_type(self, field_type: Any) -> str:
        if isinstance(field_type, dict):
            return str(
                field_type.get("name") or field_type.get("type") or "unknown"
            )
        return str(field_type)

    def _infer_data_type(self, value: Any) -> str:
        if value is None:
            return "unknown"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return "string"

    def _invalidate_table_cache(
        self, service_name: str, table_name: str
    ) -> None:
        self._table_cache.pop(self._cache_key(service_name, table_name), None)

    def _cache_key(self, service_name: str, table_name: str) -> str:
        return f"{service_name}:{table_name}"

    async def _refresh_schema_metrics(self, session) -> None:
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
        async with self.session_factory() as session:
            stmt = select(MonitoredTable).where(
                MonitoredTable.is_active
            )
            result = await session.execute(stmt)
            tables = result.scalars().all()
            return [
                {
                    "id": t.id,
                    "service_name": t.service_name,
                    "database_name": t.database_name,
                    "table_name": t.table_name,
                    "topic_name": t.topic_name,
                    "created_at": t.created_at.isoformat()
                    if t.created_at
                    else None,
                }
                for t in tables
            ]

    async def get_table_by_id(self, table_id: int) -> Optional[dict[str, Any]]:
        """Get a table by ID with its columns."""
        async with self.session_factory() as session:
            stmt = select(MonitoredTable).where(MonitoredTable.id == table_id)
            result = await session.execute(stmt)
            table = result.scalar_one_or_none()
            if not table:
                return None

            col_stmt = select(MonitoredColumn).where(
                MonitoredColumn.table_id == table_id
            )
            col_result = await session.execute(col_stmt)
            columns = col_result.scalars().all()

            return {
                "id": table.id,
                "service_name": table.service_name,
                "database_name": table.database_name,
                "table_name": table.table_name,
                "topic_name": table.topic_name,
                "created_at": table.created_at.isoformat()
                if table.created_at
                else None,
                "columns": [
                    {
                        "id": c.id,
                        "column_name": c.column_name,
                        "data_type": c.data_type,
                        "is_primary_key": c.is_primary_key,
                        "is_nullable": c.is_nullable,
                        "audit_enabled": c.audit_enabled,
                    }
                    for c in columns
                ],
            }

    async def get_table_by_name(
        self, service_name: str, table_name: str
    ) -> Optional[dict[str, Any]]:
        """Get a table by service and table name."""
        cache_key = self._cache_key(service_name, table_name)
        cached = self._table_cache.get(cache_key)
        if cached:
            cached_at, cached_table = cached
            if time.monotonic() - cached_at <= self._cache_ttl_seconds:
                return cached_table
            self._table_cache.pop(cache_key, None)

        async with self.session_factory() as session:
            stmt = select(MonitoredTable).where(
                MonitoredTable.service_name == service_name,
                MonitoredTable.table_name == table_name,
                MonitoredTable.is_active,
            )
            result = await session.execute(stmt)
            table = result.scalar_one_or_none()
            if not table:
                return None
            res = await self.get_table_by_id(table.id)
            if res is not None:
                self._table_cache[cache_key] = (time.monotonic(), res)
            return res


def extract_operation(payload: dict[str, Any]) -> str:
    """Extract operation type from Debezium payload."""
    op = (
        payload.get("op")
        or (
            payload.get("payload", {}).get("op")
            if isinstance(payload.get("payload"), dict)
            else None
        )
        or "u"
    )
    op_map = {
        "c": "INSERT",
        "r": "INSERT",  # read (snapshot)
        "u": "UPDATE",
        "d": "DELETE",
    }
    return op_map.get(op, "UPDATE")
