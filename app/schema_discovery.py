"""Schema discovery service - extracts and registers table/column schemas from Debezium messages."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import KafkaEvent, MonitoredColumn, MonitoredTable

logger = logging.getLogger(__name__)


class SchemaDiscovery:
    """Discovers and maintains schema catalog from Debezium CDC events."""

    def __init__(self, session_factory: Callable):
        self.session_factory = session_factory
        self._table_cache = {}

    async def process_event(self, event: KafkaEvent) -> Optional[int]:
        """Process an event to discover/update schema. Returns source_table_id."""
        if not event.event_data:
            return None

        payload = event.event_data
        source = payload.get("source")
        if not isinstance(source, dict):
            source = payload.get("payload", {}).get("source")
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
            service_name=service_name,
            database_name=db_name,
            table_name=table_identifier,
            topic_name=topic_name,
        )

        schema = payload.get("schema") or (
            payload.get("payload", {}).get("schema")
            if isinstance(payload.get("payload"), dict)
            else None
        )
        if table_id and schema:
            await self._upsert_columns(table_id, schema)

        return table_id

    async def _upsert_table(
        self,
        service_name: str,
        database_name: str,
        table_name: str,
        topic_name: str,
    ) -> int:
        """Upsert a monitored table and return its ID."""
        async with self.session_factory() as session:
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
            await session.commit()
            return table_id

    async def _upsert_columns(self, table_id: int, schema: dict[str, Any]) -> None:
        """Upsert columns from Debezium schema."""
        async with self.session_factory() as session:
            columns_to_insert = []

            fields = schema.get("fields", {})
            if isinstance(fields, dict):
                for field_name, field_def in fields.items():
                    if field_name in ("__crct", "__deleted"):
                        continue

                    field_type = field_def.get("type", "unknown")
                    if isinstance(field_type, dict):
                        field_type = field_type.get(
                            "name", field_type.get("type", "unknown")
                        )

                    is_pk = field_def.get("optional", True) is False and field_def.get(
                        "primaryKey", False
                    )

                    columns_to_insert.append(
                        {
                            "table_id": table_id,
                            "column_name": field_name,
                            "data_type": str(field_type),
                            "is_primary_key": is_pk,
                            "is_nullable": field_def.get("optional", True),
                            "audit_enabled": True,
                        }
                    )

            if columns_to_insert:
                stmt = (
                    pg_insert(MonitoredColumn)
                    .values(columns_to_insert)
                    .on_conflict_do_update(
                        index_elements=["table_id", "column_name"],
                        set_={
                            "data_type": MonitoredColumn.data_type,
                            "is_primary_key": MonitoredColumn.is_primary_key,
                            "is_nullable": MonitoredColumn.is_nullable,
                        },
                    )
                )
                await session.execute(stmt)
                await session.commit()

    async def get_all_tables(self) -> list[dict[str, Any]]:
        """Get all monitored tables."""
        async with self.session_factory() as session:
            stmt = select(MonitoredTable).where(MonitoredTable.is_active == True)
            result = await session.execute(stmt)
            tables = result.scalars().all()
            return [
                {
                    "id": t.id,
                    "service_name": t.service_name,
                    "database_name": t.database_name,
                    "table_name": t.table_name,
                    "topic_name": t.topic_name,
                    "created_at": t.created_at.isoformat() if t.created_at else None,
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
        cache_key = f"{service_name}:{table_name}"
        if cache_key in self._table_cache:
            return self._table_cache[cache_key]

        async with self.session_factory() as session:
            stmt = select(MonitoredTable).where(
                MonitoredTable.service_name == service_name,
                MonitoredTable.table_name == table_name,
                MonitoredTable.is_active == True,
            )
            result = await session.execute(stmt)
            table = result.scalar_one_or_none()
            if not table:
                return None
            res = await self.get_table_by_id(table.id)
            self._table_cache[cache_key] = res
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
