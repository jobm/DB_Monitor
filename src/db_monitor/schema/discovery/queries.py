"""Standalone query functions for schema discovery — table and column lookups."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Optional

from db_monitor.core.models import MonitoredColumn, MonitoredTable
from sqlalchemy import select


async def get_all_tables(
    session_factory: Callable,
) -> list[dict[str, Any]]:
    """Get all monitored tables."""
    async with session_factory() as session:
        stmt = select(MonitoredTable).where(MonitoredTable.is_active)
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


async def get_table_by_id(
    session_factory: Callable,
    table_id: int,
) -> Optional[dict[str, Any]]:
    """Get a table by ID with its columns."""
    async with session_factory() as session:
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
    session_factory: Callable,
    cache: dict[str, tuple[float, dict[str, Any]]],
    cache_ttl_seconds: float,
    service_name: str,
    table_name: str,
) -> Optional[dict[str, Any]]:
    """Get a table by service and table name with caching."""
    cache_key = f"{service_name}:{table_name}"
    cached = cache.get(cache_key)
    if cached:
        cached_at, cached_table = cached
        if time.monotonic() - cached_at <= cache_ttl_seconds:
            return cached_table
        cache.pop(cache_key, None)

    async with session_factory() as session:
        stmt = select(MonitoredTable).where(
            MonitoredTable.service_name == service_name,
            MonitoredTable.table_name == table_name,
            MonitoredTable.is_active,
        )
        result = await session.execute(stmt)
        table = result.scalar_one_or_none()
        if not table:
            return None
        res = await get_table_by_id(session_factory, table.id)
        if res is not None:
            cache[cache_key] = (time.monotonic(), res)
        return res
