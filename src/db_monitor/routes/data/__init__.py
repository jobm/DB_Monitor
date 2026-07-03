"""Data-centric table, event, and change endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from db_monitor.routes.data.changes import (
    get_changes,
    get_value_at_time,
    get_value_at_time_legacy,
    router as changes_router,
)
from db_monitor.routes.data.events import (
    get_event_stats,
    get_events,
    router as events_router,
)
from db_monitor.routes.data.tables import (
    get_table,
    get_table_columns,
    get_tables,
    router as tables_router,
)

router = APIRouter()
router.include_router(tables_router)
router.include_router(events_router)
router.include_router(changes_router)

__all__ = [
    "router",
    "get_tables",
    "get_table",
    "get_table_columns",
    "get_events",
    "get_event_stats",
    "get_changes",
    "get_value_at_time",
    "get_value_at_time_legacy",
]
