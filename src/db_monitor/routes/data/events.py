"""Event query routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.params import Param

from db_monitor.auth import get_tenant_filter, require_viewer_role
from db_monitor.core.responses import EventsResponse
from db_monitor.repositories.events import EventQueryFilters
from db_monitor.routes.data.factories import get_events_repository
from db_monitor.routes.utils import parse_json_object_query, parse_timestamp

router = APIRouter()


@router.get(
    "/events",
    dependencies=[Depends(require_viewer_role)],
    response_model=EventsResponse,
)
async def get_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(
        None,
        description=(
            "JSON cursor with capture_time and event_id, for example "
            '{"capture_time":"2026-06-02T12:00:00Z","event_id":123}'
        ),
    ),
    cursor_id: int | None = Query(None, ge=1),
    cursor_time: str | None = Query(
        None,
        description="ISO 8601 timestamp for keyset pagination. "
                    "Use with cursor_id for efficient scanning.",
    ),
    service_name: str | None = None,
    source_table_id: int | None = Query(None, ge=1),
    row_identity: str | None = None,
    event_type: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    search_term: str | None = None,
    include_total: bool | None = Query(
        None,
        description=(
            "Request total count. Omit (default) to skip totals when a "
            "cursor is active. Set true/false to force."
        ),
    ),
    tenant_filter: str | None = Depends(get_tenant_filter),
):
    """Get events with pagination and optional filtering.

    Supports keyset pagination via ``cursor`` (preferred) or the
    legacy ``cursor_time`` + ``cursor_id`` pair for efficient scanning
    over large, time-partitioned tables.

    Pass ``include_total=false`` explicitly to skip the expensive total
    count query on large datasets.
    """
    cursor_value = None if isinstance(cursor_id, Param) else cursor_id
    resolved_source_table_id = (
        None
        if isinstance(source_table_id, Param)
        else source_table_id
    )
    parsed_cursor_time = None
    if cursor is not None and not isinstance(cursor, Param):
        cursor_payload = parse_json_object_query(cursor, "cursor")
        if cursor_payload is None:
            raise HTTPException(
                status_code=400,
                detail="cursor must be a JSON object.",
            )
        cursor_value_raw = cursor_payload.get("event_id")
        if cursor_value_raw is None:
            cursor_value_raw = cursor_payload.get("id")
        if cursor_value_raw is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "cursor must include event_id (events) or id (changes)."
                ),
            )
        try:
            cursor_value = int(str(cursor_value_raw))
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="cursor event_id/id must be an integer.",
            ) from exc
        cursor_time_value = (
            cursor_payload.get("capture_time")
            or cursor_payload.get("changed_at")
        )
        if cursor_time_value is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "cursor must include capture_time (events) or "
                    "changed_at (changes)."
                ),
            )
        parsed_cursor_time = parse_timestamp(
            str(cursor_time_value),
            "cursor.capture_time",
        )
    elif cursor_time is not None and not isinstance(cursor_time, Param):
        parsed_cursor_time = parse_timestamp(cursor_time, "cursor_time")

    keyset = parsed_cursor_time is not None and cursor_value is not None

    parsed_start_time = parse_timestamp(start_time, "start_time")
    parsed_end_time = parse_timestamp(end_time, "end_time")
    parsed_row_identity = parse_json_object_query(
        row_identity,
        "row_identity",
    )

    if include_total is None or isinstance(include_total, Param):
        resolved_include_total = not keyset and cursor_value is None
    else:
        resolved_include_total = bool(include_total)

    # Enforce tenant isolation: scoped keys can only see their tenant's data
    if tenant_filter and service_name and service_name != tenant_filter:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. This API key is scoped to tenant "
                f"'{tenant_filter}', but service_name '{service_name}' "
                f"was requested."
            ),
        )
    effective_service_name = tenant_filter or service_name

    events, total = await get_events_repository().list_events(
        filters=EventQueryFilters(
            service_name=effective_service_name,
            source_table_id=resolved_source_table_id,
            row_identity=parsed_row_identity,
            event_type=event_type,
            start_time=parsed_start_time,
            end_time=parsed_end_time,
            search_term=search_term,
        ),
        limit=limit,
        offset=offset,
        cursor_id=cursor_value,
        cursor_time=parsed_cursor_time,
        include_total=resolved_include_total,
    )

    next_cursor = None
    if keyset and len(events) == limit:
        last = events[-1]
        if last.capture_time:
            next_cursor = {
                "capture_time": last.capture_time.isoformat(),
                "event_id": last.id,
            }
    elif cursor_value is not None and len(events) == limit:
        next_cursor = {
            "capture_time": (
                events[-1].capture_time.isoformat()
                if events[-1].capture_time
                else None
            ),
            "event_id": events[-1].id,
        }

    return {
        "events": [
            {
                "id": event.id,
                "event_type": event.event_type,
                "event_time": (
                    event.event_time.isoformat()
                    if event.event_time
                    else None
                ),
                "capture_time": (
                    event.capture_time.isoformat()
                    if event.capture_time
                    else None
                ),
                "ingested_at": (
                    event.ingested_at.isoformat()
                    if event.ingested_at
                    else None
                ),
                "user_id": event.user_id,
                "service_name": event.service_name,
                "operation": event.operation,
                "source_table_id": event.source_table_id,
                "row_identity": event.row_identity,
                "event_data": event.event_data,
            }
            for event in events
        ],
        "total": total,
        "limit": limit,
        "offset": 0 if keyset or cursor_value is not None else offset,
        "next_cursor": next_cursor,
    }


@router.get("/events/stats", dependencies=[Depends(require_viewer_role)])
async def get_event_stats(
    tenant_filter: str | None = Depends(get_tenant_filter),
):
    """Get aggregated event statistics.

    Tenant-scoped API keys only see statistics for their own tenant.
    """
    return await get_events_repository().event_stats(
        tenant_filter=tenant_filter
    )
