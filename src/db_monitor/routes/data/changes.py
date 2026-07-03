"""Column change query routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.params import Param

from db_monitor.auth import require_viewer_role
from db_monitor.core.responses import ChangesResponse, PointInTimeValueResponse
from db_monitor.routes.data.factories import (
    get_change_processor,
    get_schema_discovery,
    resolve_table_reference,
)
from db_monitor.routes.utils import parse_json_object_query, parse_timestamp

router = APIRouter()


@router.get(
    "/changes",
    dependencies=[Depends(require_viewer_role)],
    response_model=ChangesResponse,
)
async def get_changes(
    table_name: str = Query(...),
    service_name: str | None = None,
    column_name: str | None = None,
    row_identity: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(
        None,
        description=(
            "JSON cursor with changed_at and id, for example "
            '{"changed_at":"2026-06-02T12:00:00Z","id":123}'
        ),
    ),
    cursor_id: int | None = Query(None, ge=1),
    cursor_time: str | None = Query(
        None,
        description="ISO 8601 timestamp for keyset pagination. "
                    "Use with cursor_id for efficient scanning.",
    ),
    include_total: bool | None = Query(
        None,
        description="Request total count. Omit for default behavior.",
    ),
):
    """Get column changes for a table with optional filters.

    Supports keyset pagination ``(changed_at DESC, id DESC)`` via
    ``cursor`` (preferred) or the legacy ``cursor_time`` + ``cursor_id``
    pair for efficient scanning of large change histories. When a
    cursor is provided, ``offset`` is ignored and ``total`` is omitted
    unless ``include_total=true`` is explicitly set.
    """
    resolved_service_name, resolved_table_name = resolve_table_reference(
        table_name,
        service_name,
    )
    parsed_from_time = parse_timestamp(from_time, "from_time")
    parsed_to_time = parse_timestamp(to_time, "to_time")
    parsed_row_identity = parse_json_object_query(row_identity, "row_identity")

    table = await get_schema_discovery().get_table_by_name(
        resolved_service_name,
        resolved_table_name,
    )
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {resolved_service_name}.{resolved_table_name} "
                "was not found. Check GET /tables to confirm the table "
                "has been discovered from live events."
            ),
        )

    column_id = None
    if column_name:
        for col in table.get("columns", []):
            if col["column_name"] == column_name:
                column_id = col["id"]
                break
        if not column_id:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Column {column_name} was not found on "
                    f"{resolved_service_name}.{resolved_table_name}. "
                    "Check GET /tables/{service_name}/{table_name}/"
                    "columns for valid names."
                ),
            )

    cursor_value = None if isinstance(cursor_id, Param) else cursor_id
    parsed_cursor_time = None
    if cursor is not None and not isinstance(cursor, Param):
        cursor_payload = parse_json_object_query(cursor, "cursor")
        cursor_value_raw = cursor_payload.get("id")
        if cursor_value_raw is None:
            cursor_value_raw = cursor_payload.get("event_id")
        if cursor_value_raw is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "cursor must include id (changes) or event_id "
                    "(events)."
                ),
            )
        try:
            cursor_value = int(cursor_value_raw)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="cursor id/event_id must be an integer.",
            ) from exc
        cursor_time_value = (
            cursor_payload.get("changed_at")
            or cursor_payload.get("capture_time")
        )
        if cursor_time_value is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "cursor must include changed_at (changes) or "
                    "capture_time (events)."
                ),
            )
        parsed_cursor_time = parse_timestamp(
            cursor_time_value,
            "cursor.changed_at",
        )
    elif cursor_time is not None and not isinstance(cursor_time, Param):
        parsed_cursor_time = parse_timestamp(cursor_time, "cursor_time")

    keyset = parsed_cursor_time is not None and cursor_value is not None

    if include_total is None or isinstance(include_total, Param):
        resolved_include_total = not keyset and cursor_value is None
    else:
        resolved_include_total = bool(include_total)

    changes, total = await get_change_processor().get_changes(
        table_id=table["id"],
        column_id=column_id,
        row_identity=parsed_row_identity,
        from_time=parsed_from_time,
        to_time=parsed_to_time,
        limit=limit,
        offset=offset,
        cursor_id=cursor_value,
        cursor_time=parsed_cursor_time,
        include_total=resolved_include_total,
    )
    next_cursor = None
    if keyset and len(changes) == limit:
        last = changes[-1]
        last_changed_at = last.get("changed_at")
        if last_changed_at:
            next_cursor = {
                "changed_at": last_changed_at,
                "id": last["id"],
            }
    elif cursor_value is not None and len(changes) == limit:
        next_cursor = {
            "changed_at": changes[-1].get("changed_at"),
            "id": changes[-1]["id"],
        }

    return {
        "service_name": resolved_service_name,
        "table_name": resolved_table_name,
        "row_identity": parsed_row_identity,
        "changes": changes,
        "count": len(changes),
        "total": total,
        "limit": limit,
        "offset": 0 if keyset or cursor_value is not None else offset,
        "next_cursor": next_cursor,
    }


async def _get_value_at_time_response(
    service_name: str,
    table_name: str,
    column_name: str,
    timestamp: str,
):
    """Build a point-in-time value response."""
    parsed_timestamp = parse_timestamp(timestamp, "timestamp")
    if parsed_timestamp is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "timestamp is required. Provide an ISO 8601 value such as "
                "'2024-01-01T00:00:00Z'."
            ),
        )

    value = await get_change_processor().get_value_at_time(
        service_name=service_name,
        table_name=table_name,
        column_name=column_name,
        timestamp=parsed_timestamp,
    )
    return {
        "service_name": service_name,
        "table_name": table_name,
        "column_name": column_name,
        "timestamp": timestamp,
        "value": value,
    }


@router.get(
    "/changes/{service_name}/{table_name}/{column_name}/at",
    dependencies=[Depends(require_viewer_role)],
    response_model=PointInTimeValueResponse,
)
async def get_value_at_time(
    service_name: str,
    table_name: str,
    column_name: str,
    timestamp: str = Query(...),
):
    """Get the value of a column at a specific point in time."""
    return await _get_value_at_time_response(
        service_name=service_name,
        table_name=table_name,
        column_name=column_name,
        timestamp=timestamp,
    )


@router.get(
    "/changes/{table_name}/{column_name}/at",
    dependencies=[Depends(require_viewer_role)],
    response_model=PointInTimeValueResponse,
)
async def get_value_at_time_legacy(
    table_name: str,
    column_name: str,
    timestamp: str = Query(...),
    service_name: str | None = None,
):
    """Backward-compatible point-in-time lookup."""
    resolved_service_name, resolved_table_name = resolve_table_reference(
        table_name,
        service_name,
    )
    return await _get_value_at_time_response(
        service_name=resolved_service_name,
        table_name=resolved_table_name,
        column_name=column_name,
        timestamp=timestamp,
    )
