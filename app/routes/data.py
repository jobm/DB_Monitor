"""Data-centric table, event, and change endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select

from auth import require_viewer_role
from change_processor import ChangeProcessor
from extensions import AsyncSessionLocal
from models import KafkaEvent
from response_models import (
    ChangesResponse,
    EventsResponse,
    PointInTimeValueResponse,
    TableColumnsResponse,
    TableListResponse,
)
from .utils import parse_json_object_query, parse_timestamp
from schema_discovery import SchemaDiscovery

router = APIRouter()

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)


def resolve_table_reference(
    table_name: str,
    service_name: str | None = None,
) -> tuple[str, str]:
    """Resolve either explicit or service-qualified table references."""
    normalized_table_name = table_name.strip()
    if service_name:
        return service_name, normalized_table_name

    if "." not in normalized_table_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Table reference is ambiguous. Pass service_name explicitly, "
                "for example service_name=orderdb&table_name=orders, or use "
                "table_name=orderdb.orders."
            ),
        )

    resolved_service_name, resolved_table_name = normalized_table_name.split(
        ".",
        1,
    )
    if not resolved_service_name or not resolved_table_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid table reference. Expected '<service>.<table>', "
                "such as 'orderdb.orders'."
            ),
        )

    return resolved_service_name, resolved_table_name


@router.get(
    "/tables",
    dependencies=[Depends(require_viewer_role)],
    response_model=TableListResponse,
)
async def get_tables():
    """List all monitored tables."""
    tables = await schema_discovery.get_all_tables()
    return {"tables": tables, "count": len(tables)}


@router.get(
    "/tables/{service_name}/{table_name}",
    dependencies=[Depends(require_viewer_role)],
)
async def get_table(service_name: str, table_name: str):
    """Get table details including columns."""
    table = await schema_discovery.get_table_by_name(service_name, table_name)
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {service_name}.{table_name} was not found. "
                "Check GET /tables to confirm discovery has completed "
                "for that table."
            ),
        )
    return table


@router.get(
    "/tables/{service_name}/{table_name}/columns",
    dependencies=[Depends(require_viewer_role)],
    response_model=TableColumnsResponse,
)
async def get_table_columns(service_name: str, table_name: str):
    """Get columns for a specific table."""
    table = await schema_discovery.get_table_by_name(service_name, table_name)
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {service_name}.{table_name} was not found. "
                "Check GET /tables to confirm discovery has completed "
                "for that table."
            ),
        )
    return {"columns": table.get("columns", [])}


@router.get(
    "/events",
    dependencies=[Depends(require_viewer_role)],
    response_model=EventsResponse,
)
async def get_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service_name: str | None = None,
    source_table_id: int | None = Query(None, ge=1),
    row_identity: str | None = None,
    event_type: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    search_term: str | None = None,
):
    """Get events with pagination and optional filtering."""
    parsed_start_time = parse_timestamp(start_time, "start_time")
    parsed_end_time = parse_timestamp(end_time, "end_time")
    parsed_row_identity = parse_json_object_query(
        row_identity,
        "row_identity",
    )

    async with AsyncSessionLocal() as session:
        query = select(KafkaEvent).order_by(KafkaEvent.id.desc())

        if service_name:
            query = query.where(KafkaEvent.service_name == service_name)
        if source_table_id:
            query = query.where(KafkaEvent.source_table_id == source_table_id)
        if parsed_row_identity is not None:
            query = query.where(KafkaEvent.row_identity == parsed_row_identity)
        if event_type:
            query = query.where(KafkaEvent.event_type == event_type)

        if parsed_start_time:
            query = query.where(KafkaEvent.event_time >= parsed_start_time)
        if parsed_end_time:
            query = query.where(KafkaEvent.event_time <= parsed_end_time)

        if search_term:
            query = query.where(
                KafkaEvent.raw_payload.ilike(f"%{search_term}%")
            )

        count_query = select(func.count()).select_from(query.subquery())
        total_result = await session.execute(count_query)
        total = total_result.scalar()

        query = query.limit(limit).offset(offset)
        result = await session.execute(query)
        events = result.scalars().all()

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
            "offset": offset,
        }


@router.get("/events/stats", dependencies=[Depends(require_viewer_role)])
async def get_event_stats():
    """Get aggregated event statistics."""
    async with AsyncSessionLocal() as session:
        by_service = await session.execute(
            select(
                KafkaEvent.service_name,
                func.count(KafkaEvent.id),
            ).group_by(KafkaEvent.service_name)
        )
        by_type = await session.execute(
            select(KafkaEvent.event_type, func.count(KafkaEvent.id)).group_by(
                KafkaEvent.event_type
            )
        )
        by_operation = await session.execute(
            select(KafkaEvent.operation, func.count(KafkaEvent.id)).group_by(
                KafkaEvent.operation
            )
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
                row[0] or "unknown": row[1] for row in by_operation.fetchall()
            },
        }


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
):
    """Get column changes for a table with optional filters."""
    resolved_service_name, resolved_table_name = resolve_table_reference(
        table_name,
        service_name,
    )
    parsed_from_time = parse_timestamp(from_time, "from_time")
    parsed_to_time = parse_timestamp(to_time, "to_time")
    parsed_row_identity = parse_json_object_query(row_identity, "row_identity")

    table = await schema_discovery.get_table_by_name(
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

    changes = await change_processor.get_changes(
        table_id=table["id"],
        column_id=column_id,
        row_identity=parsed_row_identity,
        from_time=parsed_from_time,
        to_time=parsed_to_time,
        limit=limit,
        offset=offset,
    )
    return {
        "service_name": resolved_service_name,
        "table_name": resolved_table_name,
        "row_identity": parsed_row_identity,
        "changes": changes,
        "count": len(changes),
        "limit": limit,
        "offset": offset,
    }


async def _get_value_at_time_response(
    service_name: str,
    table_name: str,
    column_name: str,
    timestamp: str,
):
    parsed_timestamp = parse_timestamp(timestamp, "timestamp")
    if parsed_timestamp is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "timestamp is required. Provide an ISO 8601 value such as "
                "'2024-01-01T00:00:00Z'."
            ),
        )

    value = await change_processor.get_value_at_time(
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
