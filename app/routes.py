import json
from datetime import datetime, timezone
from typing import Optional

from auth import (
    build_access_token as issue_access_token,
)
from auth import (
    build_api_key_expiration,
    build_ws_session_token,
    generate_new_api_key,
    get_api_key_hash,
    is_api_key_usable,
    require_admin_role,
    require_viewer_role,
)
from change_processor import ChangeProcessor
from config import (
    ALLOW_BOOTSTRAP,
    API_KEY_DEFAULT_TTL_DAYS,
    READINESS_MAX_COMMIT_AGE_SECONDS,
    READINESS_MAX_CONSUMER_LAG,
    READINESS_MAX_DLQ_MESSAGES,
)
from consumer_service import (
    get_consumer_health,
    list_consumer_checkpoints_snapshot,
    list_dead_letter_events,
    replay_dead_letter_event_record,
)
from extensions import AsyncSessionLocal
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from lifecycle_manager import lifecycle_manager
from models import ApiKey, KafkaEvent
from schema_discovery import SchemaDiscovery
from sqlalchemy import func, or_, select

router = APIRouter()

schema_discovery = SchemaDiscovery(AsyncSessionLocal)
change_processor = ChangeProcessor(AsyncSessionLocal)


def _parse_timestamp(
    value: Optional[str], field_name: str
) -> Optional[datetime]:
    if value is None:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name} format. "
                "Expected an ISO 8601 timestamp such as "
                "'2024-01-01T00:00:00Z'."
            ),
        ) from exc


def _utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _parse_json_object_query(
    value: Optional[str],
    field_name: str,
) -> Optional[dict[str, object]]:
    """Parse a query parameter expected to contain a JSON object."""
    if value is None:
        return None

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name}. Expected a JSON object string such as "
                "'{\"id\": 42}'."
            ),
        ) from exc

    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid {field_name}. Expected a JSON object string such as "
                "'{\"id\": 42}'."
            ),
        )
    return parsed


def _timestamp_age_seconds(value: Optional[str]) -> Optional[float]:
    """Return the age of an ISO 8601 timestamp in seconds."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return max((_utc_now() - parsed).total_seconds(), 0.0)


def _api_key_response(api_key: ApiKey, raw_key: str, message: str) -> dict:
    """Build a standard API key creation or rotation response."""
    return {
        "api_key": f"{api_key.id}.{raw_key}",
        "owner_name": api_key.owner_name,
        "role": api_key.role,
        "expires_at": (
            api_key.expires_at.isoformat() if api_key.expires_at else None
        ),
        "message": message,
    }


def _readiness_payload() -> dict[str, object]:
    """Build readiness payload from current lifecycle and consumer state."""
    consumer_health = get_consumer_health()

    last_commit_age_seconds = _timestamp_age_seconds(
        consumer_health.get("last_commit_at")
    )
    lag_total = int(consumer_health.get("lag_total") or 0)
    dlq_total = int(consumer_health.get("dlq_messages_total") or 0)

    consumer_ready = bool(
        consumer_health.get("running") and consumer_health.get("connected")
    )
    if (
        consumer_health.get("last_message_at")
        and consumer_health.get("last_commit_at") is None
    ):
        consumer_ready = False
    if (
        last_commit_age_seconds is not None
        and last_commit_age_seconds > READINESS_MAX_COMMIT_AGE_SECONDS
    ):
        consumer_ready = False
    if lag_total > READINESS_MAX_CONSUMER_LAG:
        consumer_ready = False
    if dlq_total > READINESS_MAX_DLQ_MESSAGES:
        consumer_ready = False

    lifecycle_ready = lifecycle_manager.is_healthy
    return {
        "status": (
            "ready" if consumer_ready and lifecycle_ready else "not_ready"
        ),
        "checks": {
            "consumer": {
                **consumer_health,
                "status": "healthy" if consumer_ready else "unhealthy",
                "last_commit_age_seconds": last_commit_age_seconds,
                "max_commit_age_seconds": READINESS_MAX_COMMIT_AGE_SECONDS,
                "max_consumer_lag": READINESS_MAX_CONSUMER_LAG,
                "max_dlq_messages": READINESS_MAX_DLQ_MESSAGES,
            },
            "lifecycle": {
                "status": "healthy" if lifecycle_ready else "unhealthy",
                "shutting_down": lifecycle_manager.is_shutting_down,
            },
        },
    }


def _resolve_table_reference(
    table_name: str, service_name: Optional[str] = None
) -> tuple[str, str]:
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
        ".", 1
    )
    if not resolved_service_name or not resolved_table_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid table reference. Expected '<service>.<table>', such as "
                "'orderdb.orders'."
            ),
        )

    return resolved_service_name, resolved_table_name


@router.get("/tables", dependencies=[Depends(require_viewer_role)])
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
                "Check GET /tables to confirm discovery has completed for that table."
            ),
        )
    return table


@router.get(
    "/tables/{service_name}/{table_name}/columns",
    dependencies=[Depends(require_viewer_role)],
)
async def get_table_columns(service_name: str, table_name: str):
    """Get columns for a specific table."""
    table = await schema_discovery.get_table_by_name(service_name, table_name)
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {service_name}.{table_name} was not found. "
                "Check GET /tables to confirm discovery has completed for that table."
            ),
        )
    return {"columns": table.get("columns", [])}


@router.get("/events", dependencies=[Depends(require_viewer_role)])
async def get_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service_name: Optional[str] = None,
    source_table_id: Optional[int] = Query(None, ge=1),
    row_identity: Optional[str] = None,
    event_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    search_term: Optional[str] = None,
):
    """Get events with pagination and optional filtering."""
    parsed_start_time = _parse_timestamp(start_time, "start_time")
    parsed_end_time = _parse_timestamp(end_time, "end_time")
    parsed_row_identity = _parse_json_object_query(
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
                KafkaEvent.service_name, func.count(KafkaEvent.id)
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
                row[0] or "unknown": row[1] for row in by_service.fetchall()
            },
            "by_type": {
                row[0] or "unknown": row[1] for row in by_type.fetchall()
            },
            "by_operation": {
                row[0] or "unknown": row[1] for row in by_operation.fetchall()
            },
        }


@router.get("/changes", dependencies=[Depends(require_viewer_role)])
async def get_changes(
    table_name: str = Query(...),
    service_name: Optional[str] = None,
    column_name: Optional[str] = None,
    row_identity: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get column changes for a table with optional filters."""
    resolved_service_name, resolved_table_name = _resolve_table_reference(
        table_name,
        service_name,
    )
    parsed_from_time = _parse_timestamp(from_time, "from_time")
    parsed_to_time = _parse_timestamp(to_time, "to_time")
    parsed_row_identity = _parse_json_object_query(
        row_identity,
        "row_identity",
    )

    table = await schema_discovery.get_table_by_name(
        resolved_service_name,
        resolved_table_name,
    )
    if not table:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Table {resolved_service_name}.{resolved_table_name} was not found. "
                "Check GET /tables to confirm the table has been discovered from live events."
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
                    "Check GET /tables/{service_name}/{table_name}/columns for valid names."
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
    parsed_timestamp = _parse_timestamp(timestamp, "timestamp")
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
)
async def get_value_at_time_legacy(
    table_name: str,
    column_name: str,
    timestamp: str = Query(...),
    service_name: Optional[str] = None,
):
    """Backward-compatible point-in-time lookup.

    Supports callers that still pass an explicit service name.
    """
    resolved_service_name, resolved_table_name = _resolve_table_reference(
        table_name,
        service_name,
    )
    return await _get_value_at_time_response(
        service_name=resolved_service_name,
        table_name=resolved_table_name,
        column_name=column_name,
        timestamp=timestamp,
    )


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    database_status = "healthy"
    database_error = None
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(select(1))
    except Exception as exc:
        database_status = "unhealthy"
        database_error = str(exc)

    consumer_health = get_consumer_health()
    lifecycle_status = (
        "healthy" if lifecycle_manager.is_healthy else "unhealthy"
    )
    overall_status = (
        "healthy"
        if database_status == "healthy"
        and consumer_health["status"] == "healthy"
        and lifecycle_status == "healthy"
        else "unhealthy"
    )

    return {
        "status": overall_status,
        "shutting_down": lifecycle_manager.is_shutting_down,
        "ready": (
            database_status == "healthy"
            and _readiness_payload()["status"] == "ready"
        ),
        "checks": {
            "database": {
                "status": database_status,
                "error": database_error,
            },
            "consumer": consumer_health,
            "lifecycle": {
                "status": lifecycle_status,
                "shutting_down": lifecycle_manager.is_shutting_down,
            },
        },
    }


@router.get("/livez")
async def live_check():
    """Liveness endpoint for process-level status."""
    return {
        "status": (
            "alive"
            if not lifecycle_manager.is_shutting_down
            else "shutting_down"
        ),
        "shutting_down": lifecycle_manager.is_shutting_down,
    }


@router.get("/readyz")
async def readiness_check():
    """Readiness endpoint for operational checks."""
    database_status = "healthy"
    database_error = None
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(select(1))
    except Exception as exc:
        database_status = "unhealthy"
        database_error = str(exc)

    payload = _readiness_payload()
    payload["checks"]["database"] = {
        "status": database_status,
        "error": database_error,
    }
    if database_status != "healthy":
        payload["status"] = "not_ready"
    status_code = 200 if payload["status"] == "ready" else 503
    return JSONResponse(content=payload, status_code=status_code)


@router.get("/info", dependencies=[Depends(require_viewer_role)])
async def app_info(request: Request):
    """Application information endpoint."""
    return {
        "title": request.app.title,
        "description": request.app.description,
        "healthy": lifecycle_manager.is_healthy,
        "shutting_down": lifecycle_manager.is_shutting_down,
    }


@router.post("/auth/token")
async def create_access_token_exchange(
    authenticated_key: ApiKey = Depends(require_viewer_role),
):
    """Exchange a valid credential for a short-lived bearer token."""
    token, expires_at = issue_access_token(authenticated_key)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": expires_at.isoformat(),
        "owner_name": authenticated_key.owner_name,
        "role": authenticated_key.role,
    }


@router.post("/auth/ws-token")
async def create_websocket_session_token(
    authenticated_key: ApiKey = Depends(require_viewer_role),
):
    """Exchange a valid credential for a short-lived WebSocket token."""
    token, expires_at = build_ws_session_token(authenticated_key)
    return {
        "session_token": token,
        "expires_at": expires_at.isoformat(),
        "owner_name": authenticated_key.owner_name,
        "role": authenticated_key.role,
    }


@router.post("/auth/keys")
async def create_api_key(
    owner_name: str,
    role: str = Query("viewer", pattern="^(admin|viewer)$"),
    ttl_days: int = Query(API_KEY_DEFAULT_TTL_DAYS, ge=1, le=3650),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Create a new API Key (requires an existing Admin API key)."""
    del creator_api_key
    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)
    expires_at = build_api_key_expiration(ttl_days)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            new_key = ApiKey(
                key_hash=key_hash,
                owner_name=owner_name,
                role=role,
                is_active=True,
                expires_at=expires_at,
            )
            session.add(new_key)
            await session.flush()
            return _api_key_response(
                new_key,
                raw_key,
                "Store this key securely. It cannot be retrieved again.",
            )


@router.post("/auth/keys/{key_id}/rotate")
async def rotate_api_key(
    key_id: int,
    ttl_days: int = Query(API_KEY_DEFAULT_TTL_DAYS, ge=1, le=3650),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Rotate an existing API key and revoke the previous credential."""
    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)
    expires_at = build_api_key_expiration(ttl_days)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(ApiKey).where(ApiKey.id == key_id)
            )
            existing_key = result.scalar_one_or_none()
            if existing_key is None:
                raise HTTPException(
                    status_code=404, detail="API key not found"
                )
            if not is_api_key_usable(existing_key):
                raise HTTPException(
                    status_code=409,
                    detail="API key is not active and cannot be rotated",
                )

            existing_key.is_active = False
            existing_key.revoked_at = _utc_now()

            replacement_key = ApiKey(
                key_hash=key_hash,
                owner_name=existing_key.owner_name,
                role=existing_key.role,
                is_active=True,
                expires_at=expires_at,
            )
            session.add(replacement_key)
            await session.flush()

            response = _api_key_response(
                replacement_key,
                raw_key,
                (
                    "Store this rotated key securely. "
                    "The previous key is revoked."
                ),
            )
            response["rotated_from_key_id"] = existing_key.id
            response["rotated_by_key_id"] = creator_api_key.id
            return response


@router.post("/auth/keys/{key_id}/revoke")
async def revoke_api_key(
    key_id: int,
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Revoke an API key so it can no longer authenticate."""
    if key_id == creator_api_key.id:
        raise HTTPException(
            status_code=400,
            detail="Refusing to revoke the currently authenticated admin key.",
        )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(ApiKey).where(ApiKey.id == key_id)
            )
            existing_key = result.scalar_one_or_none()
            if existing_key is None:
                raise HTTPException(
                    status_code=404, detail="API key not found"
                )
            if not is_api_key_usable(existing_key):
                raise HTTPException(
                    status_code=409,
                    detail="API key is already inactive, expired, or revoked",
                )

            existing_key.is_active = False
            existing_key.revoked_at = _utc_now()
            return {
                "revoked_key_id": existing_key.id,
                "owner_name": existing_key.owner_name,
                "role": existing_key.role,
                "revoked_at": existing_key.revoked_at.isoformat(),
            }


@router.post("/auth/bootstrap")
async def bootstrap_admin_key(
    owner_name: str,
    ttl_days: int = Query(API_KEY_DEFAULT_TTL_DAYS, ge=1, le=3650),
):
    """Create the first admin API key.

    This bootstrap endpoint fails once any active key already exists.
    """
    if not ALLOW_BOOTSTRAP:
        raise HTTPException(
            status_code=403,
            detail=(
                "Bootstrap endpoint is disabled in this environment. "
                "For first-run local setup, start the app with ALLOW_BOOTSTRAP=true."
            ),
        )

    async with AsyncSessionLocal() as session:
        active_api_keys = await session.execute(
            select(ApiKey).where(
                ApiKey.is_active.is_(True),
                ApiKey.revoked_at.is_(None),
                or_(
                    ApiKey.expires_at.is_(None), ApiKey.expires_at > _utc_now()
                ),
            )
        )
        if active_api_keys.scalars().first() is not None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Cannot bootstrap because the system already has active API keys. "
                    "Create or rotate keys through /auth/keys instead."
                ),
            )

        raw_key = generate_new_api_key()
        key_hash = get_api_key_hash(raw_key)

        new_key = ApiKey(
            key_hash=key_hash,
            owner_name=owner_name,
            role="admin",
            is_active=True,
            expires_at=build_api_key_expiration(ttl_days),
        )
        session.add(new_key)
        await session.flush()
        await session.commit()

        return _api_key_response(
            new_key,
            raw_key,
            "Store this ADMIN key securely. It cannot be retrieved again.",
        )


@router.get("/admin/checkpoints")
async def get_consumer_checkpoints(
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return the persisted Kafka checkpoint state for operators."""
    del admin_api_key
    checkpoints = await list_consumer_checkpoints_snapshot()
    return {
        "checkpoints": checkpoints,
        "count": len(checkpoints),
    }


@router.get("/admin/dlq")
async def get_dead_letter_events(
    limit: int = Query(100, ge=1, le=1000),
    include_replayed: bool = False,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """List persisted DLQ records that are available for replay."""
    del admin_api_key
    dead_letters = await list_dead_letter_events(
        limit=limit,
        include_replayed=include_replayed,
    )
    return {
        "events": dead_letters,
        "count": len(dead_letters),
    }


@router.post("/admin/dlq/{dlq_event_id}/replay")
async def replay_dead_letter_event(
    dlq_event_id: int,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay a persisted DLQ record back through ingestion."""
    del admin_api_key
    try:
        return await replay_dead_letter_event_record(dlq_event_id)
    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"{exc}. Check GET /admin/dlq for currently available replay targets."
            ),
        ) from exc
