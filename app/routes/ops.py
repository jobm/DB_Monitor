"""Operational health/info/admin endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from auth import require_admin_role, require_viewer_role
from core.config import (
    READINESS_MAX_COMMIT_AGE_SECONDS,
    READINESS_MAX_CONSUMER_LAG,
    READINESS_MAX_DLQ_MESSAGES,
)
from core.db import AsyncSessionLocal
from core.lifecycle import lifecycle_manager
from ingestion.consumer import (
    get_consumer_health,
    list_consumer_checkpoints_snapshot,
    list_dead_letter_events as list_dead_letter_records,
    replay_dead_letter_event_record,
    replay_dead_letter_event_records,
)
from models import ApiKey
from response_models import AppInfoResponse, HealthResponse, ReadinessResponse
from .utils import timestamp_age_seconds

router = APIRouter()


def readiness_payload() -> dict[str, object]:
    """Build readiness payload from lifecycle and consumer state."""
    consumer_health = get_consumer_health()

    last_commit_age_seconds = timestamp_age_seconds(
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


@router.get("/health", response_model=HealthResponse)
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
            and readiness_payload()["status"] == "ready"
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


@router.get("/livez", response_model=HealthResponse)
async def live_check():
    """Liveness endpoint for process-level status."""
    return {
        "status": "alive"
        if not lifecycle_manager.is_shutting_down
        else "shutting_down",
        "shutting_down": lifecycle_manager.is_shutting_down,
    }


@router.get("/readyz", response_model=ReadinessResponse)
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

    payload = readiness_payload()
    payload["checks"]["database"] = {
        "status": database_status,
        "error": database_error,
    }
    if database_status != "healthy":
        payload["status"] = "not_ready"
    status_code = 200 if payload["status"] == "ready" else 503
    return JSONResponse(content=payload, status_code=status_code)


@router.get(
    "/info",
    dependencies=[Depends(require_viewer_role)],
    response_model=AppInfoResponse,
)
async def app_info(request: Request):
    """Application information endpoint."""
    return {
        "title": request.app.title,
        "description": request.app.description,
        "healthy": lifecycle_manager.is_healthy,
        "shutting_down": lifecycle_manager.is_shutting_down,
    }


@router.get("/admin/checkpoints")
async def get_consumer_checkpoints(
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return persisted consumer checkpoint state for operators."""
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
    dead_letters = await list_dead_letter_records(
        limit=limit,
        include_replayed=include_replayed,
    )
    return {
        "events": dead_letters,
        "count": len(dead_letters),
    }


@router.post("/admin/dlq/replay")
async def replay_dead_letter_events(
    limit: int = Query(100, ge=1, le=1000),
    include_replayed: bool = False,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay multiple persisted DLQ records through ingestion."""
    del admin_api_key
    return await replay_dead_letter_event_records(
        limit=limit,
        include_replayed=include_replayed,
    )


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
                f"{exc}. Check GET /admin/dlq for currently available "
                "replay targets."
            ),
        ) from exc
