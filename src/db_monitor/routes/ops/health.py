"""Health, liveness, readiness, and application-info endpoints."""

from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from db_monitor.auth import require_viewer_role
from db_monitor.core.config import settings
from db_monitor.core.responses import (
    AppInfoResponse,
    HealthResponse,
    ReadinessResponse,
)
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.ingestion.consumer import get_consumer_health
from db_monitor.lifecycle_manager import lifecycle_manager
from db_monitor.audit_log import audit_log_writer
from db_monitor.routes.utils import timestamp_age_seconds

router = APIRouter()


def readiness_payload() -> dict[str, object]:
    """Build readiness payload from lifecycle and consumer state.

    A ``SERVICE_ROLE=api`` pod runs no consumers, so consumer checks are
    reported as ``not_applicable`` and do not gate readiness; they would
    otherwise keep every API pod permanently unready.
    """
    consumer_health = get_consumer_health()
    consumer_applies = settings.service_role != "api"

    last_commit_age_seconds = timestamp_age_seconds(
        cast(str | None, consumer_health.get("last_commit_at"))
    )
    lag_total = int(cast(int, consumer_health.get("lag_total") or 0))
    dlq_total = int(cast(int, consumer_health.get("dlq_messages_total") or 0))

    consumer_ready = bool(
        consumer_health.get("running") and consumer_health.get("connected")
    )
    if (
        consumer_health.get("last_message_at")
        and consumer_health.get("last_commit_at") is None
        and lag_total > 0
    ):
        consumer_ready = False
    if (
        last_commit_age_seconds is not None
        and last_commit_age_seconds > settings.readiness_max_commit_age_seconds
    ):
        consumer_ready = False
    if lag_total > settings.readiness_max_consumer_lag:
        consumer_ready = False
    if dlq_total > settings.readiness_max_dlq_messages:
        consumer_ready = False

    lifecycle_ready = lifecycle_manager.is_healthy
    audit_log_status = (
        "healthy" if not audit_log_writer.has_flush_failure else "unhealthy"
    )
    consumer_status = "healthy" if consumer_ready else "unhealthy"
    if consumer_health.get("reconnecting"):
        consumer_status = "reconnecting"
    if not consumer_applies:
        consumer_status = "not_applicable"
    return {
        "status": (
            "ready"
            if lifecycle_ready and (consumer_ready or not consumer_applies)
            else "not_ready"
        ),
        "checks": {
            "consumer": {
                **consumer_health,
                "status": consumer_status,
                "last_commit_age_seconds": last_commit_age_seconds,
                "max_commit_age_seconds": settings.readiness_max_commit_age_seconds,
                "max_consumer_lag": settings.readiness_max_consumer_lag,
                "max_dlq_messages": settings.readiness_max_dlq_messages,
            },
            "lifecycle": {
                "status": "healthy" if lifecycle_ready else "unhealthy",
                "shutting_down": lifecycle_manager.is_shutting_down,
            },
            "audit_log": {
                "status": audit_log_status,
                "has_flush_failure": audit_log_writer.has_flush_failure,
                "last_flush_error": audit_log_writer.last_flush_error,
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
    consumer_applies = settings.service_role != "api"
    audit_log_status = (
        "healthy" if not audit_log_writer.has_flush_failure else "unhealthy"
    )
    lifecycle_status = (
        "healthy" if lifecycle_manager.is_healthy else "unhealthy"
    )
    consumer_ok = (
        not consumer_applies or consumer_health["status"] == "healthy"
    )
    overall_status = (
        "healthy"
        if database_status == "healthy"
        and consumer_ok
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
            "audit_log": {
                "status": audit_log_status,
                "has_flush_failure": audit_log_writer.has_flush_failure,
                "last_flush_error": audit_log_writer.last_flush_error,
            },
            "consumer": (
                consumer_health
                if consumer_applies
                else {**consumer_health, "status": "not_applicable"}
            ),
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
