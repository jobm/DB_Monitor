"""Operational health/info/admin endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.params import Param
from fastapi.responses import JSONResponse
from sqlalchemy import select

from db_monitor.auth import require_admin_role, require_viewer_role
from db_monitor.core.config import (
    READINESS_MAX_COMMIT_AGE_SECONDS,
    READINESS_MAX_CONSUMER_LAG,
    READINESS_MAX_DLQ_MESSAGES,
    SLO_ROLLING_WINDOW_DAYS,
    TENANT_COHORT_SLO_POLICIES,
)
from db_monitor.core.db import AsyncSessionLocal
from db_monitor.core.lifecycle import lifecycle_manager
from db_monitor import metrics as monitor_metrics
from db_monitor.provisioning import (
    ProvisioningStepExecutionError,
    execute_provisioning_step,
)
from db_monitor.provisioning_local import (
    build_alert_routing_config,
    build_namespace_guardrails,
    build_observability_labels,
    register_local_step_providers,
)
from db_monitor.ingestion.consumer import (
    get_consumer_health,
    list_consumer_checkpoints_snapshot,
    list_dead_letter_events as list_dead_letter_records,
    replay_dead_letter_event_record,
    replay_dead_letter_event_records,
)
from core.models import ApiKey, CustomerLifecycleState, CustomerProvisionJob
from core.responses import AppInfoResponse, HealthResponse, ReadinessResponse
from .utils import timestamp_age_seconds
from db_monitor.audit_log import audit_log_writer
from repositories.audit_log_spill import list_audit_log_spill_snapshot
from repositories.audit_log_replay import replay_spill_entries

router = APIRouter()
register_local_step_providers()
CUSTOMER_LIFECYCLE_STATE: dict[str, dict[str, object]] = {}
CUSTOMER_PROVISION_JOBS: dict[str, dict[str, dict[str, object]]] = {}
PROVISIONING_STEPS = [
    "register_customer",
    "generate_namespace",
    "provision_postgres",
    "provision_broker_namespace",
    "create_secrets_and_config",
    "deploy_monitor_services",
    "run_migrations_and_health_checks",
    "bootstrap_and_activate",
]


def _require_customer_scope(
    customer_id: str,
    customer_header: str | None,
) -> str:
    """Validate and normalize customer scope for mutating admin actions."""
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    customer_scope = customer_header
    if customer_scope is None or not customer_scope.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    normalized_scope = customer_scope.strip()
    if normalized_scope != normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header must match the customer_id "
                "path parameter."
            ),
        )

    return normalized_scope


def _get_customer_lifecycle_record(customer_id: str) -> dict[str, object]:
    """Return existing lifecycle state or initialize a default record."""
    existing = CUSTOMER_LIFECYCLE_STATE.get(customer_id)
    if existing is not None:
        return dict(existing)

    return {
        "customer_id": customer_id,
        "status": "active",
        "last_action": "none",
        "target_version": None,
        "reason": None,
    }


def _new_audit_entry(
    action: str,
    actor: str,
    detail: str,
) -> dict[str, str]:
    """Build one timestamped provisioning audit entry."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "actor": actor,
        "detail": detail,
    }


def _default_step_state(step_name: str) -> dict[str, object]:
    """Return default provisioning step metadata."""
    return {
        "name": step_name,
        "status": "pending",
        "note": None,
        "attempts": 0,
        "started_at": None,
        "completed_at": None,
        "last_error": None,
        "duration_seconds": None,
        "result": None,
        "updated_at": None,
    }


def _get_customer_provision_jobs(
    customer_id: str,
) -> dict[str, dict[str, object]]:
    """Return the in-memory provisioning job map for one customer."""
    return CUSTOMER_PROVISION_JOBS.setdefault(customer_id, {})


async def _load_customer_lifecycle_state(
    customer_id: str,
) -> dict[str, object] | None:
    """Load one durable lifecycle record and refresh the compatibility cache."""
    cached = CUSTOMER_LIFECYCLE_STATE.get(customer_id)
    if cached is not None:
        return dict(cached)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CustomerLifecycleState).where(
                CustomerLifecycleState.customer_id == customer_id,
            )
        )
        record = result.scalar_one_or_none()

    if record is None:
        return None

    state = dict(record.state or {})
    CUSTOMER_LIFECYCLE_STATE[customer_id] = dict(state)
    return state


async def _persist_customer_lifecycle_state(
    state: dict[str, object],
) -> dict[str, object]:
    """Persist one lifecycle record and refresh the compatibility cache."""
    customer_id = str(state["customer_id"])
    payload = dict(state)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(CustomerLifecycleState).where(
                    CustomerLifecycleState.customer_id == customer_id,
                )
            )
            record = result.scalar_one_or_none()
            if record is None:
                session.add(
                    CustomerLifecycleState(
                        customer_id=customer_id,
                        state=payload,
                    )
                )
            else:
                record.state = payload

    CUSTOMER_LIFECYCLE_STATE[customer_id] = dict(payload)
    return payload


async def _load_customer_provision_jobs_snapshot(
    customer_id: str,
) -> dict[str, dict[str, object]]:
    """Load all durable provisioning jobs for one customer."""
    cached = CUSTOMER_PROVISION_JOBS.get(customer_id)
    if cached:
        return {job_id: dict(job) for job_id, job in cached.items()}

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(CustomerProvisionJob).where(
                CustomerProvisionJob.customer_id == customer_id,
            )
        )
        records = result.scalars().all()

    jobs: dict[str, dict[str, object]] = {}
    for record in records:
        state = dict(record.state or {})
        if state:
            jobs[str(record.job_id)] = state

    if jobs:
        CUSTOMER_PROVISION_JOBS[customer_id] = {
            job_id: dict(job) for job_id, job in jobs.items()
        }

    return jobs


async def _load_customer_provision_job(
    customer_id: str,
    job_id: str,
) -> dict[str, object] | None:
    """Load one durable provisioning job and refresh the cache."""
    jobs = await _load_customer_provision_jobs_snapshot(customer_id)
    job = jobs.get(job_id)
    if job is not None:
        return dict(job)
    return None


async def _persist_customer_provision_job(
    job: dict[str, object],
) -> dict[str, object]:
    """Persist one provisioning job and refresh the compatibility cache."""
    customer_id = str(job["customer_id"])
    job_id = str(job["job_id"])
    payload = dict(job)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(CustomerProvisionJob).where(
                    CustomerProvisionJob.job_id == job_id,
                )
            )
            record = result.scalar_one_or_none()
            if record is None:
                session.add(
                    CustomerProvisionJob(
                        job_id=job_id,
                        customer_id=customer_id,
                        state=payload,
                    )
                )
            else:
                record.customer_id = customer_id
                record.state = payload

    jobs = CUSTOMER_PROVISION_JOBS.setdefault(customer_id, {})
    jobs[job_id] = dict(payload)
    return payload


def _update_step_state(
    job: dict[str, object],
    step_name: str,
    step_status: str,
    note: str | None,
) -> None:
    """Update one provisioning step and recalculate overall job state."""
    steps = list(job["steps"])
    found_step = False
    duration_seconds: float | None = None
    for step in steps:
        if step["name"] != step_name:
            continue
        started_at_raw = step.get("started_at")
        started_at = None
        if isinstance(started_at_raw, str) and started_at_raw:
            started_at = datetime.fromisoformat(started_at_raw)

        step["status"] = step_status
        step["note"] = note
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        step["updated_at"] = datetime.now(timezone.utc).isoformat()
        if step_status == "in_progress":
            attempts = int(step.get("attempts") or 0)
            step["attempts"] = attempts + 1
            step["started_at"] = now_iso
            step["completed_at"] = None
            step["last_error"] = None
            step["duration_seconds"] = None
            step["result"] = None
        elif step_status == "completed":
            step["completed_at"] = now_iso
            step["last_error"] = None
            if started_at is not None:
                duration_seconds = max(
                    0.0,
                    (now - started_at).total_seconds(),
                )
                step["duration_seconds"] = duration_seconds
        elif step_status == "failed":
            step["completed_at"] = now_iso
            step["last_error"] = note
            if started_at is not None:
                duration_seconds = max(
                    0.0,
                    (now - started_at).total_seconds(),
                )
                step["duration_seconds"] = duration_seconds

        try:
            monitor_metrics.provisioning_step_transitions_total.labels(
                customer_id=str(job["customer_id"]),
                step=step_name,
                status=step_status,
            ).inc()
            if duration_seconds is not None:
                outcome = (
                    "success" if step_status == "completed" else "failure"
                )
                monitor_metrics.provisioning_step_duration_seconds.labels(
                    customer_id=str(job["customer_id"]),
                    step=step_name,
                    outcome=outcome,
                ).observe(duration_seconds)
        except Exception:
            pass

        found_step = True
        break

    if not found_step:
        raise HTTPException(
            status_code=404,
            detail=f"Provisioning step '{step_name}' was not found.",
        )

    failed_steps = [
        step for step in steps if step["status"] == "failed"
    ]
    if failed_steps:
        job["status"] = "failed"
    elif all(step["status"] == "completed" for step in steps):
        job["status"] = "completed"
    else:
        job["status"] = "in_progress"

    job["steps"] = steps
    job["updated_at"] = datetime.now(timezone.utc).isoformat()


def _append_job_audit(
    job: dict[str, object],
    action: str,
    detail: str,
    actor: str = "system",
) -> None:
    """Append one audit entry to a provisioning job."""
    audit_entries = list(job["audit"])
    audit_entries.append(
        _new_audit_entry(
            action=action,
            actor=actor,
            detail=detail,
        )
    )
    job["audit"] = audit_entries


def _set_step_result(
    job: dict[str, object],
    step_name: str,
    result: dict[str, object],
) -> None:
    """Store adapter execution output on one provisioning step."""
    for step in list(job["steps"]):
        if step["name"] == step_name:
            step["result"] = dict(result)
            step["updated_at"] = datetime.now(timezone.utc).isoformat()
            return


def _mark_customer_bootstrap_active(customer_id: str) -> None:
    """Mark lifecycle state active once bootstrap step is completed."""
    record = _get_customer_lifecycle_record(customer_id)
    record.update(
        {
            "status": "active",
            "last_action": "bootstrap_activate",
            "reason": None,
        }
    )
    CUSTOMER_LIFECYCLE_STATE[customer_id] = dict(record)
    return record


def _merge_job_control_plane_metadata(
    job: dict[str, object],
    step_result: dict[str, object],
) -> None:
    """Persist guardrail/observability/alert metadata from step outputs."""
    labels = step_result.get("observability_labels")
    if isinstance(labels, dict):
        job["observability_labels"] = dict(labels)

    alert_routing = step_result.get("alert_routing")
    if isinstance(alert_routing, dict):
        job["alert_routing"] = dict(alert_routing)

    guardrails = step_result.get("guardrails")
    if isinstance(guardrails, dict):
        job["guardrails"] = dict(guardrails)


def _next_executable_step(
    job: dict[str, object],
    retry_failed: bool,
) -> dict[str, object] | None:
    """Return the next provisioning step that can be executed."""
    for step in list(job["steps"]):
        if step["status"] in {"pending", "in_progress"}:
            return step
        if retry_failed and step["status"] == "failed":
            return step
    return None


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
            "audit_log": {
                "status": "healthy" if not audit_log_writer.has_flush_failure else "unhealthy",
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
            "audit_log": {
                "status": "healthy" if not audit_log_writer.has_flush_failure else "unhealthy",
                "has_flush_failure": audit_log_writer.has_flush_failure,
                "last_flush_error": audit_log_writer.last_flush_error,
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


@router.get("/admin/audit-log/spill")
async def get_audit_log_spill(
    limit: int = Query(100, ge=1, le=1000),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return recent persisted spilled audit log entries for operator inspection."""
    del admin_api_key
    rows = await list_audit_log_spill_snapshot(limit=limit)
    return {"events": rows, "count": len(rows)}


@router.post("/admin/audit-log/spill/replay")
async def replay_audit_log_spill(
    limit: int = Query(100, ge=1, le=1000),
    ids: list[int] | None = Query(default=None),
    actor: str | None = Query(default=None),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay persisted spilled audit log entries into the primary audit table.

    - Provide `ids` to replay specific spill records.
    - Provide `actor` to record who initiated the replay.
    """
    del admin_api_key
    result = await replay_spill_entries(limit=limit, ids=ids, actor=actor)
    return {"result": result}


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
    customer_id: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay multiple persisted DLQ records through ingestion."""
    del admin_api_key
    customer_scope = None if isinstance(customer_id, Param) else customer_id
    if customer_scope is None or not customer_scope.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    customer_scope = customer_scope.strip()
    result = await replay_dead_letter_event_records(
        limit=limit,
        include_replayed=include_replayed,
    )
    result["customer_id"] = customer_scope
    return result


@router.post("/admin/dlq/{dlq_event_id}/replay")
async def replay_dead_letter_event(
    dlq_event_id: int,
    customer_id: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Replay a persisted DLQ record back through ingestion."""
    del admin_api_key
    customer_scope = None if isinstance(customer_id, Param) else customer_id
    if customer_scope is None or not customer_scope.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    customer_scope = customer_scope.strip()
    try:
        result = await replay_dead_letter_event_record(dlq_event_id)
        result["customer_id"] = customer_scope
        return result
    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"{exc}. Check GET /admin/dlq for currently available "
                "replay targets."
            ),
        ) from exc


@router.get("/admin/customers/{customer_id}/lifecycle")
async def get_customer_lifecycle_state(
    customer_id: str,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return lifecycle state for one customer cell."""
    del admin_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    record = await _load_customer_lifecycle_state(normalized_customer_id)
    if record is None:
        record = _get_customer_lifecycle_record(normalized_customer_id)
    return record


@router.post("/admin/customers/{customer_id}/suspend")
async def suspend_customer_cell(
    customer_id: str,
    reason: str | None = Query(None),
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Suspend one customer cell in the control plane."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    record = await _load_customer_lifecycle_state(scope)
    if record is None:
        record = _get_customer_lifecycle_record(scope)
    record.update(
        {
            "status": "suspended",
            "last_action": "suspend",
            "reason": reason,
        }
    )
    await _persist_customer_lifecycle_state(record)
    return record


@router.post("/admin/customers/{customer_id}/resume")
async def resume_customer_cell(
    customer_id: str,
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Resume one suspended customer cell in the control plane."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    record = await _load_customer_lifecycle_state(scope)
    if record is None:
        record = _get_customer_lifecycle_record(scope)
    record.update(
        {
            "status": "active",
            "last_action": "resume",
            "reason": None,
        }
    )
    await _persist_customer_lifecycle_state(record)
    return record


@router.post("/admin/customers/{customer_id}/upgrade")
async def upgrade_customer_cell(
    customer_id: str,
    target_version: str = Query(...),
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Register an upgrade target for one customer cell."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    normalized_version = target_version.strip()
    if not normalized_version:
        raise HTTPException(
            status_code=400,
            detail="target_version query parameter must not be empty.",
        )

    record = await _load_customer_lifecycle_state(scope)
    if record is None:
        record = _get_customer_lifecycle_record(scope)
    record.update(
        {
            "last_action": "upgrade",
            "target_version": normalized_version,
        }
    )
    await _persist_customer_lifecycle_state(record)
    return record


@router.post("/admin/customers/{customer_id}/provision-jobs")
async def start_customer_provision_job(
    customer_id: str,
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Create a provisioning orchestration job for one customer cell."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)

    job_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()
    job = {
        "job_id": job_id,
        "customer_id": scope,
        "status": "in_progress",
        "steps": [
            _default_step_state(step_name) for step_name in PROVISIONING_STEPS
        ],
        "audit": [
            _new_audit_entry(
                action="job_started",
                actor="admin",
                detail="Provisioning job created.",
            )
        ],
        "created_at": now,
        "updated_at": now,
    }

    await _persist_customer_provision_job(job)
    return job


@router.get("/admin/customers/{customer_id}/provision-jobs")
async def list_customer_provision_jobs(
    customer_id: str,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """List provisioning orchestration jobs for one customer."""
    del admin_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    jobs = await _load_customer_provision_jobs_snapshot(normalized_customer_id)
    sorted_jobs = sorted(
        jobs.values(),
        key=lambda job: str(job["created_at"]),
        reverse=True,
    )
    return {
        "customer_id": normalized_customer_id,
        "jobs": sorted_jobs,
        "count": len(sorted_jobs),
    }


@router.get("/admin/customers/{customer_id}/provision-jobs/{job_id}")
async def get_customer_provision_job(
    customer_id: str,
    job_id: str,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return one provisioning orchestration job by ID."""
    del admin_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    jobs = await _load_customer_provision_jobs_snapshot(normalized_customer_id)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Provisioning job '{job_id}' was not found.",
        )
    return job


@router.post(
    "/admin/customers/{customer_id}/provision-jobs/{job_id}/steps/{step_name}"
)
async def update_customer_provision_step(
    customer_id: str,
    job_id: str,
    step_name: str,
    step_status: str = Query(..., alias="status"),
    note: str | None = Query(None),
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Update one provisioning step and append an audit event."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)

    normalized_status = step_status.strip().lower()
    if normalized_status not in {"in_progress", "completed", "failed"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "status query parameter must be one of: "
                "in_progress, completed, failed."
            ),
        )

    jobs = await _load_customer_provision_jobs_snapshot(scope)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Provisioning job '{job_id}' was not found.",
        )

    _update_step_state(job, step_name, normalized_status, note)
    detail_note = note or ""
    _append_job_audit(
        job,
        action="step_updated",
        actor="admin",
        detail=(
            f"Step '{step_name}' marked '{normalized_status}'. "
            f"{detail_note}".strip()
        ),
    )
    await _persist_customer_provision_job(job)
    return job


@router.post("/admin/customers/{customer_id}/provision-jobs/{job_id}/execute")
async def execute_customer_provision_job(
    customer_id: str,
    job_id: str,
    max_steps: int = Query(1, ge=1, le=20),
    retry_failed: bool = Query(False),
    execution_id: str | None = Query(None),
    fail_step: str | None = Query(None),
    note: str | None = Query(None),
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Execute up to N provisioning steps in sequence for one job."""
    del admin_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    retry_failed_value = False if isinstance(retry_failed, Param) else bool(
        retry_failed
    )
    execution_id_value = (
        None if isinstance(execution_id, Param) else execution_id
    )

    jobs = await _load_customer_provision_jobs_snapshot(scope)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"Provisioning job '{job_id}' was not found.",
        )

    normalized_execution_id = None
    if execution_id_value is not None:
        normalized_execution_id = execution_id_value.strip()
        if not normalized_execution_id:
            raise HTTPException(
                status_code=400,
                detail="execution_id query parameter must not be empty.",
            )

    if normalized_execution_id and (
        normalized_execution_id == job.get("last_execution_id")
    ):
        return {
            "job": job,
            "executed_steps": 0,
            "idempotent_replay": True,
        }

    is_retrying_failed_job = (
        job["status"] == "failed" and retry_failed_value
    )
    if job["status"] == "completed" or (
        job["status"] == "failed" and not is_retrying_failed_job
    ):
        _append_job_audit(
            job,
            action="execution_skipped",
            actor="system",
            detail=(
                "Execution skipped because the job is terminal "
                f"('{job['status']}')."
            ),
        )
        await _persist_customer_provision_job(job)
        return {
            "job": job,
            "executed_steps": 0,
            "idempotent_replay": False,
        }

    if normalized_execution_id:
        job["last_execution_id"] = normalized_execution_id
        job["last_execution_at"] = datetime.now(timezone.utc).isoformat()

    executed_steps = 0
    while executed_steps < max_steps:
        step = _next_executable_step(
            job,
            retry_failed=retry_failed_value,
        )
        if step is None:
            break

        step_name = str(step["name"])
        _update_step_state(
            job,
            step_name,
            "in_progress",
            note,
        )
        _append_job_audit(
            job,
            action="step_started",
            actor="system",
            detail=f"Step '{step_name}' entered in_progress.",
        )
        await _persist_customer_provision_job(job)

        if fail_step is not None and fail_step.strip() == step_name:
            _update_step_state(
                job,
                step_name,
                "failed",
                note or "Execution failed by control-plane simulation.",
            )
            _append_job_audit(
                job,
                action="step_failed",
                actor="system",
                detail=f"Step '{step_name}' failed during execution.",
            )
            await _persist_customer_provision_job(job)
            executed_steps += 1
            break

        try:
            step_result = await execute_provisioning_step(
                step_name=step_name,
                customer_id=scope,
                job_id=job_id,
                note=note,
            )
        except ProvisioningStepExecutionError as exc:
            _update_step_state(
                job,
                step_name,
                "failed",
                str(exc),
            )
            _append_job_audit(
                job,
                action="step_failed",
                actor="system",
                detail=f"Step '{step_name}' failed during execution: {exc}",
            )
            await _persist_customer_provision_job(job)
            executed_steps += 1
            break

        _update_step_state(
            job,
            step_name,
            "completed",
            str(step_result.get("detail") or note or "completed"),
        )
        _set_step_result(job, step_name, step_result)
        _merge_job_control_plane_metadata(job, step_result)
        if step_name == "bootstrap_and_activate":
            _mark_customer_bootstrap_active(scope)
            await _persist_customer_lifecycle_state(
                _get_customer_lifecycle_record(scope)
            )
        _append_job_audit(
            job,
            action="step_completed",
            actor="system",
            detail=f"Step '{step_name}' completed during execution.",
        )
        await _persist_customer_provision_job(job)
        executed_steps += 1

    await _persist_customer_provision_job(job)
    return {
        "job": job,
        "executed_steps": executed_steps,
        "idempotent_replay": False,
    }


@router.get("/admin/customers/{customer_id}/guardrails")
async def get_customer_guardrails(
    customer_id: str,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return generated namespace guardrail templates for one customer."""
    del admin_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    return {
        "customer_id": normalized_customer_id,
        "guardrails": build_namespace_guardrails(normalized_customer_id),
    }


@router.get("/admin/customers/{customer_id}/observability")
async def get_customer_observability_profile(
    customer_id: str,
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return observability labels and alert routing for one customer."""
    del admin_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    return {
        "customer_id": normalized_customer_id,
        "labels": build_observability_labels(normalized_customer_id),
        "alert_routing": build_alert_routing_config(
            normalized_customer_id
        ),
    }


@router.get("/admin/slo-policy")
async def get_slo_policy(
    admin_api_key: ApiKey = Depends(require_admin_role),
):
    """Return published tenant-cohort SLO and error-budget policy."""
    del admin_api_key
    return {
        "window_days": SLO_ROLLING_WINDOW_DAYS,
        "cohorts": TENANT_COHORT_SLO_POLICIES,
        "count": len(TENANT_COHORT_SLO_POLICIES),
    }
