"""Provisioning job CRUD route endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from db_monitor.auth import require_admin_role
from db_monitor.core.models import ApiKey
from db_monitor.routes.ops.lifecycle import (
    _new_audit_entry,
    _require_customer_scope,
)
from db_monitor.routes.ops.provisioning.helpers import (
    PROVISIONING_STEPS,
    _append_job_audit,
    _default_step_state,
    _update_step_state,
)
from db_monitor.routes.ops.provisioning.persistence import (
    _load_customer_provision_jobs_snapshot,
    _persist_customer_provision_job,
)

router = APIRouter()


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
