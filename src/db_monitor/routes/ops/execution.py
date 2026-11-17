"""Provisioning job execution endpoint."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.params import Param

from db_monitor.auth import require_customer_admin_scope
from db_monitor.core.models import ApiKey
from db_monitor.provisioning import (
    ProvisioningStepExecutionError,
    execute_provisioning_step,
)
from db_monitor.routes.ops.lifecycle import (
    _get_customer_lifecycle_record,
    _mark_customer_bootstrap_active,
    _persist_customer_lifecycle_state,
    _require_customer_scope,
    CUSTOMER_LIFECYCLE_STATE,
)
from db_monitor.routes.ops.provisioning import (
    _append_job_audit,
    _load_customer_provision_jobs_snapshot,
    _merge_job_control_plane_metadata,
    _next_executable_step,
    _persist_customer_provision_job,
    _set_step_result,
    _update_step_state,
)

router = APIRouter()


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
    admin_api_key: ApiKey = Depends(require_customer_admin_scope),
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
                _get_customer_lifecycle_record(scope)  # type: ignore
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
