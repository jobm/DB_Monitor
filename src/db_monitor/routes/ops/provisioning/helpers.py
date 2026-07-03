"""Provisioning step lifecycle helpers and shared utilities."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from fastapi import HTTPException

from db_monitor import metrics as monitor_metrics
from db_monitor.routes.ops.lifecycle import _new_audit_entry

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
    steps_list: list[dict[str, object]] = cast(
        "list[dict[str, object]]", steps
    )
    for step in steps_list:
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
    audit_entries: list[dict[str, str]] = cast(
        "list[dict[str, str]]", list(job["audit"])
    )
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
    steps_list: list[dict[str, object]] = cast(
        "list[dict[str, object]]", list(job["steps"])
    )
    for step in steps_list:
        if step["name"] == step_name:
            step["result"] = dict(result)
            step["updated_at"] = datetime.now(timezone.utc).isoformat()
            return


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
    steps_list: list[dict[str, object]] = cast(
        "list[dict[str, object]]", list(job["steps"])
    )
    for step in steps_list:
        if step["status"] in {"pending", "in_progress"}:
            return step
        if retry_failed and step["status"] == "failed":
            return step
    return None
