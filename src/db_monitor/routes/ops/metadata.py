"""Guardrails, observability profile, and SLO policy endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from db_monitor.auth import require_admin_role
from db_monitor.core.config import settings
from db_monitor.core.models import ApiKey
from db_monitor.provisioning_local import (
    build_alert_routing_config,
    build_namespace_guardrails,
    build_observability_labels,
)

router = APIRouter()


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
        "window_days": settings.slo_rolling_window_days,
        "cohorts": settings.tenant_cohort_slo_policies,
        "count": len(settings.tenant_cohort_slo_policies),
    }
