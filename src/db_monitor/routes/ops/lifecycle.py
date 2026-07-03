"""Customer lifecycle management endpoints and helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import select

from db_monitor.auth import require_admin_role
from db_monitor.core.models import ApiKey, CustomerLifecycleState
from db_monitor.extensions import AsyncSessionLocal

router = APIRouter()

CUSTOMER_LIFECYCLE_STATE: dict[str, dict[str, object]] = {}


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


async def _load_customer_lifecycle_state(
    customer_id: str,
) -> dict[str, object] | None:
    """Load one lifecycle record and refresh the compatibility cache."""
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
