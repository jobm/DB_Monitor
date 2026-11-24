"""Helper functions for API key management and JWT secret lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, cast

from fastapi import HTTPException
from sqlalchemy import select

from db_monitor.core.models import ApiKey, CustomerJWTSecretState
from db_monitor.routes.utils import utc_now


def api_key_response(api_key: ApiKey, raw_key: str, message: str) -> dict:
    """Build a standard API key creation or rotation response."""
    return {
        "api_key": f"{api_key.id}.{raw_key}",
        "owner_name": api_key.owner_name,
        "role": api_key.role,
        "tenant_id": api_key.tenant_id,
        "expires_at": (
            api_key.expires_at.isoformat() if api_key.expires_at else None
        ),
        "message": message,
    }


def api_key_status(api_key: ApiKey) -> str:
    """Return a stable operator-facing status for an API key."""
    if api_key.revoked_at is not None:
        return "revoked"
    if api_key.expires_at is not None and api_key.expires_at <= utc_now():
        return "expired"
    if not api_key.is_active:
        return "inactive"
    return "active"


def serialize_api_key_summary(
    api_key: ApiKey,
    current_api_key_id: int,
) -> dict[str, object]:
    """Serialize API key inventory rows for admin tooling."""
    status = api_key_status(api_key)
    return {
        "id": api_key.id,
        "owner_name": api_key.owner_name,
        "role": api_key.role,
        "tenant_id": api_key.tenant_id,
        "status": status,
        "is_active": api_key.is_active,
        "created_at": (
            api_key.created_at.isoformat() if api_key.created_at else None
        ),
        "expires_at": (
            api_key.expires_at.isoformat() if api_key.expires_at else None
        ),
        "revoked_at": (
            api_key.revoked_at.isoformat() if api_key.revoked_at else None
        ),
        "current_authenticated": api_key.id == current_api_key_id,
        "can_rotate": status == "active",
        "can_revoke": status == "active" and api_key.id != current_api_key_id,
    }


def _utc_now_iso() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


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

    if customer_header is None or not customer_header.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header is required for mutating "
                "admin operations."
            ),
        )

    normalized_scope = customer_header.strip()
    if normalized_scope != normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "X-DBM-Customer-ID header must match the customer_id "
                "path parameter."
            ),
        )

    return normalized_scope


def _new_customer_jwt_state() -> dict[str, object]:
    """Create one customer JWT secret state record."""
    return {
        "active_secret": None,
        "next_secret": None,
        "previous_active_secret": None,
        "generation": 0,
        "last_rotated_at": None,
        "last_recovered_at": None,
        "audit": [],
    }


def _get_customer_jwt_state(
    customer_jwt_state: dict[str, dict[str, object]],
    customer_id: str,
) -> dict[str, object]:
    """Return mutable JWT secret state for one customer scope."""
    return customer_jwt_state.setdefault(
        customer_id,
        _new_customer_jwt_state(),
    )


def _secret_fingerprint(secret_value: str | None) -> str | None:
    """Return a non-sensitive fingerprint for operator visibility."""
    if secret_value is None:
        return None
    if len(secret_value) <= 8:
        return secret_value
    return f"{secret_value[:4]}...{secret_value[-4:]}"


def _serialize_customer_jwt_state(
    customer_id: str,
    state: dict[str, object],
) -> dict[str, object]:
    """Render JWT state without exposing raw secret material."""
    active_secret = state.get("active_secret")
    next_secret = state.get("next_secret")
    previous_active_secret = state.get("previous_active_secret")
    audit_entries = cast(
        list[dict[str, object]], state.get("audit") or []
    )
    last_action = None
    if audit_entries:
        last_action = audit_entries[-1].get("action")
    return {
        "customer_id": customer_id,
        "generation": int(cast(int, state.get("generation") or 0)),
        "last_rotated_at": state.get("last_rotated_at"),
        "last_recovered_at": state.get("last_recovered_at"),
        "active_fingerprint": _secret_fingerprint(
            cast(str | None, active_secret)
        ),
        "next_fingerprint": _secret_fingerprint(
            cast(str | None, next_secret)
        ),
        "has_recovery_secret": previous_active_secret is not None,
        "audit_count": len(audit_entries),
        "last_action": last_action,
    }


def _sync_customer_jwt_state_cache(
    customer_jwt_state: dict[str, dict[str, object]],
    customer_id: str,
    state: dict[str, object],
) -> dict[str, object]:
    """Mirror durable JWT state into the local compatibility cache."""
    cached_state = dict(state)
    customer_jwt_state[customer_id] = cached_state
    return cached_state


def _row_to_customer_jwt_state(
    record: CustomerJWTSecretState,
) -> dict[str, object]:
    """Normalize one persisted JWT state row into a mutable dictionary."""
    return dict(record.state or _new_customer_jwt_state())


async def _load_customer_jwt_state_row(
    session,
    customer_id: str,
) -> CustomerJWTSecretState | None:
    """Load one persisted customer JWT state row."""
    result = await session.execute(
        select(CustomerJWTSecretState).where(
            CustomerJWTSecretState.customer_id == customer_id,
        )
    )
    return cast(
        Optional[CustomerJWTSecretState],
        result.scalar_one_or_none(),
    )


async def _persist_customer_jwt_state(
    session,
    customer_id: str,
    state: dict[str, object],
) -> CustomerJWTSecretState:
    """Persist one customer JWT state payload and return the row."""
    record = await _load_customer_jwt_state_row(session, customer_id)
    payload = dict(state)
    if record is None:
        record = CustomerJWTSecretState(customer_id=customer_id, state=payload)
        session.add(record)
    else:
        record.state = payload
    return record


def _append_jwt_audit(
    state: dict[str, object],
    action: str,
    detail: str,
) -> None:
    """Append one customer JWT lifecycle audit event."""
    audit_entries = cast(
        list[dict[str, object]], state.get("audit") or []
    )
    audit_entries.append(
        {
            "action": action,
            "detail": detail,
            "timestamp": _utc_now_iso(),
        }
    )
    state["audit"] = audit_entries
