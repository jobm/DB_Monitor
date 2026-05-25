"""Authentication and API key lifecycle endpoints."""

from __future__ import annotations

import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import or_, select

from db_monitor.auth import (
    build_access_token as issue_access_token,
)
from db_monitor.auth import (
    build_api_key_expiration,
    build_ws_session_token,
    generate_new_api_key,
    get_api_key_hash,
    is_api_key_usable,
    require_admin_role,
    require_viewer_role,
)
from db_monitor.core.config import ALLOW_BOOTSTRAP, API_KEY_DEFAULT_TTL_DAYS
from db_monitor.core.db import AsyncSessionLocal
from core.models import ApiKey
from core.responses import (
    AccessTokenExchangeResponse,
    WebSocketTokenExchangeResponse,
)
from .utils import utc_now

router = APIRouter()
CUSTOMER_JWT_SECRET_STATE: dict[str, dict[str, object]] = {}


def api_key_response(api_key: ApiKey, raw_key: str, message: str) -> dict:
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


def _get_customer_jwt_state(customer_id: str) -> dict[str, object]:
    """Return mutable JWT secret state for one customer scope."""
    return CUSTOMER_JWT_SECRET_STATE.setdefault(
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
    audit_entries = list(state.get("audit") or [])
    last_action = None
    if audit_entries:
        last_action = audit_entries[-1].get("action")
    return {
        "customer_id": customer_id,
        "generation": int(state.get("generation") or 0),
        "last_rotated_at": state.get("last_rotated_at"),
        "last_recovered_at": state.get("last_recovered_at"),
        "active_fingerprint": _secret_fingerprint(active_secret),
        "next_fingerprint": _secret_fingerprint(next_secret),
        "has_recovery_secret": previous_active_secret is not None,
        "audit_count": len(audit_entries),
        "last_action": last_action,
    }


def _append_jwt_audit(
    state: dict[str, object],
    action: str,
    detail: str,
) -> None:
    """Append one customer JWT lifecycle audit event."""
    audit_entries = list(state.get("audit") or [])
    audit_entries.append(
        {
            "action": action,
            "detail": detail,
            "timestamp": _utc_now_iso(),
        }
    )
    state["audit"] = audit_entries


def _initialize_customer_jwt_secrets(
    customer_id: str,
) -> dict[str, object]:
    """Initialize per-customer JWT active/next secret material."""
    state = _get_customer_jwt_state(customer_id)
    if state.get("active_secret") is None:
        state["active_secret"] = secrets.token_urlsafe(48)
        state["generation"] = 1
    if state.get("next_secret") is None:
        state["next_secret"] = secrets.token_urlsafe(48)
    if state.get("last_rotated_at") is None:
        state["last_rotated_at"] = _utc_now_iso()
    _append_jwt_audit(
        state,
        action="bootstrap",
        detail="Customer JWT active and next secrets initialized.",
    )
    return state


@router.post("/auth/token", response_model=AccessTokenExchangeResponse)
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


@router.post(
    "/auth/ws-token",
    response_model=WebSocketTokenExchangeResponse,
)
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
            return api_key_response(
                new_key,
                raw_key,
                "Store this key securely. It cannot be retrieved again.",
            )


@router.get("/auth/keys")
async def list_api_keys(
    include_inactive: bool = False,
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """List API keys for operator inventory and lifecycle actions."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ApiKey).order_by(ApiKey.created_at.desc(), ApiKey.id.desc())
        )
        api_keys = result.scalars().all()

    serialized = [
        serialize_api_key_summary(api_key, creator_api_key.id)
        for api_key in api_keys
    ]
    if not include_inactive:
        serialized = [row for row in serialized if row["status"] == "active"]

    return {
        "keys": serialized,
        "count": len(serialized),
        "current_api_key_id": creator_api_key.id,
    }


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
                    status_code=404,
                    detail="API key not found",
                )
            if not is_api_key_usable(existing_key):
                raise HTTPException(
                    status_code=409,
                    detail="API key is not active and cannot be rotated",
                )

            existing_key.is_active = False
            existing_key.revoked_at = utc_now()

            replacement_key = ApiKey(
                key_hash=key_hash,
                owner_name=existing_key.owner_name,
                role=existing_key.role,
                is_active=True,
                expires_at=expires_at,
            )
            session.add(replacement_key)
            await session.flush()

            response = api_key_response(
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
            detail=(
                "Refusing to revoke the currently authenticated "
                "admin key."
            ),
        )

    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(ApiKey).where(ApiKey.id == key_id)
            )
            existing_key = result.scalar_one_or_none()
            if existing_key is None:
                raise HTTPException(
                    status_code=404,
                    detail="API key not found",
                )
            if not is_api_key_usable(existing_key):
                raise HTTPException(
                    status_code=409,
                    detail="API key is already inactive, expired, or revoked",
                )

            existing_key.is_active = False
            existing_key.revoked_at = utc_now()
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
    """Create the first admin API key."""
    if not ALLOW_BOOTSTRAP:
        raise HTTPException(
            status_code=403,
            detail=(
                "Bootstrap endpoint is disabled in this environment. "
                "For first-run local setup, start the app with "
                "ALLOW_BOOTSTRAP=true."
            ),
        )

    async with AsyncSessionLocal() as session:
        active_api_keys = await session.execute(
            select(ApiKey).where(
                ApiKey.is_active.is_(True),
                ApiKey.revoked_at.is_(None),
                or_(
                    ApiKey.expires_at.is_(None),
                    ApiKey.expires_at > utc_now(),
                ),
            )
        )
        if active_api_keys.scalars().first() is not None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Cannot bootstrap because the system already has "
                    "active API keys. "
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

        return api_key_response(
            new_key,
            raw_key,
            "Store this ADMIN key securely. It cannot be retrieved again.",
        )


@router.post("/admin/customers/{customer_id}/auth/bootstrap")
async def bootstrap_customer_auth_scope(
    customer_id: str,
    owner_suffix: str = Query("bootstrap-admin"),
    ttl_days: int = Query(API_KEY_DEFAULT_TTL_DAYS, ge=1, le=3650),
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Bootstrap one customer scope for API key and JWT secret lifecycle."""
    del creator_api_key
    scope = _require_customer_scope(customer_id, customer_header)

    normalized_owner_suffix = owner_suffix.strip()
    if not normalized_owner_suffix:
        raise HTTPException(
            status_code=400,
            detail="owner_suffix query parameter must not be empty.",
        )

    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)
    expires_at = build_api_key_expiration(ttl_days)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            new_key = ApiKey(
                key_hash=key_hash,
                owner_name=f"{scope}:{normalized_owner_suffix}",
                role="admin",
                is_active=True,
                expires_at=expires_at,
            )
            session.add(new_key)
            await session.flush()

            response = api_key_response(
                new_key,
                raw_key,
                (
                    "Store this customer admin key securely. "
                    "It cannot be retrieved again."
                ),
            )
            jwt_state = _initialize_customer_jwt_secrets(scope)
            response["customer_id"] = scope
            response["jwt_state"] = _serialize_customer_jwt_state(
                scope,
                jwt_state,
            )
            return response


@router.get("/admin/customers/{customer_id}/auth/jwt")
async def get_customer_jwt_state(
    customer_id: str,
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Return customer JWT secret lifecycle metadata for operators."""
    del creator_api_key
    normalized_customer_id = customer_id.strip()
    if not normalized_customer_id:
        raise HTTPException(
            status_code=400,
            detail="customer_id path parameter must not be empty.",
        )

    state = _get_customer_jwt_state(normalized_customer_id)
    if state.get("active_secret") is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Customer JWT secrets are not initialized. "
                "Run POST /admin/customers/{customer_id}/auth/bootstrap "
                "first."
            ),
        )

    return _serialize_customer_jwt_state(normalized_customer_id, state)


@router.post("/admin/customers/{customer_id}/auth/jwt/rotate")
async def rotate_customer_jwt_secrets(
    customer_id: str,
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Rotate JWT active/next secret material for one customer scope."""
    del creator_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    state = _get_customer_jwt_state(scope)

    if state.get("active_secret") is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Customer JWT secrets are not initialized. "
                "Run POST /admin/customers/{customer_id}/auth/bootstrap "
                "first."
            ),
        )

    previous_active_secret = state.get("active_secret")
    promoted_secret = state.get("next_secret")
    if promoted_secret is None:
        promoted_secret = secrets.token_urlsafe(48)

    state["previous_active_secret"] = previous_active_secret
    state["active_secret"] = promoted_secret
    state["next_secret"] = secrets.token_urlsafe(48)
    state["generation"] = int(state.get("generation") or 0) + 1
    state["last_rotated_at"] = _utc_now_iso()
    _append_jwt_audit(
        state,
        action="rotate",
        detail="Customer JWT active/next secret material rotated.",
    )

    return {
        "customer_id": scope,
        "message": "Customer JWT secrets rotated.",
        "jwt_state": _serialize_customer_jwt_state(scope, state),
    }


@router.post("/admin/customers/{customer_id}/auth/jwt/recover")
async def recover_customer_jwt_secrets(
    customer_id: str,
    customer_header: str | None = Header(
        default=None,
        alias="X-DBM-Customer-ID",
    ),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Recover prior active JWT secret material for one customer scope."""
    del creator_api_key
    scope = _require_customer_scope(customer_id, customer_header)
    state = _get_customer_jwt_state(scope)

    previous_active_secret = state.get("previous_active_secret")
    if previous_active_secret is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "No recovery secret is available for this customer scope."
            ),
        )

    current_active_secret = state.get("active_secret")
    state["active_secret"] = previous_active_secret
    state["previous_active_secret"] = current_active_secret
    state["last_recovered_at"] = _utc_now_iso()
    _append_jwt_audit(
        state,
        action="recover",
        detail="Customer JWT active secret recovered from prior state.",
    )

    return {
        "customer_id": scope,
        "message": "Customer JWT secret recovery completed.",
        "jwt_state": _serialize_customer_jwt_state(scope, state),
    }
