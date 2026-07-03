"""Customer JWT secret lifecycle endpoints."""

from __future__ import annotations

import secrets

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import select

from db_monitor.auth import (
    build_api_key_expiration,
    generate_new_api_key,
    get_api_key_hash,
    require_admin_role,
)
from db_monitor.core.config import settings
from db_monitor.core.models import ApiKey
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.routes.auth.helpers import (
    _append_jwt_audit,
    _load_customer_jwt_state_row,
    _new_customer_jwt_state,
    _persist_customer_jwt_state,
    _require_customer_scope,
    _row_to_customer_jwt_state,
    _secret_fingerprint,
    _serialize_customer_jwt_state,
    _sync_customer_jwt_state_cache,
    _utc_now_iso,
    api_key_response,
)
router = APIRouter()

CUSTOMER_JWT_SECRET_STATE: dict[str, dict[str, object]] = {}


@router.post("/admin/customers/{customer_id}/auth/bootstrap")
async def bootstrap_customer_auth_scope(
    customer_id: str,
    owner_suffix: str = Query("bootstrap-admin"),
    ttl_days: int = Query(settings.auth.api_key_default_ttl_days, ge=1, le=3650),
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

            record = await _load_customer_jwt_state_row(session, scope)
            state = (
                _row_to_customer_jwt_state(record)
                if record is not None
                else _new_customer_jwt_state()
            )
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
            await _persist_customer_jwt_state(session, scope, state)
            _sync_customer_jwt_state_cache(CUSTOMER_JWT_SECRET_STATE, scope, state)

            response = api_key_response(
                new_key,
                raw_key,
                (
                    "Store this customer admin key securely. "
                    "It cannot be retrieved again."
                ),
            )
            response["customer_id"] = scope
            response["jwt_state"] = _serialize_customer_jwt_state(
                scope,
                state,
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

    async with AsyncSessionLocal() as session:
        record = await _load_customer_jwt_state_row(
            session,
            normalized_customer_id,
        )

    if record is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Customer JWT secrets are not initialized. "
                "Run POST /admin/customers/{customer_id}/auth/bootstrap "
                "first."
            ),
        )

    state = _row_to_customer_jwt_state(record)
    if state.get("active_secret") is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Customer JWT secrets are not initialized. "
                "Run POST /admin/customers/{customer_id}/auth/bootstrap "
                "first."
            ),
        )

    _sync_customer_jwt_state_cache(CUSTOMER_JWT_SECRET_STATE, normalized_customer_id, state)
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
    async with AsyncSessionLocal() as session:
        async with session.begin():
            record = await _load_customer_jwt_state_row(session, scope)
            if record is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Customer JWT secrets are not initialized. "
                        "Run POST /admin/customers/{customer_id}/auth/"
                        "bootstrap first."
                    ),
                )

            state = _row_to_customer_jwt_state(record)
            if state.get("active_secret") is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Customer JWT secrets are not initialized. "
                        "Run POST /admin/customers/{customer_id}/auth/"
                        "bootstrap first."
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
            await _persist_customer_jwt_state(session, scope, state)
            _sync_customer_jwt_state_cache(CUSTOMER_JWT_SECRET_STATE, scope, state)

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
    async with AsyncSessionLocal() as session:
        async with session.begin():
            record = await _load_customer_jwt_state_row(session, scope)
            if record is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Customer JWT secrets are not initialized. "
                        "Run POST /admin/customers/{customer_id}/auth/"
                        "bootstrap first."
                    ),
                )

            state = _row_to_customer_jwt_state(record)
            previous_active_secret = state.get("previous_active_secret")
            if previous_active_secret is None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "No recovery secret is available for this customer "
                        "scope."
                    ),
                )

            current_active_secret = state.get("active_secret")
            state["active_secret"] = previous_active_secret
            state["previous_active_secret"] = current_active_secret
            state["last_recovered_at"] = _utc_now_iso()
            _append_jwt_audit(
                state,
                action="recover",
                detail=(
                    "Customer JWT active secret recovered from prior "
                    "state."
                ),
            )
            await _persist_customer_jwt_state(session, scope, state)
            _sync_customer_jwt_state_cache(CUSTOMER_JWT_SECRET_STATE, scope, state)

    return {
        "customer_id": scope,
        "message": "Customer JWT secret recovery completed.",
        "jwt_state": _serialize_customer_jwt_state(scope, state),
    }
