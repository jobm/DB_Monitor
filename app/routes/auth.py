"""Authentication and API key lifecycle endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select

from auth import (
    build_access_token as issue_access_token,
)
from auth import (
    build_api_key_expiration,
    build_ws_session_token,
    generate_new_api_key,
    get_api_key_hash,
    is_api_key_usable,
    require_admin_role,
    require_viewer_role,
)
from core.config import ALLOW_BOOTSTRAP, API_KEY_DEFAULT_TTL_DAYS
from core.db import AsyncSessionLocal
from models import ApiKey
from response_models import (
    AccessTokenExchangeResponse,
    WebSocketTokenExchangeResponse,
)
from .utils import utc_now

router = APIRouter()


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
