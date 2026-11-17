"""API key CRUD, rotation, revocation, and bootstrap endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select

from db_monitor.auth import (
    build_api_key_expiration,
    generate_new_api_key,
    get_api_key_hash,
    is_api_key_usable,
    require_admin_role,
)
from db_monitor.core.config import settings
from db_monitor.core.models import ApiKey
from db_monitor.extensions import AsyncSessionLocal
from db_monitor.routes.auth.helpers import (
    _require_customer_scope,
    _utc_now_iso,
    api_key_response,
    api_key_status,
    serialize_api_key_summary,
)
from db_monitor.routes.utils import utc_now

keys_router = APIRouter()


def _normalize_tenant_id(tenant_id: str | None) -> str | None:
    """Normalize and validate an optional tenant scope."""
    if tenant_id is None:
        return None
    normalized = tenant_id.strip()
    if not normalized:
        raise HTTPException(
            status_code=400,
            detail="tenant_id must not be empty.",
        )
    if len(normalized) > 128:
        raise HTTPException(
            status_code=400,
            detail="tenant_id must be 128 characters or fewer.",
        )
    return normalized


def _resolve_tenant_scope(
    creator_api_key: ApiKey,
    requested_tenant_id: str | None,
) -> str | None:
    """Resolve key scope while preventing scoped admins from crossing tenants."""
    creator_tenant_id = creator_api_key.tenant_id
    if creator_tenant_id is None:
        return requested_tenant_id
    if (
        requested_tenant_id is not None
        and requested_tenant_id != creator_tenant_id
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "A tenant-scoped admin cannot manage keys for another "
                "tenant."
            ),
        )
    return creator_tenant_id


def _require_key_tenant_scope(
    creator_api_key: ApiKey,
    target_key: ApiKey,
) -> None:
    """Reject cross-tenant key administration by a scoped admin."""
    if (
        creator_api_key.tenant_id is not None
        and target_key.tenant_id != creator_api_key.tenant_id
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "A tenant-scoped admin cannot manage keys outside its "
                "tenant."
            ),
        )


@keys_router.post("/auth/keys")
async def create_api_key(
    owner_name: str,
    role: str = Query("viewer", pattern="^(admin|viewer)$"),
    ttl_days: int = Query(settings.auth.api_key_default_ttl_days, ge=1, le=3650),
    tenant_id: str | None = Query(None, max_length=128),
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """Create a new API Key (requires an existing Admin API key)."""
    resolved_tenant_id = _resolve_tenant_scope(
        creator_api_key,
        _normalize_tenant_id(tenant_id),
    )
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
                tenant_id=resolved_tenant_id,
            )
            session.add(new_key)
            await session.flush()
            return api_key_response(
                new_key,
                raw_key,
                "Store this key securely. It cannot be retrieved again.",
            )


@keys_router.get("/auth/keys")
async def list_api_keys(
    include_inactive: bool = False,
    creator_api_key: ApiKey = Depends(require_admin_role),
):
    """List API keys for operator inventory and lifecycle actions."""
    statement = select(ApiKey)
    if creator_api_key.tenant_id is not None:
        statement = statement.where(
            ApiKey.tenant_id == creator_api_key.tenant_id
        )
    statement = statement.order_by(
        ApiKey.created_at.desc(),
        ApiKey.id.desc(),
    )

    async with AsyncSessionLocal() as session:
        result = await session.execute(statement)
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


@keys_router.post("/auth/keys/{key_id}/rotate")
async def rotate_api_key(
    key_id: int,
    ttl_days: int = Query(settings.auth.api_key_default_ttl_days, ge=1, le=3650),
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
            _require_key_tenant_scope(creator_api_key, existing_key)

            existing_key.is_active = False
            existing_key.revoked_at = utc_now()

            replacement_key = ApiKey(
                key_hash=key_hash,
                owner_name=existing_key.owner_name,
                role=existing_key.role,
                is_active=True,
                expires_at=expires_at,
                tenant_id=existing_key.tenant_id,
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


@keys_router.post("/auth/keys/{key_id}/revoke")
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
            _require_key_tenant_scope(creator_api_key, existing_key)

            existing_key.is_active = False
            existing_key.revoked_at = utc_now()
            return {
                "revoked_key_id": existing_key.id,
                "owner_name": existing_key.owner_name,
                "role": existing_key.role,
                "revoked_at": existing_key.revoked_at.isoformat(),
            }


@keys_router.post("/auth/bootstrap")
async def bootstrap_admin_key(
    owner_name: str,
    ttl_days: int = Query(settings.auth.api_key_default_ttl_days, ge=1, le=3650),
):
    """Create the first admin API key."""
    if not settings.auth.allow_bootstrap:
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
