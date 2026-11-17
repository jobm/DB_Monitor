"""Database operations for API key lookup and authentication."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from db_monitor.extensions import AsyncSessionLocal
from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from db_monitor.auth.tokens import decode_session_token
from db_monitor.auth.utils import _utc_now, api_key_header, verify_api_key_hash
from db_monitor.core.models import ApiKey
from sqlalchemy import select

bearer_scheme = HTTPBearer(auto_error=False)


def is_api_key_usable(
    api_key_record: Optional[ApiKey],
    now: Optional[datetime] = None,
) -> bool:
    """Return whether the given API key record is active and unexpired."""
    if api_key_record is None or not api_key_record.is_active:
        return False

    current_time = now or _utc_now()
    if api_key_record.revoked_at is not None:
        return False
    if api_key_record.expires_at and api_key_record.expires_at <= current_time:
        return False
    return True


async def _get_api_key_by_id(key_id: int) -> Optional[ApiKey]:
    """Fetch an API key by primary key and enforce lifecycle checks."""
    async with AsyncSessionLocal() as session:  # type: ignore[type-var]
        result = await session.execute(
            select(ApiKey).where(ApiKey.id == key_id)
        )
        api_key_record = result.scalar_one_or_none()
        if is_api_key_usable(api_key_record):
            return api_key_record
    return None


async def _authenticate_bearer_token(
    bearer_token: str,
    expected_token_type: str = "access",
) -> Optional[ApiKey]:
    """Resolve a signed bearer or WebSocket session token into an ApiKey."""
    claims = decode_session_token(
        bearer_token,
        expected_token_type=expected_token_type,
    )
    if claims is None:
        return None

    subject = claims.get("sub")
    if subject is None:
        return None

    try:
        key_id = int(subject)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return None

    return await _get_api_key_by_id(key_id)


async def get_current_api_key(
    api_key_header_value: str = Security(api_key_header),
) -> Optional[ApiKey]:
    """Retrieve the ApiKey record if the provided key is valid."""

    if not api_key_header_value:
        return None

    async with AsyncSessionLocal() as session:  # type: ignore[type-var]
        try:
            key_id_str, raw_key = api_key_header_value.split(".", 1)
            key_id = int(key_id_str)
        except ValueError:
            return None

        stmt = select(ApiKey).where(ApiKey.id == key_id)
        result = await session.execute(stmt)
        api_key_record = result.scalar_one_or_none()

        if is_api_key_usable(api_key_record) and verify_api_key_hash(
            raw_key, api_key_record.key_hash
        ):
            return api_key_record

    return None


async def get_current_bearer_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(
        bearer_scheme
    ),
) -> Optional[ApiKey]:
    """Resolve a bearer token into an ApiKey record."""
    if credentials is None or credentials.scheme.lower() != "bearer":
        return None

    return await _authenticate_bearer_token(credentials.credentials)


async def authenticate_credentials(
    api_key_header_value: Optional[str] = None,
    authorization_header_value: Optional[str] = None,
    session_token: Optional[str] = None,
    expected_token_type: str = "access",
) -> Optional[ApiKey]:
    """Authenticate API key, bearer token, or WebSocket session token."""
    if session_token:
        return await _authenticate_bearer_token(
            session_token,
            expected_token_type=expected_token_type,
        )

    if authorization_header_value:
        scheme, _, token = authorization_header_value.partition(" ")
        if scheme.lower() == "bearer" and token:
            authenticated = await _authenticate_bearer_token(
                token,
                expected_token_type=expected_token_type,
            )
            if authenticated:
                return authenticated

    if api_key_header_value:
        return await get_current_api_key(api_key_header_value)

    return None
