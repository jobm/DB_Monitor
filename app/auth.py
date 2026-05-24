"""Authentication and RBAC service for API Keys."""

import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from core.config import (
    ACCESS_TOKEN_TTL_MINUTES,
    API_KEY_DEFAULT_TTL_DAYS,
    JWT_ALGORITHM,
    JWT_SECRET,
    JWT_SECRET_NEXT,
    WS_SESSION_TOKEN_TTL_SECONDS,
)
from core.db import AsyncSessionLocal
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.api_key import APIKeyHeader
from jwt import InvalidTokenError
from core.models import ApiKey
from sqlalchemy import select

password_hasher = PasswordHasher()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


def _utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _normalize_secret(secret: str) -> str:
    """Preserve the legacy API-key normalization behavior.

    Existing keys were historically hashed using only the first 72 characters.
    Keep that input normalization so previously issued keys continue to verify.
    """
    return secret[:72]


def verify_api_key_hash(plain_key: str, hashed_key: str) -> bool:
    """Verify an API key against its hash."""
    try:
        return password_hasher.verify(
            hashed_key,
            _normalize_secret(plain_key),
        )
    except (InvalidHashError, VerificationError):
        return False


def get_api_key_hash(plain_key: str) -> str:
    """Generate a hash for a new API key."""
    return password_hasher.hash(_normalize_secret(plain_key))


def generate_new_api_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_urlsafe(32)


def build_api_key_expiration(
    ttl_days: Optional[int] = None,
) -> datetime:
    """Return the expiration timestamp for a new or rotated API key."""
    effective_ttl_days = ttl_days or API_KEY_DEFAULT_TTL_DAYS
    return _utc_now() + timedelta(days=effective_ttl_days)


def _build_session_token(
    api_key_record: ApiKey,
    token_type: str,
    expires_delta: timedelta,
) -> tuple[str, datetime]:
    """Create a signed JWT for API access or WebSocket handshakes."""
    issued_at = _utc_now()
    expires_at = issued_at + expires_delta
    payload = {
        "sub": str(api_key_record.id),
        "role": api_key_record.role,
        "owner_name": api_key_record.owner_name,
        "token_type": token_type,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, expires_at


def build_access_token(api_key_record: ApiKey) -> tuple[str, datetime]:
    """Create a short-lived bearer token for HTTP API access."""
    return _build_session_token(
        api_key_record,
        token_type="access",
        expires_delta=timedelta(minutes=ACCESS_TOKEN_TTL_MINUTES),
    )


def build_ws_session_token(api_key_record: ApiKey) -> tuple[str, datetime]:
    """Create a short-lived token for WebSocket session establishment."""
    return _build_session_token(
        api_key_record,
        token_type="ws",
        expires_delta=timedelta(seconds=WS_SESSION_TOKEN_TTL_SECONDS),
    )


def decode_session_token(
    token: str,
    expected_token_type: Optional[str] = None,
) -> Optional[dict[str, object]]:
    """Decode and validate a JWT session token."""
    payload = None
    valid_secrets = [JWT_SECRET]
    if JWT_SECRET_NEXT:
        valid_secrets.append(JWT_SECRET_NEXT)

    for secret in valid_secrets:
        try:
            payload = jwt.decode(
                token,
                secret,
                algorithms=[JWT_ALGORITHM],
            )
            break
        except InvalidTokenError:
            continue

    if payload is None:
        return None

    token_type = payload.get("token_type")
    if expected_token_type and token_type != expected_token_type:
        return None
    return payload


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
    async with AsyncSessionLocal() as session:
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
        key_id = int(subject)
    except (TypeError, ValueError):
        return None

    return await _get_api_key_by_id(key_id)


async def get_current_api_key(
    api_key_header_value: str = Security(api_key_header),
) -> Optional[ApiKey]:
    """Retrieve the ApiKey record if the provided key is valid."""
    if not api_key_header_value:
        return None

    async with AsyncSessionLocal() as session:
        # Since we hash keys, we normally couldn't look them up by hash easily
        # For API keys, a common pattern to allow lookup is prefix.key
        # But for absolute security, if we only have the raw key,
        # we must check against active keys.
        # Optimization: storing an unhashed prefix or ID to lookup the row.
        # For simplicity here, we assume the client passes `{id}.{raw_key}`

        try:
            key_id_str, raw_key = api_key_header_value.split(".", 1)
            key_id = int(key_id_str)
        except ValueError:
            # Invalid format
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


async def get_current_authenticated_api_key(
    bearer_api_key: Optional[ApiKey] = Depends(get_current_bearer_api_key),
    api_key_record: Optional[ApiKey] = Depends(get_current_api_key),
) -> Optional[ApiKey]:
    """Resolve bearer-token or API-key authentication for HTTP routes."""
    return bearer_api_key or api_key_record


async def require_valid_api_key(
    api_key_record: Optional[ApiKey] = Depends(
        get_current_authenticated_api_key
    ),
) -> ApiKey:
    """Dependency that requires any valid API Key."""
    if not api_key_record:
        raise HTTPException(
            status_code=401,
            detail=(
                "Invalid or missing credentials. "
                "Use Authorization: Bearer <token> "
                "or X-API-Key: <id>.<raw_secret>. "
                "If you only have an API key, first exchange it "
                "at POST /auth/token."
            ),
        )
    return api_key_record


async def require_admin_role(
    api_key_record: ApiKey = Depends(require_valid_api_key),
) -> ApiKey:
    """Dependency that requires an Admin API Key."""
    if api_key_record.role != "admin":
        raise HTTPException(
            status_code=403,
            detail=(
                "Admin role required. Use an admin API key or a bearer token "
                "derived from one."
            ),
        )
    return api_key_record


async def require_viewer_role(
    api_key_record: ApiKey = Depends(require_valid_api_key),
) -> ApiKey:
    """Require at least a Viewer API key.

    Admin credentials also satisfy this dependency.
    """
    if api_key_record.role not in ("admin", "viewer"):
        raise HTTPException(
            status_code=403,
            detail=(
                "Viewer role required. Use a viewer or admin credential to "
                "access this endpoint."
            ),
        )
    return api_key_record
