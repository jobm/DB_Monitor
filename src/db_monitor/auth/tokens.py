"""JWT session token building and decoding."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import jwt
from db_monitor.core.config import settings
from jwt import InvalidTokenError

from db_monitor.auth.utils import _utc_now

from db_monitor.core.models import ApiKey


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
    token = jwt.encode(payload, settings.auth.jwt_secret, algorithm=settings.auth.jwt_algorithm)
    return token, expires_at


def build_access_token(api_key_record: ApiKey) -> tuple[str, datetime]:
    """Create a short-lived bearer token for HTTP API access."""
    return _build_session_token(
        api_key_record,
        token_type="access",
        expires_delta=timedelta(minutes=settings.auth.access_token_ttl_minutes),
    )


def build_ws_session_token(api_key_record: ApiKey) -> tuple[str, datetime]:
    """Create a short-lived token for WebSocket session establishment."""
    return _build_session_token(
        api_key_record,
        token_type="ws",
        expires_delta=timedelta(seconds=settings.auth.ws_session_token_ttl_seconds),
    )


def decode_session_token(
    token: str,
    expected_token_type: Optional[str] = None,
) -> Optional[dict[str, object]]:
    """Decode and validate a JWT session token."""
    payload = None
    valid_secrets = [settings.auth.jwt_secret]
    if settings.auth.jwt_secret_next:
        valid_secrets.append(settings.auth.jwt_secret_next)

    for secret in valid_secrets:
        try:
            payload = jwt.decode(
                token,
                secret,
                algorithms=[settings.auth.jwt_algorithm],
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
