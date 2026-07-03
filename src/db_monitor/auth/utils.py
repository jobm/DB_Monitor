"""Authentication utilities — password hashing and API key generation."""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from db_monitor.core.config import settings
from fastapi.security.api_key import APIKeyHeader


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

password_hasher = PasswordHasher()


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
    effective_ttl_days = ttl_days or settings.auth.api_key_default_ttl_days
    return _utc_now() + timedelta(days=effective_ttl_days)
