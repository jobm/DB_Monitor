from __future__ import annotations

from datetime import datetime, timedelta, timezone

import auth
from models import ApiKey


def test_is_api_key_usable_rejects_expired_key() -> None:
    expired_key = ApiKey(
        key_hash="hash",
        owner_name="ops",
        role="viewer",
        is_active=True,
        expires_at=datetime.now(timezone.utc) - timedelta(days=1),
    )

    assert auth.is_api_key_usable(expired_key) is False


def test_is_api_key_usable_rejects_revoked_key() -> None:
    revoked_key = ApiKey(
        key_hash="hash",
        owner_name="ops",
        role="viewer",
        is_active=True,
        revoked_at=datetime.now(timezone.utc),
    )

    assert auth.is_api_key_usable(revoked_key) is False