from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

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


@pytest.mark.anyio
async def test_require_valid_api_key_reports_remediation_steps() -> None:
    with pytest.raises(auth.HTTPException) as exc_info:
        await auth.require_valid_api_key(api_key_record=None)

    assert exc_info.value.status_code == 401
    assert "POST /auth/token" in exc_info.value.detail


@pytest.mark.anyio
async def test_require_admin_role_reports_expected_credential() -> None:
    viewer_key = ApiKey(
        key_hash="hash",
        owner_name="viewer",
        role="viewer",
        is_active=True,
    )

    with pytest.raises(auth.HTTPException) as exc_info:
        await auth.require_admin_role(api_key_record=viewer_key)

    assert exc_info.value.status_code == 403
    assert "Admin role required" in exc_info.value.detail


@pytest.mark.anyio
async def test_require_viewer_role_reports_expected_credential() -> None:
    unknown_role_key = ApiKey(
        key_hash="hash",
        owner_name="service",
        role="service",
        is_active=True,
    )

    with pytest.raises(auth.HTTPException) as exc_info:
        await auth.require_viewer_role(api_key_record=unknown_role_key)

    assert exc_info.value.status_code == 403
    assert "viewer or admin credential" in exc_info.value.detail