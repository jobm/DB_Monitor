from __future__ import annotations

from datetime import datetime, timedelta, timezone

import auth
import pytest

import routes.auth as routes_auth
from models import ApiKey


def _api_key() -> ApiKey:
    return ApiKey(
        id=7,
        key_hash="hash",
        owner_name="ops",
        role="admin",
        is_active=True,
        expires_at=datetime.now(timezone.utc).replace(year=2099),
    )


def test_access_token_round_trip() -> None:
    api_key = _api_key()
    token, _expires_at = auth.build_access_token(api_key)

    claims = auth.decode_session_token(token, expected_token_type="access")

    assert claims is not None
    assert claims["sub"] == str(api_key.id)
    assert claims["role"] == api_key.role


def test_ws_token_rejects_wrong_token_type() -> None:
    api_key = _api_key()
    token, _expires_at = auth.build_access_token(api_key)

    claims = auth.decode_session_token(token, expected_token_type="ws")

    assert claims is None


def test_decode_session_token_accepts_next_secret(monkeypatch) -> None:
    issued_at = datetime.now(timezone.utc)
    payload = {
        "sub": "7",
        "role": "admin",
        "owner_name": "ops",
        "token_type": "access",
        "iat": int(issued_at.timestamp()),
        "exp": int((issued_at + timedelta(minutes=15)).timestamp()),
    }

    monkeypatch.setattr(auth, "JWT_SECRET", "current-secret-12345678901234567890")
    monkeypatch.setattr(auth, "JWT_SECRET_NEXT", "next-secret-123456789012345678901234")
    token = auth.jwt.encode(
        payload,
        auth.JWT_SECRET_NEXT,
        algorithm=auth.JWT_ALGORITHM,
    )

    claims = auth.decode_session_token(token, expected_token_type="access")

    assert claims is not None
    assert claims["sub"] == "7"


@pytest.mark.anyio
async def test_token_exchange_uses_authenticated_api_key() -> None:
    response = await routes_auth.create_access_token_exchange(_api_key())

    assert response["token_type"] == "bearer"
    assert response["role"] == "admin"
    assert response["owner_name"] == "ops"
    assert auth.decode_session_token(
        response["access_token"],
        expected_token_type="access",
    ) is not None


@pytest.mark.anyio
async def test_ws_token_exchange_returns_ws_session_token() -> None:
    response = await routes_auth.create_websocket_session_token(_api_key())

    assert response["role"] == "admin"
    assert auth.decode_session_token(
        response["session_token"],
        expected_token_type="ws",
    ) is not None