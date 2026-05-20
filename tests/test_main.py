from __future__ import annotations

from typing import Any

import pytest
from fastapi import Response
from starlette.requests import Request

import main


async def _receive() -> dict[str, Any]:
    """Provide an empty request body to Starlette's request wrapper."""
    return {"type": "http.request", "body": b"", "more_body": False}


def _build_request(headers: list[tuple[bytes, bytes]]) -> Request:
    """Create a minimal ASGI request for middleware testing."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/info",
        "raw_path": b"/info",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope, receive=_receive)


class FakeWebSocket:
    """Minimal WebSocket test double for auth failure handling."""

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}
        self.close_code: int | None = None
        self.close_reason: str | None = None

    async def close(self, code: int, reason: str) -> None:
        """Capture the close reason without opening a real socket."""
        self.close_code = code
        self.close_reason = reason


@pytest.mark.anyio
async def test_http_invalid_api_key_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP requests with invalid API keys should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="http",
        credential_type="api_key",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    async def fake_call_next(_request: Request) -> Response:
        return Response(status_code=401)

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )
    monkeypatch.setattr(main.audit_log_writer, "enqueue", lambda entry: None)

    request = _build_request([(b"x-api-key", b"invalid")])
    await main.log_requests(request, fake_call_next)

    assert counter._value.get() == before + 1


@pytest.mark.anyio
async def test_http_invalid_bearer_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP requests with invalid bearer tokens should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="http",
        credential_type="bearer",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    async def fake_call_next(_request: Request) -> Response:
        return Response(status_code=401)

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )
    monkeypatch.setattr(main.audit_log_writer, "enqueue", lambda entry: None)

    request = _build_request(
        [(b"authorization", b"Bearer invalid-token")]
    )
    await main.log_requests(request, fake_call_next)

    assert counter._value.get() == before + 1


@pytest.mark.anyio
async def test_websocket_invalid_session_token_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid WebSocket session tokens should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="websocket",
        credential_type="session_token",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )

    websocket = FakeWebSocket()
    await main.ws_events(websocket, session_token="invalid-session-token")

    assert websocket.close_code == 4001
    assert websocket.close_reason == "Invalid API key"
    assert counter._value.get() == before + 1