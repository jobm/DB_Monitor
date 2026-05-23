from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

import pytest

httpx_stub = types.ModuleType("httpx")
httpx_stub.AsyncClient = object
sys.modules.setdefault("httpx", httpx_stub)

websockets_stub = types.ModuleType("websockets")
websockets_stub.WebSocketClientProtocol = object
websockets_stub.ConnectionClosed = Exception
websockets_stub.connect = None
sys.modules.setdefault("websockets", websockets_stub)

TUI_CLIENT_PATH = Path(__file__).resolve().parents[1] / "tui" / "client.py"
client_spec = importlib.util.spec_from_file_location(
    "tui_client",
    TUI_CLIENT_PATH,
)
client_module = importlib.util.module_from_spec(client_spec)
assert client_spec is not None
assert client_spec.loader is not None
client_spec.loader.exec_module(client_module)
DBMonitorClient = client_module.DBMonitorClient


class FakeWebSocket:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code not in (503,):
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpClient:
    def __init__(self):
        self.calls = []

    async def get(self, path, params=None):
        self.calls.append(("GET", path, params))
        if path == "/info":
            return FakeResponse({"title": "DB Monitor"})
        if path == "/readyz":
            return FakeResponse({"status": "not_ready"}, status_code=503)
        if path == "/auth/keys":
            return FakeResponse(
                {
                    "keys": [{"id": 7, "owner_name": "ops-admin"}],
                    "count": 1,
                    "current_api_key_id": 7,
                }
            )
        if path == "/admin/checkpoints":
            return FakeResponse({"checkpoints": [{"topic": "orders"}]})
        if path == "/admin/dlq":
            return FakeResponse({"events": [{"id": 41}]})
        raise AssertionError(f"Unexpected GET path: {path}")

    async def post(self, path, params=None):
        self.calls.append(("POST", path, params))
        if path == "/auth/keys":
            return FakeResponse({"api_key": "99.secret", "role": "viewer"})
        if path == "/auth/keys/7/rotate":
            return FakeResponse(
                {
                    "api_key": "100.secret",
                    "rotated_from_key_id": 7,
                }
            )
        if path == "/auth/keys/7/revoke":
            return FakeResponse({"revoked_key_id": 7})
        if path == "/admin/dlq/replay":
            return FakeResponse({"replayed_count": 3})
        if path == "/admin/dlq/41/replay":
            return FakeResponse({"status": "replayed", "dlq_event_id": 41})
        raise AssertionError(f"Unexpected POST path: {path}")


@pytest.mark.anyio
async def test_connect_ws_clears_stale_events_before_listening(monkeypatch):
    client = DBMonitorClient(base_url="http://localhost:8000")
    client.api_key = "1.secret"
    client._ws_session_token = "signed-session"
    await client._event_queue.put({"id": 1})

    fake_ws = FakeWebSocket()

    async def fake_connect(url: str):
        assert (
            url
            == "ws://localhost:8000/ws/events?session_token=signed-session"
        )
        return fake_ws

    created_tasks: list[asyncio.Task] = []
    original_create_task = asyncio.create_task

    def fake_create_task(coro):
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(client_module.websockets, "connect", fake_connect)
    monkeypatch.setattr(client_module.asyncio, "create_task", fake_create_task)

    await client.connect_ws()

    assert client._event_queue.empty()
    assert client._ws is fake_ws
    assert client._ws_running is True

    for task in created_tasks:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.anyio
async def test_disconnect_ws_clears_queue_and_resets_state():
    client = DBMonitorClient(base_url="http://localhost:8000")
    fake_ws = FakeWebSocket()
    client._ws = fake_ws
    client._ws_running = True
    await client._event_queue.put({"id": 2})

    sleeper = asyncio.create_task(asyncio.sleep(60))
    client._ws_task = sleeper

    await client._disconnect_ws()

    assert fake_ws.closed is True
    assert client._ws is None
    assert client._ws_task is None
    assert client._ws_running is False
    assert client._event_queue.empty()


@pytest.mark.anyio
async def test_exchange_access_token_preserves_auth_context(monkeypatch):
    client = DBMonitorClient(base_url="http://localhost:8000")
    client.api_key = "1.secret"

    class FakeTokenClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, path):
            assert path == "/auth/token"
            return FakeResponse(
                {
                    "access_token": "token-123",
                    "owner_name": "ops-admin",
                    "role": "admin",
                    "expires_at": "2099-01-01T00:00:00+00:00",
                }
            )

    monkeypatch.setattr(client_module.httpx, "AsyncClient", FakeTokenClient)

    await client._exchange_access_token()

    assert client._access_token == "token-123"
    assert client.get_auth_context()["owner_name"] == "ops-admin"
    assert client.is_admin() is True


@pytest.mark.anyio
async def test_check_health_does_not_require_api_key_exchange(monkeypatch):
    client = DBMonitorClient(base_url="http://localhost:8000")
    client.api_key = "invalid.key"

    class HealthOnlyClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, path):
            assert path == "/health"
            return FakeResponse({"status": "ok"}, status_code=200)

        async def post(self, path):
            raise AssertionError(
                "check_health should not exchange an access token",
            )

    monkeypatch.setattr(client_module.httpx, "AsyncClient", HealthOnlyClient)

    assert await client.check_health() is True


@pytest.mark.anyio
async def test_check_health_returns_false_on_unreachable_host(monkeypatch):
    client = DBMonitorClient(base_url="http://localhost:8000")

    class FailingHealthClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            raise RuntimeError("connection failed")

        async def __aexit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(
        client_module.httpx,
        "AsyncClient",
        FailingHealthClient,
    )

    assert await client.check_health() is False


@pytest.mark.anyio
async def test_admin_client_methods_use_expected_routes():
    client = DBMonitorClient(base_url="http://localhost:8000")
    fake_http_client = FakeHttpClient()
    client._client = fake_http_client
    client._access_token = "token-123"
    client._auth_context = {"role": "admin", "owner_name": "ops-admin"}

    info = await client.get_info()
    readiness = await client.get_readiness()
    api_keys = await client.list_api_keys(include_inactive=True)
    created_key = await client.create_api_key(
        owner_name="ops-viewer",
        role="viewer",
        ttl_days=30,
    )
    rotated_key = await client.rotate_api_key(7, ttl_days=45)
    revoked_key = await client.revoke_api_key(7)
    checkpoints = await client.get_consumer_checkpoints()
    dlq_events = await client.get_dead_letter_events(
        limit=25,
        include_replayed=True,
    )
    replay_one = await client.replay_dead_letter_event(41)
    replay_many = await client.replay_dead_letter_events(
        limit=25,
        include_replayed=True,
    )

    assert info["title"] == "DB Monitor"
    assert readiness["status"] == "not_ready"
    assert api_keys["current_api_key_id"] == 7
    assert created_key["api_key"] == "99.secret"
    assert rotated_key["rotated_from_key_id"] == 7
    assert revoked_key["revoked_key_id"] == 7
    assert checkpoints == [{"topic": "orders"}]
    assert dlq_events == [{"id": 41}]
    assert replay_one["dlq_event_id"] == 41
    assert replay_many["replayed_count"] == 3
    assert fake_http_client.calls == [
        ("GET", "/info", None),
        ("GET", "/readyz", None),
        ("GET", "/auth/keys", {"include_inactive": True}),
        (
            "POST",
            "/auth/keys",
            {
                "owner_name": "ops-viewer",
                "role": "viewer",
                "ttl_days": 30,
            },
        ),
        ("POST", "/auth/keys/7/rotate", {"ttl_days": 45}),
        ("POST", "/auth/keys/7/revoke", None),
        ("GET", "/admin/checkpoints", None),
        (
            "GET",
            "/admin/dlq",
            {"limit": 25, "include_replayed": True},
        ),
        ("POST", "/admin/dlq/41/replay", None),
        (
            "POST",
            "/admin/dlq/replay",
            {"limit": 25, "include_replayed": True},
        ),
    ]


@pytest.mark.anyio
async def test_verify_auth_status_distinguishes_auth_failures():
    client = DBMonitorClient(base_url="http://localhost:8000")

    class AuthStatusClient:
        def __init__(self, status_code: int):
            self.status_code = status_code

        async def get(self, path, params=None):
            assert path == "/info"
            return FakeResponse({}, status_code=self.status_code)

    client._client = AuthStatusClient(401)
    client._access_token = "token-123"
    assert await client.verify_auth_status() == "unauthorized"
    assert await client.verify_auth() is False

    client._client = AuthStatusClient(403)
    assert await client.verify_auth_status() == "forbidden"
    assert await client.verify_auth() is False

    client._client = AuthStatusClient(200)
    assert await client.verify_auth_status() == "ok"
    assert await client.verify_auth() is True
