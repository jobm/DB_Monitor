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


@pytest.mark.anyio
async def test_connect_ws_clears_stale_events_before_listening(monkeypatch):
    client = DBMonitorClient(base_url="http://localhost:8000")
    client.api_key = "1.secret"
    client._ws_session_token = "signed-session"
    await client._event_queue.put({"id": 1})

    fake_ws = FakeWebSocket()

    async def fake_connect(url: str):
        assert url == "ws://localhost:8000/ws/events?session_token=signed-session"
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
