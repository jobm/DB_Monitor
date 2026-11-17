"""Tests for tenant-scoped WebSocket fan-out."""

from __future__ import annotations

import json

import pytest

from db_monitor.ws_manager import WebSocketManager


class _FakeWebSocket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.accepted = False
        self.messages: list[dict] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))


@pytest.mark.anyio
async def test_broadcast_only_reaches_matching_tenant_and_admins():
    manager = WebSocketManager()
    admin = _FakeWebSocket("admin")
    tenant_a = _FakeWebSocket("tenant-a")
    tenant_b = _FakeWebSocket("tenant-b")

    await manager.connect(admin, 1, tenant_id=None)
    await manager.connect(tenant_a, 2, tenant_id="tenant-a")
    await manager.connect(tenant_b, 3, tenant_id="tenant-b")

    message = {"type": "new_event", "event": {"id": 1}}
    await manager.broadcast(message, tenant_id="tenant-a")

    assert admin.messages == [message]
    assert tenant_a.messages == [message]
    assert tenant_b.messages == []


@pytest.mark.anyio
async def test_broadcast_without_tenant_only_reaches_admins():
    manager = WebSocketManager()
    admin = _FakeWebSocket("admin")
    tenant_a = _FakeWebSocket("tenant-a")

    await manager.connect(admin, 1, tenant_id=None)
    await manager.connect(tenant_a, 2, tenant_id="tenant-a")

    message = {"type": "new_event", "event": {"id": 2}}
    await manager.broadcast(message, tenant_id=None)

    assert admin.messages == [message]
    assert tenant_a.messages == []


@pytest.mark.anyio
async def test_disconnect_removes_tenant_connection():
    manager = WebSocketManager()
    websocket = _FakeWebSocket("tenant-a")

    await manager.connect(websocket, 7, tenant_id="tenant-a")
    assert manager.connection_count == 1

    await manager.disconnect(7)
    assert manager.connection_count == 0
