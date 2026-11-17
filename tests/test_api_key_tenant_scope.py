"""Tests for tenant-scoped API key creation and rotation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from db_monitor.core.models import ApiKey
from db_monitor.routes.auth.key_routes import keys as keys_module


class _FakeResult:
    def __init__(self, value: ApiKey | None = None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> ApiKey | None:
        return self._value


class _FakeSession:
    def __init__(self, existing_key: ApiKey | None = None) -> None:
        self.existing_key = existing_key
        self.added: list[ApiKey] = []
        self._next_id = 100

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def begin(self) -> "_FakeSession":
        return self

    def add(self, model: ApiKey) -> None:
        self.added.append(model)

    async def flush(self) -> None:
        for model in self.added:
            if model.id is None:
                model.id = self._next_id
                self._next_id += 1

    async def execute(self, statement) -> _FakeResult:
        del statement
        return _FakeResult(self.existing_key)


def _admin(tenant_id: str | None = None, key_id: int = 1) -> ApiKey:
    return ApiKey(
        id=key_id,
        key_hash="admin-hash",
        owner_name="admin",
        role="admin",
        is_active=True,
        tenant_id=tenant_id,
    )


def _existing_key(tenant_id: str | None = None) -> ApiKey:
    return ApiKey(
        id=10,
        key_hash="existing-hash",
        owner_name="existing",
        role="viewer",
        is_active=True,
        expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        tenant_id=tenant_id,
    )


@pytest.mark.anyio
async def test_create_api_key_inherits_scoped_admin_tenant(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(keys_module, "AsyncSessionLocal", lambda: session)

    response = await keys_module.create_api_key(
        owner_name="viewer-a",
        role="viewer",
        ttl_days=30,
        tenant_id=None,
        creator_api_key=_admin("tenant-a"),
    )

    assert session.added[0].tenant_id == "tenant-a"
    assert response["tenant_id"] == "tenant-a"


@pytest.mark.anyio
async def test_create_api_key_persists_explicit_tenant_for_global_admin(
    monkeypatch,
):
    session = _FakeSession()
    monkeypatch.setattr(keys_module, "AsyncSessionLocal", lambda: session)

    response = await keys_module.create_api_key(
        owner_name="viewer-b",
        role="viewer",
        ttl_days=30,
        tenant_id=" tenant-b ",
        creator_api_key=_admin(),
    )

    assert session.added[0].tenant_id == "tenant-b"
    assert response["tenant_id"] == "tenant-b"


@pytest.mark.anyio
async def test_create_api_key_rejects_cross_tenant_scope(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(keys_module, "AsyncSessionLocal", lambda: session)

    with pytest.raises(HTTPException) as exc_info:
        await keys_module.create_api_key(
            owner_name="viewer-c",
            role="viewer",
            ttl_days=30,
            tenant_id="tenant-b",
            creator_api_key=_admin("tenant-a"),
        )

    assert exc_info.value.status_code == 403
    assert session.added == []


@pytest.mark.anyio
async def test_rotate_api_key_preserves_tenant_id(monkeypatch):
    original = _existing_key("tenant-a")
    session = _FakeSession(existing_key=original)
    monkeypatch.setattr(keys_module, "AsyncSessionLocal", lambda: session)

    response = await keys_module.rotate_api_key(
        key_id=original.id,
        ttl_days=30,
        creator_api_key=_admin(),
    )

    assert session.added[0].tenant_id == "tenant-a"
    assert response["tenant_id"] == "tenant-a"
    assert original.is_active is False


@pytest.mark.anyio
async def test_scoped_admin_cannot_rotate_another_tenants_key(monkeypatch):
    original = _existing_key("tenant-b")
    session = _FakeSession(existing_key=original)
    monkeypatch.setattr(keys_module, "AsyncSessionLocal", lambda: session)

    with pytest.raises(HTTPException) as exc_info:
        await keys_module.rotate_api_key(
            key_id=original.id,
            ttl_days=30,
            creator_api_key=_admin("tenant-a"),
        )

    assert exc_info.value.status_code == 403
    assert original.is_active is True
    assert session.added == []
