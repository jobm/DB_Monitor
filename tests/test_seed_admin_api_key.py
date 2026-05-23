from __future__ import annotations

from datetime import datetime, timezone

import pytest

import scripts.seed_admin_api_key as seed_admin_api_key
from models import ApiKey


class _FakeScalarResult:
    def __init__(self, rows: list[ApiKey]):
        self._rows = rows

    def scalars(self) -> "_FakeScalarResult":
        return self

    def all(self) -> list[ApiKey]:
        return list(self._rows)


class _FakeSession:
    def __init__(self, existing_rows: list[ApiKey], new_id: int = 101):
        self._existing_rows = existing_rows
        self._new_id = new_id
        self.added: ApiKey | None = None
        self.committed = False

    async def execute(self, _stmt):
        return _FakeScalarResult(self._existing_rows)

    def add(self, model: ApiKey) -> None:
        self.added = model

    async def flush(self) -> None:
        if self.added is not None:
            self.added.id = self._new_id

    async def commit(self) -> None:
        self.committed = True


class _FakeSessionFactory:
    def __init__(self, session: _FakeSession):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self) -> _FakeSession:
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.mark.anyio
async def test_create_admin_api_key_revokes_existing_active_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_revoked_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    existing_with_null_revoked = ApiKey(
        id=7,
        key_hash="old-hash-1",
        owner_name="ops",
        role="admin",
        is_active=True,
        expires_at=None,
        revoked_at=None,
    )
    existing_with_revoked = ApiKey(
        id=8,
        key_hash="old-hash-2",
        owner_name="ops",
        role="admin",
        is_active=True,
        expires_at=None,
        revoked_at=old_revoked_at,
    )
    fake_session = _FakeSession(
        [existing_with_null_revoked, existing_with_revoked],
        new_id=222,
    )

    expiry = datetime(2027, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(
        seed_admin_api_key,
        "AsyncSessionLocal",
        _FakeSessionFactory(fake_session),
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "generate_new_api_key",
        lambda: "raw-secret",
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "get_api_key_hash",
        lambda _raw: "hashed-secret",
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "build_api_key_expiration",
        lambda _ttl_days: expiry,
    )

    composite_key = await seed_admin_api_key.create_admin_api_key("ops", 30)

    assert composite_key == "222.raw-secret"
    assert existing_with_null_revoked.is_active is False
    assert existing_with_null_revoked.revoked_at is not None
    assert existing_with_revoked.is_active is False
    assert existing_with_revoked.revoked_at == old_revoked_at
    assert fake_session.added is not None
    assert fake_session.added.owner_name == "ops"
    assert fake_session.added.role == "admin"
    assert fake_session.added.is_active is True
    assert fake_session.added.key_hash == "hashed-secret"
    assert fake_session.added.expires_at == expiry
    assert fake_session.committed is True


@pytest.mark.anyio
async def test_create_admin_api_key_creates_key_when_none_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = _FakeSession([], new_id=333)

    monkeypatch.setattr(
        seed_admin_api_key,
        "AsyncSessionLocal",
        _FakeSessionFactory(fake_session),
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "generate_new_api_key",
        lambda: "fresh-secret",
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "get_api_key_hash",
        lambda _raw: "fresh-hash",
    )
    monkeypatch.setattr(
        seed_admin_api_key,
        "build_api_key_expiration",
        lambda _ttl_days: datetime(2027, 2, 1, tzinfo=timezone.utc),
    )

    composite_key = await seed_admin_api_key.create_admin_api_key("ci", 1)

    assert composite_key == "333.fresh-secret"
    assert fake_session.added is not None
    assert fake_session.added.owner_name == "ci"
    assert fake_session.added.role == "admin"
    assert fake_session.added.is_active is True
    assert fake_session.committed is True
