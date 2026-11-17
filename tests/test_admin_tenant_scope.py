"""Tenant-scope enforcement for administrative operational endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from db_monitor.auth.deps import require_admin_role
from db_monitor.core.models import ApiKey
from db_monitor.main import app


def _admin(tenant_id: str | None = None) -> ApiKey:
    return ApiKey(
        id=1,
        key_hash="admin-hash",
        owner_name="admin",
        role="admin",
        is_active=True,
        tenant_id=tenant_id,
    )


@pytest.fixture
def client() -> TestClient:
    app.dependency_overrides.clear()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _authenticate_as(admin_key: ApiKey) -> None:
    async def _override() -> ApiKey:
        return admin_key

    app.dependency_overrides[require_admin_role] = _override


CUSTOMER_SCOPED_ENDPOINTS = [
    ("GET", "/admin/customers/tenant-b/lifecycle"),
    ("POST", "/admin/customers/tenant-b/suspend"),
    ("POST", "/admin/customers/tenant-b/resume"),
    ("POST", "/admin/customers/tenant-b/upgrade"),
    ("POST", "/admin/customers/tenant-b/provision-jobs"),
    ("GET", "/admin/customers/tenant-b/provision-jobs"),
    ("GET", "/admin/customers/tenant-b/provision-jobs/job-1"),
    ("POST", "/admin/customers/tenant-b/provision-jobs/job-1/execute"),
    ("GET", "/admin/customers/tenant-b/guardrails"),
    ("GET", "/admin/customers/tenant-b/observability"),
]

GLOBAL_ADMIN_ENDPOINTS = [
    ("GET", "/admin/checkpoints"),
    ("GET", "/admin/audit-log/spill"),
    ("POST", "/admin/audit-log/spill/replay"),
    ("GET", "/admin/dlq"),
    ("POST", "/admin/dlq/replay"),
    ("POST", "/admin/dlq/1/replay"),
    ("GET", "/admin/slo-policy"),
]


@pytest.mark.parametrize(("method", "path"), CUSTOMER_SCOPED_ENDPOINTS)
def test_scoped_admin_cannot_manage_another_customer(
    client: TestClient,
    method: str,
    path: str,
) -> None:
    _authenticate_as(_admin("tenant-a"))

    response = client.request(method, path)

    assert response.status_code == 403
    assert "another customer" in response.json()["detail"]


@pytest.mark.parametrize(("method", "path"), GLOBAL_ADMIN_ENDPOINTS)
def test_scoped_admin_cannot_access_global_admin_endpoints(
    client: TestClient,
    method: str,
    path: str,
) -> None:
    _authenticate_as(_admin("tenant-a"))

    response = client.request(method, path)

    assert response.status_code == 403
    assert "Global admin access" in response.json()["detail"]


def test_scoped_admin_can_access_own_customer_metadata(
    client: TestClient,
) -> None:
    _authenticate_as(_admin("tenant-a"))

    response = client.get("/admin/customers/tenant-a/guardrails")

    assert response.status_code == 200
    assert response.json()["customer_id"] == "tenant-a"


def test_global_admin_can_access_global_admin_endpoint(
    client: TestClient,
) -> None:
    _authenticate_as(_admin())

    response = client.get("/admin/slo-policy")

    assert response.status_code == 200
    assert "window_days" in response.json()
