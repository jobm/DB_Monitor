"""Tests for multi-tenant isolation via API key tenant scope (OS-7).

Covers:
- Tenant ID field on ApiKey model
- get_tenant_filter dependency behavior via live HTTP requests
- Event route enforces tenant filter (403 on mismatch)
- Change route enforces tenant filter
- Table routes filter by tenant
- Admin keys bypass tenant filtering

The route-level tests exercise the real FastAPI application through
``TestClient`` and inject credentials with ``app.dependency_overrides``
so dependency wiring (viewer role + tenant filter) is exercised rather
than bypassed with hand-built handler arguments.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from db_monitor.auth.deps import require_valid_api_key
from db_monitor.core.models.auth import ApiKey
from db_monitor.main import app


# ── Helpers ────────────────────────────────────────────────────────────


def _make_admin_key() -> ApiKey:
    """Create an admin API key (no tenant scope)."""
    return ApiKey(
        id=1,
        key_hash="admin-hash",
        owner_name="admin",
        role="admin",
        is_active=True,
        tenant_id=None,
    )


def _make_tenant_key(tenant_id: str = "orderdb") -> ApiKey:
    """Create a tenant-scoped API key."""
    return ApiKey(
        id=2,
        key_hash="tenant-hash",
        owner_name=tenant_id,
        role="viewer",
        is_active=True,
        tenant_id=tenant_id,
    )


def _make_unscoped_viewer_key() -> ApiKey:
    """Create a viewer key without a tenant scope."""
    return ApiKey(
        id=3,
        key_hash="viewer-hash",
        owner_name="unscoped-viewer",
        role="viewer",
        is_active=True,
        tenant_id=None,
    )


ALL_TABLES = [
    {"id": 1, "service_name": "orderdb", "table_name": "orders", "columns": []},
    {"id": 2, "service_name": "catalogdb", "table_name": "products", "columns": []},
    {"id": 3, "service_name": "shippingdb", "table_name": "shipments", "columns": []},
]


@pytest.fixture
def client() -> TestClient:
    """Provide a client whose dependency overrides are isolated per test."""
    app.dependency_overrides.clear()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _authenticate_as(api_key: ApiKey) -> None:
    """Override the credential dependency so the app resolves ``api_key``."""

    async def _override() -> ApiKey:
        return api_key

    app.dependency_overrides[require_valid_api_key] = _override


# ── Model tests ───────────────────────────────────────────────────────


class TestApiKeyTenantId:
    """ApiKey model has tenant_id field with correct defaults."""

    def test_admin_key_has_null_tenant(self):
        """Admin keys have no tenant scope."""
        key = _make_admin_key()
        assert key.tenant_id is None

    def test_tenant_key_has_tenant_id(self):
        """Tenant-scoped keys have their tenant_id set."""
        key = _make_tenant_key("catalogdb")
        assert key.tenant_id == "catalogdb"


# ── Tenant filter dependency behavior ─────────────────────────────────


class TestGetTenantFilter:
    """get_tenant_filter resolves tenant scope through the app."""

    def test_admin_key_no_filter(self, client: TestClient):
        """Admin keys get no filter applied and see every tenant."""
        _authenticate_as(_make_admin_key())

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_instance = AsyncMock()
            mock_instance.get_all_tables.return_value = list(ALL_TABLES)
            mock_sd.return_value = mock_instance

            response = client.get("/tables")

        assert response.status_code == 200
        assert response.json()["count"] == 3

    def test_tenant_key_returns_filter(self, client: TestClient):
        """Tenant-scoped keys get their tenant as the filter value."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_instance = AsyncMock()
            mock_instance.get_all_tables.return_value = list(ALL_TABLES)
            mock_sd.return_value = mock_instance

            response = client.get("/tables")

        assert response.status_code == 200
        payload = response.json()
        assert payload["count"] == 1
        assert payload["tables"][0]["service_name"] == "orderdb"

    def test_unscoped_viewer_key_is_rejected(self, client: TestClient):
        """Viewer keys without a tenant must not bypass tenant filtering."""
        _authenticate_as(_make_unscoped_viewer_key())

        response = client.get("/tables")

        assert response.status_code == 403
        assert "tenant_id" in response.json()["detail"]


# ── Route enforcement tests ────────────────────────────────────────────


class TestEventRouteTenantEnforcement:
    """Event route enforces tenant scoping."""

    def _patch_repository(self):
        return patch("db_monitor.routes.data.events.get_events_repository")

    def test_admin_can_access_any_service(self, client: TestClient):
        """Admin keys can access events for any service_name."""
        _authenticate_as(_make_admin_key())

        with self._patch_repository() as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.list_events.return_value = ([], 0)
            mock_repo.return_value = mock_instance

            response = client.get(
                "/events",
                params={"limit": 10, "service_name": "shippingdb"},
            )

        assert response.status_code == 200
        filters = mock_instance.list_events.call_args.kwargs["filters"]
        assert filters.service_name == "shippingdb"

    def test_tenant_key_cannot_access_wrong_service(self, client: TestClient):
        """Tenant-scoped key gets 403 when requesting a different service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with self._patch_repository():
            response = client.get(
                "/events",
                params={"limit": 10, "service_name": "shippingdb"},
            )

        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "orderdb" in detail
        assert "shippingdb" in detail

    def test_tenant_key_accesses_own_service(self, client: TestClient):
        """Tenant-scoped key can access events for their own service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with self._patch_repository() as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.list_events.return_value = ([], 0)
            mock_repo.return_value = mock_instance

            response = client.get(
                "/events",
                params={"limit": 10, "service_name": "orderdb"},
            )

        assert response.status_code == 200
        filters = mock_instance.list_events.call_args.kwargs["filters"]
        assert filters.service_name == "orderdb"

    def test_tenant_key_forces_own_service_without_explicit_param(
        self,
        client: TestClient,
    ):
        """A tenant key is filtered even when no service_name is supplied."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with self._patch_repository() as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.list_events.return_value = ([], 0)
            mock_repo.return_value = mock_instance

            response = client.get("/events", params={"limit": 10})

        assert response.status_code == 200
        filters = mock_instance.list_events.call_args.kwargs["filters"]
        assert filters.service_name == "orderdb"


class TestChangeRouteTenantEnforcement:
    """Change route enforces tenant scoping."""

    def test_admin_can_access_any_service(self, client: TestClient):
        """Admin keys can access changes for any service."""
        _authenticate_as(_make_admin_key())

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
            patch(
                "db_monitor.routes.data.changes.get_schema_discovery"
            ) as mock_sd,
        ):
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_table_by_name.return_value = {
                "id": 1,
                "service_name": "orderdb",
                "table_name": "orders",
                "columns": [],
            }
            mock_sd.return_value = mock_sd_instance
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_changes.return_value = ([], 0)
            mock_cp.return_value = mock_cp_instance

            response = client.get(
                "/changes",
                params={
                    "table_name": "orders",
                    "service_name": "orderdb",
                    "limit": 10,
                },
            )

        assert response.status_code == 200
        assert response.json()["service_name"] == "orderdb"

    def test_tenant_key_gets_403_on_mismatch(self, client: TestClient):
        """Tenant-scoped key gets 403 when requesting changes for wrong service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        response = client.get(
            "/changes",
            params={
                "table_name": "orders",
                "service_name": "shippingdb",
                "limit": 10,
            },
        )

        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "orderdb" in detail
        assert "shippingdb" in detail

    def test_tenant_key_accesses_own_service(self, client: TestClient):
        """Tenant-scoped key can access changes for their own service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
            patch(
                "db_monitor.routes.data.changes.get_schema_discovery"
            ) as mock_sd,
        ):
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_table_by_name.return_value = {
                "id": 1,
                "service_name": "orderdb",
                "table_name": "orders",
                "columns": [],
            }
            mock_sd.return_value = mock_sd_instance
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_changes.return_value = ([], 0)
            mock_cp.return_value = mock_cp_instance

            response = client.get(
                "/changes",
                params={
                    "table_name": "orders",
                    "service_name": "orderdb",
                    "limit": 10,
                },
            )

        assert response.status_code == 200
        assert response.json()["service_name"] == "orderdb"


class TestTableRouteTenantEnforcement:
    """Table routes enforce tenant scoping."""

    def test_admin_sees_all_tables(self, client: TestClient):
        """Admin keys see all tables."""
        _authenticate_as(_make_admin_key())

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_all_tables.return_value = list(ALL_TABLES)
            mock_sd.return_value = mock_sd_instance

            response = client.get("/tables")

        assert response.status_code == 200
        assert response.json()["count"] == 3

    def test_tenant_key_sees_only_own_tables(self, client: TestClient):
        """Tenant-scoped keys only see their own tables."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_all_tables.return_value = list(ALL_TABLES)
            mock_sd.return_value = mock_sd_instance

            response = client.get("/tables")

        assert response.status_code == 200
        payload = response.json()
        assert payload["count"] == 1
        assert payload["tables"][0]["service_name"] == "orderdb"

    def test_tenant_key_gets_403_on_table_detail_mismatch(
        self,
        client: TestClient,
    ):
        """Tenant-scoped key gets 403 requesting table detail for wrong service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        response = client.get("/tables/shippingdb/shipments")

        assert response.status_code == 403
        assert "orderdb" in response.json()["detail"]

    def test_tenant_key_gets_403_on_columns_mismatch(
        self,
        client: TestClient,
    ):
        """Tenant-scoped key gets 403 requesting columns for wrong service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        response = client.get("/tables/shippingdb/shipments/columns")

        assert response.status_code == 403

    def test_tenant_key_can_get_own_table_detail(self, client: TestClient):
        """Tenant-scoped key can get table detail for their own service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_table_by_name.return_value = {
                "service_name": "orderdb",
                "table_name": "orders",
                "columns": [],
            }
            mock_sd.return_value = mock_sd_instance

            response = client.get("/tables/orderdb/orders")

        assert response.status_code == 200
        assert response.json()["service_name"] == "orderdb"

    def test_tenant_key_can_get_own_columns(self, client: TestClient):
        """Tenant-scoped key can get columns for their own service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_table_by_name.return_value = {
                "service_name": "orderdb",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "data_type": "integer"},
                ],
            }
            mock_sd.return_value = mock_sd_instance

            response = client.get("/tables/orderdb/orders/columns")

        assert response.status_code == 200
        assert len(response.json()["columns"]) == 1


class TestEventStatsTenantEnforcement:
    """Event stats route enforces tenant scoping."""

    def test_admin_can_access_stats(self, client: TestClient):
        """Admin keys can access event stats across tenants."""
        _authenticate_as(_make_admin_key())

        with patch(
            "db_monitor.routes.data.events.get_events_repository"
        ) as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.event_stats.return_value = {
                "total_events": 100,
                "by_service": {"orderdb": 50, "catalogdb": 50},
                "by_type": {},
                "by_operation": {},
            }
            mock_repo.return_value = mock_instance

            response = client.get("/events/stats")

        assert response.status_code == 200
        assert response.json()["total_events"] == 100
        mock_instance.event_stats.assert_awaited_once_with(
            tenant_filter=None
        )

    def test_tenant_key_sees_only_own_stats(self, client: TestClient):
        """Tenant-scoped keys pass their tenant to the repository."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.events.get_events_repository"
        ) as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.event_stats.return_value = {
                "total_events": 50,
                "by_service": {"orderdb": 50},
                "by_type": {},
                "by_operation": {},
            }
            mock_repo.return_value = mock_instance

            response = client.get("/events/stats")

        assert response.status_code == 200
        mock_instance.event_stats.assert_awaited_once_with(
            tenant_filter="orderdb"
        )
        assert "orderdb" in response.json()["by_service"]


class TestValueAtTimeRouteTenantEnforcement:
    """Value-at-time routes enforce tenant scoping."""

    def test_admin_can_access_any_service(self, client: TestClient):
        """Admin keys can access value-at-time for any service."""
        _authenticate_as(_make_admin_key())

        with patch(
            "db_monitor.routes.data.changes.get_change_processor"
        ) as mock_cp:
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "value"
            mock_cp.return_value = mock_cp_instance

            response = client.get(
                "/changes/orderdb/orders/status/at",
                params={"timestamp": "2026-01-01T00:00:00Z"},
            )

        assert response.status_code == 200
        assert response.json()["value"] == "value"

    def test_tenant_key_gets_403_on_mismatch(self, client: TestClient):
        """Tenant-scoped key gets 403 when requesting value for wrong service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        response = client.get(
            "/changes/shippingdb/shipments/status/at",
            params={"timestamp": "2026-01-01T00:00:00Z"},
        )

        assert response.status_code == 403

    def test_tenant_key_can_access_own_service(self, client: TestClient):
        """Tenant-scoped key can access value for their own service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        with patch(
            "db_monitor.routes.data.changes.get_change_processor"
        ) as mock_cp:
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "shipped"
            mock_cp.return_value = mock_cp_instance

            response = client.get(
                "/changes/orderdb/orders/status/at",
                params={"timestamp": "2026-01-01T00:00:00Z"},
            )

        assert response.status_code == 200
        assert response.json()["value"] == "shipped"

    def test_legacy_route_admin_access(self, client: TestClient):
        """Admin keys can use legacy value-at-time route."""
        _authenticate_as(_make_admin_key())

        with patch(
            "db_monitor.routes.data.changes.get_change_processor"
        ) as mock_cp:
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "value"
            mock_cp.return_value = mock_cp_instance

            response = client.get(
                "/changes/orders/status/at",
                params={
                    "timestamp": "2026-01-01T00:00:00Z",
                    "service_name": "orderdb",
                },
            )

        assert response.status_code == 200
        assert response.json()["value"] == "value"

    def test_legacy_route_tenant_gets_403(self, client: TestClient):
        """Tenant-scoped key gets 403 on legacy route with wrong service."""
        _authenticate_as(_make_tenant_key("orderdb"))

        response = client.get(
            "/changes/shipments/status/at",
            params={
                "timestamp": "2026-01-01T00:00:00Z",
                "service_name": "shippingdb",
            },
        )

        assert response.status_code == 403
