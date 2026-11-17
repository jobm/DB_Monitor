"""Tests for multi-tenant isolation via API key tenant scope (OS-7).

Covers:
- Tenant ID field on ApiKey model
- get_tenant_filter dependency returns correct tenant for scoped/admin keys
- Event route enforces tenant filter (403 on mismatch)
- Change route enforces tenant filter
- Table routes filter by tenant
- Admin keys bypass tenant filtering
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db_monitor.core.models.auth import ApiKey


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


# ── Auth dependency tests ──────────────────────────────────────────────


class TestGetCurrentTenantId:
    """get_current_tenant_id dependency returns correct tenant."""

    @pytest.mark.asyncio
    async def test_admin_key_returns_none(self):
        """Admin keys return None for tenant_id."""
        from db_monitor.auth.deps import get_current_tenant_id

        admin_key = _make_admin_key()
        result = await get_current_tenant_id(api_key_record=admin_key)
        assert result is None

    @pytest.mark.asyncio
    async def test_tenant_key_returns_tenant(self):
        """Tenant-scoped keys return their tenant_id."""
        from db_monitor.auth.deps import get_current_tenant_id

        tenant_key = _make_tenant_key("shippingdb")
        result = await get_current_tenant_id(api_key_record=tenant_key)
        assert result == "shippingdb"


class TestGetTenantFilter:
    """get_tenant_filter dependency returns tenant filter for scoped keys."""

    @pytest.mark.asyncio
    async def test_admin_key_no_filter(self):
        """Admin keys get no filter applied."""
        from db_monitor.auth.deps import get_tenant_filter

        result = await get_tenant_filter(
            tenant_id=None,
            api_key_record=_make_admin_key(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_tenant_key_returns_filter(self):
        """Tenant-scoped keys get their tenant as the filter value."""
        from db_monitor.auth.deps import get_tenant_filter

        result = await get_tenant_filter(
            tenant_id="orderdb",
            api_key_record=_make_tenant_key("orderdb"),
        )
        assert result == "orderdb"

    @pytest.mark.asyncio
    async def test_unscoped_viewer_key_is_rejected(self):
        """Viewer keys without a tenant must not bypass tenant filtering."""
        from fastapi import HTTPException

        from db_monitor.auth.deps import get_tenant_filter

        with pytest.raises(HTTPException) as exc_info:
            await get_tenant_filter(
                tenant_id=None,
                api_key_record=_make_unscoped_viewer_key(),
            )

        assert exc_info.value.status_code == 403
        assert "tenant_id" in exc_info.value.detail


# ── Route enforcement tests ────────────────────────────────────────────


class TestEventRouteTenantEnforcement:
    """Event route enforces tenant scoping."""

    @pytest.mark.asyncio
    async def test_admin_can_access_any_service(self):
        """Admin keys can access events for any service_name."""
        from db_monitor.routes.data.events import get_events

        # Patch dependencies to inject admin key
        with (
            patch(
                "db_monitor.routes.data.events.get_events_repository"
            ) as mock_repo,
            patch(
                "db_monitor.routes.data.events.require_viewer_role"
            ) as mock_auth,
        ):
            mock_auth.return_value = _make_admin_key()
            mock_instance = AsyncMock()
            mock_instance.list_events.return_value = ([], 0)
            mock_repo.return_value = mock_instance

            await get_events(
                limit=10,
                offset=0,
                service_name="orderdb",
                tenant_filter=None,
            )
            # Should not raise — admin has access
            assert True

    @pytest.mark.asyncio
    async def test_tenant_key_cannot_access_wrong_service(self):
        """Tenant-scoped key gets 403 when requesting a different service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.events import get_events

        with patch(
            "db_monitor.routes.data.events.get_events_repository"
        ) as mock_repo:
            mock_repo.return_value.list_events = AsyncMock(return_value=([], 0))

            with pytest.raises(HTTPException) as exc_info:
                await get_events(
                    limit=10,
                    offset=0,
                    service_name="shippingdb",
                    tenant_filter="orderdb",
                )
            assert exc_info.value.status_code == 403
            assert "orderdb" in str(exc_info.value.detail)
            assert "shippingdb" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_tenant_key_accesses_own_service(self):
        """Tenant-scoped key can access events for their own service."""
        from db_monitor.routes.data.events import get_events

        with (
            patch(
                "db_monitor.routes.data.events.get_events_repository"
            ) as mock_repo,
        ):
            mock_instance = AsyncMock()
            mock_instance.list_events.return_value = ([], 0)
            mock_repo.return_value = mock_instance

            await get_events(
                limit=10,
                offset=0,
                service_name="orderdb",
                tenant_filter="orderdb",
            )
            # Should not raise
            assert True


class TestChangeRouteTenantEnforcement:
    """Change route enforces tenant scoping."""

    @pytest.mark.asyncio
    async def test_admin_can_access_any_service(self):
        """Admin keys can access changes for any service."""
        from db_monitor.routes.data.changes import get_changes

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

            await get_changes(
                table_name="orders",
                service_name="orderdb",
                limit=10,
                offset=0,
                tenant_filter=None,
            )
            assert True

    @pytest.mark.asyncio
    async def test_tenant_key_gets_403_on_mismatch(self):
        """Tenant-scoped key gets 403 when requesting changes for wrong service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.changes import get_changes

        with pytest.raises(HTTPException) as exc_info:
            await get_changes(
                table_name="orders",
                service_name="shippingdb",
                limit=10,
                offset=0,
                tenant_filter="orderdb",
            )
        assert exc_info.value.status_code == 403
        assert "orderdb" in str(exc_info.value.detail)
        assert "shippingdb" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_tenant_key_accesses_own_service(self):
        """Tenant-scoped key can access changes for their own service."""
        from db_monitor.routes.data.changes import get_changes

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
            patch(
                "db_monitor.routes.data.changes.get_schema_discovery"
            ) as mock_sd,
            patch(
                "db_monitor.routes.data.changes.resolve_table_reference"
            ) as mock_ref,
        ):
            mock_ref.return_value = ("orderdb", "orders")
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

            await get_changes(
                table_name="orders",
                service_name="orderdb",
                limit=10,
                offset=0,
                tenant_filter="orderdb",
            )
            assert True


class TestTableRouteTenantEnforcement:
    """Table routes enforce tenant scoping."""

    @pytest.mark.asyncio
    async def test_admin_sees_all_tables(self):
        """Admin keys see all tables."""
        from db_monitor.routes.data.tables import get_tables

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_all_tables.return_value = [
                {"service_name": "orderdb", "table_name": "orders"},
                {"service_name": "catalogdb", "table_name": "products"},
                {"service_name": "shippingdb", "table_name": "shipments"},
            ]
            mock_sd.return_value = mock_sd_instance

            result = await get_tables(tenant_filter=None)
            assert result["count"] == 3

    @pytest.mark.asyncio
    async def test_tenant_key_sees_only_own_tables(self):
        """Tenant-scoped keys only see their own tables."""
        from db_monitor.routes.data.tables import get_tables

        with patch(
            "db_monitor.routes.data.tables.get_schema_discovery"
        ) as mock_sd:
            mock_sd_instance = AsyncMock()
            mock_sd_instance.get_all_tables.return_value = [
                {"service_name": "orderdb", "table_name": "orders"},
                {"service_name": "catalogdb", "table_name": "products"},
                {"service_name": "shippingdb", "table_name": "shipments"},
            ]
            mock_sd.return_value = mock_sd_instance

            result = await get_tables(tenant_filter="orderdb")
            assert result["count"] == 1
            assert result["tables"][0]["service_name"] == "orderdb"

    @pytest.mark.asyncio
    async def test_tenant_key_gets_403_on_table_detail_mismatch(self):
        """Tenant-scoped key gets 403 requesting table detail for wrong service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.tables import get_table

        with pytest.raises(HTTPException) as exc_info:
            await get_table(
                service_name="shippingdb",
                table_name="shipments",
                tenant_filter="orderdb",
            )
        assert exc_info.value.status_code == 403
        assert "orderdb" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_tenant_key_gets_403_on_columns_mismatch(self):
        """Tenant-scoped key gets 403 requesting columns for wrong service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.tables import get_table_columns

        with pytest.raises(HTTPException) as exc_info:
            await get_table_columns(
                service_name="shippingdb",
                table_name="shipments",
                tenant_filter="orderdb",
            )
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_tenant_key_can_get_own_table_detail(self):
        """Tenant-scoped key can get table detail for their own service."""
        from db_monitor.routes.data.tables import get_table

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

            result = await get_table(
                service_name="orderdb",
                table_name="orders",
                tenant_filter="orderdb",
            )
            assert result["service_name"] == "orderdb"

    @pytest.mark.asyncio
    async def test_tenant_key_can_get_own_columns(self):
        """Tenant-scoped key can get columns for their own service."""
        from db_monitor.routes.data.tables import get_table_columns

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

            result = await get_table_columns(
                service_name="orderdb",
                table_name="orders",
                tenant_filter="orderdb",
            )
            assert len(result["columns"]) == 1


class TestEventStatsTenantEnforcement:
    """Event stats route enforces tenant scoping."""

    @pytest.mark.asyncio
    async def test_admin_can_access_stats(self):
        """Admin keys can access event stats."""
        from db_monitor.routes.data.events import get_event_stats

        with patch(
            "db_monitor.routes.data.events.get_events_repository"
        ) as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.event_stats.return_value = {
                "total_events": 100,
                "by_service": {
                    "orderdb": 50,
                    "catalogdb": 50,
                },
                "by_type": {},
                "by_operation": {},
            }
            mock_repo.return_value = mock_instance

            result = await get_event_stats(tenant_filter=None)
            assert result["total_events"] == 100

    @pytest.mark.asyncio
    async def test_tenant_key_sees_only_own_stats(self):
        """Tenant-scoped keys pass their tenant to the repository."""
        from db_monitor.routes.data.events import get_event_stats

        with patch(
            "db_monitor.routes.data.events.get_events_repository"
        ) as mock_repo:
            mock_instance = AsyncMock()
            mock_instance.event_stats.return_value = {
                "total_events": 50,
                "by_service": {
                    "orderdb": 50,
                },
                "by_type": {},
                "by_operation": {},
            }
            mock_repo.return_value = mock_instance

            result = await get_event_stats(tenant_filter="orderdb")
            # Verify the repository was called with the tenant filter
            mock_instance.event_stats.assert_called_once_with(
                tenant_filter="orderdb"
            )
            assert result["total_events"] == 50
            assert "orderdb" in result["by_service"]


class TestValueAtTimeRouteTenantEnforcement:
    """Value-at-time routes enforce tenant scoping."""

    @pytest.mark.asyncio
    async def test_admin_can_access_any_service(self):
        """Admin keys can access value-at-time for any service."""
        from db_monitor.routes.data.changes import get_value_at_time

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
        ):
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "value"
            mock_cp.return_value = mock_cp_instance

            result = await get_value_at_time(
                service_name="orderdb",
                table_name="orders",
                column_name="status",
                timestamp="2026-01-01T00:00:00Z",
                tenant_filter=None,
            )
            assert result["value"] == "value"

    @pytest.mark.asyncio
    async def test_tenant_key_gets_403_on_mismatch(self):
        """Tenant-scoped key gets 403 when requesting value for wrong service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.changes import get_value_at_time

        with pytest.raises(HTTPException) as exc_info:
            await get_value_at_time(
                service_name="shippingdb",
                table_name="shipments",
                column_name="status",
                timestamp="2026-01-01T00:00:00Z",
                tenant_filter="orderdb",
            )
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_tenant_key_can_access_own_service(self):
        """Tenant-scoped key can access value for their own service."""
        from db_monitor.routes.data.changes import get_value_at_time

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
        ):
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "shipped"
            mock_cp.return_value = mock_cp_instance

            result = await get_value_at_time(
                service_name="orderdb",
                table_name="orders",
                column_name="status",
                timestamp="2026-01-01T00:00:00Z",
                tenant_filter="orderdb",
            )
            assert result["value"] == "shipped"

    @pytest.mark.asyncio
    async def test_legacy_route_admin_access(self):
        """Admin keys can use legacy value-at-time route."""
        from db_monitor.routes.data.changes import get_value_at_time_legacy

        with (
            patch(
                "db_monitor.routes.data.changes.get_change_processor"
            ) as mock_cp,
            patch(
                "db_monitor.routes.data.changes.resolve_table_reference"
            ) as mock_ref,
        ):
            mock_ref.return_value = ("orderdb", "orders")
            mock_cp_instance = AsyncMock()
            mock_cp_instance.get_value_at_time.return_value = "value"
            mock_cp.return_value = mock_cp_instance

            result = await get_value_at_time_legacy(
                table_name="orders",
                column_name="status",
                timestamp="2026-01-01T00:00:00Z",
                service_name="orderdb",
                tenant_filter=None,
            )
            assert result["value"] == "value"

    @pytest.mark.asyncio
    async def test_legacy_route_tenant_gets_403(self):
        """Tenant-scoped key gets 403 on legacy route with wrong service."""
        from fastapi import HTTPException
        from db_monitor.routes.data.changes import get_value_at_time_legacy

        with pytest.raises(HTTPException) as exc_info:
            await get_value_at_time_legacy(
                table_name="shipments",
                column_name="status",
                timestamp="2026-01-01T00:00:00Z",
                service_name="shippingdb",
                tenant_filter="orderdb",
            )
        assert exc_info.value.status_code == 403
