"""Top-level API router composed from focused route modules.

This module intentionally re-exports legacy symbols so existing tests and
imports that monkeypatch `routes.<symbol>` continue to work.
"""

from __future__ import annotations

from fastapi import APIRouter

from config import ALLOW_BOOTSTRAP
from consumer_service import (
    get_consumer_health,
    list_consumer_checkpoints_snapshot,
    list_dead_letter_events,
    replay_dead_letter_event_record,
    replay_dead_letter_event_records,
)
from extensions import AsyncSessionLocal
from lifecycle_manager import lifecycle_manager
from . import auth as routes_auth
from . import data as routes_data
from . import ops as routes_ops

router = APIRouter()
router.include_router(routes_data.router)
router.include_router(routes_ops.router)
router.include_router(routes_auth.router)

# Backward-compatible symbol aliases used by tests.
schema_discovery = routes_data.schema_discovery
change_processor = routes_data.change_processor


def _sync_test_overrides() -> None:
    """Propagate monkeypatched route-level symbols to split modules."""
    routes_data.AsyncSessionLocal = AsyncSessionLocal
    routes_data.schema_discovery = schema_discovery
    routes_data.change_processor = change_processor

    routes_ops.AsyncSessionLocal = AsyncSessionLocal
    routes_ops.get_consumer_health = get_consumer_health
    routes_ops.list_consumer_checkpoints_snapshot = (
        list_consumer_checkpoints_snapshot
    )
    routes_ops.list_dead_letter_records = list_dead_letter_events
    routes_ops.replay_dead_letter_event_records = (
        replay_dead_letter_event_records
    )
    routes_ops.replay_dead_letter_event_record = (
        replay_dead_letter_event_record
    )
    routes_ops.lifecycle_manager = lifecycle_manager

    routes_auth.AsyncSessionLocal = AsyncSessionLocal
    routes_auth.ALLOW_BOOTSTRAP = ALLOW_BOOTSTRAP


# Re-export data endpoints.
async def get_tables():
    _sync_test_overrides()
    return await routes_data.get_tables()


async def get_table(service_name: str, table_name: str):
    _sync_test_overrides()
    return await routes_data.get_table(service_name, table_name)


async def get_table_columns(service_name: str, table_name: str):
    _sync_test_overrides()
    return await routes_data.get_table_columns(service_name, table_name)


async def get_events(
    limit: int = 100,
    offset: int = 0,
    service_name: str | None = None,
    source_table_id: int | None = None,
    row_identity: str | None = None,
    event_type: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    search_term: str | None = None,
):
    _sync_test_overrides()
    return await routes_data.get_events(
        limit=limit,
        offset=offset,
        service_name=service_name,
        source_table_id=source_table_id,
        row_identity=row_identity,
        event_type=event_type,
        start_time=start_time,
        end_time=end_time,
        search_term=search_term,
    )


async def get_event_stats():
    _sync_test_overrides()
    return await routes_data.get_event_stats()


async def get_changes(
    table_name: str,
    service_name: str | None = None,
    column_name: str | None = None,
    row_identity: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
    limit: int = 100,
    offset: int = 0,
):
    _sync_test_overrides()
    return await routes_data.get_changes(
        table_name=table_name,
        service_name=service_name,
        column_name=column_name,
        row_identity=row_identity,
        from_time=from_time,
        to_time=to_time,
        limit=limit,
        offset=offset,
    )


async def get_value_at_time(
    service_name: str,
    table_name: str,
    column_name: str,
    timestamp: str,
):
    _sync_test_overrides()
    return await routes_data.get_value_at_time(
        service_name=service_name,
        table_name=table_name,
        column_name=column_name,
        timestamp=timestamp,
    )


async def get_value_at_time_legacy(
    table_name: str,
    column_name: str,
    timestamp: str,
    service_name: str | None = None,
):
    _sync_test_overrides()
    return await routes_data.get_value_at_time_legacy(
        table_name=table_name,
        column_name=column_name,
        timestamp=timestamp,
        service_name=service_name,
    )


# Re-export operational endpoints.
async def health_check():
    _sync_test_overrides()
    return await routes_ops.health_check()


async def live_check():
    _sync_test_overrides()
    return await routes_ops.live_check()


async def readiness_check():
    _sync_test_overrides()
    return await routes_ops.readiness_check()


async def app_info(request):
    _sync_test_overrides()
    return await routes_ops.app_info(request)


async def get_consumer_checkpoints(admin_api_key):
    _sync_test_overrides()
    return await routes_ops.get_consumer_checkpoints(admin_api_key)


async def get_dead_letter_events(
    limit: int = 100,
    include_replayed: bool = False,
    admin_api_key=None,
):
    _sync_test_overrides()
    return await routes_ops.get_dead_letter_events(
        limit=limit,
        include_replayed=include_replayed,
        admin_api_key=admin_api_key,
    )


async def replay_dead_letter_events(
    limit: int = 100,
    include_replayed: bool = False,
    admin_api_key=None,
):
    _sync_test_overrides()
    return await routes_ops.replay_dead_letter_events(
        limit=limit,
        include_replayed=include_replayed,
        admin_api_key=admin_api_key,
    )


async def replay_dead_letter_event(dlq_event_id: int, admin_api_key=None):
    _sync_test_overrides()
    return await routes_ops.replay_dead_letter_event(
        dlq_event_id=dlq_event_id,
        admin_api_key=admin_api_key,
    )


# Re-export auth endpoints.
async def create_access_token_exchange(authenticated_key):
    _sync_test_overrides()
    return await routes_auth.create_access_token_exchange(authenticated_key)


async def create_websocket_session_token(authenticated_key):
    _sync_test_overrides()
    return await routes_auth.create_websocket_session_token(authenticated_key)


async def create_api_key(
    owner_name: str,
    role: str = "viewer",
    ttl_days: int = 365,
    creator_api_key=None,
):
    _sync_test_overrides()
    return await routes_auth.create_api_key(
        owner_name=owner_name,
        role=role,
        ttl_days=ttl_days,
        creator_api_key=creator_api_key,
    )


async def list_api_keys(
    include_inactive: bool = False,
    creator_api_key=None,
):
    _sync_test_overrides()
    return await routes_auth.list_api_keys(
        include_inactive=include_inactive,
        creator_api_key=creator_api_key,
    )


async def rotate_api_key(
    key_id: int,
    ttl_days: int = 365,
    creator_api_key=None,
):
    _sync_test_overrides()
    return await routes_auth.rotate_api_key(
        key_id=key_id,
        ttl_days=ttl_days,
        creator_api_key=creator_api_key,
    )


async def revoke_api_key(key_id: int, creator_api_key=None):
    _sync_test_overrides()
    return await routes_auth.revoke_api_key(
        key_id=key_id,
        creator_api_key=creator_api_key,
    )


async def bootstrap_admin_key(owner_name: str, ttl_days: int = 365):
    _sync_test_overrides()
    return await routes_auth.bootstrap_admin_key(
        owner_name=owner_name,
        ttl_days=ttl_days,
    )
