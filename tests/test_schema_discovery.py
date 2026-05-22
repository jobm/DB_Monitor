from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pytest

import schema_discovery as schema_discovery_module
from models import KafkaEvent
from schema_discovery import SchemaDiscovery


def test_extract_column_definitions_falls_back_to_payload_rows():
    discovery = SchemaDiscovery(lambda: None)

    columns = discovery._extract_column_definitions(
        {
            "before": {"id": 1, "status": "pending"},
            "after": {
                "id": 1,
                "status": "paid",
                "active": True,
                "metadata": {"source": "checkout"},
            },
        }
    )

    by_name = {column["column_name"]: column for column in columns}

    assert set(by_name) == {"id", "status", "active", "metadata"}
    assert by_name["id"]["data_type"] == "integer"
    assert by_name["status"]["data_type"] == "string"
    assert by_name["active"]["data_type"] == "boolean"
    assert by_name["metadata"]["data_type"] == "object"


def test_extract_column_definitions_reads_debezium_schema_envelope():
    discovery = SchemaDiscovery(lambda: None)

    columns = discovery._extract_column_definitions(
        {
            "schema": {
                "fields": [
                    {
                        "field": "before",
                        "fields": [
                            {
                                "field": "id",
                                "type": "int32",
                                "optional": False,
                            },
                            {
                                "field": "status",
                                "type": "string",
                                "optional": True,
                            },
                        ],
                    },
                    {
                        "field": "after",
                        "fields": [
                            {
                                "field": "id",
                                "type": "int32",
                                "optional": False,
                            },
                            {
                                "field": "status",
                                "type": "string",
                                "optional": True,
                            },
                            {
                                "field": "__deleted",
                                "type": "string",
                                "optional": True,
                            },
                        ],
                    },
                ]
            }
        }
    )

    assert columns == [
        {
            "column_name": "id",
            "data_type": "int32",
            "is_primary_key": False,
            "is_nullable": False,
            "audit_enabled": True,
        },
        {
            "column_name": "status",
            "data_type": "string",
            "is_primary_key": False,
            "is_nullable": True,
            "audit_enabled": True,
        },
    ]


@pytest.mark.anyio
async def test_cluster_invalidation_clears_sibling_instance_cache(
) -> None:
    reader = SchemaDiscovery(lambda: None)
    writer = SchemaDiscovery(lambda: None)
    cache_key = reader._cache_key("orderdb", "orders")
    reader._table_cache[cache_key] = (
        time.monotonic(),
        {"id": 1, "table_name": "orders"},
    )

    async def fake_publish_invalidation(
        service_name: str,
        table_name: str,
    ) -> None:
        del service_name, table_name

    original_publish = (
        schema_discovery_module.schema_cache_backplane.publish_invalidation
    )
    schema_discovery_module.schema_cache_backplane.publish_invalidation = (
        fake_publish_invalidation
    )
    try:
        await writer._invalidate_cluster_cache("orderdb", "orders")
    finally:
        schema_discovery_module.schema_cache_backplane.publish_invalidation = (
            original_publish
        )

    assert cache_key not in reader._table_cache


@pytest.mark.anyio
async def test_schema_cache_backplane_notification_invalidates_cache(
) -> None:
    discovery = SchemaDiscovery(lambda: None)
    cache_key = discovery._cache_key("orderdb", "orders")
    discovery._table_cache[cache_key] = (
        time.monotonic(),
        {"id": 1, "table_name": "orders"},
    )

    payload = json.dumps(
        {
            "origin_instance_id": "remote-replica",
            "service_name": "orderdb",
            "table_name": "orders",
        }
    )

    await schema_discovery_module.schema_cache_backplane._deliver_notification(
        payload
    )

    assert cache_key not in discovery._table_cache


@pytest.mark.anyio
async def test_schema_cache_backplane_ignores_local_origin() -> None:
    discovery = SchemaDiscovery(lambda: None)
    cache_key = discovery._cache_key("orderdb", "orders")
    discovery._table_cache[cache_key] = (
        time.monotonic(),
        {"id": 1, "table_name": "orders"},
    )

    payload = json.dumps(
        {
            "origin_instance_id": (
                schema_discovery_module.schema_cache_backplane._instance_id
            ),
            "service_name": "orderdb",
            "table_name": "orders",
        }
    )

    await schema_discovery_module.schema_cache_backplane._deliver_notification(
        payload
    )

    assert cache_key in discovery._table_cache


@pytest.mark.anyio
async def test_process_event_uses_schema_qualified_table_name(monkeypatch):
    discovery = SchemaDiscovery(lambda: None)
    captured: dict[str, object] = {}

    async def fake_upsert_table(**kwargs):
        captured.update(kwargs)
        return 7

    async def fake_upsert_columns(session, table_id, column_definitions):
        del session, column_definitions
        assert table_id == 7

    async def fake_invalidate(service_name: str, table_name: str) -> None:
        captured["invalidated"] = (service_name, table_name)

    monkeypatch.setattr(discovery, "_upsert_table", fake_upsert_table)
    monkeypatch.setattr(discovery, "_upsert_columns", fake_upsert_columns)
    monkeypatch.setattr(
        discovery,
        "_invalidate_cluster_cache",
        fake_invalidate,
    )

    event = KafkaEvent(
        event_type="UPDATE",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="sqlserver.inventory.orders",
        kafka_topic="sqlserver.inventory.orders",
        kafka_partition=0,
        kafka_offset=1,
        event_data={
            "source": {
                "name": "connector-a",
                "db": "inventory",
                "schema": "sales",
                "table": "orders",
            },
            "after": {"id": 1},
        },
        raw_payload="{}",
        operation="UPDATE",
    )

    result = await discovery.process_event(event, session=object())

    assert result == 7
    assert captured["service_name"] == "sqlserver"
    assert captured["database_name"] == "inventory"
    assert captured["table_name"] == "sales.orders"
    assert captured["topic_name"] == "sqlserver.inventory.orders"
    assert captured["invalidated"] == ("sqlserver", "sales.orders")
