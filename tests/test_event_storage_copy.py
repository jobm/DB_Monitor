"""Live-PostgreSQL tests for the COPY-based batch write path.

These run against the ``migrated_postgres_url`` fixture, so they only need a
reachable PostgreSQL server with the migrations applied.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from db_monitor.core.models import KafkaEvent
from db_monitor.ingestion.event_storage.persist import (
    _store_batch_graph_with_retries,
)
from db_monitor.ingestion.pg_copy import jsonb


def _make_event(offset: int, event_time: datetime) -> KafkaEvent:
    payload = {
        "before": None,
        "after": {
            "id": offset,
            "name": f"row-{offset}",
            "amount": offset % 100,
        },
        "source": {
            "name": "orders",
            "db": "orderdb",
            "schema": "public",
            "table": "orders",
        },
        "op": "c",
    }
    return KafkaEvent(
        event_type="INSERT",
        event_time=event_time,
        user_id=None,
        service_name="orderdb",
        kafka_topic="orderdb.public.orders",
        kafka_partition=0,
        kafka_offset=offset,
        operation="INSERT",
        event_data=payload,
        raw_payload=json.dumps(payload, separators=(",", ":")),
    )


def _session_factory(url: str):
    engine = create_async_engine(url)
    factory = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, factory


def test_jsonb_quotes_plain_strings() -> None:
    assert jsonb(None) is None
    assert jsonb("row-1") == '"row-1"'
    assert jsonb({"id": 1}) == '{"id":1}'


@pytest.mark.slow
@pytest.mark.anyio
async def test_batch_write_persists_events_and_changes_with_copy(
    migrated_postgres_url: str,
) -> None:
    engine, session_factory = _session_factory(migrated_postgres_url)
    try:
        event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        events = [_make_event(offset, event_time) for offset in range(5)]

        inserted = await _store_batch_graph_with_retries(
            session_factory, events, max_retries=1
        )

        expected_keys = {
            (event.kafka_topic, event.kafka_partition, event.kafka_offset)
            for event in events
        }
        assert inserted == expected_keys
        assert all(event.id is not None for event in events)

        async with session_factory() as session:
            event_rows = (
                await session.execute(
                    text(
                        "SELECT id, event_data, row_identity, capture_time "
                        "FROM events WHERE kafka_topic = :topic "
                        "ORDER BY kafka_offset"
                    ),
                    {"topic": "orderdb.public.orders"},
                )
            ).all()
            change_rows = (
                await session.execute(
                    text(
                        "SELECT cc.event_id, mc.column_name, "
                        "cc.new_value, cc.old_value "
                        "FROM column_changes cc "
                        "JOIN monitored_columns mc ON mc.id = cc.column_id "
                        "ORDER BY cc.event_id, mc.column_name"
                    )
                )
            ).all()

        assert len(event_rows) == 5
        assert [row.id for row in event_rows] == [event.id for event in events]
        for row in event_rows:
            assert row.event_data["after"]["name"].startswith("row-")
            assert row.capture_time == event_time

        # Three discovered columns, each changed, for every inserted event.
        assert len(change_rows) == 15
        assert {row.event_id for row in change_rows} == {
            event.id for event in events
        }
        # Plain strings must be stored as JSON strings, not raw JSON tokens.
        name_values = [
            row.new_value
            for row in change_rows
            if row.column_name == "name"
        ]
        assert len(name_values) == 5
        assert all(
            isinstance(value, str) and value.startswith("row-")
            for value in name_values
        )
        assert all(row.old_value is None for row in change_rows)
    finally:
        await engine.dispose()


@pytest.mark.slow
@pytest.mark.anyio
async def test_duplicate_batch_is_skipped_by_copy_merge(
    migrated_postgres_url: str,
) -> None:
    engine, session_factory = _session_factory(migrated_postgres_url)
    try:
        event_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
        first_batch = [_make_event(offset, event_time) for offset in range(3)]

        first_inserted = await _store_batch_graph_with_retries(
            session_factory, first_batch, max_retries=1
        )
        assert len(first_inserted) == 3

        duplicate_batch = [_make_event(offset, event_time) for offset in range(3)]
        second_inserted = await _store_batch_graph_with_retries(
            session_factory, duplicate_batch, max_retries=1
        )
        assert second_inserted == set()

        async with session_factory() as session:
            event_count = (
                await session.execute(
                    text(
                        "SELECT count(*) FROM events "
                        "WHERE kafka_topic = :topic"
                    ),
                    {"topic": "orderdb.public.orders"},
                )
            ).scalar_one()
            change_count = (
                await session.execute(
                    text("SELECT count(*) FROM column_changes")
                )
            ).scalar_one()

        assert event_count == 3
        assert change_count == 9
    finally:
        await engine.dispose()
