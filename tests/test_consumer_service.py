from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

import consumer_service
from models import KafkaEvent


class FakeTimerMetric:
    def labels(self, **_kwargs):
        return self

    @contextmanager
    def time(self):
        yield


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def begin(self):
        return FakeTransaction()


class FakeScalarOneResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class FakeCountSession(FakeSession):
    def __init__(self, count: int):
        self._count = count

    async def execute(self, statement):
        del statement
        return FakeScalarOneResult(self._count)


class FakeConsumer:
    def __init__(self):
        self.commit_calls = 0

    async def commit(self):
        self.commit_calls += 1


@pytest.mark.anyio
async def test_store_event_graph_persists_changes_once(monkeypatch):
    event = KafkaEvent(
        event_type="c",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="orderdb",
        kafka_topic="orderdb.public.orders",
        kafka_partition=1,
        kafka_offset=22,
        event_data={"after": {"id": 1}},
        raw_payload="{}",
        operation="INSERT",
    )
    captured: dict[str, object] = {}

    async def fake_process_schema(event_obj, session=None):
        captured["schema_session"] = session
        return 7

    async def fake_insert_record(session, event_obj):
        captured["insert_session"] = session
        event_obj.id = 99
        return True

    async def fake_process_changes(event_obj, session=None):
        captured["changes_session"] = session
        captured["change_event_id"] = event_obj.id

    monkeypatch.setattr(
        consumer_service,
        "db_write_duration_seconds",
        FakeTimerMetric(),
    )
    monkeypatch.setattr(
        consumer_service.schema_discovery,
        "process_event",
        fake_process_schema,
    )
    monkeypatch.setattr(
        consumer_service,
        "_insert_event_record",
        fake_insert_record,
    )
    monkeypatch.setattr(
        consumer_service.change_processor,
        "process_event",
        fake_process_changes,
    )

    inserted = await consumer_service._store_event_graph_with_retries(
        lambda: FakeSession(),
        event,
    )

    assert inserted is True
    assert event.id == 99
    assert event.source_table_id == 7
    assert captured["change_event_id"] == 99
    assert captured["schema_session"] is captured["insert_session"]
    assert captured["insert_session"] is captured["changes_session"]


@pytest.mark.anyio
async def test_store_event_graph_skips_changes_for_duplicates(monkeypatch):
    event = KafkaEvent(
        event_type="u",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="orderdb",
        kafka_topic="orderdb.public.orders",
        kafka_partition=1,
        kafka_offset=23,
        event_data={"after": {"id": 1}},
        raw_payload="{}",
        operation="UPDATE",
    )
    change_calls = 0

    async def fake_process_schema(_event_obj, session=None):
        return 7

    async def fake_insert_record(_session, _event_obj):
        return False

    async def fake_process_changes(_event_obj, session=None):
        nonlocal change_calls
        change_calls += 1

    monkeypatch.setattr(
        consumer_service,
        "db_write_duration_seconds",
        FakeTimerMetric(),
    )
    monkeypatch.setattr(
        consumer_service.schema_discovery,
        "process_event",
        fake_process_schema,
    )
    monkeypatch.setattr(
        consumer_service,
        "_insert_event_record",
        fake_insert_record,
    )
    monkeypatch.setattr(
        consumer_service.change_processor,
        "process_event",
        fake_process_changes,
    )

    inserted = await consumer_service._store_event_graph_with_retries(
        lambda: FakeSession(),
        event,
    )

    assert inserted is False
    assert event.id is None
    assert event.source_table_id == 7
    assert change_calls == 0


def test_checkpoint_rows_for_events_keeps_highest_offset_per_partition() -> None:
    now = datetime.now(timezone.utc)
    events = [
        KafkaEvent(
            event_type="u",
            event_time=now,
            service_name="orderdb",
            kafka_topic="orderdb.public.orders",
            kafka_partition=0,
            kafka_offset=10,
            raw_payload="{}",
            operation="UPDATE",
        ),
        KafkaEvent(
            event_type="u",
            event_time=now,
            service_name="orderdb",
            kafka_topic="orderdb.public.orders",
            kafka_partition=0,
            kafka_offset=12,
            raw_payload="{}",
            operation="UPDATE",
        ),
        KafkaEvent(
            event_type="u",
            event_time=now,
            service_name="orderdb",
            kafka_topic="orderdb.public.orders",
            kafka_partition=1,
            kafka_offset=5,
            raw_payload="{}",
            operation="UPDATE",
        ),
    ]

    checkpoints = consumer_service._checkpoint_rows_for_events(events)

    assert checkpoints == [
        {
            "consumer_group": consumer_service.KAFKA_CONSUMER_GROUP,
            "kafka_topic": "orderdb.public.orders",
            "kafka_partition": 0,
            "kafka_offset": 12,
            "last_event_time": now,
        },
        {
            "consumer_group": consumer_service.KAFKA_CONSUMER_GROUP,
            "kafka_topic": "orderdb.public.orders",
            "kafka_partition": 1,
            "kafka_offset": 5,
            "last_event_time": now,
        },
    ]


@pytest.mark.anyio
async def test_process_single_event_commits_after_dlq_persist(monkeypatch):
    event = KafkaEvent(
        event_type="u",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="orderdb",
        kafka_topic="orderdb.public.orders",
        kafka_partition=2,
        kafka_offset=44,
        event_data={"after": {"id": 1}},
        raw_payload="{}",
        operation="UPDATE",
    )
    consumer = FakeConsumer()
    checkpoint_events: list[list[KafkaEvent]] = []
    commit_recorded: list[bool] = []

    async def fake_store(*_args, **_kwargs):
        raise RuntimeError("boom")

    async def fake_send_to_dlq(*_args, **_kwargs):
        return True

    async def fake_persist_checkpoints(events):
        checkpoint_events.append(events)

    def fake_record_successful_commit():
        commit_recorded.append(True)

    monkeypatch.setattr(
        consumer_service,
        "_store_event_graph_with_retries",
        fake_store,
    )
    monkeypatch.setattr(consumer_service, "send_to_dlq", fake_send_to_dlq)
    monkeypatch.setattr(
        consumer_service,
        "persist_consumer_checkpoints",
        fake_persist_checkpoints,
    )
    monkeypatch.setattr(
        consumer_service,
        "_record_successful_commit",
        fake_record_successful_commit,
    )
    monkeypatch.setattr(
        consumer_service,
        "kafka_commit_duration_seconds",
        FakeTimerMetric(),
    )

    await consumer_service._process_single_event(
        event,
        consumer,
        producer=None,
        enable_dlq=True,
        raw_data=event.raw_payload,
    )

    assert consumer.commit_calls == 1
    assert checkpoint_events == [[event]]
    assert commit_recorded == [True]


@pytest.mark.anyio
async def test_refresh_dead_letter_backlog_metric_updates_runtime_and_gauge():
    consumer_service.consumer_runtime["dlq_messages_total"] = 0
    consumer_service.dlq_records_pending.set(0)

    pending_count = await consumer_service.refresh_dead_letter_backlog_metric(
        lambda: FakeCountSession(3)
    )

    assert pending_count == 3
    assert consumer_service.consumer_runtime["dlq_messages_total"] == 3
    assert consumer_service.dlq_records_pending._value.get() == 3
