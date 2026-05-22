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


class FakeScalarListResult:
    def __init__(self, values=None):
        self._values = values or []

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def begin(self):
        return FakeTransaction()

    async def execute(self, statement):
        del statement
        return FakeScalarListResult()


class FakeScalarIdsSession(FakeSession):
    def __init__(self, values):
        self._values = values

    async def execute(self, statement):
        del statement
        return FakeScalarListResult(self._values)


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


class FakeAssignedConsumer:
    def __init__(self, *topics, **kwargs):
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.assignments = []

    async def start(self):
        self.started = True

    def assign(self, partitions):
        self.assignments = list(partitions)

    async def stop(self):
        self.stopped = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


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


def test_checkpoint_rows_for_events_keeps_highest_offset_per_partition(
) -> None:
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

    def fake_record_successful_commit(_runtime):
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
        runtime=consumer_service.consumer_runtime,
    )

    assert consumer.commit_calls == 1
    assert checkpoint_events == [[event]]
    assert commit_recorded == [True]


@pytest.mark.anyio
async def test_broadcast_event_delivers_webhooks(monkeypatch):
    event = KafkaEvent(
        id=77,
        event_type="UPDATE",
        event_time=datetime.now(timezone.utc),
        user_id="user-1",
        service_name="orderdb",
        kafka_topic="orders.events",
        kafka_partition=0,
        kafka_offset=10,
        source_table_id=4,
        row_identity={"id": 5},
        event_data={"payload": {"after": {"id": 5}}},
        raw_payload="{}",
        operation="UPDATE",
    )
    websocket_messages = []
    webhook_messages = []

    async def fake_broadcast(message, event_id=None):
        websocket_messages.append((message, event_id))

    async def fake_send_message(message):
        webhook_messages.append(message)

    monkeypatch.setattr(
        consumer_service.ws_manager,
        "broadcast",
        fake_broadcast,
    )
    monkeypatch.setattr(
        consumer_service.webhook_notifier,
        "send_message",
        fake_send_message,
    )

    await consumer_service._broadcast_event(event)

    assert websocket_messages == [
        (
            {
                "type": "new_event",
                "event": {
                    "id": 77,
                    "event_type": "UPDATE",
                    "event_time": event.event_time.isoformat(),
                    "user_id": "user-1",
                    "service_name": "orderdb",
                    "operation": "UPDATE",
                    "source_table_id": 4,
                    "row_identity": {"id": 5},
                    "event_data": {"payload": {"after": {"id": 5}}},
                },
            },
            77,
        )
    ]
    assert webhook_messages == [websocket_messages[0][0]]


@pytest.mark.anyio
async def test_consumer_task_assigns_explicit_topic_partitions(monkeypatch):
    fake_consumer = None

    def fake_consumer_factory(*topics, **kwargs):
        nonlocal fake_consumer
        fake_consumer = FakeAssignedConsumer(*topics, **kwargs)
        return fake_consumer

    async def fake_refresh_dead_letter_backlog_metric(*_args, **_kwargs):
        return 0

    monkeypatch.setattr(
        consumer_service,
        "AIOKafkaConsumer",
        fake_consumer_factory,
    )
    monkeypatch.setattr(
        consumer_service,
        "refresh_dead_letter_backlog_metric",
        fake_refresh_dead_letter_backlog_metric,
    )

    await consumer_service.consumer_task(
        cluster_name="placement-test",
        bootstrap_servers=["kafka-a:9092"],
        consumer_group="group-a",
        topic_partitions={"orders.events": [0, 2]},
        enable_dlq=False,
        enable_batch=False,
    )

    assert fake_consumer is not None
    assert fake_consumer.started is True
    assert fake_consumer.stopped is True
    assert fake_consumer.topics == ()
    assert fake_consumer.kwargs["group_id"] == "group-a"
    assert [
        (partition.topic, partition.partition)
        for partition in fake_consumer.assignments
    ] == [("orders.events", 0), ("orders.events", 2)]


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


@pytest.mark.anyio
async def test_replay_dead_letter_event_records_aggregates_results(
    monkeypatch,
):
    async def fake_replay_dead_letter_event_record(
        dlq_event_id: int,
        session_factory=None,
    ):
        del session_factory
        if dlq_event_id == 1:
            return {"dlq_event_id": 1, "status": "replayed"}
        if dlq_event_id == 2:
            return {"dlq_event_id": 2, "status": "duplicate"}
        raise RuntimeError("replay failed")

    monkeypatch.setattr(
        consumer_service,
        "replay_dead_letter_event_record",
        fake_replay_dead_letter_event_record,
    )

    result = await consumer_service.replay_dead_letter_event_records(
        limit=3,
        session_factory=lambda: FakeScalarIdsSession([1, 2, 3]),
    )

    assert result["count"] == 3
    assert result["replayed_count"] == 1
    assert result["duplicate_count"] == 1
    assert result["failed_count"] == 1
    assert result["results"][2]["status"] == "failed"


def test_get_consumer_health_aggregates_multiple_cluster_runtimes() -> None:
    consumer_service.consumer_runtime.clear()
    consumer_service.consumer_runtime.update(
        {
            "running": True,
            "connected": True,
            "topics": ["orders.events"],
            "last_error": None,
            "last_message_at": "2026-05-22T10:00:00+00:00",
            "last_commit_at": "2026-05-22T10:00:01+00:00",
            "lag_by_partition": {"default:orders.events:0": 1},
            "lag_total": 1,
            "dlq_messages_total": 2,
        }
    )
    consumer_service.consumer_runtimes.clear()
    consumer_service.consumer_runtimes.update(
        {
            "default": consumer_service.consumer_runtime,
            "secondary": {
                "running": True,
                "connected": False,
                "topics": ["shipping.events"],
                "last_error": "timeout",
                "last_message_at": "2026-05-22T10:00:02+00:00",
                "last_commit_at": None,
                "lag_by_partition": {"secondary:shipping.events:1": 4},
                "lag_total": 4,
                "dlq_messages_total": 2,
            },
        }
    )
    consumer_service.cluster_circuit_breakers.clear()
    consumer_service.cluster_circuit_breakers.update(
        {
            "default": consumer_service.CircuitBreaker(
                metric_name="test_default"
            ),
            "secondary": consumer_service.CircuitBreaker(
                metric_name="test_secondary"
            ),
        }
    )
    consumer_service.cluster_circuit_breakers["secondary"].state = "open"

    health = consumer_service.get_consumer_health()

    assert health["status"] == "unhealthy"
    assert health["running"] is True
    assert health["connected"] is False
    assert health["lag_total"] == 5
    assert health["topics"] == ["orders.events", "shipping.events"]
    assert health["circuit_breaker_state"] == "open"
    assert "secondary" in health["clusters"]
    assert health["clusters"]["secondary"]["last_error"] == "timeout"
