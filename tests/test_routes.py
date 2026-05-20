from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

import routes
from models import KafkaEvent


class FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def execute(self, statement):
        return 1


class FakeScalarResult:
    def __init__(self, values):
        self._values = values

    def all(self):
        return list(self._values)


class FakeExecuteResult:
    def __init__(self, scalar_value=None, scalars=None):
        self._scalar_value = scalar_value
        self._scalars = scalars or []

    def scalar(self):
        return self._scalar_value

    def scalars(self):
        return FakeScalarResult(self._scalars)


class FakeEventsSession:
    def __init__(self, events):
        self._events = events
        self.statements = []
        self._count_call = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def execute(self, statement):
        self.statements.append(statement)
        self._count_call += 1
        if self._count_call == 1:
            return FakeExecuteResult(scalar_value=len(self._events))
        return FakeExecuteResult(scalars=self._events)


@pytest.mark.anyio
async def test_get_changes_parses_service_and_timestamps(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_table_by_name(service_name: str, table_name: str):
        assert service_name == "orderdb"
        assert table_name == "orders"
        return {
            "id": 1,
            "columns": [{"id": 7, "column_name": "status"}],
        }

    async def fake_get_changes(**kwargs):
        captured.update(kwargs)
        return [{"id": 10, "column_name": "status"}]

    monkeypatch.setattr(routes.schema_discovery, "get_table_by_name", fake_get_table_by_name)
    monkeypatch.setattr(routes.change_processor, "get_changes", fake_get_changes)

    response = await routes.get_changes(
        table_name="orders",
        service_name="orderdb",
        column_name="status",
        from_time="2024-01-01T10:00:00Z",
        to_time="2024-01-02T11:00:00Z",
        limit=25,
    )

    assert response["service_name"] == "orderdb"
    assert response["table_name"] == "orders"
    assert response["count"] == 1
    assert captured["table_id"] == 1
    assert captured["column_id"] == 7
    assert captured["limit"] == 25
    assert captured["from_time"] == datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert captured["to_time"] == datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc)


@pytest.mark.anyio
async def test_get_changes_rejects_invalid_timestamp():
    with pytest.raises(HTTPException) as exc_info:
        await routes.get_changes(
            table_name="orderdb.orders",
            from_time="not-a-timestamp",
        )

    assert exc_info.value.status_code == 400
    assert "from_time" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_value_at_time_legacy_resolves_service_from_table_name(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_value_at_time(
        service_name: str,
        table_name: str,
        column_name: str,
        timestamp: datetime,
    ):
        captured.update(
            service_name=service_name,
            table_name=table_name,
            column_name=column_name,
            timestamp=timestamp,
        )
        return "shipped"

    monkeypatch.setattr(routes.change_processor, "get_value_at_time", fake_get_value_at_time)

    response = await routes.get_value_at_time_legacy(
        table_name="orderdb.orders",
        column_name="status",
        timestamp="2024-01-03T12:30:00Z",
    )

    assert response["service_name"] == "orderdb"
    assert response["table_name"] == "orders"
    assert response["value"] == "shipped"
    assert captured["service_name"] == "orderdb"
    assert captured["table_name"] == "orders"
    assert captured["column_name"] == "status"
    assert captured["timestamp"] == datetime(2024, 1, 3, 12, 30, tzinfo=timezone.utc)


@pytest.mark.anyio
async def test_health_check_reports_database_and_consumer_state(monkeypatch):
    monkeypatch.setattr(routes, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(
        routes,
        "get_consumer_health",
        lambda: {
            "status": "healthy",
            "running": True,
            "connected": True,
            "topics": ["orderdb.public.orders"],
            "last_error": None,
            "last_message_at": "2024-01-01T00:00:00+00:00",
            "last_commit_at": "2024-01-01T00:00:01+00:00",
            "lag_total": 0,
            "lag_by_partition": {},
            "dlq_messages_total": 0,
            "circuit_breaker_state": "closed",
        },
    )
    monkeypatch.setattr(routes.lifecycle_manager, "_startup_complete", True)
    monkeypatch.setattr(routes.lifecycle_manager.shutdown_manager, "_shutdown_in_progress", False)

    response = await routes.health_check()

    assert response["status"] == "healthy"
    assert response["checks"]["database"]["status"] == "healthy"
    assert response["checks"]["consumer"]["status"] == "healthy"
    assert response["checks"]["lifecycle"]["status"] == "healthy"


@pytest.mark.anyio
async def test_readiness_check_returns_503_when_consumer_signals_degrade(monkeypatch):
    monkeypatch.setattr(routes, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(
        routes,
        "get_consumer_health",
        lambda: {
            "status": "healthy",
            "running": True,
            "connected": True,
            "topics": ["orderdb.public.orders"],
            "last_error": None,
            "last_message_at": "2024-01-01T00:00:00+00:00",
            "last_commit_at": "2024-01-01T00:00:01+00:00",
            "lag_total": 5001,
            "lag_by_partition": {"orderdb.public.orders:0": 5001},
            "dlq_messages_total": 1,
            "circuit_breaker_state": "closed",
        },
    )
    monkeypatch.setattr(routes.lifecycle_manager, "_startup_complete", True)
    monkeypatch.setattr(routes.lifecycle_manager.shutdown_manager, "_shutdown_in_progress", False)

    response = await routes.readiness_check()
    payload = json.loads(response.body)

    assert response.status_code == 503
    assert payload["status"] == "not_ready"
    assert payload["checks"]["consumer"]["status"] == "unhealthy"


@pytest.mark.anyio
async def test_bootstrap_admin_key_rejects_when_disabled(monkeypatch):
    monkeypatch.setattr(routes, "ALLOW_BOOTSTRAP", False)

    with pytest.raises(HTTPException) as exc_info:
        await routes.bootstrap_admin_key(owner_name="admin")

    assert exc_info.value.status_code == 403


@pytest.mark.anyio
async def test_get_events_rejects_invalid_start_time():
    with pytest.raises(HTTPException) as exc_info:
        await routes.get_events(start_time="not-a-timestamp")

    assert exc_info.value.status_code == 400
    assert "start_time" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_events_parses_time_filters(monkeypatch):
    events = [
        KafkaEvent(
            id=1,
            event_type="UPDATE",
            event_time=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
            user_id=None,
            service_name="orderdb",
            operation="UPDATE",
            source_table_id=7,
            event_data={"after": {"status": "paid"}},
            raw_payload="{}",
        )
    ]
    fake_session = FakeEventsSession(events)

    monkeypatch.setattr(routes, "AsyncSessionLocal", lambda: fake_session)

    response = await routes.get_events(
        service_name="orderdb",
        event_type="UPDATE",
        start_time="2024-01-01T10:00:00Z",
        end_time="2024-01-03T11:00:00Z",
        limit=5,
        offset=0,
    )

    rendered_event = response["events"][0]
    assert response["total"] == 1
    assert rendered_event["service_name"] == "orderdb"
    assert rendered_event["event_type"] == "UPDATE"

    first_statement = str(fake_session.statements[0])
    second_statement = str(fake_session.statements[1])
    assert "events.service_name = :service_name_1" in first_statement
    assert "events.event_type = :event_type_1" in first_statement
    assert "events.event_time >= :event_time_1" in first_statement
    assert "events.event_time <= :event_time_2" in first_statement
    assert "LIMIT :param_1" in second_statement


@pytest.mark.anyio
async def test_get_consumer_checkpoints_returns_snapshot(monkeypatch):
    async def fake_snapshot():
        return [
            {
                "consumer_group": "fastapi-consumer-group",
                "kafka_topic": "orderdb.public.orders",
                "kafka_partition": 0,
                "kafka_offset": 22,
                "last_event_time": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:01+00:00",
            }
        ]

    monkeypatch.setattr(
        routes,
        "list_consumer_checkpoints_snapshot",
        fake_snapshot,
    )

    response = await routes.get_consumer_checkpoints(admin_api_key=None)

    assert response["count"] == 1
    assert response["checkpoints"][0]["kafka_offset"] == 22


@pytest.mark.anyio
async def test_get_dead_letter_events_returns_pending_records(monkeypatch):
    async def fake_list_dead_letters(limit: int, include_replayed: bool):
        assert limit == 25
        assert include_replayed is False
        return [
            {
                "id": 4,
                "kafka_topic": "orderdb.public.orders",
                "kafka_partition": 0,
                "kafka_offset": 77,
                "is_replayed": False,
            }
        ]

    monkeypatch.setattr(routes, "list_dead_letter_events", fake_list_dead_letters)

    response = await routes.get_dead_letter_events(
        limit=25,
        include_replayed=False,
        admin_api_key=None,
    )

    assert response["count"] == 1
    assert response["events"][0]["id"] == 4


@pytest.mark.anyio
async def test_replay_dead_letter_event_returns_replay_result(monkeypatch):
    async def fake_replay(dlq_event_id: int):
        assert dlq_event_id == 9
        return {
            "dlq_event_id": 9,
            "status": "replayed",
            "inserted": True,
            "event_id": 100,
        }

    monkeypatch.setattr(routes, "replay_dead_letter_event_record", fake_replay)

    response = await routes.replay_dead_letter_event(9, admin_api_key=None)

    assert response["status"] == "replayed"
    assert response["event_id"] == 100


@pytest.mark.anyio
async def test_replay_dead_letter_event_returns_404_when_missing(monkeypatch):
    async def fake_replay(dlq_event_id: int):
        del dlq_event_id
        raise LookupError("DLQ event not found")

    monkeypatch.setattr(routes, "replay_dead_letter_event_record", fake_replay)

    with pytest.raises(HTTPException) as exc_info:
        await routes.replay_dead_letter_event(9, admin_api_key=None)

    assert exc_info.value.status_code == 404
