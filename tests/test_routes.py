from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import routes.auth as routes_auth
import routes.data as routes_data
import routes.ops as routes_ops
from models import ApiKey, CustomerJWTSecretState, KafkaEvent


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

    def scalar_one_or_none(self):
        return self._scalar_value


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


class FakeApiKeysSession:
    def __init__(self, api_keys):
        self._api_keys = api_keys
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def execute(self, statement):
        self.statements.append(statement)
        return FakeExecuteResult(scalars=self._api_keys)


class FakeWriteSession:
    def __init__(self):
        self.statements = []
        self.added = []
        self._next_id = 1
        self.customer_jwt_records: dict[str, CustomerJWTSecretState] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def begin(self):
        return self

    def add(self, model):
        self.added.append(model)
        if isinstance(model, CustomerJWTSecretState):
            self.customer_jwt_records[model.customer_id] = model

    async def flush(self):
        for model in self.added:
            if getattr(model, "id", None) is None:
                setattr(model, "id", self._next_id)
                self._next_id += 1

    async def execute(self, statement):
        self.statements.append(statement)
        if "customer_jwt_secret_state" in str(statement):
            record = next(iter(self.customer_jwt_records.values()), None)
            return FakeExecuteResult(scalar_value=record)
        return FakeExecuteResult()


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
        return [{"id": 10, "event_id": 77, "column_name": "status"}]

    monkeypatch.setattr(
        routes_data,
        "get_schema_discovery",
        lambda: SimpleNamespace(get_table_by_name=fake_get_table_by_name),
    )
    monkeypatch.setattr(
        routes_data,
        "get_change_processor",
        lambda: SimpleNamespace(get_changes=fake_get_changes),
    )

    response = await routes_data.get_changes(
        table_name="orders",
        service_name="orderdb",
        column_name="status",
        row_identity='{"id": 7}',
        from_time="2024-01-01T10:00:00Z",
        to_time="2024-01-02T11:00:00Z",
        limit=25,
        offset=5,
    )

    assert response["service_name"] == "orderdb"
    assert response["table_name"] == "orders"
    assert response["count"] == 1
    assert response["offset"] == 5
    assert response["changes"][0]["event_id"] == 77
    assert captured["table_id"] == 1
    assert captured["column_id"] == 7
    assert captured["row_identity"] == {"id": 7}
    assert captured["limit"] == 25
    assert captured["offset"] == 5
    assert captured["from_time"] == datetime(
        2024,
        1,
        1,
        10,
        0,
        tzinfo=timezone.utc,
    )
    assert captured["to_time"] == datetime(
        2024,
        1,
        2,
        11,
        0,
        tzinfo=timezone.utc,
    )


@pytest.mark.anyio
async def test_get_changes_rejects_invalid_timestamp():
    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_changes(
            table_name="orderdb.orders",
            from_time="not-a-timestamp",
        )

    assert exc_info.value.status_code == 400
    assert "from_time" in exc_info.value.detail
    assert "2024-01-01T00:00:00Z" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_value_at_time_legacy_resolves_service_from_table_name(
    monkeypatch,
):
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

    monkeypatch.setattr(
        routes_data,
        "get_change_processor",
        lambda: SimpleNamespace(get_value_at_time=fake_get_value_at_time),
    )

    response = await routes_data.get_value_at_time_legacy(
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
    assert captured["timestamp"] == datetime(
        2024,
        1,
        3,
        12,
        30,
        tzinfo=timezone.utc,
    )


@pytest.mark.anyio
async def test_health_check_reports_database_and_consumer_state(monkeypatch):
    monkeypatch.setattr(
        routes_ops,
        "AsyncSessionLocal",
        lambda: FakeSession(),
    )
    monkeypatch.setattr(
        routes_ops,
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
    monkeypatch.setattr(
        routes_ops.lifecycle_manager,
        "_startup_complete",
        True,
    )
    monkeypatch.setattr(
        routes_ops.lifecycle_manager.shutdown_manager,
        "_shutdown_in_progress",
        False,
    )

    response = await routes_ops.health_check()

    assert response["status"] == "healthy"
    assert response["checks"]["database"]["status"] == "healthy"
    assert response["checks"]["consumer"]["status"] == "healthy"
    assert response["checks"]["lifecycle"]["status"] == "healthy"


@pytest.mark.anyio
async def test_readiness_check_returns_503_when_consumer_signals_degrade(
    monkeypatch,
):
    monkeypatch.setattr(
        routes_ops,
        "AsyncSessionLocal",
        lambda: FakeSession(),
    )
    monkeypatch.setattr(
        routes_ops,
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
    monkeypatch.setattr(
        routes_ops.lifecycle_manager,
        "_startup_complete",
        True,
    )
    monkeypatch.setattr(
        routes_ops.lifecycle_manager.shutdown_manager,
        "_shutdown_in_progress",
        False,
    )

    response = await routes_ops.readiness_check()
    payload = json.loads(response.body)

    assert response.status_code == 503
    assert payload["status"] == "not_ready"
    assert payload["checks"]["consumer"]["status"] == "unhealthy"


@pytest.mark.anyio
async def test_bootstrap_admin_key_rejects_when_disabled(monkeypatch):
    monkeypatch.setattr(routes_auth, "ALLOW_BOOTSTRAP", False)

    with pytest.raises(HTTPException) as exc_info:
        await routes_auth.bootstrap_admin_key(owner_name="admin")

    assert exc_info.value.status_code == 403
    assert "ALLOW_BOOTSTRAP=true" in exc_info.value.detail


@pytest.mark.anyio
async def test_bootstrap_customer_auth_scope_creates_admin_key_and_jwt_state(
    monkeypatch,
) -> None:
    routes_auth.CUSTOMER_JWT_SECRET_STATE.clear()
    fake_session = FakeWriteSession()
    monkeypatch.setattr(
        routes_auth,
        "AsyncSessionLocal",
        lambda: fake_session,
    )

    response = await routes_auth.bootstrap_customer_auth_scope(
        "customer-a",
        owner_suffix="ops-admin",
        ttl_days=30,
        customer_header="customer-a",
        creator_api_key=None,
    )

    assert response["customer_id"] == "customer-a"
    assert response["owner_name"] == "customer-a:ops-admin"
    assert response["api_key"].startswith("1.")
    assert response["jwt_state"]["generation"] == 1
    assert response["jwt_state"]["active_fingerprint"] is not None
    assert response["jwt_state"]["audit_count"] == 1
    assert response["jwt_state"]["last_action"] == "bootstrap"


@pytest.mark.anyio
async def test_rotate_and_recover_customer_jwt_secrets(monkeypatch) -> None:
    routes_auth.CUSTOMER_JWT_SECRET_STATE.clear()
    fake_session = FakeWriteSession()
    monkeypatch.setattr(
        routes_auth,
        "AsyncSessionLocal",
        lambda: fake_session,
    )

    await routes_auth.bootstrap_customer_auth_scope(
        "customer-a",
        owner_suffix="ops-admin",
        ttl_days=30,
        customer_header="customer-a",
        creator_api_key=None,
    )
    initial_active = routes_auth.CUSTOMER_JWT_SECRET_STATE[
        "customer-a"
    ]["active_secret"]

    rotated = await routes_auth.rotate_customer_jwt_secrets(
        "customer-a",
        customer_header="customer-a",
        creator_api_key=None,
    )
    rotated_state = routes_auth.CUSTOMER_JWT_SECRET_STATE["customer-a"]
    assert rotated["jwt_state"]["generation"] == 2
    assert rotated["jwt_state"]["last_action"] == "rotate"
    assert rotated_state["active_secret"] != initial_active
    assert rotated_state["previous_active_secret"] == initial_active

    recovered = await routes_auth.recover_customer_jwt_secrets(
        "customer-a",
        customer_header="customer-a",
        creator_api_key=None,
    )
    recovered_state = routes_auth.CUSTOMER_JWT_SECRET_STATE["customer-a"]
    assert recovered_state["active_secret"] == initial_active
    assert recovered["jwt_state"]["has_recovery_secret"] is True
    assert recovered["jwt_state"]["last_action"] == "recover"


@pytest.mark.anyio
async def test_rotate_customer_jwt_secrets_requires_matching_scope() -> None:
    routes_auth.CUSTOMER_JWT_SECRET_STATE.clear()
    routes_auth.CUSTOMER_JWT_SECRET_STATE["customer-a"] = {
        "active_secret": "active-secret",
        "next_secret": "next-secret",
        "previous_active_secret": None,
        "generation": 1,
        "last_rotated_at": "2024-01-01T00:00:00+00:00",
        "last_recovered_at": None,
    }

    with pytest.raises(HTTPException) as exc_info:
        await routes_auth.rotate_customer_jwt_secrets(
            "customer-a",
            customer_header="customer-b",
            creator_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must match" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_customer_jwt_state_requires_bootstrap_first(
    monkeypatch,
) -> None:
    routes_auth.CUSTOMER_JWT_SECRET_STATE.clear()
    monkeypatch.setattr(
        routes_auth,
        "AsyncSessionLocal",
        lambda: FakeWriteSession(),
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_auth.get_customer_jwt_state(
            "customer-a",
            creator_api_key=None,
        )

    assert exc_info.value.status_code == 404
    assert "auth/bootstrap" in exc_info.value.detail


@pytest.mark.anyio
async def test_bootstrap_customer_auth_scope_requires_customer_scope(
) -> None:
    with pytest.raises(HTTPException) as exc_info:
        await routes_auth.bootstrap_customer_auth_scope(
            "customer-a",
            owner_suffix="ops-admin",
            ttl_days=30,
            customer_header=None,
            creator_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "X-DBM-Customer-ID" in exc_info.value.detail


@pytest.mark.anyio
async def test_recover_customer_jwt_secrets_requires_matching_scope() -> None:
    routes_auth.CUSTOMER_JWT_SECRET_STATE.clear()
    routes_auth.CUSTOMER_JWT_SECRET_STATE["customer-a"] = {
        "active_secret": "active-secret",
        "next_secret": "next-secret",
        "previous_active_secret": "previous-secret",
        "generation": 2,
        "last_rotated_at": "2024-01-01T00:00:00+00:00",
        "last_recovered_at": None,
        "audit": [],
    }

    with pytest.raises(HTTPException) as exc_info:
        await routes_auth.recover_customer_jwt_secrets(
            "customer-a",
            customer_header="customer-b",
            creator_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must match" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_events_rejects_invalid_start_time():
    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_events(start_time="not-a-timestamp")

    assert exc_info.value.status_code == 400
    assert "start_time" in exc_info.value.detail
    assert "2024-01-01T00:00:00Z" in exc_info.value.detail


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
            row_identity={"id": 42},
            event_data={"after": {"status": "paid"}},
            raw_payload="{}",
        )
    ]
    fake_session = FakeEventsSession(events)

    monkeypatch.setattr(routes_data, "AsyncSessionLocal", lambda: fake_session)

    response = await routes_data.get_events(
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
    assert rendered_event["row_identity"] == {"id": 42}

    first_statement = str(fake_session.statements[0])
    second_statement = str(fake_session.statements[1])
    assert "events.service_name = :service_name_1" in first_statement
    assert "events.event_type = :event_type_1" in first_statement
    assert "events.event_time >= :event_time_1" in first_statement
    assert "events.event_time <= :event_time_2" in first_statement
    assert "LIMIT :param_1" in second_statement


@pytest.mark.anyio
async def test_get_events_supports_record_filters(monkeypatch):
    events = [
        KafkaEvent(
            id=2,
            event_type="UPDATE",
            event_time=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
            user_id=None,
            service_name="orderdb",
            operation="UPDATE",
            source_table_id=7,
            row_identity={"id": 42},
            event_data={"after": {"status": "paid"}},
            raw_payload="{}",
        )
    ]
    fake_session = FakeEventsSession(events)

    monkeypatch.setattr(routes_data, "AsyncSessionLocal", lambda: fake_session)

    response = await routes_data.get_events(
        service_name="orderdb",
        source_table_id=7,
        row_identity='{"id": 42}',
        limit=5,
        offset=0,
    )

    assert response["events"][0]["row_identity"] == {"id": 42}

    first_statement = str(fake_session.statements[0])
    assert "events.source_table_id = :source_table_id_1" in first_statement
    assert "events.row_identity = :row_identity_1" in first_statement


@pytest.mark.anyio
async def test_get_events_supports_cursor_pagination(monkeypatch):
    events = [
        KafkaEvent(
            id=50,
            event_type="UPDATE",
            event_time=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
            user_id=None,
            service_name="orderdb",
            operation="UPDATE",
            source_table_id=7,
            row_identity={"id": 42},
            event_data={"after": {"status": "paid"}},
            raw_payload="{}",
        ),
        KafkaEvent(
            id=49,
            event_type="UPDATE",
            event_time=datetime(2024, 1, 2, 12, 1, tzinfo=timezone.utc),
            user_id=None,
            service_name="orderdb",
            operation="UPDATE",
            source_table_id=7,
            row_identity={"id": 43},
            event_data={"after": {"status": "paid"}},
            raw_payload="{}",
        ),
    ]

    captured: dict[str, object] = {}

    async def fake_list_events(
        *,
        filters,
        limit,
        offset,
        cursor_id,
        include_total,
    ):
        del filters
        captured["limit"] = limit
        captured["offset"] = offset
        captured["cursor_id"] = cursor_id
        captured["include_total"] = include_total
        return events, None

    monkeypatch.setattr(
        routes_data,
        "get_events_repository",
        lambda: SimpleNamespace(list_events=fake_list_events),
    )

    response = await routes_data.get_events(limit=2, offset=100, cursor_id=99)

    assert captured["limit"] == 2
    assert captured["offset"] == 100
    assert captured["cursor_id"] == 99
    assert captured["include_total"] is False

    assert response["total"] is None
    assert response["offset"] == 0
    assert response["next_cursor"] == 49


@pytest.mark.anyio
async def test_get_events_rejects_invalid_row_identity():
    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_events(row_identity="not-json")

    assert exc_info.value.status_code == 400
    assert "row_identity" in exc_info.value.detail
    assert '{"id": 42}' in exc_info.value.detail


@pytest.mark.anyio
async def test_get_changes_rejects_invalid_row_identity():
    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_changes(
            table_name="orderdb.orders",
            row_identity="not-json",
        )

    assert exc_info.value.status_code == 400
    assert "row_identity" in exc_info.value.detail
    assert '{"id": 42}' in exc_info.value.detail


@pytest.mark.anyio
async def test_get_changes_requires_unambiguous_table_reference():
    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_changes(table_name="orders")

    assert exc_info.value.status_code == 400
    assert "service_name=orderdb&table_name=orders" in exc_info.value.detail
    assert "table_name=orderdb.orders" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_changes_reports_table_discovery_hint(monkeypatch):
    async def fake_get_table_by_name(service_name: str, table_name: str):
        del service_name, table_name
        return None

    monkeypatch.setattr(
        routes_data,
        "get_schema_discovery",
        lambda: SimpleNamespace(get_table_by_name=fake_get_table_by_name),
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_data.get_changes(table_name="orderdb.orders")

    assert exc_info.value.status_code == 404
    assert "GET /tables" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_consumer_checkpoints_returns_snapshot(monkeypatch):
    async def fake_snapshot():
        return [
            {
                "consumer_group": "fastapi-consumer-group",
                "broker_kind": "kafka",
                "broker_destination": "orderdb.public.orders",
                "broker_substream": "0",
                "broker_position": "22",
                "kafka_topic": "orderdb.public.orders",
                "kafka_partition": 0,
                "kafka_offset": 22,
                "last_event_time": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:01+00:00",
            }
        ]

    monkeypatch.setattr(
        routes_ops,
        "list_consumer_checkpoints_snapshot",
        fake_snapshot,
    )

    response = await routes_ops.get_consumer_checkpoints(
        admin_api_key=None
    )

    assert response["count"] == 1
    assert response["checkpoints"][0]["kafka_offset"] == 22
    assert response["checkpoints"][0]["broker_position"] == "22"


@pytest.mark.anyio
async def test_list_api_keys_filters_inactive_by_default(monkeypatch):
    now = datetime.now(timezone.utc)
    api_keys = [
        ApiKey(
            id=10,
            owner_name="active-admin",
            role="admin",
            is_active=True,
            expires_at=now + timedelta(days=10),
            created_at=now,
        ),
        ApiKey(
            id=11,
            owner_name="expired-viewer",
            role="viewer",
            is_active=True,
            expires_at=now - timedelta(days=1),
            created_at=now - timedelta(days=1),
        ),
        ApiKey(
            id=12,
            owner_name="revoked-admin",
            role="admin",
            is_active=False,
            revoked_at=now - timedelta(hours=1),
            created_at=now - timedelta(days=2),
        ),
    ]
    fake_session = FakeApiKeysSession(api_keys)
    monkeypatch.setattr(routes_auth, "AsyncSessionLocal", lambda: fake_session)

    response = await routes_auth.list_api_keys(
        include_inactive=False,
        creator_api_key=ApiKey(id=10, owner_name="active-admin", role="admin"),
    )

    assert response["count"] == 1
    assert response["current_api_key_id"] == 10
    assert response["keys"][0]["id"] == 10
    assert response["keys"][0]["status"] == "active"
    assert response["keys"][0]["current_authenticated"] is True
    assert response["keys"][0]["can_revoke"] is False


@pytest.mark.anyio
async def test_list_api_keys_can_include_inactive(monkeypatch):
    now = datetime.now(timezone.utc)
    api_keys = [
        ApiKey(
            id=20,
            owner_name="revoked-admin",
            role="admin",
            is_active=False,
            revoked_at=now - timedelta(hours=1),
            created_at=now,
        ),
        ApiKey(
            id=21,
            owner_name="expired-viewer",
            role="viewer",
            is_active=True,
            expires_at=now - timedelta(days=1),
            created_at=now - timedelta(days=1),
        ),
    ]
    fake_session = FakeApiKeysSession(api_keys)
    monkeypatch.setattr(routes_auth, "AsyncSessionLocal", lambda: fake_session)

    response = await routes_auth.list_api_keys(
        include_inactive=True,
        creator_api_key=ApiKey(id=99, owner_name="ops", role="admin"),
    )

    assert response["count"] == 2
    assert response["keys"][0]["status"] == "revoked"
    assert response["keys"][1]["status"] == "expired"
    assert response["keys"][0]["can_rotate"] is False


@pytest.mark.anyio
async def test_get_dead_letter_events_returns_pending_records(monkeypatch):
    async def fake_list_dead_letters(limit: int, include_replayed: bool):
        assert limit == 25
        assert include_replayed is False
        return [
            {
                "id": 4,
                "broker_kind": "rabbitmq",
                "broker_destination": "cdc.orders",
                "broker_substream": None,
                "broker_position": "77",
                "kafka_topic": "orderdb.public.orders",
                "kafka_partition": 0,
                "kafka_offset": 77,
                "is_replayed": False,
            }
        ]

    monkeypatch.setattr(
        routes_ops,
        "list_dead_letter_records",
        fake_list_dead_letters,
    )

    response = await routes_ops.get_dead_letter_events(
        limit=25,
        include_replayed=False,
        admin_api_key=None,
    )

    assert response["count"] == 1
    assert response["events"][0]["id"] == 4
    assert response["events"][0]["broker_kind"] == "rabbitmq"


@pytest.mark.anyio
async def test_replay_dead_letter_events_returns_batch_summary(monkeypatch):
    async def fake_replay_many(limit: int, include_replayed: bool):
        assert limit == 10
        assert include_replayed is True
        return {
            "results": [{"dlq_event_id": 1, "status": "replayed"}],
            "count": 1,
            "replayed_count": 1,
            "duplicate_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "requested_limit": 10,
            "include_replayed": True,
        }

    monkeypatch.setattr(
        routes_ops,
        "replay_dead_letter_event_records",
        fake_replay_many,
    )

    response = await routes_ops.replay_dead_letter_events(
        limit=10,
        include_replayed=True,
        customer_id="customer-a",
        admin_api_key=None,
    )

    assert response["count"] == 1
    assert response["replayed_count"] == 1
    assert response["results"][0]["status"] == "replayed"
    assert response["customer_id"] == "customer-a"


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

    monkeypatch.setattr(
        routes_ops,
        "replay_dead_letter_event_record",
        fake_replay,
    )

    response = await routes_ops.replay_dead_letter_event(
        9,
        customer_id="customer-a",
        admin_api_key=None,
    )

    assert response["status"] == "replayed"
    assert response["event_id"] == 100
    assert response["customer_id"] == "customer-a"


@pytest.mark.anyio
async def test_replay_dead_letter_event_returns_404_when_missing(monkeypatch):
    async def fake_replay(dlq_event_id: int):
        del dlq_event_id
        raise LookupError("DLQ event not found")

    monkeypatch.setattr(
        routes_ops,
        "replay_dead_letter_event_record",
        fake_replay,
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.replay_dead_letter_event(
            9,
            customer_id="customer-a",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 404
    assert "GET /admin/dlq" in exc_info.value.detail


@pytest.mark.anyio
async def test_replay_dead_letter_events_requires_customer_scope() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.replay_dead_letter_events(
            limit=10,
            include_replayed=False,
            customer_id="",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "X-DBM-Customer-ID" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_slo_policy_returns_tenant_cohorts(monkeypatch):
    monkeypatch.setattr(routes_ops, "SLO_ROLLING_WINDOW_DAYS", 30)
    monkeypatch.setattr(
        routes_ops,
        "TENANT_COHORT_SLO_POLICIES",
        [
            {
                "name": "small",
                "max_sources": 10,
                "availability_target": 99.9,
                "error_budget_percent": 0.1,
                "max_commit_age_seconds": 180,
                "max_consumer_lag": 500,
                "max_dlq_messages": 0,
            },
            {
                "name": "medium",
                "max_sources": 50,
                "availability_target": 99.5,
                "error_budget_percent": 0.5,
                "max_commit_age_seconds": 300,
                "max_consumer_lag": 1000,
                "max_dlq_messages": 5,
            },
        ],
    )

    response = await routes_ops.get_slo_policy(admin_api_key=None)

    assert response["window_days"] == 30
    assert response["count"] == 2
    assert response["cohorts"][0]["name"] == "small"


@pytest.mark.anyio
async def test_customer_lifecycle_suspend_resume_upgrade_flow(
) -> None:
    routes_ops.CUSTOMER_LIFECYCLE_STATE.clear()

    suspended = await routes_ops.suspend_customer_cell(
        "customer-a",
        reason="maintenance",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert suspended["status"] == "suspended"
    assert suspended["last_action"] == "suspend"
    assert suspended["reason"] == "maintenance"

    upgraded = await routes_ops.upgrade_customer_cell(
        "customer-a",
        target_version="v0.2.0",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert upgraded["last_action"] == "upgrade"
    assert upgraded["target_version"] == "v0.2.0"

    resumed = await routes_ops.resume_customer_cell(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert resumed["status"] == "active"
    assert resumed["last_action"] == "resume"
    assert resumed["reason"] is None


@pytest.mark.anyio
async def test_customer_lifecycle_mutations_require_matching_scope() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.suspend_customer_cell(
            "customer-a",
            reason=None,
            customer_header="customer-b",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must match" in exc_info.value.detail


@pytest.mark.anyio
async def test_get_customer_lifecycle_state_returns_default_when_missing(
) -> None:
    routes_ops.CUSTOMER_LIFECYCLE_STATE.clear()

    response = await routes_ops.get_customer_lifecycle_state(
        "customer-z",
        admin_api_key=None,
    )

    assert response["customer_id"] == "customer-z"
    assert response["status"] == "active"
    assert response["last_action"] == "none"


@pytest.mark.anyio
async def test_customer_provision_job_tracks_steps_and_audit() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()

    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert created["customer_id"] == "customer-a"
    assert created["status"] == "in_progress"
    assert created["steps"][0]["name"] == "register_customer"
    assert created["audit"][0]["action"] == "job_started"

    updated = await routes_ops.update_customer_provision_step(
        "customer-a",
        created["job_id"],
        "register_customer",
        step_status="completed",
        note="customer metadata persisted",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert updated["steps"][0]["status"] == "completed"
    assert updated["audit"][-1]["action"] == "step_updated"

    listed = await routes_ops.list_customer_provision_jobs(
        "customer-a",
        admin_api_key=None,
    )

    assert listed["count"] == 1
    assert listed["jobs"][0]["job_id"] == created["job_id"]


@pytest.mark.anyio
async def test_customer_provision_step_rejects_unknown_status() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.update_customer_provision_step(
            "customer-a",
            created["job_id"],
            "register_customer",
            step_status="done",
            note=None,
            customer_header="customer-a",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must be one of" in exc_info.value.detail


@pytest.mark.anyio
async def test_customer_provision_step_requires_matching_scope() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.update_customer_provision_step(
            "customer-a",
            created["job_id"],
            "register_customer",
            step_status="completed",
            note=None,
            customer_header="customer-b",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must match" in exc_info.value.detail


@pytest.mark.anyio
async def test_customer_provision_execute_advances_steps_in_order() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=2,
        fail_step=None,
        note="runner-execution",
        customer_header="customer-a",
        admin_api_key=None,
    )

    job = response["job"]
    assert response["executed_steps"] == 2
    assert job["steps"][0]["status"] == "completed"
    assert job["steps"][1]["status"] == "completed"
    assert job["status"] == "in_progress"
    assert job["audit"][-1]["action"] == "step_completed"


@pytest.mark.anyio
async def test_customer_provision_execute_supports_failure_simulation(
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        fail_step="register_customer",
        note="simulated failure",
        customer_header="customer-a",
        admin_api_key=None,
    )

    job = response["job"]
    assert response["executed_steps"] == 1
    assert job["steps"][0]["status"] == "failed"
    assert job["status"] == "failed"
    assert job["audit"][-1]["action"] == "step_failed"


@pytest.mark.anyio
async def test_customer_provision_execute_requires_matching_scope() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    with pytest.raises(HTTPException) as exc_info:
        await routes_ops.execute_customer_provision_job(
            "customer-a",
            created["job_id"],
            max_steps=1,
            fail_step=None,
            note=None,
            customer_header="customer-b",
            admin_api_key=None,
        )

    assert exc_info.value.status_code == 400
    assert "must match" in exc_info.value.detail


@pytest.mark.anyio
async def test_customer_provision_execute_is_idempotent_by_execution_id(
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    first = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        retry_failed=False,
        execution_id="exec-1",
        fail_step=None,
        note="first run",
        customer_header="customer-a",
        admin_api_key=None,
    )
    assert first["executed_steps"] == 1
    assert first["idempotent_replay"] is False

    second = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        retry_failed=False,
        execution_id="exec-1",
        fail_step=None,
        note="duplicate run",
        customer_header="customer-a",
        admin_api_key=None,
    )
    assert second["executed_steps"] == 0
    assert second["idempotent_replay"] is True


@pytest.mark.anyio
async def test_customer_provision_execute_can_retry_failed_step() -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    failed_run = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        retry_failed=False,
        execution_id="exec-fail",
        fail_step="register_customer",
        note="simulated failure",
        customer_header="customer-a",
        admin_api_key=None,
    )
    assert failed_run["job"]["steps"][0]["status"] == "failed"
    assert failed_run["job"]["steps"][0]["attempts"] == 1

    retried = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        retry_failed=True,
        execution_id="exec-retry",
        fail_step=None,
        note="retry after fix",
        customer_header="customer-a",
        admin_api_key=None,
    )

    retried_step = retried["job"]["steps"][0]
    assert retried_step["status"] == "completed"
    assert retried_step["attempts"] == 2
    assert retried_step["last_error"] is None
    assert retried_step["duration_seconds"] is not None


@pytest.mark.anyio
async def test_customer_provision_execute_persists_adapter_result(
    monkeypatch,
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()

    calls: list[str] = []

    async def fake_execute_provisioning_step(
        step_name: str,
        customer_id: str,
        job_id: str,
        note: str | None = None,
    ) -> dict[str, object]:
        del customer_id, job_id
        calls.append(step_name)
        return {
            "detail": f"{step_name} done",
            "adapter": "fake",
            "note": note,
        }

    monkeypatch.setattr(
        routes_ops,
        "execute_provisioning_step",
        fake_execute_provisioning_step,
    )

    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=2,
        retry_failed=False,
        execution_id="adapter-success",
        fail_step=None,
        note="adapter-note",
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert response["executed_steps"] == 2
    assert calls == ["register_customer", "generate_namespace"]
    assert response["job"]["steps"][0]["result"]["adapter"] == "fake"
    assert response["job"]["steps"][0]["note"] == "register_customer done"


@pytest.mark.anyio
async def test_customer_provision_execute_maps_adapter_error_to_step_failure(
    monkeypatch,
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()

    async def fake_execute_provisioning_step(
        step_name: str,
        customer_id: str,
        job_id: str,
        note: str | None = None,
    ) -> dict[str, object]:
        del customer_id, job_id, note
        raise routes_ops.ProvisioningStepExecutionError(
            f"adapter failure at {step_name}"
        )

    monkeypatch.setattr(
        routes_ops,
        "execute_provisioning_step",
        fake_execute_provisioning_step,
    )

    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=1,
        retry_failed=False,
        execution_id="adapter-failure",
        fail_step=None,
        note=None,
        customer_header="customer-a",
        admin_api_key=None,
    )

    failed_step = response["job"]["steps"][0]
    assert response["executed_steps"] == 1
    assert response["job"]["status"] == "failed"
    assert failed_step["status"] == "failed"
    assert "adapter failure" in str(failed_step["last_error"])
    assert response["job"]["audit"][-1]["action"] == "step_failed"


@pytest.mark.anyio
async def test_customer_provision_execute_adds_guardrail_and_observability(
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()

    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=6,
        retry_failed=False,
        execution_id="control-plane-metadata",
        fail_step=None,
        note=None,
        customer_header="customer-a",
        admin_api_key=None,
    )

    job = response["job"]
    assert response["executed_steps"] == 6
    assert job["guardrails"]["resource_quota"]["kind"] == "ResourceQuota"
    assert job["guardrails"]["network_policy"]["kind"] == "NetworkPolicy"
    assert job["observability_labels"]["customer_id"] == "customer-a"
    assert job["alert_routing"]["matchers"]["customer_id"] == "customer-a"


@pytest.mark.anyio
async def test_get_customer_guardrails_returns_templates() -> None:
    response = await routes_ops.get_customer_guardrails(
        "customer-a",
        admin_api_key=None,
    )

    guardrails = response["guardrails"]
    assert response["customer_id"] == "customer-a"
    assert guardrails["resource_quota"]["kind"] == "ResourceQuota"
    assert guardrails["limit_range"]["kind"] == "LimitRange"
    assert guardrails["network_policy"]["kind"] == "NetworkPolicy"


@pytest.mark.anyio
async def test_get_customer_observability_profile_returns_route() -> None:
    response = await routes_ops.get_customer_observability_profile(
        "customer-a",
        admin_api_key=None,
    )

    assert response["customer_id"] == "customer-a"
    assert response["labels"]["customer_id"] == "customer-a"
    assert response["alert_routing"]["matchers"]["customer_id"] == "customer-a"


@pytest.mark.anyio
async def test_customer_bootstrap_step_marks_lifecycle_active(
    monkeypatch,
) -> None:
    routes_ops.CUSTOMER_PROVISION_JOBS.clear()
    routes_ops.CUSTOMER_LIFECYCLE_STATE.clear()

    async def fake_execute_provisioning_step(
        step_name: str,
        customer_id: str,
        job_id: str,
        note: str | None = None,
    ) -> dict[str, object]:
        del customer_id, job_id, note
        return {
            "detail": f"{step_name} done",
        }

    monkeypatch.setattr(
        routes_ops,
        "execute_provisioning_step",
        fake_execute_provisioning_step,
    )

    created = await routes_ops.start_customer_provision_job(
        "customer-a",
        customer_header="customer-a",
        admin_api_key=None,
    )

    response = await routes_ops.execute_customer_provision_job(
        "customer-a",
        created["job_id"],
        max_steps=8,
        retry_failed=False,
        execution_id="bootstrap-complete",
        fail_step=None,
        note=None,
        customer_header="customer-a",
        admin_api_key=None,
    )

    assert response["job"]["status"] == "completed"
    lifecycle = routes_ops.CUSTOMER_LIFECYCLE_STATE["customer-a"]
    assert lifecycle["status"] == "active"
    assert lifecycle["last_action"] == "bootstrap_activate"
