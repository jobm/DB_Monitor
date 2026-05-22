from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi import Response
from starlette.requests import Request

import main


async def _receive() -> dict[str, Any]:
    """Provide an empty request body to Starlette's request wrapper."""
    return {"type": "http.request", "body": b"", "more_body": False}


def _build_request(headers: list[tuple[bytes, bytes]]) -> Request:
    """Create a minimal ASGI request for middleware testing."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/info",
        "raw_path": b"/info",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope, receive=_receive)


class FakeWebSocket:
    """Minimal WebSocket test double for auth failure handling."""

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}
        self.close_code: int | None = None
        self.close_reason: str | None = None

    async def close(self, code: int, reason: str) -> None:
        """Capture the close reason without opening a real socket."""
        self.close_code = code
        self.close_reason = reason


@pytest.mark.anyio
async def test_http_invalid_api_key_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP requests with invalid API keys should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="http",
        credential_type="api_key",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    async def fake_call_next(_request: Request) -> Response:
        return Response(status_code=401)

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )
    monkeypatch.setattr(main.audit_log_writer, "enqueue", lambda entry: None)

    request = _build_request([(b"x-api-key", b"invalid")])
    await main.log_requests(request, fake_call_next)

    assert counter._value.get() == before + 1


@pytest.mark.anyio
async def test_http_invalid_bearer_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP invalid bearer tokens should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="http",
        credential_type="bearer",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    async def fake_call_next(_request: Request) -> Response:
        return Response(status_code=401)

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )
    monkeypatch.setattr(main.audit_log_writer, "enqueue", lambda entry: None)

    request = _build_request(
        [(b"authorization", b"Bearer invalid-token")]
    )
    await main.log_requests(request, fake_call_next)

    assert counter._value.get() == before + 1


@pytest.mark.anyio
async def test_websocket_invalid_session_token_increments_failed_auth_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid WebSocket session tokens should increment the counter."""
    counter = main.failed_auth_attempts_total.labels(
        surface="websocket",
        credential_type="session_token",
    )
    before = counter._value.get()

    async def fake_authenticate_credentials(**_kwargs):
        return None

    monkeypatch.setattr(
        main,
        "authenticate_credentials",
        fake_authenticate_credentials,
    )

    websocket = FakeWebSocket()
    await main.ws_events(websocket, session_token="invalid-session-token")

    assert websocket.close_code == 4001
    assert websocket.close_reason == "Invalid API key"
    assert counter._value.get() == before + 1


@pytest.mark.anyio
async def test_lifespan_manager_starts_one_consumer_per_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """App startup should create one consumer task per configured cluster."""
    started_clusters: list[tuple[str, str, list[str]]] = []

    async def fake_startup() -> None:
        return None

    async def fake_shutdown() -> None:
        return None

    def fake_setup_signal_handlers(_loop) -> None:
        return None

    def fake_register_task(_task) -> None:
        return None

    async def fake_audit_log_run() -> None:
        return None

    async def fake_start_schema_cache_listener():
        return None

    async def fake_consumer_task(**kwargs):
        started_clusters.append(
            (
                kwargs["cluster_name"],
                kwargs["broker_kind"],
                list(kwargs["topics"]),
            )
        )

    async def fake_start_backplane_listener():
        return None

    monkeypatch.setattr(main.lifecycle_manager, "startup", fake_startup)
    monkeypatch.setattr(main.lifecycle_manager, "shutdown", fake_shutdown)
    monkeypatch.setattr(
        main.lifecycle_manager,
        "setup_signal_handlers",
        fake_setup_signal_handlers,
    )
    monkeypatch.setattr(
        main.lifecycle_manager,
        "register_task",
        fake_register_task,
    )
    monkeypatch.setattr(main, "consumer_task", fake_consumer_task)
    monkeypatch.setattr(main.audit_log_writer, "run", fake_audit_log_run)
    monkeypatch.setattr(
        main.schema_cache_backplane,
        "start_listener",
        fake_start_schema_cache_listener,
    )
    monkeypatch.setattr(
        main.ws_manager,
        "start_backplane_listener",
        fake_start_backplane_listener,
    )
    monkeypatch.setattr(
        main,
        "BROKER_CLUSTERS",
        [
            {
                "name": "primary",
                "broker_kind": "kafka",
                "bootstrap_servers": ["kafka-a:9092"],
                "topics": ["orders.events"],
                "topic_partitions": None,
                "consumer_group": "group-a",
                "connection_url": None,
                "queue_names": None,
                "prefetch_count": None,
                "dlq_destination": "db-monitor-dlq",
            },
            {
                "name": "secondary",
                "broker_kind": "rabbitmq",
                "bootstrap_servers": None,
                "topics": ["cdc.shipments"],
                "topic_partitions": None,
                "consumer_group": "group-b",
                "connection_url": "amqp://rabbit/",
                "queue_names": ["cdc.shipments"],
                "prefetch_count": 50,
                "dlq_destination": "db-monitor-dlq-rabbit",
            },
        ],
    )

    async with main.lifespan_manager(main.app):
        await asyncio.sleep(0)

    assert started_clusters == [
        ("primary", "kafka", ["orders.events"]),
        ("secondary", "rabbitmq", ["cdc.shipments"]),
    ]


@pytest.mark.anyio
async def test_lifespan_manager_passes_topic_partitions_to_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """App startup should pass explicit partition placement to consumers."""
    captured_assignments: list[dict[str, list[int]] | None] = []

    async def fake_startup() -> None:
        return None

    async def fake_shutdown() -> None:
        return None

    def fake_setup_signal_handlers(_loop) -> None:
        return None

    def fake_register_task(_task) -> None:
        return None

    async def fake_audit_log_run() -> None:
        return None

    async def fake_start_schema_cache_listener():
        return None

    async def fake_consumer_task(**kwargs):
        captured_assignments.append(kwargs.get("topic_partitions"))

    async def fake_start_backplane_listener():
        return None

    monkeypatch.setattr(main.lifecycle_manager, "startup", fake_startup)
    monkeypatch.setattr(main.lifecycle_manager, "shutdown", fake_shutdown)
    monkeypatch.setattr(
        main.lifecycle_manager,
        "setup_signal_handlers",
        fake_setup_signal_handlers,
    )
    monkeypatch.setattr(
        main.lifecycle_manager,
        "register_task",
        fake_register_task,
    )
    monkeypatch.setattr(main, "consumer_task", fake_consumer_task)
    monkeypatch.setattr(main.audit_log_writer, "run", fake_audit_log_run)
    monkeypatch.setattr(
        main.schema_cache_backplane,
        "start_listener",
        fake_start_schema_cache_listener,
    )
    monkeypatch.setattr(
        main.ws_manager,
        "start_backplane_listener",
        fake_start_backplane_listener,
    )
    monkeypatch.setattr(
        main,
        "BROKER_CLUSTERS",
        [
            {
                "name": "primary",
                "broker_kind": "kafka",
                "bootstrap_servers": ["kafka-a:9092"],
                "topics": ["orders.events"],
                "topic_partitions": {"orders.events": [0, 2]},
                "consumer_group": "group-a",
                "connection_url": None,
                "queue_names": None,
                "prefetch_count": None,
                "dlq_destination": "db-monitor-dlq",
            }
        ],
    )

    async with main.lifespan_manager(main.app):
        await asyncio.sleep(0)

    assert captured_assignments == [{"orders.events": [0, 2]}]


@pytest.mark.anyio
async def test_lifespan_manager_passes_rabbitmq_cluster_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """App startup should pass RabbitMQ-specific args to consumers."""
    captured: list[dict[str, object]] = []

    async def fake_startup() -> None:
        return None

    async def fake_shutdown() -> None:
        return None

    def fake_setup_signal_handlers(_loop) -> None:
        return None

    def fake_register_task(_task) -> None:
        return None

    async def fake_audit_log_run() -> None:
        return None

    async def fake_start_schema_cache_listener():
        return None

    async def fake_consumer_task(**kwargs):
        captured.append(kwargs)

    async def fake_start_backplane_listener():
        return None

    monkeypatch.setattr(main.lifecycle_manager, "startup", fake_startup)
    monkeypatch.setattr(main.lifecycle_manager, "shutdown", fake_shutdown)
    monkeypatch.setattr(
        main.lifecycle_manager,
        "setup_signal_handlers",
        fake_setup_signal_handlers,
    )
    monkeypatch.setattr(
        main.lifecycle_manager,
        "register_task",
        fake_register_task,
    )
    monkeypatch.setattr(main, "consumer_task", fake_consumer_task)
    monkeypatch.setattr(main.audit_log_writer, "run", fake_audit_log_run)
    monkeypatch.setattr(
        main.schema_cache_backplane,
        "start_listener",
        fake_start_schema_cache_listener,
    )
    monkeypatch.setattr(
        main.ws_manager,
        "start_backplane_listener",
        fake_start_backplane_listener,
    )
    monkeypatch.setattr(
        main,
        "BROKER_CLUSTERS",
        [
            {
                "name": "rabbit",
                "broker_kind": "rabbitmq",
                "bootstrap_servers": None,
                "topics": ["cdc.orders"],
                "topic_partitions": None,
                "consumer_group": "group-a",
                "connection_url": "amqp://rabbit/",
                "queue_names": ["cdc.orders"],
                "prefetch_count": 25,
                "dlq_destination": "db-monitor-rabbit-dlq",
            }
        ],
    )

    async with main.lifespan_manager(main.app):
        await asyncio.sleep(0)

    assert captured == [
        {
            "cluster_name": "rabbit",
            "broker_kind": "rabbitmq",
            "bootstrap_servers": None,
            "consumer_group": "group-a",
            "topics": ["cdc.orders"],
            "topic_partitions": None,
            "connection_url": "amqp://rabbit/",
            "queue_names": ["cdc.orders"],
            "prefetch_count": 25,
            "dlq_destination": "db-monitor-rabbit-dlq",
            "enable_dlq": main.DLQ_ENABLED,
            "enable_batch": main.BATCH_ENABLED,
            "batch_size": main.BATCH_SIZE,
        }
    ]


def test_configure_tracing_skips_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fake_initialize_tracing(**_kwargs) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(main, "OTEL_TRACING_ENABLED", False)
    monkeypatch.setattr(main, "initialize_tracing", fake_initialize_tracing)

    main._configure_tracing(main.app)

    assert called is False


def test_configure_tracing_initializes_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_initialize_tracing(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(main, "OTEL_TRACING_ENABLED", True)
    monkeypatch.setattr(main, "OTEL_SERVICE_NAME", "db-monitor-test")
    monkeypatch.setattr(main, "OTEL_EXPORTER", "console")
    monkeypatch.setattr(main, "OTEL_EXPORTER_OTLP_ENDPOINT", None)
    monkeypatch.setattr(main, "OTEL_EXPORTER_OTLP_HEADERS", None)
    monkeypatch.setattr(main, "initialize_tracing", fake_initialize_tracing)

    main._configure_tracing(main.app)

    assert captured["app"] is main.app
    assert captured["engine"] is main.engine
    assert captured["service_name"] == "db-monitor-test"
    assert captured["exporter"] == "console"
