from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


CONFIG_PATH = Path(__file__).resolve().parents[1] / "app" / "config.py"


def _load_config_module(module_name: str):
    """Load the config module under an isolated module name."""
    spec = importlib.util.spec_from_file_location(module_name, CONFIG_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_loads_secret_files_in_production(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Production config should accept secrets injected through files."""
    postgres_url_file = tmp_path / "postgres_url"
    postgres_url_file.write_text(
        "postgresql://user:pass@db:5432/postgres\n",
        encoding="utf-8",
    )
    jwt_secret_file = tmp_path / "jwt_secret"
    jwt_secret_file.write_text(
        "current-secret-123456789012345678901234\n",
        encoding="utf-8",
    )
    jwt_secret_next_file = tmp_path / "jwt_secret_next"
    jwt_secret_next_file.write_text(
        "next-secret-1234567890123456789012345678\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT", raising=False)
    monkeypatch.setenv("POSTGRES_URL_FILE", str(postgres_url_file))
    monkeypatch.setenv("JWT_SECRET_FILE", str(jwt_secret_file))
    monkeypatch.setenv("JWT_SECRET_NEXT_FILE", str(jwt_secret_next_file))

    config = _load_config_module("config_from_secret_files")

    assert (
        config.POSTGRES_URL
        == "postgresql+asyncpg://user:pass@db:5432/postgres"
    )
    assert config.JWT_SECRET == "current-secret-123456789012345678901234"
    assert (
        config.JWT_SECRET_NEXT
        == "next-secret-1234567890123456789012345678"
    )


def test_config_rejects_missing_jwt_secret_in_production(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Production config should fail fast without an explicit JWT secret."""
    postgres_url_file = tmp_path / "postgres_url"
    postgres_url_file.write_text(
        "postgresql+asyncpg://user:pass@db:5432/postgres\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.delenv("JWT_SECRET_FILE", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT", raising=False)
    monkeypatch.delenv("JWT_SECRET_NEXT_FILE", raising=False)
    monkeypatch.setenv("POSTGRES_URL_FILE", str(postgres_url_file))

    with pytest.raises(
        ValueError,
        match="JWT_SECRET must be explicitly configured in production",
    ):
        _load_config_module("config_missing_secret")


def test_config_rejects_invalid_schema_mode_with_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config errors should identify invalid schema mode values clearly."""
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("DB_SCHEMA_MODE", "broken")

    with pytest.raises(
        ValueError,
        match="Invalid DB_SCHEMA_MODE='broken'.*apply, validate, skip",
    ):
        _load_config_module("config_invalid_schema_mode")


def test_config_reports_missing_secret_file_with_remediation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Missing secret files should point to the exact env var and fix path."""
    postgres_url_file = tmp_path / "postgres_url"
    postgres_url_file.write_text(
        "postgresql+asyncpg://user:pass@db:5432/postgres\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.setenv("POSTGRES_URL_FILE", str(postgres_url_file))
    monkeypatch.setenv("JWT_SECRET_FILE", str(tmp_path / "missing_jwt_secret"))

    with pytest.raises(
        ValueError,
        match=(
            "JWT_SECRET_FILE points to '.*missing_jwt_secret'.*"
            "set JWT_SECRET directly"
        ),
    ):
        _load_config_module("config_missing_secret_file")


def test_config_loads_multiple_kafka_clusters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kafka cluster config should parse multiple cluster definitions."""
    monkeypatch.setenv(
        "KAFKA_CLUSTERS",
        (
            '[{"name":"primary","bootstrap_servers":"kafka-a:9092",'
            '"topics":["orders.events"],"consumer_group":"group-a"},'
            '{"name":"secondary","bootstrap_servers":["kafka-b:9092"],'
            '"topics":"shipping.events"}]'
        ),
    )

    config = _load_config_module("config_multiple_kafka_clusters")

    assert config.KAFKA_CLUSTERS == [
        {
            "name": "primary",
            "bootstrap_servers": ["kafka-a:9092"],
            "topics": ["orders.events"],
            "topic_partitions": None,
            "consumer_group": "group-a",
        },
        {
            "name": "secondary",
            "bootstrap_servers": ["kafka-b:9092"],
            "topics": ["shipping.events"],
            "topic_partitions": None,
            "consumer_group": config.KAFKA_CONSUMER_GROUP,
        },
    ]


def test_config_rejects_duplicate_kafka_cluster_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kafka cluster names must be unique for health and task wiring."""
    monkeypatch.setenv(
        "KAFKA_CLUSTERS",
        (
            '[{"name":"dup","bootstrap_servers":"kafka-a:9092"},'
            '{"name":"dup","bootstrap_servers":"kafka-b:9092"}]'
        ),
    )

    with pytest.raises(
        ValueError,
        match="Duplicate Kafka cluster name 'dup'",
    ):
        _load_config_module("config_duplicate_kafka_clusters")


def test_config_loads_topic_partition_assignments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cluster config should support explicit topic-partition placement."""
    monkeypatch.setenv(
        "KAFKA_CLUSTERS",
        (
            '[{"name":"primary","bootstrap_servers":"kafka-a:9092",'
            '"topic_partitions":{"orders.events":[0,2],'
            '"shipping.events":"1,3"},"consumer_group":"group-a"}]'
        ),
    )

    config = _load_config_module("config_topic_partitions")

    assert config.KAFKA_CLUSTERS == [
        {
            "name": "primary",
            "bootstrap_servers": ["kafka-a:9092"],
            "topics": ["orders.events", "shipping.events"],
            "topic_partitions": {
                "orders.events": [0, 2],
                "shipping.events": [1, 3],
            },
            "consumer_group": "group-a",
        }
    ]


def test_config_rejects_mismatched_topics_and_topic_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit placement should not silently drift from the topic list."""
    monkeypatch.setenv(
        "KAFKA_CLUSTERS",
        (
            '[{"name":"primary","bootstrap_servers":"kafka-a:9092",'
            '"topics":["orders.events"],'
            '"topic_partitions":{"shipping.events":[0]}}]'
        ),
    )

    with pytest.raises(
        ValueError,
        match="topics and topic_partitions must describe the same topics",
    ):
        _load_config_module("config_mismatched_topic_partitions")


def test_config_requires_otlp_endpoint_when_tracing_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracing config should fail fast without an OTLP collector URL."""
    monkeypatch.setenv("OTEL_TRACING_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    with pytest.raises(
        ValueError,
        match="OTEL_EXPORTER_OTLP_ENDPOINT is required",
    ):
        _load_config_module("config_missing_otlp_endpoint")


def test_config_accepts_console_exporter_when_tracing_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local tracing should allow the console exporter without an endpoint."""
    monkeypatch.setenv("OTEL_TRACING_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    config = _load_config_module("config_console_tracing")

    assert config.OTEL_TRACING_ENABLED is True
    assert config.OTEL_EXPORTER == "console"


def test_config_loads_webhook_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Webhook URLs and timeout should parse from environment variables."""
    monkeypatch.setenv(
        "WEBHOOK_URLS",
        "https://hooks.example/a,https://hooks.example/b",
    )
    monkeypatch.setenv("WEBHOOK_TIMEOUT_SECONDS", "9.5")
    monkeypatch.setenv("WEBHOOK_SHARED_SECRET", "shared-secret")
    monkeypatch.setenv("WEBHOOK_MAX_RETRIES", "4")
    monkeypatch.setenv("WEBHOOK_RETRY_BACKOFF_SECONDS", "0.75")
    monkeypatch.setenv("WEBHOOK_CIRCUIT_BREAKER_THRESHOLD", "6")
    monkeypatch.setenv("WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS", "12")

    config = _load_config_module("config_webhooks")

    assert config.WEBHOOK_URLS == [
        "https://hooks.example/a",
        "https://hooks.example/b",
    ]
    assert config.WEBHOOK_TIMEOUT_SECONDS == 9.5
    assert config.WEBHOOK_SHARED_SECRET == "shared-secret"
    assert config.WEBHOOK_MAX_RETRIES == 4
    assert config.WEBHOOK_RETRY_BACKOFF_SECONDS == 0.75
    assert config.WEBHOOK_CIRCUIT_BREAKER_THRESHOLD == 6
    assert config.WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS == 12.0


def test_config_loads_rabbitmq_broker_clusters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Broker config should parse RabbitMQ clusters alongside Kafka ones."""
    monkeypatch.setenv(
        "BROKER_CLUSTERS",
        (
            '[{"name":"rabbit-primary","broker_kind":"rabbitmq",'
            '"connection_url":"amqp://rabbit/","queue_names":'
            '["cdc.orders","cdc.shipments"],"prefetch_count":25},'
            '{"name":"kafka-secondary","broker_kind":"kafka",'
            '"bootstrap_servers":"kafka-a:9092","topics":'
            '["orders.events"]}]'
        ),
    )

    config = _load_config_module("config_broker_clusters")

    assert config.BROKER_CLUSTERS == [
        {
            "name": "rabbit-primary",
            "broker_kind": "rabbitmq",
            "bootstrap_servers": None,
            "topics": ["cdc.orders", "cdc.shipments"],
            "topic_partitions": None,
            "consumer_group": config.KAFKA_CONSUMER_GROUP,
            "connection_url": "amqp://rabbit/",
            "queue_names": ["cdc.orders", "cdc.shipments"],
            "prefetch_count": 25,
            "dlq_destination": config.RABBITMQ_DLQ_QUEUE,
        },
        {
            "name": "kafka-secondary",
            "broker_kind": "kafka",
            "bootstrap_servers": ["kafka-a:9092"],
            "topics": ["orders.events"],
            "topic_partitions": None,
            "consumer_group": config.KAFKA_CONSUMER_GROUP,
            "connection_url": None,
            "queue_names": None,
            "prefetch_count": None,
            "dlq_destination": "db-monitor-dlq",
        },
    ]


def test_config_defaults_to_rabbitmq_cluster_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RabbitMQ mode should expose one default broker cluster."""
    monkeypatch.setenv("MESSAGE_BROKER", "rabbitmq")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://rabbitmq/")
    monkeypatch.setenv("RABBITMQ_QUEUES", "cdc.orders,cdc.shipments")

    config = _load_config_module("config_default_rabbitmq_cluster")

    assert config.BROKER_CLUSTERS == [
        {
            "name": "default",
            "broker_kind": "rabbitmq",
            "bootstrap_servers": None,
            "topics": ["cdc.orders", "cdc.shipments"],
            "topic_partitions": None,
            "consumer_group": config.KAFKA_CONSUMER_GROUP,
            "connection_url": "amqp://rabbitmq/",
            "queue_names": ["cdc.orders", "cdc.shipments"],
            "prefetch_count": config.RABBITMQ_PREFETCH_COUNT,
            "dlq_destination": config.RABBITMQ_DLQ_QUEUE,
        }
    ]


def test_config_loads_retention_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention and archival settings should parse from environment."""
    monkeypatch.setenv("RETENTION_CLEANUP_ENABLED", "true")
    monkeypatch.setenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "900")
    monkeypatch.setenv("RETENTION_CLEANUP_BATCH_SIZE", "250")
    monkeypatch.setenv("RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE", "7")
    monkeypatch.setenv("RETENTION_ARCHIVE_BEFORE_DELETE", "true")
    monkeypatch.setenv("RETENTION_ARCHIVE_DIR", "/tmp/db-monitor-archive")
    monkeypatch.setenv("EVENT_RETENTION_DAYS", "14")
    monkeypatch.setenv("COLUMN_CHANGES_RETENTION_DAYS", "21")
    monkeypatch.setenv("DEAD_LETTER_RETENTION_DAYS", "45")
    monkeypatch.setenv("API_AUDIT_LOG_RETENTION_DAYS", "120")

    config = _load_config_module("config_retention_controls")

    assert config.RETENTION_CLEANUP_ENABLED is True
    assert config.RETENTION_CLEANUP_INTERVAL_SECONDS == 900
    assert config.RETENTION_CLEANUP_BATCH_SIZE == 250
    assert config.RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE == 7
    assert config.RETENTION_ARCHIVE_BEFORE_DELETE is True
    assert config.RETENTION_ARCHIVE_DIR == "/tmp/db-monitor-archive"
    assert config.EVENT_RETENTION_DAYS == 14
    assert config.COLUMN_CHANGES_RETENTION_DAYS == 21
    assert config.DEAD_LETTER_RETENTION_DAYS == 45
    assert config.API_AUDIT_LOG_RETENTION_DAYS == 120


def test_config_rejects_non_positive_retention_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention interval must be positive when cleanup is configured."""
    monkeypatch.setenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "0")

    with pytest.raises(
        ValueError,
        match="RETENTION_CLEANUP_INTERVAL_SECONDS must be greater than zero",
    ):
        _load_config_module("config_invalid_retention_interval")


def test_config_rejects_negative_event_retention_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention day windows cannot be negative."""
    monkeypatch.setenv("EVENT_RETENTION_DAYS", "-1")

    with pytest.raises(
        ValueError,
        match="EVENT_RETENTION_DAYS cannot be negative",
    ):
        _load_config_module("config_invalid_event_retention_days")


def test_config_loads_ingestion_quota_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ingestion quota settings should parse from environment."""
    monkeypatch.setenv("INGESTION_QUOTA_ENABLED", "true")
    monkeypatch.setenv("INGESTION_QUOTA_WINDOW_SECONDS", "120")
    monkeypatch.setenv(
        "INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW",
        "1500",
    )
    monkeypatch.setenv(
        "INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW",
        "300",
    )
    monkeypatch.setenv("INGESTION_QUOTA_MODE", "drop")
    monkeypatch.setenv("INGESTION_QUOTA_MAX_THROTTLE_SECONDS", "2.5")
    monkeypatch.setenv(
        "INGESTION_SOURCE_QUOTAS",
        '{"orderdb":1200,"shippingdb":600}',
    )
    monkeypatch.setenv(
        "INGESTION_TENANT_QUOTAS",
        '{"tenant-a":250,"tenant-b":100}',
    )

    config = _load_config_module("config_ingestion_quota")

    assert config.INGESTION_QUOTA_ENABLED is True
    assert config.INGESTION_QUOTA_WINDOW_SECONDS == 120
    assert config.INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW == 1500
    assert config.INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW == 300
    assert config.INGESTION_QUOTA_MODE == "drop"
    assert config.INGESTION_QUOTA_MAX_THROTTLE_SECONDS == 2.5
    assert config.INGESTION_SOURCE_QUOTAS == {
        "orderdb": 1200,
        "shippingdb": 600,
    }
    assert config.INGESTION_TENANT_QUOTAS == {
        "tenant-a": 250,
        "tenant-b": 100,
    }


def test_config_rejects_invalid_ingestion_quota_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quota mode must be one of the supported runtime actions."""
    monkeypatch.setenv("INGESTION_QUOTA_MODE", "pause")

    with pytest.raises(
        ValueError,
        match="Invalid INGESTION_QUOTA_MODE='pause'",
    ):
        _load_config_module("config_invalid_ingestion_quota_mode")


def test_config_loads_ingestion_bulk_write_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bulk-write toggle should parse as a boolean feature flag."""
    monkeypatch.setenv("INGESTION_BULK_WRITE_ENABLED", "false")

    config = _load_config_module("config_ingestion_bulk_write")

    assert config.INGESTION_BULK_WRITE_ENABLED is False
