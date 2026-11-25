"""Tests for the unified AppSettings dataclass configuration module.

Uses ``monkeypatch.setattr`` on the module-level ``settings`` singleton
instead of the old ``importlib.reload`` + ``os.environ`` pattern.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from db_monitor.core.config import AppSettings


def _reload_settings():
    """Clear the cached settings and reload from the current environment."""
    from db_monitor.core.config import _get_settings
    _get_settings.cache_clear()
    from db_monitor.core.config import settings as _settings
    # Trigger reload
    return _settings


@pytest.fixture(autouse=True)
def _reset_settings():
    """Reset the settings cache after each test to isolate side effects."""
    from db_monitor.core.config import _get_settings
    _get_settings.cache_clear()
    yield
    _get_settings.cache_clear()


def _build_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> AppSettings:
    """Build a fresh AppSettings from the current monkeypatched environment."""
    from db_monitor.core.config import _get_settings
    _get_settings.cache_clear()
    return AppSettings.from_env()


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

    config = _build_settings_from_env(monkeypatch)

    assert (
        config.db.postgres_url
        == "postgresql+asyncpg://user:pass@db:5432/postgres"
    )
    assert config.auth.jwt_secret == "current-secret-123456789012345678901234"
    assert (
        config.auth.jwt_secret_next
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
        _build_settings_from_env(monkeypatch)


def test_config_loads_schema_mode_from_file_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """DB_SCHEMA_MODE_FILE should override the env var when set."""
    schema_mode_file = tmp_path / "db_schema_mode"
    schema_mode_file.write_text("validate\n", encoding="utf-8")

    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("DB_SCHEMA_MODE", raising=False)
    monkeypatch.setenv("DB_SCHEMA_MODE_FILE", str(schema_mode_file))

    config = _build_settings_from_env(monkeypatch)

    assert config.db_schema_mode == "validate"


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
        _build_settings_from_env(monkeypatch)


def test_config_rejects_invalid_service_role_with_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SERVICE_ROLE is an enum; typos must fail at startup, not silently."""
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SERVICE_ROLE", "worker")

    with pytest.raises(
        ValueError,
        match="Invalid SERVICE_ROLE='worker'.*all, api, consumer",
    ):
        _build_settings_from_env(monkeypatch)


def test_config_normalizes_service_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("SERVICE_ROLE", " API ")

    config = _build_settings_from_env(monkeypatch)

    assert config.service_role == "api"


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
        _build_settings_from_env(monkeypatch)


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

    config = _build_settings_from_env(monkeypatch)

    assert config.broker.kafka_clusters == [
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
            "consumer_group": config.broker.kafka_consumer_group,
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
        _build_settings_from_env(monkeypatch)


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

    config = _build_settings_from_env(monkeypatch)

    assert config.broker.kafka_clusters == [
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
        _build_settings_from_env(monkeypatch)


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
        _build_settings_from_env(monkeypatch)


def test_config_accepts_console_exporter_when_tracing_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local tracing should allow the console exporter without an endpoint."""
    monkeypatch.setenv("OTEL_TRACING_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    config = _build_settings_from_env(monkeypatch)

    assert config.observability.otel_tracing_enabled is True
    assert config.observability.otel_exporter == "console"


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

    config = _build_settings_from_env(monkeypatch)

    assert config.webhook.urls == [
        "https://hooks.example/a",
        "https://hooks.example/b",
    ]
    assert config.webhook.timeout_seconds == 9.5
    assert config.webhook.shared_secret == "shared-secret"
    assert config.webhook.max_retries == 4
    assert config.webhook.retry_backoff_seconds == 0.75
    assert config.webhook.circuit_breaker_threshold == 6
    assert config.webhook.circuit_breaker_recovery_seconds == 12.0


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

    config = _build_settings_from_env(monkeypatch)

    assert config.broker.broker_clusters == [
        {
            "name": "rabbit-primary",
            "broker_kind": "rabbitmq",
            "bootstrap_servers": None,
            "topics": ["cdc.orders", "cdc.shipments"],
            "topic_partitions": None,
            "consumer_group": config.broker.kafka_consumer_group,
            "connection_url": "amqp://rabbit/",
            "queue_names": ["cdc.orders", "cdc.shipments"],
            "prefetch_count": 25,
            "dlq_destination": config.broker.rabbitmq_dlq_queue,
        },
        {
            "name": "kafka-secondary",
            "broker_kind": "kafka",
            "bootstrap_servers": ["kafka-a:9092"],
            "topics": ["orders.events"],
            "topic_partitions": None,
            "consumer_group": config.broker.kafka_consumer_group,
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

    config = _build_settings_from_env(monkeypatch)

    assert config.broker.broker_clusters == [
        {
            "name": "default",
            "broker_kind": "rabbitmq",
            "bootstrap_servers": None,
            "topics": ["cdc.orders", "cdc.shipments"],
            "topic_partitions": None,
            "consumer_group": config.broker.kafka_consumer_group,
            "connection_url": "amqp://rabbitmq/",
            "queue_names": ["cdc.orders", "cdc.shipments"],
            "prefetch_count": config.broker.rabbitmq_prefetch_count,
            "dlq_destination": config.broker.rabbitmq_dlq_queue,
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

    config = _build_settings_from_env(monkeypatch)

    assert config.retention.cleanup_enabled is True
    assert config.retention.cleanup_interval_seconds == 900
    assert config.retention.cleanup_batch_size == 250
    assert config.retention.cleanup_max_batches_per_table == 7
    assert config.retention.archive_before_delete is True
    assert config.retention.archive_dir == "/tmp/db-monitor-archive"
    assert config.retention.event_retention_days == 14
    assert config.retention.column_changes_retention_days == 21
    assert config.retention.dead_letter_retention_days == 45
    assert config.retention.api_audit_log_retention_days == 120


def test_config_rejects_non_positive_retention_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention interval must be positive when cleanup is configured."""
    monkeypatch.setenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "0")

    with pytest.raises(
        ValueError,
        match="RETENTION_CLEANUP_INTERVAL_SECONDS must be greater than zero",
    ):
        _build_settings_from_env(monkeypatch)


def test_config_rejects_negative_event_retention_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention day windows cannot be negative."""
    monkeypatch.setenv("EVENT_RETENTION_DAYS", "-1")

    with pytest.raises(
        ValueError,
        match="EVENT_RETENTION_DAYS cannot be negative",
    ):
        _build_settings_from_env(monkeypatch)


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

    config = _build_settings_from_env(monkeypatch)

    assert config.ingestion.quota_enabled is True
    assert config.ingestion.quota_window_seconds == 120
    assert config.ingestion.source_default_events_per_window == 1500
    assert config.ingestion.tenant_default_events_per_window == 300
    assert config.ingestion.quota_mode == "drop"
    assert config.ingestion.quota_max_throttle_seconds == 2.5
    assert config.ingestion.source_quotas == {
        "orderdb": 1200,
        "shippingdb": 600,
    }
    assert config.ingestion.tenant_quotas == {
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
        _build_settings_from_env(monkeypatch)


def test_config_loads_ingestion_bulk_write_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bulk-write toggle should parse as a boolean feature flag."""
    monkeypatch.setenv("INGESTION_BULK_WRITE_ENABLED", "false")

    config = _build_settings_from_env(monkeypatch)

    assert config.ingestion.bulk_write_enabled is False


def test_config_loads_tenant_cohort_slo_policies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tenant cohort SLO policies should parse from JSON config."""
    monkeypatch.setenv("SLO_ROLLING_WINDOW_DAYS", "28")
    monkeypatch.setenv(
        "TENANT_COHORT_SLO_POLICIES",
        (
            "["
            '{"name":"small","max_sources":10,'
            '"availability_target":99.9,"error_budget_percent":0.1,'
            '"max_commit_age_seconds":180,"max_consumer_lag":500,'
            '"max_dlq_messages":0},'
            '{"name":"large","max_sources":100,'
            '"availability_target":99.0,"error_budget_percent":1.0,'
            '"max_commit_age_seconds":600,"max_consumer_lag":5000,'
            '"max_dlq_messages":20}'
            "]"
        ),
    )

    config = _build_settings_from_env(monkeypatch)

    assert config.slo_rolling_window_days == 28
    assert config.tenant_cohort_slo_policies == [
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
            "name": "large",
            "max_sources": 100,
            "availability_target": 99.0,
            "error_budget_percent": 1.0,
            "max_commit_age_seconds": 600,
            "max_consumer_lag": 5000,
            "max_dlq_messages": 20,
        },
    ]


def test_config_rejects_invalid_slo_availability_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SLO availability target must remain within percentage bounds."""
    monkeypatch.setenv(
        "TENANT_COHORT_SLO_POLICIES",
        (
            "["
            '{"name":"small","max_sources":10,'
            '"availability_target":120.0,"error_budget_percent":0.1,'
            '"max_commit_age_seconds":180,"max_consumer_lag":500,'
            '"max_dlq_messages":0}'
            "]"
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"availability_target must be in \(0, 100\]",
    ):
        _build_settings_from_env(monkeypatch)
