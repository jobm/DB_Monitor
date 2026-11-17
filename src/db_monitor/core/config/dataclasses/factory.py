"""AppSettings factory — load all settings from environment variables."""

from __future__ import annotations

import os
import ssl

from db_monitor.core.config_loader import (
    default_schema_mode,
    get_env_or_file,
    parse_csv_list,
)
from db_monitor.core.config.normalizers import (
    load_broker_clusters,
    load_ingestion_quota_overrides,
    load_kafka_clusters,
    load_tenant_cohort_slo_policies,
)
from db_monitor.core.config.dataclasses.broker import BrokerSettings
from db_monitor.core.config.dataclasses.auth_settings import AuthSettings
from db_monitor.core.config.dataclasses.backplane import BackplaneSettings
from db_monitor.core.config.dataclasses.db import DatabaseSettings
from db_monitor.core.config.dataclasses.ingestion import IngestionSettings
from db_monitor.core.config.dataclasses.observability import ObservabilitySettings
from db_monitor.core.config.dataclasses.retention import RetentionSettings
from db_monitor.core.config.dataclasses.webhook import WebhookSettings
from db_monitor.core.config.dataclasses.validation import _validate_app_settings


def _build_settings_from_env():
    """Load and validate all settings from environment variables once."""
    from db_monitor.core.config.dataclasses.core import AppSettings
    env = os.getenv("APP_ENV", "development").lower()

    db = DatabaseSettings()
    broker = BrokerSettings()
    auth = AuthSettings()
    backplane = BackplaneSettings()
    webhook = WebhookSettings()
    observability = ObservabilitySettings()
    retention = RetentionSettings()
    ingestion = IngestionSettings()

    # --- Populate computed fields on sub-settings ---

    # DB schema mode (supports DB_SCHEMA_MODE_FILE for Docker/K8s secrets)
    db_schema_mode = get_env_or_file(
        "DB_SCHEMA_MODE", default_schema_mode(env)
    ).lower()

    # Broker computed fields
    broker.kafka_topics = (
        parse_csv_list(os.getenv("KAFKA_TOPICS"))
        or [broker.kafka_topic]
    )
    broker.rabbitmq_queues = (
        parse_csv_list(os.getenv("RABBITMQ_QUEUES"))
        or [broker.rabbitmq_queue]
    )

    # SSL context
    if broker.kafka_security_protocol in ("SSL", "SASL_SSL"):
        ctx = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=broker.kafka_ssl_cafile,
        )
        if broker.kafka_ssl_certfile and broker.kafka_ssl_keyfile:
            ctx.load_cert_chain(
                certfile=broker.kafka_ssl_certfile,
                keyfile=broker.kafka_ssl_keyfile,
                password=broker.kafka_ssl_password,
            )
        broker.kafka_ssl_context = ctx

    # Multi-cluster configs
    broker.kafka_clusters = load_kafka_clusters(broker)
    broker.broker_clusters = load_broker_clusters(broker)

    # Ingestion computed fields
    ingestion.source_quotas = load_ingestion_quota_overrides(
        "INGESTION_SOURCE_QUOTAS"
    )
    ingestion.tenant_quotas = load_ingestion_quota_overrides(
        "INGESTION_TENANT_QUOTAS"
    )

    # Webhook computed fields
    webhook.urls = parse_csv_list(os.getenv("WEBHOOK_URLS"))

    # Assemble top-level
    settings_obj = AppSettings(
        env=env,
        db_schema_mode=db_schema_mode,
        app_shutdown_timeout_seconds=float(
            os.getenv("APP_SHUTDOWN_TIMEOUT_SECONDS", "45")
        ),
        readiness_max_commit_age_seconds=int(
            os.getenv("READINESS_MAX_COMMIT_AGE_SECONDS", "300")
        ),
        readiness_max_consumer_lag=int(
            os.getenv("READINESS_MAX_CONSUMER_LAG", "1000")
        ),
        readiness_max_dlq_messages=int(
            os.getenv("READINESS_MAX_DLQ_MESSAGES", "0")
        ),
        slo_rolling_window_days=int(
            os.getenv("SLO_ROLLING_WINDOW_DAYS", "30")
        ),
        tenant_cohort_slo_policies=load_tenant_cohort_slo_policies(),
        db=db,
        broker=broker,
        auth=auth,
        backplane=backplane,
        webhook=webhook,
        observability=observability,
        retention=retention,
        ingestion=ingestion,
    )
    _validate_app_settings(settings_obj)
    return settings_obj
