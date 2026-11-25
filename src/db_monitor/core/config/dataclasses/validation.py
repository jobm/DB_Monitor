"""Cross-field configuration validation for AppSettings."""

from __future__ import annotations

import os

from db_monitor.core.config_loader import config_error


def _validate_app_settings(settings) -> None:
    """Cross-check all config values and raise ``ValueError`` on misconfiguration."""
    # DB schema mode
    if settings.db_schema_mode not in {"apply", "validate", "skip"}:
        config_error(
            f"Invalid DB_SCHEMA_MODE='{settings.db_schema_mode}'.",
            hint="Use one of: apply, validate, skip.",
        )

    # Process role
    if settings.service_role not in {"all", "api", "consumer"}:
        config_error(
            f"Invalid SERVICE_ROLE='{settings.service_role}'.",
            hint="Use one of: all, api, consumer.",
        )

    # Production hardening
    if settings.env in {"production", "prod"}:
        if settings.db.pool_size < 1:
            config_error(
                "DB_POOL_SIZE must be at least 1.",
                hint="Increase DB_POOL_SIZE or remove the invalid override.",
            )
        if settings.db.max_overflow < 0:
            config_error(
                "DB_MAX_OVERFLOW cannot be negative.",
                hint="Set DB_MAX_OVERFLOW to 0 or a positive integer.",
            )
        if settings.db.pool_timeout_seconds < 1:
            config_error(
                "DB_POOL_TIMEOUT_SECONDS must be at least 1.",
                hint="Set a positive checkout timeout in seconds.",
            )
        if settings.db.pool_recycle_seconds < 0:
            config_error(
                "DB_POOL_RECYCLE_SECONDS cannot be negative.",
                hint="Use 0 to disable recycle behavior or a positive integer.",
            )
        if settings.db.postgres_url == "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres":
            config_error(
                "POSTGRES_URL must be explicitly configured in production.",
                hint="Set POSTGRES_URL or POSTGRES_URL_FILE to the real monitor database connection string.",
            )
        if settings.auth.jwt_secret == "dev-insecure-secret-change-me-please-rotate":
            config_error(
                "JWT_SECRET must be explicitly configured in production.",
                hint="Set JWT_SECRET or JWT_SECRET_FILE to a non-default secret before startup.",
            )
        if len(settings.auth.jwt_secret) < 32:
            config_error(
                "JWT_SECRET must be at least 32 characters long in production.",
                hint="Generate a longer secret and redeploy.",
            )
        if settings.auth.jwt_secret_next and len(settings.auth.jwt_secret_next) < 32:
            config_error(
                "JWT_SECRET_NEXT must be at least 32 characters long in production.",
                hint="Use a 32+ character overlap secret during rotation.",
            )
        if not os.getenv("ALLOW_BOOTSTRAP"):
            settings.auth.allow_bootstrap = False

    # Observability
    if settings.observability.otel_exporter not in {"otlp", "console"}:
        config_error(
            f"Invalid OTEL_EXPORTER='{settings.observability.otel_exporter}'.",
            hint="Use one of: otlp, console.",
        )
    if (
        settings.observability.otel_tracing_enabled
        and settings.observability.otel_exporter == "otlp"
        and not settings.observability.otel_exporter_otlp_endpoint
    ):
        config_error(
            "OTEL_EXPORTER_OTLP_ENDPOINT is required when tracing "
            "is enabled with the OTLP exporter.",
            hint="Set OTEL_EXPORTER_OTLP_ENDPOINT to your collector URL or switch OTEL_EXPORTER=console for local debugging.",
        )

    # Webhook validations
    if settings.webhook.timeout_seconds <= 0:
        config_error(
            "WEBHOOK_TIMEOUT_SECONDS must be greater than zero.",
            hint="Set a positive timeout value such as 5.0.",
        )
    if settings.webhook.max_retries < 0:
        config_error(
            "WEBHOOK_MAX_RETRIES cannot be negative.",
            hint="Use 0 to disable retries or a positive integer.",
        )
    if settings.webhook.retry_backoff_seconds < 0:
        config_error(
            "WEBHOOK_RETRY_BACKOFF_SECONDS cannot be negative.",
            hint="Use 0 for immediate retries or a positive delay.",
        )
    if settings.webhook.circuit_breaker_threshold < 1:
        config_error(
            "WEBHOOK_CIRCUIT_BREAKER_THRESHOLD must be at least 1.",
            hint="Use a positive integer failure threshold.",
        )
    if settings.webhook.circuit_breaker_recovery_seconds <= 0:
        config_error(
            "WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS must be greater than zero.",
            hint="Use a positive recovery timeout in seconds.",
        )
    for url in settings.webhook.urls:
        if not url.startswith(("http://", "https://")):
            config_error(
                f"Invalid WEBHOOK_URLS entry '{url}'.",
                hint="Use comma-separated http:// or https:// URLs.",
            )

    # Broker validations
    if settings.broker.message_broker not in {"kafka", "rabbitmq"}:
        config_error(
            f"Invalid MESSAGE_BROKER='{settings.broker.message_broker}'.",
            hint="Use one of: kafka, rabbitmq.",
        )
    if settings.broker.rabbitmq_prefetch_count <= 0:
        config_error(
            "RABBITMQ_PREFETCH_COUNT must be greater than zero.",
            hint="Set a positive prefetch value such as 100.",
        )

    # Ingestion validations
    if settings.ingestion.quota_window_seconds <= 0:
        config_error(
            "INGESTION_QUOTA_WINDOW_SECONDS must be greater than zero.",
            hint="Set a positive quota window in seconds.",
        )
    if settings.ingestion.source_default_events_per_window < 0:
        config_error(
            "INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW cannot be negative.",
            hint="Use 0 to disable default source quotas or a positive integer.",
        )
    if settings.ingestion.tenant_default_events_per_window < 0:
        config_error(
            "INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW cannot be negative.",
            hint="Use 0 to disable default tenant quotas or a positive integer.",
        )
    if settings.ingestion.quota_mode not in {"throttle", "drop"}:
        config_error(
            f"Invalid INGESTION_QUOTA_MODE='{settings.ingestion.quota_mode}'.",
            hint="Use one of: throttle, drop.",
        )
    if settings.ingestion.quota_max_throttle_seconds <= 0:
        config_error(
            "INGESTION_QUOTA_MAX_THROTTLE_SECONDS must be greater than zero.",
            hint="Set a positive max throttle sleep such as 1.0.",
        )

    # SLO validations
    if settings.slo_rolling_window_days <= 0:
        config_error(
            "SLO_ROLLING_WINDOW_DAYS must be greater than zero.",
            hint="Use a positive rolling window such as 30.",
        )

    # Retention validations
    if settings.retention.cleanup_interval_seconds <= 0:
        config_error(
            "RETENTION_CLEANUP_INTERVAL_SECONDS must be greater than zero.",
            hint="Use a positive cleanup interval in seconds.",
        )
    if settings.retention.cleanup_batch_size <= 0:
        config_error(
            "RETENTION_CLEANUP_BATCH_SIZE must be greater than zero.",
            hint="Use a positive retention cleanup batch size.",
        )
    if settings.retention.cleanup_max_batches_per_table <= 0:
        config_error(
            "RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE must be greater than zero.",
            hint="Use at least one batch per cleanup cycle.",
        )
    for days, name in (
        (settings.retention.event_retention_days, "EVENT_RETENTION_DAYS"),
        (settings.retention.column_changes_retention_days, "COLUMN_CHANGES_RETENTION_DAYS"),
        (settings.retention.dead_letter_retention_days, "DEAD_LETTER_RETENTION_DAYS"),
        (settings.retention.api_audit_log_retention_days, "API_AUDIT_LOG_RETENTION_DAYS"),
    ):
        if days < 0:
            config_error(
                f"{name} cannot be negative.",
                hint="Use 0 to disable cleanup for one table or a positive integer.",
            )
