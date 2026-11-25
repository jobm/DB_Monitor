"""Top-level AppSettings dataclass with factory and validation."""

from __future__ import annotations

from dataclasses import dataclass, field

from db_monitor.core.config.dataclasses.auth_settings import AuthSettings
from db_monitor.core.config.dataclasses.backplane import BackplaneSettings
from db_monitor.core.config.dataclasses.db import DatabaseSettings
from db_monitor.core.config.dataclasses.ingestion import IngestionSettings
from db_monitor.core.config.dataclasses.observability import ObservabilitySettings
from db_monitor.core.config.dataclasses.retention import RetentionSettings
from db_monitor.core.config.dataclasses.webhook import WebhookSettings
from db_monitor.core.config.dataclasses.broker import BrokerSettings
from db_monitor.core.config.dataclasses.validation import _validate_app_settings
from db_monitor.core.config.dataclasses.factory import _build_settings_from_env


@dataclass
class AppSettings:
    """Top-level application configuration container.

    Create via ``AppSettings.from_env()`` to lazily read all environment
    variables once and validate them.  Use the module-level ``settings``
    singleton for normal use.
    """

    # --- Core ---
    env: str = "development"
    """Runtime environment: ``development``, ``production``, ``prod``, etc."""
    db_schema_mode: str = "apply"
    """Schema migration strategy: ``apply``, ``validate``, or ``skip``."""
    service_role: str = "all"
    """Process role: ``all`` (API + consumer), ``api``, or ``consumer``.

    Split deployments set this so the API tier does not also consume from the
    broker and the consumer tier does not serve client traffic.
    """
    app_shutdown_timeout_seconds: float = 45.0
    """Graceful shutdown timeout in seconds. Default: 45."""

    # --- Readiness / SLO ---
    readiness_max_commit_age_seconds: int = 300
    """Max seconds since last consumer commit before /readyz fails. Default: 300."""
    readiness_max_consumer_lag: int = 1000
    """Max allowed consumer lag before /readyz fails. Default: 1000."""
    readiness_max_dlq_messages: int = 0
    """Max allowed DLQ messages before /readyz fails. Default: 0."""
    slo_rolling_window_days: int = 30
    """SLO evaluation rolling window in days. Default: 30."""
    tenant_cohort_slo_policies: list[dict[str, object]] = field(default_factory=list)
    """Tenant cohort SLO policies parsed from TENANT_COHORT_SLO_POLICIES JSON."""

    # --- Domain sub-settings ---
    db: DatabaseSettings = field(default_factory=DatabaseSettings)
    """Database connection-pool settings."""
    broker: BrokerSettings = field(default_factory=BrokerSettings)
    """Kafka / RabbitMQ broker settings."""
    auth: AuthSettings = field(default_factory=AuthSettings)
    """Authentication, JWT, and API key settings."""
    webhook: WebhookSettings = field(default_factory=WebhookSettings)
    """Outbound webhook delivery settings."""
    observability: ObservabilitySettings = field(default_factory=ObservabilitySettings)
    """Tracing, metrics, and audit-log buffer settings."""
    retention: RetentionSettings = field(default_factory=RetentionSettings)
    """Data-retention policy settings."""
    backplane: BackplaneSettings = field(default_factory=BackplaneSettings)
    """Cross-replica pub/sub backplane (Postgres LISTEN/NOTIFY or Redis)."""
    ingestion: IngestionSettings = field(default_factory=IngestionSettings)
    """Ingestion quota and bulk-write settings."""

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> AppSettings:
        """Load and validate all settings from environment variables once."""
        return _build_settings_from_env()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """Cross-check all config values and raise ``ValueError`` on misconfiguration."""
        _validate_app_settings(self)
