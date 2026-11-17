"""OpenTelemetry tracing, Prometheus metrics, and audit-log buffer tuning."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class ObservabilitySettings:
    """OpenTelemetry tracing, Prometheus metrics, and audit-log buffer tuning."""

    otel_tracing_enabled: bool = field(
        default_factory=lambda: os.getenv("OTEL_TRACING_ENABLED", "false").lower()
        == "true"
    )
    """Enable OpenTelemetry distributed tracing. Default: false."""
    otel_service_name: str = field(
        default_factory=lambda: os.getenv("OTEL_SERVICE_NAME", "db-monitor")
    )
    """OTel service resource name. Default: db-monitor."""
    otel_exporter: str = field(
        default_factory=lambda: os.getenv("OTEL_EXPORTER", "otlp").lower()
    )
    """OTel exporter type: ``otlp`` or ``console``. Default: otlp."""
    otel_exporter_otlp_endpoint: str | None = field(
        default_factory=lambda: os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    )
    """OTLP gRPC/HTTP collector endpoint."""
    otel_exporter_otlp_headers: str | None = field(
        default_factory=lambda: os.getenv("OTEL_EXPORTER_OTLP_HEADERS")
    )
    """OTLP exporter headers (e.g. ``Authorization=Bearer xxx``)."""
    audit_log_queue_maxsize: int = field(
        default_factory=lambda: int(os.getenv("AUDIT_LOG_QUEUE_MAXSIZE", "10000"))
    )
    """Max in-memory audit-log buffer size. Default: 10000."""
    audit_log_batch_size: int = field(
        default_factory=lambda: int(os.getenv("AUDIT_LOG_BATCH_SIZE", "100"))
    )
    """Audit-log entries per flush batch. Default: 100."""
    audit_log_flush_interval_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("AUDIT_LOG_FLUSH_INTERVAL_SECONDS", "1.0")
        )
    )
    """Max seconds between audit-log flushes. Default: 1.0."""
    audit_log_queue_mode: str = field(
        default_factory=lambda: os.getenv("AUDIT_LOG_QUEUE_MODE", "drop").lower()
    )
    """
    Backpressure policy when the in-memory audit queue is full.

    - ``drop`` (default): discard the entry and increment the
      ``audit_log_records_dropped_total`` counter.
    - ``block``: await space in the queue (naturally throttles the
      request caller until a flush frees a slot).
    - ``spill``: persist the entry directly to the
      ``api_audit_log_spill`` table so no records are lost.
    """
