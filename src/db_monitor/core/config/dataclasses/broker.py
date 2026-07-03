"""Kafka and RabbitMQ broker configuration dataclass."""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass, field

from db_monitor.core.config_loader import get_env_or_file, parse_csv_list


@dataclass
class BrokerSettings:
    """Kafka and RabbitMQ broker configuration.

    Includes SSL, topic subscriptions, consumer groups, multi-cluster setup,
    batch processing, and the dead-letter queue toggle.
    """

    message_broker: str = field(
        default_factory=lambda: os.getenv("MESSAGE_BROKER", "kafka").lower()
    )
    """Active broker kind: ``kafka`` or ``rabbitmq``. Default: kafka."""
    dlq_enabled: bool = field(
        default_factory=lambda: os.getenv("DLQ_ENABLED", "true").lower()
        == "true"
    )
    """Enable dead-letter queue persistence. Default: true."""
    batch_enabled: bool = field(
        default_factory=lambda: os.getenv("BATCH_ENABLED", "true").lower()
        == "true"
    )
    """Enable batch event processing. Default: true."""
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "100"))
    )
    """Max events per batch. Default: 100."""

    # --- Kafka ---
    kafka_broker: str = field(
        default_factory=lambda: os.getenv("KAFKA_BROKER", "kafka:9092")
    )
    """Default Kafka bootstrap server(s). Default: kafka:9092."""
    kafka_security_protocol: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
        ).upper()
    )
    """Kafka security protocol: PLAINTEXT, SSL, SASL_SSL, etc. Default: PLAINTEXT."""
    kafka_ssl_cafile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_CAFILE")
    )
    """Path to Kafka SSL CA certificate file."""
    kafka_ssl_certfile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_CERTFILE")
    )
    """Path to Kafka SSL client certificate file."""
    kafka_ssl_keyfile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_KEYFILE")
    )
    """Path to Kafka SSL client key file."""
    kafka_ssl_password: str | None = field(
        default_factory=lambda: get_env_or_file("KAFKA_SSL_PASSWORD")
    )
    """Password for the Kafka SSL key file (supports _FILE variant)."""
    kafka_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "orderdb.public.orders")
    )
    """Default single Kafka topic. Default: orderdb.public.orders."""
    kafka_consumer_group: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_CONSUMER_GROUP", "fastapi-consumer-group"
        )
    )
    """Default Kafka consumer group ID. Default: fastapi-consumer-group."""

    # --- RabbitMQ ---
    rabbitmq_url: str | None = field(
        default_factory=lambda: get_env_or_file(
            "RABBITMQ_URL", "amqp://guest:guest@localhost/"
        )
    )
    """RabbitMQ connection URL (supports _FILE variant). Default: local AMQP."""
    rabbitmq_queue: str = field(
        default_factory=lambda: os.getenv("RABBITMQ_QUEUE", "db-monitor-events")
    )
    """Default single RabbitMQ queue name. Default: db-monitor-events."""
    rabbitmq_queues: list[str] = field(default_factory=list)
    """All RabbitMQ queue names (parsed from RABBITMQ_QUEUES CSV)."""
    rabbitmq_prefetch_count: int = field(
        default_factory=lambda: int(os.getenv("RABBITMQ_PREFETCH_COUNT", "100"))
    )
    """RabbitMQ QoS prefetch count. Default: 100."""
    rabbitmq_dlq_queue: str = field(
        default_factory=lambda: os.getenv("RABBITMQ_DLQ_QUEUE", "db-monitor-dlq")
    )
    """RabbitMQ DLQ queue name. Default: db-monitor-dlq."""

    # --- Computed (populated after construction via AppSettings.from_env) ---
    kafka_topics: list[str] = field(default_factory=list)
    """All subscribed Kafka topics (derived from KAFKA_TOPICS CSV or KAFKA_TOPIC)."""
    kafka_ssl_context: ssl.SSLContext | None = field(default=None)
    """Pre-built SSL context when Kafka security protocol requires it."""
    kafka_clusters: list[dict[str, object]] = field(default_factory=list)
    """Multi-cluster Kafka definitions parsed from KAFKA_CLUSTERS JSON."""
    broker_clusters: list[dict[str, object]] = field(default_factory=list)
    """Generic broker cluster definitions parsed from BROKER_CLUSTERS JSON."""
