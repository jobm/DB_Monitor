"""Config loading helpers — normalize, parse, and load environment values."""

from __future__ import annotations

import json
import os
import ssl
from dataclasses import dataclass, field
from functools import cache
from typing import Any

from dotenv import load_dotenv

from db_monitor.core.config_loader import (
    config_error,
    default_schema_mode,
    get_env_or_file,
    parse_csv_list,
)

load_dotenv()

def _normalize_postgres_url(url: str) -> str:
    """Ensure the DSN uses the ``postgresql+asyncpg://`` scheme."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    config_error(
        "Invalid POSTGRES_URL format.",
        hint="Use a PostgreSQL DSN starting with 'postgresql+asyncpg://'.",
    )
    return url  # unreachable


def _default_allow_bootstrap() -> bool:
    """Return the safe default for ALLOW_BOOTSTRAP based on the environment."""
    env = os.getenv("APP_ENV", "development").lower()
    raw = os.getenv("ALLOW_BOOTSTRAP")
    if raw is not None:
        return raw.lower() == "true"
    return env not in {"production", "prod"}


def _default_kafka_cluster(broker: BrokerSettings) -> dict[str, object]:
    """Build the single-cluster compatibility config."""
    return {
        "name": "default",
        "bootstrap_servers": [broker.kafka_broker],
        "topics": list(broker.kafka_topics),
        "consumer_group": broker.kafka_consumer_group,
        "topic_partitions": None,
    }


def _default_rabbitmq_cluster(broker: BrokerSettings) -> dict[str, object]:
    """Build the single-cluster compatibility config for RabbitMQ."""
    return {
        "name": "default",
        "broker_kind": "rabbitmq",
        "bootstrap_servers": None,
        "topics": list(broker.rabbitmq_queues),
        "consumer_group": broker.kafka_consumer_group,
        "topic_partitions": None,
        "connection_url": broker.rabbitmq_url,
        "queue_names": list(broker.rabbitmq_queues),
        "prefetch_count": broker.rabbitmq_prefetch_count,
        "dlq_destination": broker.rabbitmq_dlq_queue,
    }


def _normalize_bootstrap_servers(
    value: str | list[str] | None,
    cluster_name: str,
) -> list[str]:
    """Normalize bootstrap server values into a non-empty list."""
    if isinstance(value, str):
        servers = parse_csv_list(value)
    elif isinstance(value, list):
        servers = [
            str(server).strip()
            for server in value
            if str(server).strip()
        ]
    else:
        servers = []

    if not servers:
        config_error(
            f"Kafka cluster '{cluster_name}' must define bootstrap_servers.",
            hint="Set bootstrap_servers to a host:port string or list.",
        )
    return servers


def _normalize_cluster_topics(
    value: object,
    cluster_name: str,
) -> list[str]:
    """Normalize per-cluster topic configuration."""
    if value is None:
        return []  # will be filled from default
    if isinstance(value, str):
        return parse_csv_list(value)
    if isinstance(value, list):
        return [str(t).strip() for t in value if str(t).strip()]
    return []


def _normalize_partition_list(
    value: object,
    cluster_name: str,
    topic_name: str,
) -> list[int]:
    """Normalize an explicit partition list for one topic."""
    if isinstance(value, str):
        raw_partitions = parse_csv_list(value)
    elif isinstance(value, list):
        raw_partitions = value
    else:
        raw_partitions = []

    partitions: list[int] = []
    for raw in raw_partitions:
        try:
            partition = int(raw)
        except (TypeError, ValueError):
            config_error(
                f"Kafka cluster '{cluster_name}' topic '{topic_name}' "
                "contains an invalid partition value.",
                hint="Use integer partition numbers such as [0, 1, 2].",
            )
        if partition < 0:
            config_error(
                f"Kafka cluster '{cluster_name}' topic '{topic_name}' "
                "contains a negative partition.",
                hint="Use partition numbers greater than or equal to zero.",
            )
        partitions.append(partition)

    if not partitions:
        config_error(
            f"Kafka cluster '{cluster_name}' topic '{topic_name}' must "
            "define at least one partition.",
            hint="Set topic_partitions to a non-empty list or CSV string.",
        )

    return sorted(set(partitions))


def _normalize_topic_partitions(
    value: object,
    cluster_name: str,
) -> dict[str, list[int]] | None:
    """Normalize explicit topic-to-partition placement config."""
    if value is None:
        return None
    if not isinstance(value, dict) or not value:
        config_error(
            f"Kafka cluster '{cluster_name}' has invalid topic_partitions.",
            hint="Use a JSON object like {'orders.events': [0, 1]}.",
        )
    topic_partitions: dict[str, list[int]] = {}
    for raw_topic_name, raw_partitions in value.items():
        topic_name = str(raw_topic_name).strip()
        if not topic_name:
            config_error(
                f"Kafka cluster '{cluster_name}' has an empty topic name.",
                hint="Provide non-empty topic names in topic_partitions.",
            )
        topic_partitions[topic_name] = _normalize_partition_list(
            raw_partitions,
            cluster_name,
            topic_name,
        )
    return topic_partitions


def _load_kafka_clusters(broker: BrokerSettings) -> list[dict[str, object]]:
    """Load multi-cluster Kafka configuration from JSON when provided."""
    raw_clusters = os.getenv("KAFKA_CLUSTERS")
    if not raw_clusters:
        return [_default_kafka_cluster(broker)]

    try:
        parsed = json.loads(raw_clusters)
    except json.JSONDecodeError as exc:
        config_error(
            "Invalid KAFKA_CLUSTERS format.",
            hint=(
                "Provide a JSON array of cluster definitions with name, "
                "bootstrap_servers, and optional topics/consumer_group. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        config_error(
            "Invalid KAFKA_CLUSTERS format.",
            hint="Provide a non-empty JSON array of cluster definitions.",
        )

    cluster_names: set[str] = set()
    clusters: list[dict[str, object]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            config_error(
                "Invalid KAFKA_CLUSTERS entry.",
                hint="Each cluster definition must be a JSON object.",
            )
        cluster_name = str(entry.get("name") or "").strip()
        if not cluster_name:
            config_error(
                "Invalid KAFKA_CLUSTERS entry.",
                hint="Each cluster definition must include a unique name.",
            )
        if cluster_name in cluster_names:
            config_error(
                f"Duplicate Kafka cluster name '{cluster_name}'.",
                hint="Use unique names for each cluster entry.",
            )
        cluster_names.add(cluster_name)

        topic_partitions = _normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics_raw = _normalize_cluster_topics(
            entry.get("topics"),
            cluster_name,
        )
        if not topics_raw:
            topics_raw = list(broker.kafka_topics)
        if topic_partitions is not None:
            partition_topics = sorted(topic_partitions.keys())
            if entry.get("topics") is not None and sorted(topics_raw) != partition_topics:
                config_error(
                    f"Kafka cluster '{cluster_name}' topics and "
                    "topic_partitions must describe the same topics.",
                    hint=(
                        "Either omit topics and derive them from "
                        "topic_partitions, or keep both lists aligned."
                    ),
                )
            topics_raw = partition_topics

        clusters.append({
            "name": cluster_name,
            "bootstrap_servers": _normalize_bootstrap_servers(
                entry.get("bootstrap_servers"),
                cluster_name,
            ),
            "topics": topics_raw,
            "topic_partitions": topic_partitions,
            "consumer_group": str(
                entry.get("consumer_group") or broker.kafka_consumer_group
            ).strip(),
        })

    return clusters


def _normalize_broker_kind(value: object, cluster_name: str) -> str:
    """Normalize one broker kind string."""
    broker_kind = str(value or "").strip().lower()
    if broker_kind not in {"kafka", "rabbitmq"}:
        config_error(
            f"Broker cluster '{cluster_name}' has unsupported "
            f"broker_kind '{broker_kind}'.",
            hint="Use one of: kafka, rabbitmq.",
        )
    return broker_kind


def _normalize_rabbitmq_queue_names(
    value: object,
    cluster_name: str,
    default_queues: list[str],
) -> list[str]:
    """Normalize per-cluster RabbitMQ queue configuration."""
    if value is None:
        return list(default_queues)
    if isinstance(value, str):
        return parse_csv_list(value)
    if isinstance(value, list):
        return [str(q).strip() for q in value if str(q).strip()]
    config_error(
        f"RabbitMQ cluster '{cluster_name}' must define queue_names.",
        hint="Set queue_names as a list or comma-separated string.",
    )
    return []  # unreachable


def _load_broker_clusters(broker: BrokerSettings) -> list[dict[str, object]]:
    """Load generic broker cluster configuration."""
    raw_clusters = os.getenv("BROKER_CLUSTERS")
    if not raw_clusters:
        if broker.message_broker == "rabbitmq":
            return [_default_rabbitmq_cluster(broker)]
        return [
            {
                "name": str(cluster["name"]),
                "broker_kind": "kafka",
                "bootstrap_servers": list(cluster["bootstrap_servers"]),
                "topics": list(cluster["topics"]),
                "topic_partitions": cluster.get("topic_partitions"),
                "consumer_group": str(cluster["consumer_group"]),
                "connection_url": None,
                "queue_names": None,
                "prefetch_count": None,
                "dlq_destination": "db-monitor-dlq",
            }
            for cluster in broker.kafka_clusters
        ]

    try:
        parsed = json.loads(raw_clusters)
    except json.JSONDecodeError as exc:
        config_error(
            "Invalid BROKER_CLUSTERS format.",
            hint=(
                "Provide a JSON array of broker definitions. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        config_error(
            "Invalid BROKER_CLUSTERS format.",
            hint="Provide a non-empty JSON array of broker definitions.",
        )

    cluster_names: set[str] = set()
    clusters: list[dict[str, object]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            config_error(
                "Invalid BROKER_CLUSTERS entry.",
                hint="Each broker definition must be a JSON object.",
            )
        cluster_name = str(entry.get("name") or "").strip()
        if not cluster_name:
            config_error(
                "Invalid BROKER_CLUSTERS entry.",
                hint="Each broker definition must include a unique name.",
            )
        if cluster_name in cluster_names:
            config_error(
                f"Duplicate broker cluster name '{cluster_name}'.",
                hint="Use unique names for each broker cluster entry.",
            )
        cluster_names.add(cluster_name)

        broker_kind = _normalize_broker_kind(
            entry.get("broker_kind") or broker.message_broker,
            cluster_name,
        )
        if broker_kind == "rabbitmq":
            if entry.get("topic_partitions") is not None:
                config_error(
                    f"RabbitMQ cluster '{cluster_name}' does not support topic_partitions.",
                    hint="Remove topic_partitions for rabbitmq clusters.",
                )
            queue_names = _normalize_rabbitmq_queue_names(
                entry.get("queue_names") or entry.get("topics"),
                cluster_name,
                broker.rabbitmq_queues,
            )
            clusters.append({
                "name": cluster_name,
                "broker_kind": broker_kind,
                "bootstrap_servers": None,
                "topics": list(queue_names),
                "topic_partitions": None,
                "consumer_group": str(
                    entry.get("consumer_group") or broker.kafka_consumer_group
                ).strip(),
                "connection_url": str(
                    entry.get("connection_url") or broker.rabbitmq_url or ""
                ).strip(),
                "queue_names": queue_names,
                "prefetch_count": int(
                    entry.get("prefetch_count") or broker.rabbitmq_prefetch_count
                ),
                "dlq_destination": str(
                    entry.get("dlq_destination") or broker.rabbitmq_dlq_queue
                ).strip(),
            })
            continue

        topic_partitions = _normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics_raw = _normalize_cluster_topics(
            entry.get("topics"),
            cluster_name,
        )
        if not topics_raw:
            topics_raw = list(broker.kafka_topics)
        if topic_partitions is not None:
            partition_topics = sorted(topic_partitions.keys())
            if entry.get("topics") is not None and sorted(topics_raw) != partition_topics:
                config_error(
                    f"Kafka cluster '{cluster_name}' topics and "
                    "topic_partitions must describe the same topics.",
                    hint="Either omit topics and derive them from topic_partitions, or keep both lists aligned.",
                )
            topics_raw = partition_topics

        clusters.append({
            "name": cluster_name,
            "broker_kind": broker_kind,
            "bootstrap_servers": _normalize_bootstrap_servers(
                entry.get("bootstrap_servers"),
                cluster_name,
            ),
            "topics": topics_raw,
            "topic_partitions": topic_partitions,
            "consumer_group": str(
                entry.get("consumer_group") or broker.kafka_consumer_group
            ).strip(),
            "connection_url": None,
            "queue_names": None,
            "prefetch_count": None,
            "dlq_destination": str(
                entry.get("dlq_destination") or "db-monitor-dlq"
            ).strip(),
        })

    return clusters


def _load_ingestion_quota_overrides(
    env_name: str,
) -> dict[str, int]:
    """Load per-identity ingestion quota overrides from JSON."""
    raw_value = os.getenv(env_name)
    if not raw_value:
        return {}

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        config_error(
            f"Invalid {env_name} format.",
            hint=(
                "Use a JSON object mapping identity names to quotas. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, dict):
        config_error(
            f"Invalid {env_name} format.",
            hint="Use a JSON object like {'orderdb': 1200}.",
        )

    normalized: dict[str, int] = {}
    for raw_name, raw_quota in parsed.items():
        identity_name = str(raw_name).strip()
        if not identity_name:
            config_error(
                f"{env_name} contains an empty identity name.",
                hint="Use non-empty identity names for each quota entry.",
            )
        try:
            quota = int(raw_quota)
        except (TypeError, ValueError):
            config_error(
                f"{env_name} contains an invalid quota for '{identity_name}'.",
                hint="Use integer quota values greater than or equal to zero.",
            )
        if quota < 0:
            config_error(
                f"{env_name} contains a negative quota for '{identity_name}'.",
                hint="Use 0 to disable one identity or a positive integer.",
            )
        normalized[identity_name] = quota

    return normalized


def _default_tenant_cohort_slo_policies() -> list[dict[str, object]]:
    """Return default SLO policy tiers for tenant cohorts."""
    return [
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


def _load_tenant_cohort_slo_policies() -> list[dict[str, object]]:
    """Load and validate cohort SLO policy overrides from JSON."""
    raw_value = os.getenv("TENANT_COHORT_SLO_POLICIES")
    if not raw_value:
        return _default_tenant_cohort_slo_policies()

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        config_error(
            "Invalid TENANT_COHORT_SLO_POLICIES format.",
            hint=(
                "Use a JSON array of cohort policy objects. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        config_error(
            "Invalid TENANT_COHORT_SLO_POLICIES format.",
            hint="Use a non-empty JSON array of cohort policy objects.",
        )

    normalized_policies: list[dict[str, object]] = []
    cohort_names: set[str] = set()
    for policy in parsed:
        if not isinstance(policy, dict):
            config_error(
                "Invalid TENANT_COHORT_SLO_POLICIES entry.",
                hint="Each cohort policy must be a JSON object.",
            )

        name = str(policy.get("name") or "").strip().lower()
        if not name:
            config_error(
                "Invalid TENANT_COHORT_SLO_POLICIES entry.",
                hint="Each cohort policy must include a non-empty name.",
            )
        if name in cohort_names:
            config_error(
                f"Duplicate cohort name '{name}' in TENANT_COHORT_SLO_POLICIES.",
                hint="Use unique cohort names.",
            )
        cohort_names.add(name)

        try:
            max_sources = int(policy.get("max_sources"))  # type: ignore[arg-type]
            max_commit_age_seconds = int(policy.get("max_commit_age_seconds"))  # type: ignore[arg-type]
            max_consumer_lag = int(policy.get("max_consumer_lag"))  # type: ignore[arg-type]
            max_dlq_messages = int(policy.get("max_dlq_messages"))  # type: ignore[arg-type]
            availability_target = float(policy.get("availability_target"))  # type: ignore[arg-type]
            error_budget_percent = float(policy.get("error_budget_percent"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            config_error(
                f"Invalid numeric SLO values for cohort '{name}'.",
                hint="Use numeric values for thresholds and percentages.",
            )

        if max_sources < 1:
            config_error(
                f"max_sources must be at least 1 for cohort '{name}'.",
                hint="Set max_sources to a positive integer.",
            )
        if not 0 < availability_target <= 100:
            config_error(
                f"availability_target must be in (0, 100] for cohort '{name}'.",
                hint="Use a percentage such as 99.9.",
            )
        if not 0 <= error_budget_percent <= 100:
            config_error(
                f"error_budget_percent must be in [0, 100] for cohort '{name}'.",
                hint="Use a percentage such as 0.1.",
            )
        if max_commit_age_seconds < 0:
            config_error(
                f"max_commit_age_seconds cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )
        if max_consumer_lag < 0:
            config_error(
                f"max_consumer_lag cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )
        if max_dlq_messages < 0:
            config_error(
                f"max_dlq_messages cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )

        normalized_policies.append({
            "name": name,
            "max_sources": max_sources,
            "availability_target": availability_target,
            "error_budget_percent": error_budget_percent,
            "max_commit_age_seconds": max_commit_age_seconds,
            "max_consumer_lag": max_consumer_lag,
            "max_dlq_messages": max_dlq_messages,
        })

    return sorted(
        normalized_policies,
        key=lambda p: p["max_sources"],
    )


# ===========================================================================
# Domain sub-settings
# ===========================================================================


@dataclass
class DatabaseSettings:
    """Connection-pool tuning for the monitor's own PostgreSQL database.

    Every field maps to a ``DB_*`` / ``POSTGRES_*`` environment variable.
    """

    postgres_url: str = field(
        default_factory=lambda: _normalize_postgres_url(
            get_env_or_file(
                "POSTGRES_URL",
                "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres",
            )
            or "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres",
        )
    )
    """Monitor database DSN. Default: local postgres on port 5437."""
    sqlalchemy_echo: bool = field(
        default_factory=lambda: os.getenv("SQLALCHEMY_ECHO", "false").lower()
        == "true"
    )
    """Enable SQLAlchemy echo logging. Default: false."""
    pool_size: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "10"))
    )
    """Persistent pool size. Default: 10."""
    max_overflow: int = field(
        default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "20"))
    )
    """Max overflow connections beyond pool_size. Default: 20."""
    pool_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30"))
    )
    """Seconds to wait for a connection from the pool. Default: 30."""
    pool_recycle_seconds: int = field(
        default_factory=lambda: int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800"))
    )
    """Recycle connections after this many seconds. Default: 1800 (30 min)."""
    pool_pre_ping: bool = field(
        default_factory=lambda: os.getenv("DB_POOL_PRE_PING", "true").lower()
        == "true"
    )
    """Test connections before checkout. Default: true."""


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

    # --- Computed (populated after construction via _post_load) ---
    kafka_topics: list[str] = field(default_factory=list)
    """All subscribed Kafka topics (derived from KAFKA_TOPICS CSV or KAFKA_TOPIC)."""
    kafka_ssl_context: ssl.SSLContext | None = field(default=None)
    """Pre-built SSL context when Kafka security protocol requires it."""
    kafka_clusters: list[dict[str, object]] = field(default_factory=list)
    """Multi-cluster Kafka definitions parsed from KAFKA_CLUSTERS JSON."""
    broker_clusters: list[dict[str, object]] = field(default_factory=list)
    """Generic broker cluster definitions parsed from BROKER_CLUSTERS JSON."""


@dataclass
class AuthSettings:
    """Authentication, JWT, and API key lifecycle settings."""

    jwt_secret: str = field(
        default_factory=lambda: get_env_or_file(
            "JWT_SECRET", "dev-insecure-secret-change-me-please-rotate"
        )
        or "dev-insecure-secret-change-me-please-rotate"
    )
    """JWT signing secret (supports _FILE variant)."""
    jwt_secret_next: str | None = field(
        default_factory=lambda: get_env_or_file("JWT_SECRET_NEXT")
    )
    """Secondary JWT secret for rotation overlap (supports _FILE variant)."""
    jwt_algorithm: str = field(
        default_factory=lambda: os.getenv("JWT_ALGORITHM", "HS256")
    )
    """JWT signing algorithm. Default: HS256."""
    access_token_ttl_minutes: int = field(
        default_factory=lambda: int(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "15"))
    )
    """Bearer token time-to-live in minutes. Default: 15."""
    ws_session_token_ttl_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("WS_SESSION_TOKEN_TTL_SECONDS", "60")
        )
    )
    """WebSocket session token TTL in seconds. Default: 60."""
    api_key_default_ttl_days: int = field(
        default_factory=lambda: int(os.getenv("API_KEY_DEFAULT_TTL_DAYS", "90"))
    )
    """Default API key expiration in days. Default: 90."""
    allow_bootstrap: bool = field(
        default_factory=lambda: _default_allow_bootstrap()
    )
    """Allow the /auth/bootstrap endpoint. Disabled in production by default."""
    ws_backplane_enabled: bool = field(
        default_factory=lambda: os.getenv("WS_BACKPLANE_ENABLED", "true").lower()
        == "true"
    )
    """Enable Postgres LISTEN/NOTIFY backplane for WebSocket fanout. Default: true."""
    ws_backplane_channel: str = field(
        default_factory=lambda: os.getenv(
            "WS_BACKPLANE_CHANNEL", "db_monitor_ws_events"
        )
    )
    """Postgres NOTIFY channel name for WebSocket backplane."""


@dataclass
class WebhookSettings:
    """Outbound webhook delivery configuration."""

    urls: list[str] = field(default_factory=list)
    """Webhook destination URLs (parsed from WEBHOOK_URLS CSV)."""
    timeout_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_TIMEOUT_SECONDS", "5.0")
        )
    )
    """Per-request webhook timeout in seconds. Default: 5.0."""
    shared_secret: str | None = field(
        default_factory=lambda: get_env_or_file("WEBHOOK_SHARED_SECRET")
    )
    """HMAC-SHA256 signing secret for webhook payloads (supports _FILE variant)."""
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
    )
    """Max delivery retries per webhook. Default: 3."""
    retry_backoff_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_RETRY_BACKOFF_SECONDS", "0.5")
        )
    )
    """Base retry backoff in seconds (exponential). Default: 0.5."""
    circuit_breaker_threshold: int = field(
        default_factory=lambda: int(
            os.getenv("WEBHOOK_CIRCUIT_BREAKER_THRESHOLD", "5")
        )
    )
    """Consecutive failures before circuit opens. Default: 5."""
    circuit_breaker_recovery_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS", "30.0")
        )
    )
    """Seconds before a tripped circuit breaker resets. Default: 30.0."""


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


@dataclass
class RetentionSettings:
    """Data-retention policies for events, column changes, DLQ, and audit logs."""

    cleanup_enabled: bool = field(
        default_factory=lambda: os.getenv("RETENTION_CLEANUP_ENABLED", "false").lower()
        == "true"
    )
    """Enable periodic retention cleanup. Default: false."""
    cleanup_interval_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "3600")
        )
    )
    """Seconds between cleanup cycles. Default: 3600 (1 hour)."""
    cleanup_batch_size: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_BATCH_SIZE", "1000")
        )
    )
    """Rows to delete per batch during cleanup. Default: 1000."""
    cleanup_max_batches_per_table: int = field(
        default_factory=lambda: int(
            os.getenv("RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE", "10")
        )
    )
    """Max batches per table per cycle. Default: 10."""
    archive_before_delete: bool = field(
        default_factory=lambda: os.getenv(
            "RETENTION_ARCHIVE_BEFORE_DELETE", "false"
        ).lower()
        == "true"
    )
    """Archive rows to disk before deleting them. Default: false."""
    archive_dir: str = field(
        default_factory=lambda: os.getenv(
            "RETENTION_ARCHIVE_DIR", "retention-archive"
        )
    )
    """Directory for retention archives. Default: retention-archive."""
    event_retention_days: int = field(
        default_factory=lambda: int(os.getenv("EVENT_RETENTION_DAYS", "30"))
    )
    """Days to retain events. Default: 30."""
    column_changes_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("COLUMN_CHANGES_RETENTION_DAYS", "30")
        )
    )
    """Days to retain column changes. Default: 30."""
    dead_letter_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("DEAD_LETTER_RETENTION_DAYS", "30")
        )
    )
    """Days to retain DLQ records. Default: 30."""
    api_audit_log_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("API_AUDIT_LOG_RETENTION_DAYS", "90")
        )
    )
    """Days to retain API audit logs. Default: 90."""


@dataclass
class IngestionSettings:
    """CDC ingestion quota enforcement and bulk-write behavior."""

    quota_enabled: bool = field(
        default_factory=lambda: os.getenv("INGESTION_QUOTA_ENABLED", "false").lower()
        == "true"
    )
    """Enable per-source and per-tenant ingestion quotas. Default: false."""
    quota_window_seconds: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_QUOTA_WINDOW_SECONDS", "60")
        )
    )
    """Quota evaluation window in seconds. Default: 60."""
    source_default_events_per_window: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW", "0")
        )
    )
    """Default event limit per source per window. 0=unlimited. Default: 0."""
    tenant_default_events_per_window: int = field(
        default_factory=lambda: int(
            os.getenv("INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW", "0")
        )
    )
    """Default event limit per tenant per window. 0=unlimited. Default: 0."""
    quota_mode: str = field(
        default_factory=lambda: os.getenv(
            "INGESTION_QUOTA_MODE", "throttle"
        ).strip().lower()
    )
    """Quota enforcement mode: ``throttle`` or ``drop``. Default: throttle."""
    quota_max_throttle_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("INGESTION_QUOTA_MAX_THROTTLE_SECONDS", "5.0")
        )
    )
    """Max throttle sleep per event in seconds. Default: 5.0."""
    bulk_write_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "INGESTION_BULK_WRITE_ENABLED", "true"
        ).lower()
        == "true"
    )
    """Enable bulk (batch) database writes for ingestion. Default: true."""

    # -- Computed (populated after construction via from_env) --
    source_quotas: dict[str, int] = field(default_factory=dict)
    """Per-source quota overrides parsed from INGESTION_SOURCE_QUOTAS JSON."""
    tenant_quotas: dict[str, int] = field(default_factory=dict)
    """Per-tenant quota overrides parsed from INGESTION_TENANT_QUOTAS JSON."""


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
    tenant_cohort_slo_policies: list[dict[str, object]] = field(
        default_factory=list
    )
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
    observability: ObservabilitySettings = field(
        default_factory=ObservabilitySettings
    )
    """Tracing, metrics, and audit-log buffer settings."""
    retention: RetentionSettings = field(default_factory=RetentionSettings)
    """Data-retention policy settings."""
    ingestion: IngestionSettings = field(default_factory=IngestionSettings)
    """Ingestion quota and bulk-write settings."""

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> AppSettings:
        """Load and validate all settings from environment variables once."""
        env = os.getenv("APP_ENV", "development").lower()

        db = DatabaseSettings()
        broker = BrokerSettings()
        auth = AuthSettings()
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
        broker.kafka_clusters = _load_kafka_clusters(broker)
        broker.broker_clusters = _load_broker_clusters(broker)

        # Ingestion computed fields
        ingestion.source_quotas = _load_ingestion_quota_overrides(
            "INGESTION_SOURCE_QUOTAS"
        )
        ingestion.tenant_quotas = _load_ingestion_quota_overrides(
            "INGESTION_TENANT_QUOTAS"
        )

        # Webhook computed fields
        webhook.urls = parse_csv_list(os.getenv("WEBHOOK_URLS"))

        # Assemble top-level
        settings_obj = cls(
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
            tenant_cohort_slo_policies=_load_tenant_cohort_slo_policies(),
            db=db,
            broker=broker,
            auth=auth,
            webhook=webhook,
            observability=observability,
            retention=retention,
            ingestion=ingestion,
        )
        settings_obj.validate()
        return settings_obj

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """Cross-check all config values and raise ``ValueError`` on misconfiguration."""
        # DB schema mode
        if self.db_schema_mode not in {"apply", "validate", "skip"}:
            config_error(
                f"Invalid DB_SCHEMA_MODE='{self.db_schema_mode}'.",
                hint="Use one of: apply, validate, skip.",
            )

        # Production hardening
        if self.env in {"production", "prod"}:
            if self.db.pool_size < 1:
                config_error(
                    "DB_POOL_SIZE must be at least 1.",
                    hint="Increase DB_POOL_SIZE or remove the invalid override.",
                )
            if self.db.max_overflow < 0:
                config_error(
                    "DB_MAX_OVERFLOW cannot be negative.",
                    hint="Set DB_MAX_OVERFLOW to 0 or a positive integer.",
                )
            if self.db.pool_timeout_seconds < 1:
                config_error(
                    "DB_POOL_TIMEOUT_SECONDS must be at least 1.",
                    hint="Set a positive checkout timeout in seconds.",
                )
            if self.db.pool_recycle_seconds < 0:
                config_error(
                    "DB_POOL_RECYCLE_SECONDS cannot be negative.",
                    hint="Use 0 to disable recycle behavior or a positive integer.",
                )
            if self.db.postgres_url == "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres":
                config_error(
                    "POSTGRES_URL must be explicitly configured in production.",
                    hint=(
                        "Set POSTGRES_URL or POSTGRES_URL_FILE to the real monitor "
                        "database connection string."
                    ),
                )
            if self.auth.jwt_secret == "dev-insecure-secret-change-me-please-rotate":
                config_error(
                    "JWT_SECRET must be explicitly configured in production.",
                    hint=(
                        "Set JWT_SECRET or JWT_SECRET_FILE to a non-default secret "
                        "before startup."
                    ),
                )
            if len(self.auth.jwt_secret) < 32:
                config_error(
                    "JWT_SECRET must be at least 32 characters long in production.",
                    hint="Generate a longer secret and redeploy.",
                )
            if self.auth.jwt_secret_next and len(self.auth.jwt_secret_next) < 32:
                config_error(
                    "JWT_SECRET_NEXT must be at least 32 characters long in production.",
                    hint="Use a 32+ character overlap secret during rotation.",
                )
            # Override allow_bootstrap for production
            if not os.getenv("ALLOW_BOOTSTRAP"):
                self.auth.allow_bootstrap = False

        # Observability
        if self.observability.otel_exporter not in {"otlp", "console"}:
            config_error(
                f"Invalid OTEL_EXPORTER='{self.observability.otel_exporter}'.",
                hint="Use one of: otlp, console.",
            )
        if (
            self.observability.otel_tracing_enabled
            and self.observability.otel_exporter == "otlp"
            and not self.observability.otel_exporter_otlp_endpoint
        ):
            config_error(
                "OTEL_EXPORTER_OTLP_ENDPOINT is required when tracing "
                "is enabled with the OTLP exporter.",
                hint=(
                    "Set OTEL_EXPORTER_OTLP_ENDPOINT to your collector URL or "
                    "switch OTEL_EXPORTER=console for local debugging."
                ),
            )

        # Webhook validations
        if self.webhook.timeout_seconds <= 0:
            config_error(
                "WEBHOOK_TIMEOUT_SECONDS must be greater than zero.",
                hint="Set a positive timeout value such as 5.0.",
            )
        if self.webhook.max_retries < 0:
            config_error(
                "WEBHOOK_MAX_RETRIES cannot be negative.",
                hint="Use 0 to disable retries or a positive integer.",
            )
        if self.webhook.retry_backoff_seconds < 0:
            config_error(
                "WEBHOOK_RETRY_BACKOFF_SECONDS cannot be negative.",
                hint="Use 0 for immediate retries or a positive delay.",
            )
        if self.webhook.circuit_breaker_threshold < 1:
            config_error(
                "WEBHOOK_CIRCUIT_BREAKER_THRESHOLD must be at least 1.",
                hint="Use a positive integer failure threshold.",
            )
        if self.webhook.circuit_breaker_recovery_seconds <= 0:
            config_error(
                "WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS must be greater than zero.",
                hint="Use a positive recovery timeout in seconds.",
            )
        for url in self.webhook.urls:
            if not url.startswith(("http://", "https://")):
                config_error(
                    f"Invalid WEBHOOK_URLS entry '{url}'.",
                    hint="Use comma-separated http:// or https:// URLs.",
                )

        # Broker validations
        if self.broker.message_broker not in {"kafka", "rabbitmq"}:
            config_error(
                f"Invalid MESSAGE_BROKER='{self.broker.message_broker}'.",
                hint="Use one of: kafka, rabbitmq.",
            )
        if self.broker.rabbitmq_prefetch_count <= 0:
            config_error(
                "RABBITMQ_PREFETCH_COUNT must be greater than zero.",
                hint="Set a positive prefetch value such as 100.",
            )

        # Ingestion validations
        if self.ingestion.quota_window_seconds <= 0:
            config_error(
                "INGESTION_QUOTA_WINDOW_SECONDS must be greater than zero.",
                hint="Set a positive quota window in seconds.",
            )
        if self.ingestion.source_default_events_per_window < 0:
            config_error(
                "INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW cannot be negative.",
                hint="Use 0 to disable default source quotas or a positive integer.",
            )
        if self.ingestion.tenant_default_events_per_window < 0:
            config_error(
                "INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW cannot be negative.",
                hint="Use 0 to disable default tenant quotas or a positive integer.",
            )
        if self.ingestion.quota_mode not in {"throttle", "drop"}:
            config_error(
                f"Invalid INGESTION_QUOTA_MODE='{self.ingestion.quota_mode}'.",
                hint="Use one of: throttle, drop.",
            )
        if self.ingestion.quota_max_throttle_seconds <= 0:
            config_error(
                "INGESTION_QUOTA_MAX_THROTTLE_SECONDS must be greater than zero.",
                hint="Set a positive max throttle sleep such as 1.0.",
            )

        # SLO validations
        if self.slo_rolling_window_days <= 0:
            config_error(
                "SLO_ROLLING_WINDOW_DAYS must be greater than zero.",
                hint="Use a positive rolling window such as 30.",
            )

        # Retention validations
        if self.retention.cleanup_interval_seconds <= 0:
            config_error(
                "RETENTION_CLEANUP_INTERVAL_SECONDS must be greater than zero.",
                hint="Use a positive cleanup interval in seconds.",
            )
        if self.retention.cleanup_batch_size <= 0:
            config_error(
                "RETENTION_CLEANUP_BATCH_SIZE must be greater than zero.",
                hint="Use a positive retention cleanup batch size.",
            )
        if self.retention.cleanup_max_batches_per_table <= 0:
            config_error(
                "RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE must be greater than zero.",
                hint="Use at least one batch per cleanup cycle.",
            )
        for days, name in (
            (self.retention.event_retention_days, "EVENT_RETENTION_DAYS"),
            (self.retention.column_changes_retention_days, "COLUMN_CHANGES_RETENTION_DAYS"),
            (self.retention.dead_letter_retention_days, "DEAD_LETTER_RETENTION_DAYS"),
            (self.retention.api_audit_log_retention_days, "API_AUDIT_LOG_RETENTION_DAYS"),
        ):
            if days < 0:
                config_error(
                    f"{name} cannot be negative.",
                    hint="Use 0 to disable cleanup for one table or a positive integer.",
                )


# ---------------------------------------------------------------------------
# Module-level singleton (loaded once via @cache)
# ---------------------------------------------------------------------------


@cache
def _get_settings() -> AppSettings:
    """Return the lazily loaded, validated settings singleton."""
    return AppSettings.from_env()


settings = _get_settings()
"""Application-wide configuration singleton.  Loaded once on first access."""

