"""Configuration and environment variables for the DB Monitor server."""

from __future__ import annotations

import json
import os
import ssl

from core.config_loader import (
    config_error as _config_error,
    default_schema_mode as _default_schema_mode,
    get_env_or_file as _get_env_or_file,
    parse_csv_list as _parse_csv_list,
)
from dotenv import load_dotenv

load_dotenv()


DEFAULT_POSTGRES_URL = (
    "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
)
DEFAULT_JWT_SECRET = "dev-insecure-secret-change-me-please-rotate"


APP_ENV = os.getenv("APP_ENV", "development").lower()
DB_SCHEMA_MODE = os.getenv(
    "DB_SCHEMA_MODE",
    _default_schema_mode(APP_ENV),
).lower()
if DB_SCHEMA_MODE not in {"apply", "validate", "skip"}:
    _config_error(
        f"Invalid DB_SCHEMA_MODE='{DB_SCHEMA_MODE}'.",
        hint="Use one of: apply, validate, skip.",
    )

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL",
    "PLAINTEXT",
).upper()
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE")
KAFKA_SSL_CERTFILE = os.getenv("KAFKA_SSL_CERTFILE")
KAFKA_SSL_KEYFILE = os.getenv("KAFKA_SSL_KEYFILE")
KAFKA_SSL_PASSWORD = _get_env_or_file("KAFKA_SSL_PASSWORD")

KAFKA_SSL_CONTEXT = None
if KAFKA_SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
    KAFKA_SSL_CONTEXT = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=KAFKA_SSL_CAFILE,
    )
    if KAFKA_SSL_CERTFILE and KAFKA_SSL_KEYFILE:
        KAFKA_SSL_CONTEXT.load_cert_chain(
            certfile=KAFKA_SSL_CERTFILE,
            keyfile=KAFKA_SSL_KEYFILE,
            password=KAFKA_SSL_PASSWORD,
        )

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orderdb.public.orders")
KAFKA_TOPICS = _parse_csv_list(os.getenv("KAFKA_TOPICS")) or [KAFKA_TOPIC]
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "fastapi-consumer-group",
)
MESSAGE_BROKER = os.getenv("MESSAGE_BROKER", "kafka").lower()
RABBITMQ_URL = _get_env_or_file(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost/",
)
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "db-monitor-events")
RABBITMQ_QUEUES = _parse_csv_list(os.getenv("RABBITMQ_QUEUES")) or [
    RABBITMQ_QUEUE
]
RABBITMQ_PREFETCH_COUNT = int(
    os.getenv("RABBITMQ_PREFETCH_COUNT", "100")
)
RABBITMQ_DLQ_QUEUE = os.getenv("RABBITMQ_DLQ_QUEUE", "db-monitor-dlq")


def _default_kafka_cluster() -> dict[str, object]:
    """Build the single-cluster compatibility config."""
    return {
        "name": "default",
        "bootstrap_servers": [KAFKA_BROKER],
        "topics": list(KAFKA_TOPICS),
        "consumer_group": KAFKA_CONSUMER_GROUP,
        "topic_partitions": None,
    }


def _default_rabbitmq_cluster() -> dict[str, object]:
    """Build the single-cluster compatibility config for RabbitMQ."""
    return {
        "name": "default",
        "broker_kind": "rabbitmq",
        "bootstrap_servers": None,
        "topics": list(RABBITMQ_QUEUES),
        "consumer_group": KAFKA_CONSUMER_GROUP,
        "topic_partitions": None,
        "connection_url": RABBITMQ_URL,
        "queue_names": list(RABBITMQ_QUEUES),
        "prefetch_count": RABBITMQ_PREFETCH_COUNT,
        "dlq_destination": RABBITMQ_DLQ_QUEUE,
    }


def _normalize_bootstrap_servers(
    value: str | list[str] | None,
    cluster_name: str,
) -> list[str]:
    """Normalize bootstrap server values into a non-empty list."""
    if isinstance(value, str):
        servers = _parse_csv_list(value)
    elif isinstance(value, list):
        servers = [
            str(server).strip()
            for server in value
            if str(server).strip()
        ]
    else:
        servers = []

    if not servers:
        _config_error(
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
        topics = list(KAFKA_TOPICS)
    elif isinstance(value, str):
        topics = _parse_csv_list(value)
    elif isinstance(value, list):
        topics = [str(topic).strip() for topic in value if str(topic).strip()]
    else:
        topics = []

    if not topics:
        _config_error(
            f"Kafka cluster '{cluster_name}' must define at least one topic.",
            hint="Set topics as a list or comma-separated string.",
        )
    return topics


def _normalize_partition_list(
    value: object,
    cluster_name: str,
    topic_name: str,
) -> list[int]:
    """Normalize an explicit partition list for one topic."""
    if isinstance(value, str):
        raw_partitions = _parse_csv_list(value)
    elif isinstance(value, list):
        raw_partitions = value
    else:
        raw_partitions = []

    partitions: list[int] = []
    for raw_partition in raw_partitions:
        try:
            partition = int(raw_partition)
        except (TypeError, ValueError):
            _config_error(
                (
                    f"Kafka cluster '{cluster_name}' topic '{topic_name}' "
                    "contains an invalid partition value."
                ),
                hint="Use integer partition numbers such as [0, 1, 2].",
            )
        if partition < 0:
            _config_error(
                (
                    f"Kafka cluster '{cluster_name}' topic '{topic_name}' "
                    "contains a negative partition."
                ),
                hint="Use partition numbers greater than or equal to zero.",
            )
        partitions.append(partition)

    if not partitions:
        _config_error(
            (
                f"Kafka cluster '{cluster_name}' topic '{topic_name}' must "
                "define at least one partition."
            ),
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
        _config_error(
            f"Kafka cluster '{cluster_name}' has invalid topic_partitions.",
            hint="Use a JSON object like {'orders.events': [0, 1] }.",
        )

    topic_partitions: dict[str, list[int]] = {}
    for raw_topic_name, raw_partitions in value.items():
        topic_name = str(raw_topic_name).strip()
        if not topic_name:
            _config_error(
                f"Kafka cluster '{cluster_name}' has an empty topic name.",
                hint="Provide non-empty topic names in topic_partitions.",
            )
        topic_partitions[topic_name] = _normalize_partition_list(
            raw_partitions,
            cluster_name,
            topic_name,
        )

    return topic_partitions


def _load_kafka_clusters() -> list[dict[str, object]]:
    """Load multi-cluster Kafka configuration from JSON when provided."""
    raw_clusters = os.getenv("KAFKA_CLUSTERS")
    if not raw_clusters:
        return [_default_kafka_cluster()]

    try:
        parsed = json.loads(raw_clusters)
    except json.JSONDecodeError as exc:
        _config_error(
            "Invalid KAFKA_CLUSTERS format.",
            hint=(
                "Provide a JSON array of cluster definitions with name, "
                "bootstrap_servers, and optional topics/consumer_group. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        _config_error(
            "Invalid KAFKA_CLUSTERS format.",
            hint="Provide a non-empty JSON array of cluster definitions.",
        )

    cluster_names: set[str] = set()
    clusters: list[dict[str, object]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            _config_error(
                "Invalid KAFKA_CLUSTERS entry.",
                hint="Each cluster definition must be a JSON object.",
            )

        cluster_name = str(entry.get("name") or "").strip()
        if not cluster_name:
            _config_error(
                "Invalid KAFKA_CLUSTERS entry.",
                hint="Each cluster definition must include a unique name.",
            )
        if cluster_name in cluster_names:
            _config_error(
                f"Duplicate Kafka cluster name '{cluster_name}'.",
                hint="Use unique names for each cluster entry.",
            )
        cluster_names.add(cluster_name)

        topic_partitions = _normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics = _normalize_cluster_topics(
            entry.get("topics"),
            cluster_name,
        )
        if topic_partitions is not None:
            partition_topics = sorted(topic_partitions.keys())
            if (
                entry.get("topics") is not None
                and sorted(topics) != partition_topics
            ):
                _config_error(
                    (
                        f"Kafka cluster '{cluster_name}' topics and "
                        "topic_partitions must describe the same topics."
                    ),
                    hint=(
                        "Either omit topics and derive them from "
                        "topic_partitions, or keep both lists aligned."
                    ),
                )
            topics = partition_topics

        clusters.append(
            {
                "name": cluster_name,
                "bootstrap_servers": _normalize_bootstrap_servers(
                    entry.get("bootstrap_servers"),
                    cluster_name,
                ),
                "topics": topics,
                "topic_partitions": topic_partitions,
                "consumer_group": str(
                    entry.get("consumer_group") or KAFKA_CONSUMER_GROUP
                ).strip(),
            }
        )

    return clusters


def _normalize_rabbitmq_queue_names(
    value: object,
    cluster_name: str,
) -> list[str]:
    """Normalize per-cluster RabbitMQ queue configuration."""
    if value is None:
        queue_names = list(RABBITMQ_QUEUES)
    elif isinstance(value, str):
        queue_names = _parse_csv_list(value)
    elif isinstance(value, list):
        queue_names = [
            str(queue_name).strip()
            for queue_name in value
            if str(queue_name).strip()
        ]
    else:
        queue_names = []

    if not queue_names:
        _config_error(
            f"RabbitMQ cluster '{cluster_name}' must define queue_names.",
            hint="Set queue_names as a list or comma-separated string.",
        )

    return queue_names


def _normalize_broker_kind(value: object, cluster_name: str) -> str:
    """Normalize one broker kind string."""
    broker_kind = str(value or MESSAGE_BROKER).strip().lower()
    if broker_kind not in {"kafka", "rabbitmq"}:
        _config_error(
            (
                f"Broker cluster '{cluster_name}' has unsupported "
                f"broker_kind '{broker_kind}'."
            ),
            hint="Use one of: kafka, rabbitmq.",
        )
    return broker_kind


def _load_broker_clusters() -> list[dict[str, object]]:
    """Load generic broker cluster configuration."""
    raw_clusters = os.getenv("BROKER_CLUSTERS")
    if not raw_clusters:
        if MESSAGE_BROKER == "rabbitmq":
            return [_default_rabbitmq_cluster()]
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
            for cluster in KAFKA_CLUSTERS
        ]

    try:
        parsed = json.loads(raw_clusters)
    except json.JSONDecodeError as exc:
        _config_error(
            "Invalid BROKER_CLUSTERS format.",
            hint=(
                "Provide a JSON array of broker definitions. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        _config_error(
            "Invalid BROKER_CLUSTERS format.",
            hint="Provide a non-empty JSON array of broker definitions.",
        )

    cluster_names: set[str] = set()
    clusters: list[dict[str, object]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            _config_error(
                "Invalid BROKER_CLUSTERS entry.",
                hint="Each broker definition must be a JSON object.",
            )

        cluster_name = str(entry.get("name") or "").strip()
        if not cluster_name:
            _config_error(
                "Invalid BROKER_CLUSTERS entry.",
                hint="Each broker definition must include a unique name.",
            )
        if cluster_name in cluster_names:
            _config_error(
                f"Duplicate broker cluster name '{cluster_name}'.",
                hint="Use unique names for each broker cluster entry.",
            )
        cluster_names.add(cluster_name)

        broker_kind = _normalize_broker_kind(
            entry.get("broker_kind"),
            cluster_name,
        )
        if broker_kind == "rabbitmq":
            if entry.get("topic_partitions") is not None:
                _config_error(
                    (
                        f"RabbitMQ cluster '{cluster_name}' does not support "
                        "topic_partitions."
                    ),
                    hint="Remove topic_partitions for rabbitmq clusters.",
                )
            queue_names = _normalize_rabbitmq_queue_names(
                entry.get("queue_names") or entry.get("topics"),
                cluster_name,
            )
            clusters.append(
                {
                    "name": cluster_name,
                    "broker_kind": broker_kind,
                    "bootstrap_servers": None,
                    "topics": list(queue_names),
                    "topic_partitions": None,
                    "consumer_group": str(
                        entry.get("consumer_group") or KAFKA_CONSUMER_GROUP
                    ).strip(),
                    "connection_url": str(
                        entry.get("connection_url") or RABBITMQ_URL
                    ).strip(),
                    "queue_names": queue_names,
                    "prefetch_count": int(
                        entry.get("prefetch_count")
                        or RABBITMQ_PREFETCH_COUNT
                    ),
                    "dlq_destination": str(
                        entry.get("dlq_destination") or RABBITMQ_DLQ_QUEUE
                    ).strip(),
                }
            )
            continue

        topic_partitions = _normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics = _normalize_cluster_topics(entry.get("topics"), cluster_name)
        if topic_partitions is not None:
            partition_topics = sorted(topic_partitions.keys())
            if (
                entry.get("topics") is not None
                and sorted(topics) != partition_topics
            ):
                _config_error(
                    (
                        f"Kafka cluster '{cluster_name}' topics and "
                        "topic_partitions must describe the same topics."
                    ),
                    hint=(
                        "Either omit topics and derive them from "
                        "topic_partitions, or keep both lists aligned."
                    ),
                )
            topics = partition_topics

        clusters.append(
            {
                "name": cluster_name,
                "broker_kind": broker_kind,
                "bootstrap_servers": _normalize_bootstrap_servers(
                    entry.get("bootstrap_servers"),
                    cluster_name,
                ),
                "topics": topics,
                "topic_partitions": topic_partitions,
                "consumer_group": str(
                    entry.get("consumer_group") or KAFKA_CONSUMER_GROUP
                ).strip(),
                "connection_url": None,
                "queue_names": None,
                "prefetch_count": None,
                "dlq_destination": str(
                    entry.get("dlq_destination") or "db-monitor-dlq"
                ).strip(),
            }
        )

    return clusters


KAFKA_CLUSTERS = _load_kafka_clusters()
BROKER_CLUSTERS = _load_broker_clusters()
POSTGRES_URL = _get_env_or_file(
    "POSTGRES_URL",
    DEFAULT_POSTGRES_URL,
)

if not POSTGRES_URL.startswith("postgresql+asyncpg://"):
    if POSTGRES_URL.startswith("postgresql://"):
        POSTGRES_URL = POSTGRES_URL.replace(
            "postgresql://",
            "postgresql+asyncpg://",
        )
    else:
        _config_error(
            "Invalid POSTGRES_URL format.",
            hint=(
                "Use a PostgreSQL DSN starting with 'postgresql+asyncpg://'."
            ),
        )

SQLALCHEMY_ECHO = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_TIMEOUT_SECONDS = int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30"))
DB_POOL_RECYCLE_SECONDS = int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800"))
DB_POOL_PRE_PING = (
    os.getenv(
        "DB_POOL_PRE_PING",
        "true",
    ).lower()
    == "true"
)
DLQ_ENABLED = os.getenv("DLQ_ENABLED", "true").lower() == "true"
BATCH_ENABLED = os.getenv("BATCH_ENABLED", "true").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
ALLOW_BOOTSTRAP = (
    os.getenv(
        "ALLOW_BOOTSTRAP",
        "false" if APP_ENV in {"production", "prod"} else "true",
    ).lower()
    == "true"
)
API_KEY_DEFAULT_TTL_DAYS = int(os.getenv("API_KEY_DEFAULT_TTL_DAYS", "90"))
READINESS_MAX_COMMIT_AGE_SECONDS = int(
    os.getenv("READINESS_MAX_COMMIT_AGE_SECONDS", "300")
)
READINESS_MAX_CONSUMER_LAG = int(
    os.getenv("READINESS_MAX_CONSUMER_LAG", "1000")
)
READINESS_MAX_DLQ_MESSAGES = int(os.getenv("READINESS_MAX_DLQ_MESSAGES", "0"))
JWT_SECRET = _get_env_or_file(
    "JWT_SECRET",
    DEFAULT_JWT_SECRET,
)
JWT_SECRET_NEXT = _get_env_or_file("JWT_SECRET_NEXT")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_TTL_MINUTES = int(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "15"))
WS_SESSION_TOKEN_TTL_SECONDS = int(
    os.getenv("WS_SESSION_TOKEN_TTL_SECONDS", "60")
)
WS_BACKPLANE_ENABLED = (
    os.getenv(
        "WS_BACKPLANE_ENABLED",
        "true",
    ).lower()
    == "true"
)
WS_BACKPLANE_CHANNEL = os.getenv(
    "WS_BACKPLANE_CHANNEL",
    "db_monitor_ws_events",
)
WEBHOOK_URLS = _parse_csv_list(os.getenv("WEBHOOK_URLS"))
WEBHOOK_TIMEOUT_SECONDS = float(
    os.getenv("WEBHOOK_TIMEOUT_SECONDS", "5.0")
)
WEBHOOK_SHARED_SECRET = _get_env_or_file("WEBHOOK_SHARED_SECRET")
WEBHOOK_MAX_RETRIES = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
WEBHOOK_RETRY_BACKOFF_SECONDS = float(
    os.getenv("WEBHOOK_RETRY_BACKOFF_SECONDS", "0.5")
)
WEBHOOK_CIRCUIT_BREAKER_THRESHOLD = int(
    os.getenv("WEBHOOK_CIRCUIT_BREAKER_THRESHOLD", "5")
)
WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS = float(
    os.getenv("WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS", "30.0")
)
OTEL_TRACING_ENABLED = (
    os.getenv(
        "OTEL_TRACING_ENABLED",
        "false",
    ).lower()
    == "true"
)
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "db-monitor")
OTEL_EXPORTER = os.getenv("OTEL_EXPORTER", "otlp").lower()
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
OTEL_EXPORTER_OTLP_HEADERS = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")
AUDIT_LOG_QUEUE_MAXSIZE = int(os.getenv("AUDIT_LOG_QUEUE_MAXSIZE", "10000"))
AUDIT_LOG_BATCH_SIZE = int(os.getenv("AUDIT_LOG_BATCH_SIZE", "100"))
AUDIT_LOG_FLUSH_INTERVAL_SECONDS = float(
    os.getenv("AUDIT_LOG_FLUSH_INTERVAL_SECONDS", "1.0")
)
APP_SHUTDOWN_TIMEOUT_SECONDS = float(
    os.getenv("APP_SHUTDOWN_TIMEOUT_SECONDS", "45")
)

if APP_ENV in {"production", "prod"}:
    if DB_POOL_SIZE < 1:
        _config_error(
            "DB_POOL_SIZE must be at least 1.",
            hint="Increase DB_POOL_SIZE or remove the invalid override.",
        )
    if DB_MAX_OVERFLOW < 0:
        _config_error(
            "DB_MAX_OVERFLOW cannot be negative.",
            hint="Set DB_MAX_OVERFLOW to 0 or a positive integer.",
        )
    if DB_POOL_TIMEOUT_SECONDS < 1:
        _config_error(
            "DB_POOL_TIMEOUT_SECONDS must be at least 1.",
            hint="Set a positive checkout timeout in seconds.",
        )
    if DB_POOL_RECYCLE_SECONDS < 0:
        _config_error(
            "DB_POOL_RECYCLE_SECONDS cannot be negative.",
            hint="Use 0 to disable recycle behavior or a positive integer.",
        )
    if POSTGRES_URL == DEFAULT_POSTGRES_URL:
        _config_error(
            "POSTGRES_URL must be explicitly configured in production.",
            hint=(
                "Set POSTGRES_URL or POSTGRES_URL_FILE to the real monitor "
                "database connection string."
            ),
        )
    if JWT_SECRET == DEFAULT_JWT_SECRET:
        _config_error(
            "JWT_SECRET must be explicitly configured in production.",
            hint=(
                "Set JWT_SECRET or JWT_SECRET_FILE to a non-default secret "
                "before startup."
            ),
        )
    if len(JWT_SECRET) < 32:
        _config_error(
            "JWT_SECRET must be at least 32 characters long in production.",
            hint="Generate a longer secret and redeploy.",
        )
    if JWT_SECRET_NEXT and len(JWT_SECRET_NEXT) < 32:
        _config_error(
            (
                "JWT_SECRET_NEXT must be at least 32 characters long "
                "in production."
            ),
            hint="Use a 32+ character overlap secret during rotation.",
        )

if OTEL_EXPORTER not in {"otlp", "console"}:
    _config_error(
        f"Invalid OTEL_EXPORTER='{OTEL_EXPORTER}'.",
        hint="Use one of: otlp, console.",
    )

if OTEL_TRACING_ENABLED and OTEL_EXPORTER == "otlp":
    if not OTEL_EXPORTER_OTLP_ENDPOINT:
        _config_error(
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT is required when tracing "
                "is enabled with the OTLP exporter."
            ),
            hint=(
                "Set OTEL_EXPORTER_OTLP_ENDPOINT to your collector URL or "
                "switch OTEL_EXPORTER=console for local debugging."
            ),
        )

if WEBHOOK_TIMEOUT_SECONDS <= 0:
    _config_error(
        "WEBHOOK_TIMEOUT_SECONDS must be greater than zero.",
        hint="Set a positive timeout value such as 5.0.",
    )

if WEBHOOK_MAX_RETRIES < 0:
    _config_error(
        "WEBHOOK_MAX_RETRIES cannot be negative.",
        hint="Use 0 to disable retries or a positive integer.",
    )

if WEBHOOK_RETRY_BACKOFF_SECONDS < 0:
    _config_error(
        "WEBHOOK_RETRY_BACKOFF_SECONDS cannot be negative.",
        hint="Use 0 for immediate retries or a positive delay.",
    )

if WEBHOOK_CIRCUIT_BREAKER_THRESHOLD < 1:
    _config_error(
        "WEBHOOK_CIRCUIT_BREAKER_THRESHOLD must be at least 1.",
        hint="Use a positive integer failure threshold.",
    )

if WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS <= 0:
    _config_error(
        "WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS must be greater than zero.",
        hint="Use a positive recovery timeout in seconds.",
    )

for webhook_url in WEBHOOK_URLS:
    if not webhook_url.startswith(("http://", "https://")):
        _config_error(
            f"Invalid WEBHOOK_URLS entry '{webhook_url}'.",
            hint="Use comma-separated http:// or https:// URLs.",
        )

if MESSAGE_BROKER not in {"kafka", "rabbitmq"}:
    _config_error(
        f"Invalid MESSAGE_BROKER='{MESSAGE_BROKER}'.",
        hint="Use one of: kafka, rabbitmq.",
    )

if RABBITMQ_PREFETCH_COUNT <= 0:
    _config_error(
        "RABBITMQ_PREFETCH_COUNT must be greater than zero.",
        hint="Set a positive prefetch value such as 100.",
    )
