"""Normalization helpers for cluster configuration fields."""

from __future__ import annotations

from db_monitor.core.config_loader import config_error, parse_csv_list


def normalize_bootstrap_servers(
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


def normalize_cluster_topics(
    value: object,
    cluster_name: str,
) -> list[str]:
    """Normalize per-cluster topic configuration."""
    if value is None:
        return []
    if isinstance(value, str):
        return parse_csv_list(value)
    if isinstance(value, list):
        return [str(t).strip() for t in value if str(t).strip()]
    return []


def normalize_partition_list(
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


def normalize_topic_partitions(
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
        topic_partitions[topic_name] = normalize_partition_list(
            raw_partitions,
            cluster_name,
            topic_name,
        )
    return topic_partitions


def normalize_broker_kind(value: object, cluster_name: str) -> str:
    """Normalize one broker kind string."""
    broker_kind = str(value or "").strip().lower()
    if broker_kind not in {"kafka", "rabbitmq"}:
        config_error(
            f"Broker cluster '{cluster_name}' has unsupported "
            f"broker_kind '{broker_kind}'.",
            hint="Use one of: kafka, rabbitmq.",
        )
    return broker_kind


def normalize_rabbitmq_queue_names(
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
    return []
