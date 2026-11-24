"""Kafka and broker cluster configuration loaders — JSON parsing and validation."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, cast

from db_monitor.core.config_loader import config_error

from db_monitor.core.config.normalizers.clusters.defaults import (
    default_kafka_cluster,
    default_rabbitmq_cluster,
)
from db_monitor.core.config.normalizers.clusters.normalizers import (
    normalize_bootstrap_servers,
    normalize_broker_kind,
    normalize_cluster_topics,
    normalize_rabbitmq_queue_names,
    normalize_topic_partitions,
)

if TYPE_CHECKING:
    from db_monitor.core.config.dataclasses import BrokerSettings


def load_kafka_clusters(broker: "BrokerSettings") -> list[dict[str, object]]:
    """Load multi-cluster Kafka configuration from JSON when provided."""
    raw_clusters = os.getenv("KAFKA_CLUSTERS")
    if not raw_clusters:
        return [default_kafka_cluster(broker)]

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

        topic_partitions = normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics_raw = normalize_cluster_topics(
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
            "bootstrap_servers": normalize_bootstrap_servers(
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


def load_broker_clusters(broker: "BrokerSettings") -> list[dict[str, object]]:
    """Load generic broker cluster configuration."""
    raw_clusters = os.getenv("BROKER_CLUSTERS")
    if not raw_clusters:
        if broker.message_broker == "rabbitmq":
            return [default_rabbitmq_cluster(broker)]
        return [
            {
                "name": str(cluster["name"]),
                "broker_kind": "kafka","bootstrap_servers": list(
                        cast(list[str], cluster["bootstrap_servers"])
                    ),
                    "topics": list(
                        cast(list[str], cluster["topics"])
                    ),

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

        broker_kind = normalize_broker_kind(
            entry.get("broker_kind") or broker.message_broker,
            cluster_name,
        )
        if broker_kind == "rabbitmq":
            if entry.get("topic_partitions") is not None:
                config_error(
                    f"RabbitMQ cluster '{cluster_name}' does not support topic_partitions.",
                    hint="Remove topic_partitions for rabbitmq clusters.",
                )
            queue_names = normalize_rabbitmq_queue_names(
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

        topic_partitions = normalize_topic_partitions(
            entry.get("topic_partitions"),
            cluster_name,
        )
        topics_raw = normalize_cluster_topics(
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
            "bootstrap_servers": normalize_bootstrap_servers(
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
