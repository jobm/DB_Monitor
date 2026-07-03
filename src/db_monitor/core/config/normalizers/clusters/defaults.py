"""Default cluster builders for Kafka and RabbitMQ single-cluster configs."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db_monitor.core.config.dataclasses import BrokerSettings


def default_kafka_cluster(broker: "BrokerSettings") -> dict[str, object]:
    """Build the single-cluster compatibility config."""
    return {
        "name": "default",
        "bootstrap_servers": [broker.kafka_broker],
        "topics": list(broker.kafka_topics),
        "consumer_group": broker.kafka_consumer_group,
        "topic_partitions": None,
    }


def default_rabbitmq_cluster(broker: "BrokerSettings") -> dict[str, object]:
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
