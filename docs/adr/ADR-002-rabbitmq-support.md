# ADR-002: RabbitMQ Support Alongside Kafka

**Status:** Implemented  
**Date:** 2026-06-23  
**Deciders:** Project leads  
**Context:** [Multi-broker configuration](../../docs/configuration.md#multiple-broker-clusters)

---

## Context

DB Monitor was originally built exclusively for Kafka as its message broker.
The CDC pipeline relied on Kafka-specific features: topic subscriptions, offset
management, consumer groups, and partition-aware consumption.

However, users in smaller deployments or non-Kafka-centric organisations
requested support for RabbitMQ as a lighter-weight transport. RabbitMQ is
widely deployed, has a smaller operational footprint than Kafka, and aligns
with existing messaging infrastructure at many potential adopters.

The question was whether to build a broker abstraction layer that unifies Kafka
and RabbitMQ, or to fork the consumer code path.

## Decision

Add RabbitMQ support as a first-class broker option behind a shared abstraction
layer, without forking the consumer logic.

### Design

1. A `BROKER_CLUSTERS` configuration array replaces the older
   `KAFKA_CLUSTERS` approach. Each cluster entry specifies `broker_kind` as
   either `kafka` or `rabbitmq`.
2. Kafka clusters use `bootstrap_servers`, `topics`, `consumer_group`, and
   optional `topic_partitions`.
3. RabbitMQ clusters use `connection_url`, `queue_names`, `prefetch_count`,
   and optional `dlq_destination`.
4. Both broker types share the same downstream pipeline: event parsing, schema
   discovery, persistence, WebSocket broadcast, and webhook delivery.

### Abstraction approach

A common `BrokerConsumer` interface (implemented in
`src/db_monitor/ingestion/brokers/`) wraps the per-broker SDK. The main
consumer loop operates on this interface and is broker-agnostic.

### Rejected alternatives

- **Fork the consumer code path:** Would have duplicated ~80% of the pipeline
  logic and created a maintenance burden.
- **Translate RabbitMQ messages onto a Kafka-compatible protocol:** Adds a
  translation layer with its own operational complexity.
- **Skip RabbitMQ support entirely:** Would exclude a meaningful portion of
  potential adopters.

## Consequences

**Positive:**

- Single consumer code path supports both brokers.
- RabbitMQ deployments get DLQ support, replay, batching, and circuit-breaker
  protection — the same as Kafka deployments.
- Mixed-broker deployments are possible (consume some topics from Kafka,
  others from RabbitMQ).
- Configuration is explicit and self-documenting.

**Negative:**

- Some Kafka-specific features (partition-level offset visibility, consumer
  lag metrics) do not have direct RabbitMQ equivalents.
- The broker abstraction layer adds ~400 lines of interface code.
- Testing must cover two broker backends.

## Compliance

- [x] Common `BrokerConsumer` interface in `src/db_monitor/ingestion/brokers/`
- [x] Kafka implementation
- [x] RabbitMQ implementation
- [x] Mixed-broker `BROKER_CLUSTERS` configuration
- [x] Tests for both backends
