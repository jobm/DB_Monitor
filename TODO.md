# Project Backlog

This file tracks realistic remaining work. Delivered items stay summarized here,
while exploratory or longer-horizon ideas live under `Future ideas` so the
active backlog stays readable.

## Recently Closed

### Framework-First Transition & CI Stabilization

- [x] Reposition the bundled `orderdb` / `catalogdb` / `shippingdb` stack as example assets instead of product-default assumptions
- [x] Define a framework-first configuration and packaging model for DB Monitor so new adopters can bring their own sources without editing repo-owned demo assets
- [x] Split docs, setup flows, and tests into `core-platform` versus `evaluation sandbox` paths so examples remain useful without shaping the runtime contract
- [x] Introduce formal source manifest contracts via JSON schemas (`sources.schema.json` & templates)
- [x] Decouple container services using docker compose profiles (`sandbox`) and align Makefile orchestrations
- [x] Stabilize the CI `Wait for app readiness` pipeline using pg_isready database sockets and verbose loop poll logs
- [x] Clean up and align SDK client workflow targets with the decoupled framework topology
- [x] Move canonical demo automation and seed assets under `examples/sandbox` while keeping top-level script entrypoints as compatibility shims

### Documentation And Developer Experience

- [x] Add comprehensive API documentation
- [x] Create example implementations
- [x] Add developer setup scripts
- [x] Improve API and configuration error messages with clearer remediation hints
- [x] Add architecture documentation
- [x] Create troubleshooting guide
- [x] Document deployment procedures
- [x] Add configuration examples

### Core Product And Operations

- [x] Redesign event storage around structured `KafkaEvent` records
- [x] Add event filtering, validation, and transformation configuration
- [x] Add authentication, RBAC, and API audit logging
- [x] Add Kafka SSL/TLS support
- [x] Add batch processing, caching, query optimization, and pooling
- [x] Add Prometheus metrics, alerting config, and dashboards
- [x] Add pagination, event-type filters, date-range queries, and search
- [x] Add unit, integration, smoke-load, and test-data tooling
- [x] Finalize the TUI change-history workflow and record drilldown UX

## Future Ideas

These are not committed backlog items yet.

### Advanced Features

- [x] Create a dedicated admin interface

### Recently Closed Future-Idea Work

- [x] Add support for custom event processors
- [x] Expand replay beyond current DLQ replay into fuller replay workflows
- [x] Add support for multiple Kafka clusters

### Scalability And Observability

- [x] Validate horizontal scaling under automated tests
- [x] Add distributed tracing
- [x] Add partition-aware scaling and placement controls
- [x] Add clustering support for more stateful components

### Integration Surface

- [x] Add support for additional databases
- [x] Implement webhook notifications
- [x] Create SDKs for common languages
- [x] Add support for different message brokers
- [x] Replace Kafka-style checkpoint semantics with broker-native progress tracking for RabbitMQ and other non-Kafka backends
- [x] Package the Python SDK for normal package imports and add package-level import coverage
