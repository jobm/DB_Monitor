# Project Backlog

This file tracks realistic remaining work. Delivered items stay summarized here,
while exploratory or longer-horizon ideas live under `Future ideas` so the
active backlog stays readable.

## Recently Closed

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
- [ ] Add clustering support for more stateful components

### Integration Surface

- [ ] Add support for additional databases
- [ ] Implement webhook notifications
- [ ] Create SDKs for common languages
- [ ] Add support for different message brokers
