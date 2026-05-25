# Architecture

DB Monitor is designed as a reusable, pluggable CDC audit-log framework. It decouples the core consumption, storage, api metadata tracking, and operational tasks from specific business schemas or source systems.

## End-To-End Flow

The pipeline operates on any source system generating compatible logical replication events:

```text
Pluggable Source DBs (Any monitored app database)
  -> Debezium Connect (or compatible CDC connectors)
  -> Message Broker (Kafka topics or RabbitMQ queues)
  -> FastAPI Core Consumer Service (Normalizes, filters, discovers schemas, records column state)
  -> Monitor PostgreSQL (Dedicated audit schema for events, discovered catalogs, delta history, API audits)
     -> REST API / SDKs
     -> WebSocket stream
     -> Prometheus metrics
     -> TUI dashboard / Admin recovery
```

## Runtime Components

DB Monitor distinguishes between **Core Platform Services** needed by any deployment, and the **Sandbox Layer** used solely for documentation, evaluation, and automated testing.

### Core Platform Services

| Component | Responsibility |
| --- | --- |
| `monitor-server` | FastAPI app, multi-broker/multi-cluster consumer, token manager, live schema discovery, and WebSocket broadcaster |
| `postgres-monitor` | Persistent DB storing normalized events, discovered metadata catalog, change deltas, API credentials, audit logs, recovery checkpoints, and DLQ entries |
| Message Broker | transport stream (Kafka cluster or RabbitMQ server) delivering raw change events |
| Debezium Connect | CDC worker capturing WAL logs and publishing event envelopes |
| `prometheus`, `grafana` | Monitoring, metric gathering, dashboards, and alerts |

### Example Sandbox Layout (Optional)

The bundled docker-compose stack provides three PostgreSQL applications to simulate a live customer environment:

| Container | Purpose |
| --- | --- |
| `postgres-order` | Simulates logical transactional Order databases (`orderdb`) |
| `postgres-catalog` | Simulates inventory/Catalog databases (`catalogdb`) |
| `postgres-shipping` | Simulates logistics/Fulfillment databases (`shippingdb`) |
| `connector-registrar` | Auto-registers Debezium connectors for the sandbox databases based on the checked-in manifest |

---

## Main Application Modules

| Module | Role |
| --- | --- |
| `app/consumer_service.py` | Consumes Kafka topics, applies retries, manages checkpoints, DLQ persistence, and replay |
| `app/event_parser.py` | Parses raw messages into structured event fields |
| `app/event_pipeline.py` | Applies filtering, validation, and masking rules |
| `app/schema_discovery.py` | Maintains discovered tables and columns from Debezium payloads |
| `app/change_processor.py` | Extracts per-column deltas and point-in-time values |
| `app/routes/` | HTTP API modules split across data, auth, and ops routes with shared helpers |
| `app/ws_manager.py` | WebSocket fanout for live event updates |
| `app/migrations.py` | Tracked schema migrations and legacy backfill steps |

## Storage Model

- `events`: structured CDC events with parsed metadata and raw payloads
- `monitored_tables`: discovered tables keyed by service, database, and table
- `monitored_columns`: discovered per-table columns and inferred metadata
- `column_changes`: per-column history with old and new values
- `api_keys`: hashed API keys, role, expiration, and revocation state
- `api_audit_logs`: buffered request audit records
- `consumer_checkpoints`: persisted Kafka offsets for recovery visibility
- `dead_letter_events`: persisted failed events that can be replayed

## Operational Model

1. The app subscribes to all configured Kafka topics.
2. Each message is parsed into a structured event.
3. Filtering and masking rules are applied.
4. The event is persisted.
5. Schema discovery updates table and column metadata.
6. Column-level changes are extracted into `column_changes`.
7. The event is broadcast to WebSocket subscribers.
8. Kafka offsets are committed only after the write path succeeds.
9. Checkpoints and DLQ state remain queryable via admin endpoints.

## Failure Handling

- transient database failures are retried with backoff
- repeated failures trip the consumer circuit breaker
- failed messages can be persisted to the DLQ table
- persisted DLQ rows can be replayed through the normal ingestion path

## Related Documents

- API reference: `../api-docs.md`
- Configuration reference: `configuration.md`
- Deployment guide: `deployment.md`
- Pre-v1 tenant isolation strategy: `pre-v1-tenant-isolation-strategy.md`
- Troubleshooting guide: `troubleshooting.md`
- Recovery runbook: `runbooks/recovery-and-validation.md`