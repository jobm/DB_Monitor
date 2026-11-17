# Architecture

DB Monitor is a reusable, pluggable CDC audit-log framework for monitoring
any source database. It decouples the core consumption, storage, API metadata
tracking, and operational tasks from specific business schemas or source
systems.

## End-To-End Flow

The pipeline operates on any source system generating compatible logical
replication events:

```text
Source Databases (Any monitored application database — PostgreSQL, MySQL, etc.)
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

DB Monitor distinguishes between **Core Platform Services** needed by any
deployment, and the **Sandbox Layer** used solely for documentation,
evaluation, and automated testing.

### Core Platform Services

| Component | Responsibility |
| --- | --- |
| `monitor-server` | FastAPI app, multi-broker/multi-cluster consumer, token manager, live schema discovery, and WebSocket broadcaster |
| `postgres-monitor` | Persistent DB storing normalized events, discovered metadata catalog, change deltas, API credentials, audit logs, recovery checkpoints, and DLQ entries |
| Message Broker | Transport stream (Kafka cluster or RabbitMQ server) delivering raw change events |
| Debezium Connect | CDC worker capturing WAL logs and publishing event envelopes |
| `prometheus`, `grafana` | Monitoring, metric gathering, dashboards, and alerts |

### Example Topology (Demo Stack)

The bundled docker-compose stack provides an optional sandbox environment with
three simulated PostgreSQL source databases for evaluation and local
development. See [`docs/example-sandbox.md`](example-sandbox.md) for full
details.

| Container | Purpose |
| --- | --- |
| `postgres-order` | Simulates a transactional order database (`orderdb`) |
| `postgres-catalog` | Simulates an inventory / catalog database (`catalogdb`) |
| `postgres-shipping` | Simulates a logistics / fulfillment database (`shippingdb`) |
| `connector-registrar` | Auto-registers Debezium connectors for the sandbox databases based on the checked-in manifest |

---

## Module Layout

All canonical implementation lives under `src/db_monitor/`. The former `app/`
directory has been fully consolidated into this single tree.

| Module | Role |
| --- | --- |
| `consumer/` | Kafka/RabbitMQ consumption, checkpoints, DLQ persistence, retries, and replay |
| `ingestion/` | Event parsing, pipeline filtering, schema discovery, change processing, source metadata, broadcast, quota |
| `routes/` | HTTP API modules split across data, auth, and ops routes |
| `repositories/` | Data access layer for events, audit logs, and spill/replay operations |
| `schema/` | Cache and operations for discovered table/column metadata |
| `auth/` | Token management, API key authentication, database credential storage |
| `start_monitor/` | Startup orchestration, manifest loading, and Uvicorn server bootstrap |
| `core/` | Configuration loading, dataclass definitions, shared response helpers |
| `lifecycle_manager/` | Signal handling, graceful shutdown, and background task lifecycle |
| `main.py` | ASGI application entry point with middleware, WebSocket handler, and metrics |
| `lifespan.py` | Startup/shutdown orchestration helpers |
| `cli.py` | Command-line entry points |

## Storage Model

- `events`: Structured CDC events with parsed metadata and raw payloads
- `monitored_tables`: Discovered tables keyed by source, database, and table
- `monitored_columns`: Discovered per-table columns and inferred metadata
- `column_changes`: Per-column history with old and new values
- `api_keys`: Hashed API keys, role, expiration, and revocation state
- `api_audit_logs`: Buffered request audit records
- `consumer_checkpoints`: Persisted broker offsets for recovery visibility
- `dead_letter_events`: Persisted failed events that can be replayed

## Data Flow

1. The app subscribes to all configured broker topics/queues (derived from the
   source manifest or explicit `KAFKA_TOPICS` / `BROKER_CLUSTERS` env vars).
2. Each message from a **source database** CDC connector is parsed into a
   structured event.
3. Filtering and masking rules are applied.
4. The event is persisted to the monitor database.
5. Schema discovery updates table and column metadata.
6. Column-level changes are extracted into `column_changes`.
7. The event is broadcast to WebSocket subscribers.
8. Broker offsets are committed only after the write path succeeds.
9. Checkpoints and DLQ state remain queryable via admin endpoints.

## Failure Handling

- Transient database failures are retried with backoff
- Repeated failures trip the consumer circuit breaker
- Failed messages can be persisted to the DLQ table
- Persisted DLQ rows can be replayed through the normal ingestion path

## Related Documents

- API reference: `api-reference.html`
- Configuration reference: `configuration.md`
- Deployment guide: `deployment.md`
- Example sandbox: `example-sandbox.md`
- Pre-v1 tenant isolation strategy: `pre-v1-tenant-isolation-strategy.md`
- Troubleshooting guide: `troubleshooting.md`
- Recovery runbook: `runbooks/recovery-and-validation.md`