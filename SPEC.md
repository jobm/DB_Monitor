# DB Monitor Technical Specification

**Version:** 1.1  
**Date:** 2026-04-29  
**Status:** Current-state specification

---

## 1. Purpose

DB Monitor captures change-data-capture events from multiple PostgreSQL services via Debezium and Kafka, stores both raw and derived audit data, and exposes query, monitoring, and realtime interfaces for operators and downstream consumers.

The goal of this document is to describe the system as it is currently implemented, clearly separate completed work from partial work, and record the next improvements without treating them as already done.

---

## 2. System overview

```text
Source Postgres DBs -> Debezium connectors -> Kafka topics -> FastAPI consumer -> Monitor Postgres
                                                                      |-> REST API
                                                                      |-> WebSocket stream
                                                                      |-> Prometheus metrics
```

### Monitored source services

| Service prefix | Tables |
| --- | --- |
| `orderdb` | `public.orders`, `public.customers` |
| `catalogdb` | `public.categories`, `public.products` |
| `shippingdb` | `public.shipments`, `public.drivers` |

### Runtime services

| Component | Responsibility |
| --- | --- |
| Debezium Connect | Reads WAL and publishes CDC events to Kafka |
| Kafka | Transport for CDC topics and DLQ messages |
| FastAPI app | Consumes CDC events and serves APIs |
| Monitor PostgreSQL | Stores raw events, schema catalog, change history, and API audit data |
| Prometheus / Grafana / Alertmanager | Metrics collection and visualization |

---

## 3. Implemented capabilities

| Capability | Status | Notes |
| --- | --- | --- |
| Multi-topic Kafka consumption | Implemented | Configured via `KAFKA_TOPICS` |
| Manual offset commits | Implemented | Commits happen after processing paths complete |
| Batch processing | Implemented | Controlled by `BATCH_ENABLED` and `BATCH_SIZE` |
| DLQ forwarding | Implemented | Failed messages are sent to `db-monitor-dlq` when enabled |
| Circuit breaker | Implemented | Protects processing during repeated failures |
| Schema catalog | Implemented | `monitored_tables` and `monitored_columns` |
| Schema discovery | Implemented | Reads Debezium schema envelopes when present, otherwise infers from `before`/`after` payloads |
| Column-level change history | Implemented | Stored in `column_changes` |
| Point-in-time lookup | Implemented | Canonical route is `/changes/{service}/{table}/{column}/at` |
| Event filtering and search | Implemented | `/events` supports pagination and basic filters |
| API-key auth and RBAC | Implemented | `viewer` and `admin` roles |
| API audit logging | Implemented | Stored in `api_audit_logs` |
| WebSocket broadcast | Implemented | `/ws/events` pushes new events |
| Prometheus metrics | Implemented | Metrics endpoint plus discovery, consumer, and request metrics |
| Structured request logging | Implemented | JSON logging formatter in app startup |
| Component health checks | Implemented | `/health` checks lifecycle, DB connectivity, and consumer state |

---

## 4. Partial or open capabilities

| Capability | Status | Current state |
| --- | --- | --- |
| Exact schema fidelity | Partial | When Debezium schema envelopes are disabled, types and PK metadata are inferred and therefore best-effort |
| Horizontal scaling validation | Partial | Consumer group support exists, but scale-out behavior is not covered by automated validation in this repo |
| Deep Kafka diagnostics | Partial | Health reporting tracks consumer runtime state, not broker-admin lag or partition diagnostics |
| Formal database migrations | Not implemented | Startup still relies on `Base.metadata.create_all()` and a lightweight backfill helper |
| Distributed tracing | Not implemented | No OpenTelemetry integration yet |
| Event replay | Not implemented | No explicit replay workflow or admin control path |
| Distributed cache | Not implemented | Schema cache is in-process with TTL and invalidation |

---

## 5. Data model

### 5.1 `events`

Structured event storage.

Key fields:

- `event_type`
- `event_time`
- `user_id`
- `service_name`
- `source_table_id`
- `operation`
- `event_data`
- `raw_payload`
- `capture_time`

### 5.2 `monitored_tables`

Tracks discovered tables.

Key fields:

- `service_name`
- `database_name`
- `table_name`
- `topic_name`
- `is_active`

Uniqueness is enforced across `service_name`, `database_name`, and `table_name`.

### 5.3 `monitored_columns`

Tracks discovered columns per table.

Key fields:

- `table_id`
- `column_name`
- `data_type`
- `is_primary_key`
- `is_nullable`
- `audit_enabled`

Uniqueness is enforced across `table_id` and `column_name`.

### 5.4 `column_changes`

Stores per-column deltas.

Key fields:

- `event_id`
- `table_id`
- `column_id`
- `operation`
- `old_value`
- `new_value`
- `changed_at`

### 5.5 `api_keys`

Stores hashed API keys and access roles.

### 5.6 `api_audit_logs`

Stores per-request API access records for authenticated and anonymous requests.

---

## 6. Processing model

1. The consumer subscribes to the configured Kafka topics.
2. Each message is parsed into a `KafkaEvent`.
3. The event pipeline may filter or transform the event.
4. The raw structured event is persisted.
5. Schema discovery updates `monitored_tables` and `monitored_columns`.
6. Column-level deltas are extracted into `column_changes`.
7. The event is broadcast to WebSocket subscribers.
8. Kafka offsets are committed after the processing path completes.
9. The committed topic/partition offsets are persisted as recovery checkpoints.

Error handling:

- transient DB writes are retried with exponential backoff
- repeated failures trip the circuit breaker
- failed messages are persisted for DLQ replay and can also be forwarded to the DLQ topic

---

## 7. API surface

### Public

- `GET /health`
- `GET /livez`
- `GET /readyz`
- `GET /metrics`
- `POST /auth/bootstrap`

### Session

- `POST /auth/token`
- `POST /auth/ws-token`

### Viewer

- `GET /info`
- `GET /tables`
- `GET /tables/{service_name}/{table_name}`
- `GET /tables/{service_name}/{table_name}/columns`
- `GET /events`
- `GET /events/stats`
- `GET /changes`
- `GET /changes/{service_name}/{table_name}/{column_name}/at`
- `GET /changes/{service.table}/{column_name}/at` (legacy-compatible route)

### Admin

- `POST /auth/keys`
- `POST /auth/keys/{key_id}/rotate`
- `POST /auth/keys/{key_id}/revoke`
- `GET /admin/checkpoints`
- `GET /admin/dlq`
- `POST /admin/dlq/{dlq_event_id}/replay`

### Realtime

- `GET /ws/events?session_token=<short-lived-token>`

Behavioral notes:

- `/changes` accepts either `table_name=<service>.<table>` or `service_name=<service>&table_name=<table>`
- timestamp filters use ISO 8601 strings
- point-in-time lookups are service-aware to avoid table-name ambiguity across multiple monitored services

---

## 8. Observability

Current observability includes:

- Prometheus counters and histograms for event consumption, processing failures, DB writes, Kafka commits, and API latency
- Gauges for discovered tables, discovered columns, circuit-breaker state, consumer lag, and last successful commit timestamp
- JSON-formatted logs for HTTP requests and runtime events
- Buffered audit-log persistence so request latency no longer waits on direct audit DB writes
- `/health` component checks for lifecycle state, DB connectivity, and consumer runtime status

Not yet implemented:

- distributed tracing
- broker-admin lag inspection
- alert-rule ownership inside this repo

---

## 9. Security model

- API keys are hashed before storage
- `viewer` keys can read data and metadata
- `admin` keys can mint new keys
- the bootstrap endpoint is intentionally one-time and only works before any active keys exist
- WebSocket access requires the same API key format via query string

Limitations:

- there is no tenant isolation
- there is no per-table or per-column authorization policy

---

## 10. Validation workflow

### Unit tests

```bash
make monitor-test
```

This runs the repository unit/regression suite through the app's managed `uv` environment.

### Integration checks

```bash
make monitor-test-integration
```

This expects the stack to already be reachable on `localhost:8000`.

### Local development

```bash
cd app
uv sync --group dev
uv run uvicorn main:app --reload
```

---

## 11. Known limitations

1. Formal migration management is still missing.
2. Schema discovery quality depends on whether Debezium schema envelopes are enabled.
3. Health reporting does not yet include Kafka admin-plane checks or lag by topic/partition.
4. Horizontal scaling is supported by consumer-group design but not proven by automated repository tests.
5. The auth library currently emits an upstream Python deprecation warning in tests.

---

## 12. Near-term roadmap

### Priority 1

- introduce a formal migration workflow
- extend health reporting with Kafka admin diagnostics where practical
- add more end-to-end validation around CDC ingestion and auth-protected routes

### Priority 2

- add distributed tracing
- improve schema fidelity when schema envelopes are absent
- define operational runbooks for DLQ handling and replay
- keep `docs/runbooks/recovery-and-validation.md` aligned with admin recovery endpoints

### Priority 3

- add replay/admin tooling
- add richer aggregation and analytics endpoints
- evaluate distributed caching only if the in-process cache becomes a real bottleneck
