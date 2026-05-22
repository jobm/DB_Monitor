# Architecture

DB Monitor captures CDC events from Debezium-backed Kafka topics, stores both
raw and derived audit data, and exposes query, monitoring, and realtime
interfaces for operators.

## End-To-End Flow

```text
Source Postgres DBs
  -> Debezium Connect
  -> Kafka topics
  -> FastAPI consumer service
  -> Monitor Postgres
     -> REST API
     -> WebSocket stream
     -> Prometheus metrics
     -> TUI dashboard
```

## Runtime Components

| Component | Responsibility |
| --- | --- |
| `postgres-order`, `postgres-catalog`, `postgres-shipping` | Source databases emitting logical replication changes |
| `connect` | Debezium Kafka Connect worker that registers PostgreSQL CDC connectors |
| `kafka` | CDC transport and consumer coordination |
| `postgres-monitor` | Persistent store for events, schema catalog, deltas, API keys, audit logs, checkpoints, and DLQ records |
| `monitor-server` | FastAPI app, Kafka consumer, auth surface, metrics, and WebSocket broadcaster |
| `prometheus`, `grafana`, `alertmanager` | Monitoring, dashboards, and alert evaluation |
| `tui` | Operator-facing Textual dashboard for record history exploration |

## Main Application Modules

| Module | Role |
| --- | --- |
| `app/consumer_service.py` | Consumes Kafka topics, applies retries, manages checkpoints, DLQ persistence, and replay |
| `app/event_parser.py` | Parses raw messages into structured event fields |
| `app/event_pipeline.py` | Applies filtering, validation, and masking rules |
| `app/schema_discovery.py` | Maintains discovered tables and columns from Debezium payloads |
| `app/change_processor.py` | Extracts per-column deltas and point-in-time values |
| `app/routes.py` | HTTP API for tables, events, changes, auth, readiness, checkpoints, and DLQ replay |
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
- Troubleshooting guide: `troubleshooting.md`
- Recovery runbook: `runbooks/recovery-and-validation.md`