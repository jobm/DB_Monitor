# Code Review Issues Found (v4-flash)

Review date: 2026-05-21
Scope: Full project scan of `app/`, `tests/`, `tui/`, `sdk/`, `examples/`, `scripts/`, and root configuration/CI files.

---

## Production Readiness Summary

**Status: Beta / Pre-Production.** Core architecture is sound (Debezium → Kafka/RabbitMQ → consumer → storage → API), but there are **hard blockers** that prevent running in production today.

### 🔴 Hard Blockers (Must Fix Before Production)

| Issue | Why it's a blocker |
|---|---|
| **6 — No graceful shutdown** | Container orchestrators send SIGTERM on scale-down/restart. Without signal handlers, in-flight events get dropped — guarantees are broken for an audit log system. |
| **5 — RabbitMQ message loss on timeout** | If a consume timeout fires mid-processing, the message is neither nacked nor requeued. Losing events is unacceptable for an audit system. |
| **10 — No webhook retry** | If the downstream webhook endpoint blips for 30s, events are silently dropped. Audit systems need at-least-once delivery semantics. |
| **3 — No DLQ for bad messages** | A single malformed Debezium event stalls the consumer batch. Poison messages happen in production and need per-message error handling with a dead-letter queue. |
| **11 — Tracing initialized unconditionally** | Spurious connection errors and memory pressure in any environment without OpenTelemetry configured. |

### 🟡 Should Fix Before Heavy Traffic

| Issue | Risk |
|---|---|
| **3 — No circuit breaker / backpressure** | Under spike load, the consumer can overwhelm the monitor Postgres. |
| **5 — No consumer lag metrics** | Cannot alert on consumer lag, the #1 CDC operational metric. Blind to a falling-behind consumer. |
| **18 — No connector status validation** | If a Debezium connector fails, you won't know until someone manually checks. |
| **12 — JSON schema not enforced** | A misconfigured `sources.json` silently produces wrong behavior. |
| **13 — SDK/example drift** | Confusing for adopters integrating via the SDK. |

### 🟢 What's Already Production-Quality

- **Auth system**: JWT-based, scoped API keys, token refresh — solid.
- **Multi-broker support**: Kafka and RabbitMQ with a clean abstraction layer.
- **Schema discovery**: Dynamic, handles column additions gracefully.
- **Change tracking**: Row-level deltas, point-in-time lookups, column change history.
- **Metrics surface**: Prometheus integration with Grafana dashboards.
- **WebSocket broadcasting**: Cross-replica fanout via Postgres backplane.
- **CI pipeline**: Automated testing with sandbox integration checks.

### Estimated Effort

| Tier | Estimate |
|---|---|
| Hard blockers (items 5, 6, 7, 10, 11) | ~2-3 days |
| High/Medium operational gaps (items 3, 4, 8, 9, 12, 13, 15, 16, 18) | ~2-3 days |
| Polish (items 1, 2, 14, 17) | ~0.5 days |
| **Total for production readiness** | **~5-7 days of focused work** |

---

## 1. `app/models.py` — Unused imports & potential async mismatch

- Several imports from `sqlalchemy` (e.g., `PrimaryKeyConstraint`, possibly `Column`, `Integer`, `String`) may be unused depending on which engine layer is active.
- The async pattern uses `sqlalchemy.ext.asyncio` but mixes standard `Column`, `Integer`, etc. from plain `sqlalchemy` — verify these work correctly with `AsyncSession` and the chosen async driver.
- No `__tablename__` mismatch checking between the declarative base and per-class declarations.

## 2. `app/routes.py` — Large file, mixed concerns

- At ~700+ lines, routes for `/events`, `/stats`, `/tables`, `/admin`, `/auth` are all in one file. This should be split into separate blueprints/modules (e.g., `routes_events.py`, `routes_admin.py`, `routes_auth.py`, etc.).
- Several endpoints lack explicit `response_model=` annotations, so generated OpenAPI docs show generic schemas instead of the actual return types.

## 3. `app/consumer_service.py` — Error handling gaps

- `consume_events()` has a broad `except Exception` at the top level but no per-message dead-letter queue handling for individual deserialization failures. One malformed message can stall the entire batch.
- No backpressure mechanism — if the consumer falls behind on heavy write loads, there's no circuit breaker or slow-start to protect the monitor database.

## 4. `app/event_parser.py` — Hardcoded string literals

- Debezium field names like `"after"`, `"before"`, `"source"`, `"op"` are repeated as raw string literals throughout the file. These should be extracted to module-level constants to prevent typos and improve maintainability.

## 5. `app/message_brokers.py` — RabbitMQ timeout & Kafka metrics

- **RabbitMQBroker**: `consume()` calls `await asyncio.wait_for(queue.aget(), timeout=...)`. If the timeout fires mid-processing, the channel is not explicitly nacked/requeued — the in-flight message could be silently lost.
- **KafkaBroker**: Consumer group does not expose `commit_lag`, `consumer_lag`, or partition offset as Prometheus gauges. These are critical for operational monitoring.

## 6. `app/start_monitor.py` — No signal handler & non-logger output

- `main()` relies on `lifecycle_manager.shutdown()` but never registers SIGTERM/SIGINT handlers. On container shutdown, default `asyncio` cancellation may not gracefully drain the Kafka/RabbitMQ consumer, potentially losing buffered events.
- Warnings about missing/corrupt manifests print to `stdout` via `print()`, not through the `logging` module. These messages will not appear correctly in Docker's `json-file` log driver context.

## 7. `app/audit_log.py` — Dynamic filter concatenation risk

- The audit log query builder appears to concatenate column names for filtering without using parameterized queries or SQLAlchemy's `.where()` clause properly. If user-controlled input reaches the column filter, this could be vulnerable to injection or unexpected query behavior.

## 8. `tests/conftest.py` — Test isolation via shared state

- Many fixtures (`client`, `db_session`, `event_pipeline`) reuse the same database across test functions without explicit truncation or rollback between tests. Tests that mutate shared state (insert events, create API keys, etc.) can bleed into each other when run in randomized order, causing flaky failures.

## 9. `tui/client.py` — Auth error handling doesn't distinguish 401 vs 403

- The `TUIClient` raises a generic `TUIError` for both HTTP 401 (expired/bad token) and 403 (insufficient permissions). A stale token silently triggers a generic error instead of prompting the user to re-login, which would be the correct UX.

## 10. `app/webhooks.py` — No retry or circuit breaker

- Webhook delivery failures are logged but never retried. There is no exponential backoff, retry queue, or circuit breaker. If the downstream webhook endpoint is temporarily down, the event is silently dropped.

## 11. `app/tracing.py` — Tracing initialized unconditionally at import time

- The OpenTelemetry tracer setup runs at module import time regardless of whether tracing is actually configured. This means it initializes resources and may attempt to connect to an OTLP endpoint even when `OTEL_EXPORTER_OTLP_ENDPOINT` is not set, wasting memory and potentially generating spurious connection errors.

## 12. `connectors/sources.json` vs `connectors/sources.schema.json` — Schema non-compliance

- The runtime `sources.json` uses properties like `source_name`, `connector_name`, `database_hostname`, `tables`, etc. The JSON schema `sources.schema.json` defines slightly different property names and constraints. There is no validation step anywhere that checks the manifest against its schema at startup or deploy time.

## 13. Example client (`examples/python_api_client.py`) vs SDK client (`sdk/python/db_monitor_sdk/client.py`) — Feature drift

- The example client exposes methods (e.g., `list_tables_with_columns()`) that the SDK client does not have.
- The SDK client has features (e.g., `get_checkpoint()`) that the example client lacks.
- These should be reconciled so the example is a thin wrapper around the SDK, or the SDK absorbs all useful methods from the example.

## 14. Hardcoded base URL in `examples/javascript_api_client.mjs`

- The JS example hardcodes `http://localhost:8000`. The SDKs should accept a configurable base URL via constructor or environment variable. This limits usability for non-local deployments.

## 15. `app/metrics.py` — No metric cleanup on schema/table removal

- If a table is dynamically removed from monitoring (via source manifest update), the corresponding Prometheus gauges (`event_count`, `table_size_bytes`, etc.) remain registered with stale label values. These should be cleaned up when source metadata is removed.

## 16. `app/row_identity.py` — Composite key representation

- The primary key resolver returns a JSON string for composite primary keys (via `json.dumps(pk_dict)`). Downstream consumers parsing this string would need to know the internal key structure. Returning a structured dict instead would be more robust.

## 17. `scripts/seed_admin_api_key.py` — No idempotency guard

- Running the script twice creates duplicate admin keys for the same `owner_name`. It should check for an existing key (by owner_name) before inserting, or upsert.

## 18. `register-connectors.sh` — No connector status validation

- The script POSTs connector configs to Debezium Connect but never polls the connector status endpoint to verify connectors transition to `RUNNING`. A misconfiguration (wrong hostname, port, or credentials) goes completely undetected until someone manually checks.

---

## Severity Key

| Severity | Count | Description |
|----------|-------|-------------|
| 🔴 **Critical** | 0 | Data loss or security vulnerability in production paths |
| 🟠 **High** | 6 | Items 3, 5, 6, 7, 10, 11 — correctness, data integrity, or operational gaps |
| 🟡 **Medium** | 9 | Items 1, 2, 4, 8, 9, 12, 13, 15, 16 — maintainability, consistency, or robustness |
| 🔵 **Low** | 3 | Items 14, 17, 18 — polish, ergonomics, and DX improvements |
