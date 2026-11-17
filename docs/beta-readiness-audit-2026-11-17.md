# DB Monitor: Architecture and Beta-Readiness Audit

**Date:** 2026-11-17  
**Repository:** `/home/jobm/Documents/codebases/DB_Monitor`  
**Package version:** `0.0.1`  
**Audit type:** Backend architecture, implementation status, beta readiness, remediation roadmap  
**Code changes made:** None

---

## Executive Summary

DB Monitor has a solid architecture and a broad feature surface: multi-broker CDC ingestion, batching, DLQ/checkpoints, tenant-aware query APIs, WebSocket fan-out, migrations, retention, metrics/tracing, SDKs, and Helm packaging.

It is **not beta-ready as-is**. It is suitable for a controlled single-tenant pilot only after the P0 items are fixed. Multi-tenant beta should wait until tenant isolation is completed across API-key provisioning and WebSocket delivery.

Primary blockers:

1. Docker image build fails because `uv.lock` is not copied before `uv sync --frozen`.
2. The core test suite has 10 failures, so CI is red.
3. Tenant isolation is incomplete: no API path creates tenant-scoped keys, rotation drops `tenant_id`, and WebSockets broadcast all events to all clients.
4. Helm `existingSecret` support is effectively broken.
5. Consumer resilience is insufficient: no explicit mid-stream reconnect/rebalance handling, and transient failures can shut down the API.

---

## 1. Scope and Method

Reviewed:

- `src/db_monitor` application, ingestion, consumer, storage, auth, routing, lifecycle, backplane, schema, and operational modules.
- `tests/`, `pytest.ini`, `Makefile`, and `.github/workflows/ci.yml`.
- `Dockerfile`, `container-entrypoint.sh`, `.dockerignore`, `docker-compose.yml`.
- `deploy/helm/db-monitor` chart templates and values.
- Connector manifests, deployment/config docs, readiness reports, SDK/TUI surfaces.

Verification performed:

- Ran core tests.
- Reproduced the Docker dependency-install failure in a clean directory.
- Exercised targeted failing tests to capture exact errors.
- Traced tenant filtering from API-key model through request dependencies and query routes.
- Inspected Helm secret/ConfigMap wiring and WebSocket fan-out behavior.

Limitations:

- No full `docker build` or Kubernetes deployment was executed.
- No production-scale load, failover, or security penetration testing was performed.
- Findings are based on the dirty local working tree at audit time.

---

## 2. Architecture Assessment

### Data flow

```text
Debezium / connector manifest
        |
        v
Kafka or RabbitMQ
        |
        v
Async consumer -> filter/validate/transform -> Postgres event + change tables
        |                                              |
        |                                              v
        +----------------------------> FastAPI REST + WebSocket + webhooks
                                               |
                                               v
                              Postgres LISTEN/NOTIFY or Redis Pub/Sub backplane
```

### Strengths

- Clear module boundaries under `src/db_monitor`: ingestion, consumer, storage, routes, auth, backplane, lifecycle, schema.
- Async SQLAlchemy with pooled connections and pool metrics.
- Manual Kafka commits, checkpoints, batching, bulk-write path, DLQ, and circuit breaker.
- Retention/archive and partition-maintenance tasks.
- Migrations, production config validation, Prometheus metrics, and OpenTelemetry.
- Manifest-driven connector registration and multi-service topic configuration.
- Broad documentation and a substantial test suite.

### Architecture risks

- API and consumer run in the same process. A critical consumer failure can trigger application shutdown.
- WebSocket broadcast is awaited inside ingestion and sends sequentially to every client. One slow client can block event processing.
- Multi-tenancy is only partially implemented: storage/query filters exist, but key provisioning and WebSocket delivery do not enforce tenant scope.
- Packaging and deployment assets have drifted from the application code and current tests.

---

## 3. Capability Status

| Area | Status | Notes |
|---|---|---|
| CDC ingestion | Implemented | Kafka/RabbitMQ adapters, transforms, filtering, validation, DLQ, checkpoints, batch/bulk paths. |
| Query API | Implemented | Events, changes, point-in-time values, table discovery, keyset pagination, tenant filters. |
| Auth | Partial | API keys, JWT, WS session tokens, roles. Tenant-scoped key lifecycle is incomplete. |
| Realtime | Partial | WS manager and cross-replica backplane exist. Missing heartbeat/resume, backpressure, and tenant filtering. |
| Operations | Implemented with gaps | Health/readiness, metrics, tracing, retention, partition maintenance. Probe semantics and metrics exposure need review. |
| Deployment | Blocked | Docker build failure and Helm secret-wiring defect. |
| Tests/docs | Partial | Broad coverage and docs, but core tests are red and documentation overstates completion. |

---

## 4. Beta-Readiness Findings

### P0: Must fix before beta

#### P0.1 Docker image cannot build

`Dockerfile:15` copies only `pyproject.toml` and `README.md`; `Dockerfile:18` and `Dockerfile:24` run `uv sync --frozen`, but `uv.lock` is never copied into the image.

Confirmed in a clean directory:

```text
error: Unable to find lockfile at `uv.lock`, but `--frozen` was provided.
```

Impact: container image build fails, so Docker Compose and Helm deployments cannot start.

Required fix:

- Copy `uv.lock` before the first `uv sync --frozen`.
- Add a `docker build` smoke test to CI.

#### P0.2 Core CI is red

Command:

```bash
.venv/bin/python -m pytest -m core tests/ -q --maxfail=100
```

Result: **10 failures**.

Known failures:

- `tests/test_main.py::test_http_invalid_api_key_increments_failed_auth_metric`
- `tests/test_main.py::test_http_invalid_bearer_increments_failed_auth_metric`
- `tests/test_routes.py::test_get_changes_parses_service_and_timestamps`
- `tests/test_routes.py::test_get_value_at_time_legacy_resolves_service_from_table_name`
- `tests/test_routes.py::test_get_events_parses_time_filters`
- `tests/test_routes.py::test_get_events_supports_record_filters`
- `tests/test_routes.py::test_get_changes_requires_unambiguous_table_reference`
- `tests/test_routes.py::test_get_changes_reports_table_discovery_hint`
- `tests/test_schema_discovery.py::test_schema_cache_backplane_notification_invalidates_cache`
- `tests/test_schema_discovery.py::test_schema_cache_backplane_ignores_local_origin`

Causes:

1. `tests/test_main.py` replaces async `audit_log_writer.enqueue` with a synchronous lambda:

   ```text
   TypeError: object NoneType can't be used in 'await' expression
   ```

2. Route tests call route functions directly, leaving `tenant_filter=Depends(get_tenant_filter)` unresolved:

   ```text
   403: Access denied. This API key is scoped to tenant 'Depends(dependency=<function get_tenant_filter ...>)',
   but service_name 'orderdb' was requested.
   ```

3. Schema-cache tests call a removed method:

   ```text
   AttributeError: 'SchemaCacheBackplane' object has no attribute '_deliver_notification'
   ```

   Production code now uses `_handle_notification`.

Required fix:

- Use `AsyncMock`/dependency overrides instead of direct route calls.
- Update schema-cache tests to the current handler.
- Require green core tests before release.

#### P0.3 Tenant isolation is not enforceable through supported APIs

`create_api_key()` does not accept or set `tenant_id` (`src/db_monitor/routes/auth/key_routes/keys.py:45-51`). No `ApiKey(..., tenant_id=...)` creation path exists in `src/`.

`rotate_api_key()` does not copy `tenant_id` from the original key (`keys.py:118-124`). A scoped key rotated to `tenant_id=NULL` is treated as unrestricted by data routes (`src/db_monitor/routes/data/changes.py:64-73`).

Impact:

- Scoped keys cannot be provisioned through the API.
- Rotating a scoped key can silently remove its tenant restriction.
- A viewer key with `tenant_id=NULL` can read all tenants’ data.

Required fix:

- Add `tenant_id` to key creation.
- Copy `tenant_id` during rotation.
- Restrict administration so a tenant-scoped admin cannot administer other tenants.
- Add regression tests proving scoped keys cannot read other tenants’ rows.

#### P0.4 WebSocket delivery is not tenant-scoped

`/ws/events` authenticates the credential (`src/db_monitor/main.py:141-170`), but `WebSocketManager.connect()` stores no tenant and `broadcast()` sends every event to every connected client (`src/db_monitor/ws_manager.py:64-92`).

Impact: a tenant-scoped key can receive other tenants’ events.

Required fix:

- Associate each WebSocket connection with its tenant.
- Filter broadcasts by tenant before delivery.
- Add cross-tenant WebSocket isolation tests.

#### P0.5 Helm `existingSecret` support is broken

The Deployment always references the chart-generated secret (`deploy/helm/db-monitor/templates/deployment.yaml:47-51`).

When `database.existingSecret` or `auth.existingSecret` is set:

- The ConfigMap omits `POSTGRES_URL` / `JWT_SECRET` (`templates/configmap.yaml:12-14, 54-55`).
- The generated Secret also omits them (`templates/secret.yaml:9-17`).
- The Deployment does not reference the user-supplied secret.

Impact: documented production secret workflows lead to default values and likely startup failure under production validation.

Required fix:

- Wire the selected existing secret into `envFrom`, or remove the option.
- Add `helm lint` and `helm template` checks for generated and existing-secret paths.

#### P0.6 Consumer failure can take down the API

Kafka uses manual commits and retry limits, but no explicit mid-stream reconnect loop or rebalance handling was found. Transient broker/commit failures can propagate into critical-task failure and graceful shutdown of the whole application.

Required fix:

- Add reconnect/backoff around broker consume and commit failures.
- Keep the API alive while the consumer reconnects.
- Surface reconnecting state through `/readyz`.
- Add chaos/failover tests for broker restart and commit failure.

### P1: Beta hardening

- WebSocket heartbeat/ping-pong, reconnect/resume with last-event ID, bounded queues, slow-client timeout, and origin checks.
- Authenticate or network-restrict `/metrics`; add CORS/security headers; avoid exposing WS session tokens in query strings.
- Authenticate once per request instead of middleware plus route-dependency double authentication.
- Add Redis backplane service to Compose and reconnect/monitoring tests for both backends.
- Helm NetworkPolicy egress for external Postgres/Kafka/Redis/webhooks/OTLP; use `/livez` for liveness; include audit-log failure in readiness.
- Reuse one `SchemaDiscovery` instance per process so its cache is effective.
- Validate the bulk-write path and migrations 0017/0018 under production-size data and load.

### P2: Polish

- Align chart `appVersion: 0.1.0` with package `0.0.1` and verify published image tags.
- Remove stale `app/` references and compatibility stubs, or make them functional.
- Replace deprecated `datetime.utcnow()`.
- Add SBOM/dependency scanning, backup/restore drills, and stale-doc cleanup.

---

## 5. Recommended Remediation Order

1. Fix Dockerfile lockfile copy and add image-build CI.
2. Restore green core tests.
3. Complete tenant-scoped API-key creation, rotation, and administration.
4. Add tenant filtering to WebSocket delivery.
5. Fix Helm existing-secret wiring and add chart validation.
6. Add consumer reconnect/backoff and isolate consumer failure from API availability.
7. Harden WebSocket lifecycle, metrics/auth exposure, and deployment network policies.
8. Run production-scale load, failover, and security validation before multi-tenant beta.

---

## 6. Release Recommendation

| Target | Recommendation |
|---|---|
| Controlled single-tenant pilot | Conditional; fix P0.1, P0.2, P0.5, P0.6 first. |
| General closed beta | Not ready. |
| Multi-tenant beta | Not ready; complete P0.3 and P0.4 and add isolation regression tests. |
| Production GA | Not ready; complete P0/P1 and production validation. |

---

## Appendix: Key Files

- `Dockerfile` — missing `uv.lock` copy before `uv sync --frozen`.
- `src/db_monitor/routes/auth/key_routes/keys.py` — key creation/rotation tenant gaps.
- `src/db_monitor/auth/deps.py` — tenant-filter dependency.
- `src/db_monitor/routes/data/changes.py` — tenant enforcement and unrestricted `None` behavior.
- `src/db_monitor/main.py` — middleware, `/metrics`, WebSocket endpoint.
- `src/db_monitor/ws_manager.py` — unscoped WebSocket broadcast and sequential fan-out.
- `src/db_monitor/consumer/connection.py`, `consumer/loop.py` — consumer retry/reconnect behavior.
- `src/db_monitor/lifecycle_manager/lifecycle.py` — critical-task shutdown behavior.
- `src/db_monitor/schema/cache.py` — schema-cache backplane handler.
- `deploy/helm/db-monitor/templates/deployment.yaml` — hardcoded secret reference.
- `deploy/helm/db-monitor/templates/configmap.yaml`, `secret.yaml` — existing-secret handling.
- `.github/workflows/ci.yml`, `pytest.ini`, `Makefile` — CI/test commands.
- `tests/test_main.py`, `tests/test_routes.py`, `tests/test_schema_discovery.py` — current failures.
