# DB Monitor

DB Monitor is a pluggable, framework-first Change Data Capture (CDC) monitoring system and audit-log library. It consumes CDC event streams (e.g., Debezium events) from Kafka or RabbitMQ, normalizes and stores them in PostgreSQL, and exposes structured events, auto-discovered table/column metadata, column-level change history, point-in-time state lookup, metrics, and realtime notifications.

Adopters can integrate DB Monitor as an audit-log platform around their own databases and message brokers, or run the bundled self-contained example stack for local evaluation.

## Documentation

- API reference: [api-docs.md](api-docs.md)
- API stability policy: [docs/api-stability.md](docs/api-stability.md)
- Architecture: [docs/architecture.md](docs/architecture.md)
- Configuration: [docs/configuration.md](docs/configuration.md)
- Deployment: [docs/deployment.md](docs/deployment.md)
- Sandbox evaluation: [docs/example-sandbox.md](docs/example-sandbox.md)
- Framework transition plan: [docs/framework-transition-plan.md](docs/framework-transition-plan.md)
- Troubleshooting: [docs/troubleshooting.md](docs/troubleshooting.md)
- Recovery runbook: [docs/runbooks/recovery-and-validation.md](docs/runbooks/recovery-and-validation.md)
- Alerts runbook: [docs/runbooks/alerts-and-thresholds.md](docs/runbooks/alerts-and-thresholds.md)

## Architecture

At its core, DB Monitor decoupled the monitoring process from specific business schemas:

```text
Any Pluggable Source DBs -> Debezium Connect -> Kafka or RabbitMQ -> FASTAPI Core Consumer -> Monitor DB -> API / WebSocket / Metrics
```

Unlike hardcoded solutions, the framework is driven entirely by a dynamic **Source Manifest**. Adopters point core consumer and registrar services at their own manifest, and the system automatically subscribes to appropriate message streams, discovers table architectures, and tracks schema changes on the fly.

### Bundled Example Stack (Optional)

To help developers experiment with the platform locally, the repository contains a self-contained compose environment with three pre-configured source databases:

| Sample Database | Simulates | Monitored tables | External DB port |
| --- | --- | --- | --- |
| `orderdb` | Order management | `public.orders`, `public.customers` | `5434` |
| `catalogdb` | Product catalog | `public.categories`, `public.products` | `5435` |
| `shippingdb` | Fulfillment | `public.shipments`, `public.drivers` | `5436` |

In this sandbox deployment, the monitoring database runs on port `5437` and stores the unified audit log.

## Current capabilities

- Multi-broker consumption with Kafka and RabbitMQ adapters
- Multi-cluster broker consumption with per-cluster consumer tasks
- Retry, batching, DLQ forwarding, and circuit-breaker protection
- Optional custom event processors inside the ingestion pipeline
- Persistent Kafka checkpoint snapshots and single/batch DLQ replay controls
   for recovery
- Schema-qualified and collection-backed Debezium source normalization for
  additional database types
- Partition-aware Kafka placement controls through explicit topic assignments
- Cross-replica schema cache invalidation for fresher table metadata on
   horizontally scaled replicas
- Optional outbound webhooks for persisted CDC events
- Lightweight Python and JavaScript SDKs for common API workflows
- Dedicated Textual admin console for readiness, key management,
  checkpoints, and DLQ replay
- Optional OpenTelemetry tracing for HTTP, database, broker, and websocket paths
- Schema discovery for monitored tables and columns
- Column-level change history and point-in-time lookup
- API-key lifecycle management plus short-lived bearer and WebSocket session tokens
- Buffered API audit logging off the request path
- Prometheus metrics and a bundled Prometheus/Grafana/Alertmanager stack
- WebSocket event broadcast for live updates, including cross-replica fanout via Postgres backplane

## Quick start (using the Local Example Stack)

Use the bundled docker-compose stack for a fast local evaluation. Full sandbox
setup, traffic simulation, and teardown steps live in
`docs/example-sandbox.md`.

Minimal bootstrap flow:

1. Start the stack:
   ```bash
   mkdir -p secrets
   cp secrets/monitor_postgres_url.example secrets/monitor_postgres_url
   cp secrets/monitor_jwt_secret.example secrets/monitor_jwt_secret
   ALLOW_BOOTSTRAP=true make monitor-up-sandbox
   ```
2. Check the service:
   ```bash
   curl http://localhost:8000/readyz
   ```
3. Bootstrap the first admin key:
   ```bash
   curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
   ```
4. Exchange the returned `id.secret` value for a short-lived bearer token:
    ```bash
    curl -X POST \
       -H "X-API-Key: <id.secret>" \
       http://localhost:8000/auth/token
    ```

For traffic generation, websocket checks, and reset workflows, continue with
the sandbox guide at `docs/example-sandbox.md`.

## Integrating with Custom Infrastructure (As a Study / Adopter)

To run DB Monitor as a framework against your own custom message brokers and PostgreSQL databases:

1. **Deploy Core Monitor DB**: Set up a clean PostgreSQL instance to store DB Monitor persistent audit data (or point to an existing dedicated schema).
2. **Configure Environment Variables**: At minimum, provide:
   - `POSTGRES_URL` (pointing to your monitor database utilizing `postgresql+asyncpg://`)
   - `MESSAGE_BROKER=kafka` or `rabbitmq`
   - `KAFKA_BROKER` or `RABBITMQ_URL` pointing to your broker
3. **Provide a Source Manifest**: Define the databases, tables, and streams you want to audit. Point DB Monitor to your manifest by setting the path in `CONNECTOR_SOURCES_FILE` (defaults to looking for `connectors/sources.json`).
4. **Launch DB Monitor**: Run the core app outside of the example docker containers (e.g., in Kubernetes, an App Service, or locally using `PYTHONPATH=src:app uv run python -m db_monitor.start_monitor`).

For full details on production deployments, refer to [docs/deployment.md](docs/deployment.md) and [docs/configuration.md](docs/configuration.md).

## Docker Sandbox Details

Sandbox lifecycle commands and local operational workflows are documented in
`docs/deployment.md` and `docs/example-sandbox.md`.

Configuration and environment variable reference (including `*_FILE` secret
loading, schema mode, broker settings, pool tuning, and JWT rotation) is
documented in `docs/configuration.md`.

Source-manifest structure and validation rules are documented in
`docs/configuration.md` under the manifest contract section.

## Service endpoints

| Service | URL |
| --- | --- |
| FastAPI app | `http://localhost:8000` |
| Kafka UI | `http://localhost:8080` |
| Kafka Connect | `http://localhost:8083` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Alertmanager | `http://localhost:9094` |

## API highlights

Public endpoints:

- `GET /health`
- `GET /livez`
- `GET /readyz`
- `GET /metrics`
- `POST /auth/bootstrap` (only succeeds before any active key exists)

Session endpoints:

- `POST /auth/token`
- `POST /auth/ws-token`

Viewer endpoints:

- `GET /info`
- `GET /tables`
- `GET /tables/{service_name}/{table_name}`
- `GET /tables/{service_name}/{table_name}/columns`
- `GET /events`
- `GET /events/stats`
- `GET /changes`
- `GET /changes/{service_name}/{table_name}/{column_name}/at`
- `GET /changes/{service.table}/{column_name}/at` (legacy-compatible form)

Admin endpoint:

- `POST /auth/keys`
- `GET /auth/keys`
- `POST /auth/keys/{key_id}/rotate`
- `POST /auth/keys/{key_id}/revoke`
- `GET /admin/checkpoints`
- `GET /admin/dlq`
- `POST /admin/dlq/replay`
- `POST /admin/dlq/{dlq_event_id}/replay`

Realtime endpoint:

- `GET /ws/events?session_token=<short-lived-token>`

See `api-docs.md` for request and response details.

## Development

Install app dependencies, including the test group:

```bash
cd app
uv sync --group dev
```

## Releases

Release automation is driven by GitHub Actions:

1. Create and push a signed annotated tag (`git tag -s vX.Y.Z`).
2. The `Release` workflow verifies tag signatures, builds wheel/sdist,
   generates checksums, attests provenance, creates a GitHub Release with
   generated notes, and publishes to PyPI.
3. The `Release Drafter` workflow continuously updates a draft changelog-based
   release note on `main`.

For operational command flows (sandbox launch, local app run, migrations,
bootstrap, and validation), use `docs/deployment.md` as the source of truth.
Configuration details for those commands are documented in
`docs/configuration.md`.

Example-stack utilities live under `examples/sandbox/`. Compatibility wrappers
remain in `scripts/`, but new automation should target `examples/sandbox/`.

Useful commands:

```bash
make monitor-up
make monitor-up-sandbox
make monitor-lint
make monitor-typecheck
make monitor-package-smoke
make monitor-test-container-readyz
make monitor-test
make monitor-test-sandbox-pytest
make monitor-test-integration
make monitor-test-smoke
make monitor-test-scale
make monitor-tui
```

The Textual dashboard includes an admin console for credentials, readiness,
API-key lifecycle, checkpoints, and DLQ replay. Launch it with
`make monitor-tui`.

Operational recovery and live-validation steps are documented in
`docs/runbooks/recovery-and-validation.md`.
Alert thresholds and first-response guidance are documented in
`docs/runbooks/alerts-and-thresholds.md`.
Architecture, deployment, troubleshooting, and configuration references now
live under `docs/`.

## Data model summary

- `events`: structured raw CDC events
- `monitored_tables`: discovered tables keyed by service/database/table
- `monitored_columns`: discovered columns for each monitored table
- `column_changes`: per-column history with `old_value` and `new_value`
- `api_keys`: hashed API keys and roles
- `api_audit_logs`: API access log entries

## Known limitations

- Production deployments should run `PYTHONPATH=src:app uv run python -m db_monitor.cli migrate apply` before app
   startup and use `APP_ENV=production` or `DB_SCHEMA_MODE=validate` so the app
   fails fast when required migrations are missing.
- When Debezium schema envelopes are disabled, column discovery falls back to inferring column names and coarse types from `before`/`after` payloads. Primary key and nullability metadata are therefore best-effort in that mode.
- `/health` checks application lifecycle state, database connectivity, and consumer state, but it does not yet expose full broker lag or deep Kafka admin diagnostics.
