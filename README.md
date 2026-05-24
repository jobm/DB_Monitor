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

The quickest way to evaluate DB Monitor is using the bundled docker-compose infrastructure, which launches the core monitoring services alongside three pre-seeded PostgreSQL databases acting as the source application stack:

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

The compose stack now runs with `APP_ENV=production`, applies migrations before
startup, then validates schema state on boot. `ALLOW_BOOTSTRAP` defaults to
`false`, so first-time local initialization must opt in explicitly as shown
above. In deployed environments, keep bootstrap disabled and pre-provision or
rotate admin keys out of band.

The monitor container now runs as a non-root user and starts through an explicit
entrypoint that applies migrations, then `exec`s the app process for cleaner
signal handling. Production deployments can also source sensitive values from
`POSTGRES_URL_FILE`, `JWT_SECRET_FILE`, and `KAFKA_SSL_PASSWORD_FILE` instead of
plain environment variables. Graceful shutdown timing is configurable with
`APP_SHUTDOWN_TIMEOUT_SECONDS`.

Database pool sizing is now configurable with `DB_POOL_SIZE`,
`DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT_SECONDS`, `DB_POOL_RECYCLE_SECONDS`, and
`DB_POOL_PRE_PING` so production deployments can tune connection pressure
explicitly.

JWT signing now supports a staged rotation window through `JWT_SECRET_NEXT` or
`JWT_SECRET_NEXT_FILE`. The app always signs with `JWT_SECRET`, but it will
accept tokens signed by either secret while the overlap window is active. The
intended rotation flow is: set `JWT_SECRET_NEXT`, deploy, wait for outstanding
access and WebSocket session tokens to expire, promote the next secret into
`JWT_SECRET`, then remove `JWT_SECRET_NEXT`.

Connectors are registered automatically by the `connector-registrar` service. You can also run `./register-connectors.sh` manually.

Connector configs now use a reusable template in `connectors/postgres-template.json`
plus a canonical manifest in `connectors/sources.json`. Add a new source by
appending one manifest entry instead of creating another full
`*-connector.json` file.

Each enabled manifest entry only needs the source identity, hostname, and
monitored tables:

```json
{
   "source_name": "catalogdb",
   "database_hostname": "postgres-catalog",
   "tables": ["public.categories", "public.products"],
   "enabled": true
}
```

The registrar derives repeated Debezium fields from `source_name`, including the
connector name, topic prefix, replication slot, and schema history topic. Use
optional fields like `connector_name`, `slot_name`, `history_topic`, or
`config_overrides` only when a source must deviate from the defaults.

Both the connector registrar and the FastAPI consumer now read the same
manifest. That keeps Debezium registration and `KAFKA_TOPICS` subscription
aligned without maintaining two separate topic lists.

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

Run the API locally:

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.start_monitor
```

Apply tracked migrations locally before startup when you want an explicit schema
step:

```bash
make monitor-migrate
```

Validate that the database is already up to date without applying changes:

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.cli migrate validate
```

Run connector registration locally against the Docker Compose stack:

```bash
./register-connectors.sh
```

Preview rendered connector payloads without calling Kafka Connect:

```bash
DRY_RUN=true ./register-connectors.sh
```

Example-stack utilities now live under `examples/sandbox/`. The older
top-level demo scripts under `scripts/` are still available as compatibility
entrypoints for existing automation, but new docs and workflows should use the
`examples/sandbox/` paths.

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

The Textual dashboard now includes an admin console for admin credentials.
After connecting with an admin API key, press `a` from the main dashboard to
inspect readiness, API key inventory, consumer checkpoints, and DLQ entries.
From the same console you can create viewer or admin keys, rotate or revoke a
selected key, and replay either a single selected DLQ record or the current
batch from inside the TUI.

`monitor-test-integration` expects the stack to already be running on `localhost:8000`.
It now validates the admin checkpoint and DLQ replay recovery endpoints in
addition to the core viewer surfaces.
GitHub Actions now runs both the unit suite and a live integration job that
starts the infrastructure stack, launches the app locally against it, and runs
the same integration script automatically on pushes and pull requests. That
workflow now finishes with a smoke-load gate that exercises the mixed
`events,stats,tables,checkpoints` profile and fails if it sees request errors,
sub-100% success rate, or a p95 above 2000 ms.

`monitor-test-smoke` expects `DB_MONITOR_ADMIN_API_KEY` to be set unless the
stack is still in first-run bootstrap mode.

`monitor-test-scale` launches two local FastAPI replicas on separate ports,
subscribes to a websocket on one replica, triggers DLQ replay on the other,
and fails unless the event crosses replicas through the Postgres websocket
backplane. When `DB_MONITOR_ADMIN_API_KEY` is not set, the script can mint and
revoke a short-lived admin key directly in the local monitor database for the
duration of the validation. Use it after infrastructure or websocket changes
when you need to confirm that horizontal scaling still preserves live event
delivery.

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
