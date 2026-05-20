# DB Monitor

DB Monitor is a FastAPI-based CDC monitoring service that consumes Debezium events from Kafka, stores them in PostgreSQL, and exposes raw events, discovered table metadata, column-level changes, metrics, and realtime notifications.

## Architecture

```text
Source Postgres DBs -> Debezium -> Kafka -> FastAPI consumer -> Monitor Postgres -> API / WebSocket / Metrics
```

The default stack monitors three source databases:

| Service | Source tables | External DB port |
| --- | --- | --- |
| `orderdb` | `public.orders`, `public.customers` | `5434` |
| `catalogdb` | `public.categories`, `public.products` | `5435` |
| `shippingdb` | `public.shipments`, `public.drivers` | `5436` |

The monitor database is exposed on `5437`.

## Current capabilities

- Multi-topic Kafka consumption with manual commits
- Retry, batching, DLQ forwarding, and circuit-breaker protection
- Persistent Kafka checkpoint snapshots and DLQ replay controls for recovery
- Schema discovery for monitored tables and columns
- Column-level change history and point-in-time lookup
- API-key lifecycle management plus short-lived bearer and WebSocket session tokens
- Buffered API audit logging off the request path
- Prometheus metrics and a bundled Prometheus/Grafana/Alertmanager stack
- WebSocket event broadcast for live updates, including cross-replica fanout via Postgres backplane

## Quick start

1. Start the stack:
   ```bash
   mkdir -p secrets
   cp secrets/monitor_postgres_url.example secrets/monitor_postgres_url
   cp secrets/monitor_jwt_secret.example secrets/monitor_jwt_secret
   ALLOW_BOOTSTRAP=true docker compose up -d
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
- `POST /auth/keys/{key_id}/rotate`
- `POST /auth/keys/{key_id}/revoke`
- `GET /admin/checkpoints`
- `GET /admin/dlq`
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

Run the API locally:

```bash
cd app
uv run python start_monitor.py
```

Apply tracked migrations locally before startup when you want an explicit schema
step:

```bash
make monitor-migrate
```

Validate that the database is already up to date without applying changes:

```bash
cd app
uv run python migrate.py validate
```

Run connector registration locally against the Docker Compose stack:

```bash
./register-connectors.sh
```

Preview rendered connector payloads without calling Kafka Connect:

```bash
DRY_RUN=true ./register-connectors.sh
```

Useful commands:

```bash
make monitor-up
make monitor-test
make monitor-test-integration
make monitor-test-smoke
make monitor-tui
```

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

Operational recovery and live-validation steps are documented in
`docs/runbooks/recovery-and-validation.md`.
Alert thresholds and first-response guidance are documented in
`docs/runbooks/alerts-and-thresholds.md`.

## Data model summary

- `events`: structured raw CDC events
- `monitored_tables`: discovered tables keyed by service/database/table
- `monitored_columns`: discovered columns for each monitored table
- `column_changes`: per-column history with `old_value` and `new_value`
- `api_keys`: hashed API keys and roles
- `api_audit_logs`: API access log entries

## Known limitations

- Production deployments should run `uv run python migrate.py apply` before app
   startup and use `APP_ENV=production` or `DB_SCHEMA_MODE=validate` so the app
   fails fast when required migrations are missing.
- When Debezium schema envelopes are disabled, column discovery falls back to inferring column names and coarse types from `before`/`after` payloads. Primary key and nullability metadata are therefore best-effort in that mode.
- `/health` checks application lifecycle state, database connectivity, and consumer state, but it does not yet expose full broker lag or deep Kafka admin diagnostics.
